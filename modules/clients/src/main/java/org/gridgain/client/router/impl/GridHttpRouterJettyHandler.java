/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router.impl;

import net.sf.json.*;
import org.apache.http.*;
import org.apache.http.client.CookieStore;
import org.apache.http.client.*;
import org.apache.http.client.entity.*;
import org.apache.http.client.methods.*;
import org.apache.http.conn.scheme.*;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.*;
import org.apache.http.impl.client.*;
import org.apache.http.impl.conn.*;
import org.apache.http.message.*;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.*;
import org.eclipse.jetty.util.*;
import org.gridgain.client.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import io.netty.util.*;
import org.jetbrains.annotations.*;

import javax.net.ssl.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.net.*;
import java.util.*;

import static org.apache.http.conn.ssl.SSLSocketFactory.*;
import static org.gridgain.grid.kernal.processors.rest.GridRestResponse.*;

/**
 *
 */
class GridHttpRouterJettyHandler extends AbstractHandler {
    /** Client for message forwarding. */
    private final GridRouterClientImpl client;

    /** Log. */
    private final GridLogger log;

    /** HTTP client. */
    private final HttpClient httpClient;

    /** Number of server requests. */
    private volatile long reqCnt;

    /**
     *
     * @param client Client for message forwarding.
     * @param sslCtx SSL context.
     * @param maxTotal Max total connections.
     * @param maxPerRoute Max connections per route.
     * @param log Logger.
     * @see <a href="http://hc.apache.org/httpcomponents-client-ga/tutorial/html/connmgmt.html">
     *     HTTP client connection management</a>
     */
    GridHttpRouterJettyHandler(GridRouterClientImpl client, SSLContext sslCtx, int maxTotal, int maxPerRoute,
        GridLogger log) {
        this.client = client;
        this.log = log;

        PoolingClientConnectionManager conMgr = new PoolingClientConnectionManager();

        conMgr.setMaxTotal(maxTotal);
        conMgr.setDefaultMaxPerRoute(maxPerRoute);
        conMgr.getSchemeRegistry().register(new Scheme("grid", 11211,
            sslCtx == null ? new PlainSocketFactory() : new SSLSocketFactory(sslCtx, ALLOW_ALL_HOSTNAME_VERIFIER)));

        httpClient = new DefaultHttpClient(conMgr);

        // Forbidding reuse - it leads to starvation on node side. See GG-4692 for example.
        ((AbstractHttpClient)httpClient).setReuseStrategy(new NoConnectionReuseStrategy());
        ((AbstractHttpClient)httpClient).setCookieStore(new EmptyCookieStore());
    }

    /** {@inheritDoc} */
    @Override public void handle(String s, Request req, HttpServletRequest httpReq, HttpServletResponse httpRes)
        throws IOException, ServletException {
        try {
            // Parse destination node ID from the request.
            String destIdStr = req.getParameter("destId");

            UUID destId;

            try {
                destId = F.isEmpty(destIdStr) ? null : UUID.fromString(destIdStr);
            }
            catch (IllegalArgumentException ignore) {
                writeFailure(httpRes, "Failed to parse required parameter 'destId': " + destIdStr);

                return;
            }

            // Check available topology.
            Collection<? extends GridClientNode> nodes = client.compute().nodes();

            if (F.isEmpty(nodes)) {
                writeFailure(httpRes, "No available nodes on the router.");

                return;
            }

            Collection<InetSocketAddress> addrs = getEndpoints(destId);

            if (F.isEmpty(addrs)) {
                writeFailure(httpRes, "No available nodes on the router for destination node ID: " + destId);

                return;
            }

            Exception lastEx = null;

            for (InetSocketAddress addr : addrs) {
                try {
                    forward(extractRequestParameters(req), httpRes, addr);

                    return;
                }
                catch (Exception e) {
                    U.warn(
                        log,
                        "Failed to perform HTTP request forwarding " +
                        "(will try next endpoint available for this request): " + e.getMessage(),
                        "Failed to forward HTTP request (will try another endpoint).");

                    lastEx = e;
                }
            }

            if (lastEx != null)
                writeFailure(httpRes, "Failed to route request to all of the available nodes. " +
                    "Last exception was: " + lastEx.getMessage());
        }
        catch (GridClientException e) {
            log.warning("Failed to connect to grid (is grid started?).", e);

            writeFailure(httpRes, "Failed to connect to grid (is grid started?): " + e.getMessage());
        }
        catch (RuntimeException e) {
            // Had to do it manually, as Jetty loosing stack traces.
            U.error(log, "Unhandled runtime exception in HTTP router.", e);

            throw e;
        }
        finally {
            // Mark response as handled (required by Jetty API).
            req.setHandled(true);

            reqCnt++;
        }
    }

    /**
     * @param destId Id of the node where request should be forwarded, may be null.
     * @return List of addresses where the given request could be forwarded.
     * @throws GridClientException If node not found.
     */
    private Collection<InetSocketAddress> getEndpoints(@Nullable UUID destId) throws GridClientException {
        if (destId != null) {
            GridClientNode node = client.compute().node(destId);

            return node != null ?
                node.availableAddresses(GridClientProtocol.HTTP) : Collections.<InetSocketAddress>emptyList();
        }
        else {
            return F.flatCollections(
                F.transform(client.compute().nodes(), new GridClosure<GridClientNode, Collection<InetSocketAddress>>() {
                    @Override public Collection<InetSocketAddress> apply(GridClientNode e) {
                        return e.availableAddresses(GridClientProtocol.HTTP);
                    }
                }));
        }
    }

    /**
     * Extracts request parameters as a list of key-value pairs.
     *
     * @param req Request.
     * @return List of parameters as key value pairs.
     */
    private Iterable<NameValuePair> extractRequestParameters(Request req) {
        MultiMap<String> paramsMap = req.getParameters();
        Collection<NameValuePair> params = new ArrayList<>(paramsMap.size());

        for (Map.Entry e : paramsMap.entrySet())
            params.add(new BasicNameValuePair((String)e.getKey(), ((List<String>)e.getValue()).get(0)));

        return params;
    }

    /**
     * Performs HTTP request forwarding.
     *
     * @param postParams POST params.
     * @param from Address to forward.
     * @param to Response
     * @throws IOException If forwarding failed.
     */
    private void forward(Iterable<NameValuePair> postParams, HttpServletResponse from,
        InetSocketAddress to) throws IOException {
        String uri = "grid://" + to.getHostName() + ':' + to.getPort() + "/gridgain";

        HttpPost post = new HttpPost(uri);

        post.setEntity(new UrlEncodedFormEntity(postParams));

        HttpResponse res = httpClient.execute(post);

        HttpEntity entity = res.getEntity();

        from.setContentType(entity.getContentType().getValue());
        from.setCharacterEncoding("UTF-8");
        from.setStatus(res.getStatusLine().getStatusCode());

        InputStreamReader reader = new InputStreamReader(entity.getContent(), CharsetUtil.UTF_8);

        U.copy(reader, from.getWriter());

        from.flushBuffer();
    }

    /**
     * Sends failure response, with the given message.
     *
     * @param res Servlet response.
     * @param failMsg Failure message.
     * @throws IOException In case of IO exception.
     */
    private static void writeFailure(ServletResponse res, String failMsg) throws IOException {
        res.setContentType("application/json");
        res.setCharacterEncoding("UTF-8");

        res.getWriter().write(JSONSerializer.toJSON(new GridRestResponse(STATUS_FAILED, failMsg)).toString());
    }

    /**
     * @return Number of server requests.
     */
    public long getRequestsCount() {
        return reqCnt;
    }

    /**
     * Empty cookie store.
     */
    private static final class EmptyCookieStore implements CookieStore {
        /** {@inheritDoc} */
        @Override public void addCookie(Cookie cookie) {
            // Don't store anything.
        }

        /** {@inheritDoc} */
        @Override public List<Cookie> getCookies() {
            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public boolean clearExpired(Date date) {
            // Don't do anything.
            return false;
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            // Don't store anything.
        }
    }
}

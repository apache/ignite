/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.protocols.http.jetty;

import net.sf.json.*;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.rest.GridRestResponse.STATUS_FAILED;

/**
 * Jetty REST handler. The following URL format is supported:
 * {@code /gridgain?cmd=cmdName&param1=abc&param2=123}
 *
 * @author @java.author
 * @version @java.version
 */
public class GridJettyRestHandler extends AbstractHandler {
    /** Logger. */
    private final GridLogger log;

    /** Request handlers. */
    private GridRestProtocolHandler hnd;

    /** Default page. */
    private volatile String dfltPage;

    /** Favicon. */
    private volatile byte[] favicon;

    /** Authentication checker. */
    private final GridClosure<String, Boolean> authChecker;

    /**
     * Creates new HTTP requests handler.
     *
     * @param hnd Handler.
     * @param authChecker Authentication checking closure.
     * @param log Logger.
     */
    GridJettyRestHandler(GridRestProtocolHandler hnd, GridClosure<String, Boolean> authChecker, GridLogger log) {
        assert hnd != null;
        assert log != null;

        this.hnd = hnd;
        this.log = log;
        this.authChecker = authChecker;

        // Init default page and favicon.
        try {
            initDefaultPage();

            if (log.isDebugEnabled())
                log.debug("Initialized default page.");
        }
        catch (IOException e) {
            U.warn(log, "Failed to initialize default page: " + e.getMessage());
        }

        try {
            initFavicon();

            if (log.isDebugEnabled())
                log.debug(favicon != null ? "Initialized favicon, size: " + favicon.length : "Favicon is null.");
        }
        catch (IOException e) {
            U.warn(log, "Failed to initialize favicon: " + e.getMessage());
        }
    }

    /**
     * @throws IOException If failed.
     */
    private void initDefaultPage() throws IOException {
        assert dfltPage == null;

        InputStream in = getClass().getResourceAsStream("rest.html");

        if (in != null) {
            LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in));

            try {
                StringBuilder buf = new StringBuilder(2048);

                for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                    buf.append(line);

                    if (!line.endsWith(" "))
                        buf.append(" ");
                }

                dfltPage = buf.toString();
            }
            finally {
                U.closeQuiet(rdr);
            }
        }
    }

    /**
     * @throws IOException If failed.
     */
    private void initFavicon() throws IOException {
        assert favicon == null;

        InputStream in = getClass().getResourceAsStream("favicon.ico");

        if (in != null) {
            BufferedInputStream bis = new BufferedInputStream(in);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            try {
                byte[] buf = new byte[2048];

                while (true) {
                    int n = bis.read(buf);

                    if (n == -1)
                        break;

                    bos.write(buf, 0, n);
                }

                favicon = bos.toByteArray();
            }
            finally {
                U.closeQuiet(bis);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void handle(String target, Request req, HttpServletRequest srvReq, HttpServletResponse res)
        throws IOException, ServletException {
        if (log.isDebugEnabled())
            log.debug("Handling request [target=" + target + ", req=" + req + ", srvReq=" + srvReq + ']');

        if (target.startsWith("/gridgain")) {
            processRequest(target, srvReq, res);

            req.setHandled(true);
        }
        else if (target.startsWith("/favicon.ico")) {
            if (favicon == null) {
                res.setStatus(HttpServletResponse.SC_NOT_FOUND);

                req.setHandled(true);

                return;
            }

            res.setStatus(HttpServletResponse.SC_OK);

            res.setContentType("image/x-icon");

            res.getOutputStream().write(favicon);
            res.getOutputStream().flush();

            req.setHandled(true);
        }
        else {
            if (dfltPage == null) {
                res.setStatus(HttpServletResponse.SC_NOT_FOUND);

                req.setHandled(true);

                return;
            }

            res.setStatus(HttpServletResponse.SC_OK);

            res.setContentType("text/html");

            res.getWriter().write(dfltPage);
            res.getWriter().flush();

            req.setHandled(true);
        }
    }

    /**
     * Process HTTP request.
     *
     * @param act Action.
     * @param req Http request.
     * @param res Http response.
     */
    private void processRequest(String act, HttpServletRequest req, HttpServletResponse res) {
        res.setContentType("application/json");
        res.setCharacterEncoding("UTF-8");

        GridRestCommand cmd = command(req);

        if (cmd == null) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);

            return;
        }

        if (!authChecker.apply(req.getHeader("X-Signature"))) {
            res.setStatus(HttpServletResponse.SC_UNAUTHORIZED);

            return;
        }

        GridRestRequest cmdReq = new GridRestRequest(cmd, act, parameters(req));

        String clientId = cmdReq.parameter("clientId");

        try {
            if (clientId != null)
                cmdReq.setClientId(UUID.fromString(clientId));
        }
        catch (Exception ignored) {
            // Ignore invalid client id. Rest handler will process this logic.
        }

        String destId = cmdReq.parameter("destId");

        try {
            if (destId != null)
                cmdReq.setDestId(UUID.fromString(destId));
        }
        catch (IllegalArgumentException ignored) {
            // Don't fail - try to execute locally.
        }

        cmdReq.setCredentials(cmdReq.parameter("cred"));

        try {
            String sesTokStr = cmdReq.parameter("sessionToken");

            if (sesTokStr != null)
                cmdReq.setSessionToken(U.hexString2ByteArray(sesTokStr));
        }
        catch (IllegalArgumentException ignored) {
            // Ignore invalid session token.
        }

        if (log.isDebugEnabled())
            log.debug("Initialized command request: " + cmdReq);

        GridRestResponse cmdRes;

        try {
            cmdRes = hnd.handle(cmdReq);

            if (cmdRes == null)
                throw new IllegalStateException("Received null result from handler: " + hnd);

            byte[] sesTok = cmdRes.sessionTokenBytes();

            if (sesTok != null)
                cmdRes.setSessionToken(U.byteArray2HexString(sesTok));

            res.setStatus(HttpServletResponse.SC_OK);
        }
        catch (Throwable e) {
            res.setStatus(HttpServletResponse.SC_OK);

            U.error(log, "Failed to process HTTP request [action=" + act + ", req=" + req + ']', e);

            cmdRes = new GridRestResponse(STATUS_FAILED, e.getMessage());
        }

        JsonConfig cfg = new GridJettyJsonConfig();
        JSON json;

        try {
            json = JSONSerializer.toJSON(cmdRes, cfg);
        }
        catch (JSONException e) {
            U.error(log, "Failed to convert response to JSON: " + cmdRes, e);

            json = JSONSerializer.toJSON(new GridRestResponse(STATUS_FAILED, e.getMessage()), cfg);
        }

        try {
            if (log.isDebugEnabled())
                log.debug("Parsed command response into JSON object: " + json.toString(2));

            res.getWriter().write(json.toString());

            if (log.isDebugEnabled())
                log.debug("Processed HTTP request [action=" + act + ", jsonRes=" + cmdRes + ", req=" + cmdReq + ']');
        }
        catch (IOException e) {
            U.error(log, "Failed to send HTTP response: " + json.toString(2), e);
        }
    }

    /**
     * @param req Request.
     * @return Command.
     */
    @Nullable GridRestCommand command(ServletRequest req) {
        String cmd = req.getParameter("cmd");

        return cmd == null ? null : GridRestCommand.fromKey(cmd.toLowerCase());
    }

    /**
     * Parses HTTP parameters in an appropriate format and return back map of
     * values to predefined list of names.
     *
     * @param req Request.
     * @return Map of parsed parameters.
     */
    @SuppressWarnings({"unchecked"})
    private Map<String, Object> parameters(ServletRequest req) {
        Map<String, String[]> params = req.getParameterMap();

        if (F.isEmpty(params))
            return Collections.emptyMap();

        Map<String, Object> map = new HashMap<>(params.size());

        for (Map.Entry<String, String[]> entry : params.entrySet())
            map.put(entry.getKey(), parameter(entry.getValue()));

        return map;
    }

    /**
     * @param obj Parameter object.
     * @return Parameter value.
     */
    @Nullable private String parameter(Object obj) {
        if (obj instanceof String)
            return (String)obj;
        else if (obj instanceof String[] && ((String[])obj).length > 0)
            return ((String[])obj)[0];

        return null;
    }
}

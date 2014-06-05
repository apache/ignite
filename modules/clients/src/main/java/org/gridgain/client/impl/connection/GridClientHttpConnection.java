/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl.connection;

import net.sf.json.*;
import org.gridgain.client.*;
import org.gridgain.client.impl.*;
import org.gridgain.client.util.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.security.*;
import org.jetbrains.annotations.*;

import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.logging.*;

/**
 * Java client implementation.
 */
public class GridClientHttpConnection extends GridClientConnection {
    /** Logger. */
    private static final Logger log = Logger.getLogger(GridClientHttpConnection.class.getName());

    /** Thread pool. */
    private final ExecutorService pool;

    /** Busy lock for graceful close. */
    private ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Pending requests  */
    private GridConcurrentHashSet<FutureWorker> pendingReqs = new GridConcurrentHashSet<>();

    /** Session token. */
    private String sesTok;

    /** Connection timeout. */
    private final int connTimeout;

    /** Read timeout. */
    private final int readTimeout;

    /**
     * Creates client.
     *
     * @param clientId Client identifier.
     * @param srvAddr Server address on which HTTP REST handler resides.
     * @param sslCtx SSL context to use if SSL is enabled, {@code null} otherwise.
     * @param connTimeout Connection timeout.
     * @param readTimeout Read timeout.
     * @param top Topology to use.
     * @param pool Thread pool executor.
     * @param cred Client credentials.
     * @throws IOException If input-output error occurs.
     */
    public GridClientHttpConnection(UUID clientId, InetSocketAddress srvAddr, SSLContext sslCtx, int connTimeout,
        int readTimeout, GridClientTopology top, ExecutorService pool, Object cred) throws IOException {
        super(clientId, srvAddr, sslCtx, top, cred);

        this.connTimeout = connTimeout;
        this.readTimeout = readTimeout;

        Socket sock = new Socket();

        try {
            sock.connect(srvAddr, connTimeout);
        }
        finally {
            GridClientUtils.closeQuiet(sock);
        }

        if (log.isLoggable(Level.INFO))
            log.info("Client HTTP connection established: " + serverAddress());

        this.pool = pool;
    }

    /** {@inheritDoc} */
    @Override void close(GridClientConnectionCloseReason reason, boolean waitCompletion) {
        busyLock.writeLock().lock();

        try {
            if (closeReason != null)
                return;

            closeReason = reason;
        }
        finally {
            busyLock.writeLock().unlock();
        }

        if (waitCompletion) {
            Iterator<FutureWorker> tasks = pendingReqs.iterator();

            try {
                while (tasks.hasNext()) {
                    FutureWorker worker = tasks.next();

                    worker.awaitCompletion();

                    tasks.remove();
                }
            }
            catch (InterruptedException ignored) {
                log.warning("Interrupted while waiting for all pending requests to complete (will cancel remaining " +
                    "requests): " + serverAddress());

                Thread.currentThread().interrupt();
            }
        }

        if (log.isLoggable(Level.FINE))
            log.fine("Cancelling " + pendingReqs.size() + " pending requests: " + serverAddress());

        Iterator<FutureWorker> tasks = pendingReqs.iterator();

        while (tasks.hasNext()) {
            FutureWorker worker = tasks.next();

            worker.cancel();

            tasks.remove();
        }

        if (log.isLoggable(Level.INFO))
            log.info("Client HTTP connection closed: " + serverAddress());
    }

    /** {@inheritDoc} */
    @Override boolean closeIfIdle(long idleTimeout) {
        return false;
    }

    /**
     * Creates new future and passes it to the makeJettyRequest.
     *
     * @param params Request parameters.
     * @param flags Cache flags to be enabled.
     * @param destNodeId Destination node ID.
     * @return Future.
     * @throws GridClientClosedException If client was manually closed.
     * @throws GridClientConnectionResetException If connection could not be established.
     */
    private <R> GridClientFutureAdapter<R> makeJettyRequest(Map<String, Object> params,
        Collection<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientClosedException, GridClientConnectionResetException {
        int flagsBitMap = encodeCacheFlags(flags);

        if (flagsBitMap != 0)
            params.put("cacheFlags", Integer.toString(flagsBitMap));

        return makeJettyRequest(params, destNodeId);
    }

    /**
     * Makes request to Jetty server.
     *
     * @param params Parameters map.
     * @param destNodeId Destination node ID.
     * @return Response.
     * @throws GridClientConnectionResetException In connection to the server can not be established.
     * @throws GridClientClosedException If connection was closed manually.
     */
    @SuppressWarnings("unchecked")
    private <R> GridClientFutureAdapter<R> makeJettyRequest(final Map<String, Object> params, final UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert params != null;
        assert params.containsKey("cmd");

        busyLock.readLock().lock();

        try {
            checkClosed(closeReason);

            try {
                final GridClientFutureAdapter<R> fut = new GridClientFutureAdapter<>();

                final URLConnection urlConn = openConnection(params, destNodeId);

                FutureWorker<R> worker = new FutureWorker<R>(fut) {
                    @Override protected void body() throws Exception {
                        try {
                            InputStream input = urlConn.getInputStream();

                            // Read reply and close input stream.
                            JSONObject json = readReply(input);

                            int okStatus = json.getInt("successStatus");

                            if (okStatus == GridClientResponse.STATUS_AUTH_FAILURE) {
                                sesTok = null;

                                InputStream inputAuth = openConnection(params, destNodeId).getInputStream();

                                // Read reply and close input stream.
                                json = readReply(inputAuth);
                            }

                            if (json.getString("sessionToken") != null)
                                sesTok = json.getString("sessionToken");

                            okStatus = json.getInt("successStatus");

                            String errMsg = (String)json.get("error");

                            if (okStatus == GridClientResponse.STATUS_AUTH_FAILURE) {
                                sesTok = null;

                                fut.onDone(new GridClientAuthenticationException("Client authentication failed " +
                                    "[clientId=" + clientId + ", srvAddr=" + serverAddress() + ", errMsg=" + errMsg +
                                    ']'));
                            }
                            else if (okStatus == GridClientResponse.STATUS_FAILED) {
                                if (errMsg == null || errMsg.isEmpty())
                                    errMsg = "Unknown server error.";

                                fut.onDone(new GridClientException(errMsg));
                            }
                            else if (okStatus == GridClientResponse.STATUS_SECURITY_CHECK_FAILED) {
                                if (errMsg == null || errMsg.isEmpty())
                                    errMsg = "Security check failed on server.";

                                fut.onDone(new GridClientException(errMsg));
                            }
                            else if (okStatus != GridClientResponse.STATUS_SUCCESS) {
                                fut.onDone(new GridClientException("Unsupported server response status code" +
                                    ": " + okStatus));
                            }
                            else {
                                Object res = json.get("response");

                                if (JSONNull.getInstance().equals(res))
                                    res = null;

                                fut.onDone((R)res);
                            }
                        }
                        catch (IOException e) {
                            fut.onDone(new GridClientConnectionResetException(
                                "Failed to perform request (connection failed): " + serverAddress(), e));
                        }
                        catch (Throwable e) {
                            fut.onDone(new GridClientException(
                                "Failed to perform request: " + serverAddress(), e));
                        }
                        finally {
                            pendingReqs.remove(this);
                        }
                    }

                    @Override protected void cancelBody() {
                        fut.onDone(new GridClientClosedException("Failed to perform request (connection was closed" +
                            " before response was received): " + serverAddress()));
                    }
                };

                pendingReqs.add(worker);

                pool.execute(worker);

                return fut;
            }
            catch (RejectedExecutionException ignored) {
                throw new GridClientClosedException(
                    "Client was closed (no public methods of client can be used anymore).");
            }
            catch (IOException e) {
                throw new GridClientConnectionResetException("Failed to read response from remote server: " +
                    serverAddress(), e);
            }
        }
        finally {
            busyLock.readLock().unlock();
        }
    }

    /**
     * Opens url-connection with specified request parameters.
     *
     * @param params Request parameters.
     * @param destId Destination ID, if {@code null} - no destination.
     * @return New opened connection.
     * @throws IOException If connection could not be established.
     */
    private URLConnection openConnection(Map<String, Object> params, @Nullable UUID destId) throws IOException {
        Map<String, Object> hdr = new HashMap<>();
        Map<String, Object> body = new HashMap<>(params);

        hdr.put("clientId", clientId.toString());

        if (destId != null)
            hdr.put("destId", destId.toString());

        if (sesTok != null)
            hdr.put("sessionToken", sesTok);
        else if (credentials() != null)
            body.put("cred", credentials());

        // Build request uri.
        String uri = (sslContext() == null ? "http://" : "https://") +
            serverAddress().getHostName() + ':' + serverAddress().getPort() +
            "/gridgain?" + encodeParams(hdr);

        HttpURLConnection conn = (HttpURLConnection)new URL(uri).openConnection();

        conn.setConnectTimeout(connTimeout);
        conn.setReadTimeout(readTimeout);

        if (sslContext() != null) {
            ((HttpsURLConnection)conn).setSSLSocketFactory(sslContext().getSocketFactory());

            // Work-around for SSL connections limit in 32 open persistent connections.
            conn.setRequestProperty("Connection", "Close");
        }

        // Provide POST body.
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Accept-Charset", "UTF-8");
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");

        OutputStream output = null;

        try {
            // Initiate connection request.
            output = conn.getOutputStream();

            output.write(encodeParams(body).toString().getBytes("UTF-8"));
            output.flush();
        }
        finally {
            GridClientUtils.closeQuiet(output);
        }

        // Initiate connection response.
        return conn;
    }

    /**
     * Encode parameters map into "application/x-www-form-urlencoded" format.
     *
     * @param params Parameters map to encode.
     * @return Form-encoded parameters.
     * @throws IOException In case of error.
     */
    private StringBuilder encodeParams(Map<String, Object> params) throws IOException {
        StringBuilder data = new StringBuilder();

        for (Map.Entry<String, Object> e : params.entrySet()) {
            if (e.getValue() instanceof String) {
                if (data.length() > 0)
                    data.append('&');

                data.append(URLEncoder.encode(e.getKey(), "UTF-8")).append('=')
                    .append(URLEncoder.encode((String)e.getValue(), "UTF-8"));
            }
            else if (e.getValue() instanceof GridSecurityCredentials) {
                GridSecurityCredentials cred = (GridSecurityCredentials)e.getValue();

                if (data.length() > 0)
                    data.append('&');

                Object login = cred.getLogin();

                if (login != null) {
                    if (!(login instanceof String))
                        throw new IllegalArgumentException("Http connection supports only string login" +
                            ", while received: " + login);

                    data.append("gridgain.login=").append(URLEncoder.encode((String)login, "UTF-8"));
                }

                if (data.length() > 0)
                    data.append('&');

                Object pwd = cred.getPassword();

                if (pwd != null) {
                    if (!(pwd instanceof String))
                        throw new IllegalArgumentException("Http connection supports only string password" +
                            ", while received: " + login);

                    data.append("gridgain.password=").append(URLEncoder.encode((String)pwd, "UTF-8"));
                }
            }
            else
                throw new IllegalArgumentException("Http connection supports only string arguments in requests" +
                    ", while received [key=" + e.getKey() + ", value=" + e.getValue() + "]");
        }

        return data;
    }

    /**
     * Reads input stream contents, parses JSON object and closes input stream.
     *
     * @param input Input stream to read from.
     * @return JSON object parsed from input stream.
     * @throws IOException If input read failed.
     */
    private JSONObject readReply(InputStream input) throws IOException {
        return JSONObject.fromObject(readRawReply(input));
    }

    /**
     * Reads input stream content as a string and closes input stream.
     *
     * @param input Input to read from.
     * @return Content of the stream.
     * @throws IOException If input read failed.
     */
    private String readRawReply(InputStream input) throws IOException {
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(input));

            StringBuilder buf = new StringBuilder();

            String line;

            while ((line = reader.readLine()) != null)
                buf.append(line);

            return buf.toString();
        }
        finally {
            input.close();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Boolean> cachePutAll(String cacheName, Map<K, V> entries,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert entries != null;

        Map<String, Object> params = new HashMap<>();

        params.put("cmd", "putall");

        if (cacheName != null)
            params.put("cacheName", cacheName);

        int i = 1;

        for (Map.Entry<K, V> e : entries.entrySet()) {
            params.put("k" + i, e.getKey());
            params.put("v" + i, e.getValue());

            i++;
        }

        return makeJettyRequest(params, flags, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<V> cacheGet(String cacheName, K key, Set<GridClientCacheFlag> flags,
        UUID destNodeId) throws GridClientConnectionResetException, GridClientClosedException {
        return makeCacheRequest("get", cacheName, key, null, flags, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Boolean> cachePut(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        return makeCacheRequest("put", cacheName, key, val, flags, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Map<K, V>> cacheGetAll(String cacheName, Collection<K> keys,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert keys != null;

        Map<String, Object> params = new HashMap<>();

        params.put("cmd", "getall");

        if (cacheName != null)
            params.put("cacheName", cacheName);

        int i = 1;

        for (K key : keys) {
            params.put("k" + i, key);

            i++;
        }

        return makeJettyRequest(params, flags, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFutureAdapter<Boolean> cacheRemove(final String cacheName, final K key,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        return makeCacheRequest("rmv", cacheName, key, null, flags, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFutureAdapter<Boolean> cacheRemoveAll(String cacheName, Collection<K> keys,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientClosedException, GridClientConnectionResetException {
        assert keys != null;

        Map<String, Object> params = new HashMap<>();

        params.put("cmd", "rmvall");

        if (cacheName != null)
            params.put("cacheName", cacheName);

        int i = 1;

        for (K key : keys) {
            params.put("k" + i, key);

            i++;
        }

        return makeJettyRequest(params, flags, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Boolean> cacheReplace(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientClosedException, GridClientConnectionResetException {
        return makeCacheRequest("rep", cacheName, key, val, flags, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Boolean> cacheCompareAndSet(String cacheName, K key, V newVal,
        V oldVal, Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert key != null;

        Map<String, Object> params = new HashMap<>();

        params.put("cmd", "cas");
        params.put("key", key);

        if (newVal != null)
            params.put("val1", newVal);

        if (oldVal != null)
            params.put("val2", oldVal);

        if (cacheName != null)
            params.put("cacheName", cacheName);

        return makeJettyRequest(params, flags, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFutureAdapter<GridClientDataMetrics> cacheMetrics(final String cacheName,
        UUID destNodeId) throws GridClientClosedException, GridClientConnectionResetException {
        Map<String, Object> params = new HashMap<>();

        params.put("cmd", "cache");

        if (cacheName != null)
            params.put("cacheName", cacheName);

        return makeJettyRequest(params, destNodeId)
            .chain(new GridClientFutureCallback<Object, GridClientDataMetrics>() {
                @Override public GridClientDataMetrics onComplete(GridClientFuture fut) throws GridClientException {
                    return metricsMapToMetrics((Map<String, Number>)fut.get());
                }
            });
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Boolean> cacheAppend(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        return makeCacheRequest("append", cacheName, key, val, flags, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFutureAdapter<Boolean> cachePrepend(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientConnectionResetException, GridClientClosedException {
        return makeCacheRequest("prepend", cacheName, key, val, flags, destNodeId);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <R> GridClientFutureAdapter<R> execute(String taskName, Object taskArg, UUID destNodeId)
        throws GridClientClosedException, GridClientConnectionResetException {
        assert taskName != null;

        Map<String, Object> paramsMap = new HashMap<>();

        paramsMap.put("cmd", "exe");
        paramsMap.put("name", taskName);

        if (taskArg != null)
            paramsMap.put("p1", taskArg);

        return makeJettyRequest(paramsMap, destNodeId).chain(new GridClientFutureCallback<Object, R>() {
            @Override public R onComplete(GridClientFuture fut) throws GridClientException {
                return (R)((JSONObject)fut.get()).get("result");
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<GridClientNode> node(final UUID id, final boolean inclAttrs,
        final boolean inclMetrics, UUID destNodeId)
        throws GridClientClosedException, GridClientConnectionResetException {
        assert id != null;

        Map<String, Object> params = new HashMap<>();

        params.put("cmd", "node");
        params.put("id", id.toString());
        params.put("attr", String.valueOf(inclAttrs));
        params.put("mtr", String.valueOf(inclMetrics));

        return makeJettyRequest(params, destNodeId).chain(new GridClientFutureCallback<Object, GridClientNode>() {
            @Override public GridClientNode onComplete(GridClientFuture fut) throws GridClientException {
                GridClientNodeImpl node = jsonBeanToNode(fut.get());

                if (node != null)
                    top.updateNode(node);

                return node;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<GridClientNode> node(final String ip, final boolean inclAttrs,
        final boolean includeMetrics, UUID destNodeId)
        throws GridClientClosedException, GridClientConnectionResetException {
        assert ip != null;

        Map<String, Object> params = new HashMap<>();

        params.put("cmd", "node");
        params.put("ip", ip);
        params.put("attr", String.valueOf(inclAttrs));
        params.put("mtr", String.valueOf(includeMetrics));

        return makeJettyRequest(params, destNodeId).chain(new GridClientFutureCallback<Object, GridClientNode>() {
            @Override public GridClientNode onComplete(GridClientFuture<Object> fut) throws GridClientException {
                GridClientNodeImpl node = jsonBeanToNode(fut.get());

                return node != null ? top.updateNode(node) : null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<List<GridClientNode>> topology(final boolean inclAttrs,
        final boolean inclMetrics, UUID destNodeId)
        throws GridClientClosedException, GridClientConnectionResetException {
        Map<String, Object> params = new HashMap<>();

        params.put("cmd", "top");
        params.put("attr", String.valueOf(inclAttrs));
        params.put("mtr", String.valueOf(inclMetrics));

        return makeJettyRequest(params, destNodeId).chain(new GridClientFutureCallback<Object, List<GridClientNode>>() {
            @Override public List<GridClientNode> onComplete(GridClientFuture fut) throws GridClientException {
                Object res = fut.get();

                assert res instanceof JSONArray : "Did not receive a JSON array [cls=" + res.getClass() + ", " +
                    "res=" + res + ']';

                JSONArray arr = (JSONArray)res;

                Collection<GridClientNodeImpl> nodeList = new ArrayList<>(arr.size());

                for (Object o : arr)
                    nodeList.add(jsonBeanToNode(o));

                return (List<GridClientNode>)(List)top.updateTopology(nodeList);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<List<String>> log(final String path, final int fromLine, final int toLine,
        UUID destNodeId) throws GridClientClosedException, GridClientConnectionResetException {
        Map<String, Object> params = new HashMap<>();

        params.put("cmd", "log");

        if (path != null)
            params.put("path", path);

        params.put("from", String.valueOf(fromLine));
        params.put("to", String.valueOf(toLine));

        return makeJettyRequest(params, destNodeId).chain(new GridClientFutureCallback<Object, List<String>>() {
            @Override public List<String> onComplete(GridClientFuture fut) throws GridClientException {
                Object res = fut.get();

                if (res == null || res instanceof JSONNull)
                    return null;

                assert res instanceof JSONArray : "Did not receive a JSON array [cls=" + res.getClass() + ", " +
                    "res=" + res + ']';

                JSONArray arr = (JSONArray)res;

                List<String> list = new ArrayList<>(arr.size());

                for (Object o : arr)
                    list.add((String)o);

                return list;
            }
        });
    }

    /**
     * Creates new future and passes it to the makeJettyRequest.
     *
     * @param cmd Cache command.
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @param flags Cache flags to be enabled.
     * @param destNodeId Destination node ID.
     * @return Future.
     * @throws GridClientClosedException If client was manually closed.
     * @throws GridClientConnectionResetException If connection could not be established.
     */
    private <K, V, R> GridClientFutureAdapter<R> makeCacheRequest(String cmd, String cacheName, K key, @Nullable V val,
        Collection<GridClientCacheFlag> flags, UUID destNodeId)
        throws GridClientClosedException, GridClientConnectionResetException {
        assert cmd != null;
        assert key != null;

        Map<String, Object> params = new HashMap<>();

        params.put("cmd", cmd);
        params.put("key", key);

        if (val != null)
            params.put("val", val);

        if (cacheName != null)
            params.put("cacheName", cacheName);

        return makeJettyRequest(params, flags, destNodeId);
    }

    /** {@inheritDoc} */
    @Override public GridClientFutureAdapter<?> forwardMessage(Object body) throws GridClientException {
        throw new UnsupportedOperationException("Forward message is not implemented for HTTP connection.");
    }

    /**
     * Creates client node impl from json object representation.
     *
     * @param obj JSONObject (possibly JSONNull).
     * @return Converted client node.
     */
    @Nullable private GridClientNodeImpl jsonBeanToNode(Object obj) {
        if (!(obj instanceof JSONObject))
            return null;

        JSONObject nodeBean = (JSONObject)obj;

        GridClientNodeImpl.Builder nodeBuilder = GridClientNodeImpl.builder()
            .nodeId(UUID.fromString((String)nodeBean.get("nodeId")))
            .consistentId(nodeBean.get("consistentId"))
            .tcpAddresses((Collection<String>)nodeBean.get("tcpAddresses"))
            .tcpHostNames((Collection<String>)nodeBean.get("tcpHostNames"))
            .jettyAddresses((Collection<String>)nodeBean.get("jettyAddresses"))
            .jettyHostNames((Collection<String>)nodeBean.get("jettyHostNames"))
            .tcpPort((Integer)nodeBean.get("tcpPort"))
            .httpPort((Integer)nodeBean.get("jettyPort"))
            .replicaCount((Integer)nodeBean.get("replicaCount"));

        Map<String, GridClientCacheMode> caches = new HashMap<>();

        if (nodeBean.get("caches") instanceof JSONObject) {
            Map<String, String> rawCaches = (Map<String, String>)nodeBean.get("caches");

            for (Map.Entry<String, String> e : rawCaches.entrySet())
                try {
                    caches.put(e.getKey(), GridClientCacheMode.valueOf(e.getValue()));
                }
                catch (IllegalArgumentException ignored) {
                    log.warning("Invalid cache mode received from remote node (will ignore) [srv=" + serverAddress() +
                        ", cacheName=" + e.getKey() + ", cacheMode=" + e.getValue() + ']');
                }

            Object dfltCacheMode = nodeBean.get("defaultCacheMode");

            if (dfltCacheMode instanceof String && !((String)dfltCacheMode).isEmpty())
                try {
                    caches.put(null, GridClientCacheMode.valueOf((String)dfltCacheMode));
                }
                catch (IllegalArgumentException ignored) {
                    log.warning("Invalid cache mode received for default cache from remote node (will ignore) [srv="
                        + serverAddress() + ", cacheMode=" + dfltCacheMode + ']');
                }

            nodeBuilder.caches(caches);
        }

        Object attrs = nodeBean.get("attributes");

        if (attrs != null && !(attrs instanceof JSONNull))
            nodeBuilder.attributes((Map<String, Object>)attrs);

        Object metrics = nodeBean.get("metrics");

        if (metrics != null && !(metrics instanceof JSONNull)) {
            Map<String, Number> metricsMap = (Map<String, Number>)metrics;

            GridClientNodeMetricsAdapter metricsAdapter = new GridClientNodeMetricsAdapter();

            metricsAdapter.setStartTime(safeLong(metricsMap, "startTime"));
            metricsAdapter.setAverageActiveJobs((float)safeDouble(metricsMap, "averageActiveJobs"));
            metricsAdapter.setAverageCancelledJobs((float)safeDouble(metricsMap, "averageCancelledJobs"));
            metricsAdapter.setAverageCpuLoad(safeDouble(metricsMap, "averageCpuLoad"));
            metricsAdapter.setAverageJobExecuteTime(safeDouble(metricsMap, "averageJobExecuteTime"));
            metricsAdapter.setAverageJobWaitTime(safeDouble(metricsMap, "averageJobWaitTime"));
            metricsAdapter.setAverageRejectedJobs((float)safeDouble(metricsMap, "averageRejectedJobs"));
            metricsAdapter.setAverageWaitingJobs((float)safeDouble(metricsMap, "averageWaitingJobs"));
            metricsAdapter.setCurrentActiveJobs((int)safeLong(metricsMap, "currentActiveJobs"));
            metricsAdapter.setCurrentCancelledJobs((int)safeLong(metricsMap, "currentCancelledJobs"));
            metricsAdapter.setCurrentCpuLoad(safeDouble(metricsMap, "currentCpuLoad"));
            metricsAdapter.setCurrentGcCpuLoad(safeDouble(metricsMap, "currentGcCpuLoad"));
            metricsAdapter.setCurrentDaemonThreadCount((int)safeLong(metricsMap, "currentDaemonThreadCount"));
            metricsAdapter.setCurrentIdleTime(safeLong(metricsMap, "currentIdleTime"));
            metricsAdapter.setCurrentJobExecuteTime(safeLong(metricsMap, "currentJobExecuteTime"));
            metricsAdapter.setCurrentJobWaitTime(safeLong(metricsMap, "currentJobWaitTime"));
            metricsAdapter.setCurrentRejectedJobs((int)safeLong(metricsMap, "currentRejectedJobs"));
            metricsAdapter.setCurrentThreadCount((int)safeLong(metricsMap, "currentThreadCount"));
            metricsAdapter.setCurrentWaitingJobs((int)safeLong(metricsMap, "currentWaitingJobs"));
            metricsAdapter.setFileSystemFreeSpace(safeLong(metricsMap, "fileSystemFreeSpace"));
            metricsAdapter.setFileSystemTotalSpace(safeLong(metricsMap, "fileSystemTotalSpace"));
            metricsAdapter.setFileSystemUsableSpace(safeLong(metricsMap, "fileSystemUsableSpace"));
            metricsAdapter.setHeapMemoryCommitted(safeLong(metricsMap, "heapMemoryCommitted"));
            metricsAdapter.setHeapMemoryInitialized(safeLong(metricsMap, "heapMemoryInitialized"));
            metricsAdapter.setHeapMemoryMaximum(safeLong(metricsMap, "heapMemoryMaximum"));
            metricsAdapter.setHeapMemoryUsed(safeLong(metricsMap, "heapMemoryUsed"));
            metricsAdapter.setLastDataVersion(safeLong(metricsMap, "lastDataVersion"));
            metricsAdapter.setLastUpdateTime(safeLong(metricsMap, "lastUpdateTime"));
            metricsAdapter.setMaximumActiveJobs((int)safeLong(metricsMap, "maximumActiveJobs"));
            metricsAdapter.setMaximumCancelledJobs((int)safeLong(metricsMap, "maximumCancelledJobs"));
            metricsAdapter.setMaximumJobExecuteTime(safeLong(metricsMap, "maximumJobExecuteTime"));
            metricsAdapter.setMaximumJobWaitTime(safeLong(metricsMap, "maximumJobWaitTime"));
            metricsAdapter.setMaximumRejectedJobs((int)safeLong(metricsMap, "maximumRejectedJobs"));
            metricsAdapter.setMaximumThreadCount((int)safeLong(metricsMap, "maximumThreadCount"));
            metricsAdapter.setMaximumWaitingJobs((int)safeLong(metricsMap, "maximumWaitingJobs"));
            metricsAdapter.setNodeStartTime(safeLong(metricsMap, "nodeStartTime"));
            metricsAdapter.setNonHeapMemoryCommitted(safeLong(metricsMap, "nonHeapMemoryCommitted"));
            metricsAdapter.setNonHeapMemoryInitialized(safeLong(metricsMap, "nonHeapMemoryInitialized"));
            metricsAdapter.setNonHeapMemoryMaximum(safeLong(metricsMap, "nonHeapMemoryMaximum"));
            metricsAdapter.setNonHeapMemoryUsed(safeLong(metricsMap, "nonHeapMemoryUsed"));
            metricsAdapter.setStartTime(safeLong(metricsMap, "startTime"));
            metricsAdapter.setTotalCancelledJobs((int)safeLong(metricsMap, "totalCancelledJobs"));
            metricsAdapter.setTotalCpus((int)safeLong(metricsMap, "totalCpus"));
            metricsAdapter.setTotalExecutedJobs((int)safeLong(metricsMap, "totalExecutedJobs"));
            metricsAdapter.setTotalIdleTime(safeLong(metricsMap, "totalIdleTime"));
            metricsAdapter.setTotalRejectedJobs((int)safeLong(metricsMap, "totalRejectedJobs"));
            metricsAdapter.setTotalStartedThreadCount(safeLong(metricsMap, "totalStartedThreadCount"));
            metricsAdapter.setSentMessagesCount((int)safeLong(metricsMap, "sentMessagesCount"));
            metricsAdapter.setSentBytesCount(safeLong(metricsMap, "sentBytesCount"));
            metricsAdapter.setReceivedMessagesCount((int)safeLong(metricsMap, "receivedMessagesCount"));
            metricsAdapter.setReceivedBytesCount(safeLong(metricsMap, "receivedBytesCount"));
            metricsAdapter.setUpTime(safeLong(metricsMap, "upTime"));

            nodeBuilder.metrics(metricsAdapter);
        }

        return nodeBuilder.build();
    }

    /**
     * Worker for processing future responses.
     */
    private abstract static class FutureWorker<T> implements Runnable {
        /** Pending future. */
        protected final GridClientFutureAdapter<T> fut;

        /** Completion latch. */
        private final CountDownLatch latch = new CountDownLatch(1);

        /**
         * Creates worker.
         *
         * @param fut Future that is being processed.
         */
        protected FutureWorker(GridClientFutureAdapter<T> fut) {
            this.fut = fut;
        }

        /** */
        @Override public void run() {
            try {
                body();
            }
            catch (Throwable e) {
                fut.onDone(e);
            }
            finally {
                latch.countDown();
            }
        }

        /**
         * Cancels worker and counts down the completion latch.
         */
        protected void cancel() {
            try {
                cancelBody();
            }
            finally {
                latch.countDown();
            }
        }

        /**
         * Wait for this future to complete or get cancelled.
         *
         * @throws InterruptedException If waiting thread was interrupted.
         */
        private void awaitCompletion() throws InterruptedException {
            latch.await();
        }

        /**
         * Future processing body.
         *
         * @throws Exception If any error occurs.
         */
        protected abstract void body() throws Exception;

        /**
         * Cancels this worker. This method will be invoked if executor was stopped and
         * this worker was not run.
         */
        protected abstract void cancelBody();
    }
}

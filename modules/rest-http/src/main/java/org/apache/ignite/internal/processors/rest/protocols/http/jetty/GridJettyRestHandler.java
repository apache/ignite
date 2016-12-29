/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.request.DataStructuresRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestLogRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestTaskRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestTopologyRequest;
import org.apache.ignite.internal.processors.rest.request.RestQueryRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_CONTAINS_KEYS;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.EXECUTE_SQL_QUERY;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;

/**
 * Jetty REST handler. The following URL format is supported: {@code /ignite?cmd=cmdName&param1=abc&param2=123}
 */
public class GridJettyRestHandler extends AbstractHandler {
    /** Used to sent request charset. */
    private static final String CHARSET = StandardCharsets.UTF_8.name();

    /** Logger. */
    private final IgniteLogger log;

    /** Authentication checker. */
    private final IgniteClosure<String, Boolean> authChecker;

    /** Request handlers. */
    private GridRestProtocolHandler hnd;

    /** Default page. */
    private volatile String dfltPage;

    /** Favicon. */
    private volatile byte[] favicon;

    /** Mapper from Java object to JSON. */
    private final ObjectMapper jsonMapper;

    /**
     * Creates new HTTP requests handler.
     *
     * @param hnd Handler.
     * @param authChecker Authentication checking closure.
     * @param log Logger.
     */
    GridJettyRestHandler(GridRestProtocolHandler hnd, IgniteClosure<String, Boolean> authChecker, IgniteLogger log) {
        assert hnd != null;
        assert log != null;

        this.hnd = hnd;
        this.log = log;
        this.authChecker = authChecker;
        this.jsonMapper = new GridJettyObjectMapper();

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
     * Retrieves long value from parameters map.
     *
     * @param key Key.
     * @param params Parameters map.
     * @param dfltVal Default value.
     * @return Long value from parameters map or {@code dfltVal} if null or not exists.
     * @throws IgniteCheckedException If parsing failed.
     */
    @Nullable private static Long longValue(String key, Map<String, Object> params,
        Long dfltVal) throws IgniteCheckedException {
        assert key != null;

        String val = (String)params.get(key);

        try {
            return val == null ? dfltVal : Long.valueOf(val);
        }
        catch (NumberFormatException ignore) {
            throw new IgniteCheckedException("Failed to parse parameter of Long type [" + key + "=" + val + "]");
        }
    }

    /**
     * Retrieves int value from parameters map.
     *
     * @param key Key.
     * @param params Parameters map.
     * @param dfltVal Default value.
     * @return Integer value from parameters map or {@code dfltVal} if null or not exists.
     * @throws IgniteCheckedException If parsing failed.
     */
    private static int intValue(String key, Map<String, Object> params, int dfltVal) throws IgniteCheckedException {
        assert key != null;

        String val = (String)params.get(key);

        try {
            return val == null ? dfltVal : Integer.parseInt(val);
        }
        catch (NumberFormatException ignore) {
            throw new IgniteCheckedException("Failed to parse parameter of Integer type [" + key + "=" + val + "]");
        }
    }

    /**
     * Retrieves UUID value from parameters map.
     *
     * @param key Key.
     * @param params Parameters map.
     * @return UUID value from parameters map or {@code null} if null or not exists.
     * @throws IgniteCheckedException If parsing failed.
     */
    @Nullable private static UUID uuidValue(String key, Map<String, Object> params) throws IgniteCheckedException {
        assert key != null;

        String val = (String)params.get(key);

        try {
            return val == null ? null : UUID.fromString(val);
        }
        catch (NumberFormatException ignore) {
            throw new IgniteCheckedException("Failed to parse parameter of UUID type [" + key + "=" + val + "]");
        }
    }

    /**
     * @throws IOException If failed.
     */
    private void initDefaultPage() throws IOException {
        assert dfltPage == null;

        InputStream in = getClass().getResourceAsStream("rest.html");

        if (in != null) {
            LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in, CHARSET));

            try {
                StringBuilder buf = new StringBuilder(2048);

                for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
                    buf.append(line);

                    if (!line.endsWith(" "))
                        buf.append(' ');
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

        if (target.startsWith("/ignite")) {
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

        GridRestResponse cmdRes;

        Map<String, Object> params = parameters(req);

        try {
            GridRestRequest cmdReq = createRequest(cmd, params, req);

            if (log.isDebugEnabled())
                log.debug("Initialized command request: " + cmdReq);

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

            if (e instanceof Error)
                throw (Error)e;
        }

        String json;

        try {
            json = jsonMapper.writeValueAsString(cmdRes);
        }
        catch (JsonProcessingException e1) {
            U.error(log, "Failed to convert response to JSON: " + cmdRes, e1);

            GridRestResponse resFailed = new GridRestResponse(STATUS_FAILED, e1.getMessage());

            try {
                json = jsonMapper.writeValueAsString(resFailed);
            }
            catch (JsonProcessingException e2) {
                json = "{\"successStatus\": \"1\", \"error:\" \"" + e2.getMessage() + "\"}}";
            }
        }

        try {
            if (log.isDebugEnabled())
                log.debug("Parsed command response into JSON object: " + json);

            res.getWriter().write(json);

            if (log.isDebugEnabled())
                log.debug("Processed HTTP request [action=" + act + ", jsonRes=" + cmdRes + ", req=" + req + ']');
        }
        catch (IOException e) {
            U.error(log, "Failed to send HTTP response: " + json, e);
        }
    }

    /**
     * Creates REST request.
     *
     * @param cmd Command.
     * @param params Parameters.
     * @param req Servlet request.
     * @return REST request.
     * @throws IgniteCheckedException If creation failed.
     */
    @Nullable private GridRestRequest createRequest(GridRestCommand cmd,
        Map<String, Object> params, HttpServletRequest req) throws IgniteCheckedException {
        GridRestRequest restReq;

        switch (cmd) {
            case GET_OR_CREATE_CACHE:
            case DESTROY_CACHE: {
                GridRestCacheRequest restReq0 = new GridRestCacheRequest();

                restReq0.cacheName((String)params.get("cacheName"));

                restReq = restReq0;

                break;
            }

            case ATOMIC_DECREMENT:
            case ATOMIC_INCREMENT: {
                DataStructuresRequest restReq0 = new DataStructuresRequest();

                restReq0.key(params.get("key"));
                restReq0.initial(longValue("init", params, null));
                restReq0.delta(longValue("delta", params, null));

                restReq = restReq0;

                break;
            }

            case CACHE_CONTAINS_KEY:
            case CACHE_CONTAINS_KEYS:
            case CACHE_GET:
            case CACHE_GET_ALL:
            case CACHE_GET_AND_PUT:
            case CACHE_GET_AND_REPLACE:
            case CACHE_PUT_IF_ABSENT:
            case CACHE_GET_AND_PUT_IF_ABSENT:
            case CACHE_PUT:
            case CACHE_PUT_ALL:
            case CACHE_REMOVE:
            case CACHE_REMOVE_VALUE:
            case CACHE_REPLACE_VALUE:
            case CACHE_GET_AND_REMOVE:
            case CACHE_REMOVE_ALL:
            case CACHE_ADD:
            case CACHE_CAS:
            case CACHE_METRICS:
            case CACHE_SIZE:
            case CACHE_METADATA:
            case CACHE_REPLACE:
            case CACHE_APPEND:
            case CACHE_PREPEND: {
                GridRestCacheRequest restReq0 = new GridRestCacheRequest();

                String cacheName = (String)params.get("cacheName");

                restReq0.cacheName(F.isEmpty(cacheName) ? null : cacheName);
                restReq0.key(params.get("key"));
                restReq0.value(params.get("val"));
                restReq0.value2(params.get("val2"));

                Object val1 = params.get("val1");

                if (val1 != null)
                    restReq0.value(val1);

                restReq0.cacheFlags(intValue("cacheFlags", params, 0));
                restReq0.ttl(longValue("exp", params, null));

                if (cmd == CACHE_GET_ALL || cmd == CACHE_PUT_ALL || cmd == CACHE_REMOVE_ALL ||
                    cmd == CACHE_CONTAINS_KEYS) {
                    List<Object> keys = values("k", params);
                    List<Object> vals = values("v", params);

                    if (keys.size() < vals.size())
                        throw new IgniteCheckedException("Number of keys must be greater or equals to number of values.");

                    Map<Object, Object> map = U.newHashMap(keys.size());

                    Iterator<Object> keyIt = keys.iterator();
                    Iterator<Object> valIt = vals.iterator();

                    while (keyIt.hasNext())
                        map.put(keyIt.next(), valIt.hasNext() ? valIt.next() : null);

                    restReq0.values(map);
                }

                restReq = restReq0;

                break;
            }

            case TOPOLOGY:
            case NODE: {
                GridRestTopologyRequest restReq0 = new GridRestTopologyRequest();

                restReq0.includeMetrics(Boolean.parseBoolean((String)params.get("mtr")));
                restReq0.includeAttributes(Boolean.parseBoolean((String)params.get("attr")));

                restReq0.nodeIp((String)params.get("ip"));

                restReq0.nodeId(uuidValue("id", params));

                restReq = restReq0;

                break;
            }

            case EXE:
            case RESULT:
            case NOOP: {
                GridRestTaskRequest restReq0 = new GridRestTaskRequest();

                restReq0.taskId((String)params.get("id"));
                restReq0.taskName((String)params.get("name"));

                restReq0.params(values("p", params));

                restReq0.async(Boolean.parseBoolean((String)params.get("async")));

                restReq0.timeout(longValue("timeout", params, 0L));

                restReq = restReq0;

                break;
            }

            case LOG: {
                GridRestLogRequest restReq0 = new GridRestLogRequest();

                restReq0.path((String)params.get("path"));

                restReq0.from(intValue("from", params, -1));
                restReq0.to(intValue("to", params, -1));

                restReq = restReq0;

                break;
            }

            case NAME:
            case VERSION: {
                restReq = new GridRestRequest();

                break;
            }

            case EXECUTE_SQL_QUERY:
            case EXECUTE_SQL_FIELDS_QUERY: {
                RestQueryRequest restReq0 = new RestQueryRequest();

                restReq0.sqlQuery((String)params.get("qry"));

                restReq0.arguments(values("arg", params).toArray());

                restReq0.typeName((String)params.get("type"));

                String pageSize = (String)params.get("pageSize");

                if (pageSize != null)
                    restReq0.pageSize(Integer.parseInt(pageSize));

                String distributedJoins = (String)params.get("distributedJoins");

                if (distributedJoins != null)
                    restReq0.distributedJoins(Boolean.parseBoolean(distributedJoins));

                restReq0.cacheName((String)params.get("cacheName"));

                if (cmd == EXECUTE_SQL_QUERY)
                    restReq0.queryType(RestQueryRequest.QueryType.SQL);
                else
                    restReq0.queryType(RestQueryRequest.QueryType.SQL_FIELDS);

                restReq = restReq0;

                break;
            }

            case EXECUTE_SCAN_QUERY: {
                RestQueryRequest restReq0 = new RestQueryRequest();

                restReq0.sqlQuery((String)params.get("qry"));

                String pageSize = (String)params.get("pageSize");

                if (pageSize != null)
                    restReq0.pageSize(Integer.parseInt(pageSize));

                restReq0.cacheName((String)params.get("cacheName"));

                restReq0.className((String)params.get("className"));

                restReq0.queryType(RestQueryRequest.QueryType.SCAN);

                restReq = restReq0;

                break;
            }

            case FETCH_SQL_QUERY: {
                RestQueryRequest restReq0 = new RestQueryRequest();

                String qryId = (String)params.get("qryId");

                if (qryId != null)
                    restReq0.queryId(Long.parseLong(qryId));

                String pageSize = (String)params.get("pageSize");

                if (pageSize != null)
                    restReq0.pageSize(Integer.parseInt(pageSize));

                restReq0.cacheName((String)params.get("cacheName"));

                restReq = restReq0;

                break;
            }

            case CLOSE_SQL_QUERY: {
                RestQueryRequest restReq0 = new RestQueryRequest();

                String qryId = (String)params.get("qryId");

                if (qryId != null)
                    restReq0.queryId(Long.parseLong(qryId));

                restReq0.cacheName((String)params.get("cacheName"));

                restReq = restReq0;

                break;
            }

            default:
                throw new IgniteCheckedException("Invalid command: " + cmd);
        }

        restReq.address(new InetSocketAddress(req.getRemoteAddr(), req.getRemotePort()));

        restReq.command(cmd);

        if (params.containsKey("ignite.login") || params.containsKey("ignite.password")) {
            SecurityCredentials cred = new SecurityCredentials(
                (String)params.get("ignite.login"), (String)params.get("ignite.password"));

            restReq.credentials(cred);
        }

        String clientId = (String)params.get("clientId");

        try {
            if (clientId != null)
                restReq.clientId(UUID.fromString(clientId));
        }
        catch (Exception ignored) {
            // Ignore invalid client id. Rest handler will process this logic.
        }

        String destId = (String)params.get("destId");

        try {
            if (destId != null)
                restReq.destinationId(UUID.fromString(destId));
        }
        catch (IllegalArgumentException ignored) {
            // Don't fail - try to execute locally.
        }

        String sesTokStr = (String)params.get("sessionToken");

        try {
            if (sesTokStr != null)
                restReq.sessionToken(U.hexString2ByteArray(sesTokStr));
        }
        catch (IllegalArgumentException ignored) {
            // Ignore invalid session token.
        }

        return restReq;
    }

    /**
     * Gets values referenced by sequential keys, e.g. {@code key1...keyN}.
     *
     * @param keyPrefix Key prefix, e.g. {@code key} for {@code key1...keyN}.
     * @param params Parameters map.
     * @return Values.
     */
    protected List<Object> values(String keyPrefix, Map<String, Object> params) {
        assert keyPrefix != null;

        List<Object> vals = new LinkedList<>();

        for (int i = 1; ; i++) {
            String key = keyPrefix + i;

            if (params.containsKey(key))
                vals.add(params.get(key));
            else
                break;
        }

        return vals;
    }

    /**
     * @param req Request.
     * @return Command.
     */
    @Nullable private GridRestCommand command(ServletRequest req) {
        String cmd = req.getParameter("cmd");

        return cmd == null ? null : GridRestCommand.fromKey(cmd.toLowerCase());
    }

    /**
     * Parses HTTP parameters in an appropriate format and return back map of values to predefined list of names.
     *
     * @param req Request.
     * @return Map of parsed parameters.
     */
    @SuppressWarnings({"unchecked"})
    private Map<String, Object> parameters(ServletRequest req) {
        Map<String, String[]> params = req.getParameterMap();

        if (F.isEmpty(params))
            return Collections.emptyMap();

        Map<String, Object> map = U.newHashMap(params.size());

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

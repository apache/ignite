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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheConfigurationOverride;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.request.DataStructuresRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestBaselineRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestChangeStateRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestClusterNameRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestClusterStateRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestLogRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestTaskRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestTopologyRequest;
import org.apache.ignite.internal.processors.rest.request.RestQueryRequest;
import org.apache.ignite.internal.processors.rest.request.RestUserActionRequest;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.format;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REST_GETALL_AS_ARRAY;
import static org.apache.ignite.internal.client.GridClientCacheFlag.KEEP_BINARIES_MASK;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_CONTAINS_KEYS;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_ACTIVATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_ACTIVE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_CURRENT_STATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_STATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.EXECUTE_SQL_QUERY;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;

/**
 * Jetty REST handler. The following URL format is supported: {@code /ignite?cmd=cmdName&param1=abc&param2=123}
 */
public class GridJettyRestHandler extends AbstractHandler {
    /** Used to sent request charset. */
    private static final String CHARSET = StandardCharsets.UTF_8.name();

    /** */
    private static final String FAILED_TO_PARSE_FORMAT = "Failed to parse parameter of %s type [%s=%s]";

    /** */
    private static final String USER_PARAM = "user";

    /** */
    private static final String PWD_PARAM = "password";

    /** */
    private static final String CACHE_NAME_PARAM = "cacheName";

    /** */
    private static final String BACKUPS_PARAM = "backups";

    /** */
    private static final String CACHE_GROUP_PARAM = "cacheGroup";

    /** */
    private static final String DATA_REGION_PARAM = "dataRegion";

    /** */
    private static final String WRITE_SYNCHRONIZATION_MODE_PARAM = "writeSynchronizationMode";

    /** @deprecated Should be replaced with AUTHENTICATION + token in IGNITE 3.0 */
    private static final String IGNITE_LOGIN = "ignite.login";

    /** @deprecated Should be replaced with AUTHENTICATION + token in IGNITE 3.0 */
    private static final String IGNITE_PASSWORD = "ignite.password";

    /** */
    private static final String TEMPLATE_NAME_PARAM = "templateName";

    /** */
    private static final NullOutputStream NULL_OUTPUT_STREAM = new NullOutputStream();

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

    /** */
    private final boolean getAllAsArray = IgniteSystemProperties.getBoolean(IGNITE_REST_GETALL_AS_ARRAY);

    /**
     * Creates new HTTP requests handler.
     *
     * @param hnd Handler.
     * @param authChecker Authentication checking closure.
     * @param ctx Kernal context.
     */
    GridJettyRestHandler(GridRestProtocolHandler hnd, C1<String, Boolean> authChecker, GridKernalContext ctx) {
        assert hnd != null;
        assert ctx != null;

        this.hnd = hnd;
        this.authChecker = authChecker;
        this.log = ctx.log(getClass());
        this.jsonMapper = new GridJettyObjectMapper(ctx);

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
    @Nullable private static Long longValue(String key, Map<String, String> params,
        Long dfltVal) throws IgniteCheckedException {
        assert key != null;

        String val = params.get(key);

        try {
            return val == null ? dfltVal : Long.valueOf(val);
        }
        catch (NumberFormatException ignore) {
            throw new IgniteCheckedException(format(FAILED_TO_PARSE_FORMAT, "Long", key, val));
        }
    }

    /**
     * Retrieves boolean value from parameters map.
     *
     * @param key Key.
     * @param params Parameters map.
     * @param dfltVal Default value.
     * @return Boolean value from parameters map or {@code dfltVal} if null or not exists.
     */
    private static boolean booleanValue(String key, Map<String, String> params, boolean dfltVal) {
        assert key != null;

        String val = params.get(key);

        return val == null ? dfltVal : Boolean.parseBoolean(val);
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
    private static int intValue(String key, Map<String, String> params, int dfltVal) throws IgniteCheckedException {
        assert key != null;

        String val = params.get(key);

        try {
            return val == null ? dfltVal : Integer.parseInt(val);
        }
        catch (NumberFormatException ignore) {
            throw new IgniteCheckedException(format(FAILED_TO_PARSE_FORMAT, "Integer", key, val));
        }
    }

    private static <T extends Enum<T>> @Nullable T enumValue(
        String key,
        Map<String, String> params,
        Class<T> enumClass
    ) throws IgniteCheckedException {
        assert key != null;
        assert enumClass != null;

        String val = params.get(key);

        if (val == null)
            return null;

        try {
            return Enum.valueOf(enumClass, val);
        }
        catch (IllegalArgumentException e) {
            throw new IgniteCheckedException(format(FAILED_TO_PARSE_FORMAT, enumClass.getSimpleName(), key, val), e);
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
    @Nullable private static UUID uuidValue(String key, Map<String, String> params) throws IgniteCheckedException {
        assert key != null;

        String val = params.get(key);

        try {
            return val == null ? null : UUID.fromString(val);
        }
        catch (NumberFormatException ignore) {
            throw new IgniteCheckedException(format(FAILED_TO_PARSE_FORMAT, "UUID", key, val));
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
        throws IOException {
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

        Map<String, String> params = parameters(req);

        try {
            GridRestRequest cmdReq = createRequest(cmd, params, req);

            if (log.isDebugEnabled())
                log.debug("Initialized command request: " + cmdReq);

            cmdRes = hnd.handle(cmdReq);

            if (cmdRes == null)
                throw new IllegalStateException("Received null result from handler: " + hnd);

            if (getAllAsArray && cmd == GridRestCommand.CACHE_GET_ALL) {
                List<Object> resKeyValue = new ArrayList<>();

                for (Map.Entry<Object, Object> me : ((Map<Object, Object>)cmdRes.getResponse()).entrySet())
                    resKeyValue.add(new IgniteBiTuple<>(me.getKey(), me.getValue()));

                cmdRes.setResponse(resKeyValue);
            }

            byte[] sesTok = cmdRes.sessionTokenBytes();

            if (sesTok != null)
                cmdRes.setSessionToken(U.byteArray2HexString(sesTok));

            res.setStatus(HttpServletResponse.SC_OK);
        }
        catch (Throwable e) {
            res.setStatus(HttpServletResponse.SC_OK);

            U.error(log, "Failed to process HTTP request [action=" + act + ", req=" + req + ']', e);

            if (e instanceof Error)
                throw (Error)e;

            cmdRes = new GridRestResponse(STATUS_FAILED, e.getMessage());
        }

        try (ServletOutputStream os = res.getOutputStream()) {
            try {
                // Try serialize.
                jsonMapper.writeValue(NULL_OUTPUT_STREAM, cmdRes);

                jsonMapper.writeValue(os, cmdRes);
            }
            catch (JsonProcessingException e) {
                U.error(log, "Failed to convert response to JSON: " + cmdRes, e);

                jsonMapper.writeValue(os, new GridRestResponse(STATUS_FAILED, e.getMessage()));
            }

            if (log.isDebugEnabled())
                log.debug("Processed HTTP request [action=" + act + ", jsonRes=" + cmdRes + ", req=" + req + ']');
        }
        catch (IOException e) {
            U.error(log, "Failed to send HTTP response: " + cmdRes, e);
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
    @Nullable private GridRestRequest createRequest(
        GridRestCommand cmd,
        Map<String, String> params,
        HttpServletRequest req
    ) throws IgniteCheckedException {
        GridRestRequest restReq;

        switch (cmd) {
            case GET_OR_CREATE_CACHE: {
                GridRestCacheRequest restReq0 = new GridRestCacheRequest();

                restReq0.cacheName(params.get(CACHE_NAME_PARAM));

                String templateName = params.get(TEMPLATE_NAME_PARAM);

                if (!F.isEmpty(templateName))
                    restReq0.templateName(templateName);

                String backups = params.get(BACKUPS_PARAM);

                CacheConfigurationOverride cfg = new CacheConfigurationOverride();

                // Set cache backups.
                if (!F.isEmpty(backups)) {
                    try {
                        cfg.backups(Integer.parseInt(backups));
                    }
                    catch (NumberFormatException e) {
                        throw new IgniteCheckedException("Failed to parse number of cache backups: " + backups, e);
                    }
                }

                // Set cache group name.
                String cacheGrp = params.get(CACHE_GROUP_PARAM);

                if (!F.isEmpty(cacheGrp))
                    cfg.cacheGroup(cacheGrp);

                // Set cache data region name.
                String dataRegion = params.get(DATA_REGION_PARAM);

                if (!F.isEmpty(dataRegion))
                    cfg.dataRegion(dataRegion);

                // Set cache write mode.
                String wrtSyncMode = params.get(WRITE_SYNCHRONIZATION_MODE_PARAM);

                if (!F.isEmpty(wrtSyncMode)) {
                    try {
                        cfg.writeSynchronizationMode(CacheWriteSynchronizationMode.valueOf(wrtSyncMode));
                    }
                    catch (IllegalArgumentException e) {
                        throw new IgniteCheckedException("Failed to parse cache write synchronization mode: " + wrtSyncMode, e);
                    }
                }

                if (!cfg.isEmpty())
                    restReq0.configuration(cfg);

                restReq = restReq0;

                break;
            }

            case DESTROY_CACHE: {
                GridRestCacheRequest restReq0 = new GridRestCacheRequest();

                restReq0.cacheName(params.get(CACHE_NAME_PARAM));

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
            case CACHE_CLEAR:
            case CACHE_ADD:
            case CACHE_CAS:
            case CACHE_METRICS:
            case CACHE_SIZE:
            case CACHE_UPDATE_TLL:
            case CACHE_METADATA:
            case CACHE_REPLACE:
            case CACHE_APPEND:
            case CACHE_PREPEND: {
                GridRestCacheRequest restReq0 = new GridRestCacheRequest();

                String cacheName = params.get(CACHE_NAME_PARAM);
                restReq0.cacheName(F.isEmpty(cacheName) ? null : cacheName);

                String keyType = params.get("keyType");
                String valType = params.get("valueType");

                Converter converter = new Converter(cacheName);

                restReq0.key(converter.convert(keyType, params.get("key")));
                restReq0.value(converter.convert(valType, params.get("val")));
                restReq0.value2(converter.convert(valType, params.get("val2")));

                Object val1 = converter.convert(valType, params.get("val1"));

                if (val1 != null)
                    restReq0.value(val1);

                // Cache operations via REST will use binary objects.
                restReq0.cacheFlags(intValue("cacheFlags", params, KEEP_BINARIES_MASK));
                restReq0.ttl(longValue("exp", params, null));

                if (cmd == CACHE_GET_ALL || cmd == CACHE_PUT_ALL || cmd == CACHE_REMOVE_ALL ||
                    cmd == CACHE_CONTAINS_KEYS) {
                    List<Object> keys = converter.values(keyType, "k", params);
                    List<Object> vals = converter.values(valType, "v", params);

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

                restReq0.includeMetrics(Boolean.parseBoolean(params.get("mtr")));
                restReq0.includeAttributes(Boolean.parseBoolean(params.get("attr")));

                String caches = params.get("caches");
                restReq0.includeCaches(caches == null || Boolean.parseBoolean(caches));

                restReq0.nodeIp(params.get("ip"));

                restReq0.nodeId(uuidValue("id", params));

                restReq = restReq0;

                break;
            }

            case EXE:
            case RESULT:
            case NOOP: {
                GridRestTaskRequest restReq0 = new GridRestTaskRequest();

                restReq0.taskId(params.get("id"));
                restReq0.taskName(params.get("name"));

                restReq0.params(new Converter().values(null, "p", params));

                restReq0.async(Boolean.parseBoolean(params.get("async")));

                restReq0.timeout(longValue("timeout", params, 0L));

                restReq = restReq0;

                break;
            }

            case LOG: {
                GridRestLogRequest restReq0 = new GridRestLogRequest();

                restReq0.path(params.get("path"));

                restReq0.from(intValue("from", params, -1));
                restReq0.to(intValue("to", params, -1));

                restReq = restReq0;

                break;
            }

            case DATA_REGION_METRICS:
            case DATA_STORAGE_METRICS:
            case NAME:
            case VERSION: {
                restReq = new GridRestRequest();

                break;
            }

            case CLUSTER_ACTIVE:
            case CLUSTER_INACTIVE:
            case CLUSTER_ACTIVATE:
            case CLUSTER_DEACTIVATE:
            case CLUSTER_CURRENT_STATE: {
                GridRestChangeStateRequest restReq0 = new GridRestChangeStateRequest();

                if (cmd == CLUSTER_CURRENT_STATE)
                    restReq0.reqCurrentState();
                else if (cmd == CLUSTER_ACTIVE || cmd == CLUSTER_ACTIVATE)
                    restReq0.active(true);
                else
                    restReq0.active(false);

                restReq0.forceDeactivation(booleanValue(GridRestClusterStateRequest.ARG_FORCE, params, false));

                restReq = restReq0;

                break;
            }

            case CLUSTER_STATE:
            case CLUSTER_SET_STATE: {
                GridRestClusterStateRequest restReq0 = new GridRestClusterStateRequest();

                if (cmd == CLUSTER_STATE)
                    restReq0.reqCurrentMode();
                else {
                    ClusterState newState = enumValue("state", params, ClusterState.class);

                    restReq0.state(newState);

                    restReq0.forceDeactivation(booleanValue(GridRestClusterStateRequest.ARG_FORCE, params, false));
                }

                restReq = restReq0;

                break;
            }

            case CLUSTER_NAME: {
                restReq = new GridRestClusterNameRequest();

                break;
            }

            case BASELINE_CURRENT_STATE:
            case BASELINE_SET:
            case BASELINE_ADD:
            case BASELINE_REMOVE: {
                GridRestBaselineRequest restReq0 = new GridRestBaselineRequest();

                restReq0.topologyVersion(longValue("topVer", params, null));
                restReq0.consistentIds(new Converter().values(null, "consistentId", params));

                restReq = restReq0;

                break;
            }

            case AUTHENTICATE: {
                restReq = new GridRestRequest();

                break;
            }

            case ADD_USER:
            case REMOVE_USER:
            case UPDATE_USER: {
                RestUserActionRequest restReq0 = new RestUserActionRequest();

                restReq0.user(params.get("user"));
                restReq0.password(params.get("password"));

                restReq = restReq0;

                break;
            }

            case EXECUTE_SQL_QUERY:
            case EXECUTE_SQL_FIELDS_QUERY: {
                RestQueryRequest restReq0 = new RestQueryRequest();

                String cacheName = params.get(CACHE_NAME_PARAM);

                restReq0.sqlQuery(params.get("qry"));

                restReq0.arguments(new Converter(cacheName).values(null, "arg", params).toArray());

                restReq0.typeName(params.get("type"));

                String pageSize = params.get("pageSize");

                if (pageSize != null)
                    restReq0.pageSize(Integer.parseInt(pageSize));

                String keepBinary = params.get("keepBinary");

                if (keepBinary != null)
                    restReq0.keepBinary(Boolean.parseBoolean(keepBinary));

                String distributedJoins = params.get("distributedJoins");

                if (distributedJoins != null)
                    restReq0.distributedJoins(Boolean.parseBoolean(distributedJoins));

                restReq0.cacheName(cacheName);

                if (cmd == EXECUTE_SQL_QUERY)
                    restReq0.queryType(RestQueryRequest.QueryType.SQL);
                else
                    restReq0.queryType(RestQueryRequest.QueryType.SQL_FIELDS);

                restReq = restReq0;

                break;
            }

            case EXECUTE_SCAN_QUERY: {
                RestQueryRequest restReq0 = new RestQueryRequest();

                restReq0.sqlQuery(params.get("qry"));

                String pageSize = params.get("pageSize");

                if (pageSize != null)
                    restReq0.pageSize(Integer.parseInt(pageSize));

                restReq0.cacheName(params.get(CACHE_NAME_PARAM));

                restReq0.className(params.get("className"));

                restReq0.queryType(RestQueryRequest.QueryType.SCAN);

                restReq = restReq0;

                break;
            }

            case FETCH_SQL_QUERY: {
                RestQueryRequest restReq0 = new RestQueryRequest();

                String qryId = params.get("qryId");

                if (qryId != null)
                    restReq0.queryId(Long.parseLong(qryId));

                String pageSize = params.get("pageSize");

                if (pageSize != null)
                    restReq0.pageSize(Integer.parseInt(pageSize));

                restReq0.cacheName(params.get(CACHE_NAME_PARAM));

                restReq = restReq0;

                break;
            }

            case CLOSE_SQL_QUERY: {
                RestQueryRequest restReq0 = new RestQueryRequest();

                String qryId = params.get("qryId");

                if (qryId != null)
                    restReq0.queryId(Long.parseLong(qryId));

                restReq0.cacheName(params.get(CACHE_NAME_PARAM));

                restReq = restReq0;

                break;
            }

            default:
                throw new IgniteCheckedException("Invalid command: " + cmd);
        }

        restReq.address(new InetSocketAddress(req.getRemoteAddr(), req.getRemotePort()));

        restReq.command(cmd);

        Object certs = req.getAttribute("javax.servlet.request.X509Certificate");

        if (certs instanceof X509Certificate[])
            restReq.certificates((X509Certificate[])certs);

        // TODO: In IGNITE 3.0 we should check credentials only for AUTHENTICATE command.
        if (!credentials(params, IGNITE_LOGIN, IGNITE_PASSWORD, restReq))
            credentials(params, USER_PARAM, PWD_PARAM, restReq);

        String clientId = params.get("clientId");

        try {
            if (clientId != null)
                restReq.clientId(UUID.fromString(clientId));
        }
        catch (Exception ignored) {
            // Ignore invalid client id. Rest handler will process this logic.
        }

        String destId = params.get("destId");

        try {
            if (destId != null)
                restReq.destinationId(UUID.fromString(destId));
        }
        catch (IllegalArgumentException ignored) {
            // Don't fail - try to execute locally.
        }

        String sesTokStr = params.get("sessionToken");

        try {
            if (sesTokStr != null) {
                // Token is a UUID encoded as 16 bytes as HEX.
                byte[] bytes = U.hexString2ByteArray(sesTokStr);

                if (bytes.length == 16)
                    restReq.sessionToken(bytes);
            }
        }
        catch (IllegalArgumentException ignored) {
            // Ignore invalid session token.
        }

        return restReq;
    }

    /**
     * @param params Parameters.
     * @param userParam Parameter name to take user name.
     * @param pwdParam Parameter name to take password.
     * @param restReq Request to add credentials if any.
     * @return {@code true} If params contains credentials.
     */
    private boolean credentials(Map<String, String> params, String userParam, String pwdParam,
        GridRestRequest restReq) {
        boolean hasCreds = params.containsKey(userParam) || params.containsKey(pwdParam);

        if (hasCreds) {
            SecurityCredentials cred = new SecurityCredentials(params.get(userParam), params.get(pwdParam));

            restReq.credentials(cred);
        }

        return hasCreds;
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
    private Map<String, String> parameters(ServletRequest req) {
        Map<String, String[]> params = req.getParameterMap();

        if (F.isEmpty(params))
            return Collections.emptyMap();

        Map<String, String> map = U.newHashMap(params.size());

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

        if (obj instanceof String[] && ((String[])obj).length > 0)
            return ((String[])obj)[0];

        return null;
    }

    /**
     * Converter from string into specified type.
     */
    private class Converter {
        /** Cache name. */
        private final String cacheName;

        /**
         * @param cacheName Cache name.
         */
        private Converter(String cacheName) {
            this.cacheName = cacheName;
        }

        /** */
        private Converter() {
            this(null);
        }

        /**
         * Gets and converts values referenced by sequential keys, e.g. {@code key1...keyN}.
         *
         * @param type Optional value type.
         * @param keyPrefix Key prefix, e.g. {@code key} for {@code key1...keyN}.
         * @param params Parameters map.
         * @return Values.
         * @throws IgniteCheckedException If failed to convert.
         */
        private List<Object> values(String type, String keyPrefix,
            Map<String, String> params) throws IgniteCheckedException {
            assert keyPrefix != null;

            List<Object> vals = new LinkedList<>();

            for (int i = 1; ; i++) {
                String key = keyPrefix + i;

                if (params.containsKey(key))
                    vals.add(convert(type, params.get(key)));
                else
                    break;
            }

            return vals;
        }

        /**
         * @param type Optional value type.
         * @param str String to convert.
         * @return Converted value.
         * @throws IgniteCheckedException If failed to convert.
         */
        private Object convert(@Nullable String type, @Nullable String str) throws IgniteCheckedException {
            if (F.isEmpty(type) || str == null)
                return str;

            try {
                switch (type.toLowerCase()) {
                    case "boolean":
                    case "java.lang.boolean":
                        return Boolean.valueOf(str);

                    case "byte":
                    case "java.lang.byte":
                        return Byte.valueOf(str);

                    case "short":
                    case "java.lang.short":
                        return Short.valueOf(str);

                    case "int":
                    case "integer":
                    case "java.lang.integer":
                        return Integer.valueOf(str);

                    case "long":
                    case "java.lang.long":
                        return Long.valueOf(str);

                    case "float":
                    case "java.lang.float":
                        return Float.valueOf(str);

                    case "double":
                    case "java.lang.double":
                        return Double.valueOf(str);

                    case "date":
                    case "java.sql.date":
                        return Date.valueOf(str);

                    case "time":
                    case "java.sql.time":
                        return Time.valueOf(str);

                    case "timestamp":
                    case "java.sql.timestamp":
                        return Timestamp.valueOf(str);

                    case "uuid":
                    case "java.util.uuid":
                        return UUID.fromString(str);

                    case "igniteuuid":
                    case "org.apache.ignite.lang.igniteuuid":
                        return IgniteUuid.fromString(str);

                    case "string":
                    case "java.lang.string":
                        return str;
                }

                // Creating an object of the specified type, if its class is available.
                Class<?> cls = U.classForName(type, null);

                if (cls != null)
                    return jsonMapper.readValue(str, cls);

                // Creating a binary object if the type is not a class name or it cannot be loaded.
                InjectableValues.Std prop = new InjectableValues.Std()
                    .addValue(IgniteBinaryObjectJsonDeserializer.BINARY_TYPE_PROPERTY, type)
                    .addValue(IgniteBinaryObjectJsonDeserializer.CACHE_NAME_PROPERTY, cacheName);

                return jsonMapper.reader(prop).forType(BinaryObject.class).readValue(str);
            }
            catch (Throwable e) {
                throw new IgniteCheckedException("Failed to convert value to specified type [type=" + type +
                    ", val=" + str + ", reason=" + e.getClass().getName() + ": " + e.getMessage() + "]", e);
            }
        }
    }

    /**
     * Special stream to check JSON serialization.
     */
    private static class NullOutputStream extends OutputStream {
        /** {@inheritDoc} */
        @Override public void write(byte[] b, int off, int len) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void write(int b) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void write(byte[] b) {
            // No-op.
        }
    }
}

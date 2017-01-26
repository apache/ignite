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

package org.apache.ignite.cache.websession;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.cache.CacheException;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.*;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.websession.WebSessionAttributeProcessor;
import org.apache.ignite.internal.websession.WebSessionEntity;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.startup.servlet.ServletContextListenerStartup;
import org.apache.ignite.transactions.Transaction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Filter for web sessions caching.
 * <p>
 * This is a request filter, that you need to specify in your {@code web.xml} along
 * with {@link ServletContextListenerStartup} to enable web sessions caching:
 * <pre name="code" class="xml">
 * &lt;listener&gt;
 *     &lt;listener-class&gt;org.apache.ignite.startup.servlet.ServletContextListenerStartup&lt;/listener-class&gt;
 * &lt;/listener&gt;
 *
 * &lt;filter&gt;
 *     &lt;filter-name&gt;WebSessionFilter&lt;/filter-name&gt;
 *     &lt;filter-class&gt;org.apache.ignite.cache.websession.WebSessionFilter&lt;/filter-class&gt;
 * &lt;/filter&gt;
 *
 * &lt;!-- You can also specify a custom URL pattern. --&gt;
 * &lt;filter-mapping&gt;
 *     &lt;filter-name&gt;IgniteWebSessionsFilter&lt;/filter-name&gt;
 *     &lt;url-pattern&gt;/*&lt;/url-pattern&gt;
 * &lt;/filter-mapping&gt;
 * </pre>
 * It is also possible to specify a servlet name in a filter mapping, and a servlet URL pattern will
 * be used in this case:
 * <pre name="code" class="xml">
 * &lt;filter&gt;
 *     &lt;filter-name&gt;WebSessionFilter&lt;/filter-name&gt;
 *     &lt;filter-class&gt;org.apache.ignite.cache.websession.WebSessionFilter&lt;/filter-class&gt;
 * &lt;/filter&gt;
 *
 * &lt;filter-mapping&gt;
 *     &lt;filter-name&gt;WebSessionFilter&lt;/filter-name&gt;
 *     &lt;servlet-name&gt;YourServletName&lt;/servlet-name&gt;
 * &lt;/filter-mapping&gt;
 * </pre>
 * The filter has the following optional configuration parameters:
 * <table class="doctable">
 *     <tr>
 *         <th>Name</th>
 *         <th>Description</th>
 *         <th>Default</th>
 *     </tr>
 *     <tr>
 *         <td>IgniteWebSessionsGridName</td>
 *         <td>Name of the grid that contains cache for web session storage.</td>
 *         <td>{@code null} (default grid)</td>
 *     </tr>
 *     <tr>
 *         <td>IgniteWebSessionsCacheName</td>
 *         <td>Name of the cache for web session storage.</td>
 *         <td>{@code null} (default cache)</td>
 *     </tr>
 *     <tr>
 *         <td>IgniteWebSessionsMaximumRetriesOnFail</td>
 *         <td>
 *             Valid for {@code ATOMIC} caches only. Maximum number of retries for session updates in case
 *             node leaves topology and update fails. If retry is enabled,
 *             some updates can be applied more than once, otherwise some
 *             updates can be lost.
 *             <p>
 *             To disable retries, set this parameter to {@code 0}.
 *         </td>
 *         <td>{@code 3}</td>
 *     </tr>
 *     <tr>
 *         <td>IgniteWebSessionsRetriesTimeout</td>
 *         <td>
 *             Retry timeout. Related to IgniteWebSessionsMaximumRetriesOnFail param.
 *             <p>
 *             Further attempts will be cancelled in case timeout was exceeded.
 *         </td>
 *         <td>{@code 10000} (10 seconds)</td>
 *     </tr>
 * </table>
 * These parameters are taken from either filter init parameter list or
 * servlet context parameters. You can specify filter init parameters as follows:
 * <pre name="code" class="xml">
 * &lt;filter&gt;
 *     &lt;filter-name&gt;WebSessionFilter&lt;/filter-name&gt;
 *     &lt;filter-class&gt;org.apache.ignite.cache.websession.WebSessionFilter&lt;/filter-class&gt;
 *     &lt;init-param&gt;
 *         &lt;param-name&gt;IgniteWebSessionsGridName&lt;/param-name&gt;
 *         &lt;param-value&gt;WebGrid&lt;/param-value&gt;
 *     &lt;/init-param&gt;
 *     &lt;init-param&gt;
 *         &lt;param-name&gt;IgniteWebSessionsCacheName&lt;/param-name&gt;
 *         &lt;param-value&gt;WebCache&lt;/param-value&gt;
 *     &lt;/init-param&gt;
 *
 *     &lt;!-- Valid for ATOMIC caches only. --&gt;
 *     &lt;init-param&gt;
 *         &lt;param-name&gt;IgniteWebSessionsMaximumRetriesOnFail&lt;/param-name&gt;
 *         &lt;param-value&gt;10&lt;/param-value&gt;
 *     &lt;/init-param&gt;
 * &lt;/filter&gt;
 * </pre>
 * <b>Note:</b> filter init parameter has a priority over servlet context
 * parameter; if you specify both, the servlet context parameter will be ignored.
 * <h1 class="header">Web sessions caching and concurrent requests</h1>
 * If your web application can accept concurrent request for one session,
 * consider using {@link org.apache.ignite.cache.CacheAtomicityMode#TRANSACTIONAL} cache
 * instead of {@link org.apache.ignite.cache.CacheAtomicityMode#ATOMIC}. In this case each request
 * be processed inside pessimistic transaction which will guarantee that all
 * updates will be applied in correct order. This is important, for example,
 * if you get some attribute from the session, update its value and set new
 * value back to the session. In case of {@link org.apache.ignite.cache.CacheAtomicityMode#ATOMIC}
 * cache concurrent requests can get equal value, but {@link org.apache.ignite.cache.CacheAtomicityMode#TRANSACTIONAL}
 * cache will always process such updates one after another.
 */
public class WebSessionFilter implements Filter {
    /** Web sessions caching grid name parameter name. */
    public static final String WEB_SES_NAME_PARAM = "IgniteWebSessionsGridName";

    /** Web sessions caching cache name parameter name. */
    public static final String WEB_SES_CACHE_NAME_PARAM = "IgniteWebSessionsCacheName";

    /** Web sessions caching retry on fail parameter name (valid for ATOMIC cache only). */
    public static final String WEB_SES_MAX_RETRIES_ON_FAIL_NAME_PARAM = "IgniteWebSessionsMaximumRetriesOnFail";

    /** Web sessions caching retry on fail timeout parameter name. */
    public static final String WEB_SES_RETRIES_TIMEOUT_NAME_PARAM = "IgniteWebSessionsRetriesTimeout";

    /** */
    public static final String WEB_SES_KEEP_BINARY_PARAM = "IgniteWebSessionsKeepBinary";

    /** Default retry on fail flag value. */
    public static final int DFLT_MAX_RETRIES_ON_FAIL = 3;

    /** Default retry on fail timeout flag value. */
    public static final int DFLT_RETRIES_ON_FAIL_TIMEOUT = 10000;

    /** Default keep binary flag. */
    public static final boolean DFLT_KEEP_BINARY_FLAG = true;

    /** Cache. */
    private IgniteCache<String, WebSession> cache;

    /** Binary cache */
    private IgniteCache<String, WebSessionEntity> binaryCache;

    /** Transactions. */
    private IgniteTransactions txs;

    /** Logger. */
    private IgniteLogger log;

    /** Servlet context. */
    private ServletContext ctx;

    /** Session ID transformer. */
    private IgniteClosure<String, String> sesIdTransformer;

    /** Transactions enabled flag. */
    private boolean txEnabled;

    /** Node. */
    private Ignite webSesIgnite;

    /** Cache name. */
    private String cacheName;

    /** */
    private int retries;

    /** */
    private int retriesTimeout;

    /** */
    private boolean keepBinary = DFLT_KEEP_BINARY_FLAG;

    /** */
    private Marshaller marshaller;

    /** {@inheritDoc} */
    @Override public void init(FilterConfig cfg) throws ServletException {
        ctx = cfg.getServletContext();

        String gridName = U.firstNotNull(
            cfg.getInitParameter(WEB_SES_NAME_PARAM),
            ctx.getInitParameter(WEB_SES_NAME_PARAM));

        cacheName = U.firstNotNull(
            cfg.getInitParameter(WEB_SES_CACHE_NAME_PARAM),
            ctx.getInitParameter(WEB_SES_CACHE_NAME_PARAM));

        String retriesStr = U.firstNotNull(
            cfg.getInitParameter(WEB_SES_MAX_RETRIES_ON_FAIL_NAME_PARAM),
            ctx.getInitParameter(WEB_SES_MAX_RETRIES_ON_FAIL_NAME_PARAM));

        try {
            retries = retriesStr != null ? Integer.parseInt(retriesStr) : DFLT_MAX_RETRIES_ON_FAIL;
        }
        catch (NumberFormatException e) {
            throw new IgniteException("Maximum number of retries parameter is invalid: " + retriesStr, e);
        }

        String retriesTimeoutStr = U.firstNotNull(
            cfg.getInitParameter(WEB_SES_RETRIES_TIMEOUT_NAME_PARAM),
            ctx.getInitParameter(WEB_SES_RETRIES_TIMEOUT_NAME_PARAM));

        try {
            retriesTimeout = retriesTimeoutStr != null ?
                Integer.parseInt(retriesTimeoutStr) : DFLT_RETRIES_ON_FAIL_TIMEOUT;
        }
        catch (NumberFormatException e) {
            throw new IgniteException("Retries timeout parameter is invalid: " + retriesTimeoutStr, e);
        }

        final String binParam = cfg.getInitParameter(WEB_SES_KEEP_BINARY_PARAM);

        if (!F.isEmpty(binParam))
            keepBinary = Boolean.parseBoolean(binParam);

        webSesIgnite = G.ignite(gridName);

        if (webSesIgnite == null)
            throw new IgniteException("Grid for web sessions caching is not started (is it configured?): " +
                gridName);

        txs = webSesIgnite.transactions();

        log = webSesIgnite.log();

        marshaller = webSesIgnite.configuration().getMarshaller();

        initCache();

        String srvInfo = ctx.getServerInfo();

        // Special case for WebLogic, which appends timestamps to session
        // IDs upon session creation (the created session ID looks like:
        // pdpTSTcCcG6CVM8BTZWzxjTB1lh3w7zFbYVvwBb4bJGjrBx3TMPl!-508312620!1385045122601).
        if (srvInfo != null && srvInfo.contains("WebLogic")) {
            sesIdTransformer = new C1<String, String>() {
                @Override public String apply(String s) {
                    // Find first exclamation mark.
                    int idx = s.indexOf('!');

                    // Return original string if not found.
                    if (idx < 0 || idx == s.length() - 1)
                        return s;

                    // Find second exclamation mark.
                    idx = s.indexOf('!', idx + 1);

                    // Return original string if not found.
                    if (idx < 0)
                        return s;

                    // Return the session ID without timestamp.
                    return s.substring(0, idx);
                }
            };
        }

        if (log.isInfoEnabled())
            log.info("Started web sessions caching [gridName=" + gridName + ", cacheName=" + cacheName +
                ", maxRetriesOnFail=" + retries + ']');
    }

    /**
     * Init cache.
     */
    @SuppressWarnings("unchecked")
    void initCache() {
        cache = webSesIgnite.cache(cacheName);
        binaryCache = webSesIgnite.cache(cacheName);

        if (cache == null)
            throw new IgniteException("Cache for web sessions is not started (is it configured?): " + cacheName);

        CacheConfiguration cacheCfg = cache.getConfiguration(CacheConfiguration.class);

        if (cacheCfg.getWriteSynchronizationMode() == FULL_ASYNC)
            throw new IgniteException("Cache for web sessions cannot be in FULL_ASYNC mode: " + cacheName);

        if (!cacheCfg.isEagerTtl())
            throw new IgniteException("Cache for web sessions cannot operate with lazy TTL. " +
                "Consider setting eagerTtl to true for cache: " + cacheName);

        if (cacheCfg.getCacheMode() == LOCAL)
            U.quietAndWarn(webSesIgnite.log(), "Using LOCAL cache for web sessions caching " +
                "(this is only OK in test mode): " + cacheName);

        if (cacheCfg.getCacheMode() == PARTITIONED && cacheCfg.getAtomicityMode() != ATOMIC)
            U.quietAndWarn(webSesIgnite.log(), "Using " + cacheCfg.getAtomicityMode() + " atomicity for web sessions " +
                "caching (switch to ATOMIC mode for better performance)");

        txEnabled = cacheCfg.getAtomicityMode() == TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
        throws IOException, ServletException {
        assert ctx != null;

        if (req instanceof HttpServletRequest) {
            HttpServletRequest httpReq = (HttpServletRequest)req;

            String sesId = null;

            try {
                if (txEnabled) {
                    try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        sesId = doFilterDispatch(httpReq, res, chain);

                        tx.commit();
                    }
                }
                else
                    sesId = doFilterDispatch(httpReq, res, chain);
            }
            catch (Exception e) {
                U.error(log, "Failed to update web session: " + sesId, e);
            }
        }
        else
            chain.doFilter(req, res);
    }

    /**
     * Use {@link WebSession} or {@link WebSessionV2} according to {@link #keepBinary} flag.
     *
     * @param httpReq Request.
     * @param res Response.
     * @param chain Filter chain.
     * @return Session ID.
     * @throws IOException
     * @throws ServletException
     * @throws CacheException
     */
    private String doFilterDispatch(HttpServletRequest httpReq, ServletResponse res, FilterChain chain)
        throws IOException, ServletException, CacheException {
        if (keepBinary)
            return doFilterV2(httpReq, res, chain);

        return doFilterV1(httpReq, res, chain);
    }

    /**
     * @param httpReq Request.
     * @param res Response.
     * @param chain Filter chain.
     * @return Session ID.
     * @throws IOException In case of I/O error.
     * @throws ServletException In case of servlet error.
     * @throws CacheException In case of other error.
     */
    private String doFilterV1(HttpServletRequest httpReq, ServletResponse res, FilterChain chain) throws IOException,
        ServletException, CacheException {
        WebSession cached = null;

        String sesId = httpReq.getRequestedSessionId();

        if (sesId != null) {
            sesId = transformSessionId(sesId);

            for (int i = 0; i < retries; i++) {
                try {
                    cached = cache.get(sesId);

                    break;
                }
                catch (CacheException | IgniteException | IllegalStateException e) {
                    handleLoadSessionException(sesId, i, e);
                }
            }

            if (cached != null) {
                if (log.isDebugEnabled())
                    log.debug("Using cached session for ID: " + sesId);

                if (cached.isNew())
                    cached = new WebSession(cached.getId(), cached, false);
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Cached session was invalidated and doesn't exist: " + sesId);

                HttpSession ses = httpReq.getSession(false);

                if (ses != null) {
                    try {
                        ses.invalidate();
                    }
                    catch (IllegalStateException ignore) {
                        // Session was already invalidated.
                    }
                }

                cached = createSession(httpReq);
            }
        }
        else
            cached = createSession(httpReq);

        assert cached != null;

        sesId = cached.getId();

        cached.servletContext(ctx);
        cached.filter(this);
        cached.resetUpdates();
        cached.genSes(httpReq.getSession(false));

        httpReq = new RequestWrapper(httpReq, cached);

        chain.doFilter(httpReq, res);

        HttpSession ses = httpReq.getSession(false);

        if (ses != null && ses instanceof WebSession) {
            Collection<T2<String, Object>> updates = ((WebSession) ses).updates();

            if (updates != null)
                updateAttributes(transformSessionId(sesId), updates, ses.getMaxInactiveInterval());
        }

        return sesId;
    }

    /**
     * @param httpReq Request.
     * @param res Response.
     * @param chain Filter chain.
     * @return Session ID.
     * @throws IOException In case of I/O error.
     * @throws ServletException In case oif servlet error.
     * @throws CacheException In case of other error.
     */
    private String doFilterV2(HttpServletRequest httpReq, ServletResponse res, FilterChain chain)
        throws IOException, ServletException, CacheException {
        WebSessionV2 cached = null;

        String sesId = httpReq.getRequestedSessionId();

        if (sesId != null) {
            sesId = transformSessionId(sesId);

            // Load from cache.
            for (int i = 0; i < retries; i++) {
                try {
                    final WebSessionEntity entity = binaryCache.get(sesId);

                    if (entity != null)
                        cached = new WebSessionV2(sesId, httpReq.getSession(false), false, ctx, entity, marshaller);

                    break;
                }
                catch (CacheException | IgniteException | IllegalStateException e) {
                    handleLoadSessionException(sesId, i, e);
                }
            }

            if (cached != null) {
                if (log.isDebugEnabled())
                    log.debug("Using cached session for ID: " + sesId);
            }
            // If not found - invalidate session and create new one.
            // Invalidate, because session might be removed from cache
            // according to expiry policy.
            else {
                if (log.isDebugEnabled())
                    log.debug("Cached session was invalidated and doesn't exist: " + sesId);

                final HttpSession ses = httpReq.getSession(false);

                if (ses != null) {
                    try {
                        ses.invalidate();
                    }
                    catch (IllegalStateException ignore) {
                        // Session was already invalidated.
                    }
                }

                cached = createSessionV2(httpReq);
            }
        }
        // No session was requested by the client, create new one and put in the request.
        else
            cached = createSessionV2(httpReq);

        assert cached != null;

        sesId = cached.getId();

        httpReq = new RequestWrapperV2(httpReq, cached);

        chain.doFilter(httpReq, res);

        if (!cached.isValid())
            binaryCache.remove(cached.id());
        // Changed session ID.
        else if (!cached.getId().equals(sesId)) {
            final String oldId = cached.getId();

            cached.invalidate();

            binaryCache.remove(oldId);
        }
        else
            updateAttributesV2(cached.getId(), cached);

        return sesId;
    }

    /**
     * Log and process exception happened on loading session from cache.
     *
     * @param sesId Session ID.
     * @param tryCnt Try count.
     * @param e Caught exception.
     */
    private void handleLoadSessionException(final String sesId, final int tryCnt, final RuntimeException e) {
        if (log.isDebugEnabled())
            log.debug(e.getMessage());

        if (tryCnt == retries - 1)
            throw new IgniteException("Failed to handle request [session= " + sesId + "]", e);
        else {
            if (log.isDebugEnabled())
                log.debug("Failed to handle request (will retry): " + sesId);

            handleCacheOperationException(e);
        }
    }

    /**
     * Transform session ID if ID transformer present.
     *
     * @param sesId Session ID to transform.
     * @return Transformed session ID or the same if no session transformer available.
     */
    private String transformSessionId(final String sesId) {
        if (sesIdTransformer != null)
            return sesIdTransformer.apply(sesId);

        return sesId;
    }

    /**
     * Creates a new session from http request.
     *
     * @param httpReq Request.
     * @return New session.
     */
    @SuppressWarnings("unchecked")
    private WebSession createSession(HttpServletRequest httpReq) {
        HttpSession ses = httpReq.getSession(true);

        String sesId = transformSessionId(ses.getId());

        return createSession(ses, sesId);
    }

    /**
     * Creates a new web session with the specified id.
     *
     * @param ses Base session.
     * @param sesId Session id.
     * @return New session.
     */
    @SuppressWarnings("unchecked")
    private WebSession createSession(HttpSession ses, String sesId) {
        WebSession cached = new WebSession(sesId, ses, true);

        cached.genSes(ses);

        if (log.isDebugEnabled())
            log.debug("Session created: " + sesId);

        for (int i = 0; i < retries; i++) {
            try {
                final IgniteCache<String, WebSession> cache0 =
                    cacheWithExpiryPolicy(cached.getMaxInactiveInterval(), cache);

                final WebSession old = cache0.getAndPutIfAbsent(sesId, cached);

                if (old != null) {
                    cached = old;

                    if (cached.isNew())
                        cached = new WebSession(cached.getId(), cached, false);
                }

                break;
            }
            catch (CacheException | IgniteException | IllegalStateException e) {
                handleCreateSessionException(sesId, i, e);
            }
        }

        return cached;
    }

    /**
     * Log error and delegate exception processing to {@link #handleCacheOperationException(Exception)}
     *
     * @param sesId Session ID.
     * @param tryCnt Try count.
     * @param e Exception to process.
     */
    private void handleCreateSessionException(final String sesId, final int tryCnt, final RuntimeException e) {
        if (log.isDebugEnabled())
            log.debug(e.getMessage());

        if (tryCnt == retries - 1)
            throw new IgniteException("Failed to save session: " + sesId, e);
        else {
            if (log.isDebugEnabled())
                log.debug("Failed to save session (will retry): " + sesId);

            handleCacheOperationException(e);
        }
    }

    /**
     * Creates a new web session with the specified id.
     *
     * @param ses Base session.
     * @param sesId Session id.
     * @return New session.
     */
    private WebSessionV2 createSessionV2(final HttpSession ses, final String sesId) throws IOException {
        assert ses != null;
        assert sesId != null;

        WebSessionV2 cached = new WebSessionV2(sesId, ses, true, ctx, null, marshaller);

        final WebSessionEntity marshaledEntity = cached.marshalAttributes();

        for (int i = 0; i < retries; i++) {
            try {
                final IgniteCache<String, WebSessionEntity> cache0 = cacheWithExpiryPolicy(
                    cached.getMaxInactiveInterval(), binaryCache);

                final WebSessionEntity old = cache0.getAndPutIfAbsent(sesId, marshaledEntity);

                if (old != null)
                    cached = new WebSessionV2(sesId, ses, false, ctx, old, marshaller);
                else
                    cached = new WebSessionV2(sesId, ses, false, ctx, marshaledEntity, marshaller);

                break;
            }
            catch (CacheException | IgniteException | IllegalStateException e) {
                handleCreateSessionException(sesId, i, e);
            }
        }

        return cached;
    }

    /**
     * @param httpReq HTTP request.
     * @return Cached session.
     */
    private WebSessionV2 createSessionV2(HttpServletRequest httpReq) throws IOException {
        final HttpSession ses = httpReq.getSession(true);

        final String sesId = transformSessionId(ses.getId());

        if (log.isDebugEnabled())
            log.debug("Session created: " + sesId);

        return createSessionV2(ses, sesId);
    }

    /**
     * @param maxInactiveInteval Interval to use in expiry policy.
     * @param cache Cache.
     * @param <T> Cached object type.
     * @return Cache with expiry policy if {@code maxInactiveInteval} greater than zero.
     */
    private <T> IgniteCache<String, T> cacheWithExpiryPolicy(final int maxInactiveInteval,
        final IgniteCache<String, T> cache) {
        if (maxInactiveInteval > 0) {
            long ttl = maxInactiveInteval * 1000;

            ExpiryPolicy plc = new ModifiedExpiryPolicy(new Duration(MILLISECONDS, ttl));

            return cache.withExpiryPolicy(plc);
        }

        return cache;
    }

    /**
     * @param sesId Session ID.
     */
    public void destroySession(String sesId) {
        assert sesId != null;
        for (int i = 0; i < retries; i++) {
            try {
                if (cache.remove(sesId) && log.isDebugEnabled())
                    log.debug("Session destroyed: " + sesId);
            }
            catch (CacheException | IgniteException | IllegalStateException e) {
                if (i == retries - 1) {
                    U.warn(log, "Failed to remove session [sesId=" +
                        sesId + ", retries=" + retries + ']');
                }
                else {
                    U.warn(log, "Failed to remove session (will retry): " + sesId);

                    handleCacheOperationException(e);
                }
            }
        }
    }

    /**
     * @param sesId Session ID.
     * @param updates Updates list.
     * @param maxInactiveInterval Max session inactive interval.
     */
    @SuppressWarnings("unchecked")
    public void updateAttributes(String sesId, Collection<T2<String, Object>> updates, int maxInactiveInterval) {
        assert sesId != null;
        assert updates != null;

        if (log.isDebugEnabled())
            log.debug("Session attributes updated [id=" + sesId + ", updates=" + updates + ']');

        try {
            for (int i = 0; i < retries; i++) {
                try {
                    final IgniteCache<String, WebSession> cache0 = cacheWithExpiryPolicy(maxInactiveInterval, cache);

                    cache0.invoke(sesId, WebSessionListener.newAttributeProcessor(updates));

                    break;
                }
                catch (CacheException | IgniteException | IllegalStateException e) {
                    handleAttributeUpdateException(sesId, i, e);
                }
            }
        }
        catch (Exception e) {
            U.error(log, "Failed to update session attributes [id=" + sesId + ']', e);
        }
    }

    /**
     * @param sesId Session ID.
     * @param ses Web session.
     */
    public void updateAttributesV2(final String sesId, final WebSessionV2 ses) throws IOException {
        assert sesId != null;
        assert ses != null;

        final Map<String, byte[]> updatesMap = ses.binaryUpdatesMap();

        if (log.isDebugEnabled())
            log.debug("Session binary attributes updated [id=" + sesId + ", updates=" + updatesMap.keySet() + ']');

        try {
            for (int i = 0; i < retries; i++) {
                try {
                    final IgniteCache<String, WebSessionEntity> cache0 =
                        cacheWithExpiryPolicy(ses.getMaxInactiveInterval(), binaryCache);

                    cache0.invoke(sesId, new WebSessionAttributeProcessor(updatesMap.isEmpty() ? null : updatesMap,
                        ses.getLastAccessedTime(), ses.getMaxInactiveInterval(), ses.isMaxInactiveIntervalChanged()));

                    break;
                }
                catch (CacheException | IgniteException | IllegalStateException e) {
                    handleAttributeUpdateException(sesId, i, e);
                }
            }
        }
        catch (Exception e) {
            U.error(log, "Failed to update session V2 attributes [id=" + sesId + ']', e);
        }
    }

    /**
     * Log error and delegate processing to {@link #handleCacheOperationException(Exception)}.
     *
     * @param sesId Session ID.
     * @param tryCnt Try count.
     * @param e Exception to process.
     */
    private void handleAttributeUpdateException(final String sesId, final int tryCnt, final RuntimeException e) {
        if (tryCnt == retries - 1) {
            U.error(log, "Failed to apply updates for session (maximum number of retries exceeded) [sesId=" +
                sesId + ", retries=" + retries + ']', e);
        }
        else {
            U.warn(log, "Failed to apply updates for session (will retry): " + sesId);

            handleCacheOperationException(e);
        }
    }


    /**
     * Handles cache operation exception.
     * @param e Exception
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    void handleCacheOperationException(Exception e){
        IgniteFuture<?> retryFut = null;

        if (e instanceof IllegalStateException) {
            initCache();

            return;
        }
        else if (X.hasCause(e, IgniteClientDisconnectedException.class)) {
            IgniteClientDisconnectedException cause = X.cause(e, IgniteClientDisconnectedException.class);

            assert cause != null : e;

            retryFut = cause.reconnectFuture();
        }
        else if (X.hasCause(e, ClusterTopologyException.class)) {
            ClusterTopologyException cause = X.cause(e, ClusterTopologyException.class);

            assert cause != null : e;

            retryFut = cause.retryReadyFuture();
        }

        if (retryFut != null) {
            try {
                retryFut.get(retriesTimeout);
            }
            catch (IgniteException retryErr) {
                throw new IgniteException("Failed to wait for retry: " + retryErr);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebSessionFilter.class, this);
    }

    /**
     * Request wrapper.
     */
    private class RequestWrapper extends HttpServletRequestWrapper {
        /** Session. */
        private volatile WebSession ses;

        /**
         * @param req Request.
         * @param ses Session.
         */
        private RequestWrapper(HttpServletRequest req, WebSession ses) {
            super(req);

            assert ses != null;

            this.ses = ses;
        }

        /** {@inheritDoc} */
        @Override public HttpSession getSession(boolean create) {
            if (!ses.isValid()) {
                if (create) {
                    this.ses = createSession((HttpServletRequest)getRequest());
                    this.ses.servletContext(ctx);
                    this.ses.filter(WebSessionFilter.this);
                    this.ses.resetUpdates();
                }
                else
                    return null;
            }

            return ses;
        }

        /** {@inheritDoc} */
        @Override public HttpSession getSession() {
            return getSession(true);
        }

        /** {@inheritDoc} */
        @Override public String changeSessionId() {
            HttpServletRequest req = (HttpServletRequest)getRequest();

            String newId = req.changeSessionId();

            this.ses.setId(newId);

            this.ses = createSession(ses, newId);
            this.ses.servletContext(ctx);
            this.ses.filter(WebSessionFilter.this);
            this.ses.resetUpdates();

            return newId;
        }

        /** {@inheritDoc} */
        @Override public void login(String username, String password) throws ServletException{
            HttpServletRequest req = (HttpServletRequest)getRequest();

            req.login(username, password);

            String newId = req.getSession(false).getId();

            this.ses.setId(newId);

            this.ses = createSession(ses, newId);
            this.ses.servletContext(ctx);
            this.ses.filter(WebSessionFilter.this);
            this.ses.resetUpdates();
        }
    }

    /**
     * Request wrapper V2.
     */
    private class RequestWrapperV2 extends HttpServletRequestWrapper {
        /** Session. */
        private WebSessionV2 ses;

        /**
         * @param req Request.
         * @param ses Session.
         */
        private RequestWrapperV2(HttpServletRequest req, WebSessionV2 ses) {
            super(req);

            assert ses != null;

            this.ses = ses;
        }

        /** {@inheritDoc} */
        @Override public HttpSession getSession(boolean create) {
            if (!ses.isValid()) {
                binaryCache.remove(ses.id());

                if (create) {
                    try {
                        ses = createSessionV2((HttpServletRequest) getRequest());
                    }
                    catch (IOException e) {
                        throw new IgniteException(e);
                    }
                }
                else
                    return null;
            }

            return ses;
        }

        /** {@inheritDoc} */
        @Override public HttpSession getSession() {
            return getSession(true);
        }

        /** {@inheritDoc} */
        @Override public String changeSessionId() {
            final HttpServletRequest req = (HttpServletRequest) getRequest();

            final String newId = req.changeSessionId();

            if (!F.eq(newId, ses.getId())) {
                try {
                    ses = createSessionV2(ses, newId);
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }
            }

            return newId;
        }

        /** {@inheritDoc} */
        @Override public void login(String username, String password) throws ServletException{
            final HttpServletRequest req = (HttpServletRequest)getRequest();

            req.login(username, password);

            final String newId = req.getSession(false).getId();

            if (!F.eq(newId, ses.getId())) {
                try {
                    ses = createSessionV2(ses, newId);
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }
            }
        }
    }
}

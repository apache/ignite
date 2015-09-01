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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpSession;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
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

    /** Web sessions caching retry on fail parameter name (valid for ATOMIC */
    public static final String WEB_SES_MAX_RETRIES_ON_FAIL_NAME_PARAM = "IgniteWebSessionsMaximumRetriesOnFail";

    /** Default retry on fail flag value. */
    public static final int DFLT_MAX_RETRIES_ON_FAIL = 3;

    /** Cache. */
    private IgniteCache<String, WebSession> cache;

    /** Transactions. */
    private IgniteTransactions txs;

    /** Listener. */
    private WebSessionListener lsnr;

    /** Logger. */
    private IgniteLogger log;

    /** Servlet context. */
    private ServletContext ctx;

    /** Session ID transformer. */
    private IgniteClosure<String, String> sesIdTransformer;

    /** Transactions enabled flag. */
    private boolean txEnabled;

    /** {@inheritDoc} */
    @Override public void init(FilterConfig cfg) throws ServletException {
        ctx = cfg.getServletContext();

        String gridName = U.firstNotNull(
            cfg.getInitParameter(WEB_SES_NAME_PARAM),
            ctx.getInitParameter(WEB_SES_NAME_PARAM));

        String cacheName = U.firstNotNull(
            cfg.getInitParameter(WEB_SES_CACHE_NAME_PARAM),
            ctx.getInitParameter(WEB_SES_CACHE_NAME_PARAM));

        String retriesStr = U.firstNotNull(
            cfg.getInitParameter(WEB_SES_MAX_RETRIES_ON_FAIL_NAME_PARAM),
            ctx.getInitParameter(WEB_SES_MAX_RETRIES_ON_FAIL_NAME_PARAM));

        int retries;

        try {
            retries = retriesStr != null ? Integer.parseInt(retriesStr) : DFLT_MAX_RETRIES_ON_FAIL;
        }
        catch (NumberFormatException e) {
            throw new IgniteException("Maximum number of retries parameter is invalid: " + retriesStr, e);
        }

        Ignite webSesIgnite = G.ignite(gridName);

        if (webSesIgnite == null)
            throw new IgniteException("Grid for web sessions caching is not started (is it configured?): " +
                gridName);

        txs = webSesIgnite.transactions();

        log = webSesIgnite.log();

        if (webSesIgnite == null)
            throw new IgniteException("Grid for web sessions caching is not started (is it configured?): " +
                gridName);

        cache = webSesIgnite.cache(cacheName);

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

        if (log.isInfoEnabled())
            log.info("Started web sessions caching [gridName=" + gridName + ", cacheName=" + cacheName +
                ", maxRetriesOnFail=" + retries + ']');

        txEnabled = cacheCfg.getAtomicityMode() == TRANSACTIONAL;

        lsnr = new WebSessionListener(webSesIgnite, cache, retries);

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
                        sesId = doFilter0(httpReq, res, chain);

                        tx.commit();
                    }
                }
                else
                    sesId = doFilter0(httpReq, res, chain);
            }
            catch (Exception e) {
                U.error(log, "Failed to update web session: " + sesId, e);
            }
        }
        else
            chain.doFilter(req, res);
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
    private String doFilter0(HttpServletRequest httpReq, ServletResponse res, FilterChain chain) throws IOException,
        ServletException, CacheException {
        WebSession cached;

        String sesId = httpReq.getRequestedSessionId();

        if (sesId != null) {
            cached = cache.get(sesId);

            if (cached != null) {
                if (log.isDebugEnabled())
                    log.debug("Using cached session for ID: " + sesId);

                if (cached.isNew())
                    cached = new WebSession(cached, false);
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
        else {
            cached = createSession(httpReq);

            sesId = cached.getId();
        }

        assert cached != null;

        cached.servletContext(ctx);
        cached.listener(lsnr);
        cached.resetUpdates();

        httpReq = new RequestWrapper(httpReq, cached);

        chain.doFilter(httpReq, res);

        HttpSession ses = httpReq.getSession(false);

        if (ses != null && ses instanceof WebSession) {
            Collection<T2<String, Object>> updates = ((WebSession)ses).updates();

            if (updates != null)
                lsnr.updateAttributes(ses.getId(), updates, ses.getMaxInactiveInterval());
        }

        return sesId;
    }

    /**
     * @param httpReq HTTP request.
     * @return Cached session.
     */
    @SuppressWarnings("unchecked")
    private WebSession createSession(HttpServletRequest httpReq) {
        HttpSession ses = httpReq.getSession(true);

        String sesId = sesIdTransformer != null ? sesIdTransformer.apply(ses.getId()) : ses.getId();

        if (log.isDebugEnabled())
            log.debug("Session created: " + sesId);

        WebSession cached = new WebSession(ses, true);

        try {
            while (true) {
                try {
                    IgniteCache<String, WebSession> cache0;

                    if (cached.getMaxInactiveInterval() > 0) {
                        long ttl = cached.getMaxInactiveInterval() * 1000;

                        ExpiryPolicy plc = new ModifiedExpiryPolicy(new Duration(MILLISECONDS, ttl));

                        cache0 = cache.withExpiryPolicy(plc);
                    }
                    else
                        cache0 = cache;

                    WebSession old = cache0.getAndPutIfAbsent(sesId, cached);

                    if (old != null) {
                        cached = old;

                        if (cached.isNew())
                            cached = new WebSession(cached, false);
                    }

                    break;
                }
                catch (CachePartialUpdateException e) {
                    if (log.isDebugEnabled())
                        log.debug(e.getMessage());
                }
            }
        }
        catch (CacheException e) {
            throw new IgniteException("Failed to save session: " + sesId, e);
        }

        return cached;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebSessionFilter.class, this);
    }

    /**
     * Request wrapper.
     */
    private static class RequestWrapper extends HttpServletRequestWrapper {
        /** Session. */
        private final WebSession ses;

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
            return ses;
        }

        /** {@inheritDoc} */
        @Override public HttpSession getSession() {
            return ses;
        }
    }
}
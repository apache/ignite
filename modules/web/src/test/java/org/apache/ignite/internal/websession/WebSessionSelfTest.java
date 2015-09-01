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

package org.apache.ignite.internal.websession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jetbrains.annotations.Nullable;

/**
 * Tests the correctness of web sessions caching functionality.
 */
public class WebSessionSelfTest extends GridCommonAbstractTest {
    /** Port for test Jetty server. */
    private static final int TEST_JETTY_PORT = 49090;

    /** Servers count in load test. */
    private static final int SRV_CNT = 3;

    /**
     * @return Name of the cache for this test.
     */
    protected String getCacheName() {
        return "partitioned";
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleRequest() throws Exception {
        testSingleRequest("/modules/core/src/test/config/websession/example-cache.xml");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleRequestMetaInf() throws Exception {
        testSingleRequest("ignite-webapp-config.xml");
    }

    /**
     * Tests single request to a server. Checks the presence of session in cache.
     *
     * @param cfg Configuration.
     * @throws Exception If failed.
     */
    private void testSingleRequest(String cfg) throws Exception {
        Server srv = null;

        try {
            srv = startServer(TEST_JETTY_PORT, cfg, null, new SessionCreateServlet());

            URLConnection conn = new URL("http://localhost:" + TEST_JETTY_PORT + "/ignitetest/test").openConnection();

            conn.connect();

            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String sesId = rdr.readLine();

                IgniteCache<String, HttpSession> cache = G.ignite().cache(getCacheName());

                assertNotNull(cache);

                HttpSession ses = cache.get(sesId);

                assertNotNull(ses);
                assertEquals("val1", ses.getAttribute("key1"));
            }
        }
        finally {
            stopServer(srv);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestarts() throws Exception {
        final AtomicReference<String> sesIdRef = new AtomicReference<>();

        final AtomicReferenceArray<Server> srvs = new AtomicReferenceArray<>(SRV_CNT);

        for (int idx = 0; idx < SRV_CNT; idx++) {
            String cfg = "/modules/core/src/test/config/websession/spring-cache-" + (idx + 1) + ".xml";

            srvs.set(idx, startServer(
                TEST_JETTY_PORT + idx, cfg, "grid-" + (idx + 1), new RestartsTestServlet(sesIdRef)));
        }

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> restarterFut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @SuppressWarnings("BusyWait")
            @Override public Object call() throws Exception {
                Random rnd = new Random();

                for (int i = 0; i < 10; i++) {
                    int idx = -1;
                    Server srv = null;

                    while (srv == null) {
                        idx = rnd.nextInt(SRV_CNT);

                        srv = srvs.getAndSet(idx, null);
                    }

                    assert idx != -1;
                    assert srv != null;

                    stopServer(srv);

                    String cfg = "/modules/core/src/test/config/websession/spring-cache-" + (idx + 1) + ".xml";

                    srv = startServer(
                        TEST_JETTY_PORT + idx, cfg, "grid-" + (idx + 1), new RestartsTestServlet(sesIdRef));

                    assert srvs.compareAndSet(idx, null, srv);

                    Thread.sleep(100);
                }

                X.println("Stopping...");

                stop.set(true);

                return null;
            }
        }, 1, "restarter");

        Server srv = null;

        try {
            Random rnd = new Random();

            int n = 0;

            while (!stop.get()) {
                int idx = -1;

                while (srv == null) {
                    idx = rnd.nextInt(SRV_CNT);

                    srv = srvs.getAndSet(idx, null);
                }

                assert idx != -1;
                assert srv != null;

                int port = TEST_JETTY_PORT + idx;

                URLConnection conn = new URL("http://localhost:" + port + "/ignitetest/test").openConnection();

                String sesId = sesIdRef.get();

                if (sesId != null)
                    conn.addRequestProperty("Cookie", "JSESSIONID=" + sesId);

                conn.connect();

                String str;

                try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    str = rdr.readLine();
                }

                assertEquals(n, Integer.parseInt(str));

                n++;

                assert srvs.compareAndSet(idx, null, srv);

                srv = null;
            }

            X.println(">>> Made " + n + " requests.");
        }
        finally {
            restarterFut.get();

            if (srv != null)
                stopServer(srv);

            for (int i = 0; i < srvs.length(); i++)
                stopServer(srvs.get(i));
        }
    }

    /**
     * @param cfg Configuration.
     * @param gridName Grid name.
     * @param servlet Servlet.
     * @return Servlet container web context for this test.
     */
    protected WebAppContext getWebContext(@Nullable String cfg, @Nullable String gridName, HttpServlet servlet) {
        WebAppContext ctx = new WebAppContext(U.resolveIgnitePath("modules/core/src/test/webapp").getAbsolutePath(),
            "/ignitetest");

        ctx.setInitParameter("IgniteConfigurationFilePath", cfg);
        ctx.setInitParameter("IgniteWebSessionsGridName", gridName);
        ctx.setInitParameter("IgniteWebSessionsCacheName", getCacheName());
        ctx.setInitParameter("IgniteWebSessionsMaximumRetriesOnFail", "100");

        ctx.addServlet(new ServletHolder(servlet), "/*");

        return ctx;
    }

    /**
     * Starts server.
     *
     * @param port Port number.
     * @param cfg Configuration.
     * @param gridName Grid name.
     * @param servlet Servlet.
     * @return Server.
     * @throws Exception In case of error.
     */
    private Server startServer(int port, @Nullable String cfg, @Nullable String gridName, HttpServlet servlet)
        throws Exception {
        Server srv = new Server(port);

        WebAppContext ctx = getWebContext(cfg, gridName, servlet);

        srv.setHandler(ctx);

        srv.start();

        return srv;
    }

    /**
     * Stops server.
     *
     * @param srv Server.
     * @throws Exception In case of error.
     */
    private void stopServer(@Nullable Server srv) throws Exception {
        if (srv != null)
            srv.stop();
    }

    /**
     * Test servlet.
     */
    private static class SessionCreateServlet extends HttpServlet {
        /** {@inheritDoc} */
        @Override protected void doGet(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
            HttpSession ses = req.getSession(true);

            ses.setAttribute("checkCnt", 0);
            ses.setAttribute("key1", "val1");
            ses.setAttribute("key2", "val2");

            X.println(">>>", "Created session: " + ses.getId(), ">>>");

            res.getWriter().write(ses.getId());

            res.getWriter().flush();
        }
    }

    /**
     * Servlet for restarts test.
     */
    private static class RestartsTestServlet extends HttpServlet {
        /** Session ID. */
        private final AtomicReference<String> sesId;

        /**
         * @param sesId Session ID.
         */
        RestartsTestServlet(AtomicReference<String> sesId) {
            this.sesId = sesId;
        }

        /** {@inheritDoc} */
        @Override protected void doGet(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
            HttpSession ses = req.getSession(true);

            sesId.compareAndSet(null, ses.getId());

            Integer attr = (Integer)ses.getAttribute("attr");

            if (attr == null)
                attr = 0;
            else
                attr++;

            ses.setAttribute("attr", attr);

            res.getWriter().write(attr.toString());

            res.getWriter().flush();
        }
    }
}
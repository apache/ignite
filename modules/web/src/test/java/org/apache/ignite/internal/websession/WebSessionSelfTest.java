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

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.Event;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testsuites.IgniteIgnore;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

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
     * @return Keep binary flag.
     */
    protected boolean keepBinary() {
        return true;
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
    @IgniteIgnore("https://issues.apache.org/jira/browse/IGNITE-3663")
    public void testSessionRenewalDuringLogin() throws Exception {
        testSessionRenewalDuringLogin("/modules/core/src/test/config/websession/example-cache.xml");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleRequestMetaInf() throws Exception {
        testSingleRequest("ignite-webapp-config.xml");
    }

    /**
     * @throws Exception If failed.
     */
    public void testImplicitlyAttributeModification() throws Exception {
        testImplicitlyModification("ignite-webapp-config.xml");
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectRequest() throws Exception {
        testClientReconnectRequest("/modules/core/src/test/config/websession/example-cache.xml",
            "/modules/core/src/test/config/websession/example-cache2.xml",
            "/modules/core/src/test/config/websession/example-cache-client.xml");
    }

    /**
     * Tests single request to a server. Checks the presence of session in cache.
     *
     * @param srvCfg Server configuration.
     * @param clientCfg Client configuration.
     * @throws Exception If failed.
     */
    private void testClientReconnectRequest(String srvCfg, String srvCfg2, String clientCfg) throws Exception {
        Server srv = null;

        Ignite ignite = Ignition.start(srvCfg);

        try {
            srv = startServer(TEST_JETTY_PORT, clientCfg, "client", new SessionCreateServlet(keepBinary()));

            URL url = new URL("http://localhost:" + TEST_JETTY_PORT + "/ignitetest/test");

            URLConnection conn = url.openConnection();

            conn.connect();

            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String sesId = rdr.readLine();

                assertNotNull(sesId);
            }

            stopGrid(ignite.name());

            ignite = Ignition.start(srvCfg);

            conn = url.openConnection();

            conn.connect();

            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String sesId = rdr.readLine();

                assertNotNull(sesId);
            }

            Ignition.start(srvCfg2);

            stopGrid(ignite.name());

            conn = url.openConnection();

            conn.connect();

            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String sesId = rdr.readLine();

                assertNotNull(sesId);
            }
        }
        finally {
            stopServer(srv);

            stopAllGrids();
        }
    }

    /**
     * Tests implicitly modification attribute in session. Checks the presence of session in cache.
     *
     * @param cfg Configuration.
     * @throws Exception If failed.
     */
    private void testImplicitlyModification(String cfg) throws Exception {
        Server srv = null;

        try {
            srv = startServer(TEST_JETTY_PORT, cfg, null, new SessionCreateServlet(keepBinary()));

            String sesId = sendRequestAndCheckMarker("marker1", null);
            sendRequestAndCheckMarker("test_string", sesId);
            sendRequestAndCheckMarker("ignite_test_attribute", sesId);
        }
        finally {
            stopServer(srv);
        }
    }

    private String sendRequestAndCheckMarker(String reqMarker, String sesId) throws IOException, IgniteCheckedException {
        URLConnection conn = new URL("http://localhost:" + TEST_JETTY_PORT +
            "/ignitetest/test?marker=" + reqMarker).openConnection();
        conn.addRequestProperty("Cookie", "JSESSIONID=" + sesId);

        conn.connect();

        try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            sesId = rdr.readLine();

            if (!keepBinary()) {
                IgniteCache<String, HttpSession> cache = G.ignite().cache(getCacheName());

                assertNotNull(cache);

                HttpSession ses = cache.get(sesId);

                assertNotNull(ses);
                assertEquals(reqMarker, ((Profile) ses.getAttribute("profile")).getMarker());
            }
            else {
                IgniteCache<String, WebSessionEntity> cache = G.ignite().cache(getCacheName());

                assertNotNull(cache);

                WebSessionEntity ses = cache.get(sesId);

                assertNotNull(ses);

                final byte[] data = ses.attributes().get("profile");

                assertNotNull(data);

                final Marshaller marshaller = G.ignite().configuration().getMarshaller();

                assertEquals(reqMarker, marshaller.<Profile>unmarshal(data, getClass().getClassLoader()).getMarker());
            }
        }
        return sesId;
    }

    /**
     * Tests single request to a server. Checks modification attribute in cache.
     *
     * @param cfg Configuration.
     * @throws Exception If failed.
     */
    private void testSingleRequest(String cfg) throws Exception {
        Server srv = null;

        try {
            srv = startServer(TEST_JETTY_PORT, cfg, null, new SessionCreateServlet(keepBinary()));

            URLConnection conn = new URL("http://localhost:" + TEST_JETTY_PORT + "/ignitetest/test").openConnection();

            conn.connect();

            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String sesId = rdr.readLine();

                if (!keepBinary()) {
                    IgniteCache<String, HttpSession> cache = G.ignite().cache(getCacheName());

                    assertNotNull(cache);

                    HttpSession ses = cache.get(sesId);

                    assertNotNull(ses);

                    assertEquals("val1", ses.getAttribute("key1"));
                }
                else {
                    final IgniteCache<String, WebSessionEntity> cache = G.ignite().cache(getCacheName());

                    assertNotNull(cache);

                    final WebSessionEntity entity = cache.get(sesId);

                    assertNotNull(entity);

                    final byte[] data = entity.attributes().get("key1");

                    assertNotNull(data);

                    final Marshaller marshaller = G.ignite().configuration().getMarshaller();

                    final String val = marshaller.unmarshal(data, getClass().getClassLoader());

                    assertEquals("val1", val);
                }
            }
        }
        finally {
            stopServer(srv);
        }
    }

    /**
     * Tests session renewal during login. Checks modification attribute in cache.
     *
     * @param cfg Configuration.
     * @throws Exception If failed.
     */
    private void testSessionRenewalDuringLogin(String cfg) throws Exception {
        Server srv = null;
        String sesId;
        try {
            srv = startServerWithLoginService(TEST_JETTY_PORT, cfg, null, new SessionLoginServlet());

            URLConnection conn = new URL("http://localhost:" + TEST_JETTY_PORT + "/ignitetest/test").openConnection();

            conn.connect();

            String sesIdCookie1 = getSessionIdFromCookie(conn);

            X.println(">>>", "Initial session Cookie: " + sesIdCookie1, ">>>");

            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                sesId = rdr.readLine();

                if (!keepBinary()) {
                    IgniteCache<String, HttpSession> cache = G.ignite().cache(getCacheName());

                    assertNotNull(cache);

                    HttpSession ses = cache.get(sesId);

                    assertNotNull(ses);

                    assertEquals("val1", ses.getAttribute("key1"));
                }
                else {
                    final IgniteCache<String, WebSessionEntity> cache = G.ignite().cache(getCacheName());

                    assertNotNull(cache);

                    final WebSessionEntity entity = cache.get(sesId);

                    assertNotNull(entity);

                    final byte[] data = entity.attributes().get("key1");

                    assertNotNull(data);

                    final Marshaller marshaller = G.ignite().configuration().getMarshaller();

                    final String val = marshaller.unmarshal(data, getClass().getClassLoader());

                    assertEquals("val1", val);
                }
            }

            URLConnection conn2 = new URL("http://localhost:" + TEST_JETTY_PORT + "/ignitetest/login").openConnection();

            HttpURLConnection con = (HttpURLConnection) conn2;

            con.addRequestProperty("Cookie", "JSESSIONID=" + sesIdCookie1);

            con.setRequestMethod("POST");

            con.setDoOutput(true);

            String sesIdCookie2 = getSessionIdFromCookie(con);

            X.println(">>>", "Logged In session Cookie: " + sesIdCookie2, ">>>");

            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
                String sesId2 = rdr.readLine();

                if (!keepBinary()) {
                    IgniteCache<String, HttpSession> cache = G.ignite().cache(getCacheName());

                    assertNotNull(cache);

                    HttpSession ses = cache.get(sesId2);

                    assertNotNull(ses);

                    assertEquals("val1", ses.getAttribute("key1"));

                }
                else {
                    final IgniteCache<String, WebSessionEntity> cache = G.ignite().cache(getCacheName());

                    assertNotNull(cache);

                    final WebSessionEntity entity = cache.get(sesId2);

                    assertNotNull(entity);

                    final byte[] data = entity.attributes().get("key1");

                    assertNotNull(data);

                    final Marshaller marshaller = G.ignite().configuration().getMarshaller();

                    final String val = marshaller.unmarshal(data, getClass().getClassLoader());

                    assertEquals("val1", val);

                }

            }

            URLConnection conn3 = new URL("http://localhost:" + TEST_JETTY_PORT + "/ignitetest/simple").openConnection();

            conn3.addRequestProperty("Cookie", "JSESSIONID=" + sesIdCookie2);

            conn3.connect();

            String sesIdCookie3 = getSessionIdFromCookie(conn3);

            X.println(">>>", "Post Logged In session Cookie: " + sesIdCookie3, ">>>");

            assertEquals(sesIdCookie2, sesIdCookie3);

            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn3.getInputStream()))) {
                String sesId3 = rdr.readLine();

                if (!keepBinary()) {
                    IgniteCache<String, HttpSession> cache = G.ignite().cache(getCacheName());

                    HttpSession session = cache.get(sesId3);

                    assertNotNull(session);

                    assertNotNull(cache);

                    HttpSession ses = cache.get(sesId3);

                    assertNotNull(ses);

                    assertEquals("val1", ses.getAttribute("key1"));
                }
                else {
                    final IgniteCache<String, WebSessionEntity> cache = G.ignite().cache(getCacheName());

                    assertNotNull(cache);

                    final WebSessionEntity entity = cache.get(sesId3);

                    assertNotNull(entity);

                    assertNotNull(cache.get(sesId3));

                    final byte[] data = entity.attributes().get("key1");

                    assertNotNull(data);

                    final Marshaller marshaller = G.ignite().configuration().getMarshaller();

                    final String val = marshaller.unmarshal(data, getClass().getClassLoader());

                    assertEquals("val1", val);
                }
            }
        }
        finally {
            stopServerWithLoginService(srv);
        }
    }

    /**
     * Tests invalidated sessions.
     *
     * @throws Exception Exception If failed.
     */
    public void testInvalidatedSession() throws Exception {
        String invalidatedSesId;
        Server srv = null;

        try {
            srv = startServer(TEST_JETTY_PORT, "/modules/core/src/test/config/websession/example-cache.xml",
                null, new InvalidatedSessionServlet());

            Ignite ignite = G.ignite();

            URLConnection conn = new URL("http://localhost:" + TEST_JETTY_PORT + "/ignitetest/invalidated").openConnection();

            conn.connect();

            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {

                // checks if the old session object is invalidated.
                invalidatedSesId = rdr.readLine();

                assertNotNull(invalidatedSesId);

                if (!keepBinary()) {
                    IgniteCache<String, HttpSession> cache = ignite.cache(getCacheName());

                    assertNotNull(cache);

                    HttpSession invalidatedSes = cache.get(invalidatedSesId);

                    assertNull(invalidatedSes);

                    // requests to subsequent getSession() returns null.
                    String ses = rdr.readLine();

                    assertEquals("null", ses);
                }
                else {
                    IgniteCache<String, WebSessionEntity> cache = ignite.cache(getCacheName());

                    assertNotNull(cache);

                    WebSessionEntity invalidatedSes = cache.get(invalidatedSesId);

                    assertNull(invalidatedSes);

                    // requests to subsequent getSession() returns null.
                    String ses = rdr.readLine();

                    assertEquals("null", ses);
                }
            }

            // put and update.
            final CountDownLatch latch = new CountDownLatch(2);

            final IgnitePredicate<Event> putLsnr = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt != null;

                    latch.countDown();

                    return true;
                }
            };

            ignite.events().localListen(putLsnr, EVT_CACHE_OBJECT_PUT);

            // new request that creates a new session.
            conn = new URL("http://localhost:" + TEST_JETTY_PORT + "/ignitetest/valid").openConnection();

            conn.addRequestProperty("Cookie", "JSESSIONID=" + invalidatedSesId);

            conn.connect();

            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String sesId = rdr.readLine();

                assertFalse(sesId.equals("null"));

                assertTrue(latch.await(10, TimeUnit.SECONDS));

                if (!keepBinary()) {
                    IgniteCache<String, HttpSession> cache = ignite.cache(getCacheName());

                    assertNotNull(cache);

                    HttpSession ses = cache.get(sesId);

                    assertNotNull(ses);

                    assertEquals("val10", ses.getAttribute("key10"));
                }
                else {
                    IgniteCache<String, WebSessionEntity> cache = ignite.cache(getCacheName());

                    assertNotNull(cache);

                    WebSessionEntity entity = cache.get(sesId);

                    assertNotNull(entity);

                    final Marshaller marshaller = ignite.configuration().getMarshaller();

                    assertEquals("val10",
                        marshaller.unmarshal(entity.attributes().get("key10"), getClass().getClassLoader()));
                }
            }
        }
        finally {
            stopServer(srv);
        }
    }

    /**
     * Tests session id change.
     *
     * @throws Exception Exception If failed.
     */
    public void testChangeSessionId() throws Exception {
        String newWebSesId;
        Server srv = null;

        try {
            srv = startServer(TEST_JETTY_PORT, "/modules/core/src/test/config/websession/example-cache.xml",
                null, new SessionIdChangeServlet());

            Ignite ignite = G.ignite();

            URLConnection conn = new URL("http://localhost:" + TEST_JETTY_PORT + "/ignitetest/chngsesid").openConnection();

            conn.connect();

            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {

                // checks if the old session object is invalidated.
                String oldId = rdr.readLine();

                assertNotNull(oldId);

                // id from genuine session
                String newGenSesId = rdr.readLine();

                assertNotNull(newGenSesId);

                assertFalse(newGenSesId.equals(oldId));

                // id from replicated session
                newWebSesId = rdr.readLine();

                assertNotNull(newWebSesId);

                assertTrue(newGenSesId.equals(newWebSesId));

                if (!keepBinary()) {
                    IgniteCache<String, HttpSession> cache = ignite.cache(getCacheName());

                    assertNotNull(cache);

                    Thread.sleep(1000);

                    HttpSession ses = cache.get(newWebSesId);

                    assertNotNull(ses);

                    assertEquals("val1", ses.getAttribute("key1"));
                }
                else {
                    IgniteCache<String, WebSessionEntity> cache = ignite.cache(getCacheName());

                    assertNotNull(cache);

                    Thread.sleep(1000);

                    WebSessionEntity ses = cache.get(newWebSesId);

                    assertNotNull(ses);

                    final Marshaller marshaller = ignite.configuration().getMarshaller();

                    assertEquals("val1",
                        marshaller.<String>unmarshal(ses.attributes().get("key1"), getClass().getClassLoader()));
                }
            }

            conn = new URL("http://localhost:" + TEST_JETTY_PORT + "/ignitetest/simple").openConnection();

            conn.addRequestProperty("Cookie", "JSESSIONID=" + newWebSesId);

            conn.connect();

            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {

                // checks if it can be handled with the subsequent request.
                String sesId = rdr.readLine();

                assertTrue(newWebSesId.equals(sesId));

                String attr = rdr.readLine();

                assertEquals("val1", attr);

                String reqSesValid = rdr.readLine();

                assertEquals("true", reqSesValid);

                assertEquals("invalidated", rdr.readLine());
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
    protected WebAppContext getWebContext(@Nullable String cfg, @Nullable String gridName,
        boolean keepBinaryFlag, HttpServlet servlet) {
        final String path = keepBinaryFlag ? "modules/core/src/test/webapp" : "modules/web/src/test/webapp2";

        WebAppContext ctx = new WebAppContext(U.resolveIgnitePath(path).getAbsolutePath(),
            "/ignitetest");

        ctx.setInitParameter("IgniteConfigurationFilePath", cfg);
        ctx.setInitParameter("IgniteWebSessionsGridName", gridName);
        ctx.setInitParameter("IgniteWebSessionsCacheName", getCacheName());
        ctx.setInitParameter("IgniteWebSessionsMaximumRetriesOnFail", "100");
        ctx.setInitParameter("IgniteWebSessionsKeepBinary", Boolean.toString(keepBinaryFlag));

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

        WebAppContext ctx = getWebContext(cfg, gridName, keepBinary(), servlet);

        srv.setHandler(ctx);

        srv.start();

        return srv;
    }

    /**
     * Starts server with Login Service and create a realm file.
     *
     * @param port Port number.
     * @param cfg Configuration.
     * @param gridName Grid name.
     * @param servlet Servlet.
     * @return Server.
     * @throws Exception In case of error.
     */
    private Server startServerWithLoginService(int port, @Nullable String cfg, @Nullable String gridName, HttpServlet servlet)
            throws Exception {
        Server srv = new Server(port);

        WebAppContext ctx = getWebContext(cfg, gridName, keepBinary(), servlet);

        HashLoginService hashLoginService = new HashLoginService();
        hashLoginService.setName("Test Realm");
        createRealm();
        hashLoginService.setConfig("/tmp/realm.properties");
        ctx.getSecurityHandler().setLoginService(hashLoginService);

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
     * Stops server and delete realm file.
     *
     * @param srv Server.
     * @throws Exception In case of error.
     */
    private void stopServerWithLoginService(@Nullable Server srv) throws Exception{
        if (srv != null){
            srv.stop();
            File realmFile = new File("/tmp/realm.properties");
            realmFile.delete();
        }
    }

    /** Creates a realm file to store test user credentials */
    private void createRealm() throws Exception{
        File realmFile = new File("/tmp/realm.properties");
        FileWriter fileWriter = new FileWriter(realmFile);
        fileWriter.append("admin:admin");
        fileWriter.flush();
        fileWriter.close();
    }

    /**
     * Retrieves HttpSession sessionId from Cookie
     *
     * @param conn URLConnection
     * @return sesId
     */
    private String getSessionIdFromCookie(URLConnection conn) {
        String sessionCookieValue = null;
        String sesId = null;
        Map<String, List<String>> headerFields = conn.getHeaderFields();
        Set<String> headerFieldsSet = headerFields.keySet();
        Iterator<String> hearerFieldsIter = headerFieldsSet.iterator();

        while (hearerFieldsIter.hasNext()) {
            String headerFieldKey = hearerFieldsIter.next();

            if ("Set-Cookie".equalsIgnoreCase(headerFieldKey)) {
                List<String> headerFieldValue = headerFields.get(headerFieldKey);

                for (String headerValue : headerFieldValue) {
                    String[] fields = headerValue.split(";");
                    sessionCookieValue = fields[0];
                    sesId = sessionCookieValue.substring(sessionCookieValue.indexOf("=")+1,
                            sessionCookieValue.length());
                }
            }
        }

        return sesId;
    }

    /**
     * Test servlet.
     */
    private static class SessionCreateServlet extends HttpServlet {
        /** Keep binary flag. */
        private final boolean keepBinaryFlag;

        /**
         * @param keepBinaryFlag Keep binary flag.
         */
        private SessionCreateServlet(final boolean keepBinaryFlag) {
            this.keepBinaryFlag = keepBinaryFlag;
        }

        /** {@inheritDoc} */
        @Override protected void doGet(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
            HttpSession ses = req.getSession(true);

            ses.setAttribute("checkCnt", 0);
            ses.setAttribute("key1", "val1");
            ses.setAttribute("key2", "val2");
            ses.setAttribute("mkey", new TestObj("mval", keepBinaryFlag));

            Profile p = (Profile)ses.getAttribute("profile");

            if (p == null) {
                p = new Profile();
                ses.setAttribute("profile", p);
            }

            p.setMarker(req.getParameter("marker"));

            X.println(">>>", "Created session: " + ses.getId(), ">>>");

            res.getWriter().write(ses.getId());

            res.getWriter().flush();
        }
    }

    /**
     * Complex class for stored in session.
     */
    private static class Profile implements Serializable {

        /**
         * Marker string.
         */
        String marker;

        /**
         * @return marker
         */
        public String getMarker() {
            return marker;
        }

        /**
         * @param marker
         */
        public void setMarker(String marker) {
            this.marker = marker;
        }
    }

    /**
     * Test for invalidated sessions.
     */
    private static class InvalidatedSessionServlet extends HttpServlet {
        /** {@inheritDoc} */
        @Override protected void doGet(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
            HttpSession ses = req.getSession();

            assert ses != null;

            final String sesId = ses.getId();

            if (req.getPathInfo().equals("/invalidated")) {
                X.println(">>>", "Session to invalidate with id: " + sesId, ">>>");

                ses.invalidate();

                res.getWriter().println(sesId);

                // invalidates again.
                req.getSession().invalidate();
            }
            else if (req.getPathInfo().equals("/valid")) {
                X.println(">>>", "Created session: " + sesId, ">>>");

                ses.setAttribute("key10", "val10");
            }

            res.getWriter().println((req.getSession(false) == null) ? "null" : sesId);

            res.getWriter().flush();
        }
    }

    /**
     * Test session behavior on id change.
     */
    private static class SessionIdChangeServlet extends HttpServlet {
        /** {@inheritDoc} */
        @Override protected void doGet(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
            HttpSession ses = req.getSession();

            assertNotNull(ses);

            if (req.getPathInfo().equals("/chngsesid")) {

                ses.setAttribute("key1", "val1");

                X.println(">>>", "Created session: " + ses.getId(), ">>>");

                res.getWriter().println(req.getSession().getId());

                String newId = req.changeSessionId();

                // new id from genuine session.
                res.getWriter().println(newId);

                // new id from WebSession.
                res.getWriter().println(req.getSession().getId());

                res.getWriter().flush();
            }
            else if (req.getPathInfo().equals("/simple")) {
                res.getWriter().println(req.getSession().getId());

                res.getWriter().println(req.getSession().getAttribute("key1"));

                res.getWriter().println(req.isRequestedSessionIdValid());

                try {
                    req.getSession().invalidate();
                    res.getWriter().println("invalidated");
                }
                catch (Exception ignored) {
                    res.getWriter().println("failed");
                }

                res.getWriter().flush();
            }
            else
                throw new ServletException("Nonexisting path: " + req.getPathInfo());
        }
    }

    /**
     * Test session behavior on id change.
     */
    private static class SessionLoginServlet extends HttpServlet {
        /** {@inheritDoc} */
        @Override protected void doGet(HttpServletRequest req, HttpServletResponse res)
                throws ServletException, IOException {

            if (req.getPathInfo().equals("/test")) {
                HttpSession ses = req.getSession(true);
                assertNotNull(ses);
                ses.setAttribute("checkCnt", 0);
                ses.setAttribute("key1", "val1");
                ses.setAttribute("key2", "val2");
                ses.setAttribute("mkey", new TestObj());

                Profile p = (Profile) ses.getAttribute("profile");

                if (p == null) {
                    p = new Profile();
                    ses.setAttribute("profile", p);
                }

                p.setMarker(req.getParameter("marker"));

                X.println(">>>", "Request session test: " + ses.getId(), ">>>");

                res.getWriter().write(ses.getId());

                res.getWriter().flush();

            } else if (req.getPathInfo().equals("/simple")) {
                HttpSession session = req.getSession();
                X.println(">>>", "Request session simple: " + session.getId(), ">>>");

                res.getWriter().write(session.getId());

                res.getWriter().flush();
            }
        }
        /** {@inheritDoc} */
        @Override protected void doPost(HttpServletRequest req, HttpServletResponse res)
                throws ServletException, IOException {
            if (req.getPathInfo().equals("/login")) {
                try {
                    req.login("admin", "admin");
                } catch (Exception e) {
                    X.printerrln("Login failed due to exception.", e);
                }

                HttpSession session = req.getSession();

                X.println(">>>", "Logged In session: " + session.getId(), ">>>");

                res.getWriter().write(session.getId());

                res.getWriter().flush();
            }
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

    /**
     *
     */
    private static class TestObj implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String val;

        /** */
        private boolean keepBinaryFlag;

        /**
         *
         */
        public TestObj() {
        }

        /**
         * @param val Value.
         * @param keepBinaryFlag Keep binary flag.
         */
        public TestObj(final String val, final boolean keepBinaryFlag) {
            this.val = val;
            this.keepBinaryFlag = keepBinaryFlag;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(final ObjectOutput out) throws IOException {
            U.writeString(out, val);
            out.writeBoolean(keepBinaryFlag);
            System.out.println("TestObj marshalled");
        }

        /** {@inheritDoc} */
        @Override public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
            val = U.readString(in);
            keepBinaryFlag = in.readBoolean();

            // It must be unmarshalled only on client side.
            if (keepBinaryFlag)
                fail("Should not be unmarshalled");

            System.out.println("TestObj unmarshalled");
        }

        /** {@inheritDoc} */
        @Override public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final TestObj testObj = (TestObj) o;

            if (keepBinaryFlag != testObj.keepBinaryFlag) return false;
            return val != null ? val.equals(testObj.val) : testObj.val == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = val != null ? val.hashCode() : 0;
            result = 31 * result + (keepBinaryFlag ? 1 : 0);
            return result;
        }
    }
}

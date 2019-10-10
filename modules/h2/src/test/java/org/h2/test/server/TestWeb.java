/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Vector;

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.server.web.WebServlet;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.utils.AssertThrows;
import org.h2.tools.Server;
import org.h2.util.StringUtils;
import org.h2.util.Task;

/**
 * Tests the H2 Console application.
 */
public class TestWeb extends TestBase {

    private static volatile String lastUrl;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testServlet();
        testWrongParameters();
        testTools();
        testAlreadyRunning();
        testStartWebServerWithConnection();
        testServer();
        testWebApp();
        testIfExists();
    }

    private void testServlet() throws Exception {
        WebServlet servlet = new WebServlet();
        final HashMap<String, String> configMap = new HashMap<>();
        configMap.put("ifExists", "");
        configMap.put("", "");
        configMap.put("", "");
        configMap.put("", "");
        ServletConfig config = new ServletConfig() {

            @Override
            public String getServletName() {
                return "H2Console";
            }

            @Override
            public Enumeration<String> getInitParameterNames() {
                return new Vector<>(configMap.keySet()).elements();
            }

            @Override
            public String getInitParameter(String name) {
                return configMap.get(name);
            }

            @Override
            public ServletContext getServletContext() {
                return null;
            }

        };
        servlet.init(config);


        TestHttpServletRequest request = new TestHttpServletRequest();
        request.setPathInfo("/");
        TestHttpServletResponse response = new TestHttpServletResponse();
        TestServletOutputStream out = new TestServletOutputStream();
        response.setServletOutputStream(out);
        servlet.doGet(request, response);
        assertContains(out.toString(), "location.href = 'login.jsp");
        servlet.destroy();
    }

    private static void testWrongParameters() {
        new AssertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1) {
            @Override
            public void test() throws SQLException {
                Server.createPgServer("-pgPort 8182");
        }};
        new AssertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1) {
            @Override
            public void test() throws SQLException {
                Server.createTcpServer("-tcpPort 8182");
        }};
        new AssertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1) {
            @Override
            public void test() throws SQLException {
                Server.createWebServer("-webPort=8182");
        }};
    }

    private void testAlreadyRunning() throws Exception {
        Server server = Server.createWebServer(
                "-webPort", "8182", "-properties", "null");
        server.start();
        assertContains(server.getStatus(), "server running");
        Server server2 = Server.createWebServer(
                "-webPort", "8182", "-properties", "null");
        assertEquals("Not started", server2.getStatus());
        try {
            server2.start();
            fail();
        } catch (Exception e) {
            assertContains(e.toString(), "port may be in use");
            assertContains(server2.getStatus(),
                    "could not be started");
        }
        server.stop();
    }

    private void testTools() throws Exception {
        if (config.memory || config.cipher != null) {
            return;
        }
        deleteDb(getTestName());
        Connection conn = getConnection(getTestName());
        conn.createStatement().execute(
                "create table test(id int) as select 1");
        conn.close();
        Server server = new Server();
        server.setOut(new PrintStream(new ByteArrayOutputStream()));
        server.runTool("-web", "-webPort", "8182",
                "-properties", "null", "-tcp", "-tcpPort", "9101");
        try {
            String url = "http://localhost:8182";
            WebClient client;
            String result;
            client = new WebClient();
            result = client.get(url);
            client.readSessionId(result);
            result = client.get(url, "tools.jsp");
            FileUtils.delete(getBaseDir() + "/backup.zip");
            result = client.get(url, "tools.do?tool=Backup&args=-dir," +
                    getBaseDir() + ",-db," + getTestName() + ",-file," +
                    getBaseDir() + "/backup.zip");
            deleteDb(getTestName());
            assertTrue(FileUtils.exists(getBaseDir() + "/backup.zip"));
            result = client.get(url,
                    "tools.do?tool=DeleteDbFiles&args=-dir," +
                    getBaseDir() + ",-db," + getTestName());
            String fn = getBaseDir() + "/" + getTestName();
            if (config.mvStore) {
                fn += Constants.SUFFIX_MV_FILE;
            } else {
                fn += Constants.SUFFIX_PAGE_FILE;
            }
            assertFalse(FileUtils.exists(fn));
            result = client.get(url, "tools.do?tool=Restore&args=-dir," +
                    getBaseDir() + ",-db," + getTestName() +",-file," + getBaseDir() +
                    "/backup.zip");
            assertTrue(FileUtils.exists(fn));
            FileUtils.delete(getBaseDir() + "/web.h2.sql");
            FileUtils.delete(getBaseDir() + "/backup.zip");
            result = client.get(url, "tools.do?tool=Recover&args=-dir," +
                    getBaseDir() + ",-db," + getTestName());
            assertTrue(FileUtils.exists(getBaseDir() + "/" + getTestName() + ".h2.sql"));
            FileUtils.delete(getBaseDir() + "/web.h2.sql");
            result = client.get(url, "tools.do?tool=RunScript&args=-script," +
                    getBaseDir() + "/" + getTestName() + ".h2.sql,-url," +
                    getURL(getTestName(), true) +
                    ",-user," + getUser() + ",-password," + getPassword());
            FileUtils.delete(getBaseDir() + "/" + getTestName() + ".h2.sql");
            assertTrue(FileUtils.exists(fn));
            deleteDb(getTestName());
        } finally {
            server.shutdown();
        }
    }

    private void testServer() throws Exception {
        Server server = new Server();
        server.setOut(new PrintStream(new ByteArrayOutputStream()));
        server.runTool("-web", "-webPort", "8182", "-properties",
                "null", "-tcp", "-tcpPort", "9101");
        try {
            String url = "http://localhost:8182";
            WebClient client;
            String result;
            client = new WebClient();
            client.setAcceptLanguage("de-de,de;q=0.5");
            result = client.get(url);
            client.readSessionId(result);
            result = client.get(url, "login.jsp");
            assertEquals("text/html", client.getContentType());
            assertContains(result, "Einstellung");
            client.get(url, "favicon.ico");
            assertEquals("image/x-icon", client.getContentType());
            client.get(url, "ico_ok.gif");
            assertEquals("image/gif", client.getContentType());
            client.get(url, "tree.js");
            assertEquals("text/javascript", client.getContentType());
            client.get(url, "stylesheet.css");
            assertEquals("text/css", client.getContentType());
            client.get(url, "admin.do");
            try {
                client.get(url, "adminShutdown.do");
            } catch (IOException e) {
                // expected
                Thread.sleep(1000);
            }
        } finally {
            server.shutdown();
        }
        // it should be stopped now
        server = Server.createTcpServer("-tcpPort", "9101");
        server.start();
        server.stop();
    }

    private void testIfExists() throws Exception {
        Connection conn = getConnection("jdbc:h2:mem:" + getTestName(),
                getUser(), getPassword());
        Server server = new Server();
        server.setOut(new PrintStream(new ByteArrayOutputStream()));
        server.runTool("-ifExists", "-web", "-webPort", "8182",
                "-properties", "null", "-tcp", "-tcpPort", "9101");
        try {
            String url = "http://localhost:8182";
            WebClient client;
            String result;
            client = new WebClient();
            result = client.get(url);
            client.readSessionId(result);
            result = client.get(url, "login.jsp");
            result = client.get(url, "test.do?driver=org.h2.Driver" +
                    "&url=jdbc:h2:mem:" + getTestName() +
                    "&user=" + getUser() + "&password=" +
                    getPassword() + "&name=_test_");
            assertTrue(result.indexOf("Exception") < 0);
            result = client.get(url, "test.do?driver=org.h2.Driver" +
                    "&url=jdbc:h2:mem:" + getTestName() + "Wrong" +
                    "&user=" + getUser() + "&password=" +
                    getPassword() + "&name=_test_");
            assertContains(result, "Exception");
        } finally {
            server.shutdown();
            conn.close();
        }
    }

    private void testWebApp() throws Exception {
        Server server = new Server();
        server.setOut(new PrintStream(new ByteArrayOutputStream()));
        server.runTool("-web", "-webPort", "8182",
                "-properties", "null", "-tcp", "-tcpPort", "9101");
        try {
            String url = "http://localhost:8182";
            WebClient client;
            String result;
            client = new WebClient();
            result = client.get(url);
            client.readSessionId(result);
            client.get(url, "login.jsp");
            client.get(url, "adminSave.do");
            result = client.get(url, "index.do?language=de");
            result = client.get(url, "login.jsp");
            assertContains(result, "Einstellung");
            result = client.get(url, "index.do?language=en");
            result = client.get(url, "login.jsp");
            assertTrue(result.indexOf("Einstellung") < 0);
            result = client.get(url, "test.do?driver=abc" +
                    "&url=jdbc:abc:mem: " + getTestName() +
                    "&user=sa&password=sa&name=_test_");
            assertContains(result, "Exception");
            result = client.get(url, "test.do?driver=org.h2.Driver" +
                    "&url=jdbc:h2:mem:" + getTestName() +
                    "&user=sa&password=sa&name=_test_");
            assertTrue(result.indexOf("Exception") < 0);
            result = client.get(url, "login.do?driver=org.h2.Driver" +
                    "&url=jdbc:h2:mem:" + getTestName() +
                    "&user=sa&password=sa&name=_test_");
            result = client.get(url, "header.jsp");
            result = client.get(url, "query.do?sql=" +
                    "create table test(id int primary key, name varchar);" +
                    "insert into test values(1, 'Hello')");
            result = client.get(url, "query.do?sql=create sequence test_sequence");
            result = client.get(url, "query.do?sql=create schema test_schema");
            result = client.get(url, "query.do?sql=" +
                    "create view test_view as select * from test");
            result = client.get(url, "tables.do");
            result = client.get(url, "query.jsp");
            result = client.get(url, "query.do?sql=select * from test");
            assertContains(result, "Hello");
            result = client.get(url, "query.do?sql=select * from test");
            result = client.get(url, "query.do?sql=@META select * from test");
            assertContains(result, "typeName");
            result = client.get(url, "query.do?sql=delete from test");
            result = client.get(url, "query.do?sql=@LOOP 1000 " +
                    "insert into test values(?, 'Hello ' || ?/*RND*/)");
            assertContains(result, "1000 * (Prepared)");
            result = client.get(url, "query.do?sql=select * from test");
            result = client.get(url, "query.do?sql=@list select * from test");
            assertContains(result, "Row #");
            result = client.get(url, "query.do?sql=@parameter_meta " +
                    "select * from test where id = ?");
            assertContains(result, "INTEGER");
            result = client.get(url, "query.do?sql=@edit select * from test");
            assertContains(result, "editResult.do");
            result = client.get(url, "query.do?sql=" +
                    StringUtils.urlEncode("select space(100001) a, 1 b"));
            assertContains(result, "...");
            result = client.get(url, "query.do?sql=" +
                    StringUtils.urlEncode("call '<&>'"));
            assertContains(result, "&lt;&amp;&gt;");
            result = client.get(url, "query.do?sql=@HISTORY");
            result = client.get(url, "getHistory.do?id=4");
            assertContains(result, "select * from test");
            result = client.get(url, "query.do?sql=delete from test");
            // op 1 (row -1: insert, otherwise update): ok,
            // 2: delete  3: cancel,
            result = client.get(url, "editResult.do?sql=@edit " +
                    "select * from test&op=1&row=-1&r-1c1=1&r-1c2=Hello");
            assertContains(result, "1");
            assertContains(result, "Hello");
            result = client.get(url, "editResult.do?sql=@edit " +
                    "select * from test&op=1&row=1&r1c1=1&r1c2=Hallo");
            assertContains(result, "1");
            assertContains(result, "Hallo");
            result = client.get(url, "query.do?sql=select * from test");
            assertContains(result, "1");
            assertContains(result, "Hallo");
            result = client.get(url, "editResult.do?sql=@edit " +
                    "select * from test&op=2&row=1");
            result = client.get(url, "query.do?sql=select * from test");
            assertContains(result, "no rows");

            // autoComplete
            result = client.get(url, "autoCompleteList.do?query=select 'abc");
            assertContains(StringUtils.urlDecode(result), "'");
            result = client.get(url, "autoCompleteList.do?query=select 'abc''");
            assertContains(StringUtils.urlDecode(result), "'");
            result = client.get(url, "autoCompleteList.do?query=select 'abc' ");
            assertContains(StringUtils.urlDecode(result), "||");
            result = client.get(url, "autoCompleteList.do?query=select 'abc' |");
            assertContains(StringUtils.urlDecode(result), "|");
            result = client.get(url, "autoCompleteList.do?query=select 'abc' || ");
            assertContains(StringUtils.urlDecode(result), "'");
            result = client.get(url, "autoCompleteList.do?query=call timestamp '2");
            assertContains(result, "20");
            result = client.get(url, "autoCompleteList.do?query=call time '1");
            assertContains(StringUtils.urlDecode(result), "12:00:00");
            result = client.get(url, "autoCompleteList.do?query=" +
                    "call timestamp '2001-01-01 12:00:00.");
            assertContains(result, "nanoseconds");
            result = client.get(url, "autoCompleteList.do?query=" +
                    "call timestamp '2001-01-01 12:00:00.00");
            assertContains(result, "nanoseconds");
            result = client.get(url, "autoCompleteList.do?query=" +
                    "call $$ hello world");
            assertContains(StringUtils.urlDecode(result), "$$");
            result = client.get(url, "autoCompleteList.do?query=alter index ");
            assertContains(StringUtils.urlDecode(result), "character");
            result = client.get(url, "autoCompleteList.do?query=alter index idx");
            assertContains(StringUtils.urlDecode(result), "character");
            result = client.get(url, "autoCompleteList.do?query=alter index \"IDX_");
            assertContains(StringUtils.urlDecode(result), "\"");
            result = client.get(url, "autoCompleteList.do?query=alter index \"IDX_\"\"");
            assertContains(StringUtils.urlDecode(result), "\"");
            result = client.get(url, "autoCompleteList.do?query=help ");
            assertContains(result, "anything");
            result = client.get(url, "autoCompleteList.do?query=help select");
            assertContains(result, "anything");
            result = client.get(url, "autoCompleteList.do?query=call ");
            assertContains(result, "0x");
            result = client.get(url, "autoCompleteList.do?query=call 0");
            assertContains(result, ".");
            result = client.get(url, "autoCompleteList.do?query=se");
            assertContains(result, "select");
            assertContains(result, "set");
            result = client.get(url, "tables.do");
            assertContains(result, "TEST");
            result = client.get(url, "autoCompleteList.do?query=" +
                    "select * from ");
            assertContains(result, "test");
            result = client.get(url, "autoCompleteList.do?query=" +
                    "select * from test t where t.");
            assertContains(result, "id");
            result = client.get(url, "autoCompleteList.do?query=" +
                    "select id x from test te where t");
            assertContains(result, "te");
            result = client.get(url, "autoCompleteList.do?query=" +
                    "select * from test where name = '");
            assertContains(StringUtils.urlDecode(result), "'");
            result = client.get(url, "autoCompleteList.do?query=" +
                    "select * from information_schema.columns where columns.");
            assertContains(result, "column_name");

            result = client.get(url, "query.do?sql=delete from test");

            // special commands
            result = client.get(url, "query.do?sql=@autocommit_true");
            assertContains(result, "Auto commit is now ON");
            result = client.get(url, "query.do?sql=@autocommit_false");
            assertContains(result, "Auto commit is now OFF");
            result = client.get(url, "query.do?sql=@cancel");
            assertContains(result, "There is currently no running statement");
            result = client.get(url,
                    "query.do?sql=@generated insert into test(id) values(test_sequence.nextval)");
            assertContains(result, "<tr><th>ID</th></tr><tr><td>1</td></tr>");
            result = client.get(url, "query.do?sql=@maxrows 2000");
            assertContains(result, "Max rowcount is set");
            result = client.get(url, "query.do?sql=@password_hash user password");
            assertContains(result,
                    "501cf5c163c184c26e62e76d25d441979f8f25dfd7a683484995b4a43a112fdf");
            result = client.get(url, "query.do?sql=@sleep 1");
            assertContains(result, "Ok");
            result = client.get(url, "query.do?sql=@catalogs");
            assertContains(result, "PUBLIC");
            result = client.get(url,
                    "query.do?sql=@column_privileges null null null TEST null");
            assertContains(result, "PRIVILEGE");
            result = client.get(url,
                    "query.do?sql=@cross_references null null null TEST");
            assertContains(result, "PKTABLE_NAME");
            result = client.get(url,
                    "query.do?sql=@exported_keys null null null TEST");
            assertContains(result, "PKTABLE_NAME");
            result = client.get(url,
                    "query.do?sql=@imported_keys null null null TEST");
            assertContains(result, "PKTABLE_NAME");
            result = client.get(url,
                    "query.do?sql=@primary_keys null null null TEST");
            assertContains(result, "PK_NAME");
            result = client.get(url, "query.do?sql=@procedures null null null");
            assertContains(result, "PROCEDURE_NAME");
            result = client.get(url, "query.do?sql=@procedure_columns");
            assertContains(result, "PROCEDURE_NAME");
            result = client.get(url, "query.do?sql=@schemas");
            assertContains(result, "PUBLIC");
            result = client.get(url, "query.do?sql=@table_privileges");
            assertContains(result, "PRIVILEGE");
            result = client.get(url, "query.do?sql=@table_types");
            assertContains(result, "SYSTEM TABLE");
            result = client.get(url, "query.do?sql=@type_info");
            assertContains(result, "CLOB");
            result = client.get(url, "query.do?sql=@version_columns");
            assertContains(result, "PSEUDO_COLUMN");
            result = client.get(url, "query.do?sql=@attributes");
            assertContains(result, "Feature not supported: &quot;attributes&quot;");
            result = client.get(url, "query.do?sql=@super_tables");
            assertContains(result, "SUPERTABLE_NAME");
            result = client.get(url, "query.do?sql=@super_types");
            assertContains(result, "Feature not supported: &quot;superTypes&quot;");
            result = client.get(url, "query.do?sql=@prof_start");
            assertContains(result, "Ok");
            result = client.get(url, "query.do?sql=@prof_stop");
            assertContains(result, "Top Stack Trace(s)");
            result = client.get(url,
                    "query.do?sql=@best_row_identifier null null TEST");
            assertContains(result, "SCOPE");
            assertContains(result, "COLUMN_NAME");
            assertContains(result, "ID");
            result = client.get(url, "query.do?sql=@udts");
            assertContains(result, "CLASS_NAME");
            result = client.get(url, "query.do?sql=@udts null null null 1,2,3");
            assertContains(result, "CLASS_NAME");
            result = client.get(url, "query.do?sql=@LOOP 10 " +
                    "@STATEMENT insert into test values(?, 'Hello')");
            result = client.get(url, "query.do?sql=select * from test");
            assertContains(result, "8");
            result = client.get(url, "query.do?sql=@EDIT select * from test");
            assertContains(result, "editRow");

            result = client.get(url, "query.do?sql=@AUTOCOMMIT TRUE");
            result = client.get(url, "query.do?sql=@AUTOCOMMIT FALSE");
            result = client.get(url, "query.do?sql=@TRANSACTION_ISOLATION");
            result = client.get(url, "query.do?sql=@SET MAXROWS 1");
            result = client.get(url, "query.do?sql=select * from test order by id");
            result = client.get(url, "query.do?sql=@SET MAXROWS 1000");
            result = client.get(url, "query.do?sql=@TABLES");
            assertContains(result, "TEST");
            result = client.get(url, "query.do?sql=@COLUMNS null null TEST");
            assertContains(result, "ID");
            result = client.get(url, "query.do?sql=@INDEX_INFO null null TEST");
            assertContains(result, "PRIMARY");
            result = client.get(url, "query.do?sql=@CATALOG");
            assertContains(result, "PUBLIC");
            result = client.get(url, "query.do?sql=@MEMORY");
            assertContains(result, "Used");

            result = client.get(url, "query.do?sql=@INFO");
            assertContains(result, "getCatalog");

            result = client.get(url, "logout.do");
            result = client.get(url, "login.do?driver=org.h2.Driver&" +
                    "url=jdbc:h2:mem:" + getTestName() +
                    "&user=sa&password=sa&name=_test_");

            result = client.get(url, "logout.do");
            result = client.get(url, "settingRemove.do?name=_test_");

            client.get(url, "admin.do");
        } finally {
            server.shutdown();
        }
    }

    private void testStartWebServerWithConnection() throws Exception {
        String old = System.getProperty(SysProperties.H2_BROWSER);
        try {
            System.setProperty(SysProperties.H2_BROWSER,
                    "call:" + TestWeb.class.getName() + ".openBrowser");
            Server.openBrowser("testUrl");
            assertEquals("testUrl", lastUrl);
            String oldUrl = lastUrl;
            final Connection conn = getConnection(getTestName());
            Task t = new Task() {
                @Override
                public void call() throws Exception {
                    Server.startWebServer(conn, true);
                }
            };
            t.execute();
            for (int i = 0; lastUrl == oldUrl; i++) {
                if (i > 100) {
                    throw new Exception("Browser not started");
                }
                Thread.sleep(100);
            }
            String url = lastUrl;
            WebClient client = new WebClient();
            client.readSessionId(url);
            url = client.getBaseUrl(url);
            try {
                client.get(url, "logout.do");
            } catch (ConnectException e) {
                // the server stops on logout
            }
            t.get();
            conn.close();
        } finally {
            if (old != null) {
                System.setProperty(SysProperties.H2_BROWSER, old);
            } else {
                System.clearProperty(SysProperties.H2_BROWSER);
            }
        }
    }

    /**
     * This method is called via reflection.
     *
     * @param url the browser url
     */
    public static void openBrowser(String url) {
        lastUrl = url;
    }

    /**
     * A HTTP servlet request for testing.
     */
    static class TestHttpServletRequest implements HttpServletRequest {

        private String pathInfo;

        void setPathInfo(String pathInfo) {
            this.pathInfo = pathInfo;
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public Enumeration<String> getAttributeNames() {
            return new Vector<String>().elements();
        }

        @Override
        public String getCharacterEncoding() {
            return null;
        }

        @Override
        public int getContentLength() {
            return 0;
        }

        @Override
        public String getContentType() {
            return null;
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            return null;
        }

        @Override
        public String getLocalAddr() {
            return null;
        }

        @Override
        public String getLocalName() {
            return null;
        }

        @Override
        public int getLocalPort() {
            return 0;
        }

        @Override
        public Locale getLocale() {
            return null;
        }

        @Override
        public Enumeration<Locale> getLocales() {
            return null;
        }

        @Override
        public String getParameter(String name) {
            return null;
        }

        @Override
        public Map<String, String[]> getParameterMap() {
            return null;
        }

        @Override
        public Enumeration<String> getParameterNames() {
            return new Vector<String>().elements();
        }

        @Override
        public String[] getParameterValues(String name) {
            return null;
        }

        @Override
        public String getProtocol() {
            return null;
        }

        @Override
        public BufferedReader getReader() throws IOException {
            return null;
        }

        @Override
        @Deprecated
        public String getRealPath(String path) {
            return null;
        }

        @Override
        public String getRemoteAddr() {
            return null;
        }

        @Override
        public String getRemoteHost() {
            return null;
        }

        @Override
        public int getRemotePort() {
            return 0;
        }

        @Override
        public RequestDispatcher getRequestDispatcher(String name) {
            return null;
        }

        @Override
        public String getScheme() {
            return null;
        }

        @Override
        public String getServerName() {
            return null;
        }

        @Override
        public int getServerPort() {
            return 0;
        }

        @Override
        public boolean isSecure() {
            return false;
        }

        @Override
        public void removeAttribute(String name) {
            // ignore
        }

        @Override
        public void setAttribute(String name, Object value) {
            // ignore
        }

        @Override
        public void setCharacterEncoding(String encoding)
                throws UnsupportedEncodingException {
            // ignore
        }

        @Override
        public String getAuthType() {
            return null;
        }

        @Override
        public String getContextPath() {
            return null;
        }

        @Override
        public Cookie[] getCookies() {
            return null;
        }

        @Override
        public long getDateHeader(String x) {
            return 0;
        }

        @Override
        public String getHeader(String name) {
            return null;
        }

        @Override
        public Enumeration<String> getHeaderNames() {
            return null;
        }

        @Override
        public Enumeration<String> getHeaders(String name) {
            return null;
        }

        @Override
        public int getIntHeader(String name) {
            return 0;
        }

        @Override
        public String getMethod() {
            return null;
        }

        @Override
        public String getPathInfo() {
            return pathInfo;
        }

        @Override
        public String getPathTranslated() {
            return null;
        }

        @Override
        public String getQueryString() {
            return null;
        }

        @Override
        public String getRemoteUser() {
            return null;
        }

        @Override
        public String getRequestURI() {
            return null;
        }

        @Override
        public StringBuffer getRequestURL() {
            return null;
        }

        @Override
        public String getRequestedSessionId() {
            return null;
        }

        @Override
        public String getServletPath() {
            return null;
        }

        @Override
        public HttpSession getSession() {
            return null;
        }

        @Override
        public HttpSession getSession(boolean x) {
            return null;
        }

        @Override
        public Principal getUserPrincipal() {
            return null;
        }

        @Override
        public boolean isRequestedSessionIdFromCookie() {
            return false;
        }

        @Override
        public boolean isRequestedSessionIdFromURL() {
            return false;
        }

        @Override
        @Deprecated
        public boolean isRequestedSessionIdFromUrl() {
            return false;
        }

        @Override
        public boolean isRequestedSessionIdValid() {
            return false;
        }

        @Override
        public boolean isUserInRole(String x) {
            return false;
        }

        @Override
        public java.util.Collection<Part> getParts() {
            return null;
        }

        @Override
        public Part getPart(String name) {
            return null;
        }

        @Override
        public boolean authenticate(HttpServletResponse response) {
            return false;
        }

        @Override
        public void login(String username, String password) {
            // ignore
        }

        @Override
        public void logout() {
            // ignore
        }

        @Override
        public ServletContext getServletContext() {
            return null;
        }

        @Override
        public AsyncContext startAsync() {
            return null;
        }

        @Override
        public AsyncContext startAsync(
                ServletRequest servletRequest,
                ServletResponse servletResponse) {
            return null;
        }

        @Override
        public boolean isAsyncStarted() {
            return false;
        }

        @Override
        public boolean isAsyncSupported() {
            return false;
        }

        @Override
        public AsyncContext getAsyncContext() {
            return null;
        }

        @Override
        public DispatcherType getDispatcherType() {
            return null;
        }

        @Override
        public long getContentLengthLong() {
            return 0;
        }

        @Override
        public String changeSessionId() {
            return null;
        }

        @Override
        public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass)
                throws IOException, ServletException {
            return null;
        }

    }

    /**
     * A HTTP servlet response for testing.
     */
    static class TestHttpServletResponse implements HttpServletResponse {

        ServletOutputStream servletOutputStream;

        void setServletOutputStream(ServletOutputStream servletOutputStream) {
            this.servletOutputStream = servletOutputStream;
        }

        @Override
        public void flushBuffer() throws IOException {
            // ignore
        }

        @Override
        public int getBufferSize() {
            return 0;
        }

        @Override
        public String getCharacterEncoding() {
            return null;
        }

        @Override
        public String getContentType() {
            return null;
        }

        @Override
        public Locale getLocale() {
            return null;
        }

        @Override
        public ServletOutputStream getOutputStream() throws IOException {
            return servletOutputStream;
        }

        @Override
        public PrintWriter getWriter() throws IOException {
            return null;
        }

        @Override
        public boolean isCommitted() {
            return false;
        }

        @Override
        public void reset() {
            // ignore
        }

        @Override
        public void resetBuffer() {
            // ignore
        }

        @Override
        public void setBufferSize(int arg0) {
            // ignore
        }

        @Override
        public void setCharacterEncoding(String arg0) {
            // ignore
        }

        @Override
        public void setContentLength(int arg0) {
            // ignore
        }

        @Override
        public void setContentLengthLong(long arg0) {
            // ignore
        }

        @Override
        public void setContentType(String arg0) {
            // ignore
        }

        @Override
        public void setLocale(Locale arg0) {
            // ignore
        }

        @Override
        public void addCookie(Cookie arg0) {
            // ignore
        }

        @Override
        public void addDateHeader(String arg0, long arg1) {
            // ignore
        }

        @Override
        public void addHeader(String arg0, String arg1) {
            // ignore
        }

        @Override
        public void addIntHeader(String arg0, int arg1) {
            // ignore
        }

        @Override
        public boolean containsHeader(String arg0) {
            return false;
        }

        @Override
        public String encodeRedirectURL(String arg0) {
            return null;
        }

        @Override
        @Deprecated
        public String encodeRedirectUrl(String arg0) {
            return null;
        }

        @Override
        public String encodeURL(String arg0) {
            return null;
        }

        @Override
        @Deprecated
        public String encodeUrl(String arg0) {
            return null;
        }

        @Override
        public void sendError(int arg0) throws IOException {
            // ignore
        }

        @Override
        public void sendError(int arg0, String arg1) throws IOException {
            // ignore
        }

        @Override
        public void sendRedirect(String arg0) throws IOException {
            // ignore
        }

        @Override
        public void setDateHeader(String arg0, long arg1) {
            // ignore
        }

        @Override
        public void setHeader(String arg0, String arg1) {
            // ignore
        }

        @Override
        public void setIntHeader(String arg0, int arg1) {
            // ignore
        }

        @Override
        public void setStatus(int arg0) {
            // ignore
        }

        @Override
        @Deprecated
        public void setStatus(int arg0, String arg1) {
            // ignore
        }

        @Override
        public int getStatus() {
            return 0;
        }

        @Override
        public String getHeader(String name) {
            return null;
        }

        @Override
        public java.util.Collection<String> getHeaders(String name) {
            return null;
        }

        @Override
        public java.util.Collection<String> getHeaderNames() {
            return null;
        }

    }

    /**
     * A servlet output stream for testing.
     */
    static class TestServletOutputStream extends ServletOutputStream {

        private final ByteArrayOutputStream buff = new ByteArrayOutputStream();

        @Override
        public void write(int b) throws IOException {
            buff.write(b);
        }

        @Override
        public String toString() {
            return new String(buff.toByteArray(), StandardCharsets.UTF_8);
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
            // ignore
        }

    }

}

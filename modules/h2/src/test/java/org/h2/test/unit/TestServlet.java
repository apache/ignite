/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.EventListener;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.servlet.Filter;
import javax.servlet.FilterRegistration;
import javax.servlet.FilterRegistration.Dynamic;
import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;
import javax.servlet.SessionCookieConfig;
import javax.servlet.SessionTrackingMode;
import javax.servlet.descriptor.JspConfigDescriptor;
import org.h2.api.ErrorCode;
import org.h2.server.web.DbStarter;
import org.h2.test.TestBase;

/**
 * Tests the DbStarter servlet.
 * This test simulates a minimum servlet container environment.
 */
public class TestServlet extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    /**
     * Minimum ServletContext implementation.
     * Most methods are not implemented.
     */
    static class TestServletContext implements ServletContext {

        private final Properties initParams = new Properties();
        private final HashMap<String, Object> attributes = new HashMap<>();

        @Override
        public void setAttribute(String key, Object value) {
            attributes.put(key, value);
        }

        @Override
        public Object getAttribute(String key) {
            return attributes.get(key);
        }

        @Override
        public boolean setInitParameter(String key, String value) {
            initParams.setProperty(key, value);
            return true;
        }

        @Override
        public String getInitParameter(String key) {
            return initParams.getProperty(key);
        }

        @Override
        public Enumeration<String> getAttributeNames() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ServletContext getContext(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Enumeration<String> getInitParameterNames() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getMajorVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getMimeType(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getMinorVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RequestDispatcher getNamedDispatcher(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getRealPath(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RequestDispatcher getRequestDispatcher(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public URL getResource(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream getResourceAsStream(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getResourcePaths(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getServerInfo() {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated as of servlet API 2.1
         */
        @Override
        @Deprecated
        public Servlet getServlet(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getServletContextName() {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated as of servlet API 2.1
         */
        @Deprecated
        @Override
        public Enumeration<String> getServletNames() {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated as of servlet API 2.0
         */
        @Deprecated
        @Override
        public Enumeration<Servlet> getServlets() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void log(String string) {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated as of servlet API 2.1
         */
        @Deprecated
        @Override
        public void log(Exception exception, String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void log(String string, Throwable throwable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeAttribute(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Dynamic addFilter(String arg0, String arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Dynamic addFilter(String arg0, Filter arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Dynamic addFilter(String arg0, Class<? extends Filter> arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addListener(String arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends EventListener> void addListener(T arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addListener(Class<? extends EventListener> arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public javax.servlet.ServletRegistration.Dynamic addServlet(
                String arg0, String arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public javax.servlet.ServletRegistration.Dynamic addServlet(
                String arg0, Servlet arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public javax.servlet.ServletRegistration.Dynamic addServlet(
                String arg0, Class<? extends Servlet> arg1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends Filter> T createFilter(Class<T> arg0)
                throws ServletException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends EventListener> T createListener(Class<T> arg0)
                throws ServletException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends Servlet> T createServlet(Class<T> arg0)
                throws ServletException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void declareRoles(String... arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClassLoader getClassLoader() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getContextPath() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<SessionTrackingMode> getDefaultSessionTrackingModes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getEffectiveMajorVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getEffectiveMinorVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<SessionTrackingMode> getEffectiveSessionTrackingModes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public FilterRegistration getFilterRegistration(String arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, ? extends FilterRegistration> getFilterRegistrations() {
            throw new UnsupportedOperationException();
        }

        @Override
        public JspConfigDescriptor getJspConfigDescriptor() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ServletRegistration getServletRegistration(String arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, ? extends ServletRegistration> getServletRegistrations() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SessionCookieConfig getSessionCookieConfig() {
            throw new UnsupportedOperationException();
        }


        @Override
        public void setSessionTrackingModes(Set<SessionTrackingMode> arg0) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getVirtualServerName() {
            throw new UnsupportedOperationException();
        }

    }

    @Override
    public void test() throws SQLException {
        if (config.networked || config.memory) {
            return;
        }
        DbStarter listener = new DbStarter();

        TestServletContext context = new TestServletContext();
        String url = getURL("servlet", true);
        context.setInitParameter("db.url", url);
        context.setInitParameter("db.user", getUser());
        context.setInitParameter("db.password", getPassword());
        context.setInitParameter("db.tcpServer", "-tcpPort 8888");

        ServletContextEvent event = new ServletContextEvent(context);
        listener.contextInitialized(event);

        Connection conn1 = listener.getConnection();
        Connection conn1a = (Connection) context.getAttribute("connection");
        assertTrue(conn1 == conn1a);
        Statement stat1 = conn1.createStatement();
        stat1.execute("CREATE TABLE T(ID INT)");

        String u2 = url.substring(url.indexOf("servlet"));
        u2 = "jdbc:h2:tcp://localhost:8888/" + getBaseDir() + "/" + u2;
        Connection conn2 = DriverManager.getConnection(
                u2, getUser(), getPassword());
        Statement stat2 = conn2.createStatement();
        stat2.execute("SELECT * FROM T");
        stat2.execute("DROP TABLE T");

        assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, stat1).
                execute("SELECT * FROM T");
        conn2.close();

        listener.contextDestroyed(event);

        // listener must be stopped
        assertThrows(ErrorCode.CONNECTION_BROKEN_1, this).getConnection(
                "jdbc:h2:tcp://localhost:8888/" + getBaseDir() + "/servlet",
                getUser(), getPassword());

        // connection must be closed
        assertThrows(ErrorCode.OBJECT_CLOSED, stat1).
                execute("SELECT * FROM DUAL");

        deleteDb("servlet");

    }

}

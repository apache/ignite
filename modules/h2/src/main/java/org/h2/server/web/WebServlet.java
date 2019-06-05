/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.h2.util.New;

/**
 * This servlet lets the H2 Console be used in a standard servlet container
 * such as Tomcat or Jetty.
 */
public class WebServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private transient WebServer server;

    @Override
    public void init() {
        ServletConfig config = getServletConfig();
        Enumeration<?> en = config.getInitParameterNames();
        ArrayList<String> list = New.arrayList();
        while (en.hasMoreElements()) {
            String name = en.nextElement().toString();
            String value = config.getInitParameter(name);
            if (!name.startsWith("-")) {
                name = "-" + name;
            }
            list.add(name);
            if (value.length() > 0) {
                list.add(value);
            }
        }
        String[] args = list.toArray(new String[0]);
        server = new WebServer();
        server.setAllowChunked(false);
        server.init(args);
    }

    @Override
    public void destroy() {
        server.stop();
    }

    private boolean allow(HttpServletRequest req) {
        if (server.getAllowOthers()) {
            return true;
        }
        String addr = req.getRemoteAddr();
        try {
            InetAddress address = InetAddress.getByName(addr);
            return address.isLoopbackAddress();
        } catch (UnknownHostException e) {
            return false;
        } catch (NoClassDefFoundError e) {
            // Google App Engine does not allow java.net.InetAddress
            return false;
        }
    }

    private String getAllowedFile(HttpServletRequest req, String requestedFile) {
        if (!allow(req)) {
            return "notAllowed.jsp";
        }
        if (requestedFile.length() == 0) {
            return "index.do";
        }
        return requestedFile;
    }

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        req.setCharacterEncoding("utf-8");
        String file = req.getPathInfo();
        if (file == null) {
            resp.sendRedirect(req.getRequestURI() + "/");
            return;
        } else if (file.startsWith("/")) {
            file = file.substring(1);
        }
        file = getAllowedFile(req, file);

        // extract the request attributes
        Properties attributes = new Properties();
        Enumeration<?> en = req.getAttributeNames();
        while (en.hasMoreElements()) {
            String name = en.nextElement().toString();
            String value = req.getAttribute(name).toString();
            attributes.put(name, value);
        }
        en = req.getParameterNames();
        while (en.hasMoreElements()) {
            String name = en.nextElement().toString();
            String value = req.getParameter(name);
            attributes.put(name, value);
        }

        WebSession session = null;
        String sessionId = attributes.getProperty("jsessionid");
        if (sessionId != null) {
            session = server.getSession(sessionId);
        }
        WebApp app = new WebApp(server);
        app.setSession(session, attributes);
        String ifModifiedSince = req.getHeader("if-modified-since");

        String hostAddr = req.getRemoteAddr();
        file = app.processRequest(file, hostAddr);
        session = app.getSession();

        String mimeType = app.getMimeType();
        boolean cache = app.getCache();

        if (cache && server.getStartDateTime().equals(ifModifiedSince)) {
            resp.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            return;
        }
        byte[] bytes = server.getFile(file);
        if (bytes == null) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            bytes = ("File not found: " + file).getBytes(StandardCharsets.UTF_8);
        } else {
            if (session != null && file.endsWith(".jsp")) {
                String page = new String(bytes, StandardCharsets.UTF_8);
                page = PageParser.parse(page, session.map);
                bytes = page.getBytes(StandardCharsets.UTF_8);
            }
            resp.setContentType(mimeType);
            if (!cache) {
                resp.setHeader("Cache-Control", "no-cache");
            } else {
                resp.setHeader("Cache-Control", "max-age=10");
                resp.setHeader("Last-Modified", server.getStartDateTime());
            }
        }
        if (bytes != null) {
            ServletOutputStream out = resp.getOutputStream();
            out.write(bytes);
        }
    }

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        doGet(req, resp);
    }

}

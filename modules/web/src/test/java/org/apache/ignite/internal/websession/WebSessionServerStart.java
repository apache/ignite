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

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * Server starter for web sessions caching test.
 */
public class WebSessionServerStart {
    /**
     * @param args Arguments.
     * @throws Exception In case of error.
     */
    public static void main(String[] args) throws Exception {
        Server srv = jettyServer(Integer.valueOf(args[0]), Boolean.valueOf(args[1]) ?
            new SessionCheckServlet() : new SessionCreateServlet());

        srv.start();
        srv.join();
    }

    /**
     * @param port Port.
     * @param servlet Servlet.
     * @return Started Jetty server.
     * @throws Exception In case of error.
     */
    private static Server jettyServer(int port, HttpServlet servlet) throws Exception {
        Server srv = new Server(port);

        WebAppContext ctx = new WebAppContext(U.resolveIgnitePath("modules/tests/webapp").getAbsolutePath(),
            "/ignitetest");

        ctx.setInitParameter("cfgFilePath", "/examples/config/spring-cache.xml");
        ctx.setInitParameter("IgniteWebSessionsCacheName", "partitioned");

        ctx.addServlet(new ServletHolder(servlet), "/*");

        srv.setHandler(ctx);

        return srv;
    }

    /**
     * Servlet.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class SessionCreateServlet extends HttpServlet {
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
     * Servlet.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class SessionCheckServlet extends HttpServlet {
        /** {@inheritDoc} */
        @Override protected void doGet(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
            HttpSession ses = req.getSession(false);

            assert ses != null;

            X.println(">>>", "Checking session: " + ses.getId(), ">>>");

            Integer checkCnt = (Integer)ses.getAttribute("checkCnt");

            if (checkCnt == null) {
                assert ses.getAttribute("key1") == null;
                assert ses.getAttribute("key2") == null;
                assert ses.getAttribute("key3") == null;
            }
            else if (checkCnt == 0) {
                assert "val1".equals(ses.getAttribute("key1"));
                assert "val2".equals(ses.getAttribute("key2"));

                ses.removeAttribute("key1");
                ses.setAttribute("key2", "val20");
                ses.setAttribute("key3", "val3");

                ses.setAttribute("checkCnt", 1);
            }
            else if (checkCnt == 1) {
                assert ses.getAttribute("key1") == null;
                assert "val20".equals(ses.getAttribute("key2"));
                assert "val3".equals(ses.getAttribute("key3"));

                ses.invalidate();
            }
        }
    }
}
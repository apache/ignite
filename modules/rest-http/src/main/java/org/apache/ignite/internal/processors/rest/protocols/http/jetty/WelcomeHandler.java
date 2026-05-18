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

import java.io.IOException;
import java.io.InputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.ignite.IgniteLogger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 * Handles welcome page.
 */
public class WelcomeHandler extends AbstractHandler {
    /** Default page. */
    private final byte[] dfltPage;

    /** Favicon. */
    private final byte[] favicon;

    /** Logo. */
    private final byte[] logo;

    /** */
    private final IgniteLogger log;

    /** */
    public WelcomeHandler(IgniteLogger log) {
        this.log = log;

        favicon = loadResource("ignite-rest-http/favicon.ico");
        dfltPage = loadResource("ignite-rest-http/rest.html");
        logo = loadResource("ignite-rest-http/logo.svg");
    }

    /** {@inheritDoc} */
    @Override public void handle(String target, Request req, HttpServletRequest srvReq, HttpServletResponse res) throws IOException {
        if (dfltPage == null || favicon == null || logo == null) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            req.setHandled(true);

            return;
        }

        if (target.startsWith("/favicon.ico")) {
            res.setContentType("image/x-icon");
            res.getOutputStream().write(favicon);
        }
        else if (target.startsWith("/logo.svg")) {
            res.setContentType("image/svg+xml");
            res.getOutputStream().write(logo);
        }
        else {
            res.setContentType("text/html; charset=utf-8");
            res.getOutputStream().write(dfltPage);
        }

        res.getOutputStream().flush();

        res.setStatus(HttpServletResponse.SC_OK);
        req.setHandled(true);
    }

    /** */
    private byte[] loadResource(String path) {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(path)) {
            return in != null ? in.readAllBytes() : null;
        }
        catch (IOException e) {
            log.error("Failed to load REST resource [path=" + path + ']', e);

            return null;
        }
    }
}

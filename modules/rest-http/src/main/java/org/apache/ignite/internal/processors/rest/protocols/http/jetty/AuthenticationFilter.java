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
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.rest.GridRestProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyRestHandler.sessionToken;

/**
 * Servlet filter that authenticates REST requests via session token.
 */
public class AuthenticationFilter implements Filter {
    /** */
    private final GridKernalContext ctx;

    /** */
    public AuthenticationFilter(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void doFilter(
        ServletRequest req,
        ServletResponse res,
        FilterChain chain
    ) throws IOException, ServletException {
        SecurityContext secCtx = resolveSession((HttpServletRequest)req);

        if (secCtx == null) {
            ((HttpServletResponse)res).sendError(HttpServletResponse.SC_UNAUTHORIZED,
                "Missing or invalid authentication token (maybe expired session)");

            return;
        }

        try (Scope ignored = ctx.security().withContext(secCtx)) {
            chain.doFilter(req, res);
        }
    }

    /** @return Security context for given session token, or {@code null} if none found. */
    @Nullable private SecurityContext resolveSession(HttpServletRequest req) {
        byte[] token = sessionToken(req.getParameter("sessionToken"));

        if (token == null)
            return null;

        return ((GridRestProcessor)ctx.rest()).securityContext(U.bytesToUuid(token, 0));
    }
}

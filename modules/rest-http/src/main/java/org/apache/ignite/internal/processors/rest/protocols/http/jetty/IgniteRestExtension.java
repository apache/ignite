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

import org.eclipse.jetty.ee11.servlet.ServletContextHandler;

/**
 * Extension point for registering custom HTTP REST endpoints in the Jetty REST protocol.
 * <p>
 * Each extension is configured within an isolated {@link ServletContextHandler} instance
 * managed by the Ignite REST subsystem.
 * Extensions are discovered using Java {@link java.util.ServiceLoader}.
 * <p>
 * Implementations are responsible for:
 * <ul>
 *     <li>Configuring the servlet context path via
 *     {@link ServletContextHandler#setContextPath(String)}.</li>
 *     <li>Registering servlets, filters, and related HTTP components.</li>
 * </ul>
 * <p>
 * Context paths must be unique across all registered extensions.
 * <p>
 * Authentication and other common infrastructure are configured by Ignite.
 */
public interface IgniteRestExtension {
    /**
     * Configures the REST extension.
     *
     * @param ctx Servlet context handler dedicated to this extension.
     * @throws Exception If configuration failed.
     */
    void configure(ServletContextHandler ctx) throws Exception;
}

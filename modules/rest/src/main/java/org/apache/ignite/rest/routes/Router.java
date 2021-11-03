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

package org.apache.ignite.rest.routes;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.AsciiString;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.ignite.rest.netty.RestApiHttpRequest;
import org.apache.ignite.rest.netty.RestApiHttpResponse;

/**
 * Dispatcher of http requests.
 *
 * <p>Example:
 * <pre>
 * {@code
 * var router = new Router();
 * router.get("/user", (req, resp) -> {
 *     resp.status(HttpResponseStatus.OK);
 * });
 * }
 * </pre>
 */
public class Router {
    /** Routes. */
    private final List<Route> routes;

    /**
     * Creates a new router with the given list of {@code routes}.
     *
     * @param routes Routes.
     */
    public Router(List<Route> routes) {
        this.routes = routes;
    }

    /**
     * Creates a new empty router.
     */
    public Router() {
        routes = new ArrayList<>();
    }

    /**
     * GET query helper.
     *
     * @param route      Route.
     * @param acceptType Accept type.
     * @param hnd        Actual handler of the request.
     * @return Router
     */
    public Router get(
            String route,
            AsciiString acceptType,
            BiConsumer<RestApiHttpRequest, RestApiHttpResponse> hnd
    ) {
        addRoute(new Route(route, HttpMethod.GET, acceptType.toString(), hnd));
        return this;
    }

    /**
     * GET query helper.
     *
     * @param route Route.
     * @param hnd   Actual handler of the request.
     * @return Router
     */
    public Router get(String route, BiConsumer<RestApiHttpRequest, RestApiHttpResponse> hnd) {
        addRoute(new Route(route, HttpMethod.GET, null, hnd));
        return this;
    }

    /**
     * PUT query helper.
     *
     * @param route      Route.
     * @param acceptType Accept type.
     * @param hnd        Actual handler of the request.
     * @return Router
     */
    public Router put(
            String route,
            AsciiString acceptType,
            BiConsumer<RestApiHttpRequest, RestApiHttpResponse> hnd
    ) {
        addRoute(new Route(route, HttpMethod.PUT, acceptType.toString(), hnd));
        return this;
    }

    /**
     * Adds the route to router chain.
     *
     * @param route Route
     */
    public void addRoute(Route route) {
        routes.add(route);
    }

    /**
     * Finds the route by request.
     *
     * @param req Request.
     * @return Route if founded.
     */
    public Optional<Route> route(HttpRequest req) {
        return routes.stream().filter(r -> r.match(req)).findFirst();
    }
}

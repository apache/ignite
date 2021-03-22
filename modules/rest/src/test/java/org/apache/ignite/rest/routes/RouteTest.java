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

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import org.junit.jupiter.api.Test;

import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class RouteTest {
    /**
     *
     */
    @Test
    void testMatchByUri() {
       var route = new Route("/user", GET, null, (request, response) -> {});
       var req = new DefaultHttpRequest(HTTP_1_1, GET, "/user");
       assertTrue(route.match(req));
    }

    /**
     *
     */
    @Test
    void testNonMatchByUri() {
        var route = new Route("/user", GET, null, (request, response) -> {});
        var req = new DefaultHttpRequest(HTTP_1_1, GET, "/user/1");
        assertFalse(route.match(req));
    }

    /**
     *
     */
    @Test
    void testMatchByContentTypeIfAcceptTypeEmpty() {
        var route = new Route("/user", GET, null, (request, response) -> {});
        var req = new DefaultHttpRequest(HTTP_1_1, GET, "/user/");
        req.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
        assertTrue(route.match(req));
    }

    /**
     *
     */
    @Test
    void testMatchByContentTypeIfAcceptTypeNonEmpty() {
        var route = new Route("/user", PUT, "text/plain", (request, response) -> {});
        var req = new DefaultHttpRequest(HTTP_1_1, PUT, "/user");
        req.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
        assertTrue(route.match(req));
    }

    /**
     *
     */
    @Test
    void testNonMatchByContentTypeIfAcceptTypeNonEmpty() {
        var route = new Route("/user", PUT, "text/plain", (request, response) -> {});
        var req = new DefaultHttpRequest(HTTP_1_1, GET, "/user/");
        req.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        assertFalse(route.match(req));
    }

    /**
     *
     */
    @Test
    void testMatchByUriWithParams() {
        var route = new Route("/user/:user", GET, null, (request, response) -> {});
        var req = new DefaultHttpRequest(HTTP_1_1, GET, "/user/John");
        assertTrue(route.match(req));
    }
}

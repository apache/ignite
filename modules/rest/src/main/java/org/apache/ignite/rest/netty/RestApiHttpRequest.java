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

package org.apache.ignite.rest.netty;

import io.netty.handler.codec.http.FullHttpRequest;
import java.util.Collections;
import java.util.Map;

/**
 * HTTP request wrapper with GET query params if exists.
 */
public class RestApiHttpRequest {
    /** Request. */
    private final FullHttpRequest req;

    /** Query params. */
    private final Map<String, String> qryParams;

    /**
     * Creates a new instance of http request.
     *
     * @param req       Request.
     * @param qryParams Query params.
     */
    public RestApiHttpRequest(FullHttpRequest req, Map<String, String> qryParams) {
        this.req = req;
        this.qryParams = Collections.unmodifiableMap(qryParams);
    }

    /**
     * Returns complete HTTP request.
     *
     * @return Complete HTTP request.
     */
    public FullHttpRequest request() {
        return req;
    }

    /**
     * Returns query parameters associated with the request.
     *
     * @return Query parameters.
     */
    public Map<String, String> queryParams() {
        return qryParams;
    }
}

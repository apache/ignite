/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;

/**
 * Command protocol handler.
 */
public interface GridRestProtocolHandler {
    /**
     * @param req Request.
     * @return Response.
     * @throws IgniteCheckedException In case of error.
     */
    public GridRestResponse handle(GridRestRequest req) throws IgniteCheckedException;

    /**
     * @param req Request.
     * @return Future.
     */
    public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req);
}
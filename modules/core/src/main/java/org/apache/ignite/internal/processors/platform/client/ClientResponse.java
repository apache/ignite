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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.internal.processors.odbc.SqlListenerResponse;
import org.jetbrains.annotations.Nullable;

/**
 * Thin client response.
 */
class ClientResponse extends SqlListenerResponse {
    /** Request id. */
    private final int requestId;

    /**
     * Ctor.
     *
     * @param requestId Request id.
     */
    ClientResponse(int requestId) {
        super(STATUS_SUCCESS, null);

        this.requestId = requestId;
    }

    /**
     * Encodes the response data.
     */
    public void encode(BinaryRawWriter writer) {
        writer.writeInt(requestId);
        writer.writeByte((byte)0);  // Flags (compression, etc);
    }
}

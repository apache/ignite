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

import org.apache.ignite.internal.processors.odbc.SqlListenerResponse;
import org.jetbrains.annotations.Nullable;

/**
 * Platform thin client response.
 */
public class PlatformResponse extends SqlListenerResponse {
    /** Response data. */
    private final byte[] data;

    /**
     * Constructs failed rest response.
     *
     * @param status Response status.
     * @param err    Error, {@code null} if success is {@code true}.
     */
    PlatformResponse(int status, @Nullable String err, byte[] data) {
        super(status, err);

        assert data != null;

        this.data = data;
    }

    /**
     * Gets the data.
     *
     * @return Response data.
     */
    public byte[] getData() {
        return data;
    }
}

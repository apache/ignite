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

package org.apache.ignite.internal;

import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** */
public class IgniteDiagnosticResponse implements Message {
    /** */
    @Order(value = 0, method = "futureId")
    private long futId;

    /** */
    @Order(value = 1, method = "responseInfo")
    private @Nullable String respInfo;

    /**
     * Default constructor required by {@link GridIoMessageFactory}.
     */
    public IgniteDiagnosticResponse() {
        // No-op.
    }

    /**
     * Creates a diagnostic response.
     *
     * @param futId Future ID.
     * @param resp Diagnostic info result.
     */
    public IgniteDiagnosticResponse(long futId, @Nullable String resp) {
        this.futId = futId;
        respInfo = resp;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /** */
    public void futureId(long futId) {
        this.futId = futId;
    }

    /** */
    public @Nullable String responseInfo() {
        return respInfo;
    }

    /** */
    public void responseInfo(@Nullable String respInfo) {
        this.respInfo = respInfo;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -62;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDiagnosticResponse.class, this);
    }
}

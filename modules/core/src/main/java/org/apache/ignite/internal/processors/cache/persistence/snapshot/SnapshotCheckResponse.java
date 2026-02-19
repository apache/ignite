/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** A DTO to transfer node's results for the both phases. */
public final class SnapshotCheckResponse<T extends Message> implements Message {
    /** The result. Is usually a collection or a map of hashes, metas, etc. */
    @Order(0)
    private Message result;

    /** Exceptions per consistent id. */
    @Order(1)
    @Nullable private Map<String, ErrorMessage> errors;

    /** Default constructor for {@link GridIoMessageFactory}. */
    public SnapshotCheckResponse() {
        // No-op.
    }

    /** */
    public SnapshotCheckResponse(T result, @Nullable Map<String, Throwable> exceptions) {
        assert exceptions == null || exceptions instanceof Serializable : "Snapshot check exceptions aren't serializable.";

        this.result = result;
        this.errors = exceptions == null ? null : F.viewReadOnly(exceptions, ErrorMessage::new);
    }

    /** @return Exceptions per snapshot part's consistent id. */
    public @Nullable Map<String, Throwable> exceptions() {
        return F.viewReadOnly(errors, errMsg -> ErrorMessage.error(errMsg));
    }

    /** @return Certain phase's and process' result. */
    public T result() {
        return (T)result;
    }

    /** @param result Certain phase's and process' result. */
    public void result(T result) {
        this.result = result;
    }

    /** */
    public @Nullable Map<String, ErrorMessage> errors() {
        return errors;
    }

    /** */
    public void errors(@Nullable Map<String, ErrorMessage> errors) {
        this.errors = errors;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 520;
    }
}

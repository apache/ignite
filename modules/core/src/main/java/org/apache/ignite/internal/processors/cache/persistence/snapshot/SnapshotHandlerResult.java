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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Result of local processing on the node. In addition to the result received from the handler, it also includes
 * information about the error (if any) and the node on which this result was received.
 *
 * @param <T> Type of the local processing result.
 */
public class SnapshotHandlerResult<T extends Message> implements Message {
    /** Result of local processing. */
    @Order(0)
    private Message data;

    /** Processing error. */
    @Order(value = 1, method = "errorMessage")
    private ErrorMessage errMsg;

    /** Default constructor for {@link GridIoMessageFactory}. */
    public SnapshotHandlerResult() {
        // No-op.
    }

    /**
     * @param data Result of local processing.
     * @param err Processing error.
     */
    public SnapshotHandlerResult(@Nullable T data, @Nullable Exception err) {
        this.data = data;

        if (err != null)
            errMsg = new ErrorMessage(err);
    }

    /** @return Result of local processing. */
    public @Nullable T data() {
        return (T)data;
    }

    /** @param data Result of local processing. */
    public void data(T data) {
        this.data = data;
    }

    /** @return Processing error. */
    public @Nullable Exception error() {
        return (Exception)ErrorMessage.error(errMsg);
    }

    /** */
    public @Nullable ErrorMessage errorMessage() {
        return errMsg;
    }

    /** */
    public void errorMessage(@Nullable ErrorMessage errMsg) {
        this.errMsg = errMsg;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 518;
    }
}

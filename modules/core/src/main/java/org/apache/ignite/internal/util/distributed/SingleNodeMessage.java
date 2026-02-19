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

package org.apache.ignite.internal.util.distributed;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Single node result message.
 *
 * @param <R> Result type.
 * @see DistributedProcess
 * @see FullMessage
 * @see InitMessage
 */
public class SingleNodeMessage<R extends Message> implements Message {
    /** Initial channel message type (value is {@code 176}). */
    public static final short TYPE_CODE = 176;

    /** Process id. */
    @Order(0)
    private UUID processId;

    /** Process type. */
    @Order(1)
    private int type;

    /** Single node response. */
    @Order(value = 2, method = "response")
    private Message resp;

    /** Error. */
    @Order(value = 3, method = "errorMessage")
    private @Nullable ErrorMessage errMsg;

    /** Default constructor for {@link GridIoMessageFactory}. */
    public SingleNodeMessage() {
    }

    /**
     * @param processId Process id.
     * @param type Process type.
     * @param resp Single node response.
     * @param err Error.
     */
    public SingleNodeMessage(UUID processId, DistributedProcessType type, R resp, Throwable err) {
        this.processId = processId;
        this.type = type.ordinal();
        this.resp = resp;

        if (err != null)
            errMsg = new ErrorMessage(err);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** @return Process id. */
    public UUID processId() {
        return processId;
    }

    /** @param processId Process id. */
    public void processId(UUID processId) {
        this.processId = processId;
    }

    /** @return Process type. */
    public int type() {
        return type;
    }

    /** @param type Process type. */
    public void type(int type) {
        this.type = type;
    }

    /** @return Response. */
    public R response() {
        return (R)resp;
    }

    /** @param resp Response. */
    public void response(R resp) {
        this.resp = resp;
    }

    /** @return {@code True} if finished with error. */
    public boolean hasError() {
        return errMsg != null;
    }

    /** @return Error. */
    @Nullable public Throwable error() {
        return ErrorMessage.error(errMsg);
    }

    /** */
    public @Nullable ErrorMessage errorMessage() {
        return errMsg;
    }

    /** */
    public void errorMessage(@Nullable ErrorMessage errMsg) {
        this.errMsg = errMsg;
    }
}

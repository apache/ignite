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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class UpdateErrors implements Message {
    /** Failed keys. */
    @GridToStringInclude
    @Order(0)
    List<KeyCacheObject> failedKeys;

    /** Error message. */
    @Order(value = 1, method = "errorMessage")
    ErrorMessage errMsg;

    /**
     *
     */
    public UpdateErrors() {
        // No-op.
    }

    /**
     * @param err Error.
     */
    public UpdateErrors(Throwable err) {
        assert err != null;

        errMsg = new ErrorMessage(err);
    }

    /**
     * @param err Error.
     */
    public void error(Throwable err) {
        errMsg = new ErrorMessage(err);
    }

    /**
     * @return Error.
     */
    public Throwable error() {
        return ErrorMessage.error(errMsg);
    }

    /**
     * @param errMsg New error message.
     */
    public void errorMessage(ErrorMessage errMsg) {
        this.errMsg = errMsg;
    }

    /**
     * @return Error message.
     */
    public ErrorMessage errorMessage() {
        return errMsg;
    }

    /**
     * @return Failed keys.
     */
    public Collection<KeyCacheObject> failedKeys() {
        return failedKeys;
    }

    /**
     * @param failedKeys New failed keys.
     */
    public void failedKeys(List<KeyCacheObject> failedKeys) {
        this.failedKeys = failedKeys;
    }

    /**
     * Adds key to collection of failed keys.
     *
     * @param key Key to add.
     * @param e Error cause.
     */
    void addFailedKey(KeyCacheObject key, Throwable e) {
        if (failedKeys == null)
            failedKeys = new ArrayList<>();

        failedKeys.add(key);

        if (errMsg == null)
            errMsg = new ErrorMessage(new IgniteCheckedException("Failed to update keys."));

        errMsg.error().addSuppressed(e);
    }

    /**
     * @param keys Keys.
     * @param e Error.
     */
    void addFailedKeys(Collection<KeyCacheObject> keys, Throwable e) {
        if (failedKeys == null)
            failedKeys = new ArrayList<>(keys.size());

        failedKeys.addAll(keys);

        if (errMsg == null)
            errMsg = new ErrorMessage(new IgniteCheckedException("Failed to update keys on primary node."));

        errMsg.error().addSuppressed(e);
    }

    /** */
    void prepareMarshal(GridCacheMessage msg, GridCacheContext<?, ?> cctx) throws IgniteCheckedException {
        msg.prepareMarshalCacheObjects(failedKeys, cctx);
    }

    /** */
    void finishUnmarshal(GridCacheMessage msg, GridCacheContext<?, ?> cctx, ClassLoader ldr) throws IgniteCheckedException {
        msg.finishUnmarshalCacheObjects(failedKeys, cctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -49;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UpdateErrors.class, this);
    }
}

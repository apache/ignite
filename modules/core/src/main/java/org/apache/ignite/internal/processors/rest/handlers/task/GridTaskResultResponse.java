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

package org.apache.ignite.internal.processors.rest.handlers.task;

import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Task result response.
 */
public class GridTaskResultResponse implements Message {
    /** Result. */
    private Object res;

    /** Serialized result. */
    @Order(value = 0, method = "resultBytes")
    private byte[] resBytes;

    /** Finished flag. */
    @Order(1)
    private boolean finished;

    /** Flag indicating that task has ever been launched on node. */
    @Order(2)
    private boolean found;

    /** Error. */
    @Order(value = 3, method = "error")
    private String err;

    /**
     * @return Task result.
     */
    @Nullable public Object result() {
        return res;
    }

    /**
     * @param res Task result.
     */
    public void result(@Nullable Object res) {
        this.res = res;
    }

    /**
     * @param resBytes Serialized result.
     */
    public void resultBytes(byte[] resBytes) {
        this.resBytes = resBytes;
    }

    /**
     * @return Serialized result.
     */
    public byte[] resultBytes() {
        return resBytes;
    }

    /**
     * @return {@code true} if finished.
     */
    public boolean finished() {
        return finished;
    }

    /**
     * @param finished {@code true} if finished.
     */
    public void finished(boolean finished) {
        this.finished = finished;
    }

    /**
     * @return {@code true} if found.
     */
    public boolean found() {
        return found;
    }

    /**
     * @param found {@code true} if found.
     */
    public void found(boolean found) {
        this.found = found;
    }

    /**
     * @return Error.
     */
    public String error() {
        return err;
    }

    /**
     * @param err Error.
     */
    public void error(String err) {
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 77;
    }
}

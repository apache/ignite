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

package org.apache.ignite.internal.managers.deployment;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Grid deployment response containing requested resource bytes.
 */
public class GridDeploymentResponse implements Message {
    /** Result state. */
    @Order(0)
    private boolean success;

    /** */
    @Order(value = 1, method = "errorMessage")
    private String errMsg;

    /** Raw class/resource/task. */
    @Order(value = 2, method = "byteSource")
    private GridByteArrayList byteSrc;

    /**
     * Default constructor.
     */
    @SuppressWarnings({"RedundantNoArgConstructor"})
    public GridDeploymentResponse() {
        // No-op.
    }

    /**
     * Sets raw class/resource or serialized task as bytes array.
     *
     * @param byteSrc Class/resource/task source.
     */
    public void byteSource(GridByteArrayList byteSrc) {
        this.byteSrc = byteSrc;
    }

    /**
     * Gets raw class/resource or serialized task source as bytes array.
     * @return Class/resource/task source.
     */
    public GridByteArrayList byteSource() {
        return byteSrc;
    }

    /**
     * Tests whether corresponding request was processed successful of not.
     *
     * @return {@code true} if request for the source processed
     *      successfully and {@code false} if not.
     */
    public boolean success() {
        return success;
    }

    /**
     * Sets corresponding request processing status.
     *
     * @param success {@code true} if request processed successfully and
     *      response keeps source inside and {@code false} otherwise.
     */
    public void success(boolean success) {
        this.success = success;
    }

    /**
     * Gets request processing error message. If request processed with error,
     * message will be put in response.
     *
     * @return  Request processing error message.
     */
    public String errorMessage() {
        return errMsg;
    }

    /**
     * Sets request processing error message.
     *
     * @param errMsg Request processing error message.
     */
    public void errorMessage(String errMsg) {
        this.errMsg = errMsg;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 12;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDeploymentResponse.class, this);
    }
}

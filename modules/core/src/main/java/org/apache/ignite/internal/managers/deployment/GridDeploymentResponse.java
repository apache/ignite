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

import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;

/**
 * Grid deployment response containing requested resource bytes.
 */
public class GridDeploymentResponse implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Result state. */
    private boolean success;

    /** */
    private String errMsg;

    /** Raw class/resource/task. */
    private GridByteArrayList byteSrc;

    /**
     * No-op constructor to support {@link Externalizable} interface.
     * This constructor is not meant to be used for other purposes.
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
    void byteSource(GridByteArrayList byteSrc) {
        this.byteSrc = byteSrc;
    }

    /**
     * Gets raw class/resource or serialized task source as bytes array.
     * @return Class/resource/task source.
     */
    GridByteArrayList byteSource() {
        return byteSrc;
    }

    /**
     * Tests whether corresponding request was processed successful of not.
     *
     * @return {@code true} if request for the source processed
     *      successfully and {@code false} if not.
     */
    boolean success() {
        return success;
    }

    /**
     * Sets corresponding request processing status.
     *
     * @param success {@code true} if request processed successfully and
     *      response keeps source inside and {@code false} otherwise.
     */
    void success(boolean success) {
        this.success = success;
    }

    /**
     * Gets request processing error message. If request processed with error,
     * message will be put in response.
     *
     * @return  Request processing error message.
     */
    String errorMessage() {
        return errMsg;
    }

    /**
     * Sets request processing error message.
     *
     * @param errMsg Request processing error message.
     */
    void errorMessage(String errMsg) {
        this.errMsg = errMsg;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeMessage("byteSrc", byteSrc))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString("errMsg", errMsg))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeBoolean("success", success))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                byteSrc = reader.readMessage("byteSrc");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                errMsg = reader.readString("errMsg");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                success = reader.readBoolean("success");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDeploymentResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 12;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDeploymentResponse.class, this);
    }
}

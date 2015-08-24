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

import org.apache.ignite.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.nio.*;

/**
 * Task result response.
 */
public class GridTaskResultResponse implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Result. */
    @GridDirectTransient
    private Object res;

    /** Serialized result. */
    private byte[] resBytes;

    /** Finished flag. */
    private boolean finished;

    /** Flag indicating that task has ever been launched on node. */
    private boolean found;

    /** Error. */
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
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeString("err", err))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeBoolean("finished", finished))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeBoolean("found", found))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByteArray("resBytes", resBytes))
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
                err = reader.readString("err");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                finished = reader.readBoolean("finished");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                found = reader.readBoolean("found");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                resBytes = reader.readByteArray("resBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridTaskResultResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 77;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }
}

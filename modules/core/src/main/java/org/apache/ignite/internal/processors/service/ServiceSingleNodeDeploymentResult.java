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

package org.apache.ignite.internal.processors.service;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType.BYTE_ARR;

/**
 * Service single node deployment result.
 * <p/>
 * Contains count of deployed service instances on single node and deployment errors if exist.
 */
public class ServiceSingleNodeDeploymentResult implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Count of service's instances. */
    private int cnt;

    /** Serialized exceptions. */
    private Collection<byte[]> errors;

    /**
     * Empty constructor for marshalling purposes.
     */
    public ServiceSingleNodeDeploymentResult() {
    }

    /**
     * @param cnt Count of service's instances.
     */
    public ServiceSingleNodeDeploymentResult(int cnt) {
        this.cnt = cnt;
    }

    /**
     * @return Count of service's instances.
     */
    public int count() {
        return cnt;
    }

    /**
     * @param cnt Count of service's instances.
     */
    public void count(int cnt) {
        this.cnt = cnt;
    }

    /**
     * @return Serialized exceptions.
     */
    @NotNull public Collection<byte[]> errors() {
        return errors != null ? errors : Collections.emptyList();
    }

    /**
     * @param errors Serialized exceptions.
     */
    public void errors(Collection<byte[]> errors) {
        this.errors = errors;
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
                if (!writer.writeInt("cnt", cnt))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCollection("errors", errors, BYTE_ARR))
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
                cnt = reader.readInt("cnt");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                errors = reader.readCollection("errors", BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(ServiceSingleNodeDeploymentResult.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 169;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceSingleNodeDeploymentResult.class, this);
    }
}

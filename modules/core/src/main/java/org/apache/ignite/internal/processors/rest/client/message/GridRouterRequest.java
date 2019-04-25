/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

/**
 * Container for routed message information.
 */
public class GridRouterRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Raw message. */
    private final byte[] body;

    /**
     * For {@link Externalizable} (not supported).
     */
    public GridRouterRequest() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param body Message in raw form.
     * @param clientId Client id.
     * @param reqId Request id.
     * @param destId Destination where this message should be delivered.
     */
    public GridRouterRequest(byte[] body, Long reqId, UUID clientId, UUID destId) {
        this.body = body;

        destinationId(destId);
        clientId(clientId);
        requestId(reqId);
    }

    /**
     * @return Raw message.
     */
    public byte[] body() {
        return body;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridRouterRequest [clientId=" + clientId() + ", reqId=" + requestId() + ", " +
            "destId=" + destinationId() + ", length=" + body.length + "]";
    }
}
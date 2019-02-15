/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.igfs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Handshake message.
 */
public class IgfsHandshakeResponse implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** IGFS name. */
    private String igfsName;

    /** Server block size. */
    private long blockSize;

    /** Whether to force sampling on client's side. */
    private Boolean sampling;

    /**
     * {@link Externalizable} support.
     */
    public IgfsHandshakeResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param blockSize Server default block size.
     */
    public IgfsHandshakeResponse(String igfsName, long blockSize, Boolean sampling) {
        this.igfsName = igfsName;
        this.blockSize = blockSize;
        this.sampling = sampling;
    }

    /**
     * @return IGFS name.
     */
    public String igfsName() {
        return igfsName;
    }

    /**
     * @return Server default block size.
     */
    public long blockSize() {
        return blockSize;
    }

    /**
     * @return Sampling flag.
     */
    public Boolean sampling() {
        return sampling;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, igfsName);

        out.writeLong(blockSize);

        if (sampling != null) {
            out.writeBoolean(true);
            out.writeBoolean(sampling);
        }
        else
            out.writeBoolean(false);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        igfsName = U.readString(in);

        blockSize = in.readLong();

        if (in.readBoolean())
            sampling = in.readBoolean();
    }
}
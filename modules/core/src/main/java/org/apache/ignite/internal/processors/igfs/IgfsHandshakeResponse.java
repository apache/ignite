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
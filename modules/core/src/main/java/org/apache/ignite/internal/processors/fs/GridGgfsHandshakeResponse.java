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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Handshake message.
 */
public class GridGgfsHandshakeResponse implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** GGFS name. */
    private String ggfsName;

    /** SECONDARY paths. */
    private GridGgfsPaths paths;

    /** Server block size. */
    private long blockSize;

    /** Whether to force sampling on client's side. */
    private Boolean sampling;

    /**
     * {@link Externalizable} support.
     */
    public GridGgfsHandshakeResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param paths Secondary paths.
     * @param blockSize Server default block size.
     */
    public GridGgfsHandshakeResponse(String ggfsName, GridGgfsPaths paths, long blockSize, Boolean sampling) {
        assert paths != null;

        this.ggfsName = ggfsName;
        this.paths = paths;
        this.blockSize = blockSize;
        this.sampling = sampling;
    }

    /**
     * @return GGFS name.
     */
    public String ggfsName() {
        return ggfsName;
    }

    /**
     * @return SECONDARY paths configured on server.
     */
    public GridGgfsPaths secondaryPaths() {
        return paths;
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
        U.writeString(out, ggfsName);

        paths.writeExternal(out);

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
        ggfsName = U.readString(in);

        paths = new GridGgfsPaths();

        paths.readExternal(in);

        blockSize = in.readLong();

        if (in.readBoolean())
            sampling = in.readBoolean();
    }
}

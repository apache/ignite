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

package org.apache.ignite.internal.visor.file;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Arguments for {@link VisorFileBlockTask}
 */
public class VisorFileBlockTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Log file path. */
    private String path;

    /** Log file offset. */
    private long off;

    /** Block size. */
    private int blockSz;

    /** Log file last modified timestamp. */
    private long lastModified;

    /**
     * Default constructor.
     */
    public VisorFileBlockTaskArg() {
        // No-op.
    }

    /**
     * @param path Log file path.
     * @param off Offset in file.
     * @param blockSz Block size.
     * @param lastModified Log file last modified timestamp.
     */
    public VisorFileBlockTaskArg(String path, long off, int blockSz, long lastModified) {
        this.path = path;
        this.off = off;
        this.blockSz = blockSz;
        this.lastModified = lastModified;
    }

    /**
     * @return Log file path.
     */
    public String getPath() {
        return path;
    }

    /**
     * @return Log file offset.
     */
    public long getOffset() {
        return off;
    }

    /**
     * @return Block size
     */
    public int getBlockSize() {
        return blockSz;
    }

    /**
     * @return Log file last modified timestamp.
     */
    public long getLastModified() {
        return lastModified;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, path);
        out.writeLong(off);
        out.writeInt(blockSz);
        out.writeLong(lastModified);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        path = U.readString(in);
        off = in.readLong();
        blockSz = in.readInt();
        lastModified = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorFileBlockTaskArg.class, this);
    }
}

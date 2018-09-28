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
 * Represents block of bytes from a file, could be optionally zipped.
 */
public class VisorFileBlock extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** File path. */
    private String path;

    /** Marker position. */
    private long off;

    /** File size. */
    private long size;

    /** Timestamp of last modification of the file. */
    private long lastModified;

    /** Whether data was zipped. */
    private boolean zipped;

    /** Data bytes. */
    private byte[] data;

    /**
     * Default constructor.
     */
    public VisorFileBlock() {
        // No-op.
    }

    /**
     * Create file block with given parameters.
     *
     * @param path File path.
     * @param off Marker position.
     * @param size File size.
     * @param lastModified Timestamp of last modification of the file.
     * @param zipped Whether data was zipped.
     * @param data Data bytes.
     */
    public VisorFileBlock(String path, long off, long size, long lastModified, boolean zipped, byte[] data) {
        this.path = path;
        this.off = off;
        this.size = size;
        this.lastModified = lastModified;
        this.zipped = zipped;
        this.data = data;
    }

    /**
     * @return File path.
     */
    public String getPath() {
        return path;
    }

    /**
     * @return Marker position.
     */
    public long getOffset() {
        return off;
    }

    /**
     * @return File size.
     */
    public long getSize() {
        return size;
    }

    /**
     * @return Timestamp of last modification of the file.
     */
    public long getLastModified() {
        return lastModified;
    }

    /**
     * @return Whether data was zipped.
     */
    public boolean isZipped() {
        return zipped;
    }

    /**
     * @return Data bytes.
     */
    public byte[] getData() {
        return data;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, path);
        out.writeLong(off);
        out.writeLong(size);
        out.writeLong(lastModified);
        out.writeBoolean(zipped);
        U.writeByteArray(out, data);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        path = U.readString(in);
        off = in.readLong();
        size = in.readLong();
        lastModified = in.readLong();
        zipped = in.readBoolean();
        data = U.readByteArray(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorFileBlock.class, this);
    }
}

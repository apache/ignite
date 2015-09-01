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

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Represents block of bytes from a file, could be optionally zipped.
 */
public class VisorFileBlock implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** File path. */
    private final String path;

    /** Marker position. */
    private final long off;

    /** File size. */
    private final long size;

    /** Timestamp of last modification of the file. */
    private final long lastModified;

    /** Whether data was zipped. */
    private final boolean zipped;

    /** Data bytes. */
    private final byte[] data;

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
    public String path() {
        return path;
    }

    /**
     * @return Marker position.
     */
    public long offset() {
        return off;
    }

    /**
     * @return File size.
     */
    public long size() {
        return size;
    }

    /**
     * @return Timestamp of last modification of the file.
     */
    public long lastModified() {
        return lastModified;
    }

    /**
     * @return Whether data was zipped.
     */
    public boolean zipped() {
        return zipped;
    }

    /**
     * @return Data bytes.
     */
    public byte[] data() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorFileBlock.class, this);
    }
}
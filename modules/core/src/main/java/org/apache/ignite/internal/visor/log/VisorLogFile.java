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

package org.apache.ignite.internal.visor.log;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Visor log file.
 */
public class VisorLogFile extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** File path. */
    private String path;

    /** File size. */
    private long size;

    /** File last modified timestamp. */
    private long lastModified;

    /**
     * Default constructor.
     */
    public VisorLogFile() {
        // No-op.
    }

    /**
     * Create log file for given file.
     *
     * @param file Log file.
     */
    public VisorLogFile(File file) {
        this(file.getAbsolutePath(), file.length(), file.lastModified());
    }

    /**
     * Create log file with given parameters.
     *
     * @param path File path.
     * @param size File size.
     * @param lastModified File last modified date.
     */
    public VisorLogFile(String path, long size, long lastModified) {
        this.path = path;
        this.size = size;
        this.lastModified = lastModified;
    }

    /**
     * @return File path.
     */
    public String getPath() {
        return path;
    }

    /**
     * @return File size.
     */
    public long getSize() {
        return size;
    }

    /**
     * @return File last modified timestamp.
     */
    public long getLastModified() {
        return lastModified;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, path);
        out.writeLong(size);
        out.writeLong(lastModified);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        path = U.readString(in);
        size = in.readLong();
        lastModified = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorLogFile.class, this);
    }
}

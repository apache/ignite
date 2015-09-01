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

package org.apache.ignite.igfs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Path summary: total files count, total directories count, total length.
 */
public class IgfsPathSummary implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Path. */
    private IgfsPath path;

    /** File count. */
    private int filesCnt;

    /** Directories count. */
    private int dirCnt;

    /** Length consumed. */
    private long totalLen;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgfsPathSummary() {
        // No-op.
    }

    /**
     * Construct empty path summary.
     *
     * @param path Path.
     */
    public IgfsPathSummary(IgfsPath path) {
        this.path = path;
    }

    /**
     * @return Files count.
     */
    public int filesCount() {
        return filesCnt;
    }

    /**
     * @param filesCnt Files count.
     */
    public void filesCount(int filesCnt) {
        this.filesCnt = filesCnt;
    }

    /**
     * @return Directories count.
     */
    public int directoriesCount() {
        return dirCnt;
    }

    /**
     * @param dirCnt Directories count.
     */
    public void directoriesCount(int dirCnt) {
        this.dirCnt = dirCnt;
    }

    /**
     * @return Total length.
     */
    public long totalLength() {
        return totalLen;
    }

    /**
     * @param totalLen Total length.
     */
    public void totalLength(long totalLen) {
        this.totalLen = totalLen;
    }

    /**
     * @return Path for which summary is obtained.
     */
    public IgfsPath path() {
        return path;
    }

    /**
     * @param path Path for which summary is obtained.
     */
    public void path(IgfsPath path) {
        this.path = path;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(filesCnt);
        out.writeInt(dirCnt);
        out.writeLong(totalLen);

        path.writeExternal(out);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        filesCnt = in.readInt();
        dirCnt = in.readInt();
        totalLen = in.readLong();

        path = new IgfsPath();
        path.readExternal(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsPathSummary.class, this);
    }
}
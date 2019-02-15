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

package org.apache.ignite.igfs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Path summary: total files count, total directories count, total length.
 */
public class IgfsPathSummary implements Externalizable, Binarylizable {
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

        path = IgfsUtils.readPath(in);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeInt(filesCnt);
        rawWriter.writeInt(dirCnt);
        rawWriter.writeLong(totalLen);

        IgfsUtils.writePath(rawWriter, path);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader rawReader = reader.rawReader();

        filesCnt = rawReader.readInt();
        dirCnt = rawReader.readInt();
        totalLen = rawReader.readLong();

        path = IgfsUtils.readPath(rawReader);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsPathSummary.class, this);
    }
}
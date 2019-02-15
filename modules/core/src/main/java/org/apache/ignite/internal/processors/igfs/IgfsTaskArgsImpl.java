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
import java.util.Collection;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.mapreduce.IgfsRecordResolver;
import org.apache.ignite.igfs.mapreduce.IgfsTaskArgs;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * IGFS task arguments implementation.
 */
public class IgfsTaskArgsImpl<T> implements IgfsTaskArgs<T>,  Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** IGFS name. */
    private String igfsName;

    /** Paths. */
    private Collection<IgfsPath> paths;

    /** Record resolver. */
    private IgfsRecordResolver recRslvr;

    /** Skip non existent files flag. */
    private boolean skipNonExistentFiles;

    /** Maximum range length. */
    private long maxRangeLen;

    /** User argument. */
    private T usrArg;

    /**
     * {@link Externalizable} support.
     */
    public IgfsTaskArgsImpl() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param igfsName IGFS name.
     * @param paths Paths.
     * @param recRslvr Record resolver.
     * @param skipNonExistentFiles Skip non existent files flag.
     * @param maxRangeLen Maximum range length.
     * @param usrArg User argument.
     */
    public IgfsTaskArgsImpl(String igfsName, Collection<IgfsPath> paths, IgfsRecordResolver recRslvr,
        boolean skipNonExistentFiles, long maxRangeLen, T usrArg) {
        this.igfsName = igfsName;
        this.paths = paths;
        this.recRslvr = recRslvr;
        this.skipNonExistentFiles = skipNonExistentFiles;
        this.maxRangeLen = maxRangeLen;
        this.usrArg = usrArg;
    }

    /** {@inheritDoc} */
    @Override public String igfsName() {
        return igfsName;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsPath> paths() {
        return paths;
    }

    /** {@inheritDoc} */
    @Override public IgfsRecordResolver recordResolver() {
        return recRslvr;
    }

    /** {@inheritDoc} */
    @Override public boolean skipNonExistentFiles() {
        return skipNonExistentFiles;
    }

    /** {@inheritDoc} */
    @Override public long maxRangeLength() {
        return maxRangeLen;
    }

    /** {@inheritDoc} */
    @Override public T userArgument() {
        return usrArg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsTaskArgsImpl.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, igfsName);
        U.writeCollection(out, paths);

        out.writeObject(recRslvr);
        out.writeBoolean(skipNonExistentFiles);
        out.writeLong(maxRangeLen);
        out.writeObject(usrArg);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        igfsName = U.readString(in);
        paths = U.readCollection(in);

        recRslvr = (IgfsRecordResolver)in.readObject();
        skipNonExistentFiles = in.readBoolean();
        maxRangeLen = in.readLong();
        usrArg = (T)in.readObject();
    }
}
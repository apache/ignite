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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Directory listing entry.
 */
public class IgfsListingEntry implements Externalizable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID. */
    private IgniteUuid id;

    /** Directory marker. */
    private boolean dir;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgfsListingEntry() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param fileInfo File info to construct listing entry from.
     */
    public IgfsListingEntry(IgfsEntryInfo fileInfo) {
        id = fileInfo.id();
        dir = fileInfo.isDirectory();
    }

    /**
     * Constructor.
     *
     * @param id File ID.
     * @param dir Directory marker.
     */
    public IgfsListingEntry(IgniteUuid id, boolean dir) {
        this.id = id;
        this.dir = dir;
    }

    /**
     * @return Entry file ID.
     */
    public IgniteUuid fileId() {
        return id;
    }

    /**
     * @return {@code True} if entry represents file.
     */
    public boolean isFile() {
        return !dir;
    }

    /**
     * @return {@code True} if entry represents directory.
     */
    public boolean isDirectory() {
        return dir;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, id);
        out.writeBoolean(dir);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readGridUuid(in);
        dir = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter out = writer.rawWriter();

        BinaryUtils.writeIgniteUuid(out, id);
        out.writeBoolean(dir);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader in = reader.rawReader();

        id = BinaryUtils.readIgniteUuid(in);
        dir = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        return this == other || other instanceof IgfsListingEntry && F.eq(id, ((IgfsListingEntry)other).id);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsListingEntry.class, this);
    }
}

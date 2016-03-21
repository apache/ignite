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

package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Update path closure.
 */
public final class IgfsMetaUpdatePathProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, Void>,
    Externalizable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** New path. */
    private IgfsPath path;

    /**
     * @param path Path.
     */
    public IgfsMetaUpdatePathProcessor(IgfsPath path) {
        this.path = path;
    }

    /**
     * Default constructor (required by Externalizable).
     */
    public IgfsMetaUpdatePathProcessor() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Void process(MutableEntry<IgniteUuid, IgfsEntryInfo> e, Object... args) {
        IgfsEntryInfo info = e.getValue();

        IgfsEntryInfo newInfo = info.path(path);

        e.setValue(newInfo);

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(path);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        path = (IgfsPath)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter out = writer.rawWriter();

        out.writeObject(path);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader in = reader.rawReader();

        path = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsMetaUpdatePathProcessor.class, this);
    }
}

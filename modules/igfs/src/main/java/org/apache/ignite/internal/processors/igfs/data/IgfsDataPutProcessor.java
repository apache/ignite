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

package org.apache.ignite.internal.processors.igfs.data;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.processors.igfs.IgfsBlockKey;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Entry processor to set or replace block byte value.
 */
public class IgfsDataPutProcessor implements EntryProcessor<IgfsBlockKey, byte[], Void>, Externalizable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** The new value. */
    private byte[] newVal;

    /**
     * Non-arg constructor required by externalizable.
     */
    public IgfsDataPutProcessor() {
        // no-op
    }

    /**
     * Constructor.
     *
     * @param newVal The new value.
     */
    public IgfsDataPutProcessor(byte[] newVal) {
        assert newVal != null;

        this.newVal = newVal;
    }

    /** {@inheritDoc} */
    @Override public Void process(MutableEntry<IgfsBlockKey, byte[]> entry, Object... args)
        throws EntryProcessorException {
        byte[] curVal = entry.getValue();

        if (curVal == null || newVal.length > curVal.length)
            entry.setValue(newVal);

        return null;
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        newVal = U.readByteArray(in);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeByteArray(out, newVal);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        newVal = reader.rawReader().readByteArray();
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        writer.rawWriter().writeByteArray(newVal);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsDataPutProcessor.class, this);
    }
}

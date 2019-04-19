/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;

import java.io.ObjectStreamException;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * Binary {@link TreeSet} wrapper.
 */
public class BinaryTreeSet implements Binarylizable {
    /** Original set. */
    private TreeSet set;

    /**
     * Default constructor.
     */
    public BinaryTreeSet() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param set Original set.
     */
    public BinaryTreeSet(TreeSet set) {
        this.set = set;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeObject(set.comparator());

        int size = set.size();

        rawWriter.writeInt(size);

        for (Object val : set)
            rawWriter.writeObject(val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader rawReader = reader.rawReader();

        Comparator comp =  rawReader.readObject();

        set = comp == null ? new TreeSet() : new TreeSet(comp);

        int size = rawReader.readInt();

        for (int i = 0; i < size; i++)
            set.add(rawReader.readObject());
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        return set;
    }
}

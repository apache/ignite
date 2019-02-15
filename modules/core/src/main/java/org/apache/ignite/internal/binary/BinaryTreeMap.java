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

package org.apache.ignite.internal.binary;

import java.io.Serializable;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;

import java.io.ObjectStreamException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Binary {@link TreeMap} wrapper.
 */
public class BinaryTreeMap implements Binarylizable, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Original map. */
    private TreeMap map;

    /**
     * Default constructor.
     */
    public BinaryTreeMap() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param map Original map.
     */
    public BinaryTreeMap(TreeMap map) {
        this.map = map;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeObject(map.comparator());

        int size = map.size();

        rawWriter.writeInt(size);

        for (Map.Entry<Object, Object> entry : ((TreeMap<Object, Object>)map).entrySet()) {
            rawWriter.writeObject(entry.getKey());
            rawWriter.writeObject(entry.getValue());
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader rawReader = reader.rawReader();

        Comparator comp =  rawReader.readObject();

        map = comp == null ? new TreeMap() : new TreeMap(comp);

        int size = rawReader.readInt();

        for (int i = 0; i < size; i++)
            map.put(rawReader.readObject(), rawReader.readObject());
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        return map;
    }
}

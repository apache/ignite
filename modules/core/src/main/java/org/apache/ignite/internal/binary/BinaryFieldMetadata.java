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

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Binary field metadata.
 */
public class BinaryFieldMetadata implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Field id in schema. */
    @GridToStringInclude(sensitive = true)
    private int fieldId;

    /** Field type ID. */
    @GridToStringInclude(sensitive = true)
    private int typeId;

    /**
     * For {@link Externalizable}.
     */
    public BinaryFieldMetadata() {
        // No-op.
    }

    /**
     * Constructor.
     * @param typeId Field type ID.
     * @param fieldId Field id in schema.
     */
    public BinaryFieldMetadata(int typeId, int fieldId) {
        this.typeId = typeId;
        this.fieldId = fieldId;
    }

    /**
     * Constructor.
     * @param accessor Field accessor.
     */
    public BinaryFieldMetadata(BinaryFieldAccessor accessor) {
        this.typeId = accessor.mode().typeId();
        this.fieldId = accessor.id;
    }

    /**
     * @return Field ID in binary schema.
     */
    public int fieldId() {
        return fieldId;
    }

    /**
     * @return ID of the type of the field.
     */
    public int typeId() {
        return typeId;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        writeTo(out);
    }

    /**
     * The object implements the writeTo method to save its contents
     * by calling the methods of DataOutput for its primitive values and strings or
     * calling the writeTo method for other objects.
     *
     * @param out the stream to write the object to.
     * @exception IOException Includes any I/O exceptions that may occur.
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(typeId);
        out.writeInt(fieldId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readFrom(in);
    }

    /**
     * The object implements the readFrom method to restore its
     * contents by calling the methods of DataInput for primitive
     * types and strings or calling readExternal for other objects.  The
     * readFrom method must read the values in the same sequence
     * and with the same types as were written by writeTo.
     *
     * @param in the stream to read data from in order to restore the object.
     * @exception IOException if I/O errors occur.
     */
    public void readFrom(DataInput in) throws IOException {
        typeId = in.readInt();
        fieldId = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryFieldMetadata.class, this);
    }
}

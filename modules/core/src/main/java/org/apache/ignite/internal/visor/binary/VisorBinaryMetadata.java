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

package org.apache.ignite.internal.visor.binary;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 * Binary object metadata to show in Visor.
 */
public class VisorBinaryMetadata extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Type name */
    private String typeName;

    /** Type Id */
    private int typeId;

    /** Affinity key field name. */
    private String affinityKeyFieldName;

    /** Filed list */
    private List<VisorBinaryMetadataField> fields;

    /**
     * @param binary Binary objects.
     * @return List of data transfer objects for binary objects metadata.
     */
    public static List<VisorBinaryMetadata> list(IgniteBinary binary) {
        List<VisorBinaryMetadata> res = new ArrayList<>();

        if (binary != null) {
            for (BinaryType binaryType : binary.types())
                res.add(new VisorBinaryMetadata(binary, binaryType));
        }

        return res;
    }

    /**
     * Default constructor.
     */
    public VisorBinaryMetadata() {
        // No-op.
    }

    /**
     * Default constructor.
     */
    public VisorBinaryMetadata(IgniteBinary binary, BinaryType binaryType) {
        typeName = binaryType.typeName();
        typeId = binary.typeId(typeName);
        affinityKeyFieldName = binaryType.affinityKeyFieldName();

        Collection<String> binaryTypeFields = binaryType.fieldNames();

        fields = new ArrayList<>(binaryTypeFields.size());

        for (String metaField : binaryTypeFields)
            fields.add(new VisorBinaryMetadataField(metaField, binaryType.fieldTypeName(metaField), null));
    }

    /**
     * @return Type name.
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * @return Type Id.
     */
    public int getTypeId() {
        return typeId;
    }

    /**
     * @return Fields list.
     */
    public Collection<VisorBinaryMetadataField> getFields() {
        return fields;
    }

    /**
     * @return Affinity key field name.
     */
    @Nullable public String getAffinityKeyFieldName() {
        return affinityKeyFieldName;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, typeName);
        out.writeInt(typeId);
        U.writeString(out, affinityKeyFieldName);
        U.writeCollection(out, fields);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        typeName = U.readString(in);
        typeId = in.readInt();
        affinityKeyFieldName = U.readString(in);
        fields = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBinaryMetadata.class, this);
    }
}

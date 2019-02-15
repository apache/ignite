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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for configuration of binary type structures.
 */
public class VisorBinaryTypeConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Class name. */
    private String typeName;

    /** ID mapper. */
    private String idMapper;

    /** Name mapper. */
    private String nameMapper;

    /** Serializer. */
    private String serializer;

    /** Enum flag. */
    private boolean isEnum;

    /**
     * Construct data transfer object for Executor configurations properties.
     *
     * @param cfgs Executor configurations.
     * @return Executor configurations properties.
     */
    public static List<VisorBinaryTypeConfiguration> list(Collection<BinaryTypeConfiguration> cfgs) {
        List<VisorBinaryTypeConfiguration> res = new ArrayList<>();

        if (!F.isEmpty(cfgs)) {
            for (BinaryTypeConfiguration cfg : cfgs)
                res.add(new VisorBinaryTypeConfiguration(cfg));
        }

        return res;
    }

    /**
     * Default constructor.
     */
    public VisorBinaryTypeConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for binary type configuration.
     *
     * @param src Binary type configuration.
     */
    public VisorBinaryTypeConfiguration(BinaryTypeConfiguration src) {
        typeName = src.getTypeName();
        idMapper = compactClass(src.getIdMapper());
        nameMapper = compactClass(src.getNameMapper());
        serializer = compactClass(src.getSerializer());
        isEnum = src.isEnum();
    }

    /**
     * @return Class name.
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * @return ID mapper.
     */
    public String getIdMapper() {
        return idMapper;
    }

    /**
     * @return Name mapper.
     */
    public String getNameMapper() {
        return nameMapper;
    }

    /**
     * @return Serializer.
     */
    public String getSerializer() {
        return serializer;
    }

    /**
     * @return Enum flag.
     */
    public boolean isEnum() {
        return isEnum;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, typeName);
        U.writeString(out, idMapper);
        U.writeString(out, nameMapper);
        U.writeString(out, serializer);
        out.writeBoolean(isEnum);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        typeName = U.readString(in);
        idMapper = U.readString(in);
        nameMapper = U.readString(in);
        serializer = U.readString(in);
        isEnum = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBinaryTypeConfiguration.class, this);
    }
}

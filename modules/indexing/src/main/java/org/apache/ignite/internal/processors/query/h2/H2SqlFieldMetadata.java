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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Field descriptor.
 */
public class H2SqlFieldMetadata implements GridQueryFieldMetadata {
    /** */
    private static final long serialVersionUID = 0L;

    /** Schema name. */
    private String schemaName;

    /** Type name. */
    private String typeName;

    /** Name. */
    private String name;

    /** Type. */
    private String type;

    /** Precision. */
    private int precision;

    /** Scale. */
    private int scale;

    /**
     * Required by {@link Externalizable}.
     */
    public H2SqlFieldMetadata() {
        // No-op
    }

    /**
     * @param schemaName Schema name.
     * @param typeName Type name.
     * @param name Name.
     * @param type Type.
     * @param precision Precision.
     * @param scale Scale.
     */
    H2SqlFieldMetadata(@Nullable String schemaName, @Nullable String typeName, String name, String type,
        int precision, int scale) {
        assert name != null && type != null : schemaName + " | " + typeName + " | " + name + " | " + type;

        this.schemaName = schemaName;
        this.typeName = typeName;
        this.name = name;
        this.type = type;
        this.precision = precision;
        this.scale = scale;
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public String typeName() {
        return typeName;
    }

    /** {@inheritDoc} */
    @Override public String fieldName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String fieldTypeName() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public int precision() {
        return precision;
    }

    /** {@inheritDoc} */
    @Override public int scale() {
        return scale;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, schemaName);
        U.writeString(out, typeName);
        U.writeString(out, name);
        U.writeString(out, type);
        out.write(precision);
        out.write(scale);

    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        schemaName = U.readString(in);
        typeName = U.readString(in);
        name = U.readString(in);
        type = U.readString(in);
        precision = in.read();
        scale = in.read();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2SqlFieldMetadata.class, this);
    }
}

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

package org.apache.ignite.internal.visor.query;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for query field type description.
 */
public class VisorQueryField extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Schema name. */
    private String schemaName;

    /** Type name. */
    private String typeName;

    /** Field name. */
    private String fieldName;

    /** Field type name. */
    private String fieldTypeName;

    /**
     * Default constructor.
     */
    public VisorQueryField() {
        // No-op.
    }

    /**
     * Create data transfer object with given parameters.
     *
     * @param schemaName Schema name.
     * @param typeName Type name.
     * @param fieldName Name.
     * @param fieldTypeName Type.
     */
    public VisorQueryField(String schemaName, String typeName, String fieldName, String fieldTypeName) {
        this.schemaName = schemaName;
        this.typeName = typeName;
        this.fieldName = fieldName;
        this.fieldTypeName = fieldTypeName;
    }

    /**
     * @return Schema name.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @return Type name.
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * @return Field name.
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * @return Field type name.
     */
    public String getFieldTypeName() {
        return fieldTypeName;
    }

    /**
     * @param schema If {@code true} then add schema name to full name.
     * @return Fully qualified field name with type name and schema name.
     */
    public String getFullName(boolean schema) {
        if (!F.isEmpty(typeName)) {
            if (schema && !F.isEmpty(schemaName))
                return schemaName + "." + typeName + "." + fieldName;

            return typeName + "." + fieldName;
        }

        return fieldName;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, schemaName);
        U.writeString(out, typeName);
        U.writeString(out, fieldName);
        U.writeString(out, fieldTypeName);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        schemaName = U.readString(in);
        typeName = U.readString(in);
        fieldName = U.readString(in);
        fieldTypeName = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryField.class, this);
    }
}

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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;

/**
 *
 */
public class CalciteQueryFieldMetadata implements GridQueryFieldMetadata {
    /** */
    private String schemaName;

    /** */
    private String typeName;

    /** */
    private String fieldName;

    /** */
    private String fieldTypeName;

    /** */
    private int precision;

    /** */
    private int scale;

    /** Blank constructor for external serialization. */
    public CalciteQueryFieldMetadata() {
    }

    /**
     * @param schemaName Schema name.
     * @param typeName Type name.
     * @param fieldName Field name.
     * @param fieldTypeName Field type name.
     * @param precision Precision.
     * @param scale Scale.
     */
    public CalciteQueryFieldMetadata(String schemaName, String typeName, String fieldName, String fieldTypeName, int precision, int scale) {
        this.schemaName = schemaName;
        this.typeName = typeName;
        this.fieldName = fieldName;
        this.fieldTypeName = fieldTypeName;
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
        return fieldName;
    }

    /** {@inheritDoc} */
    @Override public String fieldTypeName() {
        return fieldTypeName;
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
        out.writeUTF(schemaName);
        out.writeUTF(typeName);
        out.writeUTF(fieldName);
        out.writeUTF(fieldTypeName);
        out.writeInt(precision);
        out.writeInt(scale);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        schemaName = in.readUTF();
        typeName = in.readUTF();
        fieldName = in.readUTF();
        fieldTypeName = in.readUTF();
        precision = in.readInt();
        scale = in.readInt();
    }
}

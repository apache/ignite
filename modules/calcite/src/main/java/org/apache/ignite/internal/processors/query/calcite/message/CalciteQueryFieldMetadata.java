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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSetMetaData;
import org.apache.ignite.internal.processors.cache.query.GridQueryFieldMetadataMessage;

/**
 *
 */
public class CalciteQueryFieldMetadata extends GridQueryFieldMetadataMessage implements Externalizable, CalciteMessage {
    /** Blank constructor for external serialization. */
    public CalciteQueryFieldMetadata() {
        // No-op.
    }

    /** */
    public CalciteQueryFieldMetadata(
        String schemaName,
        String typeName,
        String fieldTypeName,
        String fieldName,
        int precision,
        int scale,
        boolean isNullable
    ) {
        super(schemaName, typeName, fieldTypeName, fieldName, precision, scale,
            isNullable ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls);
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.QUERY_FIELD_METADATA;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // TODO: revise
        assert false;

        out.writeUTF(schemaName);
        out.writeUTF(typeName);
        out.writeUTF(fieldName);
        out.writeUTF(fieldTypeName);
        out.writeInt(precision);
        out.writeInt(scale);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        // TODO: revise
        assert false;

        schemaName = in.readUTF();
        typeName = in.readUTF();
        fieldName = in.readUTF();
        fieldTypeName = in.readUTF();
        precision = in.readInt();
        scale = in.readInt();
    }
}

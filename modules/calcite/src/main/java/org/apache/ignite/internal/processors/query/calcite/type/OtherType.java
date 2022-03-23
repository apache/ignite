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

package org.apache.ignite.internal.processors.query.calcite.type;

import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/** OTHER SQL type. */
public class OtherType extends RelDataTypeImpl {
    /** Nullable flag. */
    private final boolean nullable;

    /** Nullable flag. */
    private final boolean isUuid;

    /** Ctor. */
    public OtherType(boolean isUuid, boolean nullable) {
        this.nullable = nullable;

        this.isUuid = isUuid;

        computeDigest();
    }

    /** @return {@code True} if type is UUID. {@code False} otherwise. */
    public boolean isUuid() {
        return isUuid;
    }

    /** {@inheritDoc} */
    @Override public boolean isNullable() {
        return nullable;
    }

    /** {@inheritDoc} */
    @Override public RelDataTypeFamily getFamily() {
        return SqlTypeFamily.ANY;
    }

    /** {@inheritDoc} */
    @Override public SqlTypeName getSqlTypeName() {
        return SqlTypeName.ANY;
    }

    /** {@inheritDoc} */
    @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(isUuid ? "UUID" : "ANOTHER");
    }
}

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

package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.time.LocalDateTime;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests SQL types exposed by user-defined function parameters. */
public class IgniteFunctionParameterTest {
    /** */
    @Test
    public void testUsesProvidedTypeSystem() {
        RelDataTypeSystem typeSys = new RelDataTypeSystemImpl() {
            @Override public int getDefaultPrecision(SqlTypeName typeName) {
                return typeName == SqlTypeName.TIMESTAMP ? 2 : super.getDefaultPrecision(typeName);
            }
        };

        FunctionParameter param = new IgniteFunctionParameter(
            ReflectiveFunctionBase.builder().add(LocalDateTime.class, "ts").build().get(0));

        for (RelDataTypeFactory factory : new RelDataTypeFactory[] {
            new JavaTypeFactoryImpl(typeSys), new IgniteTypeFactory(typeSys)
        }) {
            RelDataType type = param.getType(factory);

            assertEquals(SqlTypeName.TIMESTAMP, type.getSqlTypeName());
            assertEquals(2, type.getPrecision());
            assertTrue(type.isNullable());
        }
    }
}

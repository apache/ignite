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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

/** */
public class OtherTypeTest {
    /** */
    @Test
    public void testEquality() {
        for (boolean nullable : new boolean[] {false, true}) {
            RelDataType igniteType = new OtherType(nullable);
            RelDataType calciteType = new BasicSqlType(IgniteTypeSystem.INSTANCE, SqlTypeName.OTHER)
                .createWithNullability(nullable);

            assertNotEquals(igniteType, calciteType);
            assertNotEquals(calciteType, igniteType);
            assertEquals(igniteType, new OtherType(nullable));
            assertNotEquals(igniteType, new OtherType(!nullable));
            assertEquals("OTHER", igniteType.toString());
        }
    }

    /** */
    @Test
    public void testTypeInterning() {
        IgniteTypeFactory igniteFactory = new IgniteTypeFactory();
        JavaTypeFactoryImpl calciteFactory = new JavaTypeFactoryImpl();

        for (boolean nullable : new boolean[] {false, true}) {
            RelDataType igniteType = igniteFactory.createCustomType(Object.class, nullable);
            RelDataType calciteType = calciteFactory.createTypeWithNullability(
                calciteFactory.createSqlType(SqlTypeName.OTHER), nullable);

            assertEquals(OtherType.class, igniteType.getClass());
            assertEquals(BasicSqlType.class, calciteType.getClass());
            assertEquals(nullable, igniteType.isNullable());
            assertEquals(nullable, calciteType.isNullable());
            assertSame(igniteType, igniteFactory.createCustomType(Object.class, nullable));
            assertSame(calciteType, calciteFactory.createTypeWithNullability(
                calciteFactory.createSqlType(SqlTypeName.OTHER), nullable));
        }
    }
}

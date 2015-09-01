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

package org.apache.ignite.schema.test.parser;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.schema.model.PojoDescriptor;
import org.apache.ignite.schema.model.PojoField;
import org.apache.ignite.schema.test.AbstractSchemaImportTest;

/**
 * Tests for metadata parsing.
 */
public class DbMetadataParserTest extends AbstractSchemaImportTest {
    /**
     * Check that field is correspond to expected.
     *
     * @param field Field descriptor.
     * @param name Expected field name.
     * @param primitive Expected field primitive type.
     * @param cls Expected field type.
     */
    private void checkField(PojoField field, String name, boolean primitive, Class<?> cls) {
        assertEquals("Name of field should be " + name, name, field.javaName());

        assertEquals("Type of field should be " + cls.getName(), cls.getName(), field.javaTypeName());

        assertEquals("Field primitive should be " + primitive, primitive, field.primitive());
    }

    /**
     * Check that type is correspond to expected.
     *
     * @param type Type descriptor.
     */
    private void checkType(PojoDescriptor type) {
        assertFalse("Type key class name should be defined", type.keyClassName().isEmpty());

        assertFalse("Type value class name should be defined", type.valueClassName().isEmpty());

        Collection<PojoField> keyFields = type.keyFields();

        assertEquals("Key type should have 1 field", 1, keyFields.size());

        checkField(keyFields.iterator().next(), "pk", true, int.class);

        List<PojoField> fields = type.fields();

        assertEquals("Value type should have 15 fields", 15, fields.size());

        Iterator<PojoField> fieldsIt = fields.iterator();

        checkField(fieldsIt.next(), "pk", true, int.class);

        if ("Objects".equals(type.valueClassName())) {
            checkField(fieldsIt.next(), "boolcol", false, Boolean.class);
            checkField(fieldsIt.next(), "bytecol", false, Byte.class);
            checkField(fieldsIt.next(), "shortcol", false, Short.class);
            checkField(fieldsIt.next(), "intcol", false, Integer.class);
            checkField(fieldsIt.next(), "longcol", false, Long.class);
            checkField(fieldsIt.next(), "floatcol", false, Float.class);
            checkField(fieldsIt.next(), "doublecol", false, Double.class);
            checkField(fieldsIt.next(), "doublecol2", false, Double.class);
        }
        else {
            checkField(fieldsIt.next(), "boolcol", true, boolean.class);
            checkField(fieldsIt.next(), "bytecol", true, byte.class);
            checkField(fieldsIt.next(), "shortcol", true, short.class);
            checkField(fieldsIt.next(), "intcol", true, int.class);
            checkField(fieldsIt.next(), "longcol", true, long.class);
            checkField(fieldsIt.next(), "floatcol", true, float.class);
            checkField(fieldsIt.next(), "doublecol", true, double.class);
            checkField(fieldsIt.next(), "doublecol2", true, double.class);
        }

        checkField(fieldsIt.next(), "bigdecimalcol", false, BigDecimal.class);
        checkField(fieldsIt.next(), "strcol", false, String.class);
        checkField(fieldsIt.next(), "datecol", false, Date.class);
        checkField(fieldsIt.next(), "timecol", false, Time.class);
        checkField(fieldsIt.next(), "tscol", false, Timestamp.class);
        checkField(fieldsIt.next(), "arrcol", false, Object.class);
    }

    /**
     * Test that metadata generated correctly.
     */
    public void testCheckMetadata() {
        assertEquals("Metadata should contain 3 element", 3, pojos.size());

        Iterator<PojoDescriptor> it = pojos.iterator();

        PojoDescriptor schema = it.next();

        assertTrue("First element is schema description. Its class name should be empty",
                schema.valueClassName().isEmpty());

        checkType(it.next());

        checkType(it.next());
    }
}
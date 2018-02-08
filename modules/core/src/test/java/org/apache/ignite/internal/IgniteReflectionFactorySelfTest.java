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

package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import junit.framework.TestCase;
import org.apache.ignite.configuration.IgniteReflectionFactory;

/**
 * Tests for {@link IgniteReflectionFactory} class.
 */
public class IgniteReflectionFactorySelfTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    public void testByteMethod() throws Exception {
        byte    expByteVal    = 42;
        short   expShortVal   = 42;
        int     expIntVal     = 42;
        long    expLongVal    = 42L;
        float   expFloatVal   = 42.0f;
        double  expDoubleVal  = 42.0d;
        char    expCharVal    = 'z';
        boolean expBooleanVal = true;

        Map<String, Serializable> props = new HashMap<>();
        props.put("byteField", expByteVal);
        props.put("shortField", expShortVal);
        props.put("intField", expIntVal);
        props.put("longField", expLongVal);
        props.put("floatField", expFloatVal);
        props.put("doubleField", expDoubleVal);
        props.put("charField", expCharVal);
        props.put("booleanField", expBooleanVal);

        IgniteReflectionFactory<TestClass> factory = new IgniteReflectionFactory<>(TestClass.class);
        factory.setProperties(props);

        TestClass instance = factory.create();

        assertEquals(expByteVal, instance.getByteField());
        assertEquals(expShortVal, instance.getShortField());
        assertEquals(expIntVal, instance.getIntField());
        assertEquals(expLongVal, instance.getLongField());
        assertEquals(expFloatVal, instance.getFloatField());
        assertEquals(expDoubleVal, instance.getDoubleField());
        assertEquals(expCharVal, instance.getCharField());
        assertEquals(expBooleanVal, instance.getBooleanField());
    }

    /**
     * Test class that is created by {@link IgniteReflectionFactory}.
     */
    public static class TestClass {
        /** */
        private byte byteField;
        /** */
        private short shortField;
        /** */
        private int intField;
        /** */
        private long longField;
        /** */
        private float floatField;
        /** */
        private double doubleField;
        /** */
        private char charField;
        /** */
        private boolean booleanField;

        /** */
        public byte getByteField() {
            return byteField;
        }

        /** */
        public void setByteField(byte byteField) {
            this.byteField = byteField;
        }

        /** */
        public short getShortField() {
            return shortField;
        }

        /** */
        public void setShortField(short shortField) {
            this.shortField = shortField;
        }

        /** */
        public long getLongField() {
            return longField;
        }

        /** */
        public void setLongField(long longField) {
            this.longField = longField;
        }

        /** */
        public float getFloatField() {
            return floatField;
        }

        /** */
        public void setFloatField(float floatField) {
            this.floatField = floatField;
        }

        /** */
        public double getDoubleField() {
            return doubleField;
        }

        /** */
        public void setDoubleField(double doubleField) {
            this.doubleField = doubleField;
        }

        /** */
        public char getCharField() {
            return charField;
        }

        /** */
        public void setCharField(char charField) {
            this.charField = charField;
        }

        /** */
        public boolean getBooleanField() {
            return booleanField;
        }

        /** */
        public void setBooleanField(boolean booleanField) {
            this.booleanField = booleanField;
        }

        /** */
        public int getIntField() {
            return intField;
        }

        /** */
        public void setIntField(int intField) {
            this.intField = intField;
        }
    }
}

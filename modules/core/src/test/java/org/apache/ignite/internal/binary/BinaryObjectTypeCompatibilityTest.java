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

package org.apache.ignite.internal.binary;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class BinaryObjectTypeCompatibilityTest extends GridCommonAbstractTest {
    /** */
    private static final Random RANDOM = new Random(System.currentTimeMillis());

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCompatibilityWithObject() throws Exception {
        Ignite ignite = startGrid();

        BinaryObjectBuilder bldr = ignite.binary().builder("ObjectWrapper");
        bldr.setField("objField", new Object());
        bldr.build();

        validateMap(bldr, "objField", new HashMap<>());
        validateMap(bldr, "objField", new LinkedHashMap<>());
        validateMap(bldr, "objField", new TreeMap<>());

        validateCollection(bldr, "objField", new ArrayList<>());
        validateCollection(bldr, "objField", new LinkedList<>());
        validateCollection(bldr, "objField", new HashSet<>());
        validateCollection(bldr, "objField", new LinkedHashSet<>());
        validateCollection(bldr, "objField", new TreeSet<>());

        validate(bldr, "objField", (byte)RANDOM.nextInt());
        validate(bldr, "objField", (short)RANDOM.nextInt());
        validate(bldr, "objField", (char)RANDOM.nextInt());
        validate(bldr, "objField", RANDOM.nextInt());
        validate(bldr, "objField", RANDOM.nextLong());
        validate(bldr, "objField", RANDOM.nextFloat());
        validate(bldr, "objField", RANDOM.nextDouble());

        validate(bldr, "objField", Enum.DEFAULT);
        validate(bldr, "objField", new BigDecimal(RANDOM.nextInt()));
        validate(bldr, "objField", "Test string");
        validate(bldr, "objField", new Date());
        validate(bldr, "objField", new Timestamp(System.currentTimeMillis()));
        validate(bldr, "objField", new Time(System.currentTimeMillis()));
        validate(bldr, "objField", UUID.randomUUID());
    }

    /**
     * @param bldr {@link BinaryObjectBuilder}, that will be used for testing.
     * @param fldName Name of the field being tested.
     * @param src {@link Collection} object, that should be tested.
     */
    private void validateCollection(BinaryObjectBuilder bldr, String fldName, Collection<Integer> src) {
        for (int i = 0; i < 1000; i++)
            src.add(RANDOM.nextInt());

        bldr.setField(fldName, src);

        BinaryObject binObj = bldr.build();

        Collection<Integer> res = deserialize(binObj.field(fldName));

        assertEqualsCollections(src, res);
    }

    /**
     * @param bldr {@link BinaryObjectBuilder}, that will be used for testing.
     * @param fldName Name of the field being tested.
     * @param src {@link Map} object, that should be tested.
     */
    private void validateMap(BinaryObjectBuilder bldr, String fldName, Map<Integer, String> src) {
        for (int i = 0; i < 1000; i++) {
            int key = RANDOM.nextInt();
            src.put(key, Integer.toString(key));
        }

        bldr.setField(fldName, src);

        BinaryObject binObj = bldr.build();

        Map<Integer, String> res = deserialize(binObj.field(fldName));

        assertEquals(src, res);
    }

    /**
     * @param bldr {@code BinaryObjectBuilder}, that will be used for testing.
     * @param fldName Name of the field being tested.
     * @param src Value being tested.
     * @param <T> Type of the value.
     */
    private <T> void validate(BinaryObjectBuilder bldr, String fldName, T src) {
        bldr.setField(fldName, src);

        BinaryObject binObj = bldr.build();

        T res = deserialize(binObj.field(fldName));

        assertEquals(src, res);
    }

    /**
     * @param obj Object being deserialized.
     * @param <T> Result type.
     * @return Deserialized value, if {@link BinaryObject} was provided, or the same object otherwise.
     */
    private <T> T deserialize(Object obj) {
        if (obj instanceof BinaryObject)
            return ((BinaryObject)obj).deserialize();
        else
            return (T)obj;
    }

    /**
     * Enumeration for tests.
     */
    private enum Enum {
        /** */
        DEFAULT
    }
}

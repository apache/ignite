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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

/**
 * Record view tests.
 */
@SuppressWarnings("ZeroLengthArrayAllocation")
public class ClientKeyValueViewTest extends AbstractClientTableTest {
    @Test
    public void testBinaryPutPojoGet() {
        Table table = defaultTable();
        KeyValueView<Long, PersonPojo> pojoView = table.keyValueView(Mapper.of(Long.class), Mapper.of(PersonPojo.class));

        table.recordView().upsert(null, tuple());

        PersonPojo val = pojoView.get(null, DEFAULT_ID);
        PersonPojo missingVal = pojoView.get(null, -1L);

        assertEquals(DEFAULT_NAME, val.name);
        assertEquals(0, val.id); // Not mapped in value part.
        assertNull(missingVal);
    }

    @Test
    public void testBinaryPutPrimitiveGet() {
        Table table = defaultTable();
        KeyValueView<Long, String> primitiveView = table.keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        table.recordView().upsert(null, tuple());

        String val = primitiveView.get(null, DEFAULT_ID);
        String missingVal = primitiveView.get(null, -1L);

        assertEquals(DEFAULT_NAME, val);
        assertNull(missingVal);
    }

    @Test
    public void testPrimitivePutBinaryGet() {
        Table table = defaultTable();
        KeyValueView<Long, String> primitiveView = table.keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        primitiveView.put(null, DEFAULT_ID, DEFAULT_NAME);

        Tuple tuple = table.recordView().get(null, tupleKey(DEFAULT_ID));
        assertEquals(DEFAULT_NAME, tuple.stringValue(1));
    }

    @Test
    public void testMissingValueColumnsAreSkipped() {
        Table table = fullTable();
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();
        KeyValueView<IncompletePojo, IncompletePojo> pojoView = table.keyValueView(IncompletePojo.class, IncompletePojo.class);

        kvView.put(null, allClumnsTableKey(1), allColumnsTableVal("x"));

        var key = new IncompletePojo();
        key.id = "1";
        key.gid = 1;

        // This POJO does not have fields for all table columns, and this is ok.
        IncompletePojo val = pojoView.get(null, key);

        assertEquals(0, val.gid);
        assertNull(val.id);
        assertEquals("x", val.zstring);
        assertEquals(2, val.zbytes[1]);
        assertEquals(11, val.zbyte);
    }

    @Test
    public void testAllColumnsBinaryPutPojoGet() {
        Table table = fullTable();
        KeyValueView<IncompletePojo, AllColumnsPojo> pojoView = table.keyValueView(
                Mapper.of(IncompletePojo.class),
                Mapper.of(AllColumnsPojo.class));

        table.recordView().upsert(null, allColumnsTableVal("foo"));

        var key = new IncompletePojo();
        key.gid = (int) (long) DEFAULT_ID;
        key.id = String.valueOf(DEFAULT_ID);

        AllColumnsPojo res = pojoView.get(null, key);
        assertEquals(11, res.zbyte);
        assertEquals(12, res.zshort);
        assertEquals(13, res.zint);
        assertEquals(14, res.zlong);
        assertEquals(1.5f, res.zfloat);
        assertEquals(1.6, res.zdouble);
        assertEquals(localDate, res.zdate);
        assertEquals(localTime, res.ztime);
        assertEquals(instant, res.ztimestamp);
        assertEquals("foo", res.zstring);
        assertArrayEquals(new byte[]{1, 2}, res.zbytes);
        assertEquals(BitSet.valueOf(new byte[]{32}), res.zbitmask);
        assertEquals(21, res.zdecimal.longValue());
        assertEquals(22, res.znumber.longValue());
        assertEquals(uuid, res.zuuid);
    }

    @Test
    public void testAllColumnsPojoPutBinaryGet() {
        Table table = fullTable();
        KeyValueView<AllColumnsPojo, AllColumnsPojo> pojoView = table.keyValueView(
                Mapper.of(AllColumnsPojo.class),
                Mapper.of(AllColumnsPojo.class));

        var val = new AllColumnsPojo();

        val.gid = 111;
        val.id = "112";
        val.zbyte = 113;
        val.zshort = 114;
        val.zint = 115;
        val.zlong = 116;
        val.zfloat = 1.17f;
        val.zdouble = 1.18;
        val.zdate = localDate;
        val.ztime = localTime;
        val.ztimestamp = instant;
        val.zstring = "119";
        val.zbytes = new byte[]{120};
        val.zbitmask = BitSet.valueOf(new byte[]{121});
        val.zdecimal = BigDecimal.valueOf(122);
        val.znumber = BigInteger.valueOf(123);
        val.zuuid = uuid;

        pojoView.put(null, val, val);

        Tuple res = table.recordView().get(null, Tuple.create().set("id", "112").set("gid", 111));

        assertNotNull(res);
        assertEquals(111, res.intValue("gid"));
        assertEquals("112", res.stringValue("id"));
        assertEquals(113, res.byteValue("zbyte"));
        assertEquals(114, res.shortValue("zshort"));
        assertEquals(115, res.intValue("zint"));
        assertEquals(116, res.longValue("zlong"));
        assertEquals(1.17f, res.floatValue("zfloat"));
        assertEquals(1.18, res.doubleValue("zdouble"));
        assertEquals(localDate, res.dateValue("zdate"));
        assertEquals(localTime, res.timeValue("ztime"));
        assertEquals(instant, res.timestampValue("ztimestamp"));
        assertEquals("119", res.stringValue("zstring"));
        assertEquals(120, ((byte[]) res.value("zbytes"))[0]);
        assertEquals(BitSet.valueOf(new byte[]{121}), res.bitmaskValue("zbitmask"));
        assertEquals(122, ((Number) res.value("zdecimal")).longValue());
        assertEquals(BigInteger.valueOf(123), res.value("znumber"));
        assertEquals(uuid, res.uuidValue("zuuid"));
    }

    @Test
    public void testMissingKeyColumnThrowsException() {
        var kvView = defaultTable().keyValueView(NamePojo.class, NamePojo.class);

        CompletionException e = assertThrows(CompletionException.class, () -> kvView.get(null, new NamePojo()));
        IgniteClientException ice = (IgniteClientException) e.getCause();

        assertEquals("No field found for column id", ice.getMessage());
    }

    @Test
    public void testNullablePrimitiveFields() {
        KeyValueView<IncompletePojoNullable, IncompletePojoNullable> pojoView = fullTable().keyValueView(
                IncompletePojoNullable.class,
                IncompletePojoNullable.class);

        RecordView<Tuple> tupleView = fullTable().recordView();

        var rec = new IncompletePojoNullable();
        rec.id = "1";
        rec.gid = 1;

        pojoView.put(null, rec, rec);

        IncompletePojoNullable res = pojoView.get(null, rec);
        Tuple binRes = tupleView.get(null, Tuple.create().set("id", "1").set("gid", 1L));

        assertNotNull(res);
        assertNotNull(binRes);

        assertNull(res.zbyte);
        assertNull(res.zshort);
        assertNull(res.zint);
        assertNull(res.zlong);
        assertNull(res.zfloat);
        assertNull(res.zdouble);

        for (int i = 0; i < binRes.columnCount(); i++) {
            if (binRes.columnName(i).endsWith("id")) {
                continue;
            }

            assertNull(binRes.value(i));
        }
    }

    @Test
    public void testGetAll() {
        Table table = defaultTable();
        KeyValueView<Long, PersonPojo> pojoView = table.keyValueView(Mapper.of(Long.class), Mapper.of(PersonPojo.class));

        table.recordView().upsert(null, tuple());
        table.recordView().upsert(null, tuple(100L, "100"));

        Collection<Long> keys = List.of(DEFAULT_ID, 101L, 100L);

        Map<Long, PersonPojo> res = pojoView.getAll(null, keys);
        Long[] resKeys = res.keySet().toArray(new Long[0]);
        PersonPojo[] resVals = res.values().toArray(new PersonPojo[0]);

        assertEquals(2, resVals.length);

        assertNotNull(resVals[0]);
        assertNotNull(resVals[1]);

        assertEquals(DEFAULT_ID, resKeys[0]);
        assertEquals(DEFAULT_NAME, resVals[0].name);

        assertEquals(100L, resKeys[1]);
        assertEquals("100", resVals[1].name);
    }

    @Test
    public void testGetAllPrimitive() {
        Table table = defaultTable();
        KeyValueView<Long, String> pojoView = table.keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        table.recordView().upsert(null, tuple());
        table.recordView().upsert(null, tuple(100L, "100"));

        Collection<Long> keys = List.of(DEFAULT_ID, 101L, 100L);

        String[] res = pojoView.getAll(null, keys).values().toArray(new String[0]);

        assertEquals(2, res.length);
        assertEquals(DEFAULT_NAME, res[0]);
        assertEquals("100", res[1]);
    }

    @Test
    public void testPutAll() {
        KeyValueView<Long, String> pojoView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        Map<Long, String> pojos = Map.of(
                DEFAULT_ID, DEFAULT_NAME,
                100L, "100",
                101L, "101");

        pojoView.putAll(null, pojos);

        assertEquals(DEFAULT_NAME, pojoView.get(null, DEFAULT_ID));
        assertEquals("100", pojoView.get(null, 100L));
        assertEquals("101", pojoView.get(null, 101L));
    }

    @Test
    public void testGetAndPut() {
        KeyValueView<Long, String> pojoView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        pojoView.put(null, DEFAULT_ID, DEFAULT_NAME);

        String res1 = pojoView.getAndPut(null, DEFAULT_ID, "new_name");
        String res2 = pojoView.getAndPut(null, 100L, "name");

        assertEquals(DEFAULT_NAME, res1);
        assertEquals("new_name", pojoView.get(null, DEFAULT_ID));

        assertNull(res2);
        assertEquals("name", pojoView.get(null, 100L));
    }

    @Test
    public void testPutNull() {
        KeyValueView<Long, String> pojoView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        pojoView.put(null, DEFAULT_ID, DEFAULT_NAME);
        pojoView.put(null, DEFAULT_ID, null);

        assertNull(pojoView.get(null, DEFAULT_ID));
    }

    @Test
    public void testPutIfAbsent() {
        KeyValueView<Long, String> pojoView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        pojoView.put(null, DEFAULT_ID, DEFAULT_NAME);

        boolean res1 = pojoView.putIfAbsent(null, DEFAULT_ID, "foobar");
        boolean res2 = pojoView.putIfAbsent(null, 100L, "100");

        assertFalse(res1);
        assertTrue(res2);
        assertEquals("100", pojoView.get(null, 100L));
    }

    @Test
    public void testReplace() {
        KeyValueView<Long, String> pojoView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        pojoView.put(null, DEFAULT_ID, DEFAULT_NAME);

        assertFalse(pojoView.replace(null, -1L, "x"));
        assertTrue(pojoView.replace(null, DEFAULT_ID, "new_name"));

        assertNull(pojoView.get(null, -1L));
        assertEquals("new_name", pojoView.get(null, DEFAULT_ID));
    }

    @Test
    public void testReplaceExact() {
        KeyValueView<Long, String> pojoView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        pojoView.put(null, DEFAULT_ID, DEFAULT_NAME);

        assertFalse(pojoView.replace(null, DEFAULT_ID, "x", "new_name"));
        assertFalse(pojoView.replace(null, -1L, "x", "new_name"));
        assertTrue(pojoView.replace(null, DEFAULT_ID, DEFAULT_NAME, "new_name2"));

        assertNull(pojoView.get(null, -1L));
        assertEquals("new_name2", pojoView.get(null, DEFAULT_ID));
    }

    @Test
    public void testGetAndReplace() {
        KeyValueView<Long, String> pojoView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        pojoView.put(null, DEFAULT_ID, DEFAULT_NAME);

        String res1 = pojoView.getAndReplace(null, DEFAULT_ID, "new_name");
        String res2 = pojoView.getAndReplace(null, 100L, "name");

        assertEquals(DEFAULT_NAME, res1);
        assertEquals("new_name", pojoView.get(null, DEFAULT_ID));

        assertNull(res2);
        assertNull(pojoView.get(null, 100L));
    }

    @Test
    public void testRemove() {
        KeyValueView<Long, String> pojoView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        pojoView.put(null, DEFAULT_ID, DEFAULT_NAME);

        boolean res1 = pojoView.remove(null, DEFAULT_ID);
        boolean res2 = pojoView.remove(null, 100L);

        assertTrue(res1);
        assertFalse(res2);

        assertNull(pojoView.get(null, DEFAULT_ID));
    }

    @Test
    public void testRemoveExact() {
        KeyValueView<Long, String> pojoView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        pojoView.put(null, DEFAULT_ID, DEFAULT_NAME);
        pojoView.put(null, 100L, "100");

        boolean res1 = pojoView.remove(null, DEFAULT_ID, "x");
        boolean res2 = pojoView.remove(null, 100L, "y");
        boolean res3 = pojoView.remove(null, 100L, "100");

        assertFalse(res1);
        assertFalse(res2);
        assertTrue(res3);

        assertNotNull(pojoView.get(null, DEFAULT_ID));
        assertNull(pojoView.get(null, 100L));
    }

    @Test
    public void testGetAndRemove() {
        KeyValueView<Long, String> pojoView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        pojoView.put(null, DEFAULT_ID, DEFAULT_NAME);

        String res1 = pojoView.getAndRemove(null, DEFAULT_ID);
        String res2 = pojoView.getAndRemove(null, 100L);

        assertEquals(DEFAULT_NAME, res1);
        assertNull(pojoView.get(null, DEFAULT_ID));

        assertNull(res2);
    }

    @Test
    public void testRemoveAll() {
        KeyValueView<Long, String> pojoView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        pojoView.putAll(null, Map.of(1L, "1", 2L, "2", 3L, "3"));

        Collection<Long> res1 = pojoView.removeAll(null, List.of(10L, 20L));
        Collection<Long> res2 = pojoView.removeAll(null, List.of(1L, 3L));

        assertEquals(2, res1.size());
        assertEquals(0, res2.size());

        assertNull(pojoView.get(null, 1L));
        assertEquals("2", pojoView.get(null, 2L));
        assertNull(pojoView.get(null, 3L));
    }

    @Test
    public void testContains() {
        KeyValueView<Long, String> pojoView = defaultTable().keyValueView(Mapper.of(Long.class), Mapper.of(String.class));

        pojoView.put(null, DEFAULT_ID, DEFAULT_NAME);

        assertTrue(pojoView.contains(null, DEFAULT_ID));
        assertFalse(pojoView.contains(null, -1L));
    }
}

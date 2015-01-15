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

package org.gridgain.grid.lang;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
@GridCommonTest(group = "Lang")
public class GridTupleSelfTest extends GridCommonAbstractTest {
    /** Creates test. */
    public GridTupleSelfTest() {
        super(/*start grid*/false);
    }

    /**
     * JUnit.
     */
    public void testGridTupleAsIterable() {
        String str = "A test string";

        Iterable<String> tpl = new GridTuple<>(str);

        Iterator<String> iter = tpl.iterator();

        assert iter != null;

        List<Object> elems = new ArrayList<>();

        while (iter.hasNext())
            elems.add(iter.next());

        assert elems.size() == 1;
        assert str.equals(elems.get(0));

        try {
            iter.next();

            fail("NoSuchElementException must have been thrown.");
        }
        catch (NoSuchElementException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     */
    public void testGridTuple2AsIterable() {
        String str1 = "A test string 1";
        String str2 = "A test string 2";

        Iterable<Object> tpl = new IgniteBiTuple<>(str1, str2);

        Iterator<Object> iter = tpl.iterator();

        assert iter != null;

        List<Object> elems = new ArrayList<>();

        while (iter.hasNext())
            elems.add(iter.next());

        assert elems.size() == 2;
        assert str1.equals(elems.get(0));
        assert str2.equals(elems.get(1));

        try {
            iter.next();

            fail("NoSuchElementException must have been thrown.");
        }
        catch (NoSuchElementException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     */
    public void testGridTuple3AsIterable() {
        String str1 = "A test string 1";
        String str2 = "A test string 2";
        String str3 = "A test string 3";

        Iterable<Object> tpl = new GridTuple3<>(str1, str2, str3);

        Iterator<Object> iter = tpl.iterator();

        assert iter != null;

        List<Object> elems = new ArrayList<>();

        while (iter.hasNext())
            elems.add(iter.next());

        assert elems.size() == 3;
        assert str1.equals(elems.get(0));
        assert str2.equals(elems.get(1));
        assert str3.equals(elems.get(2));

        try {
            iter.next();

            fail("NoSuchElementException must have been thrown.");
        }
        catch (NoSuchElementException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     */
    public void testGridTupleVAsIterable() {
        String strVal = "A test string";
        Integer intVal = 1;
        Double doubleVal = 2.5d;

        Iterable<Object> tpl = new GridTupleV(strVal, intVal, doubleVal);

        Iterator<Object> iter = tpl.iterator();

        assert iter != null;

        List<Object> elems = new ArrayList<>();

        while (iter.hasNext())
            elems.add(iter.next());

        assert elems.size() == 3;
        assert strVal.equals(elems.get(0));
        assert intVal.equals(elems.get(1));
        assert doubleVal.equals(elems.get(2));

        try {
            iter.next();

            fail("NoSuchElementException must have been thrown.");
        }
        catch (NoSuchElementException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * Helper method that checks the correctness of {@link GridMetadataAware}
     * implementation.
     *
     * @param ma Metadata aware object.
     */
    private void checkMetadataAware(GridMetadataAware ma) {
        // addMeta(name, val).
        assert ma.addMeta("attr1", "val1") == null;
        assert ma.addMeta("attr2", 1) == null;

        // hasMeta(name).
        assert ma.hasMeta("attr1");
        assert !ma.hasMeta("attr3");

        // hasMeta(name, val).
        assert ma.hasMeta("attr1", "val1");
        assert !ma.hasMeta("attr1", "some another val");

        // meta(name).
        assertEquals("val1", ma.meta("attr1"));
        assertEquals(1, ma.meta("attr2"));

        // allMeta().
        Map<String, Object> allMeta = ma.allMeta();

        assert allMeta != null;
        assert allMeta.size() == 2;

        assertEquals("val1", allMeta.get("attr1"));
        assertEquals(1, allMeta.get("attr2"));

        // addMetaIfAbsent(name, val).
        assert ma.addMetaIfAbsent("attr2", 2) == 1;
        assert ma.addMetaIfAbsent("attr3", 3) == 3;

        // addMetaIfAbsent(name, c).
        assert ma.addMetaIfAbsent("attr2", new Callable<Integer>() {
            @Override public Integer call() throws Exception {
                return 5;
            }
        }) == 1;

        assert ma.addMetaIfAbsent("attr4", new Callable<Integer>() {
            @Override public Integer call() throws Exception {
                return 5;
            }
        }) == 5;

        // removeMeta(name).
        assertEquals(3, ma.removeMeta("attr3"));
        assertEquals(5, ma.removeMeta("attr4"));

        assert ma.removeMeta("attr156") == null;

        // replaceMeta(name, newVal, curVal).
        assert !ma.replaceMeta("attr2", 5, 4);
        assert ma.replaceMeta("attr2", 1, 4);

        // copyMeta(from).
        ma.copyMeta(new GridMetadataAwareAdapter(F.<String, Object>asMap("k1", "v1", "k2", 2)));

        assertEquals("v1", ma.meta("k1"));
        assertEquals(2, ma.meta("k2"));
        assertEquals("val1", allMeta.get("attr1"));
        assertEquals(4, allMeta.get("attr2"));

        assert !ma.hasMeta("k3");

        // copyMeta(from).
        ma.copyMeta(F.asMap("1", 1, "2", 2));

        assertEquals(1, ma.meta("1"));
        assertEquals(2, ma.meta("2"));
        assertEquals("v1", ma.meta("k1"));
        assertEquals(2, ma.meta("k2"));
        assertEquals("val1", allMeta.get("attr1"));
        assertEquals(4, allMeta.get("attr2"));

        assert !ma.hasMeta("3");
    }
}

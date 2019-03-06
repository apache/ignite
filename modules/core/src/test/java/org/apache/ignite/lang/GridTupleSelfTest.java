/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.lang;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

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
    @Test
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
    @Test
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
    @Test
    public void testGridTuple2AsMap() {
        String str1 = "A test string 1";
        String str2 = "A test string 2";

        IgniteBiTuple<String, String> tpl = new IgniteBiTuple<>();

        tpl.put(str1, str2);

        assertEquals(str2, tpl.get(str1));
        assertEquals(1, tpl.size());

        assert tpl.containsKey(str1);
        assert tpl.containsValue(str2);

        Iterator<Map.Entry<String, String>> it = tpl.entrySet().iterator();

        assert it.hasNext();

        Map.Entry<String, String> next = it.next();

        assertEquals(str1, next.getKey());
        assertEquals(str2, next.getValue());

        assert !it.hasNext();

        next = F.firstEntry(tpl);

        assertEquals(str1, next.getKey());
        assertEquals(str2, next.getValue());

        tpl = new IgniteBiTuple<>();

        assert !tpl.entrySet().iterator().hasNext();
    }

    /**
     * JUnit.
     */
    @Test
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
}

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

package org.apache.ignite.lang;

import java.util.concurrent.Callable;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 *
 */
@GridCommonTest(group = "Lang")
public class GridMetadataAwareAdapterSelfTest extends GridCommonAbstractTest {
    /** Creates test. */
    public GridMetadataAwareAdapterSelfTest() {
        super(/*start grid*/false);
    }

    /**
     * Junit.
     */
    @SuppressWarnings({"AssertWithSideEffects"})
    public void test() {
        GridMetadataAwareAdapter ma = new GridMetadataAwareAdapter();

        int cnt = 0;

        int attr1 = cnt++;
        int attr2 = cnt++;
        int attr3 = cnt++;
        int attr4 = cnt++;
        int attr156 = cnt++;
        int k1 = cnt++;
        int k2 = cnt++;
        int k3 = cnt++;
        int a1 = cnt++;
        int a2 = cnt++;
        int a3 = cnt;

        // addMeta(name, val).
        assert ma.addMeta(attr1, "val1") == null;
        assert ma.addMeta(attr2, 1) == null;

        // hasMeta(name).
        assert ma.hasMeta(attr1);
        assert !ma.hasMeta(attr3);

        // hasMeta(name, val).
        assert ma.hasMeta(attr1, "val1");
        assert !ma.hasMeta(attr1, "some another val");

        // meta(name).
        assertEquals("val1", ma.meta(attr1));
        assertEquals(new Integer(1), ma.meta(attr2));

        // allMeta().
        Object[] allMeta = ma.allMeta();

        assert allMeta != null;

        assertEquals("val1", allMeta[attr1]);
        assertEquals(1, allMeta[attr2]);

        // addMetaIfAbsent(name, val).
        assert ma.addMetaIfAbsent(attr2, 2) == 1;
        assert ma.addMetaIfAbsent(attr3, 3) == 3;

        // addMetaIfAbsent(name, c).
        assert ma.addMetaIfAbsent(attr2, new Callable<Integer>() {
            @Override public Integer call() throws Exception {
                return 5;
            }
        }) == 1;

        assert ma.addMetaIfAbsent(attr4, new Callable<Integer>() {
            @Override public Integer call() throws Exception {
                return 5;
            }
        }) == 5;

        // removeMeta(name).
        assertEquals(new Integer(3), ma.removeMeta(attr3));
        assertEquals(new Integer(5), ma.removeMeta(attr4));

        assert ma.removeMeta(attr156) == null;

        // replaceMeta(name, newVal, curVal).
        assert !ma.replaceMeta(attr2, 5, 4);
        assert ma.replaceMeta(attr2, 1, 4);

        // copyMeta(from).
        GridMetadataAwareAdapter adapter = new GridMetadataAwareAdapter();
        adapter.addMeta(k1, "v1");
        adapter.addMeta(k2, 2);

        ma.copyMeta(adapter);

        allMeta = ma.allMeta();

        assertEquals("v1", ma.meta(k1));
        assertEquals(new Integer(2), ma.meta(k2));
        assertEquals("val1", allMeta[attr1]);
        assertEquals(4, allMeta[attr2]);

        assert !ma.hasMeta(k3);

        // copyMeta(from).
        Object[] objs = new Object[20];

        objs[a1] = 1;
        objs[a2] = 2;
        objs[19] = 19;

        ma.copyMeta(objs);

        assertEquals(new Integer(1), ma.meta(a1));
        assertEquals(new Integer(2), ma.meta(a2));
        assertEquals("v1", ma.meta(k1));
        assertEquals(new Integer(2), ma.meta(k2));
        assertEquals("val1", allMeta[attr1]);
        assertEquals(4, allMeta[attr2]);
        assertEquals(19, 19);

        assert !ma.hasMeta(a3);
    }
}

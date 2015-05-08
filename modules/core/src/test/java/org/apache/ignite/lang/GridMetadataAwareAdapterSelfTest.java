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

import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

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

        UUID attr1 = UUID.randomUUID();
        UUID attr2 = UUID.randomUUID();
        UUID attr3 = UUID.randomUUID();
        UUID attr4 = UUID.randomUUID();
        UUID attr156 = UUID.randomUUID();
        UUID k1 = UUID.randomUUID();
        UUID k2 = UUID.randomUUID();
        UUID k3 = UUID.randomUUID();
        UUID a1 = UUID.randomUUID();
        UUID a2 = UUID.randomUUID();
        UUID a3 = UUID.randomUUID();

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
        Map<UUID, Object> allMeta = ma.allMeta();

        assert allMeta != null;
        assert allMeta.size() == 2;

        assertEquals("val1", allMeta.get(attr1));
        assertEquals(1, allMeta.get(attr2));

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
        ma.copyMeta(new GridMetadataAwareAdapter(F.<UUID, Object>asMap(k1, "v1", k2, 2)));

        assertEquals("v1", ma.meta(k1));
        assertEquals(new Integer(2), ma.meta(k2));
        assertEquals("val1", allMeta.get(attr1));
        assertEquals(4, allMeta.get(attr2));

        assert !ma.hasMeta(k3);

        // copyMeta(from).
        ma.copyMeta(F.asMap(a1, 1, a2, 2));

        assertEquals(new Integer(1), ma.meta(a1));
        assertEquals(new Integer(2), ma.meta(a2));
        assertEquals("v1", ma.meta(k1));
        assertEquals(new Integer(2), ma.meta(k2));
        assertEquals("val1", allMeta.get(attr1));
        assertEquals(4, allMeta.get(attr2));

        assert !ma.hasMeta(a3);
    }
}

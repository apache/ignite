/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class RobinHoodHashTest {
    private boolean dump = false;

    private void withMap(Consumer<RobinHoodBackwardShiftHashMap> tester, int cap) {
        long addr = GridUnsafe.allocateMemory(cap * RobinHoodBackwardShiftHashMap.BYTES_PER_ENTRY);

        RobinHoodBackwardShiftHashMap map = new RobinHoodBackwardShiftHashMap(addr, cap);
        try {
            tester.accept(map);
        }
        finally {
            System.err.println(map.dump());

            GridUnsafe.freeMemory(addr);
        }
    }

    @Test
    public void testSimplestPutGet() throws Exception {
        withMap(map -> {
                for (int i = 0; i < 100; i++) {
                    int grpId = i + 1;
                    int val = grpId * grpId;
                    assertTrue("Unique put should be successful " + grpId, map.put(grpId, 1, val, 1));
                    assertEquals(val, map.get(grpId, 1, 0, -1, -2));

                    assertFalse("Duplicate put for " + grpId, map.put(grpId, 1, 1, 1));
                    assertEquals(1, map.get(grpId, 1, 0, -1, -2));
                }
            }
            , 100);
    }

    @Test
    public void testSimplestOverflow() throws Exception {
        withMap(map -> {
                for (int i = 0; i < 10; i++) {
                    int grpId = i + 1;
                    int val = grpId * grpId;
                    assertTrue("Unique put should be successful " + grpId, map.put(grpId, 1, val, 1));

                    assertEquals(val, map.get(grpId, 1, 0, -1, -2));

                    assertFalse("Duplicate put for " + grpId, map.put(grpId, 1, 1, 1));
                    assertEquals(1, map.get(grpId, 1, 0, -1, -2));
                }

                boolean put = map.put(11, 1, 11, 1);
                assertFalse(put);
            }
            , 10);
    }

    @Test
    public void testPutRemoveOnSamePlaces() throws Exception {
        withMap(map -> {
                doAddRemove(map);

                //fill with 1 space left;
                for (int i = 0; i < 99; i++) {
                    int grpId = i + 1;
                    int val = grpId * grpId;
                    assertTrue("Unique put should be successful " + grpId, map.put(grpId, 1, val, 1));
                }

                doAddRemove(map);
            }
            , 100);
    }

    private void doAddRemove(RobinHoodBackwardShiftHashMap map) {
        for (int i = 0; i < 100; i++) {
            int grpId = i + 1;
            int val = grpId * grpId;
            map.put(grpId, 1, val, 1);
            assertEquals(val, map.get(grpId, 1, 0, -1, -2));

            assertTrue(map.remove(grpId, 1, 1));
            assertEquals(-1, map.get(grpId, 1, 0, -1, -2));
        }
    }

    @Test
    public void testCollisionOnRemove() {
        LinkedHashMap<FullPageId, Long> control = new LinkedHashMap<>();
        int elem = 10;
        int cap = elem;
        FullPageId baseId = new FullPageId(0, 1);

        withMap(map -> {
            for (int i = 0; i < elem; i++) {
                int grpId = i + 1;
                int pageId = findPageIdForCollision(grpId, baseId, cap);
                long val = grpId;
                control.put(new FullPageId(pageId, grpId), val);
                map.put(grpId, pageId, val, 1);
            }
            for (FullPageId next : control.keySet()) {
                assertTrue(map.remove(next.groupId(), next.pageId(), 1));
            }
        }, cap);
    }

    private int findPageIdForCollision(int grpId, FullPageId id, int cap) {
        int bucket = U.safeAbs(id.hashCode()) % cap;

        for (int p = 0; p < 1_000_000; p++) {
            if (U.safeAbs(FullPageId.hashCode(grpId, p)) % cap == bucket)
                return p;
        }
        assertTrue(false);
        return -1;
    }

    @Test
    public void testRandomOpsPutRemove() {
        doPutRemoveTest(1);
    }

    private void doPutRemoveTest(long seed) {
        int elementsCnt = 10_000;

        System.setProperty(IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR, "11");

        withMap(tbl -> {
            Random rnd = new Random(seed);

            Map<FullPageId, Long> check = new HashMap<>();

            int tag = 0;
            for (int i = 0; i < 1_000_000; i++) {
                int op = rnd.nextInt(5);

                int cacheId = rnd.nextInt(100) + 1;
                int pageId = rnd.nextInt(100);

                FullPageId fullId = new FullPageId(pageId, cacheId);

                if (op == 0) {
                    long val = tbl.get(cacheId, pageId, tag, -1, -2);
                    if (val == -2)
                        tbl.refresh(cacheId, pageId, tag);
                    else {
                        Long checkVal = check.get(fullId);

                        if (checkVal != null) {
                            assertEquals("Ret." +
                                    getPageString(fullId) +
                                    " tbl: " + val + " Check " + checkVal,
                                checkVal.longValue(), val);
                        }
                    }
                }
                else if ((op == 1 || op == 2) && (check.size() < elementsCnt)) {
                    long val = U.safeAbs(rnd.nextInt(30));

                    Long prevPut = check.put(fullId, val);
                    boolean prevValuePresent = prevPut != null;
                    tbl.put(cacheId, pageId, val, tag);

                    if (dump)
                        System.out.println("put " + getPageString(fullId) + " -> " + val);
                }
                else if ((op == 3) && check.size() >= elementsCnt * 2 / 3) {
                    tbl.remove(cacheId, pageId, tag);
                    check.remove(fullId);


                    System.out.println("remove " + getPageString(fullId) + " ");
                }
                else if (check.size() >= elementsCnt * 2 / 3) {
                    ReplaceCandidate ec = null; //tbl.getNearestAt(rnd.nextInt(tbl.capacity()));
                    if (ec != null) {
                        FullPageId fullPageId = ec.fullId();

                        tbl.remove(fullPageId.groupId(), fullPageId.pageId(), ec.tag());

                        check.remove(fullPageId);
                    }
                }


                if (i > 0 && i % 100_000 == 0) {
                   /*                IntervalBasedMeasurement avgPutSteps = U.field(tbl, "avgPutSteps");
                     info("Done: " + i
                        + " Size: " + check.size()
                        + " Capacity: " + tbl.capacity()
                        + " avg steps " + avgPutSteps.getAverage());
*/
                    //verifyLinear(tbl, check);

                    // tag++;
                }
                i++;

                // if(avgPutSteps.getAverage()>2000)
                //    break;
            }

//            IntervalBasedMeasurement avgPutSteps = U.field(tbl, "avgPutSteps");
   //         System.out.println("Average put required: " + avgPutSteps.getAverage());
        }, elementsCnt);
    }

    @NotNull private String getPageString(FullPageId fullId) {
        StringBuilder sb = new StringBuilder();
        sb.append("(grp=").append(fullId.groupId()).append(",");
        sb.append("page=").append(fullId.pageId()).append(")");

        return sb.toString();
    }
}

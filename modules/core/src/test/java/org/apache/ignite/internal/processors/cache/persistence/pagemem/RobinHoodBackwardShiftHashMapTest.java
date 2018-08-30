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
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests of {@link RobinHoodBackwardShiftHashMap} implementation.
 */
public class RobinHoodBackwardShiftHashMapTest {
    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = new Timeout((int) GridTestUtils.DFLT_TEST_TIMEOUT);

    /**
     * @param tester map test code
     * @param cap required map capacity.
     */
    private void withMap(Consumer<RobinHoodBackwardShiftHashMap> tester, int cap) {
        long memSize = RobinHoodBackwardShiftHashMap.requiredMemoryByBuckets(cap);
        long addr = GridUnsafe.allocateMemory(memSize);

        RobinHoodBackwardShiftHashMap map = new RobinHoodBackwardShiftHashMap(addr, memSize);
        boolean success = false;
        try {
            tester.accept(map);

            success = true;
        }
        finally {
            if (!success)
                System.err.println(map.dump());

            GridUnsafe.freeMemory(addr);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testShortSize() throws Exception {
        withMap(map -> {
            map.put(1, 1, 0, 0);
            map.put(2, 0, 1, 1);
            map.remove(1, 1);
        }, 2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimplestPutGet() throws Exception {
        int cnt = 100;
        withMap(map -> {
                for (int i = 0; i < cnt; i++) {
                    int grpId = i + 1;
                    int val = grpId * grpId;

                    assertSizeChanged("Unique put should be successful " + grpId,
                        map, () -> map.put(grpId, 1, val, 1));
                    assertEquals(val, map.get(grpId, 1, 0, -1, -2));

                    assertSizeNotChanged("Duplicate put for " + grpId,
                        map, () -> map.put(grpId, 1, 1, 1));
                    assertEquals(1, map.get(grpId, 1, 0, -1, -2));
                }

                assertEquals(cnt, map.size());
            }
            , cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test(expected = IgniteOutOfMemoryException.class)
    public void testSimplestOverflow() throws Exception {
        withMap(map -> {
                for (int i = 0; i < 10; i++) {
                    int grpId = i + 1;
                    int val = grpId * grpId;
                    assertSizeChanged("Unique put should be successful [" + grpId + "]", map, () -> map.put(grpId, 1, val, 1));

                    assertEquals(val, map.get(grpId, 1, 0, -1, -2));

                    assertSizeNotChanged("Duplicate put for " + grpId, map, () -> map.put(grpId, 1, 1, 1));
                    assertEquals(1, map.get(grpId, 1, 0, -1, -2));
                }

                map.put(11, 1, 11, 1);
            }
            , 10);
    }

    /**
     * @param msg message to dump in case assertion failed.
     * @param map page map to check.
     * @param act action during which size is expected to be changed.
     */
    private static void assertSizeChanged(String msg, LoadedPagesMap map, Runnable act) {
        int size = map.size();
        act.run();
        int newSize = map.size();

        assertNotEquals(msg, size, newSize);
    }

    /**
     * @param msg message to dump in case assertion failed.
     * @param map page map to check.
     * @param act action during which size is expected to constant.
     */
    private static void assertSizeNotChanged(String msg, LoadedPagesMap map, Runnable act) {
        int size = map.size();
        act.run();
        int newSize = map.size();

        assertEquals(msg, size, newSize);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutRemoveOnSamePlaces() throws Exception {
        withMap(map -> {
                doAddRemove(map);

                //fill with 1 space left;
                for (int i = 0; i < 99; i++) {
                    int grpId = i + 1;
                    int val = grpId * grpId;
                    assertSizeChanged("Unique put should be successful " + grpId, map,
                        () -> map.put(grpId, 1, val, 1));
                }

                doAddRemove(map);
            }
            , 100);
    }

    /**
     * @param map tested map implementation
     */
    private void doAddRemove(LoadedPagesMap map) {
        for (int i = 0; i < 100; i++) {
            int grpId = i + 1;
            int val = grpId * grpId;
            map.put(grpId, 1, val, 1);
            assertEquals(val, map.get(grpId, 1, 0, -1, -2));

            assertTrue(map.remove(grpId, 1));
            assertEquals(-1, map.get(grpId, 1, 0, -1, -2));
        }
    }

    /**
     *
     */
    @Test
    public void testCollisionOnRemove() {
        Map<FullPageId, Long> ctrl = new LinkedHashMap<>();
        int cap = 10;
        FullPageId baseId = new FullPageId(0, 1);

        withMap(map -> {
            for (int i = 0; i < cap; i++) {
                int grpId = i + 1;
                int pageId = findPageIdForCollision(grpId, baseId, cap);
                ctrl.put(new FullPageId(pageId, grpId), (long)grpId);
                map.put(grpId, pageId, (long)grpId, 1);
            }
            for (FullPageId next : ctrl.keySet()) {
                assertTrue(map.remove(next.groupId(), next.pageId()));
            }
        }, cap);
    }

    /**
     * @param grpId Group ID to use
     * @param id Page to be placed to same bucket with
     * @param cap map maximum cells.
     * @return page ID to use in addition to provided {@code grpId} to reach collision.
     */
    private int findPageIdForCollision(int grpId, FullPageId id, int cap) {
        int bucket = U.safeAbs(id.hashCode()) % cap;

        for (int p = 0; p < 1_000_000; p++) {
            if (U.safeAbs(FullPageId.hashCode(grpId, p)) % cap == bucket)
                return p;
        }
        assertTrue(false);
        return -1;
    }

    /**
     *
     */
    @Test
    public void testRandomOpsPutRemove() {
        doPutRemoveTest(System.currentTimeMillis());
    }

    /**
     * @param seed random seed, use timer to random run.
     */
    private void doPutRemoveTest(long seed) {
        System.setProperty(IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR, "11");

        int elementsCnt = 10_000;

        withMap(tbl -> {
            Random rnd = new Random(seed);

            Map<FullPageId, Long> check = new HashMap<>();

            int tag = 0;
            for (int i = 0; i < 20_000_000; i++) {
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

                    check.put(fullId, val);

                    tbl.put(cacheId, pageId, val, tag);
                }
                else if ((op == 3) && check.size() >= elementsCnt * 2 / 3) {
                    tbl.remove(cacheId, pageId);

                    check.remove(fullId);
                }
                else if (check.size() >= elementsCnt * 2 / 3) {
                    ReplaceCandidate ec = tbl.getNearestAt(rnd.nextInt(tbl.capacity()));

                    if (ec != null) {
                        FullPageId fullPageId = ec.fullId();

                        tbl.remove(fullPageId.groupId(), fullPageId.pageId());

                        check.remove(fullPageId);
                    }
                }

                i++;
            }

        }, elementsCnt);
    }

    /**
     * @param fullId page ID.
     * @return Printable string for page ID.
     */
    @NotNull private String getPageString(FullPageId fullId) {
        return "(grp=" + fullId.groupId() + "," +
            "page=" + fullId.pageId() + ")";
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAndCantGetOutdatedValue() throws Exception {
        withMap(map -> {
            //fill with 1 space left;
            for (int i = 0; i < 99; i++) {
                int ver = i;
                int grpId = ver + 1;
                int val = grpId * grpId;
                map.put(grpId, 1, val, ver);

                assertEquals(val, map.get(grpId, 1, ver, -1, -2));

                assertEquals(-2, map.get(grpId, 1, ver + 1, -1, -2));
            }
        }, 100);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAndRefreshValue() throws Exception {
        withMap(map -> {
            //fill with 1 space left;
            for (int i = 0; i < 99; i++) {
                int ver = i;
                int grpId = ver + 1;
                int val = grpId * grpId;
                int pageId = 1;
                map.put(grpId, pageId, val, ver);

                map.refresh(grpId, pageId, ver + 1);

                assertEquals(val, map.get(grpId, pageId, ver + 1, -1, -2));

            }

            doAddRemove(map);
        }, 100);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClearAtWithControlMap3() throws Exception {
        int cap = 100;

        doRemovalTests(cap, (grpId, pageId) -> {
            int hc = Integer.hashCode(grpId) + 31 * Long.hashCode(pageId);

            return hc % 3 == 0;
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClearAtWithControlMap7() throws Exception {
        int cap = 100;

        doRemovalTests(cap, (grpId, pageId) -> {
            int hc = Integer.hashCode(grpId) + 31 * Long.hashCode(pageId);

            return hc % 7 == 0;
        });
    }

    /**
     * @throws Exception  If failed.
     */
    @Test
    public void testClearAllWithControlMap() throws Exception {
        int cap = 100;

        doRemovalTests(cap, (grpId, pageId) -> true);
    }

    /**
     * @param cap capacity of map
     * @param pred predicate to filter entries to be removed.
     */
    private void doRemovalTests(int cap, LoadedPagesMap.KeyPredicate pred) {
        withMap(map -> {
            Map<FullPageId, Long> check = new HashMap<>();

            int elems = cap - 1;  //fill with 1 space left;
            for (int i = 0; i < elems; i++) {
                int grpId = i + 1;
                long val = grpId * grpId;
                int pageId = 1;

                map.put(grpId, pageId, val, i);

                check.put(new FullPageId(pageId, grpId), val);
            }

            int sz = map.size();

            GridLongList list = map.removeIf(pred);
            for (int i = 0; i < list.size(); i++) {
                long val = list.get(i);

                assertTrue(val > 0);
            }

            assertEquals(sz - map.size(), list.size());

            Map<FullPageId, Long> res = new HashMap<>();

            map.forEach(res::put);

            for (int i = 0; i < cap; i++) {
                GridLongList list1 = map.removeIf(i, i + 1, pred);

                assertTrue(list1.isEmpty());  // clear should not be successful.
            }

            check.keySet().removeIf(entry -> pred.test(entry.groupId(), entry.pageId()));

            assertEquals(res, check);

        }, cap);
    }
}

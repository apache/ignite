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
import java.util.Map;
import java.util.Random;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.java.JavaLogger;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class FullPageIdTableTest  {
    /** */
    private static final int CACHE_ID_RANGE = 1;

    /** */
    private static final int PAGE_ID_RANGE = 3000;

    /** */
    private static final int CACHE_ID_RANGE2 = 1024;

    /** */
    private static final int PAGE_ID_RANGE2 = 3000;

    /** Logger. */
    private JavaLogger log = new JavaLogger();

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRandomOperations() throws Exception {
        int cnt = CACHE_ID_RANGE * PAGE_ID_RANGE;

        long mem = FullPageIdTable.requiredMemory(cnt);

        UnsafeMemoryProvider prov = new UnsafeMemoryProvider(log);

        prov.initialize(new long[] {mem});

        DirectMemoryRegion region = prov.nextRegion();

        try {
            long seed = U.currentTimeMillis();

            info("Seed: " + seed + "L; //");

            Random rnd = new Random(seed);

            FullPageIdTable tbl = new FullPageIdTable(region.address(), region.size(), true);

            Map<FullPageId, Long> check = new HashMap<>();

            for (int i = 0; i < 10_000; i++) {
                int cacheId = rnd.nextInt(CACHE_ID_RANGE) + 1;
                int pageId = rnd.nextInt(PAGE_ID_RANGE);

                FullPageId fullId = new FullPageId(pageId, cacheId);

                boolean put = rnd.nextInt(3) != -1;

                if (put) {
                    long val = rnd.nextLong();

                    tbl.put(cacheId, pageId, val, 0);
                    check.put(fullId, val);
                }
                else {
                    tbl.remove(cacheId, pageId, 0);
                    check.remove(fullId);
                }

                verifyLinear(tbl, check);

                if (i > 0 && i % 1000 == 0)
                    info("Done: " + i);
            }
        }
        finally {
            prov.shutdown();
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void putRemoveScenario() throws Exception {
        long seed   = U.currentTimeMillis();
        doPutRemoveTest(seed);
    }

    @Test
    public void putRemoveScenarioFixedSeed() throws Exception {
        long seed = 1518082843319L;

        doPutRemoveTest(seed);
    }

    private void doPutRemoveTest(long seed) {
        int elementsCnt = 7000;

        System.setProperty(IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR, "11");

        long mem = FullPageIdTable.requiredMemory(elementsCnt);

        DirectMemoryProvider prov = new UnsafeMemoryProvider(log);

        prov.initialize(new long[] {mem});

        DirectMemoryRegion region = prov.nextRegion();

        try {
            info("Seed: " + seed + "L; //");

            Random rnd = new Random(seed);

            FullPageIdTable tbl = new FullPageIdTable(region.address(), region.size(), true);

            Map<FullPageId, Long> check = new HashMap<>();

            int tag = 0;
            for (int i = 0; i < 30_000_000 ; i++) {
                int op = rnd.nextInt(5);

                int cacheId = rnd.nextInt(CACHE_ID_RANGE2) + 1;
                int pageId = rnd.nextInt(PAGE_ID_RANGE2  );

                FullPageId fullId = new FullPageId(pageId, cacheId);

                if (op == 0) {
                    long val = tbl.get(cacheId, pageId, tag, -1, -2);
                    if (val == -2)
                        tbl.refresh(cacheId, pageId, tag);
                    else {
                        Long checkVal = check.get(fullId);

                        if (checkVal != null) {
                            assertEquals("Ret " + val + "Check " + checkVal,
                                checkVal.longValue(), val);
                        }
                    }

                }
                else if ((op == 1 || op == 2) && (check.size() < elementsCnt)) {
                    long val = U.safeAbs(rnd.nextLong());

                    tbl.put(cacheId, pageId, val, tag);
                    check.put(fullId, val);
                }
                else if ((op == 3) && check.size() >= elementsCnt * 2 / 3) {
                    tbl.remove(cacheId, pageId, tag);
                    check.remove(fullId);
                }
                else if (check.size() >= elementsCnt * 2 / 3) {
                    int idx = rnd.nextInt(tbl.capacity());
                    EvictCandidate ec = tbl.getNearestAt(idx, -2);
                    if (ec != null) {
                        FullPageId fullPageId = ec.fullId();

                        tbl.remove(fullPageId.groupId(), fullPageId.pageId(), ec.tag());

                        check.remove(fullPageId);
                    }
                }

                IntervalBasedMeasurement avgPutSteps = U.field(tbl, "avgPutSteps");
                if (i > 0 && i % 100_000 == 0) {
                    info("Done: " + i
                        + " Size: " + check.size()
                        + " Capacity: " + tbl.capacity()
                        + " avg steps " + avgPutSteps.getAverage());

                    verifyLinear(tbl, check);

                    tag++;
                }
                i++;


               // if(avgPutSteps.getAverage()>2000)
                //    break;
            }

            verifyLinear(tbl, check);
            IntervalBasedMeasurement avgPutSteps = U.field(tbl, "avgPutSteps");
            System.out.println("Average put required: " + avgPutSteps.getAverage());
        }
        finally {
            long msPassed = U.currentTimeMillis() - seed;
            System.err.println("Seed used [" + seed + "] duration ["+ msPassed+ "] ms");
            prov.shutdown();
        }
    }

    /**
     * @param msg Message to print.
     */
    protected void info(String msg) {
        if (log.isInfoEnabled())
            log.info(msg);

        System.out.println(msg);
    }


    /**
     * @param tbl Table to check.
     * @param check Expected mapping.
     */
    private void verifyLinear(FullPageIdTable tbl, Map<FullPageId, Long> check) {
        final Map<FullPageId, Long> tblSnapshot = new HashMap<>();

        tbl.visitAll(new CI2<FullPageId, Long>() {
            @Override public void apply(FullPageId fullId, Long val) {
                if (tblSnapshot.put(fullId, val) != null)
                    throw new AssertionError("Duplicate full page ID mapping: " + fullId);
            }
        });

        int chkSize = check.size();
        int foundTblSize = tblSnapshot.size();

        HashMap<FullPageId, Long> cp = new HashMap<>(tblSnapshot);
        check.keySet().forEach(cp::remove);

        if (chkSize != foundTblSize) {
            tbl.lastDump.set(0);
            tbl.dumpIfNeed();
        }
        assertEquals("Size check failed, check map size " +
            chkSize + " but found in table " + foundTblSize + " elements," +
            " table size " + tbl.size() +
            "\n Difference: " + cp, chkSize, foundTblSize);

        for (Map.Entry<FullPageId, Long> entry : check.entrySet()) {
            Long valCheck = entry.getValue();
            Long actual = tblSnapshot.get(entry.getKey());

            if (!valCheck.equals(actual))
                assertEquals("Mapping comparison failed for key: " + entry.getKey(),
                    valCheck, actual);
        }
    }
}

/*
 *                    GridGain Community Edition Licensing
 *                    Copyright 2019 GridGain Systems, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 *  Restriction; you may not use this file except in compliance with the License. You may obtain a
 *  copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 *  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the specific language governing permissions
 *  and limitations under the License.
 *
 *  Commons Clause Restriction
 *
 *  The Software is provided to you by the Licensor under the License, as defined below, subject to
 *  the following condition.
 *
 *  Without limiting other conditions in the License, the grant of rights under the License will not
 *  include, and the License does not grant to you, the right to Sell the Software.
 *  For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 *  under the License to provide to third parties, for a fee or other consideration (including without
 *  limitation fees for hosting or consulting/ support services related to the Software), a product or
 *  service whose value derives, entirely or substantially, from the functionality of the Software.
 *  Any license notice or attribution required by the License must also include this Commons Clause
 *  License Condition notice.
 *
 *  For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 *  the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 *  Edition software provided with this notice.
 */

package org.apache.ignite.lang;

import java.util.UUID;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.eviction.GridCacheMockEntry;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Check how much memory and time required to fill 1_000_000 entries with meta.
 * Do not include this test to suits.
 */
@RunWith(JUnit4.class)
public class GridMetadataAwareAdapterLoadTest extends GridCommonAbstractTest {
    /** Creates test. */
    public GridMetadataAwareAdapterLoadTest() {
        super(/*start grid*/false);
    }

    private static final String KEY_VALUE = "test";

    /**
     * Junit.
     *
     * @throws Exception
     */
    @Test
    public void test() throws Exception {
        String[] dic = new String[1_000_000];

        for (int i = 0; i < 1_000_000; i++)
            dic[i] = String.valueOf(i);

        doTest(-1, "all 10 keys     ", dic);
        doTest(0, "all 3 keys      ", dic);
        doTest(1, "first key only  ", dic);
        doTest(2, "second key only ", dic);
        doTest(3, "third key only  ", dic);
        doTest(4, "tenth key only  ", dic);
        doTest(5, "random (1-3) key", dic);
        doTest(6, "no meta         ", dic);
    }

    public void doTest(int c, String message, String[] dic) throws IgniteInterruptedCheckedException {
        UUID[] uuids = new UUID[10];

        for (int j = 0; j < 10; j++)
            uuids[j] = UUID.randomUUID();

        for (int t = 0; t < 2; t++) {
            long mTotal = 0;
            long tTotal = 0;

            for (int k = 0; k < 20; k++) {
                System.gc();

                U.sleep(500);

                GridCacheMockEntry[] entries = new GridCacheMockEntry[1_000_000];

                long mBefore = Runtime.getRuntime().freeMemory();
                long tBefore = System.currentTimeMillis();

                for (int i = 0; i < 1_000_000; i++) {
                    GridCacheMockEntry<String, String> entry = new GridCacheMockEntry<>(KEY_VALUE);
                    switch (c) {//commented lines for old API
                        case -1:
                            for (int j = 9; j >= 0; j--)
                                //entry.addMeta(uuids[j], dic[i]);
                            entry.addMeta(j, dic[i]);
                            break;
                        case 0:
                            for (int j = 2; j >= 0; j--)
                                //entry.addMeta(uuids[j], dic[i]);
                            entry.addMeta(j, dic[i]);
                            break;
                        case 1:
                            //entry.addMeta(uuids[0], dic[i]);
                            entry.addMeta(0, dic[i]);
                            break;
                        case 2:
                            //entry.addMeta(uuids[1], dic[i]);
                            entry.addMeta(1, dic[i]);
                            break;
                        case 3:
                            //entry.addMeta(uuids[2], dic[i]);
                            entry.addMeta(2, dic[i]);
                            break;
                        case 4:
                            //entry.addMeta(uuids[9], dic[i]);
                            entry.addMeta(9, dic[i]);
                            break;
                        case 5:
                            //entry.addMeta(uuids[i % 3], dic[i]);
                            entry.addMeta(i % 3, dic[i]);
                            break;
                        case 6:

                            break;
                    }

                    entries[i] = entry;
                }

                long mAfter = Runtime.getRuntime().freeMemory();
                long tAfter = System.currentTimeMillis();

                if (k >= 10) {
                    mTotal += (mBefore - mAfter);
                    tTotal += (tAfter - tBefore);
                }
            }

            log.info(message + " [time=" + tTotal + " ms, memory=" + mTotal / 1_000_000_0 + "." + mTotal % 1_000_000_0 + " mb]");
        }

        log.info(" ");
    }
}

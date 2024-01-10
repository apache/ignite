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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class ClockPageReplacementFlagsTest extends GridCommonAbstractTest {
    /** Max pages count. */
    private static final int MAX_PAGES_CNT = 1000;

    /** Memory provider. */
    private static DirectMemoryProvider provider;

    /** Memory region. */
    private static DirectMemoryRegion region;

    /** Clock replacement algorithm implementation. */
    ClockPageReplacementFlags clockFlags;

    /** */
    @BeforeClass
    public static void setUp() {
        provider = new UnsafeMemoryProvider(log);
        provider.initialize(new long[] {ClockPageReplacementFlags.requiredMemory(MAX_PAGES_CNT)});

        region = provider.nextRegion();
    }

    /** */
    @AfterClass
    public static void tearDown() {
        provider.shutdown(true);
    }

    /**
     * Test setFlag() and getFlag() methods.
     */
    @Test
    public void testSetGet() {
        clockFlags = new ClockPageReplacementFlags(MAX_PAGES_CNT, region.address());

        setRange(50, 100);

        for (int i = 0; i < MAX_PAGES_CNT; i++)
            assertEquals("Unexpectd value of " + i + " item", i >= 50 && i <= 100, clockFlags.getFlag(i));
    }


    /**
     * Test poll() method.
     */
    @Test
    public void testPoll() {
        clockFlags = new ClockPageReplacementFlags(MAX_PAGES_CNT, region.address());

        setRange(2, 2);
        setRange(4, 7);
        setRange(9, 254);
        setRange(256, 320);
        setRange(322, 499);
        setRange(501, MAX_PAGES_CNT - 1);

        assertEquals(0, clockFlags.poll());
        assertEquals(1, clockFlags.poll());
        assertEquals(3, clockFlags.poll());
        assertEquals(8, clockFlags.poll());
        assertEquals(255, clockFlags.poll());
        assertEquals(321, clockFlags.poll());
        assertEquals(500, clockFlags.poll());

        setRange(0, 1);

        assertEquals(2, clockFlags.poll());
        assertEquals(3, clockFlags.poll());

        setRange(4, MAX_PAGES_CNT - 2);

        assertEquals(MAX_PAGES_CNT - 1, clockFlags.poll());
        assertEquals(0, clockFlags.poll());

        for (int i = 0; i < MAX_PAGES_CNT; i++)
            assertEquals("Unexpectd value of " + i + " item", false, clockFlags.getFlag(i));
    }

    /**
     * Set flag for range of page indexes.
     *
     * @param fromIdx From index.
     * @param toIdx To index.
     */
    private void setRange(int fromIdx, int toIdx) {
        for (int i = fromIdx; i <= toIdx; i++)
            clockFlags.setFlag(i);
    }
}

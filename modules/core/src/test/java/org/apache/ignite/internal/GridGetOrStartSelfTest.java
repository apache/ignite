/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.testframework.junits.common.*;
import org.junit.Test;

/**
 * The GirdGetOrStartSelfTest tests get or start semantics. See IGNITE-2941
 */

@GridCommonTest(group = "Kernal Self")
public class GridGetOrStartSelfTest extends GridCommonAbstractTest {
    /**
     * Default constructor.
     */
    public GridGetOrStartSelfTest() {
        super(false);
    }

    /**
     * Tests default Ignite instance
     */
    @Test
    public void testDefaultIgniteInstanceGetOrStart() throws Exception {
        IgniteConfiguration cfg = getConfiguration(null);
        try(Ignite ignite = Ignition.getOrStart(cfg)) {
            try {
                Ignition.start(cfg);
                fail("Expected exception after grid started");
            }
            catch (IgniteException ignored) {
            }
            Ignite ignite2 = Ignition.getOrStart(cfg);
            assertEquals("Must return same instance", ignite, ignite2);
        }
    }

    /**
     * Tests named Ignite instance
     */
    @Test
    public void testNamedIgniteInstanceGetOrStart() throws Exception {
        IgniteConfiguration cfg = getConfiguration("test");
        try(Ignite ignite = Ignition.getOrStart(cfg)) {
            try {
                Ignition.start(cfg);
                fail("Expected exception after grid started");
            }
            catch (IgniteException ignored) {
            }
            Ignite ignite2 = Ignition.getOrStart(cfg);
            assertEquals("Must return same instance", ignite, ignite2);
        }
    }
}

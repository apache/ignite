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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ClassSet} class.
 */
public class ClassSetTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAddAndContains() throws Exception {
        ClassSet clsSet = new ClassSet();

        clsSet.add("org.apache.ignite.Ignite");

        assertTrue(clsSet.contains("org.apache.ignite.Ignite"));
        assertFalse(clsSet.contains("org.apache.ignite.NotIgnite"));
        assertFalse(clsSet.contains("org.apache.Ignite"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAddWithMaskAndContains() throws Exception {
        ClassSet clsSet = new ClassSet();

        clsSet.add("org.apache.ignite.*");

        assertTrue(clsSet.contains("org.apache.ignite.Ignite"));
        assertTrue(clsSet.contains("org.apache.ignite.NotIgnite"));
        assertFalse(clsSet.contains("org.apache.Ignite"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReduceOnAddWithMask() throws Exception {
        ClassSet clsSet = new ClassSet();

        clsSet.add("org.apache.ignite.Ignite");
        clsSet.add("org.apache.ignite.Ignition");

        assertTrue(clsSet.contains("org.apache.ignite.Ignite"));
        assertTrue(clsSet.contains("org.apache.ignite.Ignition"));
        assertFalse(clsSet.contains("org.apache.ignite.NotIgnite"));

        clsSet.add("org.apache.ignite.*");

        assertTrue(clsSet.contains("org.apache.ignite.Ignite"));
        assertTrue(clsSet.contains("org.apache.ignite.Ignition"));
        assertTrue(clsSet.contains("org.apache.ignite.NotIgnite"));
    }
}

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

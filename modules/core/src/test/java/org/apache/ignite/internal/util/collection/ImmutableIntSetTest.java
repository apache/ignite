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

package org.apache.ignite.internal.util.collection;

import java.util.Arrays;
import java.util.HashSet;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test for immutable int set wrapper
 */
public class ImmutableIntSetTest {
    /** */
    @Test
    public void shouldWrapHashSet() {
        IntSet immutableSet = ImmutableIntSet.wrap(new HashSet<>(Arrays.asList(2)));

        assertTrue(immutableSet.contains(2));
        assertFalse(immutableSet.contains(1));
    }

    /** */
    @Test
    public void emptySet() {
        ImmutableIntSet emptySet = ImmutableIntSet.emptySet();

        assertTrue(emptySet.isEmpty());
        assertThat(emptySet.size(), is(0));
    }

    /** */
    @Test
    public void toIntArray() {
        IntSet hashSet = ImmutableIntSet.wrap(new HashSet<>(Arrays.asList(2)));
        int[] fromHash = hashSet.toIntArray();
        assertThat(fromHash.length, is(1));
        assertThat(fromHash[0], is(2));

        IntSet bitSet = ImmutableIntSet.wrap(new BitSetIntSet(2, Arrays.asList(2)));
        int[] fromBit = bitSet.toIntArray();
        assertThat(fromBit.length, is(1));
        assertThat(fromBit[0], is(2));
    }

    /** */
    @Test
    public void contains() {
        IntSet immutableSet = ImmutableIntSet.wrap(new BitSetIntSet(2, Arrays.asList(2)));

        assertThat(immutableSet.size(), is(1));
        assertFalse(immutableSet.isEmpty());

        assertTrue(immutableSet.contains(2));
        assertFalse(immutableSet.contains(1));
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void throwExceptionForAddOperation() {
        IntSet immutableSet = ImmutableIntSet.wrap(new BitSetIntSet(4));

        immutableSet.add(1);
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void throwExceptionForRemoveOperation() {
        IntSet immutableSet = ImmutableIntSet.wrap(new BitSetIntSet(2, Arrays.asList(2)));

        immutableSet.remove(2);
    }
}

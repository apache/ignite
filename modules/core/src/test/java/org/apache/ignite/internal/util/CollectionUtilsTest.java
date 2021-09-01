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

package org.apache.ignite.internal.util;

import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.CollectionUtils.concat;
import static org.apache.ignite.internal.util.CollectionUtils.union;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Testing the {@link CollectionUtils}.
 */
public class CollectionUtilsTest {
    /** */
    @Test
    void testConcatIterables() {
        assertTrue(collect(concat(null)).isEmpty());
        assertTrue(collect(concat(List.of())).isEmpty());
        assertTrue(collect(concat(List.of(), List.of())).isEmpty());

        assertEquals(List.of(1), collect(concat(List.of(1))));
        assertEquals(List.of(1), collect(concat(List.of(1), List.of())));
        assertEquals(List.of(1), collect(concat(List.of(), List.of(1))));

        assertEquals(List.of(1, 2, 3), collect(concat(List.of(1), List.of(2, 3))));
    }

    /** */
    @Test
    void testUnion() {
        assertTrue(union(null, null).isEmpty());
        assertTrue(union(Set.of(), null).isEmpty());
        assertTrue(union(null, new Object[] {}).isEmpty());

        assertEquals(Set.of(1), union(Set.of(1), null));
        assertEquals(Set.of(1), union(Set.of(1), new Integer[] {}));
        assertEquals(Set.of(1), union(null, new Integer[] {1}));
        assertEquals(Set.of(1), union(Set.of(), new Integer[] {1}));

        assertEquals(Set.of(1, 2), union(Set.of(1), new Integer[] {2}));
    }

    /**
     * Collect of elements.
     *
     * @param iterable Iterable.
     * @param <T> Type of the elements.
     * @return Collected elements.
     */
    private <T> List<? extends T> collect(Iterable<? extends T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(toList());
    }
}

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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class ClosableIteratorsHolderTest extends GridCommonAbstractTest {
    /** */
    private static final int GENERATED = 10000;

    /** */
    private Set<Iterator<?>> iterators;

    /** */
    private ClosableIteratorsHolder holder;

    @Before
    public void setup() throws Exception {
        iterators = Collections.newSetFromMap(new ConcurrentHashMap<>());
        holder = new ClosableIteratorsHolder(log());
        holder.init();
    }

    @After
    public void tearDown() throws Exception {
        holder.tearDown();

        holder = null;
        iterators = null;
    }

    @Test
    public void iterator() throws Exception {
        for (int i = 0; i < GENERATED; i++)
            holder.iterator(newIterator());

        System.gc();

        GridTestUtils.waitForCondition(() -> iterators.size() < GENERATED, 10_000);

        assertTrue(iterators.size() < GENERATED);
    }

    /** */
    private Iterator<?> newIterator() {
        final ClosableIterator iterator = new ClosableIterator();
        iterators.add(iterator);
        return iterator;
    }

    /** */
    private class ClosableIterator implements Iterator<Object>, AutoCloseable {
        /** */
        public final byte[] data;

        private ClosableIterator() {
            data = new byte[4096];
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Object next() {
            throw new NoSuchElementException();
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            Optional.ofNullable(iterators)
                .ifPresent(set -> set.remove(this));
        }
    }
}

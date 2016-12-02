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

package org.apache.ignite.internal.processors.hadoop.shuffle.collections;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.processors.hadoop.shuffle.mem.MemoryManager;

/**
 * Iterator over values.
 */
class ValueIterator implements Iterator<Object> {
    /** */
    private long valPtr;

    /** */
    private final ReaderBase valReader;

    /** */
    private final MemoryManager mem;

    /**
     * @param valPtr Value page pointer.
     * @param valReader Value reader.
     * @param mem Memory manager.
     */
    protected ValueIterator(long valPtr, ReaderBase valReader, MemoryManager mem) {
        this.valPtr = valPtr;
        this.valReader = valReader;
        this.mem = mem;
    }

    /**
     * @param valPtr Head value pointer.
     */
    public void head(long valPtr) {
        this.valPtr = valPtr;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return valPtr != 0;
    }

    /** {@inheritDoc} */
    @Override public Object next() {
        if (!hasNext())
            throw new NoSuchElementException();

        Object res = valReader.readValue(valPtr);

        valPtr = mem.nextValue(valPtr);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException();
    }
}

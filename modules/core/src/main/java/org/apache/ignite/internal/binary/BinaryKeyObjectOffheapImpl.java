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

package org.apache.ignite.internal.binary;

import java.io.Externalizable;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;

/**
 *  Binary key object implementation over offheap memory.
 */
public class BinaryKeyObjectOffheapImpl extends BinaryObjectOffheapImpl implements KeyCacheObject {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * For {@link Externalizable} (not supported).
     */
    public BinaryKeyObjectOffheapImpl() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param ctx Context.
     * @param ptr Memory address.
     * @param start Object start.
     * @param size Memory size.
     */
    public BinaryKeyObjectOffheapImpl(BinaryContext ctx, long ptr, int start, int size) {
        super(ctx, ptr, start, size);
    }

    /** {@inheritDoc} */
    @Override public boolean internal() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void partition(int part) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject copy(int part) {
        throw new UnsupportedOperationException();
    }
}

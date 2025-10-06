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

package org.apache.ignite.internal.thread.context;

/** */
public final class ScopedContext extends ContextDataChain<ScopedContext> {
    /** */
    private static final ScopedContext ROOT = new ScopedContext();

    /** */
    private final ContextAttribute<?> attr;
    
    /** */
    private final Object val;

    /** */
    private ScopedContext() {
        attr = null;
        val = null;
    }

    /** */
    private <T> ScopedContext(ContextAttribute<T> attr, T val, ScopedContext prev) {
        super(prev.storedAttributeBits() | attr.bitmask(), prev);
        
        this.attr = attr;
        this.val = val;
    }

    /** */
    public <T> ScopedContext with(ContextAttribute<T> attr, T val) {
        return attr.get() == val ? this : new ScopedContext(attr, val, this);
    }

    /** */
    public static ScopedContext create() {
        return ROOT;
    }

    /** */
    ContextAttribute<?> attribute() {
        assert !isEmpty();
        
        return attr;
    }
    
    /** */
    <T> T value() {
        assert !isEmpty();
        
        return (T)val;
    }

    /** {@inheritDoc} */
    @Override boolean isEmpty() {
        return this == ROOT;
    }

    /** */
    public Scope open() {
        if (isEmpty())
            return Scope.NOOP_SCOPE;

        ThreadLocalContextStorage threadData = ThreadLocalContextStorage.get();

        threadData.attach(this);

        return () -> ThreadLocalContextStorage.get().detach(this);
    }
}

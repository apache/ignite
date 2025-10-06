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
public class ContextSnapshot extends ContextDataChain<ContextSnapshot> {
    /** */
    static final ContextSnapshot ROOT = new ContextSnapshot();
    
    /** */
    private final ScopedContext scopedCtx;

    /** */
    private ContextSnapshot() {
        scopedCtx = null;
    }

    /** */
    private ContextSnapshot(ScopedContext scopedCtx, ContextSnapshot prev) {
        super(scopedCtx.storedAttributeBits() | prev.storedAttributeBits(), prev);
        
        this.scopedCtx = scopedCtx;
    }

    /** */
    public Scope restore() {
        ThreadLocalContextStorage threadData = ThreadLocalContextStorage.get();

        ContextSnapshot prev = threadData.snapshot();

        threadData.reinitialize(this);

        return () -> {
            ThreadLocalContextStorage td = ThreadLocalContextStorage.get();

            assert td.snapshot() == this : "Scopes must be closed in the same order and in the same thread they are opened";

            td.reinitialize(prev);
        };
    }

    /** */
    ContextSnapshot attach(ScopedContext scopedCtx) {
        return new ContextSnapshot(scopedCtx, this);
    }

    /** */
    ScopedContext scopedContext() {
        assert !isEmpty();

        return scopedCtx;
    }

    /** {@inheritDoc} */
    @Override boolean isEmpty() {
        return this == ROOT;
    }

    /** */
    public static ContextSnapshot capture() {
        return ThreadLocalContextStorage.get().snapshot();
    }
}

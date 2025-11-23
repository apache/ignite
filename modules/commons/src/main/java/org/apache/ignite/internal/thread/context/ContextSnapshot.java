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

/**
 * Represents Snapshot of all {@link ContextAttribute}s and their corresponding values. Note that Snapshot also stores
 * the states of {@link ContextAttribute}s for which value are note explicitly specified.
 */
public class ContextSnapshot extends ContextDataChainNode<ContextSnapshot> {
    /** */
    static final ContextSnapshot ROOT = new ContextSnapshot();
    
    /** */
    private final Context.AttributeValueHolder data;

    /** */
    private ContextSnapshot() {
        data = null;
    }

    /** */
    private ContextSnapshot(Context.AttributeValueHolder data, ContextSnapshot prev) {
        super(data.storedAttributeIdBits() | prev.storedAttributeIdBits(), prev);
        
        this.data = data;
    }

    /**
     * Stashes all {@link ContextAttribute} values attached to the thread from which this method is called and replaces
     * them with ones stored in the current Snapshot.
     *
     * @return {@link Scope} instance that, when closed, restores the values of all {@link ContextAttribute}s to the
     * state they were in before the current method was called.
     */
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
    ContextSnapshot attach(Context.AttributeValueHolder data) {
        return new ContextSnapshot(data, this);
    }

    /** */
    Context.AttributeValueHolder data() {
        assert !isEmpty();

        return data;
    }

    /** {@inheritDoc} */
    @Override boolean isEmpty() {
        return this == ROOT;
    }

    /**
     * Captures Snapshot of all {@link ContextAttribute}s and their corresponding values attached to the thread from which
     * this method is called.
     */
    public static ContextSnapshot capture() {
        return ThreadLocalContextStorage.get().snapshot();
    }
}

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
 * Represents a set of mappings from the {@link ContextAttribute} to its corresponding value. The Context can be attached
 * to a thread, making the {@link ContextAttribute} values accessible through the {@link ContextAttribute#get()}
 * method when invoked from the same thread.
 *
 * @see ContextAttribute
 * @see ContextSnapshot
 * @see AttributeValueHolder#attach()
 */
public final class Context {
    /**
     * Creates a new Context containing a single mapping from the specified {@link AttributeValueHolder} to its value.
     * The returned {@link AttributeValueHolder} represents the added mapping and can be used to accumulate to Context
     * more mappings form {@link ContextAttribute} to its value.
     *
     * @see AttributeValueHolder#with(ContextAttribute, Object)
     */
    public static <T> AttributeValueHolder with(ContextAttribute<T> attr, T val) {
        return AttributeValueHolder.ROOT.with(attr, val);
    }

    /** */
    public static final class AttributeValueHolder extends ContextDataChainNode<AttributeValueHolder> {
        /** */
        private static final AttributeValueHolder ROOT = new AttributeValueHolder();

        /** */
        private final ContextAttribute<?> attr;

        /** */
        private final Object val;

        /** */
        private AttributeValueHolder() {
            attr = null;
            val = null;
        }

        /** */
        private <T> AttributeValueHolder(ContextAttribute<T> attr, T val, AttributeValueHolder prev) {
            super(prev.storedAttributeIdBits() | attr.bitmask(), prev);

            this.attr = attr;
            this.val = val;
        }

        /**
         * Expands Context by adding new mapping from the specified {@link ContextAttribute} to its value.
         * After this operation, the Context contains all previously added mappings plus the new one.
         *
         * @return {@link AttributeValueHolder} instance that represents the added mapping and can be used to accumulate
         * more mappings from {@link ContextAttribute} to its values.
         */
        public <T> AttributeValueHolder with(ContextAttribute<T> attr, T val) {
            return attr.get() == val ? this : new AttributeValueHolder(attr, val, this);
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

        /**
         * Attaches {@link ContextAttribute} values stored in current Context to the thread from which this method is
         * called. If {@link ContextAttribute} value was already attached for the current thread, its value will be
         * stashed and replaced by the new one.
         *
         * @return {@link Scope} instance that, when closed, resets the values for all {@link ContextAttribute}s added
         * to the current Context and restores them to the previously attached values, if any.
         */
        public Scope attach() {
            if (isEmpty())
                return Scope.NOOP_SCOPE;

            ThreadLocalContextStorage.get().attach(this);

            return () -> ThreadLocalContextStorage.get().detach(this);
        }
    }
}

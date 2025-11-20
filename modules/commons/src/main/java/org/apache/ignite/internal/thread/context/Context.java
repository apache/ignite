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

/** Represents mapping of {@link ContextAttribute} to their corresponding values with the ability to be attached to the thread. */
public final class Context {
    /**
     * Creates a new {@link AttributeValueHolder} with a single mapping of the specified {@link AttributeValueHolder}
     * to the specified value. The {@link AttributeValueHolder} can be used to accumulate mappings.
     *
     * @see AttributeValueHolder#with(ContextAttribute, Object)
     */
    public static <T> AttributeValueHolder with(ContextAttribute<T> attr, T val) {
        return AttributeValueHolder.ROOT.with(attr, val);
    }

    /** */
    public static final class AttributeValueHolder extends ContextDataChain<AttributeValueHolder> {
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

        /** Adds a new mapping of the specified attribute to its value to the current {@link ContextDataChain}. */
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
         * Attaches {@link ContextAttribute} values stored in current {@link ContextDataChain} to the thread
         * this method is called from. If {@link ContextAttribute} value was already attached for the current thread,
         * its value will be stashed and replaced by the new ones.
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

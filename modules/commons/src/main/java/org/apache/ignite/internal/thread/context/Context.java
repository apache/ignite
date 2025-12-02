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

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a set of mappings from the {@link ContextAttribute} to its corresponding value. The Context can be attached
 * to a thread, making the {@link ContextAttribute} values accessible through the {@link ContextAttribute#get()}
 * method when invoked from the same thread.
 *
 * @see Context#attach()
 * @see ContextAttribute
 * @see ContextSnapshot
 */
public final class Context implements Iterable<AttributeValueHolder> {
    /** */
    private static final Context EMPTY = new Context(null, 0);

    /** */
    private final AttributeValueHolder listTail;

    /** */
    private final int storedAttrIdBits;

    /** */
    private Context(AttributeValueHolder listTail, int storedAttrIdBits) {
        this.listTail = listTail;

        this.storedAttrIdBits = storedAttrIdBits;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<AttributeValueHolder> iterator() {
        return new Iterator<>() {
            AttributeValueHolder listTail = Context.this.listTail;

            @Override public boolean hasNext() {
                return listTail != null;
            }

            @Override public AttributeValueHolder next() {
                AttributeValueHolder res = listTail;

                if (res == null)
                    throw new NoSuchElementException();

                listTail = listTail.previous();

                return res;
            }
        };
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
        if (listTail == null)
            return Scope.NOOP_SCOPE;

        ThreadLocalContextStorage.get().attach(this);

        return () -> ThreadLocalContextStorage.get().detach(this);
    }

    /** */
    boolean containsValueFor(ContextAttribute<?> attr) {
        return (storedAttrIdBits & attr.bitmask()) != 0;
    }

    /** */
    int storedAttributeIdBits() {
        return storedAttrIdBits;
    }

    /** */
    public static ContextSnapshot createSnapshot() {
        return ThreadLocalContextStorage.get().createSnapshot();
    }

    /** */
    public static final class Builder {
        /** */
        private AttributeValueHolder listTail;

        /** */
        private int storedAttrIdBits;

        /** */
        private Builder() {
            // No-op.
        }

        /**
         * Adds new mapping from the specified {@link ContextAttribute} to its value.
         *
         * @return {@code this} for chaining.
         */
        public <T> Builder with(ContextAttribute<T> attr, T val) {
            if (attr.get() != val) {
                listTail = new AttributeValueHolder(attr, val, listTail);

                storedAttrIdBits |= attr.bitmask();
            }

            return this;
        }

        /** Creates new Context builder. */
        public static Builder create() {
            return new Builder();
        }

        /**
         * Builds {@link Context} instance that stores previously added mapping from the specified
         * {@link ContextAttribute} to its value.
         */
        public Context build() {
            return listTail == null ? EMPTY : new Context(listTail, storedAttrIdBits);
        }
    }
}

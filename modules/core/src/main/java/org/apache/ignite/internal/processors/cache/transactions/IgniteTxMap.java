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

package org.apache.ignite.internal.processors.cache.transactions;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.ignite.internal.util.GridSerializableIterator;
import org.apache.ignite.internal.util.GridSerializableSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Grid cache transaction read or write set.
 */
public class IgniteTxMap extends AbstractMap<IgniteTxKey, IgniteTxEntry> implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Base transaction map. */
    private Map<IgniteTxKey, IgniteTxEntry> txMap;

    /** Entry set. */
    private Set<Entry<IgniteTxKey, IgniteTxEntry>> entrySet;

    /** Cached size. */
    private int size = -1;

    /** Empty flag. */
    private Boolean empty;

    /** Sealed flag. */
    private boolean sealed;

    /** Filter. */
    private IgnitePredicate<IgniteTxEntry> filter;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public IgniteTxMap() {
        // No-op.
    }

    /**
     * @param txMap Transaction map.
     * @param filter Filter.
     */
    public IgniteTxMap(Map<IgniteTxKey, IgniteTxEntry> txMap,
        IgnitePredicate<IgniteTxEntry> filter) {
        this.txMap = txMap;
        this.filter = filter;
    }

    /**
     * Seals this map.
     *
     * @return This map for chaining.
     */
    IgniteTxMap seal() {
        sealed = true;

        return this;
    }

    /**
     * @return Sealed flag.
     */
    boolean sealed() {
        return sealed;
    }

    /** {@inheritDoc} */
    @Override public Set<Entry<IgniteTxKey, IgniteTxEntry>> entrySet() {
        if (entrySet == null) {
            entrySet = new GridSerializableSet<Entry<IgniteTxKey, IgniteTxEntry>>() {
                private Set<Entry<IgniteTxKey, IgniteTxEntry>> set = txMap.entrySet();

                @Override public Iterator<Entry<IgniteTxKey, IgniteTxEntry>> iterator() {
                    return new GridSerializableIterator<Entry<IgniteTxKey, IgniteTxEntry>>() {
                        private Iterator<Entry<IgniteTxKey, IgniteTxEntry>> it = set.iterator();

                        private Entry<IgniteTxKey, IgniteTxEntry> cur;

                        // Constructor.
                        {
                            advance();
                        }

                        @Override public boolean hasNext() {
                            return cur != null;
                        }

                        @Override public Entry<IgniteTxKey, IgniteTxEntry> next() {
                            if (cur == null)
                                throw new NoSuchElementException();

                            Entry<IgniteTxKey, IgniteTxEntry> e = cur;

                            advance();

                            return e;
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }

                        private void advance() {
                            cur = null;

                            while (cur == null && it.hasNext()) {
                                Entry<IgniteTxKey, IgniteTxEntry> e = it.next();

                                if (filter.apply(e.getValue()))
                                    cur = e;
                            }
                        }
                    };
                }

                @Override public int size() {
                    return !sealed ? F.size(iterator()) : size == -1 ? size = F.size(iterator()) : size;
                }

                @Override public boolean isEmpty() {
                    return !sealed ? !iterator().hasNext() : empty == null ? empty = !iterator().hasNext() : empty;
                }
            };
        }

        return entrySet;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return entrySet().isEmpty();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return entrySet().size();
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        return get(key) != null;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public IgniteTxEntry get(Object key) {
        IgniteTxEntry e = txMap.get(key);

        return e == null ? null : filter.apply(e) ? e : null;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxEntry remove(Object key) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        throw new IllegalStateException("Transaction view map should never be serialized: " + this);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new IllegalStateException("Transaction view map should never be serialized: " + this);
    }
}
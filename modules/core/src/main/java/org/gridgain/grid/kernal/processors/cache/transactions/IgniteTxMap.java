/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.transactions;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Grid cache transaction read or write set.
 */
public class IgniteTxMap<K, V> extends AbstractMap<IgniteTxKey<K>, IgniteTxEntry<K, V>> implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Base transaction map. */
    private Map<IgniteTxKey<K>, IgniteTxEntry<K, V>> txMap;

    /** Entry set. */
    private Set<Entry<IgniteTxKey<K>, IgniteTxEntry<K, V>>> entrySet;

    /** Cached size. */
    private int size = -1;

    /** Empty flag. */
    private Boolean empty;

    /** Sealed flag. */
    private boolean sealed;

    /** Filter. */
    private IgnitePredicate<IgniteTxEntry<K, V>> filter;

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
    public IgniteTxMap(Map<IgniteTxKey<K>, IgniteTxEntry<K, V>> txMap,
        IgnitePredicate<IgniteTxEntry<K, V>> filter) {
        this.txMap = txMap;
        this.filter = filter;
    }

    /**
     * Seals this map.
     *
     * @return This map for chaining.
     */
    IgniteTxMap<K, V> seal() {
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
    @Override public Set<Entry<IgniteTxKey<K>, IgniteTxEntry<K, V>>> entrySet() {
        if (entrySet == null) {
            entrySet = new GridSerializableSet<Entry<IgniteTxKey<K>, IgniteTxEntry<K, V>>>() {
                private Set<Entry<IgniteTxKey<K>, IgniteTxEntry<K, V>>> set = txMap.entrySet();

                @Override public Iterator<Entry<IgniteTxKey<K>, IgniteTxEntry<K, V>>> iterator() {
                    return new GridSerializableIterator<Entry<IgniteTxKey<K>, IgniteTxEntry<K, V>>>() {
                        private Iterator<Entry<IgniteTxKey<K>, IgniteTxEntry<K, V>>> it = set.iterator();

                        private Entry<IgniteTxKey<K>, IgniteTxEntry<K, V>> cur;

                        // Constructor.
                        {
                            advance();
                        }

                        @Override public boolean hasNext() {
                            return cur != null;
                        }

                        @Override public Entry<IgniteTxKey<K>, IgniteTxEntry<K, V>> next() {
                            if (cur == null)
                                throw new NoSuchElementException();

                            Entry<IgniteTxKey<K>, IgniteTxEntry<K, V>> e = cur;

                            advance();

                            return e;
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }

                        private void advance() {
                            cur = null;

                            while (cur == null && it.hasNext()) {
                                Entry<IgniteTxKey<K>, IgniteTxEntry<K, V>> e = it.next();

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
    @Override public IgniteTxEntry<K, V> get(Object key) {
        IgniteTxEntry<K, V> e = txMap.get(key);

        return e == null ? null : filter.apply(e) ? e : null;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxEntry<K, V> remove(Object key) {
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

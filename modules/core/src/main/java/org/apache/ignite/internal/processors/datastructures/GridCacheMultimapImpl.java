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

package org.apache.ignite.internal.processors.datastructures;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteMultimap;
import org.apache.ignite.internal.processors.cache.CacheIteratorConverter;
import org.apache.ignite.internal.processors.cache.CacheWeakQueryIteratorsHolder;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;

/** Cache multimap implementation */
public class GridCacheMultimapImpl<K, V> extends GridCacheAbstractMapImpl<List<V>> implements IgniteMultimap<K, V> {
    /** Removed flag. */
    private volatile boolean rmvd;

    /**
     * @param name Multimap name.
     * @param hdr Multimap hdr.
     * @param cctx Cache context.
     */
    public GridCacheMultimapImpl(String name, GridCacheMapHeader hdr, GridCacheContext<?, ?> cctx) {
        super(cctx, name, hdr);
    }

    /**
     * @return Multimap unique ID.
     */
    public IgniteUuid id() {
        return hdr.id();
    }

    /** {@inheritDoc} */
    @Override public List<V> get(K key) {
        try {
            List<V> list = cache.get(itemKey(key));
            return list != null ? list : Collections.emptyList();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V get(K key, int index) {
        try {
            List<V> list = cache.get(itemKey(key));

            if (list == null)
                return null;

            return list.get(index);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public List<V> get(K key, int min, int max) {
        try {
            if(min > max)
                throw new IllegalArgumentException("The value of argument min [" + min +
                    "] is greater than the value of max [" + max + "]");

            List<V> list = cache.get(itemKey(key));

            if (list == null)
                return Collections.emptyList();

            List<V> res = new ArrayList<>(max - min + 1);
            for (int i = min; i <= max; i++)
                res.add(list.get(i));

            return res;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public List<V> get(K key, Iterable<Integer> indexes) {
        try {
            List<V> list = cache.get(itemKey(key));

            if (list == null)
                return Collections.<V>emptyList();

            List<V> res = new ArrayList<>();
            for (Integer idx : indexes)
                res.add(list.get(idx));

            return res;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Map<K, List<V>> getAll(Collection<K> keys) {
        try {
            Map<K, List<V>> map = (Map<K, List<V>>) getAll0(keys);

            for (K key : keys) {
                if (!map.containsKey(key))
                    map.put(key, Collections.emptyList());
            }

            return map;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Map<K, V> getAll(Collection<K> keys, int index) {
        try {
            Map<K, List<V>> map = (Map<K, List<V>>) getAll0(keys);

            Map<K, V> res = new HashMap<>();
            for (Entry<K, List<V>> e : map.entrySet())
                res.put(e.getKey(), e.getValue().get(index));

            return res;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, List<V>> getAll(Collection<K> keys, int min, int max) {
        if(min > max)
            throw new IllegalArgumentException("The value of argument min [" + min +
                "] is greater than the value of max [" + max + "]");

        List<Integer> indexes = new ArrayList<>(max - min + 1);
        for (int i = min; i <= max; i++)
            indexes.add(i);

        return getAll(keys, indexes);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Map<K, List<V>> getAll(Collection<K> keys, Iterable<Integer> indexes) {
        try {
            Map<K, List<V>> map = (Map<K, List<V>>) getAll0(keys);
            for (Entry<K, List<V>> e : map.entrySet()) {
                List<V> l = new ArrayList<>();
                for (Integer idx : indexes)
                    l.add(e.getValue().get(idx));

                e.setValue(l);
            }

            for (K key : keys) {
                if (!map.containsKey(key))
                    map.put(key, Collections.emptyList());
            }

            return map;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        return cache.containsKey(itemKey(key));
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(V value) {
        if (!collocated()) {
            Collection<Boolean> results = compute.broadcast(localContainsValue(value));

            for (Boolean r : results) {
                if(r)
                    return true;
            }

            return false;
        }
        else {
            return affinityCall(localContainsValue(value));
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsEntry(K key, V value) {
        try {
            List<V> list = cache.get(itemKey(key));
            return list != null && list.contains(value);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Iterator<Entry<K, V>> entries() {
        final GridCloseableIterator<Entry<K, List<V>>> iter = createGridIterator();

        CacheIteratorConverter<Entry<K, V>, Entry<K, V>> converter = new CacheIteratorConverter<Entry<K, V>, Entry<K, V>>() {
            @Override protected Entry<K, V> convert(Entry<K, V> e) {
                return e;
            }

            @Override protected void remove(Entry<K, V> item) {
            }
        };

        GridCloseableIterator<Entry<K, V>> it = new MultimapEntryCloseableIterator<>(iter);
        CacheWeakQueryIteratorsHolder<Entry<K, V>> itHolder = ((GridCacheContext<K, V>)cctx).itHolder();
        return itHolder.iterator(it, converter);
    }

    /** {@inheritDoc} */
    @Override public Set<K> localKeySet() {
        try {
            return localKeySet0().call();
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Set<K> keySet() {
        if (!collocated()) {
            Collection<Set<K>> calls = compute.broadcast(localKeySet0());

            Set<K> res = new HashSet<>();
            for (Set<K> r : calls)
                res.addAll(r);

            return res;
        }
        else {
            return affinityCall(localKeySet0());
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean put(K key, V value) {
        try {
            IgniteBiTuple<Boolean, Integer> res = cache.invoke(itemKey(key),
                new EntryProcessor<MapItemKey, List<V>, IgniteBiTuple<Boolean, Integer>>() {
                    @Override public IgniteBiTuple<Boolean, Integer> process(
                        MutableEntry<MapItemKey, List<V>> entry, Object... arguments) {
                        int size = 0;
                        List<V> list = entry.getValue();
                        if (list == null) {
                            size = 1;
                            list = getListInstance();
                        }
                        list.add(value);
                        entry.setValue(list);
                        return new IgniteBiTuple<>(true, size);
                    }
                }).get();
            changeSize(res.get2());
            return res.get1();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putAll(K key, Iterable<? extends V> values) {
        try {
            IgniteBiTuple<Boolean, Integer> res = cache.invoke(itemKey(key),
                new EntryProcessor<MapItemKey, List<V>, IgniteBiTuple<Boolean, Integer>>() {
                    @Override public IgniteBiTuple<Boolean, Integer> process(
                        MutableEntry<MapItemKey, List<V>> entry, Object... arguments) {
                        int size = 0;
                        List<V> list = entry.getValue();
                        if (list == null) {
                            size = 1;
                            list = getListInstance();
                        }
                        values.forEach(list::add);
                        entry.setValue(list);
                        return new IgniteBiTuple<>(true, size);
                    }
                }).get();
            changeSize(res.get2());
            return res.get1();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putAll(IgniteMultimap<? extends K, ? extends V> multimap) {
        Iterator<? extends Entry<? extends K, ? extends V>> it = multimap.entries();
        K k = null;
        boolean res = false;
        List<V> list = new LinkedList<>();
        while (it.hasNext()) {
            Entry<? extends K, ? extends V> e = it.next();

            if (k == null)
                k = e.getKey();

            if (k.equals(e.getKey())) {
                list.add(e.getValue());
            }
            else if (!list.isEmpty()) {
                putAll(k, list);
                k = null;
                list.clear();
                res = true;
            }
        }

        if (!list.isEmpty()) {
            putAll(k, list);
            res = true;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public List<V> remove(K key) {
        try {
            IgniteBiTuple<List<V>, Integer> res = cache.invoke(itemKey(key),
                new EntryProcessor<MapItemKey, List<V>, IgniteBiTuple<List<V>, Integer>>() {
                    @Override public IgniteBiTuple<List<V>, Integer> process(
                        MutableEntry<MapItemKey, List<V>> entry, Object... arguments) {
                        int size = 0;
                        List<V> list = entry.getValue();

                        if (list != null)
                            size = -1;

                        entry.remove();

                        return new IgniteBiTuple<>(list == null ? Collections.emptyList() : list, size);
                    }
                }).get();

            changeSize(res.get2());

            return res.get1();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V value) {
        try {
            IgniteBiTuple<Boolean, Integer> res = cache.invoke(itemKey(key), new EntryProcessor<MapItemKey, List<V>, IgniteBiTuple<Boolean, Integer>>() {
                @Override
                public IgniteBiTuple<Boolean, Integer> process(MutableEntry<MapItemKey, List<V>> entry,
                    Object... arguments) {
                    int size = 0;
                    List<V> list = entry.getValue();
                    boolean b = list != null && list.remove(value);

                    if (list != null) {
                        if (list.isEmpty()) {
                            size = -1;
                            entry.remove();
                        }
                        else {
                            entry.setValue(list);
                        }
                    }

                    return new IgniteBiTuple<>(b, size);
                }
            }).get();

            changeSize(res.get2());

            return res.get1();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public List<V> replaceValues(K key, Iterable<? extends V> values) {
        try {
            return cache.invoke(itemKey(key), new EntryProcessor<MapItemKey, List<V>, List<V>>() {
                @Override public List<V> process(MutableEntry<MapItemKey, List<V>> entry, Object... arguments) {
                    List<V> list = entry.getValue();

                    if (list == null)
                        return Collections.emptyList();

                    List<V> newList = getListInstance();
                    for (V v : values)
                        newList.add(v);

                    if (newList.isEmpty())
                        throw new IllegalArgumentException("Values can not be empty");

                    entry.setValue(newList);

                    return list;
                }
            }).get();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        try {
            return ((GridCacheMapHeader)cctx.kernalContext().cache()
                .internalCache(cctx.name()).get(hdrKey)).size() == 0;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Iterator<Entry<K, V>> iterate(int index) {
        final GridCloseableIterator<Entry<K, List<V>>> iter = createGridIterator();

        CacheIteratorConverter<Entry<K, V>, Entry<K, List<V>>> converter = new CacheIteratorConverter<Entry<K, V>, Entry<K, List<V>>>() {
            @Override protected Entry<K, V> convert(Entry<K, List<V>> e) {
                return new IgniteBiTuple<>(e.getKey(), e.getValue().get(index));
            }

            @Override protected void remove(Entry<K, V> item) {
            }
        };

        CacheWeakQueryIteratorsHolder<Entry<K, List<V>>> itHolder = ((GridCacheContext<K, List<V>>)cctx).itHolder();
        return itHolder.iterator(iter, converter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Iterator<V> values() {
        final GridCloseableIterator<Entry<K, List<V>>> iter = createGridIterator();

        CacheIteratorConverter<V, Entry<K, V>> converter = new CacheIteratorConverter<V, Entry<K, V>>() {
            @Override protected V convert(Entry<K, V> e) {
                return e.getValue();
            }

            @Override protected void remove(V item) {
            }
        };

        GridCloseableIterator<Entry<K, V>> it = new MultimapEntryCloseableIterator<>(iter);
        CacheWeakQueryIteratorsHolder<Entry<K, V>> itHolder = ((GridCacheContext<K, V>)cctx).itHolder();
        return itHolder.iterator(it, converter);
    }

    /** {@inheritDoc} */
    @Override public int valueCount(K key) {
        return get(key).size();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void close() {
        try {
            cctx.kernalContext().cache().internalCache(cctx.name()).remove(hdrKey);
            cctx.kernalContext().dataStructures().removeMultimap(name, cctx);
            if (collocated()) {
                affinityRun(new IgniteRunnable() {
                    @Override public void run() {
                        try {
                            for (Cache.Entry e : localEntries()) {
                                if (MapItemKey.class.isAssignableFrom(e.getKey().getClass())) {
                                    MapItemKey key = ((Cache.Entry<MapItemKey, List>)e).getKey();
                                    if (key.id().equals(id()))
                                        cache.remove(key);
                                }
                            }
                        }
                        catch (IgniteCheckedException e) {
                            throw U.convertException(e);
                        }
                    }
                });
            }
            else {
                cctx.kernalContext().cache().dynamicDestroyCache(cctx.name(), false, true, false);
            }
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean collocated() {
        return hdr.collocated();
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return rmvd;
    }

    /**
     * Marks multimap as removed.
     *
     * @param throw0 If {@code true} then throws {@link IllegalStateException}.
     */
    public void onRemoved(boolean throw0) {
        rmvd = true;

        if (throw0)
            throw new IllegalStateException("Queue has been removed from cache: " + this);
    }

    /**
     * @return Callable to collect local keys
     */
    @SuppressWarnings("unchecked")
    @NotNull private IgniteCallable<Set<K>> localKeySet0() {
        return new IgniteCallable<Set<K>>() {
            @Override public Set<K> call() throws Exception {
                Set set = new HashSet();
                for (Cache.Entry e : GridCacheMultimapImpl.this.localEntries()) {
                    if (MapItemKey.class.isAssignableFrom(e.getKey().getClass())) {
                        MapItemKey key = ((Cache.Entry<MapItemKey, List<V>>)e).getKey();
                        if (key.id().equals(GridCacheMultimapImpl.this.id()))
                            set.add(key.item());
                    }
                }
                return set;
            }
        };
    }

    /**
     * @return Callable to check local entries for value
     */
    @SuppressWarnings("unchecked")
    @NotNull private IgniteCallable<Boolean> localContainsValue(V value) {
        return new IgniteCallable<Boolean>() {
            @Override public Boolean call() throws Exception {
                for (Cache.Entry e : GridCacheMultimapImpl.this.localEntries()) {
                    if (MapItemKey.class.isAssignableFrom(e.getKey().getClass())) {
                        Cache.Entry<MapItemKey, List<V>> ee = (Cache.Entry<MapItemKey, List<V>>)e;
                        if (ee.getKey().id().equals(id()) && ee.getValue().contains(value))
                            return true;
                    }
                }
                return false;
            }
        };
    }

    /**
     * @return Callable to fetch keys from cache
     */
    private Map<Object, List<V>> getAll0(Iterable<K> keys) throws IgniteCheckedException {
        List<MapItemKey> itemKeys = new ArrayList<>();
        for (K k : keys)
            itemKeys.add(itemKey(k));

        Map<Object, List<V>> map = new HashMap<>();
        for (Entry<MapItemKey, List<V>> e : cache.getAll(itemKeys).entrySet())
            map.put(e.getKey().item(), e.getValue());

        return map;
    }

    /**
     * @param key User key.
     * @return Item key.
     */
    private MapItemKey itemKey(K key) {
        return collocated() ?
            new CollocatedMapItemKey(name, id(), key) :
            new GridCacheMapItemKey(id(), key);
    }

    /**
     * @return Multimap backing list instance
     */
    @NotNull private static <T> List<T> getListInstance() {
        return new ArrayList<>();
    }

    /**
     * Create an iterator over multimap elements
     *
     * @return multimap iterator
     */
    @SuppressWarnings("unchecked")
    private GridCloseableIterator<Entry<K, List<V>>> createGridIterator() {
        try {
            return ((GridCacheContext<MapItemKey, List<V>>)cctx).queries()
                .createScanQuery(
                    new IgniteBiPredicate() {
                        @Override public boolean apply(Object k, Object v) {
                            if (MapItemKey.class.isAssignableFrom(k.getClass())) {
                                MapItemKey key = (MapItemKey)k;

                                return key.id().equals(id()) /*&& key.getMultimapName().equals(name)*/;
                            }
                            return false;
                        }
                    },
                    new IgniteClosure<Cache.Entry<MapItemKey, List<V>>, Entry<K, List<V>>>() {
                        @Override
                        public Entry<K, List<V>> apply(Cache.Entry<MapItemKey, List<V>> e) {
                            return new IgniteBiTuple(e.getKey().item(), e.getValue());
                        }
                    },
                    collocated() ? cctx.affinity().partition(hdrKey) : null,
                    cctx.keepBinary())
                .keepAll(false)
                .executeScanQuery();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheMultimapImpl that = (GridCacheMultimapImpl)o;

        return id().equals(that.id());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id().hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMultimapImpl.class, this);
    }

    /**
     * Partitioned closable iterator.
     */
    private static class MultimapEntryCloseableIterator<K, V> extends GridCloseableIteratorAdapter<Entry<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Current iterator. */
        private GridCloseableIterator<Entry<K, List<V>>> curIt;

        private int cursor = 0;

        private Entry<K, List<V>> curItem;

        private MultimapEntryCloseableIterator(GridCloseableIterator<Entry<K, List<V>>> it) {
            curIt = it;
            if (curIt != null && curIt.hasNext())
                curItem = curIt.next();
        }

        /** {@inheritDoc} */
        @Override protected Entry<K, V> onNext() throws IgniteCheckedException {
            if (curItem == null)
                throw new NoSuchElementException();

            return new IgniteBiTuple<>(curItem.getKey(), curItem.getValue().get(cursor++));
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            if (curItem == null)
                return false;

            if (curItem.getValue().size() > cursor)
                return true;

            boolean hasNext = curIt.hasNext();
            if (hasNext) {
                cursor = 0;
                curItem = curIt.next();
            }
            else {
                curIt.close();
                curItem = null;
            }

            return hasNext;
        }

        /** {@inheritDoc} */
        @Override protected void onRemove() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            if (curIt != null)
                curIt.close();
        }
    }
}

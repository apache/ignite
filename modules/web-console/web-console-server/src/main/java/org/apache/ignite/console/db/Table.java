/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.console.dto.AbstractDto;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toSet;

/**
 * Table for data objects.
 *
 * @param <T> Type of DTO.
 */
public class Table<T extends AbstractDto> extends CacheHolder<UUID, T> {
    /** Unique indexes. */
    private final List<UniqueIndex<T>> uniqueIndexes = new ArrayList<>();

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    public Table(Ignite ignite, String cacheName) {
        super(ignite, cacheName);
    }

    /**
     * @param keyGenerator Key generator.
     * @param msgGenerator Message generator.
     */
    public Table<T> addUniqueIndex(Function<T, Object> keyGenerator, Function<T, String> msgGenerator) {
        uniqueIndexes.add(new UniqueIndex<>(keyGenerator, msgGenerator));

        return this;
    }

    /**
     * @param id ID.
     * @return {@code true} If table contains specified key.
     */
    public boolean contains(UUID id) {
        return cache().containsKey(id);
    }

    /**
     * @param id ID.
     * @return DTO.
     */
    @Nullable public T load(UUID id) {
        return cache().get(id);
    }

    /**
     * @param key Indexed key.
     * @return DTO.
     */
    @Nullable public T getByIndex(Object key) {
        Object id = cache.get(key);

        if (id == null)
            return null;

        return (T)cache.get(id);
    }

    /**
     * @return Collection of DTOs.
     */
    public List<T> loadAll() {
        try(QueryCursor<Cache.Entry<String, Object>> cursor = cache.query(new ScanQuery())) {
            ArrayList<T> res = new ArrayList<>();

            cursor.forEach(item -> {
                Object v = item.getValue();

                if (v instanceof AbstractDto)
                    res.add((T)v);
            });

            return res;
        }
    }

    /**
     * @param keys Indexed keys.
     * @return Collection of DTOs.
     */
    public Collection<T> loadAllByIndex(Set<?> keys) {
        Map ids = cache.getAll(keys);

        if (ids.isEmpty())
            return Collections.emptyList();

        return loadAll(new HashSet<>(ids.values()));
    }

    /**
     * @param ids IDs.
     * @return Collection of DTOs.
     */
    public Collection<T> loadAll(Set<?> ids) {
        Map<?, T> res = cache.getAll(ids);

        return F.isEmpty(res) ? Collections.emptyList() : res.values();
    }

    /**
     * @param idx Index.
     * @param newVal New value.
     * @param oldVal Old value.
     */
    public void saveUniqueIndexValue(UniqueIndex<T> idx, T newVal, T oldVal) {
        Object newIdxKey = idx.key(newVal);
        UUID newId = newVal.getId();

        Object oldId = cache.getAndPutIfAbsent(newIdxKey, newId);

        if (oldId != null && !newId.equals(oldId))
            throw new IgniteException(idx.message(newVal));

        if (oldVal != null && !idx.key(oldVal).equals(newIdxKey))
            cache.remove(idx.key(oldVal));
    }

    /**
     * @param val DTO.
     * @return Saved DTO.
     */
    public T save(T val) throws IgniteException {
        T oldVal = cache().getAndPut(val.getId(), val);

        for (UniqueIndex<T> idx : uniqueIndexes)
            saveUniqueIndexValue(idx, val, oldVal);

        return oldVal;
    }

    /**
     * @param map Map of DTOs.
     */
    public void saveAll(Map<UUID, T> map) throws IgniteException {
        if (F.isEmpty(uniqueIndexes)) {
            cache().putAll(map);
            
            return;
        }

        for (T item : map.values())
            save(item);
    }

    /**
     * @param id ID.
     * @return Previous value.
     */
    @Nullable public T delete(UUID id) {
        T val = cache().getAndRemove(id);

        if (val != null)
            cache.removeAll(uniqueIndexes.stream().map(idx -> idx.key(val)).collect(toSet()));

        return val;
    }

    /**
     * @param ids IDs.
     */
    public void deleteAll(Set<UUID> ids) {
        Set<Object> idxIds = ids.stream()
            .map(cache()::getAndRemove)
            .flatMap((payload) -> uniqueIndexes.stream().map(idx -> idx.key(payload)))
            .collect(toSet());

        cache.removeAll(idxIds);
    }

    /**
     * Index for unique constraint.
     */
    public static class UniqueIndex<T> {
        /** */
        private final Function<T, Object> keyGenerator;

        /** */
        private final Function<T, String> msgGenerator;

        /**
         * Constructor.
         */
        UniqueIndex(Function<T, Object> keyGenerator, Function<T, String> msgGenerator) {
            this.keyGenerator = keyGenerator;
            this.msgGenerator = msgGenerator;
        }

        /**
         * @param val Value.
         * @return Unique key.
         */
        public Object key(T val) {
            return keyGenerator.apply(val);
        }

        /**
         * @param val Value.
         */
        public String message(T val) {
            return msgGenerator.apply(val);
        }
    }
}

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

package org.gridgain.grid.kernal.processors.cache.eviction;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Mock cache entry.
 */
public class GridCacheMockEntry<K, V> extends GridMetadataAwareAdapter implements GridCacheEntry<K, V> {
    /** */
    @GridToStringInclude
    private K key;

    /** */
    @GridToStringInclude
    private boolean evicted;

    /** */
    @GridToStringInclude
    private boolean canEvict = true;

    /**
     * Constructor.
     *
     * @param key Key.
     */
    public GridCacheMockEntry(K key) {
        this.key = key;
    }

    /**
     * Constructor.
     *
     * @param key Key.
     * @param canEvict Evict or not.
     */
    public GridCacheMockEntry(K key, boolean canEvict) {
        this.key = key;
        this.canEvict = canEvict;
    }

    /** {@inheritDoc} */
    @Override public K getKey() throws IllegalStateException {
        return key;
    }

    /** {@inheritDoc} */
    @Override public V getValue() throws IllegalStateException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public V setValue(V val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean evict() {
        evicted = true;

        onEvicted();

        return canEvict;
    }

    /**
     * Eviction callback.
     */
    public void onEvicted() {
        for (String key : allMeta().keySet())
            removeMeta(key);
    }

    /**
     *
     * @return Evicted or not.
     */
    public boolean isEvicted() {
        return evicted;
    }

    /** {@inheritDoc} */
    @Override public V peek() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public V peek(@Nullable Collection<GridCachePeekMode> modes) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object version() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long expirationTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long timeToLive() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean primary() {
        // No-op.
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean backup() {
        // No-op.
        return false;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return 0;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V set(V val,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFuture<V> setAsync(V val,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V setIfAbsent(V val) throws IgniteCheckedException {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFuture<V> setIfAbsentAsync(V val) {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean setx(V val,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        // No-op.
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFuture<Boolean> setxAsync(V val,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean setxIfAbsent(@Nullable V val) throws IgniteCheckedException {
        // No-op.
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFuture<Boolean> setxIfAbsentAsync(V val) {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V replace(V val) throws IgniteCheckedException {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFuture<V> replaceAsync(V val) {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(V val) throws IgniteCheckedException {
        // No-op.
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFuture<Boolean> replacexAsync(V val) {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(V oldVal, V newVal) throws IgniteCheckedException {
        // No-op.
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFuture<Boolean> replaceAsync(V oldVal, V newVal) {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V remove(
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFuture<V> removeAsync(
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean removex(@Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        // No-op.
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFuture<Boolean> removexAsync(
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(V val) throws IgniteCheckedException {
        // No-op.
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFuture<Boolean> removeAsync(V val) {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Override public void timeToLive(long ttl) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean lock(long timeout,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> lockAsync(long timeout,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        return new GridFinishedFuture<>(null, false);
    }

    /** {@inheritDoc} */
    @Override public void unlock(IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isCached() {
        return !evicted;
    }

    /** {@inheritDoc} */
    @Override public int memorySize() {
        return 1024;
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjection<K, V> projection() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V reload() throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> reloadAsync() {
        return new GridFinishedFuture<>();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V get() throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAsync() {
        return new GridFinishedFuture<>();
    }

    /** {@inheritDoc} */
    @Override public boolean clear() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean compact() throws IgniteCheckedException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if(clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        throw new IllegalArgumentException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMockEntry.class, this);
    }
}

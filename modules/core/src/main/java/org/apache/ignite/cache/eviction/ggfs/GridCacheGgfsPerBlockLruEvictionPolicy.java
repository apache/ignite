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

package org.apache.ignite.cache.eviction.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.internal.processors.fs.*;
import org.jdk8.backport.*;
import org.jdk8.backport.ConcurrentLinkedDeque8.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.regex.*;

/**
 * GGFS eviction policy which evicts particular blocks.
 */
public class GridCacheGgfsPerBlockLruEvictionPolicy implements GridCacheEvictionPolicy<GridGgfsBlockKey, byte[]>,
    GridCacheGgfsPerBlockLruEvictionPolicyMBean {
    /** Meta denoting node in the queue. */
    public static final String META_NODE = "ggfs_node";

    /** Maximum size. When reached, eviction begins. */
    private volatile long maxSize;

    /** Maximum amount of blocks. When reached, eviction begins. */
    private volatile int maxBlocks;

    /** Collection of regex for paths which must not be evicted. */
    private volatile Collection<String> excludePaths;

    /** Exclusion patterns. */
    private volatile Collection<Pattern> excludePatterns;

    /** Whether patterns must be recompiled during the next call. */
    private final AtomicBoolean excludeRecompile = new AtomicBoolean(true);

    /** Queue. */
    private final ConcurrentLinkedDeque8<GridCacheEntry<GridGgfsBlockKey, byte[]>> queue =
        new ConcurrentLinkedDeque8<>();

    /** Current size of all enqueued blocks in bytes. */
    private final LongAdder curSize = new LongAdder();

    /**
     * Default constructor.
     */
    public GridCacheGgfsPerBlockLruEvictionPolicy() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param maxSize Maximum size. When reached, eviction begins.
     * @param maxBlocks Maximum amount of blocks. When reached, eviction begins.
     */
    public GridCacheGgfsPerBlockLruEvictionPolicy(long maxSize, int maxBlocks) {
        this(maxSize, maxBlocks, null);
    }

    /**
     * Constructor.
     *
     * @param maxSize Maximum size. When reached, eviction begins.
     * @param maxBlocks Maximum amount of blocks. When reached, eviction begins.
     * @param excludePaths Collection of regex for path which must not be evicted.
     */
    public GridCacheGgfsPerBlockLruEvictionPolicy(long maxSize, int maxBlocks,
        @Nullable Collection<String> excludePaths) {
        this.maxSize = maxSize;
        this.maxBlocks = maxBlocks;
        this.excludePaths = excludePaths;
    }

    /** {@inheritDoc} */
    @Override public void onEntryAccessed(boolean rmv, GridCacheEntry<GridGgfsBlockKey, byte[]> entry) {
        if (!rmv) {
            if (!entry.isCached())
                return;

            if (touch(entry))
                shrink();
        }
        else {
            MetaEntry meta = entry.removeMeta(META_NODE);

            if (meta != null && queue.unlinkx(meta.node()))
                changeSize(-meta.size());
        }
    }

    /**
     * @param entry Entry to touch.
     * @return {@code True} if new node has been added to queue by this call.
     */
    private boolean touch(GridCacheEntry<GridGgfsBlockKey, byte[]> entry) {
        byte[] val = entry.peek();

        int blockSize = val != null ? val.length : 0;

        MetaEntry meta = entry.meta(META_NODE);

        // Entry has not been enqueued yet.
        if (meta == null) {
            while (true) {
                Node<GridCacheEntry<GridGgfsBlockKey, byte[]>> node = queue.offerLastx(entry);

                meta = new MetaEntry(node, blockSize);

                if (entry.putMetaIfAbsent(META_NODE, meta) != null) {
                    // Was concurrently added, need to clear it from queue.
                    queue.unlinkx(node);

                    // Queue has not been changed.
                    return false;
                }
                else if (node.item() != null) {
                    if (!entry.isCached()) {
                        // Was concurrently evicted, need to clear it from queue.
                        queue.unlinkx(node);

                        return false;
                    }

                    // Increment current size.
                    changeSize(blockSize);

                    return true;
                }
                // If node was unlinked by concurrent shrink() call, we must repeat the whole cycle.
                else if (!entry.removeMeta(META_NODE, node))
                    return false;
            }
        }
        else {
            int oldBlockSize = meta.size();

            Node<GridCacheEntry<GridGgfsBlockKey, byte[]>> node = meta.node();

            if (queue.unlinkx(node)) {
                // Move node to tail.
                Node<GridCacheEntry<GridGgfsBlockKey, byte[]>> newNode = queue.offerLastx(entry);

                int delta = blockSize - oldBlockSize;

                if (!entry.replaceMeta(META_NODE, meta, new MetaEntry(newNode, blockSize))) {
                    // Was concurrently added, need to clear it from queue.
                    if (queue.unlinkx(newNode))
                        delta -= blockSize;
                }

                if (delta != 0) {
                    changeSize(delta);

                   if (delta > 0)
                       // Total size increased, so shrinking could be needed.
                       return true;
                }
            }
        }

        // Entry is already in queue.
        return false;
    }

    /**
     * Shrinks queue to maximum allowed size.
     */
    private void shrink() {
        long maxSize = this.maxSize;
        int maxBlocks = this.maxBlocks;

        int cnt = queue.sizex();

        for (int i = 0; i < cnt && (maxBlocks > 0 && queue.sizex() > maxBlocks ||
            maxSize > 0 && curSize.longValue() > maxSize); i++) {
            GridCacheEntry<GridGgfsBlockKey, byte[]> entry = queue.poll();

            if (entry == null)
                break; // Queue is empty.

            byte[] val = entry.peek();

            if (val != null)
                changeSize(-val.length); // Change current size as we polled entry from the queue.

            if (!entry.evict()) {
                // Reorder entries which we failed to evict.
                entry.removeMeta(META_NODE);

                touch(entry);
            }
        }
    }

    /**
     * Change current size.
     *
     * @param delta Delta in bytes.
     */
    private void changeSize(int delta) {
        if (delta != 0)
            curSize.add(delta);
    }

    /** {@inheritDoc} */
    @Override public long getMaxSize() {
        return maxSize;
    }

    /** {@inheritDoc} */
    @Override public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    /** {@inheritDoc} */
    @Override public int getMaxBlocks() {
        return maxBlocks;
    }

    /** {@inheritDoc} */
    @Override public void setMaxBlocks(int maxBlocks) {
        this.maxBlocks = maxBlocks;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> getExcludePaths() {
        return Collections.unmodifiableCollection(excludePaths);
    }

    /** {@inheritDoc} */
    @Override public void setExcludePaths(@Nullable Collection<String> excludePaths) {
        this.excludePaths = excludePaths;

        excludeRecompile.set(true);
    }

    /** {@inheritDoc} */
    @Override public long getCurrentSize() {
        return curSize.longValue();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentBlocks() {
        return queue.size();
    }

    /**
     * Check whether provided path must be excluded from evictions.
     *
     * @param path Path.
     * @return {@code True} in case non block of related file must be excluded.
     * @throws IgniteCheckedException In case of faulty patterns.
     */
    public boolean exclude(IgniteFsPath path) throws IgniteCheckedException {
        assert path != null;

        Collection<Pattern> excludePatterns0;

        if (excludeRecompile.compareAndSet(true, false)) {
            // Recompile.
            Collection<String> excludePaths0 = excludePaths;

            if (excludePaths0 != null) {
                excludePatterns0 = new HashSet<>(excludePaths0.size(), 1.0f);

                for (String excludePath : excludePaths0) {
                    try {
                        excludePatterns0.add(Pattern.compile(excludePath));
                    }
                    catch (PatternSyntaxException ignore) {
                        throw new IgniteCheckedException("Invalid regex pattern: " + excludePath);
                    }
                }

                excludePatterns = excludePatterns0;
            }
            else
                excludePatterns0 = excludePatterns = null;
        }
        else
            excludePatterns0 = excludePatterns;

        if (excludePatterns0 != null) {
            String pathStr = path.toString();

            for (Pattern pattern : excludePatterns0) {
                if (pattern.matcher(pathStr).matches())
                    return true;
            }
        }

        return false;
    }

    /**
     * Meta entry.
     */
    private static class MetaEntry {
        /** Queue node. */
        private final Node<GridCacheEntry<GridGgfsBlockKey, byte[]>> node;

        /** Data size. */
        private final int size;

        /**
         * Constructor.
         *
         * @param node Queue node.
         * @param size Data size.
         */
        private MetaEntry(Node<GridCacheEntry<GridGgfsBlockKey, byte[]>> node, int size) {
            assert node != null;
            assert size >= 0;

            this.node = node;
            this.size = size;
        }

        /**
         * @return Queue node.
         */
        private Node<GridCacheEntry<GridGgfsBlockKey, byte[]>> node() {
            return node;
        }

        /**
         * @return Data size.
         */
        private int size() {
            return size;
        }
    }
}

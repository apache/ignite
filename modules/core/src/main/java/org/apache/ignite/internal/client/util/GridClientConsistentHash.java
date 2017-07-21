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

package org.apache.ignite.internal.client.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Controls key to node affinity using consistent hash algorithm. This class is thread-safe
 * and does not have to be externally synchronized.
 * <p>
 * For a good explanation of what consistent hashing is, you can refer to
 * <a href="http://weblogs.java.net/blog/tomwhite/archive/2007/11/consistent_hash.html">Tom White's Blog</a>.
 */
@SuppressWarnings("NullableProblems")
public class GridClientConsistentHash<N> {
    /** Prime number. */
    private static final int PRIME = 15485857;

    /** Random generator. */
    private static final Random RAND = new Random();

    /** Affinity seed. */
    private final Object affSeed;

    /** Map of hash assignments. */
    private final NavigableMap<Integer, SortedSet<N>> circle = new TreeMap<>();

    /** Read/write lock. */
    private final ReadWriteLock rw = new ReentrantReadWriteLock();

    /** Distinct nodes in the hash. */
    private Collection<N> nodes = new HashSet<>();

    /** Nodes comparator to resolve hash codes collisions. */
    private Comparator<N> nodesComp;

    /**
     * Constructs consistent hash using empty affinity seed and {@code MD5} hasher function.
     */
    public GridClientConsistentHash() {
        this(null, null);
    }

    /**
     * Constructs consistent hash using given affinity seed and {@code MD5} hasher function.
     *
     * @param affSeed Affinity seed (will be used as key prefix for hashing).
     */
    public GridClientConsistentHash(Object affSeed) {
        this(null, affSeed);
    }

    /**
     * Constructs consistent hash using given affinity seed and hasher function.
     *
     * @param nodesComp Nodes comparator to resolve hash codes collisions.
     *  If {@code null} natural order will be used.
     * @param affSeed Affinity seed (will be used as key prefix for hashing).
     */
    public GridClientConsistentHash(Comparator<N> nodesComp, Object affSeed) {
        this.nodesComp = nodesComp;
        this.affSeed = affSeed == null ? new Integer(PRIME) : affSeed;
    }

    /**
     * Adds nodes to consistent hash algorithm (if nodes are {@code null} or empty, then no-op).
     *
     * @param nodes Nodes to add.
     * @param replicas Number of replicas for every node.
     */
    public void addNodes(Collection<N> nodes, int replicas) {
        if (nodes == null || nodes.isEmpty())
            return;

        rw.writeLock().lock();

        try {
            for (N node : nodes)
                addNode(node, replicas);
        }
        finally {
            rw.writeLock().unlock();
        }
    }

    /**
     * Adds a node to consistent hash algorithm.
     *
     * @param node New node (if {@code null} then no-op).
     * @param replicas Number of replicas for the node.
     * @return {@code True} if node was added, {@code false} if it is {@code null} or
     *      is already contained in the hash.
     */
    public boolean addNode(N node, int replicas) {
        if (node == null)
            return false;

        long seed = affSeed.hashCode() * 31 + hash(node);

        rw.writeLock().lock();

        try {
            if (!nodes.add(node))
                return false;

            int hash = hash(seed);

            SortedSet<N> set = circle.get(hash);

            if (set == null)
                circle.put(hash, set = new TreeSet<>(nodesComp));

            set.add(node);

            for (int i = 1; i <= replicas; i++) {
                seed = seed * affSeed.hashCode() + i;

                hash = hash(seed);

                set = circle.get(hash);

                if (set == null)
                    circle.put(hash, set = new TreeSet<>(nodesComp));

                set.add(node);
            }

            return true;
        }
        finally {
            rw.writeLock().unlock();
        }
    }

    /**
     * Removes a node and all of its replicas.
     *
     * @param node Node to remove (if {@code null}, then no-op).
     * @return {@code True} if node was removed, {@code false} if node is {@code null} or
     *      not present in hash.
     */
    public boolean removeNode(N node) {
        if (node == null)
            return false;

        rw.writeLock().lock();

        try {
            if (!nodes.remove(node))
                return false;

            for (Iterator<SortedSet<N>> it = circle.values().iterator(); it.hasNext();) {
                SortedSet<N> set = it.next();

                if (!set.remove(node))
                    continue;

                if (set.isEmpty())
                    it.remove();
            }

            return true;
        }
        finally {
            rw.writeLock().unlock();
        }
    }

    /**
     * Gets number of distinct nodes, excluding replicas, in consistent hash.
     *
     * @return Number of distinct nodes, excluding replicas, in consistent hash.
     */
    public int count() {
        rw.readLock().lock();

        try {
            return nodes.size();
        }
        finally {
            rw.readLock().unlock();
        }
    }

    /**
     * Gets size of all nodes (including replicas) in consistent hash.
     *
     * @return Size of all nodes (including replicas) in consistent hash.
     */
    public int size() {
        rw.readLock().lock();

        try {
            int size = 0;

            for (SortedSet<N> set : circle.values())
                size += set.size();

            return size;
        }
        finally {
            rw.readLock().unlock();
        }
    }

    /**
     * Checks if consistent hash has nodes added to it.
     *
     * @return {@code True} if consistent hash is empty, {@code false} otherwise.
     */
    public boolean isEmpty() {
        return count() == 0;
    }

    /**
     * Gets set of all distinct nodes in the consistent hash (in no particular order).
     *
     * @return Set of all distinct nodes in the consistent hash.
     */
    public Set<N> nodes() {
        rw.readLock().lock();

        try {
            return new HashSet<>(nodes);
        }
        finally {
            rw.readLock().unlock();
        }
    }

    /**
     * Picks a random node from consistent hash.
     *
     * @return Random node from consistent hash or {@code null} if there are no nodes.
     */
    public N random() {
        return node(RAND.nextLong());
    }

    /**
     * Gets node for a key.
     *
     * @param key Key.
     * @return Node.
     */
    public N node(Object key) {
        int hash = hash(key);

        rw.readLock().lock();

        try {
            Map.Entry<Integer, SortedSet<N>> firstEntry = circle.firstEntry();

            if (firstEntry == null)
                return null;

            Map.Entry<Integer, SortedSet<N>> tailEntry = circle.tailMap(hash, true).firstEntry();

            // Get first node hash in the circle clock-wise.
            return circle.get(tailEntry == null ? firstEntry.getKey() : tailEntry.getKey()).first();
        }
        finally {
            rw.readLock().unlock();
        }
    }

    /**
     * Gets node for a given key.
     *
     * @param key Key to get node for.
     * @param inc Optional inclusion set. Only nodes contained in this set may be returned.
     *      If {@code null}, then all nodes may be included.
     * @return Node for key, or {@code null} if node was not found.
     */
    public N node(Object key, Collection<N> inc) {
        return node(key, inc, null);
    }

    /**
     * Gets node for a given key.
     *
     * @param key Key to get node for.
     * @param inc Optional inclusion set. Only nodes contained in this set may be returned.
     *      If {@code null}, then all nodes may be included.
     * @param exc Optional exclusion set. Only nodes not contained in this set may be returned.
     *      If {@code null}, then all nodes may be returned.
     * @return Node for key, or {@code null} if node was not found.
     */
    public N node(Object key, @Nullable final Collection<N> inc, @Nullable final Collection<N> exc) {
        if (inc == null && exc == null)
            return node(key);

        return node(key, new GridClientPredicate<N>() {
            @Override public boolean apply(N n) {
                return (inc == null || inc.contains(n)) && (exc == null || !exc.contains(n));
            }
        });
    }

    /**
     * Gets node for a given key.
     *
     * @param key Key to get node for.
     * @param p Optional predicate for node filtering.
     * @return Node for key, or {@code null} if node was not found.
     */
    public N node(Object key, GridClientPredicate<N>... p) {
        if (p == null || p.length == 0)
            return node(key);

        int hash = hash(key);

        rw.readLock().lock();

        try {
            final int size = nodes.size();

            if (size == 0)
                return null;

            Set<N> failed = null;

            // Move clock-wise starting from selected position 'hash'.
            for (SortedSet<N> set : circle.tailMap(hash, true).values()) {
                for (N n : set) {
                    if (failed != null && failed.contains(n))
                        continue;

                    if (apply(p, n))
                        return n;

                    if (failed == null)
                        failed = new HashSet<>();

                    failed.add(n);

                    if (failed.size() == size)
                        return null;
                }
            }

            //
            // Copy-paste is used to escape several new objects creation.
            //

            // Wrap around moving clock-wise from the circle start.
            for (SortedSet<N> set : circle.headMap(hash, false).values()) { // Circle head.
                for (N n : set) {
                    if (failed != null && failed.contains(n))
                        continue;

                    if (apply(p, n))
                        return n;

                    if (failed == null)
                        failed = U.newHashSet(size);

                    failed.add(n);

                    if (failed.size() == size)
                        return null;
                }
            }

            return null;
        }
        finally {
            rw.readLock().unlock();
        }
    }

    /**
     * Apply predicate to the node.
     *
     * @param p Predicate.
     * @param n Node.
     * @return {@code True} if filter passed or empty.
     */
    private boolean apply(GridClientPredicate<N>[] p, N n) {
        if (p != null) {
            for (GridClientPredicate<? super N> r : p) {
                if (r != null && !r.apply(n))
                    return false;
            }
        }

        return true;
    }

    /**
     * Gets hash code for a given object.
     *
     * @param o Object to get hash code for.
     * @return Hash code.
     */
    public static int hash(Object o) {
        int h = o == null ? 0 : o instanceof byte[] ? Arrays.hashCode((byte[])o) : o.hashCode();

        // Spread bits to hash code.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);

        return h ^ (h >>> 16);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [affSeed=" + affSeed +
            ", circle=" + circle +
            ", nodesComp=" + nodesComp +
            ", nodes=" + nodes + "]";
    }
}

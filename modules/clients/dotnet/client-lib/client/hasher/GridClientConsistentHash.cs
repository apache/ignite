/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Hasher {
    using System;
    using System.Threading;
    using System.Collections.Generic;
    using GridGain.Client;

    using U = GridGain.Client.Util.GridClientUtils;

    /**
     * <summary>
     * Base consistent hash implementation controls key to node affinity using consistent hash algorithm.
     * This class is thread-safe and does not have to be externally synchronized.</summary>
     *
     * <remarks>
     * For a good explanation of what consistent hashing is, you can refer to
     * <a href="http://weblogs.java.net/blog/tomwhite/archive/2007/11/consistent_hash.html">Tom White's Blog</a>.
     * </remarks>
     */
    internal class GridClientConsistentHash {
        /** <summary>Prime number.</summary> */
        public static readonly UInt32 Prime = 15485857;

        /**
         * <summary>
         * Constructs client consistent hash. Deny public construction of this class.</summary>
         */
        protected GridClientConsistentHash() {
        }
    }

    /**
     * <summary>
     * Generic consistent hash implementation controls key to node affinity using consistent hash algorithm.
     * This class is thread-safe and does not have to be externally synchronized.</summary>
     *
     * <remarks>
     * For a good explanation of what consistent hashing is, you can refer to
     * <a href="http://weblogs.java.net/blog/tomwhite/archive/2007/11/consistent_hash.html">Tom White's Blog</a>.
     * </remarks>
     */
    internal class GridClientConsistentHash<TNode> : GridClientConsistentHash {
        /** <summary>Random generator.</summary> */
        private readonly Random Rnd = new Random();

        /** <summary>Affinity seed.</summary> */
        private readonly Object affSeed;

        /** <summary>Map of hash assignments.</summary> */
        private readonly C5.TreeDictionary<int, SortedSet<TNode>> circle = new C5.TreeDictionary<int, SortedSet<TNode>>();

        /** <summary>Read/write lock.</summary> */
        private readonly ReaderWriterLock rw = new ReaderWriterLock();

        /** <summary>Distinct nodes in the hash.</summary> */
        private ISet<TNode> nodes = new HashSet<TNode>();

        /** <summary>Nodes comparator to resolve hash codes collisions. </summary> */
        private readonly IComparer<TNode> nodesComp;

        /**
         * <summary>
         * Constructs consistent hash using empty affinity seed.</summary>
         */
        public GridClientConsistentHash()
            : this(null, null) {
        }

        /**
         * <summary>
         * Constructs consistent hash using given affinity seed.</summary>
         *
         * <param name="affSeed">Affinity seed (will be used as key prefix for hashing).</param>
         */
        public GridClientConsistentHash(Object affSeed)
            : this(null, affSeed) {
        }

        /**
         * <summary>
         * Constructs consistent hash using given affinity seed and hasher function.</summary>
         *
         * <param name="nodesComp">Nodes comparator to resolve hash codes collisions.</param>
         * <param name="affSeed">Affinity seed (will be used as key prefix for hashing).</param>
         */
        public GridClientConsistentHash(IComparer<TNode> nodesComp, Object affSeed) {
            this.nodesComp = nodesComp;
            this.affSeed = affSeed == null ? Prime : affSeed;
        }

        /**
         * <summary>
         * Adds nodes to consistent hash algorithm (if nodes are <c>null</c> or empty, then no-op).</summary>
         *
         * <param name="nodes">Nodes to add.</param>
         * <param name="replicas">Number of replicas for every node.</param>
         */
        public void AddNodes(ICollection<TNode> nodes, int replicas) {
            if (nodes == null || nodes.Count == 0)
                return;

            rw.AcquireWriterLock(Timeout.Infinite);

            try {
                foreach (TNode node in nodes) {
                    AddNode(node, replicas);
                }
            }
            finally {
                rw.ReleaseWriterLock();
            }
        }

        /**
         * <summary>
         * Adds a node to consistent hash algorithm.</summary>
         *
         * <param name="node">New node (if <c>null</c> then no-op).</param>
         * <param name="replicas">Number of replicas for the node.</param>
         * <returns><c>True</c> if node was added, <c>null</c> if it is <c>null</c> or</returns>
         *      is already contained in the hash.
         */
        public bool AddNode(TNode node, int replicas) {
            if (node == null)
                return false;

            long seed = GridClientJavaHelper.GetJavaHashCode(affSeed) * 31 + SpreadHash(node);

            rw.AcquireWriterLock(Timeout.Infinite);

            try {
                if (!nodes.Add(node))
                    return false;

                int hash = SpreadHash(seed);

                SortedSet<TNode> set;

                if (!circle.Find(hash, out set))
                    circle.Add(hash, set = new SortedSet<TNode>(nodesComp));

                set.Add(node);

                for (int i = 1; i <= replicas; i++) {
                    seed = seed * GridClientJavaHelper.GetJavaHashCode(affSeed) + i;

                    hash = SpreadHash(seed);

                    if (!circle.Find(hash, out set))
                        circle.Add(hash, set = new SortedSet<TNode>(nodesComp));

                    set.Add(node);
                }

                return true;
            }
            finally {
                rw.ReleaseWriterLock();
            }
        }

        /**
         * <summary>
         * Removes a node and all of its replicas.</summary>
         *
         * <param name="node">Node to remove (if <c>null</c>, then no-op).</param>
         * <returns><c>True</c> if node was removed, <c>True</c> if node is <c>True</c> or</returns>
         *      not present in hash.
         */
        public bool RemoveNode(TNode node) {
            if (node == null)
                return false;

            rw.AcquireWriterLock(Timeout.Infinite);

            try {
                if (!nodes.Remove(node))
                    return false;

                ISet<int> keys = new HashSet<int>(circle.Keys);

                foreach (int key in keys) {
                    SortedSet<TNode> set;

                    if (!circle.Find(key, out set))
                        continue;

                    if (set.Remove(node) && set.Count == 0)
                        circle.Remove(key);
                }

                return true;
            }
            finally {
                rw.ReleaseWriterLock();
            }
        }

        /**
         * <summary>
         * Gets number of distinct nodes, excluding replicas, in consistent hash.</summary>
         *
         * <returns>Number of distinct nodes, excluding replicas, in consistent hash.</returns>
         */
        public int Count {
            get {
                rw.AcquireReaderLock(Timeout.Infinite);

                try {
                    return nodes.Count;
                }
                finally {
                    rw.ReleaseReaderLock();
                }
            }
        }

        /**
         * <summary>
         * Gets size of all nodes (including replicas) in consistent hash.</summary>
         *
         * <returns>Size of all nodes (including replicas) in consistent hash.</returns>
         */
        public int Size {
            get {
                rw.AcquireReaderLock(Timeout.Infinite);

                try {
                    int size = 0;

                    foreach (var set in circle.Values)
                        size += set.Count;

                    return size;
                }
                finally {
                    rw.ReleaseReaderLock();
                }
            }
        }

        /**
         * <summary>
         * Checks if consistent hash has nodes added to it.</summary>
         *
         * <returns><c>True</c> if consistent hash is empty, <c>null</c> otherwise.</returns>
         */
        public bool IsEmpty {
            get {
                return Size == 0;
            }
        }

        /**
         * <summary>
         * Picks a random node from consistent hash.</summary>
         *
         * <returns>Random node from consistent hash or <c>null</c> if there are no nodes.</returns>
         */
        public TNode Random {
            get {
                IList<TNode> nodes = Nodes;

                return nodes.Count == 0 ? default(TNode) : nodes[Rnd.Next(nodes.Count)];
            }
        }

        /**
         * <summary>
         * Gets set of all distinct nodes in the consistent hash (in no particular order).</summary>
         *
         * <returns>Set of all distinct nodes in the consistent hash.</returns>
         */
        public IList<TNode> Nodes {
            get {
                rw.AcquireReaderLock(Timeout.Infinite);

                try {
                    return new List<TNode>(nodes);
                }
                finally {
                    rw.ReleaseReaderLock();
                }
            }
        }

        /**
         * <summary>
         * Gets node for a key.</summary>
         *
         * <param name="key">Key.</param>
         * <returns>Node.</returns>
         */
        public TNode Node(Object key) {
            return Node(key, U.All<TNode>());
        }

        /**
         * <summary>
         * Gets node for a given key.</summary>
         *
         * <param name="key">Key to get node for.</param>
         * <param name="inc">Optional inclusion set. Only nodes contained in this set may be returned.</param>
         *      If <c>null</c>, then all nodes may be included.
         * <returns>Node for key, or <c>null</c> if node was not found.</returns>
         */
        public TNode Node(Object key, ICollection<TNode> inc) {
            return Node(key, inc, null);
        }

        /**
         * <summary>
         * Gets node for a given key.</summary>
         *
         * <param name="key">Key to get node for.</param>
         * <param name="inc">Optional inclusion set. Only nodes contained in this set may be returned.</param>
         *      If <c>null</c>, then all nodes may be included.
         * <param name="exc">Optional exclusion set. Only nodes not contained in this set may be returned.</param>
         *      If <c>null</c>, then all nodes may be returned.
         * <returns>Node for key, or <c>null</c> if node was not found.</returns>
         */
        public TNode Node(Object key, ICollection<TNode> inc, ICollection<TNode> exc) {
            return Node(key, U.Filter<TNode>(inc, exc));
        }

        /**
         * <summary>
         * Gets node for a given key.</summary>
         *
         * <param name="key">Key to get node for.</param>
         * <param name="filter">Optional predicate for node filtering.</param>
         * <returns>Node for key, or <c>null</c> if node was not found.</returns>
         */
        public TNode Node(Object key, Predicate<TNode> filter) {
            int hash = SpreadHash(key);

            if (filter == null)
                filter = U.All<TNode>();

            rw.AcquireReaderLock(Timeout.Infinite);

            try {
                // Get the first node by hash in the circle clock-wise.
                foreach (C5.KeyValuePair<int, SortedSet<TNode>> pair in circle.RangeFrom(hash))
                    foreach (TNode n in pair.Value)
                        if (filter(n))
                            return n;

                foreach (C5.KeyValuePair<int, SortedSet<TNode>> pair in circle.RangeTo(hash))
                    foreach (TNode n in pair.Value)
                        if (filter(n))
                            return n;

                return default(TNode); // Circle is empty.
            }
            finally {
                rw.ReleaseReaderLock();
            }
        }

        /**
         * <summary>
         * Gets hash code for a given object.</summary>
         *
         * <param name="val">Value to get hash code for.</param>
         * <returns>Hash code.</returns>
         */
        protected internal int SpreadHash(Object val) {
            // Obtaining server hash code.
            int h = val == null ? 0 : GridClientJavaHelper.GetJavaHashCode(val);

            // 'unchecked((uint)h)' makes an unsigned integer from signed one,
            // so '>>' bahaves as '>>>' in java.
            // 'unchecked((int)...)' required to make result signed again.
            h += (h << 15) ^ unchecked((int)0xffffcd7d);
            h ^= unchecked((int)(unchecked((uint)h) >> 10));
            h += (h << 3);
            h ^= unchecked((int)(unchecked((uint)h) >> 6));
            h += (h << 2) + (h << 14);
            h = h ^ unchecked((int)(unchecked((uint)h) >> 16));

            return h;
        }
    }
}

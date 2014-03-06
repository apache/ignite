// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Hasher {
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using NUnit.Framework;

    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;
    
    /** <summary>Test for consistent hash management class.</summary> */
    [TestFixture]
    public class GridClientConsistentHashTest {
        private const int TEST_CYCLE_SIZE = 10000;

        /** Replicas count. */
        private static readonly int REPLICAS = 512;

        /** <summary>Test fully initialized consistent hash.</summary> */
        [Test]
        public void TestHashInitialization() {
            GridClientConsistentHash<TestNode> hash = new GridClientConsistentHash<TestNode>();

            IList<TestNode> nodes = new List<TestNode>();

            for (int i = 0; i < 100; i++)
                nodes.Add(new TestNode(i));

            foreach (TestNode n in nodes)
                hash.AddNode(n, 10);

            hash.AddNodes(nodes, 10);
        }

        /**
         * Test hash codes collisions.
         * 
         * @throws Exception In case of any exception.
         */
        public void TestCollisions() {
            IDictionary<int, ISet<Guid>> map = new Dictionary<int, ISet<Guid>>();

            // Different nodes, but collide hash codes.
            ISet<Guid> nodes = new HashSet<Guid>();

            // Generate several nodes with collide hash codes.
            while (nodes.Count < 10) {
                Guid uuid = Guid.NewGuid();
                int hashCode = uuid.GetHashCode();

                ISet<Guid> set;

                if (!map.TryGetValue(hashCode, out set) || set == null)
                    map.Add(hashCode, set = new HashSet<Guid>());

                set.Add(uuid);

                if (set.Count > 1)
                    foreach (Guid i in set)
                        nodes.Add(i);
            }

            map.Clear(); // Clean up.

            GridClientConsistentHash<Guid> hash = new GridClientConsistentHash<Guid>(null, null);

            hash.AddNodes(nodes, REPLICAS);

            bool fail = false;

            foreach (Guid exp in nodes) {
                Guid act = hash.Node(0, U.List(exp));

                if (exp.Equals(act))
                    Dbg.WriteLine("Validation succeed [exp=" + exp + ", act=" + act + ']');
                else {
                    Dbg.WriteLine("Validation failed  [exp=" + exp + ", act=" + act + ']');

                    fail = true;
                }
            }

            if (fail)
                Assert.Fail("Failed to resolve consistent hash node, when node's hash codes collide: " + nodes);
        }

        /** <summary>Sample class to serialize and pass from the client into the grid.</summary> */
        private class TestNode : IGridClientConsistentHashObject {
            /**
             * <summary>
             * Constructs sample serializable object to pass from the client into the grid.
             *
             * <param name="id">Object id.</param>
             */
            public TestNode(int id) {
                this.Id = id;
            }

            /** <summary>Object id.</summary> */
            public int Id {
                get;
                private set;
            }

            /** <inheritdoc /> */
            override public int GetHashCode() {
                return Id;
            }

            /** <inheritdoc /> */
            override public bool Equals(object obj) {
                var n = obj as TestNode;

                return n != null && n.Id == Id;
            }
        }
    }
}

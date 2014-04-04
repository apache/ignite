// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Threading;
    using System.Collections.Generic;
    using NUnit.Framework;

    using U = GridGain.Client.Util.GridClientUtils;

    [TestFixture]
    public class GridClientTopologyTest {
        private GridClientTopology top;

        [SetUp]
        public void InitTopology() {
            top = new GridClientTopology(Guid.NewGuid(), true);
        }

        [TearDown]
        public void CloseTopology() {
            top.Dispose();
        }

        [Test]
        public void TestTopology() {
            IList<GridClientNodeImpl> nodes = new List<GridClientNodeImpl>();

            // Fill nodes collection.
            for (int i = 0; i < 100; i++)
                nodes.Add(new GridClientNodeImpl(Guid.NewGuid()));

            TestTopologyListener lsnr = new TestTopologyListener();

            top.AddTopologyListener(lsnr);

            // Test initial topology state.
            Assert.AreEqual(0, lsnr.added.Count);
            Assert.AreEqual(0, lsnr.removed.Count);
            Assert.AreEqual(0, top.Nodes().Count);

            // Test topology the first update.
            top.UpdateTopology(nodes);

            DateTime timeout = U.Now + TimeSpan.FromSeconds(1);

            while (lsnr.added.Count != nodes.Count && U.Now < timeout)
                Thread.Sleep(50);

            Assert.AreEqual(nodes.Count, lsnr.added.Count);
            Assert.AreEqual(0, lsnr.removed.Count);

            for (int i = 0; i < nodes.Count; i++)
                Assert.AreEqual(nodes[i].Id, lsnr.added[i].Id);

            // Test the list of nodes failed and removed from topology.
            lsnr.Clear();

            for (int i = 0; i < 20; i++)
                top.NodeFailed(nodes[i].Id);

            timeout = U.Now + TimeSpan.FromSeconds(1);

            while (lsnr.removed.Count != 20 && U.Now < timeout)
                Thread.Sleep(50);

            Assert.AreEqual(0, lsnr.added.Count);
            Assert.AreEqual(20, lsnr.removed.Count);

            // Validate all removed nodes vere passed into listeners.
            for (int i = 0; i < 20; i++)
                Assert.AreEqual(nodes[i].Id, lsnr.removed[i].Id);

            // Validate topology node by id.
            for (int i = 0; i < 100; i++)
                Assert.AreEqual(i < 20 ? null : nodes[i], top.Node(nodes[i].Id));

            // Validate full topology snapshot.
            IList<IGridClientNode> snapshot = top.Nodes();

            for (int i = 20; i < 100; i++)
                Assert.AreEqual(nodes[i].Id, snapshot[i - 20].Id);

            // Validate single node updates for new and existent ids.
            lsnr.Clear();

            for (int i = 10; i < 30; i++) {
                top.UpdateNode(nodes[i]);

                Guid id = nodes[i].Id;

                Assert.AreEqual(id, top.Node(id).Id);
            }

            timeout = U.Now + TimeSpan.FromSeconds(1);

            while (lsnr.added.Count != 10 && lsnr.removed.Count != 10 && U.Now < timeout)
                Thread.Sleep(50);

            Assert.AreEqual(10, lsnr.added.Count);
            Assert.AreEqual(0, lsnr.removed.Count);
        }

        private class TestTopologyListener : IGridClientTopologyListener {
            public IList<IGridClientNode> added = new List<IGridClientNode>();
            public IList<IGridClientNode> removed = new List<IGridClientNode>();

            public void OnNodeAdded(IGridClientNode node) {
                added.Add(node);
            }

            public void OnNodeRemoved(IGridClientNode node) {
                removed.Add(node);
            }

            public void Clear() {
                added.Clear();
                removed.Clear();
            }
        }
    }
}

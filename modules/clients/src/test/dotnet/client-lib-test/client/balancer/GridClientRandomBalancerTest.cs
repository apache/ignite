// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Balancer {
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using NUnit.Framework;

    [TestFixture]
    public class GridClientRandomBalancerTest {
            [Test]
            public void TestBalancedNode() {
                GridClientRandomBalancer balancer = new GridClientRandomBalancer();
                ICollection<IGridClientNode> nodes = new List<IGridClientNode>();

                Assert.AreEqual(0, nodes.Count);
                try {
                    balancer.BalancedNode(nodes);

                    Assert.Fail("Expected empty list exception.");
                }
                catch (ArgumentException) {
                    /* Noop - normal behaviour */
                }

                // Fill nodes collection.
                for (int i = 0; i < 100; i++)
                    nodes.Add(null);

                for (int i = 0; i < 100; i++) {
                    IGridClientNode node = balancer.BalancedNode(nodes);

                    Assert.IsTrue(nodes.Contains(node));
                }

                nodes = new HashSet<IGridClientNode>(nodes);

                for (int i = 0; i < 100; i++) {
                    IGridClientNode node = balancer.BalancedNode(nodes);

                    Assert.IsTrue(nodes.Contains(node));
                }

        }
    }
}

// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Threading;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using NUnit.Framework;
    using GridGain.Client.Hasher;
    using GridGain.Client.Ssl;
    using GridGain.Client.Util;

    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;

    public class GridClientPartitionAffinityTest {
        /**
         * Test predefined affinity - must be ported to other clients.
         */
        [Test]
        public void TestPredefined() {
            GridClientPartitionAffinity aff = new GridClientPartitionAffinity(
                GridClientPartitionAffinity.DefaultPartitionsCount, null);

            ((GridClientPartitionAffinity)aff).HashIdResolver = (IGridClientNode node) => node.Id;

            IList<IGridClientNode> nodes = new List<IGridClientNode>();

            nodes.Add(CreateNode("000ea4cd-f449-4dcb-869a-5317c63bd619", 50));
            nodes.Add(CreateNode("010ea4cd-f449-4dcb-869a-5317c63bd62a", 60));
            nodes.Add(CreateNode("0209ec54-ff53-4fdb-8239-5a3ac1fb31bd", 70));
            nodes.Add(CreateNode("0309ec54-ff53-4fdb-8239-5a3ac1fb31ef", 80));
            nodes.Add(CreateNode("040c9b94-02ae-45a6-9d5c-a066dbdf2636", 90));
            nodes.Add(CreateNode("050c9b94-02ae-45a6-9d5c-a066dbdf2747", 100));
            nodes.Add(CreateNode("0601f916-4357-4cfe-a7df-49d4721690bf", 110));
            nodes.Add(CreateNode("0702f916-4357-4cfe-a7df-49d4721691c0", 120));

            foreach (IGridClientNode n in nodes)
                Dbg.WriteLine(GridClientJavaHelper.GetJavaHashCode(n));

            IDictionary<Object, int> data = new Dictionary<Object, int>();

            data.Add("", 4);
            data.Add("asdf", 4);
            data.Add("224ea4cd-f449-4dcb-869a-5317c63bd619", 5);
            data.Add("fdc9ec54-ff53-4fdb-8239-5a3ac1fb31bd", 2);
            data.Add("0f9c9b94-02ae-45a6-9d5c-a066dbdf2636", 2);
            data.Add("d8f1f916-4357-4cfe-a7df-49d4721690bf", 7);
            data.Add("c77ffeae-78a1-4ee6-a0fd-8d197a794412", 3);
            data.Add("35de9f21-3c9b-4f4a-a7d5-3e2c6cb01564", 1);
            data.Add("d67eb652-4e76-47fb-ad4e-cd902d9b868a", 7);

            data.Add(0, 4);
            data.Add(1, 7);
            data.Add(12, 5);
            data.Add(123, 6);
            data.Add(1234, 4);
            data.Add(12345, 6);
            data.Add(123456, 6);
            data.Add(1234567, 6);
            data.Add(12345678, 0);
            data.Add(123456789, 7);
            data.Add(1234567890, 7);
            data.Add(1234567890L, 7);
            data.Add(12345678901L, 2);
            data.Add(123456789012L, 1);
            data.Add(1234567890123L, 0);
            data.Add(12345678901234L, 1);
            data.Add(123456789012345L, 6);
            data.Add(1234567890123456L, 7);
            data.Add(-23456789012345L, 4);
            data.Add(-2345678901234L, 1);
            data.Add(-234567890123L, 5);
            data.Add(-23456789012L, 5);
            data.Add(-2345678901L, 7);
            data.Add(-234567890L, 4);
            data.Add(-234567890, 7);
            data.Add(-23456789, 7);
            data.Add(-2345678, 0);
            data.Add(-234567, 6);
            data.Add(-23456, 6);
            data.Add(-2345, 6);
            data.Add(-234, 7);
            data.Add(-23, 5);
            data.Add(-2, 4);

            data.Add(0x80000000, 4);
            data.Add(0x7fffffff, 7);
            data.Add(0x8000000000000000L, 4);
            data.Add(0x7fffffffffffffffL, 4);

            data.Add(+1.1, 3);
            data.Add(-10.01, 4);
            data.Add(+100.001, 4);
            data.Add(-1000.0001, 4);
            data.Add(+1.7976931348623157E+308, 6);
            data.Add(-1.7976931348623157E+308, 6);
            data.Add(+4.9E-324, 7);
            data.Add(-4.9E-324, 7);

            bool ok = true;

            foreach (KeyValuePair<Object, int> entry in data) {
                Guid exp = nodes[entry.Value].Id;
                Guid act = aff.Node(entry.Key, nodes).Id;

                if (exp.Equals(act))
                    continue;

                ok = false;

                Dbg.WriteLine("Failed to validate affinity for key '" + entry.Key + "' [expected=" + exp +
                    ", actual=" + act + ".");
            }

            if (ok)
                return;

            Dbg.Fail("Client partitioned affinity validation fails.");
        }

        /**
         * Test predefined affinity - must be ported to other clients.
         */
        [Test]
        public void TestPredefinedHashIdResolver() {
            IGridClientDataAffinity aff = new GridClientPartitionAffinity(
                GridClientPartitionAffinity.DefaultPartitionsCount, null);

            ((GridClientPartitionAffinity)aff).HashIdResolver = (IGridClientNode node) => node.ReplicaCount;

            IList<IGridClientNode> nodes = new List<IGridClientNode>();

            nodes.Add(CreateNode("000ea4cd-f449-4dcb-869a-5317c63bd619", 50));
            nodes.Add(CreateNode("010ea4cd-f449-4dcb-869a-5317c63bd62a", 60));
            nodes.Add(CreateNode("0209ec54-ff53-4fdb-8239-5a3ac1fb31bd", 70));
            nodes.Add(CreateNode("0309ec54-ff53-4fdb-8239-5a3ac1fb31ef", 80));
            nodes.Add(CreateNode("040c9b94-02ae-45a6-9d5c-a066dbdf2636", 90));
            nodes.Add(CreateNode("050c9b94-02ae-45a6-9d5c-a066dbdf2747", 100));
            nodes.Add(CreateNode("0601f916-4357-4cfe-a7df-49d4721690bf", 110));
            nodes.Add(CreateNode("0702f916-4357-4cfe-a7df-49d4721691c0", 120));

            IDictionary<Object, int> data = new Dictionary<Object, int>();

            data.Add("", 4);
            data.Add("asdf", 3);
            data.Add("224ea4cd-f449-4dcb-869a-5317c63bd619", 5);
            data.Add("fdc9ec54-ff53-4fdb-8239-5a3ac1fb31bd", 2);
            data.Add("0f9c9b94-02ae-45a6-9d5c-a066dbdf2636", 2);
            data.Add("d8f1f916-4357-4cfe-a7df-49d4721690bf", 4);
            data.Add("c77ffeae-78a1-4ee6-a0fd-8d197a794412", 3);
            data.Add("35de9f21-3c9b-4f4a-a7d5-3e2c6cb01564", 4);
            data.Add("d67eb652-4e76-47fb-ad4e-cd902d9b868a", 2);

            data.Add(0, 4);
            data.Add(1, 1);
            data.Add(12, 7);
            data.Add(123, 1);
            data.Add(1234, 6);
            data.Add(12345, 2);
            data.Add(123456, 5);
            data.Add(1234567, 4);
            data.Add(12345678, 6);
            data.Add(123456789, 3);
            data.Add(1234567890, 3);
            data.Add(1234567890L, 3);
            data.Add(12345678901L, 0);
            data.Add(123456789012L, 1);
            data.Add(1234567890123L, 3);
            data.Add(12345678901234L, 5);
            data.Add(123456789012345L, 5);
            data.Add(1234567890123456L, 7);
            data.Add(-23456789012345L, 6);
            data.Add(-2345678901234L, 4);
            data.Add(-234567890123L, 3);
            data.Add(-23456789012L, 0);
            data.Add(-2345678901L, 4);
            data.Add(-234567890L, 5);
            data.Add(-234567890, 3);
            data.Add(-23456789, 3);
            data.Add(-2345678, 6);
            data.Add(-234567, 4);
            data.Add(-23456, 5);
            data.Add(-2345, 2);
            data.Add(-234, 7);
            data.Add(-23, 6);
            data.Add(-2, 6);

            data.Add(0x80000000, 7);
            data.Add(0x7fffffff, 1);
            data.Add(0x8000000000000000L, 7);
            data.Add(0x7fffffffffffffffL, 7);

            data.Add(+1.1, 2);
            data.Add(-10.01, 0);
            data.Add(+100.001, 2);
            data.Add(-1000.0001, 0);
            data.Add(+1.7976931348623157E+308, 6);
            data.Add(-1.7976931348623157E+308, 1);
            data.Add(+4.9E-324, 1);
            data.Add(-4.9E-324, 1);

            bool ok = true;

            foreach (KeyValuePair<Object, int> entry in data) {
                Guid exp = nodes[entry.Value].Id;
                Guid act = aff.Node(entry.Key, nodes).Id;

                if (exp.Equals(act))
                    continue;

                ok = false;

                Dbg.WriteLine("Failed to validate affinity for key '" + entry.Key + "' [expected=" + exp +
                    ", actual=" + act + ".");
            }

            if (ok)
                return;

            Dbg.Fail("Client partitioned affinity validation fails.");
        }

        /**
         * Create node with specified node id and replica count.
         *
         * @param nodeId Node id.
         * @param replicaCnt Node partitioned affinity replica count.
         * @return New node with specified node id and replica count.
         */
        private IGridClientNode CreateNode(String nodeId, int replicaCnt) {
            GridGain.Client.Impl.GridClientNodeImpl node = new GridGain.Client.Impl.GridClientNodeImpl(Guid.Parse(nodeId));

            node.ReplicaCount = replicaCnt;

            return node;
        }
    }
}

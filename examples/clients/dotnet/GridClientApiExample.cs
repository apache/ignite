/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;

    using GridGain.Client.Balancer;

    using X = System.Console;

    /**
     * <summary>
     * This example demonstrates use of C# remote Compute Client API. To execute
     * this example you should start an instance of <c>ClientExampleNodeStartup</c>
     * Java class which will start up a GridGain node with proper configuration.
     * <para/>
     * Note that this example requires <c>org.gridgain.examples.misc.client.api.ClientExampleTask</c>
     * class to be present in remote nodes' classpath. If remote nodes are run by <c>ggstart.{sh|bat}</c> script
     * then <c>gridgain-examples.jar</c> file should be placed to <c>GRIDGAIN_HOME/libs/ext</c> folder.
     * <para/>
     * After node has been started this example creates a client and executes few test tasks.
     * <para/>
     * Note that different nodes cannot share the same port for rest services. If you want
     * to start more than one node on the same physical machine you must provide different
     * configurations for each node. Otherwise, this example would not work.</summary>
     */
    public class GridClientApiExample {
        /** <summary>Grid node address to connect to.</summary> */
        private static readonly String ServerAddress = "127.0.0.1";

        /**
         * <summary>
         * Starts up an empty node with specified configuration, then runs client compute example.</summary>
         *
         * <exception cref="GridClientException">If failed.</exception>
         */
        [STAThread]
        public static void Main() {
            /* Enable debug messages. */
            //Debug.Listeners.Add(new TextWriterTraceListener(System.Console.Out));

            String taskName = "org.gridgain.examples.misc.client.api.ClientExampleTask";
            String taskArg = ".NET client - ";

            IGridClient client = CreateClient();

            try {
                // Show grid topology.
                X.WriteLine(">>> Client created, current grid topology: " + ToString(client.Compute().Nodes()));

                // Random node ID.
                Guid randNodeId = client.Compute().Nodes()[0].Id;

                // Note that in this example we get a fixed projection for task call because we cannot guarantee that
                // other nodes contain ClientExampleTask in classpath.
                IGridClientCompute prj = client.Compute().Projection(delegate(IGridClientNode node) {
                    return node.Id.Equals(randNodeId);
                });

                // Execute test task that will count total number of nodes in grid.
                int entryCnt = prj.Execute<int>(taskName, taskArg + "predicate projection");

                X.WriteLine(">>> Predicate projection : there are totally " + entryCnt + " nodes in the grid");

                // Same as above, using different projection API.
                IGridClientNode clntNode = prj.Node(randNodeId);

                prj = prj.Projection(clntNode);

                entryCnt = prj.Execute<int>(taskName, taskArg + "node projection");

                X.WriteLine(">>> GridClientNode projection : there are totally " + entryCnt + " nodes in the grid");

                // Use of collections is also possible.
                prj = prj.Projection(new IGridClientNode[] { clntNode });

                entryCnt = prj.Execute<int>(taskName, taskArg + "nodes collection projection");

                X.WriteLine(">>> Collection projection : there are totally " + entryCnt + " nodes in the grid");

                // Balancing - may be random or round-robin. Users can create
                // custom load balancers as well.
                IGridClientLoadBalancer balancer = new GridClientRandomBalancer();

                // Balancer may be added to predicate or collection examples.
                prj = client.Compute().Projection(delegate(IGridClientNode node) {
                    return node.Id.Equals(randNodeId);
                }, balancer);

                entryCnt = prj.Execute<int>(taskName, taskArg + "predicate projection with balancer");

                X.WriteLine(">>> Predicate projection with balancer : there are totally " + entryCnt +
                    " nodes in the grid");

                // Now let's try round-robin load balancer.
                balancer = new GridClientRoundRobinBalancer();

                prj = prj.Projection(new IGridClientNode[] { clntNode }, balancer);

                entryCnt = prj.Execute<int>(taskName, taskArg + "node projection with balancer");

                X.WriteLine(">>> GridClientNode projection : there are totally " + entryCnt + " nodes in the grid");

                // Execution may be asynchronous.
                IGridClientFuture<int> fut = prj.ExecuteAsync<int>(taskName, taskArg + "asynchronous execution");

                X.WriteLine(">>> Execute async : there are totally " + fut.Result + " nodes in the grid");

                // GridClientCompute can be queried for nodes participating in it.
                ICollection<IGridClientNode> c = prj.Nodes(new Guid[] { randNodeId });

                X.WriteLine(">>> Nodes with Guid " + randNodeId + " : " + ToString(c));

                // Nodes may also be filtered with predicate. Here
                // we create projection which only contains local node.
                c = prj.Nodes(delegate(IGridClientNode node) {
                    return node.Id.Equals(randNodeId);
                });

                X.WriteLine(">>> Nodes filtered with predicate : " + ToString(c));

                // Information about nodes may be refreshed explicitly.
                clntNode = prj.RefreshNode(randNodeId, true, true);

                X.WriteLine(">>> Refreshed node : " + clntNode);

                // As usual, there's also an asynchronous version.
                IGridClientFuture<IGridClientNode> futClntNode = prj.RefreshNodeAsync(randNodeId, false, false);

                X.WriteLine(">>> Refreshed node asynchronously : " + futClntNode.Result);

                // Nodes may also be refreshed by IP address.
                String clntAddr = "127.0.0.1";

                foreach (var addr in clntNode.AvailableAddresses(GridClientProtocol.Tcp))
                    if (addr != null)
                        clntAddr = addr.Address.ToString();

                // Force node metrics refresh (by default it happens periodically in the background).
                clntNode = prj.RefreshNode(clntAddr, true, true);

                X.WriteLine(">>> Refreshed node by IP : " + clntNode);

                // Asynchronous version.
                futClntNode = prj.RefreshNodeAsync(clntAddr, false, false);

                X.WriteLine(">>> Refreshed node by IP asynchronously : " + futClntNode.Result);

                // Topology as a whole may be refreshed, too.
                ICollection<IGridClientNode> top = prj.RefreshTopology(true, true);

                X.WriteLine(">>> Refreshed topology : " + ToString(top));

                // Asynchronous version.
                IGridClientFuture<IList<IGridClientNode>> topFut = prj.RefreshTopologyAsync(false, false);

                X.WriteLine(">>> Refreshed topology asynchronously : " + ToString(topFut.Result));
            }
            catch (GridClientException e) {
                Console.WriteLine("Unexpected grid client exception happens: {0}", e);
            }
            finally {
                GridClientFactory.StopAll();
            }
        }

        /**
         * <summary>
         * This method will create a client with default configuration. Note that this method expects that
         * first node will bind rest binary protocol on default port.</summary>
         *
         * <returns>Client instance.</returns>
         * <exception cref="GridClientException">If client could not be created.</exception>
         */
        private static IGridClient CreateClient() {
            var cfg = new GridClientConfiguration();

            // Point client to a local node. Note that this server is only used
            // for initial connection. After having established initial connection
            // client will make decisions which grid node to use based on collocation
            // with key affinity or load balancing.
            cfg.Servers.Add(ServerAddress + ':' + GridClientConfiguration.DefaultTcpPort);

            return GridClientFactory.Start(cfg);
        }

        /**
         * <summary>
         * Concatenates the members of a collection, using the specified separator between each member.</summary>
         *
         * <param name="list">A collection that contains the objects to concatenate.</param>
         * <param name="separator">The string to use as a separator.</param>
         * <returns>A string that consists of the members of values delimited by the separator string.</return>
         */
        public static String ToString<T>(IEnumerable<T> list, String separator = ",") {
            return list == null ? "null" : String.Join(separator, list);
        }
    }
}

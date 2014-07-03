// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Linq;
    using System.Threading;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using NUnit.Framework;
    using GridGain.Client.Ssl;
    using GridGain.Client.Query;
    using GridGain.Client.Portable;
    using GridGain.Client.Util;

    using sc = System.Collections;
    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;

    public abstract class GridClientAbstractTest {
        /** */
        protected static IList<String> DefaultTaskArgs {
            get {
                return U.List("executing", "test", "task");
            }
        }

        /** */
        protected static readonly String DefaultCacheName = "partitioned";

        /** */
        protected static readonly String InvalidCacheName = Guid.NewGuid().ToString() + "-invalid-cache-name";

        /** */
        protected static readonly String Host = "127.0.0.1";

        /** */
        protected static readonly int TcpPort = 10080;

        /** */
        protected static readonly int HttpPort = 11080;

        /** */
        protected static readonly int TcpSslPort = 10443;

        /** */
        protected static readonly int HttpSslPort = 11443;

        /** */
        protected static readonly int RouterTcpPort = 12100;

        /** */
        protected static readonly int RouterHttpPort = 12200;

        /** */
        protected static readonly int RouterTcpSslPort = 12300;

        /** */
        protected static readonly int RouterHttpSslPort = 12400;

        /** */
        private IGridClient client;

        /** */
        private IGridClient client2;

        /** <summary>Random strings generation lock.</summary> */
        private static Object rndLock = new Object();

        /**
         * <summary>
         * Whether SSL should be used in test.</summary>
         *
         * <returns>SSL context used in tests or null to disable SSL.</returns>
         */
        virtual protected IGridClientSslContext SslContext()
        {
            return null;
        }

        /** <summary>Server address "host:port" to which client should connect.</summary> */
        virtual protected String ServerAddress()
        {
            return null;
        }

        /** <summary>Router address "host:port" to which client should connect.</summary> */
        virtual protected String RouterAddress() {
            return null;
        }

        /**
         * <summary>
         * Get task name.</summary>
         *
         * <returns>Task name.</returns>
         */
        virtual protected String TaskName() {
            return "org.gridgain.client.GridClientTcpTask";
        }

        /**
         * <summary>
         * Get task argument.</summary>
         *
         * <returns>Task argument.</returns>
         */
        virtual protected Object TaskArgs(IList<String> list) {
            return list;
        }

        [TestFixtureSetUp]
        public virtual void InitClient() {
            // Bypass all certificates.
            System.Net.ServicePointManager.ServerCertificateValidationCallback = (sender, cert, chain, error) => true;

            StopClient();

            GridClientConfiguration cfg = CreateClientConfig();

            lock (this) {
                client = GridClientFactory.Start(cfg);
                client2 = GridClientFactory.Start(cfg);
            }
        }

        [TestFixtureTearDown]
        public virtual void StopClient() {
            lock (this) {
                if (client != null)
                    GridClientFactory.Stop(client.Id);

                if (client2 != null)
                    GridClientFactory.Stop(client2.Id);

                client = null;
                client2 = null;
            }

            GridClientFactory.StopAll(false);
        }

        protected String ServerNodeType() {
            IGridClientSslContext sslCtx = this.SslContext();

            return sslCtx == null ? "tcp" : "tcp+ssl";
        }

        protected GridClientConfiguration CreateClientConfig() {
            GridClientDataConfiguration nullCache = new GridClientDataConfiguration();
            GridClientDataConfiguration invalidCache = new GridClientDataConfiguration();
            GridClientDataConfiguration partitioned = new GridClientDataConfiguration();
            GridClientDataConfiguration replicated = new GridClientDataConfiguration();
            GridClientDataConfiguration withStore = new GridClientDataConfiguration();

            nullCache.Name = null;
            invalidCache.Name = InvalidCacheName;

            partitioned.Name =  "partitioned";
            partitioned.Affinity = new GridClientPartitionAffinity();

            replicated.Name = "replicated";

            withStore.Name = "replicated.store";

            GridClientPortableConfiguration portableCfg = new GridClientPortableConfiguration();

            ICollection<GridClientPortableTypeConfiguration> types = new List<GridClientPortableTypeConfiguration>();

            types.Add(new GridClientPortableTypeConfiguration(typeof(GridPortablePerson)));
            types.Add(new GridClientPortableTypeConfiguration(typeof(GridImplicitPortablePerson)));
            types.Add(new GridClientPortableTypeConfiguration(typeof(GridNoDefPortablePerson)));

            portableCfg.TypeConfigurations = types;

            GridClientConfiguration cfg = new GridClientConfiguration();

            cfg.PortableConfiguration = portableCfg;
            
            cfg.DataConfigurations.Add(nullCache);
            cfg.DataConfigurations.Add(invalidCache);
            cfg.DataConfigurations.Add(partitioned);
            cfg.DataConfigurations.Add(replicated);
            cfg.DataConfigurations.Add(withStore);

            cfg.SslContext = SslContext();

            if (RouterAddress() != null)
                cfg.Routers.Add(RouterAddress());
            else
                cfg.Servers.Add(ServerAddress());

            cfg.Credentials = "s3cret";

            cfg.ConnectTimeout = Int32.MaxValue;
            cfg.ConnectionIdleTimeout = new TimeSpan(1, 0, 0);
            cfg.TopologyRefreshFrequency = new TimeSpan(1, 0, 0);

            return cfg;
        }

        [Test]
        public void TestDataProjection() {
            /* Synchronous. */
            WithClientData((data1, data2) => {
                IList<IGridClientNode> nodes = client.Compute().Nodes();

                Assert.NotNull(nodes);
                Assert.IsTrue(nodes.Count > 0, "Unexpected nodes count: " + nodes.Count);

                IList<IGridClientData> dataPrjs = new List<IGridClientData>();

                dataPrjs.Add(data1);

                foreach (IGridClientNode node in nodes) {
                    dataPrjs.Add(data1.PinNodes(node));
                    dataPrjs.Add(data1.PinNodes(node, null));
                }

                foreach (IGridClientData data in dataPrjs)
                    data.GetItem<String, Object>(Guid.NewGuid().ToString());

                try {
                    client.Data(InvalidCacheName).GetItem<String, Object>("key");

                    Assert.Fail("Expected to fail on empty projection");
                }
                catch (GridClientServerUnreachableException e) {
                    Dbg.WriteLine("Normal behaviour for non-existent cache: " + e.Message);
                }
            });
        }

        [Test]
        public void TestPutSync() {
            /* Synchronous. */
            WithClientData((data1, data2) => {
                String key = Guid.NewGuid().ToString();
                String val = Guid.NewGuid().ToString();

                // Validate URL restrictions for 1kB limit.
                while (val.Length < 1)
                    val += "|" + val;

                // Check absence.
                Assert.IsNull(data1.GetItem<String, String>(key));
                Assert.IsNull(data2.GetItem<String, String>(key));

                Assert.IsTrue(data1.Put(key, val));

                // Check appearance.
                String cVal = data1.GetItem<String, String>(key);

                Assert.AreEqual(val, data1.GetItem<String, String>(key));
                Assert.AreEqual(val, data2.GetItem<String, String>(key));

                // Replace.
                String key2 = key + "-invalid";
                String val2 = val + "-new";

                Assert.IsNull(data1.GetItem<String, String>(key2));
                Assert.IsNull(data2.GetItem<String, String>(key2));
                Assert.IsFalse(data1.Replace<String, String>(key2, val2));

                Assert.IsTrue(data1.Replace<String, String>(key, val));
                Assert.IsTrue(data1.Replace<String, String>(key, val2));
                Assert.AreEqual(val2, data1.GetItem<String, String>(key));
                Assert.AreEqual(val2, data2.GetItem<String, String>(key));

                // Compare and set.
                Assert.IsFalse(data1.Cas(key, val2, val));  // Already changed in "Replace" section.
                Assert.IsFalse(data1.Cas(key, val2, null)); // Already exists.
                Assert.IsTrue(data1.Cas(key, val, val2));   // Change to original value.
                Assert.AreEqual(val, data1.GetItem<String, String>(key));
                Assert.AreEqual(val, data2.GetItem<String, String>(key));

                // Remove.
                Assert.IsTrue(data1.Remove(key));

                // Check absence.
                Assert.IsNull(data1.GetItem<String, String>(key));
                Assert.IsNull(data2.GetItem<String, String>(key));

                // Remove (ignored).
                Assert.IsFalse(data1.Remove(key));
                Assert.IsFalse(data2.Remove(key));
            });
        }

        [Test]
        public void TestPutAsync() {
            /* Asynchronous. */
            WithClientData((data1, data2) => {
                String key = Rnd();
                String val = Rnd();

                // Check absence.
                Assert.IsNull(data1.GetAsync<String, String>(key).Result);
                Assert.IsNull(data2.GetAsync<String, String>(key).Result);


                Assert.IsTrue(data1.PutAsync(key, val).Result);

                // Check appearance.
                Assert.AreEqual(val, data1.GetAsync<String, String>(key).Result);
                Assert.AreEqual(val, data2.GetAsync<String, String>(key).Result);

                // Replace.
                String key2 = key + "-invalid";
                String val2 = val + "-new";

                Assert.IsNull(data1.GetAsync<String, String>(key2).Result);
                Assert.IsNull(data2.GetAsync<String, String>(key2).Result);
                Assert.IsFalse(data1.ReplaceAsync<String, String>(key2, val2).Result);

                Assert.IsTrue(data1.ReplaceAsync<String, String>(key, val).Result);
                Assert.IsTrue(data1.ReplaceAsync<String, String>(key, val2).Result);
                Assert.AreEqual(val2, data1.GetAsync<String, String>(key).Result);
                Assert.AreEqual(val2, data2.GetAsync<String, String>(key).Result);

                // Compare and set.
                Assert.IsFalse(data1.CasAsync(key, val2, val).Result);  // Already changed in "Replace" section.
                Assert.IsFalse(data1.CasAsync(key, val2, null).Result); // Already exists.
                Assert.IsTrue(data1.CasAsync(key, val, val2).Result);   // Change to original value.
                Assert.AreEqual(val, data1.GetAsync<String, String>(key).Result);
                Assert.AreEqual(val, data2.GetAsync<String, String>(key).Result);

                // Remove.
                Assert.IsTrue(data1.RemoveAsync(key).Result);

                // Check absence.
                Assert.IsNull(data1.GetAsync<String, String>(key).Result);
                Assert.IsNull(data2.GetAsync<String, String>(key).Result);

                // Remove (ignored).
                Assert.IsFalse(data1.RemoveAsync(key).Result);
                Assert.IsFalse(data2.RemoveAsync(key).Result);
            });
        }

//        [Test] TODO: GG-7579
        public void TestCacheFlags() {
            /* Note! Only 'SkipStore' flag is validated. */
            IList<GridClientCacheFlag> readOnlyFlags = U.List(GridClientCacheFlag.SkipStore);

            IList<IGridClientNode> nodes = client.Compute().RefreshTopology(false, false);

            Assert.True(nodes.Count > 0);

            IGridClientData data = client.Data("replicated.store").PinNodes(nodes[0]);
            IGridClientData readData = data.CacheFlagsOn(readOnlyFlags);
            IGridClientData writeData = readData.CacheFlagsOff(readOnlyFlags);

            Assert.AreEqual(new HashSet<GridClientCacheFlag>(U.List(GridClientCacheFlag.SkipStore)), readData.CacheFlags);
            Assert.AreEqual(new HashSet<GridClientCacheFlag>(), writeData.CacheFlags);

            for (int i = 0; i < 10; i++) {
                String key = Guid.NewGuid().ToString();
                Object val = Guid.NewGuid().ToString();

                // Put entry into cache & store.
                Assert.IsTrue(writeData.Put(key, val));

                Assert.AreEqual(val, readData.GetItem<String, Object>(key));
                Assert.AreEqual(val, writeData.GetItem<String, Object>(key));

                // Remove from cache, skip store
                Assert.IsTrue(readData.Remove(key));

                Assert.IsNull(readData.GetItem<String, Object>(key));
                Assert.AreEqual(val, writeData.GetItem<String, Object>(key));
                Assert.AreEqual(val, readData.GetItem<String, Object>(key));

                // Remove from cache and from store
                Assert.IsTrue(writeData.Remove(key));

                Assert.IsNull(readData.GetItem<String, Object>(key));
                Assert.IsNull(writeData.GetItem<String, Object>(key));
            }
        }

        [Test]
        public void TestPutAllSync() {
            /* Synchronous. */
            WithClientData((data1, data2) => {
                IDictionary<String, String> all = RndMap(20);

                Assert.AreEqual(20, all.Count);

                // Check absence.
                Assert.AreEqual(0, data1.GetAll<String, String>(all.Keys).Count);
                Assert.AreEqual(0, data2.GetAll<String, String>(all.Keys).Count);

                if (!data1.PutAll(all))
                    Assert.Fail("Could not sync put all values: " + all);

                // Check appearance.
                Assert.AreEqual(all, data1.GetAll<String, String>(all.Keys));
                Assert.AreEqual(all, data2.GetAll<String, String>(all.Keys));

                // Remove.
                data1.RemoveAll(all.Keys);

                // Check absence.
                Assert.AreEqual(0, data1.GetAll<String, String>(all.Keys).Count);
                Assert.AreEqual(0, data2.GetAll<String, String>(all.Keys).Count);
            });
        }

        [Test]
        public void TestPutAllAsync() {
            /* Asynchronous. */
            WithClientData((data1, data2) => {
                IDictionary<String, String> all = RndMap(20);

                Assert.AreEqual(20, all.Count);

                // Check absence.
                Assert.AreEqual(0, data1.GetAllAsync<String, String>(all.Keys).Result.Count);
                Assert.AreEqual(0, data2.GetAllAsync<String, String>(all.Keys).Result.Count);

                if (!data1.PutAllAsync(all).Result)
                    Assert.Fail("Could not async put all values: " + all);

                // Check appearance.
                Assert.AreEqual(all, data1.GetAllAsync<String, String>(all.Keys).Result);
                Assert.AreEqual(all, data2.GetAllAsync<String, String>(all.Keys).Result);

                // Remove.
                data1.RemoveAllAsync(all.Keys).WaitDone();

                // Check absence.
                Assert.AreEqual(0, data1.GetAllAsync<String, String>(all.Keys).Result.Count);
                Assert.AreEqual(0, data2.GetAllAsync<String, String>(all.Keys).Result.Count);
            });
        }

        [Test]
        public void ATestMetricsSync() {
            TestMetrics((data, nodes) => nodes.Select(node => data.PinNodes(node).Metrics()));
        }

        [Test]
        public void TestMetricsAsync() {
            TestMetrics((data, nodes) => nodes.Select(node => data.PinNodes(node).MetricsAsync().Result));
        }

        private void TestMetrics(Func<IGridClientData, IList<IGridClientNode>, IEnumerable<IGridClientDataMetrics>> metrics) {
            /* Asynchronous. */
            WithClientData((data1, data2) => {
                int iterations = 21;
                String key = Rnd();

                IList<IGridClientNode> nodes = client.Compute().RefreshTopology(false, false);

                IEnumerable<IGridClientDataMetrics> m0 = metrics(data1, nodes);            // Initial metrics.

                for (int i = 0; i < iterations; i++) {
                    data1.Put(key, Rnd());
                    data1.GetItem<String, String>(key);
                }

                IEnumerable<IGridClientDataMetrics> m = metrics(data1, nodes);            // Metrics after calculations.

                Assert.AreEqual(m0.Count(), m.Count());

                var past = DateTime.Now - TimeSpan.FromDays(365);
                var nodesCount = client.Compute().RefreshTopology(false, false).Count;

                foreach (var node in U.List(m0, m))
                    foreach (var cur in node)
                    {
                        Assert.IsTrue(cur.CreateTime >= past);
                        Assert.IsTrue(cur.ReadTime >= past);
                        Assert.IsTrue(cur.WriteTime >= past);
                        Assert.IsTrue(cur.Hits >= 0);
                        Assert.IsTrue(cur.Reads >= 0);
                        Assert.IsTrue(cur.Writes >= 0);
                        Assert.IsTrue(cur.Misses >= 0);
                    }

                foreach (var node in U.List(m))
                    foreach (var cur in node) {
                        Assert.IsTrue(cur.ReadTime <= U.Now);
                        Assert.IsTrue(cur.ReadTime >= (U.Now - TimeSpan.FromMinutes(5)));
                        Assert.IsTrue(cur.WriteTime <= U.Now);
                        Assert.IsTrue(cur.WriteTime >= (U.Now - TimeSpan.FromMinutes(5)));
                    }

                foreach (var k in U.List<Func<IGridClientDataMetrics, long>>(dm => dm.Hits, dm => dm.Reads, dm => dm.Writes)) {
                    var value0 = 0L;
                    var value = 0L;

                    foreach (var dm in m0)
                        value0 += k(dm);
                    foreach (var dm in m)
                        value += k(dm);

                    Assert.IsTrue(0 <= value0);
                    Assert.IsTrue(value >= value0, "value={0}, value0={1}", value, value0);
                }
            });
        }

        [Test]
        public void TestLogSync() {
            TestLog((comp, fromLine, toLine) => comp.Log(fromLine, toLine));
            TestLog((comp, fromLine, toLine) => comp.Log("work/log/gridgain.log", fromLine, toLine));
        }

        [Test]
        public void TestLogAsync() {
            TestLog((comp, fromLine, toLine) => comp.LogAsync(fromLine, toLine).Result);
            TestLog((comp, fromLine, toLine) => comp.LogAsync("work/log/gridgain.log", fromLine, toLine).Result);
        }

        private void TestLog(Func<IGridClientCompute, int, int, IList<String>> logs) {
            WithClientCompute((comp1, comp2) => {
                var list = logs(comp1, 0, 9/* inclusive */);

                Assert.NotNull(list, "Expects log is available.");
                Assert.IsTrue(list.Count >= 0, "{0} >= 0", list.Count);
                Assert.IsTrue(list.Count <= 10, "{0} <= 10", list.Count);
            });
        }

        [Test]
        public void TestNodeMetricsSync() {
            TestNodeMetrics((comp, id) => comp.RefreshNode(id, true, true).Metrics);
        }

        [Test]
        public void TestNodeMetricsAsync() {
            TestNodeMetrics((comp, id) => comp.RefreshNodeAsync(id, true, true).Result.Metrics);
        }

        private void TestNodeMetrics(Func<IGridClientCompute, Guid, IGridClientNodeMetrics> nodeMetrics) {
            TimeSpan zero = new TimeSpan(0);
            TimeSpan delta = TimeSpan.FromMinutes(30);

            WithClientCompute((comp1, comp2) => {
                var nodes1 = comp1.Nodes();
                var nodes2 = comp2.Nodes();

                Assert.AreEqual(nodes1.Count, nodes2.Count);

                ISet<Guid> ids = new HashSet<Guid>();

                foreach (var node in nodes1)
                    ids.Add(node.Id);

                foreach (var node in nodes2) {
                    Assert.IsTrue(ids.Remove(node.Id));

                    var m1 = nodeMetrics(comp1, node.Id);
                    var m2 = nodeMetrics(comp2, node.Id);

                    Assert.AreEqual(m1.StartTime, m2.StartTime);
                    Assert.AreEqual(m1.NodeStartTime, m2.NodeStartTime);

                    Assert.IsTrue(m1.UpTime > zero);
                    Assert.IsTrue(m1.StartTime + m1.UpTime <= U.Now + delta);
                    Assert.IsTrue(m1.StartTime + m1.UpTime > U.Now - delta);
                    Assert.IsTrue(m1.BusyTimeTotal <= m1.UpTime);
                    Assert.IsTrue(m1.BusyTimeTotal >= zero);
                    Assert.IsTrue(m1.IdleTimeTotal <= m1.UpTime);
                    Assert.IsTrue(m1.IdleTimeTotal >= zero);

                    Assert.IsTrue(m1.BusyTimePercentage >= 0);
                    Assert.IsTrue(m1.BusyTimePercentage <= 1);
                    Assert.IsTrue(m1.IdleTimePercentage >= 0);
                    Assert.IsTrue(m1.IdleTimePercentage <= 1);

                    Assert.IsTrue(m1.CpuCount >= 1);

                    ValidateCounter(m1.WaitingJobs, "waiting jobs");
                    ValidateCounter(m1.ExecutedJobs, "executed jobs");
                    ValidateCounter(m1.RejectedJobs, "rejected jobs");
                    ValidateCounter(m1.CancelledJobs, "canceled jobs");
                }

                Assert.AreEqual(ids.Count, 0);
            });
        }

        private static void ValidateCounter(GridClientNodeMetricsCounter<long, double> c, String name) {
            name += ": ";

            Assert.IsTrue(c.Current >= 0, name + "Current >= 0");
            Assert.IsTrue(c.Maximum >= 0, name + "Maximum >= 0");
            Assert.IsTrue(c.Total >= 0, name + "Total >= 0");
            Assert.IsTrue(c.Average >= 0, name + "Average >= 0");
            Assert.IsTrue(c.Current <= c.Maximum, name + "Current <= Maximum");
            Assert.IsTrue(c.Average <= c.Maximum, name + "Average <= Maximum");
            Assert.IsTrue(c.Maximum <= c.Total, name + "Maximum <= Total");
        }

        [Test]
        public void TestAppendPrepend() {
            IList<IGridClientData> datas = U.List(client.Data("replicated"), client.Data("partitioned"));

            String key = Guid.NewGuid().ToString();

            foreach (IGridClientData data in datas) {
                Assert.IsNotNull(data);

                data.Remove(key);

                Assert.IsFalse(data.Append(key, ".suffix"));
                Assert.IsTrue(data.Put(key, "val"));
                Assert.IsTrue(data.Append(key, ".suffix"));
                Assert.AreEqual("val.suffix", data.GetItem<String, String>(key));
                Assert.IsTrue(data.Remove(key));
                Assert.IsFalse(data.Append(key, ".suffix"));

                data.Remove(key);

                Assert.IsFalse(data.Prepend(key, "postfix."));
                Assert.IsTrue(data.Put(key, "val"));
                Assert.IsTrue(data.Prepend(key, "postfix."));
                Assert.AreEqual("postfix.val", data.GetItem<String, String>(key));
                Assert.IsTrue(data.Remove(key));
                Assert.IsFalse(data.Prepend(key, "postfix."));
            }

            IList<String> origList = U.List<String>("1", "2");
            IList<String> newList = U.List<String>("3", "4");

            sc::IList appendList = new sc::ArrayList();

            appendList.Add("1");
            appendList.Add("2");
            appendList.Add("3");
            appendList.Add("4");

            sc::IList prependList = new sc::ArrayList();

            prependList.Add("3");
            prependList.Add("4");
            prependList.Add("1");
            prependList.Add("2");

            IDictionary<String, String> origMap = new Dictionary<String, String>();

            origMap.Add("1", "a1");
            origMap.Add("2", "a2");

            IDictionary<String, String> newMap = new Dictionary<String, String>();

            newMap.Add("2", "b2");
            newMap.Add("3", "b3");

            sc::IDictionary appendMap = new Dictionary<String, String>();

            appendMap.Add("1", "a1");
            appendMap.Add("2", "b2");
            appendMap.Add("3", "b3");

            sc::IDictionary prependMap = new Dictionary<String, String>();

            prependMap.Add("1", "a1");
            prependMap.Add("2", "a2");
            prependMap.Add("3", "b3");

            foreach (IGridClientData data in datas) {
                Assert.IsNotNull(data);

                data.Remove(key);

                Assert.IsFalse(data.Append(key, newList));
                Assert.IsTrue(data.Put(key, origList));
                Assert.IsTrue(data.Append(key, newList));

                Assert.AreEqual(appendList, data.GetItem<String, sc::IList>(key));

                data.Remove(key);

                Assert.IsFalse(data.Prepend(key, newList));
                Assert.IsTrue(data.Put(key, origList));
                Assert.IsTrue(data.Prepend(key, newList));
                Assert.AreEqual(prependList, data.GetItem<String, sc::IList>(key));

                data.Remove(key);

                Assert.IsFalse(data.Append(key, newMap));
                Assert.IsTrue(data.Put(key, origMap));
                Assert.IsTrue(data.Append(key, newMap));

                Assert.AreEqual(appendMap, data.GetItem<String, sc::IDictionary>(key));

                data.Remove(key);

                Assert.IsFalse(data.Prepend(key, newMap));
                Assert.IsTrue(data.Put(key, origMap));
                Assert.IsTrue(data.Prepend(key, newMap));
                Assert.AreEqual(prependMap, data.GetItem<String, sc::IDictionary>(key));
            }
        }

        [Test]
        public void TestExecuteSync() {
            IGridClientCompute compute = client.Compute();

            TestExecute((taskName, arg) => compute.Execute<int>(taskName, arg));
        }

        [Test]
        public void TestExecuteAsync() {
            IGridClientCompute compute = client.Compute();

            TestExecute((taskName, arg) => compute.ExecuteAsync<int>(taskName, arg).Result);
        }

        private void TestExecute(Func<String, Object, int> execute) {
            String NULL = null;

            // Invalid task argument: cannot split task.
            Assert.Throws<GridClientException>(() => execute(TaskName(), TaskArgs(null)));
            Assert.Throws<GridClientException>(() => execute(TaskName(), TaskArgs(U.List<String>())));

            // Validate task executions.
            Assert.AreEqual(0, execute(TaskName(), TaskArgs(U.List(NULL))));
            Assert.AreEqual(0, execute(TaskName(), TaskArgs(U.List(NULL, NULL))));
            Assert.AreEqual(17, execute(TaskName(), TaskArgs(U.List("executing", "test", "task"))));
            Assert.AreEqual(20, execute(TaskName(), TaskArgs(U.List("executing", NULL, "test", "task", "201"))));
        }

        /**
         * Check async API methods don't generate exceptions.
         *
         * @throws Exception If failed.
         */
        [Test]
        public void TestNoAsyncExceptions() {
            IGridClient client = GridClientFactory.Start(CreateClientConfig());

            IGridClientData data = client.Data(DefaultCacheName);
            IGridClientCompute compute = client.Compute().Projection((IGridClientNode node) => false);

            IDictionary<String, IGridClientFuture> futs = new Dictionary<String, IGridClientFuture>();

            futs["exec"] = compute.ExecuteAsync<Object>("taskName", "taskArg");
            futs["affExec"] = compute.AffinityExecuteAsync<Object>("taskName", "cacheName", "affKey", "taskArg");
            futs["refreshById"] = compute.RefreshNodeAsync(Guid.NewGuid(), true, true);
            futs["refreshByIP"] = compute.RefreshNodeAsync("nodeIP", true, true);
            futs["refreshTop"] = compute.RefreshTopologyAsync(true, true);
            futs["log"] = compute.LogAsync(-1, -1);
            futs["logForPath"] = compute.LogAsync("path/to/log", -1, -1);

            GridClientFactory.Stop(client.Id, false);

            var all = new Dictionary<String, String>();

            all.Add("key", "val");

            futs["put"] = data.PutAsync("key", "val");
            futs["putAll"] = data.PutAllAsync(all);
            futs["get"] = data.GetAsync<String, String>("key");
            futs["getAll"] = data.GetAllAsync<String, String>(U.List("key"));
            futs["remove"] = data.RemoveAsync<String>("key");
            futs["removeAll"] = data.RemoveAllAsync<String>(U.List("key"));
            futs["replace"] = data.ReplaceAsync<String, String>("key", "val");
            futs["cas"] = data.CasAsync("key", "val", "val2");
            futs["metrics"] = data.MetricsAsync();

            foreach (KeyValuePair<String, IGridClientFuture> e in futs) {
                try {
                    e.Value.WaitDone();

                    Assert.Fail("Expects '" + e.Key + "' fails with grid client exception.");
                }
                catch (GridClientServerUnreachableException) {
                    // No op: compute projection is empty.
                }
                catch (GridClientClosedException) {
                    // No op: data projection in closed client.
                }
                catch (GridClientException x) {
                    Assert.Fail("Unexpected client exception: " + x);
                }
            }
        }

        [Test]
        public void TestGracefulShutdown() {
            TestShutdown(true, failed => Assert.AreEqual(0, failed));
        }

        [Test]
        public void TestHaltShutdown() {
            TestShutdown(false, failed => Assert.IsTrue(failed >= 0));
        }

        private void TestShutdown(bool waiting, Action<int> doFailed) {
            IGridClient c = GridClientFactory.Start(CreateClientConfig());

            IGridClientCompute compute = c.Compute();

            var futs = new List<IGridClientFuture<int>>();

            // Validate connection works.
            compute.Execute<int>(TaskName(), TaskArgs(DefaultTaskArgs));

            for (int i = 0; i < 100; i++)
                futs.Add(compute.ExecuteAsync<int>(TaskName(), TaskArgs(DefaultTaskArgs)));

            /* Stop client. */
            GridClientFactory.Stop(c.Id, waiting);

            int failed = 0;

            foreach (var fut in futs)
                try {
                    Assert.AreEqual(17, fut.Result);
                }
                catch (GridClientException e) {
                    failed++;

                    Dbg.WriteLine(e.Message);
                }

            doFailed(failed);
        }

        [Test]
        public void TestOpenClose() {
            int count = 50;
            int failed = 0;
            var execs = new List<IGridClientFuture>();
            var closes = new List<IGridClientFuture>();

            for (int i = 0; i < count; i++) {
                IGridClient c;

                try {
                    c = GridClientFactory.Start(CreateClientConfig());
                }
                catch (Exception e) {
                    Dbg.WriteLine("Create client #" + i + " failed: " + e);

                    failed++;

                    continue;
                }

                execs.Add(c.Compute().ExecuteAsync<int>(TaskName(), TaskArgs(DefaultTaskArgs)));
                closes.Add(U.Async(() => GridClientFactory.Stop(c.Id, true)));
            }

            Assert.AreEqual(execs.Count, closes.Count);

            for (int i = 0; i < execs.Count; i++) {
                try {
                    execs[i].WaitDone();
                    closes[i].WaitDone();
                }
                catch (Exception e) {
                    Dbg.WriteLine("Stop client #" + i + " failed: " + e);

                    continue;
                }
            }

            Assert.AreEqual(0, failed);
        }

        /**
         * <summary>Tests SQL query.</summary>
         */
        [Test]
        public void TestSqlQueryPortable() {
            CheckSqlQuery<GridPortablePerson>(
                "GridPortablePerson",
                (String name, int age) => new GridPortablePerson(name, age),
                (GridPortablePerson person) => person.age
            );
        }

        /**
         * <summary>Tests SQL query.</summary>
         */
        [Test]
        public void TestSqlQueryImplicitPortable() {
            CheckSqlQuery<GridImplicitPortablePerson>(
                "GridPortablePerson",
                (String name, int age) => new GridImplicitPortablePerson(name, age),
                (GridImplicitPortablePerson person) => person.age
            );
        }

        /**
         * <summary>Tests SQL query.</summary>
         */
        [Test]
        public void TestSqlQueryNoDefPortable() {
            CheckSqlQuery<GridNoDefPortablePerson>(
                "GridPortablePerson",
                (String name, int age) => new GridNoDefPortablePerson(name, age),
                (GridNoDefPortablePerson person) => person.age
            );
        }

        /**
         * <summary>Checks SQL query for some data  type.</summary>
         * <param name="type">Type name.</param>
         * <param name="factory">Type factory.</param>
         * <param name="age">Age getter.</param>
         */
        private void CheckSqlQuery<T>(String type, Func<String, int, T> factory, Func<T, int> age) {
            IGridClientData data = client.Data("partitioned");

            Assert.IsNotNull(data);

            for (int i = 0; i < 100; i++) {
                int key = i;

                T person = factory("Person" + i, i * 10);

                data.Put(key, person);
            }

            IGridClientDataQueries qrys = data.Queries();

            IGridClientDataQuery<DictionaryEntry?> qry = qrys.createSqlQuery(type, "age >= ?");

            qry.IncludeBackups = false;
            qry.EnableDedup = true;
            qry.KeepAll = true;

            IGridClientDataQueryFuture<DictionaryEntry?> qryFut = qry.Execute(500);

            ICollection<DictionaryEntry?> res = qryFut.Result;

            Assert.AreEqual(50, res.Count);

            foreach (DictionaryEntry entry in res) {
                T person = (T)entry.Value;

                Assert.GreaterOrEqual(age(person), 500);
            }

            qry = qrys.createSqlQuery(type, "age >= ?");

            qry.IncludeBackups = false;
            qry.EnableDedup = true;
            qry.KeepAll = true;
            // Force multiple requests.
            qry.PageSize = 10;

            qryFut = qry.Execute(500);

            int cnt = 0;

            do {
                DictionaryEntry? entry = qryFut.next();

                if (!entry.HasValue)
                    break;

                T person = (T)entry.Value.Value;

                Assert.GreaterOrEqual(age(person), 500);

                cnt++;
            }
            while (true);

            Assert.AreEqual(50, cnt);
        }

        /**
         * <summary>Tests SQL query.</summary>
         */
        [Test]
        public void TestSqlFieldsQueryPortable() {
            CheckSqlFieldsQuery<GridPortablePerson>(
                "GridPortablePerson",
                (String name, int age) => new GridPortablePerson(name, age)
            );
        }

        /**
         * <summary>Tests SQL query.</summary>
         */
        [Test]
        public void TestSqlFieldsQueryImplicitPortable() {
            CheckSqlFieldsQuery<GridImplicitPortablePerson>(
                "GridPortablePerson",
                (String name, int age) => new GridImplicitPortablePerson(name, age)
            );
        }

        /**
         * <summary>Tests SQL query.</summary>
         */
        [Test]
        public void TestSqlFieldsQueryNoDefPortable() {
            CheckSqlFieldsQuery<GridNoDefPortablePerson>(
                "GridPortablePerson",
                (String name, int age) => new GridNoDefPortablePerson(name, age)
            );
        }
        
        /**
         * <summary>Checks SQL fields query for some data  type.</summary>
         * <param name="type">Type name.</param>
         * <param name="factory">Type factory.</param>
         */
        private void CheckSqlFieldsQuery<T>(String type, Func<String, int, T> factory) {
            IGridClientData data = client.Data("partitioned");

            Assert.IsNotNull(data);

            for (int i = 0; i < 100; i++) {
                int key = i;

                T person = factory("Person" + i, i * 10);

                data.Put(key, person);
            }

            IGridClientDataQueries qrys = data.Queries();

            IGridClientDataQuery<IList> qry = qrys.createSqlFieldsQuery("select age, name from " + type + " where age >= ?");

            qry.IncludeBackups = false;
            qry.EnableDedup = true;
            qry.KeepAll = true;

            IGridClientDataQueryFuture<IList> qryFut = qry.Execute(500);

            ICollection<IList> res = qryFut.Result;

            Assert.AreEqual(50, res.Count);

            foreach (IList row in res) {
                Assert.AreEqual(2, row.Count);
                Assert.GreaterOrEqual((int)row[0], 500);
                Assert.NotNull(row[1]);
            }

            qry = qrys.createSqlFieldsQuery("select age, name from GridPortablePerson where age >= ?");

            qry.IncludeBackups = false;
            qry.EnableDedup = true;
            qry.KeepAll = true;
            // Force multiple requests.
            qry.PageSize = 10;

            qryFut = qry.Execute(500);

            int cnt = 0;

            do {
                IList row = qryFut.next();

                if (row == null)
                    break;

                Assert.AreEqual(2, row.Count);
                Assert.GreaterOrEqual((int)row[0], 500);
                Assert.NotNull(row[1]);

                cnt++;
            }
            while (true);

            Assert.AreEqual(50, cnt);
        }

        [Test]
        virtual public void TestMultithreadedAccessToOneClient() {
            // Use testcase-initialized clients from several threads.

            var threads = new ConcurrentBag<Thread>();
            Exception failure = null;

            Action<Exception> haltAll = delegate(Exception e) {
                lock (threads) {
                    int count = 0;

                    Thread thread;

                    while (threads.TryTake(out thread)) {
                        if (thread.Equals(Thread.CurrentThread))
                            failure = e;
                        else
                            thread.Abort();

                        count++;
                    }

                    if (count > 0)
                        Console.Out.WriteLine("\n\n\nFailed (stop {0}): {1}\n\n\n", count, e);
                }
            };

            IDictionary<String, Action> actions = new Dictionary<String, Action>();

            actions.Add("testExecuteAsync", () => TestExecuteAsync());
            actions.Add("testPutAsync", () => TestPutAsync());
            actions.Add("testPutAllAsync", () => TestPutAllAsync());
            actions.Add("testMetricsAsync", () => TestMetricsAsync());
            //actions.Add("testOpenClose", () => testOpenClose()); // Leads to out of memory (normal behaviour - too much threads).

            for (int i = 0; i < 4; i++)
                foreach (var action in actions) {
                    var job = action; // Use local variable in closure instead of iterator.
                    var thread = new Thread(() => U.DoSilent<Exception>(job.Value, haltAll));
                    thread.Name = job.Key + "-" + i;

                    threads.Add(thread);
                }

            foreach (var thread in threads)
                try {
                    thread.Start();
                }
                catch (Exception e) {
                    Dbg.WriteLine("Failed to start thread [thread={0}, e={1}]", thread.Name, e.Message);
                }

            foreach (var thread in threads)
                try {
                    if (thread.IsAlive)
                        thread.Join();
                }
                catch (Exception e) {
                    Dbg.WriteLine("Failed to join thread [thread={0}, e={1}]", thread.Name, e.Message);
                }

            lock (threads) {
                Assert.AreEqual(null, failure);
            }
        }

        [Test]
        public virtual void TestAffinity() {
            String cacheName = "partitioned";

            IList<IGridClientNode> initialTop = client.Compute().RefreshTopology(false, false);

            Assert.True(initialTop.Count > 0);

            IGridClientData data =  client.Data(cacheName);

            // Save original node as pinned projection, to protect from connection failures when executing service requests.
            IGridClientCompute comp = client.Compute().Projection(initialTop[0]);
            IGridClientCompute comp2 = client2.Compute().Projection(initialTop[0]);

            IList<String> keys = new List<String>();

            for (int i = 0; i < 10; i++)
                keys.Add("step-" + i + "-" + Guid.NewGuid());

            bool ok = true;
            int cnt = comp.RefreshTopology(false, false).Count;

            Assert.AreEqual(2, cnt);

            for (uint i = 0; i < 2; i++) {
                IncreaseNodes(1, comp);

                IList<IGridClientNode> nodes = comp.RefreshTopology(false, false);

                Assert.AreEqual(cnt + i + 1, nodes.Count);

                foreach (String key in keys) {
                    String res = comp.Execute<String>("org.gridgain.client.GridClientGetAffinityTask", cacheName + ":" + key);

                    Guid nodeId1 = Guid.Parse(res);
                    Guid nodeId2 = data.Affinity(key);

                    if (!nodeId1.Equals(nodeId2)) {
                        Dbg.WriteLine("Invalid affinity [key={0}, srvNode={1}, clientNode={2}]", key, nodeId1, nodeId2);

                        ok = false;
                    }
                }
            }

            DecreaseNodes(comp);

            comp.RefreshTopology(false, false);
            comp2.RefreshTopology(false, false);

            if (ok)
                return;

            Assert.Fail("Affinity test failed.");
        }

        /**
         * <summary>
         * Increase nodes count in topology.</summary>
         *
         * <param name="count">Number of nodes to add to topology.</param>
         * <param name="prj">Compute projection to use for new node start.</param>
         */
        protected void IncreaseNodes(int count, IGridClientCompute prj) {
            String taskName = "org.gridgain.client.GridClientStartNodeTask";

            Dbg.WriteLine("Increasing node count by " + count);

            while (count-- > 0)
                prj.Execute<Object>(taskName, ServerNodeType());
        }

        /**
         * <summary>
         * Decrease nodes count in topology.</summary>
         */
        protected void DecreaseNodes(IGridClientCompute prj) {
            String taskName = "org.gridgain.client.GridClientStopNodeTask";

            Dbg.WriteLine("Decreasing node count.");

            const int maxRetries = 20;

            for (int i = 0; i < maxRetries; i++) {
                try {
                    Object stoppedCnt = prj.Execute<Object>(taskName, ServerNodeType());

                    Dbg.WriteLine("Returned stopped count: " + stoppedCnt);

                    Thread.Sleep(5000);

                    return;
                }
                catch (GridClientException e) {
                    Dbg.WriteLine("Ignored exception: " + e.Message);
                }
            }

            Assert.Fail("Failed to stop nodes after " + maxRetries + " tries.");
        }

        /**
         * <summary>
         * Execute closure with initialized clients data in different environments.</summary>
         *
         * <param name="test">Closure to execute with initialized clients data.</param>
         */
        private void WithClientData(Action<IGridClientData, IGridClientData> test) {
            String[] cacheNames = new String[] { "replicated", "partitioned" };

            // Process closure several times.
            for (int i = 0; i < 10; i++)
                foreach (var cacheName in cacheNames) {
                    IGridClientData data = client.Data(cacheName);
                    IGridClientData data2 = client2.Data(cacheName);

                    Assert.IsNotNull(data);
                    Assert.IsNotNull(data2);

                    test(data, data2);
                }
        }

        /**
         * <summary>
         * Execute closure with initialized clients compute in different environments.</summary>
         *
         * <param name="test">Closure to execute with initialized clients compute.</param>
         */
        private void WithClientCompute(Action<IGridClientCompute, IGridClientCompute> test) {
            // Process closure several times.
            for (int i = 0; i < 10; i++) {
                IGridClientCompute compute = client.Compute();
                IGridClientCompute compute2 = client2.Compute();

                Assert.IsNotNull(compute);
                Assert.IsNotNull(compute2);

                test(compute, compute2);
            }
        }

        /**
         * <summary>
         * Creates random-generated string.</summary>
         *
         * <returns>Random-generated string.</returns>
         */
        private static String Rnd() {
            lock (rndLock) {
                return Guid.NewGuid().ToString() + Guid.NewGuid().ToString();
            }
        }

        /**
         * <summary>
         * Create map with random-generated keys and values.</summary>
         *
         * <param name="size">Size of the map.</param>
         * <returns>New map with random-generated key-value pairs.</returns>
         */
        private static IDictionary<String, String> RndMap(int size) {
            var map = new Dictionary<String, String>();

            while (map.Count < size)
                map[Rnd()] = Rnd();

            return map;
        }
    }
}

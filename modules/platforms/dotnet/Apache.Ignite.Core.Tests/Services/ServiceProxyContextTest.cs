namespace Apache.Ignite.Core.Tests.Services
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Resource;
    using Apache.Ignite.Core.Services;
    using NUnit.Framework;
    using Apache.Ignite.Platform.Model;

    public class ServiceProxyContextTest
    {
        /** */
        private const string SvcName = "Service1";

        /** */
        private const string CacheName = "cache1";

        /** */
        private const int AffKey = 25;

        /** */
        protected IIgnite Grid1;

//        /** */
//        protected IIgnite Grid2;
//
//        /** */
//        protected IIgnite Grid3;
//
//        /** */
//        private IIgnite _client;
//
//        /** */
//        protected IIgnite[] Grids;

        [TearDown]
        public void FixtureTearDown()
        {
            StopGrids();
        }

        /// <summary>
        /// Executes before each test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            StartGrids();
        }
        /// <summary>
        /// Starts the grids.
        /// </summary>
        private void StartGrids()
        {
//            if (Grid1 != null)
//                return;

//            var path = Path.Combine("Config", "Compute", "compute-grid");

            Grid1 = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration(false, "grid1")));
//            {
////                BinaryConfiguration = new BinaryConfiguration(typeof(Data)),
//                AutoGenerateIgniteInstanceName = true,
////                DataStorageConfiguration = new DataStorageConfiguration
////                {
////                    DefaultDataRegionConfiguration = new DataRegionConfiguration
////                    {
////                        PersistenceEnabled = _enableSecurity,
////                        Name = DataStorageConfiguration.DefaultDataRegionName,
////                    }
////                },
////                AuthenticationEnabled = _enableSecurity,
////                Logger = _logger,
////                IsActiveOnStart = false
//            }

//            Grid2 = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration(false, "grid2")));
//
//            Grid3 = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration(false, "grid3")));
//
//            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration(false, "client"));
//
//            cfg.ClientMode = true;
//            cfg.IgniteInstanceName = "client";
//
//            _client = Ignition.Start(cfg);
//
//            Grids = new[] { Grid1, Grid2, Grid3, _client };
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
//            Grid1 = Grid2 = Grid3 = null;
//            Grids = null;

            Ignition.StopAll(true);
        }
        
        
        /// <summary>
        /// Tests deployment.
        /// </summary>
        [Test]
        public void TestProxyContext() // [Values(true, false)] bool binarizable
        {
//            var cfg = new ServiceConfiguration
//            {
//                Name = SvcName,
//                MaxPerNodeCount = 3,
//                TotalCount = 3,
//                NodeFilter = new NodeFilter {NodeId = Grid1.GetCluster().GetLocalNode().Id},
//                Service = binarizable ? new TestIgniteServiceBinarizable() : new TestIgniteServiceSerializable()
//            };

            Console.Out.WriteLine(">xxx> MyService");

            var svc = new MyService();
            
//            svc.GetType().GetFields(BindingFlags.Public | 
//                                         BindingFlags.NonPublic | 
//                                         BindingFlags.Instance);

            var filedInfo = svc.GetType().GetField("headers");

            

//            ThreadLocal<Dictionary<object, object>> headers = (ThreadLocal<Dictionary<object, object>>)filedInfo.GetValue(svc);
            
                        
            Dictionary<string, object> dict = new Dictionary<string, object>();
            
            dict.Add("id", "12345");
            dict.Add("id2", "2131231");
            dict.Add("id3", "Adasdas");

//            headers.Value = dict;
            
            
//            Assert.AreEqual("12345", svc.headers.Value["id"]);
            

            Services.DeployClusterSingleton(SvcName, svc);

            Console.WriteLine(">xxx> get proxy");

            var svcProxy = Services.GetServiceProxy<IMyService>(SvcName, false, dict);
            
            Console.WriteLine(">xxx> invoke method");

            var res = svcProxy.Method("id");
            
            Console.WriteLine(">xxx> all done");

            Thread.Sleep(1_000);
            
            Assert.AreEqual("12345", res);

//            CheckServiceStarted(Grid1, 3);
        }

        public interface IMyService : IService
        {
            object Method(string arg);
        }
        
        [Serializable]
        public class MyService : IMyService
        {
//            public ThreadLocal<Dictionary<object, object>> headers = new ThreadLocal<Dictionary<object, object>>();

            public object Method(string arg)
            {
                return context.Attr(arg);
//                object value;
//
//                var hdrs = context.headers();
//
//                if (hdrs != null && hdrs.TryGetValue("id", out value))
//                    return value;

//                try
//                {
//                    if (context.headers().TryGetValue("id", out value))
//                        return value;
//                }
//                catch (NullReferenceException e)
//                {
//                    Console.WriteLine("NPE: " + e.Message);
//                }
//                return null;
            }

            private IServiceContext context;

            public void Init(IServiceContext context)
            {
//                throw new NotImplementedException();
                Console.Out.WriteLine(">xxx> init");
                
                if (this.context != null)
                    Assert.AreSame(this.context,context);

                this.context = context;
            }

            public void Execute(IServiceContext context)
            {
                Console.Out.WriteLine(">xxx> Execute");

                if (this.context != null)
                    Assert.AreSame(this.context,context);
                
                this.context = context;
                
                
            }

            public void Cancel(IServiceContext context)
            {
                Console.Out.WriteLine(">xxx> cancel");
//                if (this.context != null)
//                    Assert.AreEqual(this.context,context);
//                
//                this.context = context;
            }
        }
        
        /// <summary>
        /// Gets the services.
        /// </summary>
        protected virtual IServices Services
        {
            get { return Grid1.GetServices(); }
        }
    }
}
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
namespace Apache.Ignite.Core.Tests.Plugin
{
    using System;
    using Apache.Ignite.Core.Plugin;
    using NUnit.Framework;

    /// <summary>
    /// Ignite plugin test.
    /// </summary>
    public class PluginTest
    {
        private const string PluginName = "TestPlugin1";

        /// <summary>
        /// Tests the start stop.
        /// </summary>
        [Test]
        public void TestStartStop()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration());

            using (var ignite = Ignition.Start(cfg))
            {
                Assert.Throws<PluginNotFoundException>(() => ignite.GetPlugin<object>("foobar"));
                Assert.Throws<Exception>(() => ignite.GetPlugin<string>(PluginName));

                var plugin = ignite.GetPlugin<TestIgnitePlugin>(PluginName);
                Assert.IsNotNull(plugin);

                var prov = plugin.Provider;
                Assert.IsTrue(prov.Started);
                Assert.AreEqual(PluginName, prov.Name);
                Assert.IsNotNull(prov.Context);

                var ctx = prov.Context;
                Assert.IsNotNull(ctx.Ignite);
                Assert.AreEqual(cfg, ctx.IgniteConfiguration);
            }
        }

        public class TestIgnitePlugin
        {
            public TestIgnitePluginProvider Provider { get; private set; }

            public TestIgnitePlugin(TestIgnitePluginProvider provider)
            {
                Provider = provider;
            }
        }

        public class TestIgnitePluginProvider : IPluginProvider
        {
            private readonly TestIgnitePlugin _plugin;


            public TestIgnitePluginProvider()
            {
                _plugin = new TestIgnitePlugin(this);
            }

            public string Name
            {
                get { return PluginName; }
            }

            public string Copyright
            {
                get { return "Copyright (c) FooBar."; }
            }

            public T GetPlugin<T>() where T : class
            {
                var p = _plugin as T;

                if (p != null)
                    return p;

                throw new Exception("Invalid plugin type: " + typeof(T));
            }

            public void Start(IPluginContext context)
            {
                Context = context;
            }

            public void Stop(bool cancel)
            {
                Stopped = cancel;
            }

            public void OnIgniteStart()
            {
                Started = true;

                Assert.NotNull(Context);
                Assert.NotNull(Context.Ignite);
                Assert.NotNull(Context.IgniteConfiguration);
            }

            public bool Started { get; set; }

            public bool? Stopped { get; set; }

            public IPluginContext Context { get; private set; }
        }
    }
}

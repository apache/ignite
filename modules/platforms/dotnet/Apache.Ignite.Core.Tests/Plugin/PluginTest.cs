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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Interop;
    using Apache.Ignite.Core.Plugin;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Ignite plugin test.
    /// </summary>
    public class PluginTest
    {
        /** Plugin log. */
        private static readonly List<string> PluginLog = new List<string>();

        /// <summary>
        /// Tests the plugin life cycle.
        /// </summary>
        [Test]
        public void TestIgniteStartStop()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                PluginConfigurations = new[] {new TestIgnitePluginConfiguration {PluginProperty = "barbaz"}}
            };

            TestIgnitePlugin plugin;

            using (var ignite = Ignition.Start(cfg))
            {
                Assert.Throws<PluginNotFoundException>(() => ignite.GetPlugin<object>("foobar"));
                Assert.Throws<Exception>(() => ignite.GetPlugin<string>(TestIgnitePluginProvider.PluginName));

                plugin = ignite.GetPlugin<TestIgnitePlugin>(TestIgnitePluginProvider.PluginName);
                Assert.IsNotNull(plugin);

                var prov = plugin.Provider;
                Assert.IsTrue(prov.Started);
                Assert.AreEqual(null, prov.Stopped);
                Assert.AreEqual(TestIgnitePluginProvider.PluginName, prov.Name);
                Assert.IsNotNullOrEmpty(prov.Copyright);
                Assert.IsNotNull(prov.Context);

                var ctx = prov.Context;
                Assert.IsNotNull(ctx.Ignite);
                Assert.AreEqual("barbaz", ctx.PluginConfiguration.PluginProperty);
                CheckResourceInjection(ctx);

                var plugin2 = ignite.GetPlugin<TestIgnitePlugin>(TestIgnitePluginProvider.PluginName);
                Assert.AreEqual(plugin, plugin2);

                var extension = plugin.Provider.Context.GetExtension(0);
                Assert.IsNotNull(extension);

                CheckPluginTarget(extension, "barbaz", plugin.Provider);
            }

            Assert.AreEqual(true, plugin.Provider.Stopped);
            Assert.AreEqual(true, plugin.Provider.IgniteStopped);
        }

        /// <summary>
        /// Checks the resource injection.
        /// </summary>
        private static void CheckResourceInjection(IPluginContext<TestIgnitePluginConfiguration> ctx)
        {
            var obj = new Injectee();

            Assert.IsNull(obj.Ignite);

            ctx.InjectResources(obj);

            Assert.IsNotNull(obj.Ignite);
            Assert.AreEqual(ctx.Ignite.Name, obj.Ignite.Name);
        }

        /// <summary>
        /// Checks the plugin target operations.
        /// </summary>
        private static void CheckPluginTarget(IPlatformTarget target, string expectedName,
            TestIgnitePluginProvider provider)
        {
            // Returns name.
            Assert.AreEqual(expectedName, target.OutStream(1, r => r.ReadString()));

            // Increments arg by one.
            Assert.AreEqual(3, target.InLongOutLong(1, 2));
            Assert.AreEqual(5, target.InLongOutLong(1, 4));

            // Returns string length.
            Assert.AreEqual(3, target.InStreamOutLong(1, w => w.WriteString("foo")));
            Assert.AreEqual(6, target.InStreamOutLong(1, w => w.WriteString("foobar")));

            // Returns uppercase string.
            Assert.AreEqual("FOO", target.InStreamOutStream(1, w => w.WriteString("foo"), r => r.ReadString()));
            Assert.AreEqual("BAR", target.InStreamOutStream(1, w => w.WriteString("bar"), r => r.ReadString()));

            // Returns target with specified name.
            var newTarget = target.InStreamOutObject(1, w => w.WriteString("name1"));
            Assert.AreEqual("name1", newTarget.OutStream(1, r => r.ReadString()));

            // Invokes callback to modify name, returns target with specified name appended.
            var res = target.InObjectStreamOutObjectStream(1, newTarget, w => w.WriteString("_abc"),
                (reader, t) => Tuple.Create(reader.ReadString(), t));

            Assert.AreEqual("NAME1", res.Item1);  // Old name converted by callback.
            Assert.AreEqual("name1_abc", res.Item2.OutStream(1, r => r.ReadString()));
            Assert.AreEqual("name1", provider.CallbackResult);  // Old name.

            // Returns a copy with same name.
            var resCopy = res.Item2.OutObject(1);
            Assert.AreEqual("name1_abc", resCopy.OutStream(1, r => r.ReadString()));

            // Async operation.
            var task = target.DoOutOpAsync(1, w => w.WriteString("foo"), r => r.ReadString());
            Assert.IsFalse(task.IsCompleted);
            var asyncRes = task.Result;
            Assert.IsTrue(task.IsCompleted);
            Assert.AreEqual("FOO", asyncRes);

            // Async operation with cancellation.
            var cts = new CancellationTokenSource();
            task = target.DoOutOpAsync(1, w => w.WriteString("foo"), r => r.ReadString(), cts.Token);
            Assert.IsFalse(task.IsCompleted);
            cts.Cancel();
            Assert.IsTrue(task.IsCanceled);
            var aex = Assert.Throws<AggregateException>(() => { asyncRes = task.Result; });
            Assert.IsInstanceOf<TaskCanceledException>(aex.GetBaseException());

            // Async operation with exception in entry point.
            Assert.Throws<TestIgnitePluginException>(() => target.DoOutOpAsync<object>(2, null, null));

            // Async operation with exception in future.
            var errTask = target.DoOutOpAsync<object>(3, null, null);
            Assert.IsFalse(errTask.IsCompleted);
            aex = Assert.Throws<AggregateException>(() => errTask.Wait());
            Assert.IsInstanceOf<IgniteException>(aex.InnerExceptions.Single());

            // Throws custom mapped exception.
            var ex = Assert.Throws<TestIgnitePluginException>(() => target.InLongOutLong(-1, 0));
            Assert.AreEqual("Baz", ex.Message);
            Assert.AreEqual(Ignition.GetIgnite(null), ex.Ignite);
            Assert.AreEqual("org.apache.ignite.platform.plugin.PlatformTestPluginException", ex.ClassName);

            var javaEx = ex.InnerException as JavaException;
            Assert.IsNotNull(javaEx);
            Assert.AreEqual("Baz", javaEx.JavaMessage);
            Assert.AreEqual("org.apache.ignite.platform.plugin.PlatformTestPluginException", javaEx.JavaClassName);
            Assert.IsTrue(javaEx.Message.Contains(
                "at org.apache.ignite.platform.plugin.PlatformTestPluginTarget.processInLongOutLong"));
        }

        /// <summary>
        /// Tests invalid plugins.
        /// </summary>
        [Test]
        public void TestInvalidPlugins()
        {
            Action<ICollection<IPluginConfiguration>> check = x => Ignition.Start(
                new IgniteConfiguration(TestUtils.GetTestConfiguration()) {PluginConfigurations = x});

            // Missing attribute.
            var ex = Assert.Throws<IgniteException>(() => check(new[] { new NoAttributeConfig(),  }));
            Assert.IsNotNull(ex.InnerException);
            Assert.AreEqual(string.Format("{0} of type {1} has no {2}", typeof(IPluginConfiguration),
                typeof(NoAttributeConfig), typeof(PluginProviderTypeAttribute)), ex.InnerException.Message);

            // Empty plugin name.
            ex = Assert.Throws<IgniteException>(() => check(new[] {new EmptyNameConfig()}));
            Assert.IsNotNull(ex.InnerException);
            Assert.AreEqual(string.Format("{0}.Name should not be null or empty: {1}", typeof(IPluginProvider<>),
                typeof(EmptyNamePluginProvider)), ex.InnerException.Message);

            // Duplicate plugin name.
            ex = Assert.Throws<IgniteException>(() => check(new[]
            {
                new TestIgnitePluginConfiguration(),
                new TestIgnitePluginConfiguration()
            }));
            Assert.IsNotNull(ex.InnerException);
            Assert.AreEqual(string.Format("Duplicate plugin name 'TestPlugin1' is used by plugin providers " +
                                          "'{0}' and '{0}'", typeof(TestIgnitePluginProvider)),
                                          ex.InnerException.Message);

            // Provider throws an exception.
            PluginLog.Clear();

            ex = Assert.Throws<IgniteException>(() => check(new IPluginConfiguration[]
            {
                new NormalConfig(), new ExceptionConfig()
            }));
            Assert.IsNotNull(ex.InnerException);
            Assert.IsInstanceOf<IOException>(ex.InnerException);
            Assert.AreEqual("Failure in plugin provider", ex.InnerException.Message);

            // Verify that plugins are started and stopped in correct order:
            Assert.AreEqual(
                new[]
                {
                    "normalPlugin.Start", "errPlugin.Start",
                    "errPlugin.Stop", "normalPlugin.Stop"
                }, PluginLog);
        }

        private class NoAttributeConfig : IPluginConfiguration
        {
            public int? PluginConfigurationClosureFactoryId
            {
                get { return null; }
            }

            public void WriteBinary(IBinaryRawWriter writer)
            {
                throw new NotImplementedException();
            }
        }

        [PluginProviderType(typeof(EmptyNamePluginProvider))]
        private class EmptyNameConfig : IPluginConfiguration
        {
            public int? PluginConfigurationClosureFactoryId
            {
                get { return null; }
            }

            public void WriteBinary(IBinaryRawWriter writer)
            {
                throw new NotImplementedException();
            }
        }

        private class EmptyNamePluginProvider : IPluginProvider<EmptyNameConfig>
        {
            public string Name { get { return null; } }
            public string Copyright { get { return null; } }
            public T GetPlugin<T>() where T : class { return default(T); }
            public void Start(IPluginContext<EmptyNameConfig> context) { /* No-op. */ }
            public void Stop(bool cancel) { /* No-op. */ }
            public void OnIgniteStart() { /* No-op. */ }
            public void OnIgniteStop(bool cancel) { /* No-op. */ }
        }

        [PluginProviderType(typeof(ExceptionPluginProvider))]
        private class ExceptionConfig : IPluginConfiguration
        {
            public int? PluginConfigurationClosureFactoryId
            {
                get { return null; }
            }

            public void WriteBinary(IBinaryRawWriter writer)
            {
                throw new NotImplementedException();
            }
        }

        private class ExceptionPluginProvider : IPluginProvider<ExceptionConfig>
        {
            public string Name { get { return "errPlugin"; } }
            public string Copyright { get { return null; } }
            public T GetPlugin<T>() where T : class { return default(T); }

            public void Stop(bool cancel)
            {
                PluginLog.Add(Name + ".Stop");
            }

            public void OnIgniteStart()
            {
                PluginLog.Add(Name + ".OnIgniteStart");
            }

            public void OnIgniteStop(bool cancel)
            {
                PluginLog.Add(Name + ".OnIgniteStop");
            }

            public void Start(IPluginContext<ExceptionConfig> context)
            {
                PluginLog.Add(Name + ".Start");
                throw new IOException("Failure in plugin provider");
            }
        }

        [PluginProviderType(typeof(NormalPluginProvider))]
        private class NormalConfig : IPluginConfiguration
        {
            public int? PluginConfigurationClosureFactoryId
            {
                get { return null; }
            }

            public void WriteBinary(IBinaryRawWriter writer)
            {
                throw new NotImplementedException();
            }
        }

        private class NormalPluginProvider : IPluginProvider<NormalConfig>
        {
            public string Name { get { return "normalPlugin"; } }
            public string Copyright { get { return null; } }
            public T GetPlugin<T>() where T : class { return default(T); }

            public void Stop(bool cancel)
            {
                PluginLog.Add(Name + ".Stop");
            }

            public void OnIgniteStart()
            {
                PluginLog.Add(Name + ".OnIgniteStart");
            }

            public void OnIgniteStop(bool cancel)
            {
                PluginLog.Add(Name + ".OnIgniteStop");
            }

            public void Start(IPluginContext<NormalConfig> context)
            {
                PluginLog.Add(Name + ".Start");
            }
        }

        private class Injectee
        {
            [InstanceResource]
            // ReSharper disable once UnusedAutoPropertyAccessor.Local
            public IIgnite Ignite { get; set; }
        }
    }
}

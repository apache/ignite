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
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests the plugin system.
    /// </summary>
    public class PluginTest
    {
        /** */
        private IIgnite _ignite;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            _ignite = Ignition.Start(TestUtils.GetTestConfiguration());
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the initialization.
        /// </summary>
        [Test]
        public void TestInitialization()
        {
            var plugin = _ignite.GetTestPlugin();

            Assert.IsNotNull(plugin);

            var plugin2 = _ignite.GetTestPlugin();

            Assert.AreSame(plugin.Target, plugin2.Target);
        }

        /// <summary>
        /// Tests the invoke operation.
        /// </summary>
        [Test]
        public void TestInvokeOperation()
        {
            var plugin = _ignite.GetTestPlugin();

            // Test round trip.
            Assert.AreEqual(1, plugin.ReadWrite(1));
            Assert.AreEqual("hello", plugin.ReadWrite("hello"));

            // Test node id from PluginContext.
            Assert.AreEqual(_ignite.GetCluster().GetLocalNode().Id, plugin.GetNodeId());

            // Test exception.
            var ex = Assert.Throws<IgniteException>(() => plugin.Error("error text"));

            Assert.AreEqual("error text", ex.Message);
        }

        /// <summary>
        /// Tests the callback.
        /// </summary>
        [Test]
        public void TestCallback()
        {
            var plugin = _ignite.GetTestPlugin();
            var target = plugin.Target;

            object sender = null;

            target.Callback += (s, a) =>
            {
                Assert.AreEqual("test value", a.Reader.ReadObject<string>());

                a.Writer.WriteString("callback response");

                sender = s;
            };

            plugin.InvokeCallback("test value");

            Assert.AreEqual(target, sender);

            Assert.AreEqual("callback response", plugin.GetCallbackResponse());
        }

        /// <summary>
        /// Tests returning a new plugin target.
        /// </summary>
        [Test]
        public void TestReturnObject()
        {
            var plugin = _ignite.GetTestPlugin();

            Assert.AreEqual("root", plugin.GetName());

            var child = plugin.GetChild("child");

            // Verify new target.
            Assert.AreNotSame(plugin.Target, child.Target);

            // Call the new object.
            Assert.AreEqual("child", child.GetName());

            // Pass the new object as an argument.
            Assert.AreEqual("child", plugin.GetObjectName(child.Target));

            // Check disposal.
            child.Dispose();

            Assert.Throws<ObjectDisposedException>(() => child.GetName());
        }
    }
}

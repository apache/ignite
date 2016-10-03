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
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Plugin;
    using NUnit.Framework;

    /// <summary>
    /// Tests the plugin system.
    /// </summary>
    public class PluginTest
    {
        /** */
        private const int OpReadWrite = 1;

        /** */
        private const int OpError = 2;

        /** */
        private const int OpInvokeCallback = 3;

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
            var target = _ignite.GetTestPlugin().Target;

            // Test round trip.
            Assert.AreEqual(1, target.InvokeOperation(OpReadWrite,
                w => w.WriteObject(1), (r, _) => r.ReadObject<int>(), null));

            Assert.AreEqual("hello", target.InvokeOperation(OpReadWrite,
                w => w.WriteObject("hello"), (r, _) => r.ReadObject<string>(), null));

            // Test exception.
            var ex = Assert.Throws<IgniteException>(() => target.InvokeOperation(OpError,
                w => w.WriteObject("error text"), (r, _) => (object) null, null));

            Assert.AreEqual("error text", ex.Message);
        }

        /// <summary>
        /// Tests the callback.
        /// </summary>
        [Test]
        public void TestCallback()
        {
            var target = _ignite.GetTestPlugin().Target;

            object sender = null;
            PluginCallbackEventArgs args = null;

            target.Callback += (s, a) =>
            {
                sender = s;
                args = a;
            };

            target.InvokeOperation(OpInvokeCallback, w => w.WriteObject("test value"), (r, _) => (object) null, null);

            Assert.AreEqual(target, sender);
            Assert.IsNotNull(args);

            Assert.IsNotNull(args.Reader);
            Assert.IsNotNull(args.Writer);

            Assert.AreEqual("test value", args.Reader.ReadObject<string>());

            // TODO: Verify that Java part got our response back via additional op.
        }

        /// <summary>
        /// Tests returning a new plugin target.
        /// </summary>
        [Test]
        public void TestReturnObject()
        {
            // TODO
        }
    }
}

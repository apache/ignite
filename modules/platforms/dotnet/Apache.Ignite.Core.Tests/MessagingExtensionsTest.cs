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

namespace Apache.Ignite.Core.Tests
{
    using Apache.Ignite.Core.Messaging;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="MessagingExtensions"/>.
    /// </summary>
    public class MessagingExtensionsTest : IgniteTestBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MessagingExtensionsTest"/> class.
        /// </summary>
        public MessagingExtensionsTest()
            : base("config\\compute\\compute-grid1.xml", "config\\compute\\compute-grid2.xml")
        {
            // No-op.
        }

        /// <summary>
        /// Tests LocalListen.
        /// </summary>
        [Test]
        public void TestLocalListen()
        {
            int counter = 0;

            var listener = Messaging.LocalListen<int>((nodeId, msg) => (counter += msg) > 0);

            Messaging.Send(5);
            Messaging.Send(10);

            Assert.AreEqual(15, counter);

            Messaging.StopLocalListen(listener);

            Messaging.Send(10);

            Assert.AreEqual(15, counter);
        }

        /// <summary>
        /// Tests RemoteListen.
        /// </summary>
        [Test]
        public void TestRemoteListen()
        {
            int counter = 0;

            var listenId = Messaging.RemoteListen<int>((nodeId, msg) => (counter += msg) > 0);

            Messaging.Send(5);
            Messaging.Send(10);

            Assert.AreEqual(15, counter);

            Messaging.StopRemoteListen(listenId);

            Messaging.Send(10);

            Assert.AreEqual(15, counter);
        }

        /// <summary>
        /// Tests RemoteListen.
        /// </summary>
        [Test]
        public void TestRemoteListenAsync()
        {
            int counter = 0;

            var listenId = Messaging.RemoteListenAsync<int>((nodeId, msg) => (counter += msg) > 0).Result;

            Messaging.Send(5);
            Messaging.Send(10);

            Assert.AreEqual(15, counter);

            Messaging.StopRemoteListen(listenId);

            Messaging.Send(10);

            Assert.AreEqual(15, counter);
        }
    }
}

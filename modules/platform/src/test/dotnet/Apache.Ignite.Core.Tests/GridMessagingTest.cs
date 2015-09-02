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
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// <see cref="IMessaging"/> tests.
    /// </summary>
    public class GridMessagingTest
    {
        /** */
        private IIgnite grid1;

        /** */
        private IIgnite grid2;

        /** */
        private IIgnite grid3;

        /** */
        public static int messageId;

        /// <summary>
        /// Executes before each test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            grid1 = GridFactory.Start(Configuration("config\\compute\\compute-grid1.xml"));
            grid2 = GridFactory.Start(Configuration("config\\compute\\compute-grid2.xml"));
            grid3 = GridFactory.Start(Configuration("config\\compute\\compute-grid3.xml"));
        }

        /// <summary>
        /// Executes after each test.
        /// </summary>
        [TearDown]
        public virtual void TearDown()
        {
            try
            {
                GridTestUtils.AssertHandleRegistryIsEmpty(1000, grid1, grid2, grid3);

                MessagingTestHelper.AssertFailures();
            }
            finally 
            {
                // Stop all grids between tests to drop any hanging messages
                GridFactory.StopAll(true);
            }
        }

        /// <summary>
        /// Tests LocalListen.
        /// </summary>
        [Test]
        public void TestLocalListen()
        {
            TestLocalListen(null);
            TestLocalListen("string topic");
            TestLocalListen(NextId());
        }

        /// <summary>
        /// Tests LocalListen.
        /// </summary>
        [SuppressMessage("ReSharper", "AccessToModifiedClosure")]
        public void TestLocalListen(object topic)
        {
            var messaging = grid1.Message();
            var listener = MessagingTestHelper.GetListener();
            messaging.LocalListen(listener, topic);

            // Test sending
            CheckSend(topic);
            CheckSend(topic, grid2);
            CheckSend(topic, grid3);

            // Test different topic
            CheckNoMessage(NextId());
            CheckNoMessage(NextId(), grid2);

            // Test multiple subscriptions for the same filter
            messaging.LocalListen(listener, topic);
            messaging.LocalListen(listener, topic);
            CheckSend(topic, repeatMultiplier: 3); // expect all messages repeated 3 times

            messaging.StopLocalListen(listener, topic);
            CheckSend(topic, repeatMultiplier: 2); // expect all messages repeated 2 times

            messaging.StopLocalListen(listener, topic);
            CheckSend(topic); // back to 1 listener

            // Test message type mismatch
            var ex = Assert.Throws<IgniteException>(() => messaging.Send(1.1, topic));
            Assert.AreEqual("Unable to cast object of type 'System.Double' to type 'System.String'.", ex.Message);

            // Test end listen
            MessagingTestHelper.listenResult = false;
            CheckSend(topic, single: true); // we'll receive one more and then unsubscribe because of delegate result.
            CheckNoMessage(topic);

            // Start again
            MessagingTestHelper.listenResult = true;
            messaging.LocalListen(listener, topic);
            CheckSend(topic);

            // Stop
            messaging.StopLocalListen(listener, topic);
            CheckNoMessage(topic);
        }

        /// <summary>
        /// Tests LocalListen with projection.
        /// </summary>
        [Test]
        public void TestLocalListenProjection()
        {
            TestLocalListenProjection(null);
            TestLocalListenProjection("prj");
            TestLocalListenProjection(NextId());
        }

        /// <summary>
        /// Tests LocalListen with projection.
        /// </summary>
        private void TestLocalListenProjection(object topic)
        {
            var grid3GotMessage = false;

            var grid3Listener = new MessageFilter<string>((id, x) =>
            {
                grid3GotMessage = true;
                return true;
            });

            grid3.Message().LocalListen(grid3Listener, topic);

            var clusterMessaging = grid1.Cluster.ForNodes(grid1.Cluster.LocalNode, grid2.Cluster.LocalNode).Message();
            var clusterListener = MessagingTestHelper.GetListener();
            clusterMessaging.LocalListen(clusterListener, topic);

            CheckSend(msg: clusterMessaging, topic: topic);
            Assert.IsFalse(grid3GotMessage, "Grid3 should not get messages");

            CheckSend(grid: grid2, msg: clusterMessaging, topic: topic);
            Assert.IsFalse(grid3GotMessage, "Grid3 should not get messages");

            clusterMessaging.StopLocalListen(clusterListener, topic);
            grid3.Message().StopLocalListen(grid3Listener, topic);
        }

        /// <summary>
        /// Tests LocalListen in multithreaded mode.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "AccessToModifiedClosure")]
        [Category(GridTestUtils.CATEGORY_INTENSIVE)]
        public void TestLocalListenMultithreaded()
        {
            const int threadCnt = 20;
            const int runSeconds = 20;

            var messaging = grid1.Message();

            var senders = Task.Factory.StartNew(() => GridTestUtils.RunMultiThreaded(() =>
            {
                messaging.Send((object) NextMessage());
                Thread.Sleep(50);
            }, threadCnt, runSeconds));


            var sharedReceived = 0;

            var sharedListener = new MessageFilter<string>((id, x) =>
            {
                Interlocked.Increment(ref sharedReceived);
                Thread.MemoryBarrier();
                return true;
            });

            GridTestUtils.RunMultiThreaded(() =>
            {
                // Check that listen/stop work concurrently
                messaging.LocalListen(sharedListener);

                for (int i = 0; i < 100; i++)
                {
                    messaging.LocalListen(sharedListener);
                    messaging.StopLocalListen(sharedListener);
                }

                var localReceived = 0;
                var stopLocal = 0;

                var localListener = new MessageFilter<string>((id, x) =>
                {
                    Interlocked.Increment(ref localReceived);
                    Thread.MemoryBarrier();
                    return Thread.VolatileRead(ref stopLocal) == 0;
                });

                messaging.LocalListen(localListener);

                Thread.Sleep(100);

                Thread.VolatileWrite(ref stopLocal, 1);

                Thread.Sleep(1000);

                var result = Thread.VolatileRead(ref localReceived);

                Thread.Sleep(100);

                // Check that unsubscription worked properly
                Assert.AreEqual(result, Thread.VolatileRead(ref localReceived));

                messaging.StopLocalListen(sharedListener);

            }, threadCnt, runSeconds);

            senders.Wait();

            Thread.Sleep(100);

            var sharedResult = Thread.VolatileRead(ref sharedReceived);

            messaging.Send((object)NextMessage());

            Thread.Sleep(MessagingTestHelper.MESSAGE_TIMEOUT);

            // Check that unsubscription worked properly
            Assert.AreEqual(sharedResult, Thread.VolatileRead(ref sharedReceived));
        }

        /// <summary>
        /// Tests RemoteListen.
        /// </summary>
        [Test]
        public void TestRemoteListen()
        {
            TestRemoteListen(null);
            TestRemoteListen("string topic");
            TestRemoteListen(NextId());
        }

        /// <summary>
        /// Tests RemoteListen with async mode enabled.
        /// </summary>
        [Test]
        public void TestRemoteListenAsync()
        {
            TestRemoteListen(null, true);
            TestRemoteListen("string topic", true);
            TestRemoteListen(NextId(), true);
        }

        /// <summary>
        /// Tests RemoteListen.
        /// </summary>
        public void TestRemoteListen(object topic, bool async = false)
        {
            var messaging = async ? grid1.Message().WithAsync() : grid1.Message();

            var listener = MessagingTestHelper.GetListener();
            var listenId = messaging.RemoteListen(listener, topic);

            if (async)
                listenId = messaging.GetFuture<Guid>().Get();

            // Test sending
            CheckSend(topic, msg: messaging, remoteListen: true);

            // Test different topic
            CheckNoMessage(NextId());

            // Test multiple subscriptions for the same filter
            var listenId2 = messaging.RemoteListen(listener, topic);

            if (async)
                listenId2 = messaging.GetFuture<Guid>().Get();

            CheckSend(topic, msg: messaging, remoteListen: true, repeatMultiplier: 2); // expect twice the messages

            messaging.StopRemoteListen(listenId2);

            if (async)
                messaging.GetFuture().Get();

            CheckSend(topic, msg: messaging, remoteListen: true); // back to normal after unsubscription

            // Test message type mismatch
            var ex = Assert.Throws<IgniteException>(() => messaging.Send(1.1, topic));
            Assert.AreEqual("Unable to cast object of type 'System.Double' to type 'System.String'.", ex.Message);

            // Test end listen
            messaging.StopRemoteListen(listenId);

            if (async)
                messaging.GetFuture().Get();

            CheckNoMessage(topic);
        }

        /// <summary>
        /// Tests RemoteListen with a projection.
        /// </summary>
        [Test]
        public void TestRemoteListenProjection()
        {
            TestRemoteListenProjection(null);
            TestRemoteListenProjection("string topic");
            TestRemoteListenProjection(NextId());
        }

        /// <summary>
        /// Tests RemoteListen with a projection.
        /// </summary>
        private void TestRemoteListenProjection(object topic)
        {
            var clusterMessaging = grid1.Cluster.ForNodes(grid1.Cluster.LocalNode, grid2.Cluster.LocalNode).Message();
            var clusterListener = MessagingTestHelper.GetListener();
            var listenId = clusterMessaging.RemoteListen(clusterListener, topic);

            CheckSend(msg: clusterMessaging, topic: topic, remoteListen: true);

            clusterMessaging.StopRemoteListen(listenId);

            CheckNoMessage(topic);
        }

        /// <summary>
        /// Tests LocalListen in multithreaded mode.
        /// </summary>
        [Test]
        [Category(GridTestUtils.CATEGORY_INTENSIVE)]
        public void TestRemoteListenMultithreaded()
        {
            const int threadCnt = 20;
            const int runSeconds = 20;

            var messaging = grid1.Message();

            var senders = Task.Factory.StartNew(() => GridTestUtils.RunMultiThreaded(() =>
            {
                MessagingTestHelper.ClearReceived(int.MaxValue);
                messaging.Send((object) NextMessage());
                Thread.Sleep(50);
            }, threadCnt, runSeconds));


            var sharedListener = MessagingTestHelper.GetListener();

            for (int i = 0; i < 100; i++)
                messaging.RemoteListen(sharedListener);  // add some listeners to be stopped by filter result

            GridTestUtils.RunMultiThreaded(() =>
            {
                // Check that listen/stop work concurrently
                messaging.StopRemoteListen(messaging.RemoteListen(sharedListener));

            }, threadCnt, runSeconds);

            MessagingTestHelper.listenResult = false;

            messaging.Send((object) NextMessage()); // send a message to make filters return false

            Thread.Sleep(MessagingTestHelper.MESSAGE_TIMEOUT); // wait for all to unsubscribe

            MessagingTestHelper.listenResult = true;

            senders.Wait(); // wait for senders to stop

            var sharedResult = MessagingTestHelper.RECEIVED_MESSAGES.Count;

            messaging.Send((object) NextMessage());

            Thread.Sleep(MessagingTestHelper.MESSAGE_TIMEOUT);

            // Check that unsubscription worked properly
            Assert.AreEqual(sharedResult, MessagingTestHelper.RECEIVED_MESSAGES.Count);
            
        }

        /// <summary>
        /// Sends messages in various ways and verefies correct receival.
        /// </summary>
        /// <param name="topic">Topic.</param>
        /// <param name="grid">The grid to use.</param>
        /// <param name="msg">Messaging to use.</param>
        /// <param name="remoteListen">Whether to expect remote listeners.</param>
        /// <param name="single">When true, only check one message.</param>
        /// <param name="repeatMultiplier">Expected message count multiplier.</param>
        private void CheckSend(object topic = null, IIgnite grid = null,
            IMessaging msg = null, bool remoteListen = false, bool single = false, int repeatMultiplier = 1)
        {
            IClusterGroup cluster;

            if (msg != null)
                cluster = msg.ClusterGroup;
            else
            {
                grid = grid ?? grid1;
                msg = grid.Message();
                cluster = grid.Cluster.ForLocal();
            }

            // Messages will repeat due to multiple nodes listening
            var expectedRepeat = repeatMultiplier * (remoteListen ? cluster.Nodes().Count : 1);

            var messages = Enumerable.Range(1, 10).Select(x => NextMessage()).OrderBy(x => x).ToList();

            // Single message
            MessagingTestHelper.ClearReceived(expectedRepeat);
            msg.Send((object) messages[0], topic);
            MessagingTestHelper.VerifyReceive(cluster, messages.Take(1), m => m.ToList(), expectedRepeat);

            if (single)
                return;

            // Multiple messages (receive order is undefined)
            MessagingTestHelper.ClearReceived(messages.Count * expectedRepeat);
            msg.Send(messages, topic);
            MessagingTestHelper.VerifyReceive(cluster, messages, m => m.OrderBy(x => x), expectedRepeat);

            // Multiple messages, ordered
            MessagingTestHelper.ClearReceived(messages.Count * expectedRepeat);
            messages.ForEach(x => msg.SendOrdered(x, topic, MessagingTestHelper.MESSAGE_TIMEOUT));

            if (remoteListen) // in remote scenario messages get mixed up due to different timing on different nodes
                MessagingTestHelper.VerifyReceive(cluster, messages, m => m.OrderBy(x => x), expectedRepeat);
            else
                MessagingTestHelper.VerifyReceive(cluster, messages, m => m.Reverse(), expectedRepeat);
        }

        /// <summary>
        /// Checks that no message has arrived.
        /// </summary>
        private void CheckNoMessage(object topic, IIgnite grid = null)
        {
            // this will result in an exception in case of a message
            MessagingTestHelper.ClearReceived(0);

            (grid ?? grid1).Message().Send(NextMessage(), topic);

            Thread.Sleep(MessagingTestHelper.MESSAGE_TIMEOUT);

            MessagingTestHelper.AssertFailures();
        }

        /// <summary>
        /// Gets the grid configuration.
        /// </summary>
        private static GridConfiguration Configuration(string springConfigUrl)
        {
            return new GridConfiguration
            {
                SpringConfigUrl = springConfigUrl,
                JvmClasspath = GridTestUtils.CreateTestClasspath(),
                JvmOptions = GridTestUtils.TestJavaOptions()
            };
        }

        /// <summary>
        /// Generates next message with sequential ID and current test name.
        /// </summary>
        private static string NextMessage()
        {
            var id = NextId();
            return id + "_" + TestContext.CurrentContext.Test.Name;
        }

        /// <summary>
        /// Generates next sequential ID.
        /// </summary>
        private static int NextId()
        {
            return Interlocked.Increment(ref messageId);
        }
    }

    /// <summary>
    /// Messaging test helper class.
    /// </summary>
    [Serializable]
    public static class MessagingTestHelper
    {
        /** */
        public static readonly ConcurrentStack<string> RECEIVED_MESSAGES = new ConcurrentStack<string>();
        
        /** */
        public static readonly ConcurrentStack<string> FAILURES = new ConcurrentStack<string>();

        /** */
        public static readonly CountdownEvent RECEIVED_EVENT = new CountdownEvent(0);

        /** */
        public static readonly ConcurrentStack<Guid> LAST_NODE_IDS = new ConcurrentStack<Guid>();

        /** */
        public static volatile bool listenResult = true;

        /** */
        public static readonly TimeSpan MESSAGE_TIMEOUT = TimeSpan.FromMilliseconds(700);

        /// <summary>
        /// Clears received message information.
        /// </summary>
        /// <param name="expectedCount">The expected count of messages to be received.</param>
        public static void ClearReceived(int expectedCount)
        {
            RECEIVED_MESSAGES.Clear();
            RECEIVED_EVENT.Reset(expectedCount);
            LAST_NODE_IDS.Clear();
        }

        /// <summary>
        /// Verifies received messages against expected messages.
        /// </summary>
        /// <param name="cluster">Cluster.</param>
        /// <param name="expectedMessages">Expected messages.</param>
        /// <param name="resultFunc">Result transform function.</param>
        /// <param name="expectedRepeat">Expected repeat count.</param>
        public static void VerifyReceive(IClusterGroup cluster, IEnumerable<string> expectedMessages,
            Func<IEnumerable<string>, IEnumerable<string>> resultFunc, int expectedRepeat)
        {
            // check if expected message count has been received; Wait returns false if there were none.
            Assert.IsTrue(RECEIVED_EVENT.Wait(MESSAGE_TIMEOUT));

            expectedMessages = expectedMessages.SelectMany(x => Enumerable.Repeat(x, expectedRepeat));

            Assert.AreEqual(expectedMessages, resultFunc(RECEIVED_MESSAGES));

            // check that all messages came from local node.
            var localNodeId = cluster.Grid.Cluster.LocalNode.Id;
            Assert.AreEqual(localNodeId, LAST_NODE_IDS.Distinct().Single());
            
            AssertFailures();
        }

        /// <summary>
        /// Gets the message listener.
        /// </summary>
        /// <returns>New instance of message listener.</returns>
        public static IMessageFilter<string> GetListener()
        {
            return new MessageFilter<string>(Listen);
        }

        /// <summary>
        /// Combines accumulated failures and throws an assertion, if there are any.
        /// Clears accumulated failures.
        /// </summary>
        public static void AssertFailures()
        {
            if (FAILURES.Any())
                Assert.Fail(FAILURES.Reverse().Aggregate((x, y) => string.Format("{0}\n{1}", x, y)));

            FAILURES.Clear();
        }

        /// <summary>
        /// Listen method.
        /// </summary>
        /// <param name="id">Originating node ID.</param>
        /// <param name="msg">Message.</param>
        private static bool Listen(Guid id, string msg)
        {
            try
            {
                LAST_NODE_IDS.Push(id);
                RECEIVED_MESSAGES.Push(msg);

                RECEIVED_EVENT.Signal();

                return listenResult;
            }
            catch (Exception ex)
            {
                // When executed on remote nodes, these exceptions will not go to sender, 
                // so we have to accumulate them.
                FAILURES.Push(string.Format("Exception in Listen (msg: {0}, id: {1}): {2}", msg, id, ex));
                throw;
            }
        }
    }

    /// <summary>
    /// Test message filter.
    /// </summary>
    [Serializable]
    public class MessageFilter<T> : IMessageFilter<T>
    {
        /** */
        private readonly Func<Guid, T, bool> invoke;

        #pragma warning disable 649
        /** Grid. */
        [InstanceResource]
        private IIgnite grid;
        #pragma warning restore 649

        /// <summary>
        /// Initializes a new instance of the <see cref="MessageFilter{T}"/> class.
        /// </summary>
        /// <param name="invoke">The invoke delegate.</param>
        public MessageFilter(Func<Guid, T, bool> invoke)
        {
            this.invoke = invoke;
        }

        /** <inheritdoc /> */
        public bool Invoke(Guid nodeId, T message)
        {
            Assert.IsNotNull(grid);
            return invoke(nodeId, message);
        }
    }
}

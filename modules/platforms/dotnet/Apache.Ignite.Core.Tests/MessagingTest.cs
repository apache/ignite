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
    public class MessagingTest
    {
        /** */
        private IIgnite _grid1;

        /** */
        private IIgnite _grid2;

        /** */
        private IIgnite _grid3;

        /** */
        public static int MessageId;

        /// <summary>
        /// Executes before each test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            _grid1 = Ignition.Start(Configuration("config\\compute\\compute-grid1.xml"));
            _grid2 = Ignition.Start(Configuration("config\\compute\\compute-grid2.xml"));
            _grid3 = Ignition.Start(Configuration("config\\compute\\compute-grid3.xml"));

            Assert.AreEqual(3, _grid1.GetCluster().GetNodes().Count);
        }

        /// <summary>
        /// Executes after each test.
        /// </summary>
        [TearDown]
        public virtual void TearDown()
        {
            try
            {
                TestUtils.AssertHandleRegistryIsEmpty(1000, _grid1, _grid2, _grid3);

                MessagingTestHelper.AssertFailures();
            }
            finally 
            {
                // Stop all grids between tests to drop any hanging messages
                Ignition.StopAll(true);
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
            var messaging = _grid1.GetMessaging();
            var listener = MessagingTestHelper.GetListener();
            messaging.LocalListen(listener, topic);

            // Test sending
            CheckSend(topic);
            CheckSend(topic, _grid2);
            CheckSend(topic, _grid3);

            // Test different topic
            CheckNoMessage(NextId());
            CheckNoMessage(NextId(), _grid2);

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
            MessagingTestHelper.ListenResult = false;
            CheckSend(topic, single: true); // we'll receive one more and then unsubscribe because of delegate result.
            CheckNoMessage(topic);

            // Start again
            MessagingTestHelper.ListenResult = true;
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

            var grid3Listener = new MessageListener<string>((id, x) =>
            {
                grid3GotMessage = true;
                return true;
            });

            _grid3.GetMessaging().LocalListen(grid3Listener, topic);

            var clusterMessaging = _grid1.GetCluster().ForNodes(_grid1.GetCluster().GetLocalNode(), _grid2.GetCluster().GetLocalNode()).GetMessaging();
            var clusterListener = MessagingTestHelper.GetListener();
            clusterMessaging.LocalListen(clusterListener, topic);

            CheckSend(msg: clusterMessaging, topic: topic);
            Assert.IsFalse(grid3GotMessage, "Grid3 should not get messages");

            CheckSend(grid: _grid2, msg: clusterMessaging, topic: topic);
            Assert.IsFalse(grid3GotMessage, "Grid3 should not get messages");

            clusterMessaging.StopLocalListen(clusterListener, topic);
            _grid3.GetMessaging().StopLocalListen(grid3Listener, topic);
        }

        /// <summary>
        /// Tests LocalListen in multithreaded mode.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "AccessToModifiedClosure")]
        [Category(TestUtils.CategoryIntensive)]
        public void TestLocalListenMultithreaded()
        {
            const int threadCnt = 20;
            const int runSeconds = 20;

            var messaging = _grid1.GetMessaging();

            var senders = Task.Factory.StartNew(() => TestUtils.RunMultiThreaded(() =>
            {
                messaging.Send(NextMessage());
                Thread.Sleep(50);
            }, threadCnt, runSeconds));


            var sharedReceived = 0;

            var sharedListener = new MessageListener<string>((id, x) =>
            {
                Interlocked.Increment(ref sharedReceived);
                Thread.MemoryBarrier();
                return true;
            });

            TestUtils.RunMultiThreaded(() =>
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

                var localListener = new MessageListener<string>((id, x) =>
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

            messaging.Send(NextMessage());

            Thread.Sleep(MessagingTestHelper.MessageTimeout);

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
        private void TestRemoteListen(object topic, bool async = false)
        {
            var messaging =_grid1.GetMessaging();

            var listener = MessagingTestHelper.GetListener();
            var listenId = async
                ? messaging.RemoteListenAsync(listener, topic).Result
                : messaging.RemoteListen(listener, topic);

            // Test sending
            CheckSend(topic, msg: messaging, remoteListen: true);

            // Test different topic
            CheckNoMessage(NextId());

            // Test multiple subscriptions for the same filter
            var listenId2 = async
                ? messaging.RemoteListenAsync(listener, topic).Result
                : messaging.RemoteListen(listener, topic);

            CheckSend(topic, msg: messaging, remoteListen: true, repeatMultiplier: 2); // expect twice the messages

            if (async)
                messaging.StopRemoteListenAsync(listenId2).Wait();
            else
                messaging.StopRemoteListen(listenId2);

            CheckSend(topic, msg: messaging, remoteListen: true); // back to normal after unsubscription

            // Test message type mismatch
            var ex = Assert.Throws<IgniteException>(() => messaging.Send(1.1, topic));
            Assert.AreEqual("Unable to cast object of type 'System.Double' to type 'System.String'.", ex.Message);

            // Test end listen
            if (async)
                messaging.StopRemoteListenAsync(listenId).Wait();
            else
                messaging.StopRemoteListen(listenId);

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
            var clusterMessaging = _grid1.GetCluster().ForNodes(_grid1.GetCluster().GetLocalNode(), _grid2.GetCluster().GetLocalNode()).GetMessaging();
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
        [Category(TestUtils.CategoryIntensive)]
        public void TestRemoteListenMultithreaded()
        {
            const int threadCnt = 20;
            const int runSeconds = 20;

            var messaging = _grid1.GetMessaging();

            var senders = Task.Factory.StartNew(() => TestUtils.RunMultiThreaded(() =>
            {
                MessagingTestHelper.ClearReceived(int.MaxValue);
                messaging.Send(NextMessage());
                Thread.Sleep(50);
            }, threadCnt, runSeconds));


            var sharedListener = MessagingTestHelper.GetListener();

            for (int i = 0; i < 100; i++)
                messaging.RemoteListen(sharedListener);  // add some listeners to be stopped by filter result

            TestUtils.RunMultiThreaded(() =>
            {
                // Check that listen/stop work concurrently
                messaging.StopRemoteListen(messaging.RemoteListen(sharedListener));

            }, threadCnt, runSeconds / 2);

            MessagingTestHelper.ListenResult = false;

            messaging.Send(NextMessage()); // send a message to make filters return false

            Thread.Sleep(MessagingTestHelper.MessageTimeout); // wait for all to unsubscribe

            MessagingTestHelper.ListenResult = true;

            senders.Wait(); // wait for senders to stop

            MessagingTestHelper.ClearReceived(int.MaxValue);

            var lastMsg = NextMessage();
            messaging.Send(lastMsg);

            Thread.Sleep(MessagingTestHelper.MessageTimeout);

            // Check that unsubscription worked properly
            var sharedResult = MessagingTestHelper.ReceivedMessages.ToArray();

            if (sharedResult.Length != 0)
            {
                Assert.Fail("Unexpected messages ({0}): {1}; last sent message: {2}", sharedResult.Length, 
                    string.Join(",", sharedResult), lastMsg);
            }
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
                grid = grid ?? _grid1;
                msg = grid.GetMessaging();
                cluster = grid.GetCluster().ForLocal();
            }

            // Messages will repeat due to multiple nodes listening
            var expectedRepeat = repeatMultiplier * (remoteListen ? cluster.GetNodes().Count : 1);

            var messages = Enumerable.Range(1, 10).Select(x => NextMessage()).OrderBy(x => x).ToList();

            // Single message
            MessagingTestHelper.ClearReceived(expectedRepeat);
            msg.Send(messages[0], topic);
            MessagingTestHelper.VerifyReceive(cluster, messages.Take(1), m => m.ToList(), expectedRepeat);

            if (single)
                return;

            // Multiple messages (receive order is undefined)
            MessagingTestHelper.ClearReceived(messages.Count * expectedRepeat);
            msg.SendAll(messages, topic);
            MessagingTestHelper.VerifyReceive(cluster, messages, m => m.OrderBy(x => x), expectedRepeat);

            // Multiple messages, ordered
            MessagingTestHelper.ClearReceived(messages.Count * expectedRepeat);
            messages.ForEach(x => msg.SendOrdered(x, topic, MessagingTestHelper.MessageTimeout));

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

            (grid ?? _grid1).GetMessaging().SendAll(NextMessage(), topic);

            Thread.Sleep(MessagingTestHelper.MessageTimeout);

            MessagingTestHelper.AssertFailures();
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        private static IgniteConfiguration Configuration(string springConfigUrl)
        {
            return new IgniteConfiguration
            {
                SpringConfigUrl = springConfigUrl,
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions()
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
            return Interlocked.Increment(ref MessageId);
        }
    }

    /// <summary>
    /// Messaging test helper class.
    /// </summary>
    [Serializable]
    public static class MessagingTestHelper
    {
        /** */
        public static readonly ConcurrentStack<string> ReceivedMessages = new ConcurrentStack<string>();
        
        /** */
        public static readonly ConcurrentStack<string> Failures = new ConcurrentStack<string>();

        /** */
        public static readonly CountdownEvent ReceivedEvent = new CountdownEvent(0);

        /** */
        public static readonly ConcurrentStack<Guid> LastNodeIds = new ConcurrentStack<Guid>();

        /** */
        public static volatile bool ListenResult = true;

        /** */
        public static readonly TimeSpan MessageTimeout = TimeSpan.FromMilliseconds(700);

        /// <summary>
        /// Clears received message information.
        /// </summary>
        /// <param name="expectedCount">The expected count of messages to be received.</param>
        public static void ClearReceived(int expectedCount)
        {
            ReceivedMessages.Clear();
            ReceivedEvent.Reset(expectedCount);
            LastNodeIds.Clear();
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
            Assert.IsTrue(ReceivedEvent.Wait(MessageTimeout),
                string.Format("expectedMessages: {0}, expectedRepeat: {1}, remaining: {2}",
                    expectedMessages, expectedRepeat, ReceivedEvent.CurrentCount));

            expectedMessages = expectedMessages.SelectMany(x => Enumerable.Repeat(x, expectedRepeat));

            Assert.AreEqual(expectedMessages, resultFunc(ReceivedMessages));

            // check that all messages came from local node.
            var localNodeId = cluster.Ignite.GetCluster().GetLocalNode().Id;
            Assert.AreEqual(localNodeId, LastNodeIds.Distinct().Single());
            
            AssertFailures();
        }

        /// <summary>
        /// Gets the message listener.
        /// </summary>
        /// <returns>New instance of message listener.</returns>
        public static IMessageListener<string> GetListener()
        {
            return new MessageListener<string>(Listen);
        }

        /// <summary>
        /// Combines accumulated failures and throws an assertion, if there are any.
        /// Clears accumulated failures.
        /// </summary>
        public static void AssertFailures()
        {
            if (Failures.Any())
                Assert.Fail(Failures.Reverse().Aggregate((x, y) => string.Format("{0}\n{1}", x, y)));

            Failures.Clear();
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
                LastNodeIds.Push(id);
                ReceivedMessages.Push(msg);

                ReceivedEvent.Signal();

                return ListenResult;
            }
            catch (Exception ex)
            {
                // When executed on remote nodes, these exceptions will not go to sender, 
                // so we have to accumulate them.
                Failures.Push(string.Format("Exception in Listen (msg: {0}, id: {1}): {2}", msg, id, ex));
                throw;
            }
        }
    }

    /// <summary>
    /// Test message filter.
    /// </summary>
    [Serializable]
    public class MessageListener<T> : IMessageListener<T>
    {
        /** */
        private readonly Func<Guid, T, bool> _invoke;

        #pragma warning disable 649
        /** Grid. */
        [InstanceResource]
        private IIgnite _grid;
        #pragma warning restore 649

        /// <summary>
        /// Initializes a new instance of the <see cref="MessageListener{T}"/> class.
        /// </summary>
        /// <param name="invoke">The invoke delegate.</param>
        public MessageListener(Func<Guid, T, bool> invoke)
        {
            _invoke = invoke;
        }

        /** <inheritdoc /> */
        public bool Invoke(Guid nodeId, T message)
        {
            Assert.IsNotNull(_grid);
            return _invoke(nodeId, message);
        }
    }
}

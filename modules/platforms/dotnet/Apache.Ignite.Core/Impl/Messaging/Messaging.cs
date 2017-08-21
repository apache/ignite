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

namespace Apache.Ignite.Core.Impl.Messaging
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Collections;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Messaging;

    /// <summary>
    /// Messaging functionality.
    /// </summary>
    internal class Messaging : PlatformTargetAdapter, IMessaging
    {
        /// <summary>
        /// Opcodes.
        /// </summary>
        private enum Op
        {
            LocalListen = 1,
            RemoteListen = 2,
            Send = 3,
            SendMulti = 4,
            SendOrdered = 5,
            StopLocalListen = 6,
            StopRemoteListen = 7,
            RemoteListenAsync = 9,
            StopRemoteListenAsync = 10
        }

        /** Map from user (func+topic) -> id, needed for unsubscription. */
        private readonly MultiValueDictionary<KeyValuePair<object, object>, long> _funcMap =
            new MultiValueDictionary<KeyValuePair<object, object>, long>();

        /** Grid */
        private readonly Ignite _ignite;
        
        /** Cluster group. */
        private readonly IClusterGroup _clusterGroup;

        /// <summary>
        /// Initializes a new instance of the <see cref="Messaging" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="prj">Cluster group.</param>
        public Messaging(IPlatformTargetInternal target, IClusterGroup prj)
            : base(target)
        {
            Debug.Assert(prj != null);

            _clusterGroup = prj;

            _ignite = (Ignite) prj.Ignite;
        }

        /** <inheritdoc /> */
        public IClusterGroup ClusterGroup
        {
            get { return _clusterGroup; }
        }

        /** <inheritdoc /> */
        public void Send(object message, object topic = null)
        {
            IgniteArgumentCheck.NotNull(message, "message");

            DoOutOp((int) Op.Send, topic, message);
        }

        /** <inheritdoc /> */
        public void SendAll(IEnumerable messages, object topic = null)
        {
            IgniteArgumentCheck.NotNull(messages, "messages");

            DoOutOp((int) Op.SendMulti, writer =>
            {
                writer.Write(topic);

                writer.WriteEnumerable(messages.OfType<object>());
            });
        }

        /** <inheritdoc /> */
        public void SendOrdered(object message, object topic = null, TimeSpan? timeout = null)
        {
            IgniteArgumentCheck.NotNull(message, "message");

            DoOutOp((int) Op.SendOrdered, writer =>
            {
                writer.Write(topic);
                writer.Write(message);

                writer.WriteLong((long)(timeout == null ? 0 : timeout.Value.TotalMilliseconds));
            });
        }

        /** <inheritdoc /> */
        public void LocalListen<T>(IMessageListener<T> listener, object topic = null)
        {
            IgniteArgumentCheck.NotNull(listener, "filter");

            ResourceProcessor.Inject(listener, _ignite);

            lock (_funcMap)
            {
                var key = GetKey(listener, topic);

                MessageListenerHolder filter0 = MessageListenerHolder.CreateLocal(_ignite, listener); 

                var filterHnd = _ignite.HandleRegistry.Allocate(filter0);

                filter0.DestroyAction = () =>
                {
                    lock (_funcMap)
                    {
                        _funcMap.Remove(key, filterHnd);
                    }
                };

                try
                {
                    DoOutOp((int) Op.LocalListen, writer =>
                    {
                        writer.WriteLong(filterHnd);
                        writer.Write(topic);
                    });
                }
                catch (Exception)
                {
                    _ignite.HandleRegistry.Release(filterHnd);

                    throw;
                }

                _funcMap.Add(key, filterHnd);
            }
        }

        /** <inheritdoc /> */
        public void StopLocalListen<T>(IMessageListener<T> listener, object topic = null)
        {
            IgniteArgumentCheck.NotNull(listener, "filter");

            long filterHnd;
            bool removed;

            lock (_funcMap)
            {
                removed = _funcMap.TryRemove(GetKey(listener, topic), out filterHnd);
            }

            if (removed)
            {
                DoOutOp((int) Op.StopLocalListen, writer =>
                {
                    writer.WriteLong(filterHnd);
                    writer.Write(topic);
                });
            }
        }

        /** <inheritdoc /> */
        public Guid RemoteListen<T>(IMessageListener<T> listener, object topic = null)
        {
            return RemoteListen(listener, topic,
                (writeAct, readAct) => DoOutInOp((int) Op.RemoteListen, writeAct,
                    stream => readAct(Marshaller.StartUnmarshal(stream))));
        }

        /** <inheritdoc /> */
        public Task<Guid> RemoteListenAsync<T>(IMessageListener<T> listener, object topic = null)
        {
            return RemoteListen(listener, topic,
                (writeAct, readAct) => DoOutOpAsync((int) Op.RemoteListenAsync, writeAct, convertFunc: readAct));
        }

        /** <inheritdoc /> */
        public void StopRemoteListen(Guid opId)
        {
            DoOutOp((int) Op.StopRemoteListen, writer => writer.WriteGuid(opId));
        }

        /** <inheritdoc /> */
        public Task StopRemoteListenAsync(Guid opId)
        {
            return DoOutOpAsync((int) Op.StopRemoteListenAsync, writer => writer.WriteGuid(opId));
        }

        /// <summary>
        /// Gets the key for user-provided filter and topic.
        /// </summary>
        /// <param name="filter">Filter.</param>
        /// <param name="topic">Topic.</param>
        /// <returns>Compound dictionary key.</returns>
        private static KeyValuePair<object, object> GetKey(object filter, object topic)
        {
            return new KeyValuePair<object, object>(filter, topic);
        }

        /// <summary>
        /// Remotes listen.
        /// </summary>
        private TRes RemoteListen<T, TRes>(IMessageListener<T> filter, object topic,
            Func<Action<IBinaryRawWriter>, Func<BinaryReader, Guid>, TRes> invoker)
        {
            IgniteArgumentCheck.NotNull(filter, "filter");

            var filter0 = MessageListenerHolder.CreateLocal(_ignite, filter);
            var filterHnd = _ignite.HandleRegistry.AllocateSafe(filter0);

            try
            {
                return invoker(writer =>
                {
                    writer.WriteObject(filter0);
                    writer.WriteLong(filterHnd);
                    writer.WriteObject(topic);
                }, input => input.ReadGuid() ?? Guid.Empty);
            }
            catch (Exception)
            {
                _ignite.HandleRegistry.Release(filterHnd);

                throw;
            }
        }
    }
}
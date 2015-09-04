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
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Collections;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Messaging;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Messaging functionality.
    /// </summary>
    internal class Messaging : PlatformTarget, IMessaging
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
            StopRemoteListen = 7
        }

        /** Map from user (func+topic) -> id, needed for unsubscription. */
        private readonly MultiValueDictionary<KeyValuePair<object, object>, long> _funcMap =
            new MultiValueDictionary<KeyValuePair<object, object>, long>();

        /** Grid */
        private readonly Ignite _ignite;

        /// <summary>
        /// Initializes a new instance of the <see cref="Messaging" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="prj">Cluster group.</param>
        public Messaging(IUnmanagedTarget target, PortableMarshaller marsh, IClusterGroup prj)
            : base(target, marsh)
        {
            Debug.Assert(prj != null);

            ClusterGroup = prj;

            _ignite = (Ignite) prj.Ignite;
        }

        /** <inheritdoc /> */
        public IClusterGroup ClusterGroup { get; private set; }

        /** <inheritdoc /> */
        public void Send(object message, object topic = null)
        {
            IgniteArgumentCheck.NotNull(message, "message");

            DoOutOp((int) Op.Send, topic, message);
        }

        /** <inheritdoc /> */
        public void Send(IEnumerable messages, object topic = null)
        {
            IgniteArgumentCheck.NotNull(messages, "messages");

            DoOutOp((int) Op.SendMulti, writer =>
            {
                writer.Write(topic);

                WriteEnumerable(writer, messages.OfType<object>());
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
        public void LocalListen<T>(IMessageFilter<T> filter, object topic = null)
        {
            IgniteArgumentCheck.NotNull(filter, "filter");

            ResourceProcessor.Inject(filter, _ignite);

            lock (_funcMap)
            {
                var key = GetKey(filter, topic);

                MessageFilterHolder filter0 = MessageFilterHolder.CreateLocal(_ignite, filter); 

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
        public void StopLocalListen<T>(IMessageFilter<T> filter, object topic = null)
        {
            IgniteArgumentCheck.NotNull(filter, "filter");

            long filterHnd;
            bool removed;

            lock (_funcMap)
            {
                removed = _funcMap.TryRemove(GetKey(filter, topic), out filterHnd);
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
        public Guid RemoteListen<T>(IMessageFilter<T> filter, object topic = null)
        {
            IgniteArgumentCheck.NotNull(filter, "filter");

            var filter0 = MessageFilterHolder.CreateLocal(_ignite, filter);
            var filterHnd = _ignite.HandleRegistry.AllocateSafe(filter0);

            try
            {
                Guid id = Guid.Empty;

                DoOutInOp((int) Op.RemoteListen, writer =>
                {
                    writer.Write(filter0);
                    writer.WriteLong(filterHnd);
                    writer.Write(topic);
                }, 
                input =>
                {
                    var id0 = Marshaller.StartUnmarshal(input).RawReader().ReadGuid();

                    Debug.Assert(IsAsync || id0.HasValue);

                    if (id0.HasValue)
                        id = id0.Value;
                });

                return id;
            }
            catch (Exception)
            {
                _ignite.HandleRegistry.Release(filterHnd);

                throw;
            }
        }

        /** <inheritdoc /> */
        public void StopRemoteListen(Guid opId)
        {
            DoOutOp((int) Op.StopRemoteListen, writer =>
            {
                writer.WriteGuid(opId);
            });
        }

        /** <inheritdoc /> */
        public virtual IMessaging WithAsync()
        {
            return new MessagingAsync(UU.MessagingWithASync(Target), Marshaller, ClusterGroup);
        }

        /** <inheritdoc /> */
        public virtual bool IsAsync
        {
            get { return false; }
        }

        /** <inheritdoc /> */
        public virtual IFuture GetFuture()
        {
            throw IgniteUtils.GetAsyncModeDisabledException();
        }

        /** <inheritdoc /> */
        public virtual IFuture<TResult> GetFuture<TResult>()
        {
            throw IgniteUtils.GetAsyncModeDisabledException();
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
    }
}
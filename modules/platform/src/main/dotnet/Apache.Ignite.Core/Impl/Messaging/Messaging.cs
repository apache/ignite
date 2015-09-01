﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Messaging
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Collections;
    using GridGain.Cluster;
    using GridGain.Common;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Resource;
    using GridGain.Impl.Unmanaged;
    using GridGain.Messaging;

    using U = GridGain.Impl.GridUtils;
    using UU = GridGain.Impl.Unmanaged.UnmanagedUtils;
    using A = Apache.Ignite.Core.Impl.Common.GridArgumentCheck;

    /// <summary>
    /// Messaging functionality.
    /// </summary>
    internal class Messaging : GridTarget, IMessaging
    {
        /// <summary>
        /// Opcodes.
        /// </summary>
        private enum Op
        {
            LOC_LISTEN = 1,
            REMOTE_LISTEN = 2,
            SEND = 3,
            SEND_MULTI = 4,
            SEND_ORDERED = 5,
            STOP_LOC_LISTEN = 6,
            STOP_REMOTE_LISTEN = 7
        }

        /** Map from user (func+topic) -> id, needed for unsubscription. */
        private readonly MultiValueDictionary<KeyValuePair<object, object>, long> funcMap =
            new MultiValueDictionary<KeyValuePair<object, object>, long>();

        /** Grid */
        private readonly GridImpl grid;

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

            grid = (GridImpl) prj.Grid;
        }

        /** <inheritdoc /> */
        public IClusterGroup ClusterGroup { get; private set; }

        /** <inheritdoc /> */
        public void Send(object message, object topic = null)
        {
            A.NotNull(message, "message");

            DoOutOp((int) Op.SEND, topic, message);
        }

        /** <inheritdoc /> */
        public void Send(IEnumerable messages, object topic = null)
        {
            A.NotNull(messages, "messages");

            DoOutOp((int) Op.SEND_MULTI, writer =>
            {
                writer.Write(topic);

                WriteEnumerable(writer, messages.OfType<object>());
            });
        }

        /** <inheritdoc /> */
        public void SendOrdered(object message, object topic = null, TimeSpan? timeout = null)
        {
            A.NotNull(message, "message");

            DoOutOp((int) Op.SEND_ORDERED, writer =>
            {
                writer.Write(topic);
                writer.Write(message);

                writer.WriteLong((long)(timeout == null ? 0 : timeout.Value.TotalMilliseconds));
            });
        }

        /** <inheritdoc /> */
        public void LocalListen<T>(IMessageFilter<T> filter, object topic = null)
        {
            A.NotNull(filter, "filter");

            ResourceProcessor.Inject(filter, grid);

            lock (funcMap)
            {
                var key = GetKey(filter, topic);

                MessageFilterHolder filter0 = MessageFilterHolder.CreateLocal(grid, filter); 

                var filterHnd = grid.HandleRegistry.Allocate(filter0);

                filter0.DestroyAction = () =>
                {
                    lock (funcMap)
                    {
                        funcMap.Remove(key, filterHnd);
                    }
                };

                try
                {
                    DoOutOp((int) Op.LOC_LISTEN, writer =>
                    {
                        writer.WriteLong(filterHnd);
                        writer.Write(topic);
                    });
                }
                catch (Exception)
                {
                    grid.HandleRegistry.Release(filterHnd);

                    throw;
                }

                funcMap.Add(key, filterHnd);
            }
        }

        /** <inheritdoc /> */
        public void StopLocalListen<T>(IMessageFilter<T> filter, object topic = null)
        {
            A.NotNull(filter, "filter");

            long filterHnd;
            bool removed;

            lock (funcMap)
            {
                removed = funcMap.TryRemove(GetKey(filter, topic), out filterHnd);
            }

            if (removed)
            {
                DoOutOp((int) Op.STOP_LOC_LISTEN, writer =>
                {
                    writer.WriteLong(filterHnd);
                    writer.Write(topic);
                });
            }
        }

        /** <inheritdoc /> */
        public Guid RemoteListen<T>(IMessageFilter<T> filter, object topic = null)
        {
            A.NotNull(filter, "filter");

            var filter0 = MessageFilterHolder.CreateLocal(grid, filter);
            var filterHnd = grid.HandleRegistry.AllocateSafe(filter0);

            try
            {
                Guid id = Guid.Empty;

                DoOutInOp((int) Op.REMOTE_LISTEN, writer =>
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
                grid.HandleRegistry.Release(filterHnd);

                throw;
            }
        }

        /** <inheritdoc /> */
        public void StopRemoteListen(Guid opId)
        {
            DoOutOp((int) Op.STOP_REMOTE_LISTEN, writer =>
            {
                writer.WriteGuid(opId);
            });
        }

        /** <inheritdoc /> */
        public virtual IMessaging WithAsync()
        {
            return new MessagingAsync(UU.MessagingWithASync(target), Marshaller, ClusterGroup);
        }

        /** <inheritdoc /> */
        public virtual bool IsAsync
        {
            get { return false; }
        }

        /** <inheritdoc /> */
        public virtual IFuture GetFuture()
        {
            throw U.GetAsyncModeDisabledException();
        }

        /** <inheritdoc /> */
        public virtual IFuture<TResult> GetFuture<TResult>()
        {
            throw U.GetAsyncModeDisabledException();
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
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

namespace Apache.Ignite.Core.Impl.Events
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Portable;
    using A = Apache.Ignite.Core.Impl.Common.GridArgumentCheck;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Grid events.
    /// </summary>
    internal class Events : GridTarget, IEvents
    {
        /// <summary>
        /// Opcodes.
        /// </summary>
        protected enum Op
        {
            REMOTE_QUERY = 1,
            REMOTE_LISTEN = 2,
            STOP_REMOTE_LISTEN = 3,
            WAIT_FOR_LOCAL = 4,
            LOCAL_QUERY = 5,
            RECORD_LOCAL = 6,
            ENABLE_LOCAL = 8,
            DISABLE_LOCAL = 9,
            GET_ENABLED_EVENTS = 10
        }

        /** Map from user func to local wrapper, needed for invoke/unsubscribe. */
        private readonly Dictionary<object, Dictionary<int, LocalHandledEventFilter>> _localFilters
            = new Dictionary<object, Dictionary<int, LocalHandledEventFilter>>();

        /** Grid. */
        protected readonly Ignite Grid;

        /// <summary>
        /// Initializes a new instance of the <see cref="Events"/> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="clusterGroup">Cluster group.</param>
        public Events(IUnmanagedTarget target, PortableMarshaller marsh, IClusterGroup clusterGroup)
            : base(target, marsh)
        {
            Debug.Assert(clusterGroup != null);

            ClusterGroup = clusterGroup;

            Grid = (Ignite) clusterGroup.Grid;
        }

        /** <inheritDoc /> */
        public virtual IEvents WithAsync()
        {
            return new EventsAsync(UU.EventsWithAsync(target), Marshaller, ClusterGroup);
        }

        /** <inheritDoc /> */
        public virtual bool IsAsync
        {
            get { return false; }
        }

        /** <inheritDoc /> */
        public virtual IFuture GetFuture()
        {
            throw IgniteUtils.GetAsyncModeDisabledException();
        }

        /** <inheritDoc /> */
        public virtual IFuture<TResult> GetFuture<TResult>()
        {
            throw IgniteUtils.GetAsyncModeDisabledException();
        }

        /** <inheritDoc /> */
        public IClusterGroup ClusterGroup { get; private set; }

        /** <inheritDoc /> */
        public virtual List<T> RemoteQuery<T>(IEventFilter<T> filter, TimeSpan? timeout = null, params int[] types)
            where T : IEvent
        {
            A.NotNull(filter, "filter");

            return DoOutInOp((int) Op.REMOTE_QUERY,
                writer =>
                {
                    writer.Write(new PortableOrSerializableObjectHolder(filter));

                    writer.WriteLong((long) (timeout == null ? 0 : timeout.Value.TotalMilliseconds));

                    WriteEventTypes(types, writer);
                },
                reader => ReadEvents<T>(reader));
        }

        /** <inheritDoc /> */
        public virtual Guid RemoteListen<T>(int bufSize = 1, TimeSpan? interval = null, bool autoUnsubscribe = true,
            IEventFilter<T> localListener = null, IEventFilter<T> remoteFilter = null, params int[] types)
            where T : IEvent
        {
            A.Ensure(bufSize > 0, "bufSize", "should be > 0");
            A.Ensure(interval == null || interval.Value.TotalMilliseconds > 0, "interval", "should be null or >= 0");

            return DoOutInOp((int) Op.REMOTE_LISTEN,
                writer =>
                {
                    writer.WriteInt(bufSize);
                    writer.WriteLong((long) (interval == null ? 0 : interval.Value.TotalMilliseconds));
                    writer.WriteBoolean(autoUnsubscribe);

                    writer.WriteBoolean(localListener != null);

                    if (localListener != null)
                    {
                        var listener = new RemoteListenEventFilter(Grid, (id, e) => localListener.Invoke(id, (T) e));
                        writer.WriteLong(Grid.HandleRegistry.Allocate(listener));
                    }

                    writer.WriteBoolean(remoteFilter != null);

                    if (remoteFilter != null)
                        writer.Write(new PortableOrSerializableObjectHolder(remoteFilter));

                    WriteEventTypes(types, writer);
                },
                reader => Marsh.StartUnmarshal(reader).ReadGuid() ?? Guid.Empty);
        }

        /** <inheritDoc /> */
        public virtual void StopRemoteListen(Guid opId)
        {
            DoOutOp((int) Op.STOP_REMOTE_LISTEN, writer =>
            {
                Marsh.StartMarshal(writer).WriteGuid(opId);
            });
        }

        /** <inheritDoc /> */
        public IEvent WaitForLocal(params int[] types)
        {
            return WaitForLocal<IEvent>(null, types);
        }

        /** <inheritDoc /> */
        public virtual T WaitForLocal<T>(IEventFilter<T> filter, params int[] types) where T : IEvent
        {
            long hnd = 0;

            try
            {
                return WaitForLocal0(filter, ref hnd, types);
            }
            finally
            {
                if (filter != null)
                    Grid.HandleRegistry.Release(hnd);
            }
        }

        /** <inheritDoc /> */
        public List<IEvent> LocalQuery(params int[] types)
        {
            return DoOutInOp((int) Op.LOCAL_QUERY,
                writer => WriteEventTypes(types, writer),
                reader => ReadEvents<IEvent>(reader));
        }

        /** <inheritDoc /> */
        public void RecordLocal(IEvent evt)
        {
            throw new NotImplementedException("GG-10244");
        }

        /** <inheritDoc /> */
        public void LocalListen<T>(IEventFilter<T> listener, params int[] types) where T : IEvent
        {
            A.NotNull(listener, "listener");
            A.NotNullOrEmpty(types, "types");

            foreach (var type in types)
                LocalListen(listener, type);
        }

        /** <inheritDoc /> */
        public bool StopLocalListen<T>(IEventFilter<T> listener, params int[] types) where T : IEvent
        {
            lock (_localFilters)
            {
                Dictionary<int, LocalHandledEventFilter> filters;

                if (!_localFilters.TryGetValue(listener, out filters))
                    return false;

                var success = false;

                // Should do this inside lock to avoid race with subscription
                // ToArray is required because we are going to modify underlying dictionary during enumeration
                foreach (var filter in GetLocalFilters(listener, types).ToArray())
                    success |= UU.EventsStopLocalListen(target, filter.Handle);

                return success;
            }
        }

        /** <inheritDoc /> */
        public void EnableLocal(params int[] types)
        {
            A.NotNullOrEmpty(types, "types");

            DoOutOp((int)Op.ENABLE_LOCAL, writer => WriteEventTypes(types, writer));
        }

        /** <inheritDoc /> */
        public void DisableLocal(params int[] types)
        {
            A.NotNullOrEmpty(types, "types");

            DoOutOp((int)Op.DISABLE_LOCAL, writer => WriteEventTypes(types, writer));
        }

        /** <inheritDoc /> */
        public int[] GetEnabledEvents()
        {
            return DoInOp((int)Op.GET_ENABLED_EVENTS, reader => ReadEventTypes(reader));
        }

        /** <inheritDoc /> */
        public bool IsEnabled(int type)
        {
            return UU.EventsIsEnabled(target, type);
        }

        /// <summary>
        /// Waits for the specified events.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="filter">Optional filtering predicate. Event wait will end as soon as it returns false.</param>
        /// <param name="handle">The filter handle, if applicable.</param>
        /// <param name="types">Types of the events to wait for. 
        /// If not provided, all events will be passed to the filter.</param>
        /// <returns>Grid event.</returns>
        protected T WaitForLocal0<T>(IEventFilter<T> filter, ref long handle, params int[] types) where T : IEvent
        {
            if (filter != null)
                handle = Grid.HandleRegistry.Allocate(new LocalEventFilter
                {
                    InvokeFunc = stream => InvokeLocalFilter(stream, filter)
                });

            var hnd = handle;

            return DoOutInOp((int)Op.WAIT_FOR_LOCAL,
                writer =>
                {
                    if (filter != null)
                    {
                        writer.WriteBoolean(true);
                        writer.WriteLong(hnd);
                    }
                    else
                        writer.WriteBoolean(false);

                    WriteEventTypes(types, writer);
                },
                reader => EventReader.Read<T>(Marshaller.StartUnmarshal(reader)));
        }

        /// <summary>
        /// Reads events from a portable stream.
        /// </summary>
        /// <typeparam name="T">Event type.</typeparam>
        /// <param name="reader">Reader.</param>
        /// <returns>Resulting list or null.</returns>
        private List<T> ReadEvents<T>(IPortableStream reader) where T : IEvent
        {
            return ReadEvents<T>(Marshaller.StartUnmarshal(reader));
        }

        /// <summary>
        /// Reads events from a portable reader.
        /// </summary>
        /// <typeparam name="T">Event type.</typeparam>
        /// <param name="portableReader">Reader.</param>
        /// <returns>Resulting list or null.</returns>
        protected static List<T> ReadEvents<T>(PortableReaderImpl portableReader) where T : IEvent
        {
            var count = portableReader.RawReader().ReadInt();

            if (count == -1)
                return null;

            var result = new List<T>(count);

            for (var i = 0; i < count; i++)
                result.Add(EventReader.Read<T>(portableReader));

            return result;
        }

        /// <summary>
        /// Gets local filters by user listener and event type.
        /// </summary>
        /// <param name="listener">Listener.</param>
        /// <param name="types">Types.</param>
        /// <returns>Collection of local listener wrappers.</returns>
        [SuppressMessage("ReSharper", "InconsistentlySynchronizedField",
            Justification = "This private method should be always called within a lock on localFilters")]
        private IEnumerable<LocalHandledEventFilter> GetLocalFilters(object listener, int[] types)
        {
            Dictionary<int, LocalHandledEventFilter> filters;

            if (!_localFilters.TryGetValue(listener, out filters))
                return Enumerable.Empty<LocalHandledEventFilter>();

            if (types.Length == 0)
                return filters.Values;

            return types.Select(type =>
            {
                LocalHandledEventFilter filter;

                return filters.TryGetValue(type, out filter) ? filter : null;
            }).Where(x => x != null);
        }

        /// <summary>
        /// Adds an event listener for local events.
        /// </summary>
        /// <typeparam name="T">Type of events.</typeparam>
        /// <param name="listener">Predicate that is called on each received event.</param>
        /// <param name="type">Event type for which this listener will be notified</param>
        private void LocalListen<T>(IEventFilter<T> listener, int type) where T : IEvent
        {
            lock (_localFilters)
            {
                Dictionary<int, LocalHandledEventFilter> filters;

                if (!_localFilters.TryGetValue(listener, out filters))
                {
                    filters = new Dictionary<int, LocalHandledEventFilter>();

                    _localFilters[listener] = filters;
                }

                LocalHandledEventFilter localFilter;

                if (!filters.TryGetValue(type, out localFilter))
                {
                    localFilter = CreateLocalFilter(listener, type);

                    filters[type] = localFilter;
                }

                UU.EventsLocalListen(target, localFilter.Handle, type);
            }
        }

        /// <summary>
        /// Creates a user filter wrapper.
        /// </summary>
        /// <typeparam name="T">Event object type.</typeparam>
        /// <param name="listener">Listener.</param>
        /// <param name="type">Event type.</param>
        /// <returns>Created wrapper.</returns>
        private LocalHandledEventFilter CreateLocalFilter<T>(IEventFilter<T> listener, int type) where T : IEvent
        {
            var result = new LocalHandledEventFilter(
                stream => InvokeLocalFilter(stream, listener),
                unused =>
                {
                    lock (_localFilters)
                    {
                        Dictionary<int, LocalHandledEventFilter> filters;

                        if (_localFilters.TryGetValue(listener, out filters))
                        {
                            filters.Remove(type);

                            if (filters.Count == 0)
                                _localFilters.Remove(listener);
                        }
                    }
                });

            result.Handle = Grid.HandleRegistry.Allocate(result);

            return result;
        }

        /// <summary>
        /// Invokes local filter using data from specified stream.
        /// </summary>
        /// <typeparam name="T">Event object type.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <param name="listener">The listener.</param>
        /// <returns>Filter invocation result.</returns>
        private bool InvokeLocalFilter<T>(IPortableStream stream, IEventFilter<T> listener) where T : IEvent
        {
            var evt = EventReader.Read<T>(Marshaller.StartUnmarshal(stream));

            // No guid in local mode
            return listener.Invoke(Guid.Empty, evt);
        }

        /// <summary>
        /// Writes the event types.
        /// </summary>
        /// <param name="types">Types.</param>
        /// <param name="writer">Writer.</param>
        private static void WriteEventTypes(int[] types, IPortableRawWriter writer)
        {
            if (types.Length == 0)
                types = null;  // empty array means no type filtering

            writer.WriteIntArray(types);
        }

        /// <summary>
        /// Writes the event types.
        /// </summary>
        /// <param name="reader">Reader.</param>
        private int[] ReadEventTypes(IPortableStream reader)
        {
            return Marsh.StartUnmarshal(reader).ReadIntArray();
        }

        /// <summary>
        /// Local user filter wrapper.
        /// </summary>
        private class LocalEventFilter : IInteropCallback
        {
            /** */
            public Func<IPortableStream, bool> InvokeFunc;

            /** <inheritdoc /> */
            public int Invoke(IPortableStream stream)
            {
                return InvokeFunc(stream) ? 1 : 0;
            }
        }

        /// <summary>
        /// Local user filter wrapper with handle.
        /// </summary>
        private class LocalHandledEventFilter : Handle<Func<IPortableStream, bool>>, IInteropCallback
        {
            /** */
            public long Handle;

            /** <inheritdoc /> */
            public int Invoke(IPortableStream stream)
            {
                return Target(stream) ? 1 : 0;
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="LocalHandledEventFilter"/> class.
            /// </summary>
            /// <param name="invokeFunc">The invoke function.</param>
            /// <param name="releaseAction">The release action.</param>
            public LocalHandledEventFilter(
                Func<IPortableStream, bool> invokeFunc, Action<Func<IPortableStream, bool>> releaseAction) 
                : base(invokeFunc, releaseAction)
            {
                // No-op.
            }
        }
    }
}
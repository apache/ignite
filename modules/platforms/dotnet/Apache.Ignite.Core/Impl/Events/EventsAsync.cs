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
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Async Ignite events.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class EventsAsync : Events
    {
        /** */
        private readonly ThreadLocal<int> _lastAsyncOp = new ThreadLocal<int>(() => OpNone);

        /** */
        private readonly ThreadLocal<IFuture> _curFut = new ThreadLocal<IFuture>();

        /// <summary>
        /// Initializes a new instance of the <see cref="Events"/> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="clusterGroup">Cluster group.</param>
        public EventsAsync(IUnmanagedTarget target, PortableMarshaller marsh, IClusterGroup clusterGroup)
            : base(target, marsh, clusterGroup)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public override List<T> RemoteQuery<T>(IEventFilter<T> filter, TimeSpan? timeout = null, params int[] types)
        {
            _lastAsyncOp.Value = (int) Op.RemoteQuery;

            var result = base.RemoteQuery(filter, timeout, types);

            // Result is a List<T> so we can't create proper converter later in GetFuture call from user.
            // ReSharper disable once RedundantTypeArgumentsOfMethod (otherwise won't compile in VS2010 / TC)
            _curFut.Value = GetFuture<List<T>>((futId, futTyp) => UU.TargetListenFutureForOperation(Target, futId, futTyp,
                (int) Op.RemoteQuery), convertFunc: ReadEvents<T>);

            return result;
        }

        /** <inheritdoc /> */
        public override Guid RemoteListen<T>(int bufSize = 1, TimeSpan? interval = null, bool autoUnsubscribe = true,
            IEventFilter<T> localListener = null, IEventFilter<T> remoteFilter = null, params int[] types)
        {
            _lastAsyncOp.Value = (int) Op.RemoteListen;
            _curFut.Value = null;

            return base.RemoteListen(bufSize, interval, autoUnsubscribe, localListener, remoteFilter, types);
        }

        /** <inheritdoc /> */
        public override void StopRemoteListen(Guid opId)
        {
            _lastAsyncOp.Value = (int) Op.StopRemoteListen;
            _curFut.Value = null;

            base.StopRemoteListen(opId);
        }

        /** <inheritdoc /> */
        public override T WaitForLocal<T>(IEventFilter<T> filter, params int[] types)
        {
            _lastAsyncOp.Value = (int) Op.WaitForLocal;

            long hnd = 0;

            try
            {
                var result = WaitForLocal0(filter, ref hnd, types);

                if (filter != null)
                {
                    // Dispose handle as soon as future ends.
                    var fut = GetFuture<T>();

                    _curFut.Value = fut;

                    fut.Listen(() => Ignite.HandleRegistry.Release(hnd));
                }
                else
                    _curFut.Value = null;

                return result;
            }
            catch (Exception)
            {
                Ignite.HandleRegistry.Release(hnd);
                throw;
            }
        }

        /** <inheritdoc /> */
        public override IEvents WithAsync()
        {
            return this;
        }

        /** <inheritdoc /> */
        public override bool IsAsync
        {
            get { return true; }
        }

        /** <inheritdoc /> */
        public override IFuture GetFuture()
        {
            return GetFuture<object>();
        }

        /** <inheritdoc /> */
        public override IFuture<T> GetFuture<T>()
        {
            if (_curFut.Value != null)
            {
                var fut = _curFut.Value;
                _curFut.Value = null;
                return (IFuture<T>) fut;
            }

            Func<PortableReaderImpl, T> converter = null;

            if (_lastAsyncOp.Value == (int) Op.WaitForLocal)
                converter = reader => (T) EventReader.Read<IEvent>(reader);

            return GetFuture((futId, futTyp) => UU.TargetListenFutureForOperation(Target, futId, futTyp, _lastAsyncOp.Value),
                convertFunc: converter);
        }
    }
}
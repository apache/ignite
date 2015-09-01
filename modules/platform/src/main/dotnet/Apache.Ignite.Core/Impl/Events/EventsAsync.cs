/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Events
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using Apache.Ignite.Core.Common;
    using GridGain.Cluster;
    using GridGain.Common;
    using GridGain.Events;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Unmanaged;

    using UU = GridGain.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Async grid events.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class EventsAsync : Events
    {
        /** */
        private readonly ThreadLocal<int> lastAsyncOp = new ThreadLocal<int>(() => OP_NONE);

        /** */
        private readonly ThreadLocal<IFuture> curFut = new ThreadLocal<IFuture>();

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
            lastAsyncOp.Value = (int) Op.REMOTE_QUERY;

            var result = base.RemoteQuery(filter, timeout, types);

            // Result is a List<T> so we can't create proper converter later in GetFuture call from user.
            // ReSharper disable once RedundantTypeArgumentsOfMethod (otherwise won't compile in VS2010 / TC)
            curFut.Value = GetFuture<List<T>>((futId, futTyp) => UU.TargetListenFutureForOperation(target, futId, futTyp,
                (int) Op.REMOTE_QUERY), convertFunc: ReadEvents<T>);

            return result;
        }

        /** <inheritdoc /> */
        public override Guid RemoteListen<T>(int bufSize = 1, TimeSpan? interval = null, bool autoUnsubscribe = true,
            IEventFilter<T> localListener = null, IEventFilter<T> remoteFilter = null, params int[] types)
        {
            lastAsyncOp.Value = (int) Op.REMOTE_LISTEN;
            curFut.Value = null;

            return base.RemoteListen(bufSize, interval, autoUnsubscribe, localListener, remoteFilter, types);
        }

        /** <inheritdoc /> */
        public override void StopRemoteListen(Guid opId)
        {
            lastAsyncOp.Value = (int) Op.STOP_REMOTE_LISTEN;
            curFut.Value = null;

            base.StopRemoteListen(opId);
        }

        /** <inheritdoc /> */
        public override T WaitForLocal<T>(IEventFilter<T> filter, params int[] types)
        {
            lastAsyncOp.Value = (int) Op.WAIT_FOR_LOCAL;

            long hnd = 0;

            try
            {
                var result = WaitForLocal0(filter, ref hnd, types);

                if (filter != null)
                {
                    // Dispose handle as soon as future ends.
                    var fut = GetFuture<T>();

                    curFut.Value = fut;

                    fut.Listen(() => grid.HandleRegistry.Release(hnd));
                }
                else
                    curFut.Value = null;

                return result;
            }
            catch (Exception)
            {
                grid.HandleRegistry.Release(hnd);
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
            if (curFut.Value != null)
            {
                var fut = curFut.Value;
                curFut.Value = null;
                return (IFuture<T>) fut;
            }

            Func<PortableReaderImpl, T> converter = null;

            if (lastAsyncOp.Value == (int) Op.WAIT_FOR_LOCAL)
                converter = reader => (T) EventReader.Read<IEvent>(reader);

            return GetFuture((futId, futTyp) => UU.TargetListenFutureForOperation(target, futId, futTyp, lastAsyncOp.Value),
                convertFunc: converter);
        }
    }
}
/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Messaging
{
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Messaging;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Async messaging implementation.
    /// </summary>
    internal class MessagingAsync : Messaging
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MessagingAsync" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="prj">Cluster group.</param>
        public MessagingAsync(IUnmanagedTarget target, PortableMarshaller marsh, 
            IClusterGroup prj) : base(target, marsh, prj)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public override IMessaging WithAsync()
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
            return GetFuture<T>((futId, futTyp) => UU.TargetListenFuture(target, futId, futTyp));
        }
    }
}
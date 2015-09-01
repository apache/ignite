/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Services
{
    using Apache.Ignite.Core.Common;
    using GridGain.Cluster;
    using GridGain.Common;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Unmanaged;
    using GridGain.Services;

    using UU = GridGain.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Async services implementation.
    /// </summary>
    internal class ServicesAsync : Services
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ServicesAsync" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="clusterGroup">Cluster group.</param>
        /// <param name="keepPortable">Portable flag.</param>
        /// <param name="srvKeepPortable">Server portable flag.</param>
        public ServicesAsync(IUnmanagedTarget target, PortableMarshaller marsh, IClusterGroup clusterGroup,
            bool keepPortable, bool srvKeepPortable)
            : base(target, marsh, clusterGroup, keepPortable, srvKeepPortable)
        {
            // No-op
        }

        /** <inheritDoc /> */
        public override bool IsAsync
        {
            get { return true; }
        }

        /** <inheritDoc /> */
        public override IServices WithKeepPortable()
        {
            if (keepPortable)
                return this;

            return new ServicesAsync(target, marsh, clusterGroup, true, srvKeepPortable);
        }

        /** <inheritDoc /> */
        public override IServices WithServerKeepPortable()
        {
            if (srvKeepPortable)
                return this;

            return new ServicesAsync(target, marsh, clusterGroup, keepPortable, true);
        }

        /** <inheritDoc /> */
        public override IServices WithAsync()
        {
            return this;
        }

        /** <inheritDoc /> */
        public override IFuture GetFuture()
        {
            return GetFuture<object>();
        }

        /** <inheritDoc /> */
        public override IFuture<T> GetFuture<T>()
        {
            return GetFuture<T>((futId, futTyp) => UU.TargetListenFuture(target, futId, futTyp));
        }
    }
}
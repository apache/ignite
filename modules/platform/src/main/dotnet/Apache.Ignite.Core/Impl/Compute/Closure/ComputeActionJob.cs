/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Compute
{
    using System;

    using GridGain.Compute;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Resource;
    using GridGain.Portable;

    /// <summary>
    /// System job which wraps over <c>Action</c>.
    /// </summary>
    internal class ComputeActionJob : IComputeJob, IComputeResourceInjector, IPortableWriteAware
    {
        /** Closure. */
        private readonly IComputeAction action;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="action">Action.</param>
        public ComputeActionJob(IComputeAction action)
        {
            this.action = action;
        }

        /** <inheritDoc /> */
        public object Execute()
        {
            action.Invoke();
            
            return null;
        }

        /** <inheritDoc /> */
        public void Cancel()
        {
            throw new NotSupportedException("Func job cannot be cancelled.");
        }

        /** <inheritDoc /> */
        public void Inject(GridImpl grid)
        {
            ResourceProcessor.Inject(action, grid);
        }

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var writer0 = (PortableWriterImpl)writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, action);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeActionJob"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ComputeActionJob(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl)reader.RawReader();

            action = PortableUtils.ReadPortableOrSerializable<IComputeAction>(reader0);
        }
    }
}
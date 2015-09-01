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
    /// System job which wraps over <c>Func</c>.
    /// </summary>
    internal class ComputeOutFuncJob : IComputeJob, IComputeResourceInjector, IPortableWriteAware
    {
        /** Closure. */
        private readonly IComputeOutFunc clo;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="clo">Closure.</param>
        public ComputeOutFuncJob(IComputeOutFunc clo)
        {
            this.clo = clo;
        }

        /** <inheritDoc /> */
        public object Execute()
        {
            return clo.Invoke();
        }

        /** <inheritDoc /> */
        public void Cancel()
        {
            throw new NotSupportedException("Func job cannot be cancelled.");
        }

        /** <inheritDoc /> */
        public void Inject(GridImpl grid)
        {
            ResourceProcessor.Inject(clo, grid);
        }

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var writer0 = (PortableWriterImpl) writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, clo);
        }

        public ComputeOutFuncJob(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl) reader.RawReader();

            clo = PortableUtils.ReadPortableOrSerializable<IComputeOutFunc>(reader0);
        }
    }
}

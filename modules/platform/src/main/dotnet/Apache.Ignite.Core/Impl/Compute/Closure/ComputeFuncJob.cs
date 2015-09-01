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
    internal class ComputeFuncJob : IComputeJob, IComputeResourceInjector, IPortableWriteAware
    {
        /** Closure. */
        private readonly IComputeFunc clo;

        /** Argument. */
        private readonly object arg;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="clo">Closure.</param>
        /// <param name="arg">Argument.</param>
        public ComputeFuncJob(IComputeFunc clo, object arg)
        {
            this.clo = clo;
            this.arg = arg;
        }

        /** <inheritDoc /> */
        public object Execute()
        {
            return clo.Invoke(arg);
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
            PortableWriterImpl writer0 = (PortableWriterImpl) writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, clo);

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, arg);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeFuncJob"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ComputeFuncJob(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl) reader.RawReader();
            
            clo = PortableUtils.ReadPortableOrSerializable<IComputeFunc>(reader0);
            arg = PortableUtils.ReadPortableOrSerializable<object>(reader0);
        }
    }
}

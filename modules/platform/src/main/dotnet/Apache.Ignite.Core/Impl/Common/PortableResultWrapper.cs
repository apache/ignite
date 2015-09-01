/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Common
{
    using GridGain.Impl.Portable;
    using GridGain.Portable;

    /// <summary>
    /// Simple wrapper over result to handle marshalling properly.
    /// </summary>
    internal class PortableResultWrapper : IPortableWriteAware
    {
        /** */
        private readonly object result;

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableResultWrapper"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public PortableResultWrapper(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl)reader.RawReader();

            result = PortableUtils.ReadPortableOrSerializable<object>(reader0);
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="res">Result.</param>
        public PortableResultWrapper(object res)
        {
            result = res;
        }

        /// <summary>
        /// Result.
        /// </summary>
        public object Result
        {
            get { return result; }
        }

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var writer0 = (PortableWriterImpl) writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, Result);
        }
    }
}

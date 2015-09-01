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
    using System;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using GridGain.Impl.Portable;

    /// <summary>
    /// Marshals and converts future value.
    /// </summary>
    internal class FutureConverter<T> : IFutureConverter<T>
    {
        /** Marshaller. */
        private readonly PortableMarshaller marsh;

        /** Keep portable flag. */
        private readonly bool keepPortable;

        /** Converting function. */
        private readonly Func<PortableReaderImpl, T> func;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepPortable">Keep portable.</param>
        /// <param name="func">Converting function.</param>
        public FutureConverter(PortableMarshaller marsh, bool keepPortable,
            Func<PortableReaderImpl, T> func = null)
        {
            this.marsh = marsh;
            this.keepPortable = keepPortable;
            this.func = func ?? (reader => reader.ReadObject<T>());
        }

        /// <summary>
        /// Read and convert a value.
        /// </summary>
        public T Convert(IPortableStream stream)
        {
            var reader = marsh.StartUnmarshal(stream, keepPortable);

            return func(reader);
        }
    }
}

/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Marshals and converts future value.
    /// </summary>
    internal class FutureConverter<T> : IFutureConverter<T>
    {
        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Keep binary flag. */
        private readonly bool _keepBinary;

        /** Converting function. */
        private readonly Func<BinaryReader, T> _func;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        /// <param name="func">Converting function.</param>
        public FutureConverter(Marshaller marsh, bool keepBinary,
            Func<BinaryReader, T> func = null)
        {
            _marsh = marsh;
            _keepBinary = keepBinary;
            _func = func ?? (reader => reader == null ? default(T) : reader.ReadObject<T>());
        }

        /// <summary>
        /// Read and convert a value.
        /// </summary>
        public T Convert(IBinaryStream stream)
        {
            var reader = stream == null ? null : _marsh.StartUnmarshal(stream, _keepBinary);

            return _func(reader);
        }
    }
}

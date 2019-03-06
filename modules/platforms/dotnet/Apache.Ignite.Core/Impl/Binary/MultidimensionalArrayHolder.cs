/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;
    using System.Collections;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Deployment;

    /// <summary>
    /// Wrapper for multidimensional arrays (int[,] -style).
    /// <para />
    /// Jagged arrays (int[][]) are fully supported by the engine and are interoperable with Java.
    /// However, there is no int[,]-style arrays in Java, and there is no way to support them in a generic way.
    /// So we have to wrap them inside an object (so it looks like a BinaryObject in Java).
    /// </summary>
    internal sealed class MultidimensionalArrayHolder : IBinaryWriteAware
    {
        /** Object. */
        private readonly Array _array;

        /// <summary>
        /// Initializes a new instance of the <see cref="PeerLoadingObjectHolder"/> class.
        /// </summary>
        /// <param name="o">The object.</param>
        public MultidimensionalArrayHolder(Array o)
        {
            Debug.Assert(o != null);
            Debug.Assert(o.Rank > 1);

            _array = o;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultidimensionalArrayHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public MultidimensionalArrayHolder(BinaryReader reader)
        {
            var typeId = reader.ReadInt();
            var type = BinaryUtils.GetArrayElementType(typeId, reader.Marshaller);

            var rank = reader.ReadInt();
            var lengths = new int[rank];
            var totalLen = 1;

            for (var i = 0; i < rank; i++)
            {
                var len = reader.ReadInt();
                lengths[i] = len;
                totalLen *= len;
            }

            _array = System.Array.CreateInstance(type, lengths);

            for (var i = 0; i < totalLen; i++)
            {
                var obj = reader.ReadObject<object>();
                var idx = GetIndices(i, lengths);

                _array.SetValue(Convert.ChangeType(obj, type), idx);
            }
        }

        /// <summary>
        /// Gets the indices in a multidimensional array from a global index.
        /// </summary>
        private static int[] GetIndices(int globalIdx, int[] lengths)
        {
            var res = new int[lengths.Length];

            for (var i = lengths.Length - 1; i >= 0; i--)
            {
                var len = lengths[i];

                res[i] = globalIdx % len;
                globalIdx = globalIdx / len;
            }

            return res;
        }

        /// <summary>
        /// Gets the object.
        /// </summary>
        public object Array
        {
            get { return _array; }
        }

        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var raw = writer.GetRawWriter();

            // Array type.
            raw.WriteInt(BinaryUtils.GetArrayElementTypeId(_array, ((BinaryWriter) writer).Marshaller));

            // Number of dimensions.
            var rank = _array.Rank;
            raw.WriteInt(rank);

            // Sizes per dimensions.
            for (var i = 0; i < rank; i++)
            {
                raw.WriteInt(_array.GetLength(i));
            }

            // Data.
            foreach (var obj in (IEnumerable)_array)
            {
                raw.WriteObject(obj);
            }
        }
    }
}

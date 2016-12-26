/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Binary
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Compares binary object equality using underlying byte array.
    /// </summary>
    public class BinaryArrayEqualityComparer : IEqualityComparer<IBinaryObject>, IBinaryEqualityComparer,
        IBinaryStreamProcessor<KeyValuePair<int,int>, int>
    {
        /// <summary>
        /// Determines whether the specified objects are equal.
        /// </summary>
        /// <param name="x">The first object to compare.</param>
        /// <param name="y">The second object to compare.</param>
        /// <returns>
        /// true if the specified objects are equal; otherwise, false.
        /// </returns>
        public bool Equals(IBinaryObject x, IBinaryObject y)
        {
            throw new NotSupportedException(GetType() + "is not intended for direct usage.");
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public int GetHashCode(IBinaryObject obj)
        {
            throw new NotSupportedException(GetType() + "is not intended for direct usage.");
        }

        /** <inheritdoc /> */
        int IBinaryEqualityComparer.GetHashCode(IBinaryStream stream, int startPos, int length, 
            BinaryObjectSchemaHolder schema, int schemaId, Marshaller marshaller, IBinaryTypeDescriptor desc)
        {
            Debug.Assert(stream != null);
            Debug.Assert(startPos >= 0);
            Debug.Assert(length >= 0);

            var arg = new KeyValuePair<int, int>(startPos, length);

            return stream.Apply(this, arg);
        }

        /** <inheritdoc /> */
        unsafe int IBinaryStreamProcessor<KeyValuePair<int, int>, int>.Invoke(byte* data, KeyValuePair<int, int> arg)
        {
            var hash = 1;
            var ptr = data + arg.Key;

            for (var i = 0; i < arg.Value; i++)
                hash = 31 * hash + *(ptr + i);

            return hash;
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Table
{
    using System;
    using Internal.Common;

    /// <summary>
    /// Ignite Tuple.
    /// <para />
    /// Default implementation is <see cref="IgniteTuple"/>.
    /// </summary>
    public interface IIgniteTuple
    {
        /// <summary>
        /// Gets the number of columns.
        /// </summary>
        int FieldCount { get; }

        /// <summary>
        /// Gets the value of the specified column as an object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        object? this[int ordinal] { get; set; }

        /// <summary>
        /// Gets the value of the specified column as an object.
        /// </summary>
        /// <param name="name">The column name.</param>
        object? this[string name] { get; set; }

        /// <summary>
        /// Gets the name of the column, given the zero-based column ordinal.
        /// </summary>
        /// <param name="ordinal">Zero-based column ordinal.</param>
        /// <returns>Column name.</returns>
        string GetName(int ordinal);

        /// <summary>
        /// Gets the column ordinal given the name of the column,
        /// or -1 when the column with the given name does not exist.
        /// </summary>
        /// <param name="name">Column name.</param>
        /// <returns>Column ordinal, or -1 when the column with the given name does not exist.</returns>
        int GetOrdinal(string name);

        /// <summary>
        /// Gets a hash code for the current tuple according to column names and values.
        /// </summary>
        /// <param name="tuple">Tuple.</param>
        /// <returns>A hash code for the specified tuple.</returns>
        public static int GetHashCode(IIgniteTuple tuple)
        {
            IgniteArgumentCheck.NotNull(tuple, nameof(tuple));

            var hash = default(HashCode);

            for (int i = 0; i < tuple.FieldCount; i++)
            {
                var name = tuple.GetName(i);
                var val = tuple[i];

                hash.Add(name);
                hash.Add(val);
            }

            return hash.ToHashCode();
        }

        /// <summary>
        /// Determines whether the specified object instances are considered equal.
        /// </summary>
        /// <param name="tuple1">The first tuple to compare.</param>
        /// <param name="tuple2">The second tuple to compare.</param>
        /// <returns>
        /// <see langword="true" /> if the tuples are considered equal; otherwise, <see langword="false" />.
        /// If both <paramref name="tuple1" /> and <paramref name="tuple1" /> are null, the method returns <see langword="true" />.
        /// </returns>
        public static bool Equals(IIgniteTuple? tuple1, IIgniteTuple? tuple2)
        {
            if (tuple1 == null)
            {
                return tuple2 == null;
            }

            if (tuple2 == null)
            {
                return false;
            }

            if (ReferenceEquals(tuple1, tuple2))
            {
                return true;
            }

            if (tuple1.FieldCount != tuple2.FieldCount)
            {
                return false;
            }

            for (int i = 0; i < tuple1.FieldCount; i++)
            {
                if (tuple1.GetName(i) != tuple2.GetName(i))
                {
                    return false;
                }

                if (!Equals(tuple1[i], tuple2[i]))
                {
                    return false;
                }
            }

            return true;
        }
    }
}

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
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Ignite tuple.
    /// </summary>
    public sealed class IgniteTuple : IIgniteTuple
    {
        /** Key-value pairs. */
        private readonly List<(string Key, object? Value)> _pairs;

        /** Column index map. */
        private readonly Dictionary<string, int> _indexes;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteTuple"/> class.
        /// </summary>
        /// <param name="capacity">Capacity.</param>
        public IgniteTuple(int capacity = 16)
        {
            _pairs = new List<(string, object?)>(capacity);
            _indexes = new Dictionary<string, int>(capacity);
        }

        /// <inheritdoc/>
        public int FieldCount => _pairs.Count;

        /// <inheritdoc/>
        public object? this[int ordinal]
        {
            get => _pairs[ordinal].Value;
            set => _pairs[ordinal] = (_pairs[ordinal].Key, value);
        }

        /// <inheritdoc/>
        public object? this[string name]
        {
            get => _pairs[_indexes[name]].Value;
            set
            {
                var pair = (name, value);

                if (_indexes.TryGetValue(name, out var index))
                {
                    _pairs[index] = pair;
                }
                else
                {
                    index = FieldCount;
                    _indexes[name] = index;
                    _pairs.Add(pair);
                }
            }
        }

        /// <inheritdoc/>
        public string GetName(int ordinal) => _pairs[ordinal].Key;

        /// <inheritdoc/>
        public int GetOrdinal(string name) => _indexes.TryGetValue(name, out var index) ? index : -1;

        /// <inheritdoc />
        public override string ToString()
        {
            var sb = new StringBuilder();

            sb.Append(nameof(IgniteTuple)).Append(" [");

            for (var i = 0; i < FieldCount; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }

                sb.Append(GetName(i)).Append('=').Append(this[i]);
            }

            sb.Append(']');

            return sb.ToString();
        }
    }
}

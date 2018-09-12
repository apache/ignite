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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Client;

    /// <summary>
    /// Writer extensions.
    /// </summary>
    internal static class BinaryWriterExtensions
    {
        /// <summary>
        /// Writes the nullable boolean.
        /// </summary>
        public static void WriteBooleanNullable(this IBinaryRawWriter writer, bool? value)
        {
            if (value != null)
            {
                writer.WriteBoolean(true);
                writer.WriteBoolean(value.Value);
            }
            else
                writer.WriteBoolean(false);
        }

        /// <summary>
        /// Writes the nullable boolean.
        /// </summary>
        public static void WriteIntNullable(this IBinaryRawWriter writer, int? value)
        {
            if (value != null)
            {
                writer.WriteBoolean(true);
                writer.WriteInt(value.Value);
            }
            else
                writer.WriteBoolean(false);
        }

        /// <summary>
        /// Writes the timespan.
        /// </summary>
        public static void WriteTimeSpanAsLong(this IBinaryRawWriter writer, TimeSpan value)
        {
            writer.WriteLong((long) value.TotalMilliseconds);
        }

        /// <summary>
        /// Writes the nullable boolean.
        /// </summary>
        public static void WriteTimeSpanAsLongNullable(this IBinaryRawWriter writer, TimeSpan? value)
        {
            if (value != null)
            {
                writer.WriteBoolean(true);
                writer.WriteTimeSpanAsLong(value.Value);
            }
            else
                writer.WriteBoolean(false);
        }

        /// <summary>
        /// Write collection.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="vals">Values.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <returns>The same writer for chaining.</returns>
        private static void WriteCollection<T1, T2>(this BinaryWriter writer, ICollection<T1> vals, 
            Func<T1, T2> selector)
        {
            writer.WriteInt(vals.Count);

            if (selector == null)
            {
                foreach (var val in vals)
                    writer.WriteObjectDetached(val);
            }
            else
            {
                foreach (var val in vals)
                    writer.WriteObjectDetached(selector(val));
            }
        }

        /// <summary>
        /// Write enumerable.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="vals">Values.</param>
        /// <returns>The same writer for chaining.</returns>
        public static void WriteEnumerable<T>(this BinaryWriter writer, IEnumerable<T> vals)
        {
            WriteEnumerable<T, T>(writer, vals, null);
        }

        /// <summary>
        /// Write enumerable.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="vals">Values.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <returns>The same writer for chaining.</returns>
        public static void WriteEnumerable<T1, T2>(this BinaryWriter writer, IEnumerable<T1> vals, 
            Func<T1, T2> selector)
        {
            var col = vals as ICollection<T1>;

            if (col != null)
            {
                WriteCollection(writer, col, selector);
                return;
            }

            var stream = writer.Stream;

            var pos = stream.Position;

            stream.Seek(4, SeekOrigin.Current);

            var size = 0;

            if (selector == null)
            {
                foreach (var val in vals)
                {
                    writer.WriteObjectDetached(val);

                    size++;
                }
            }
            else
            {
                foreach (var val in vals)
                {
                    writer.WriteObjectDetached(selector(val));

                    size++;
                }
            }

            stream.WriteInt(pos, size);
        }

        /// <summary>
        /// Write dictionary.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="vals">Values.</param>
        public static void WriteDictionary<T1, T2>(this BinaryWriter writer, IEnumerable<KeyValuePair<T1, T2>> vals)
        {
            var pos = writer.Stream.Position;
            writer.WriteInt(0);  // Reserve count.

            int cnt = 0;

            foreach (var pair in vals)
            {
                writer.WriteObjectDetached(pair.Key);
                writer.WriteObjectDetached(pair.Value);

                cnt++;
            }

            writer.Stream.WriteInt(pos, cnt);
        }

        /// <summary>
        /// Writes the collection of write-aware items.
        /// </summary>
        public static void WriteCollectionRaw<T, TWriter>(this TWriter writer, ICollection<T> collection)
            where T : IBinaryRawWriteAware<TWriter> where TWriter: IBinaryRawWriter
        {
            Debug.Assert(writer != null);

            if (collection != null)
            {
                writer.WriteInt(collection.Count);

                foreach (var x in collection)
                {
                    if (x == null)
                    {
                        throw new ArgumentNullException(string.Format("{0} can not be null", typeof(T).Name));
                    }

                    x.Write(writer);
                }
            }
            else
            {
                writer.WriteInt(0);
            }
        }

        /// <summary>
        /// Writes the collection of write-aware-ex items.
        /// </summary>
        public static void WriteCollectionRaw<T, TWriter>(this TWriter writer, ICollection<T> collection,
            ClientProtocolVersion srvVer) where T : IBinaryRawWriteAwareEx<TWriter> where TWriter: IBinaryRawWriter
        {
            Debug.Assert(writer != null);

            if (collection != null)
            {
                writer.WriteInt(collection.Count);

                foreach (var x in collection)
                {
                    if (x == null)
                    {
                        throw new ArgumentNullException(string.Format("{0} can not be null", typeof(T).Name));
                    }

                    x.Write(writer, srvVer);
                }
            }
            else
            {
                writer.WriteInt(0);
            }
        }
    }
}

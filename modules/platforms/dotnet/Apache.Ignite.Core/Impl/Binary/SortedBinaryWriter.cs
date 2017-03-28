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
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Wrapper over <see cref="BinaryWriter"/> which ensures that fields are written in alphabetic order.
    /// </summary>
    internal class SortedBinaryWriter : IBinaryWriter
    {
        /** */
        private readonly IBinaryWriter _writer;

        /** */
        private SortedDictionary<string, Action<IBinaryWriter, string>> _writeActions;

        /// <summary>
        /// Initializes a new instance of the <see cref="SortedBinaryWriter"/> class.
        /// </summary>
        /// <param name="writer">The writer.</param>
        public SortedBinaryWriter(IBinaryWriter writer)
        {
            Debug.Assert(writer != null);

            _writer = writer;
        }

        /// <summary>
        /// Gets the inner writer.
        /// </summary>
        public IBinaryWriter Writer
        {
            get { return _writer; }
        }

        /** <inheritDoc /> */
        public void WriteByte(string fieldName, byte val)
        {
            AddAction(fieldName, (w, f) => w.WriteByte(f, val));
        }

        /** <inheritDoc /> */
        public void WriteByteArray(string fieldName, byte[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteByteArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteChar(string fieldName, char val)
        {
            AddAction(fieldName, (w, f) => w.WriteChar(f, val));
        }

        /** <inheritDoc /> */
        public void WriteCharArray(string fieldName, char[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteCharArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteShort(string fieldName, short val)
        {
            AddAction(fieldName, (w, f) => w.WriteShort(f, val));
        }

        /** <inheritDoc /> */
        public void WriteShortArray(string fieldName, short[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteShortArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteInt(string fieldName, int val)
        {
            AddAction(fieldName, (w, f) => w.WriteInt(f, val));
        }

        /** <inheritDoc /> */
        public void WriteIntArray(string fieldName, int[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteIntArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteLong(string fieldName, long val)
        {
            AddAction(fieldName, (w, f) => w.WriteLong(f, val));
        }

        /** <inheritDoc /> */
        public void WriteLongArray(string fieldName, long[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteLongArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteBoolean(string fieldName, bool val)
        {
            AddAction(fieldName, (w, f) => w.WriteBoolean(f, val));
        }

        /** <inheritDoc /> */
        public void WriteBooleanArray(string fieldName, bool[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteBooleanArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteFloat(string fieldName, float val)
        {
            AddAction(fieldName, (w, f) => w.WriteFloat(f, val));
        }

        /** <inheritDoc /> */
        public void WriteFloatArray(string fieldName, float[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteFloatArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteDouble(string fieldName, double val)
        {
            AddAction(fieldName, (w, f) => w.WriteDouble(f, val));
        }

        /** <inheritDoc /> */
        public void WriteDoubleArray(string fieldName, double[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteDoubleArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteDecimal(string fieldName, decimal? val)
        {
            AddAction(fieldName, (w, f) => w.WriteDecimal(f, val));
        }

        /** <inheritDoc /> */
        public void WriteDecimalArray(string fieldName, decimal?[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteDecimalArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteTimestamp(string fieldName, DateTime? val)
        {
            AddAction(fieldName, (w, f) => w.WriteTimestamp(f, val));
        }

        /** <inheritDoc /> */
        public void WriteTimestampArray(string fieldName, DateTime?[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteTimestampArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteString(string fieldName, string val)
        {
            AddAction(fieldName, (w, f) => w.WriteString(f, val));
        }

        /** <inheritDoc /> */
        public void WriteStringArray(string fieldName, string[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteStringArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteGuid(string fieldName, Guid? val)
        {
            AddAction(fieldName, (w, f) => w.WriteGuid(f, val));
        }

        /** <inheritDoc /> */
        public void WriteGuidArray(string fieldName, Guid?[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteGuidArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteEnum<T>(string fieldName, T val)
        {
            AddAction(fieldName, (w, f) => w.WriteEnum(f, val));
        }

        /** <inheritDoc /> */
        public void WriteEnumArray<T>(string fieldName, T[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteEnumArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteObject<T>(string fieldName, T val)
        {
            AddAction(fieldName, (w, f) => w.WriteObject(f, val));
        }

        /** <inheritDoc /> */
        public void WriteArray<T>(string fieldName, T[] val)
        {
            AddAction(fieldName, (w, f) => w.WriteArray(f, val));
        }

        /** <inheritDoc /> */
        public void WriteCollection(string fieldName, ICollection val)
        {
            AddAction(fieldName, (w, f) => w.WriteCollection(f, val));
        }

        /** <inheritDoc /> */
        public void WriteDictionary(string fieldName, IDictionary val)
        {
            AddAction(fieldName, (w, f) => w.WriteDictionary(f, val));
        }

        /** <inheritDoc /> */
        public IBinaryRawWriter GetRawWriter()
        {
            Flush();

            return _writer.GetRawWriter();
        }

        /// <summary>
        /// Flushes the pending writes to underlying writer..
        /// </summary>
        public void Flush()
        {
            if (_writeActions == null)
                return;

            foreach (var action in _writeActions)
            {
                action.Value(_writer, action.Key);
            }

            _writeActions = null;
        }

        /// <summary>
        /// Adds the action.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="action">The action.</param>
        private void AddAction(string fieldName, Action<IBinaryWriter, string> action)
        {
            if (_writeActions == null)
            {
                _writeActions = new SortedDictionary<string, Action<IBinaryWriter, string>>();
            }

            _writeActions.Add(fieldName, action);
        }
    }
}

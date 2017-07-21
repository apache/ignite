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

namespace Apache.Ignite.EntityFramework.Impl
{
    using System;
    using System.Collections;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Reads the data from array.
    /// </summary>
    internal class ArrayDbDataReader : DbDataReader
    {
        /** */
        private readonly object[][] _data;

        /** */
        private readonly DataReaderField[] _schema;

        /** */
        private int _pos = -1;

        /** */
        private bool _closed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ArrayDbDataReader"/> class.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <param name="schema">The schema.</param>
        public ArrayDbDataReader(object[][] data, DataReaderField[] schema)
        {
            Debug.Assert(data != null);
            Debug.Assert(schema != null);

            _data = data;
            _schema = schema;
        }

        /** <inheritDoc /> */
        public override void Close()
        {
            _closed = true;
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public override DataTable GetSchemaTable()
        {
            throw new NotSupportedException();
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public override bool NextResult()
        {
            return false;  // multiple result sets are not supported
        }

        /** <inheritDoc /> */
        public override bool Read()
        {
            if (_pos >= _data.Length - 1)
                return false;

            _pos++;

            return true;
        }

        /** <inheritDoc /> */
        public override int Depth
        {
            get { return 0; }
        }

        /** <inheritDoc /> */
        public override bool IsClosed
        {
            get { return _closed; }
        }

        /** <inheritDoc /> */
        public override int RecordsAffected
        {
            get { return -1; }
        }

        /** <inheritDoc /> */
        public override bool GetBoolean(int ordinal)
        {
            return (bool) GetValue(ordinal);
        }

        /** <inheritDoc /> */
        public override byte GetByte(int ordinal)
        {
            return (byte) GetValue(ordinal);
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            Debug.Assert(buffer != null);

            var data = (byte[]) GetValue(ordinal);

            var size = Math.Min(buffer.Length - bufferOffset, data.Length - dataOffset);

            Array.Copy(data, dataOffset, buffer, bufferOffset, size);

            return size;
        }

        /** <inheritDoc /> */
        public override char GetChar(int ordinal)
        {
            return (char) GetValue(ordinal);
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            Debug.Assert(buffer != null);

            var data = (char[]) GetValue(ordinal);

            var size = Math.Min(buffer.Length - bufferOffset, data.Length - dataOffset);

            Array.Copy(data, dataOffset, buffer, bufferOffset, size);

            return size;
        }

        /** <inheritDoc /> */
        public override Guid GetGuid(int ordinal)
        {
            return (Guid) GetValue(ordinal);
        }

        /** <inheritDoc /> */
        public override short GetInt16(int ordinal)
        {
            return (short) GetValue(ordinal);
        }

        /** <inheritDoc /> */
        public override int GetInt32(int ordinal)
        {
            return (int) GetValue(ordinal);
        }

        /** <inheritDoc /> */
        public override long GetInt64(int ordinal)
        {
            return (long) GetValue(ordinal);
        }

        /** <inheritDoc /> */
        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime) GetValue(ordinal);
        }

        /** <inheritDoc /> */
        public override string GetString(int ordinal)
        {
            return (string) GetValue(ordinal);
        }

        /** <inheritDoc /> */
        public override object GetValue(int ordinal)
        {
            return GetRow()[ordinal];
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override int GetValues(object[] values)
        {
            Debug.Assert(values != null);

            var row = GetRow();

            var size = Math.Min(row.Length, values.Length);

            Array.Copy(row, values, size);

            return size;
        }

        /** <inheritDoc /> */
        public override bool IsDBNull(int ordinal)
        {
            var val = GetValue(ordinal);

            return val == null || val == DBNull.Value;
        }

        /** <inheritDoc /> */
        public override int FieldCount
        {
            get { return _schema.Length; }
        }

        /** <inheritDoc /> */
        public override object this[int ordinal]
        {
            get { return GetValue(ordinal); }
        }

        /** <inheritDoc /> */
        public override object this[string name]
        {
            get { return GetValue(GetOrdinal(name)); }
        }

        /** <inheritDoc /> */
        public override bool HasRows
        {
            get { return _data.Length > 0; }
        }

        /** <inheritDoc /> */
        public override decimal GetDecimal(int ordinal)
        {
            return (decimal) GetValue(ordinal);
        }

        /** <inheritDoc /> */
        public override double GetDouble(int ordinal)
        {
            return (double) GetValue(ordinal);
        }

        /** <inheritDoc /> */
        public override float GetFloat(int ordinal)
        {
            return (float) GetValue(ordinal);
        }

        /** <inheritDoc /> */
        public override string GetName(int ordinal)
        {
            return _schema[ordinal].Name;
        }

        /** <inheritDoc /> */
        public override int GetOrdinal(string name)
        {
            for (int i = 0; i < _schema.Length; i++)
            {
                if (_schema[i].Name == name)
                    return i;
            }

            throw new InvalidOperationException("Field not found: " + name);
        }

        /** <inheritDoc /> */
        public override string GetDataTypeName(int ordinal)
        {
            return _schema[ordinal].DataType;
        }

        /** <inheritDoc /> */
        public override Type GetFieldType(int ordinal)
        {
            return _schema[ordinal].FieldType;
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public override IEnumerator GetEnumerator()
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets the row.
        /// </summary>
        private object[] GetRow()
        {
            if (_pos < 0)
                throw new InvalidOperationException("Data reading has not started.");

            return _data[_pos];
        }
    }
}
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

        public override void Close()
        {
            _closed = true;
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotSupportedException();
        }

        public override bool NextResult()
        {
            throw new NotSupportedException();
        }

        public override bool Read()
        {
            if (_pos >= _data.Length - 1)
                return false;

            _pos++;

            return true;
        }

        public override int Depth
        {
            get { return 1; }
        }

        public override bool IsClosed
        {
            get { return _closed; }
        }

        public override int RecordsAffected
        {
            get { return 0; }
        }

        public override bool GetBoolean(int ordinal)
        {
            return (bool) GetValue(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            return (byte) GetValue(ordinal);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            var bytes = (byte[]) GetValue(ordinal);

            // TODO: Bounds check. See the docs!
            Array.Copy(bytes, dataOffset, buffer, bufferOffset, length);

            return bytes.Length;
        }

        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override Guid GetGuid(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override short GetInt16(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetInt32(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetInt64(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override string GetString(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override object GetValue(int ordinal)
        {
            return GetRow()[ordinal];
        }

        public override int GetValues(object[] values)
        {
            var row = GetRow();

            var size = Math.Min(row.Length, values.Length);

            Array.Copy(row, values, size);

            return size;
        }

        public override bool IsDBNull(int ordinal)
        {
            var val = GetValue(ordinal);

            return val == null || val == DBNull.Value;
        }

        public override int FieldCount
        {
            get { return _schema.Length; }
        }

        public override object this[int ordinal]
        {
            get { return GetValue(ordinal); }
        }

        public override object this[string name]
        {
            get { return GetValue(GetOrdinal(name)); }
        }

        public override bool HasRows
        {
            get { return _data.Length > 0; }
        }

        public override decimal GetDecimal(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override double GetDouble(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override float GetFloat(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override string GetName(int ordinal)
        {
            return _schema[ordinal].Name;
        }

        public override int GetOrdinal(string name)
        {
            for (int i = 0; i < _schema.Length; i++)
            {
                if (_schema[i].Name == name)
                    return i;
            }

            throw new InvalidOperationException("Field not found: " + name);
        }

        public override string GetDataTypeName(int ordinal)
        {
            return _schema[ordinal].DataType;
        }

        public override Type GetFieldType(int ordinal)
        {
            return _schema[ordinal].FieldType;
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotSupportedException();
        }

        private object[] GetRow()
        {
            if (_pos < 0)
                throw new InvalidOperationException("Data reading has not started.");

            return _data[_pos];
        }
    }
}
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

namespace Apache.Ignite.EntityFramework.Impl
{
    using System;

    /// <summary>
    /// Represents a data reader field.
    /// </summary>
    [Serializable]
    internal class DataReaderField
    {
        /** */
        private readonly string _name;

        /** */
        private readonly Type _fieldType;

        /** */
        private readonly string _dataType;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataReaderField"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="fieldType">The type.</param>
        /// <param name="dataType">Type of the data.</param>
        public DataReaderField(string name, Type fieldType, string dataType)
        {
            _name = name;
            _fieldType = fieldType;
            _dataType = dataType;
        }

        /// <summary>
        /// Gets the name.
        /// </summary>
        public string Name
        {
            get { return _name; }
        }

        /// <summary>
        /// Gets the type of the field.
        /// </summary>
        public Type FieldType
        {
            get { return _fieldType; }
        }

        /// <summary>
        /// Gets the type of the data.
        /// </summary>
        public string DataType
        {
            get { return _dataType; }
        }
    }
}
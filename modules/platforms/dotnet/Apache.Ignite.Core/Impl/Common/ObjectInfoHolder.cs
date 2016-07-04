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

namespace Apache.Ignite.Core.Impl.Common
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Holds the information to instantiate an object and set it's properties.
    /// Typically used for .NET objects defined in Spring XML.
    /// </summary>
    internal class ObjectInfoHolder : IBinaryWriteAware
    {
        /** Type name. */
        private readonly string _typeName;

        /** Properties. */
        private readonly Dictionary<string, object> _properties;

        /// <summary>
        /// Initializes a new instance of the <see cref="ObjectInfoHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal ObjectInfoHolder(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            _typeName = reader.ReadString();
            _properties = reader.ReadDictionaryAsGeneric<string, object>();

            Debug.Assert(!string.IsNullOrEmpty(_typeName));
        }

        /// <summary>
        /// Gets the name of the type.
        /// </summary>
        public string TypeName
        {
            get { return _typeName; }
        }

        /// <summary>
        /// Gets the properties.
        /// </summary>
        public Dictionary<string, object> Properties
        {
            get { return _properties; }
        }
    }
}

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

namespace Apache.Ignite.Core.Tests.Query
{
    using System;
    using Apache.Ignite.Core.Portable;

    /**
     * 
     */
    class GridPortablePerson : IPortableMarshalAware {
        /**
         * 
         */
        public GridPortablePerson(string _name, int _age) 
        {
            Name = _name;
            Age = _age;
        }

        /**
         * 
         */
        public GridPortablePerson(string _name, int _age, string _address)
        {
            Name = _name;
            Address = _address;
            Age = _age;
        }

        /**
         * 
         */
        public string Name 
        { 
            get; 
            set; 
        }

        /**
         * 
         */
        public string Address
        {
            get;
            set;
        }

        /**
         * 
         */
        public int Age 
        {
            get;
            set;
        }

        /**
         * <summary>Writes this object to the given writer.</summary>
         *
         * <param name="writer">Writer.</param>
         * <exception cref="System.IO.IOException">If write failed.</exception>
         */
        public void WritePortable(IPortableWriter writer) {
            writer.WriteString("name", Name);
            writer.WriteString("address", Address);
            writer.WriteInt("age", Age);
        }

        /**
         * <summary>Reads this object from the given reader.</summary>
         *
         * <param name="reader">Reader.</param>
         * <exception cref="System.IO.IOException">If read failed.</exception>
         */
        public void ReadPortable(IPortableReader reader) {
            Name = reader.ReadString("name");
            Address = reader.ReadString("address");
            Age = reader.ReadInt("age");
        }
    }
}

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
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Test person.
    /// </summary>
    internal class PortablePerson : IPortableMarshalAware
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PortablePerson"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="age">The age.</param>
        public PortablePerson(string name, int age)
        {
            Name = name;
            Age = age;
        }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the address.
        /// </summary>
        public string Address { get; set; }

        /// <summary>
        /// Gets or sets the age.
        /// </summary>
        public int Age { get; set; }

        /** <ineritdoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteString("name", Name);
            writer.WriteString("address", Address);
            writer.WriteInt("age", Age);
        }

        /** <ineritdoc /> */
        public void ReadPortable(IPortableReader reader)
        {
            Name = reader.ReadString("name");
            Address = reader.ReadString("address");
            Age = reader.ReadInt("age");
        }
    }
}

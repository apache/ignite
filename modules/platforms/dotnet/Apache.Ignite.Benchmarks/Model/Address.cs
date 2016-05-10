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

namespace Apache.Ignite.Benchmarks.Model
{
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Address.
    /// </summary>
    internal class Address : IBinarizable
    {
        /// <summary>
        /// City.
        /// </summary>
        public string City { get; set; }
        
        /// <summary>
        /// Street.
        /// </summary>
        public string Street { get; set; }
        
        /// <summary>
        /// Street number.
        /// </summary>
        public int StreetNumber { get; set; }

        /// <summary>
        /// Flat number.
        /// </summary>
        public int FlatNumber { get; set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="city">City.</param>
        /// <param name="street">Street.</param>
        /// <param name="streetNum">Street number.</param>
        /// <param name="flatNum">Flat number.</param>
        public Address(string city, string street, int streetNum, int flatNum)
        {
            City = city;
            Street = street;
            StreetNumber = streetNum;
            FlatNumber = flatNum;
        }

        /** <inheritDoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            writer.WriteInt("streetNum", StreetNumber);
            writer.WriteInt("flatNum", FlatNumber);
            writer.WriteString("city", City);
            writer.WriteString("street", Street);
        }

        /** <inheritDoc /> */
        public void ReadBinary(IBinaryReader reader)
        {
            StreetNumber = reader.ReadInt("streetNum");
            FlatNumber = reader.ReadInt("flatNum");
            City = reader.ReadString("city");
            Street = reader.ReadString("street");
        }
    }
}

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

namespace Apache.Ignite.Core.Tests.TestDll2
{
    /// <summary>
    /// Address.
    /// </summary>
    public class Address
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="street">Street.</param>
        /// <param name="zip">ZIP code.</param>
        public Address(string street, int zip)
        {
            Street = street;
            Zip = zip;
        }

        /// <summary>
        /// Street.
        /// </summary>
        public string Street { get; set; }

        /// <summary>
        /// ZIP code.
        /// </summary>
        public int Zip { get; set; }

       /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return string.Format("{0} [street={1}, zip={2}]", typeof(Address).Name, Street, Zip);
        }
    }
}

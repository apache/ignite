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

namespace Apache.Ignite.ExamplesDll.Binary
{
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;

    /// <summary>
    /// Employee.
    /// </summary>
    public class Employee
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <param name="salary">Salary.</param>
        /// <param name="address">Address.</param>
        /// <param name="departments">Departments.</param>
        /// <param name="organizationId">The organization identifier.</param>
        public Employee(string name, long salary, Address address, ICollection<string> departments,
            int organizationId = 0)
        {
            Name = name;
            Salary = salary;
            Address = address;
            Departments = departments;
            OrganizationId = organizationId;
        }

        /// <summary>
        /// Name.
        /// </summary>
        [QuerySqlField]
        public string Name { get; set; }

        /// <summary>
        /// Organization id.
        /// </summary>
        [QuerySqlField(IsIndexed = true)]
        public int OrganizationId { get; set; }

        /// <summary>
        /// Salary.
        /// </summary>
        [QuerySqlField]
        public long Salary { get; set; }

        /// <summary>
        /// Address.
        /// </summary>
        [QuerySqlField]
        public Address Address { get; set; }

        /// <summary>
        /// Departments.
        /// </summary>
        public ICollection<string> Departments { get; set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return string.Format("{0} [name={1}, salary={2}, address={3}, departments={4}]", typeof(Employee).Name,
                Name, Salary, Address, CollectionToString(Departments));
        }

        /// <summary>
        /// Get string representation of collection.
        /// </summary>
        /// <returns></returns>
        private static string CollectionToString<T>(ICollection<T> col)
        {
            if (col == null)
                return "null";

            var elements = col.Any()
                ? col.Select(x => x.ToString()).Aggregate((x, y) => x + ", " + y)
                : string.Empty;

            return string.Format("[{0}]", elements);
        }
    }
}

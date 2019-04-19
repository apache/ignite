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

namespace Apache.Ignite.Examples
{
    using Core.Cache.Configuration;

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
        /// <param name="organizationId">The organization identifier.</param>
        public Employee(string name, long salary, int organizationId = 0)
        {
            Name = name;
            Salary = salary;
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
        /// Returns a string that represents the current object.
        /// </summary>
        public override string ToString()
        {
            return $"{typeof(Employee).Name} [{nameof(Name)}={Name}, {nameof(Salary)}={Salary}, " +
                   $"{nameof(OrganizationId)}={OrganizationId}]";
        }
    }
}

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

namespace Apache.Ignite.Examples
{
    using System;
    using Core.Cache.Configuration;

    /// <summary>
    /// Organization.
    /// </summary>
    public class Organization
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Name.</param>
        public Organization(string name)
        {
            Name = name;
            LastUpdated = DateTime.UtcNow;
        }

        /// <summary>
        /// Name.
        /// </summary>
        [QuerySqlField(IsIndexed = true)]
        public string Name { get; set; }

        /// <summary>
        /// Last update time.
        /// </summary>
        [QuerySqlField]
        public DateTime LastUpdated { get; set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        public override string ToString()
        {
            return $"{typeof(Organization).Name} [{nameof(Name)}={Name}, {nameof(LastUpdated)}={LastUpdated}]";
        }
    }
}

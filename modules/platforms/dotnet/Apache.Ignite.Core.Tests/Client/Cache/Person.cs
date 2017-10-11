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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using Apache.Ignite.Core.Cache.Configuration;

    /// <summary>
    /// Test person.
    /// </summary>
    public class Person
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Person"/> class.
        /// </summary>
        public Person()
        {
            DateTime = DateTime.UtcNow;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Person"/> class.
        /// </summary>
        public Person(int id)
        {
            Id = id;
            Name = "Person " + id;
            DateTime = DateTime.UtcNow.AddDays(id);
        }

        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [QuerySqlField(IsIndexed = true)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        [QuerySqlField]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the date time.
        /// </summary>
        [QuerySqlField]
        public DateTime DateTime { get; set; }

        /// <summary>
        /// Gets or sets the parent.
        /// </summary>
        public Person Parent { get;set; }
    }

    /// <summary>
    /// Test person 2.
    /// </summary>
    public class Person2 : Person
    {
        // No-op.
    }
}

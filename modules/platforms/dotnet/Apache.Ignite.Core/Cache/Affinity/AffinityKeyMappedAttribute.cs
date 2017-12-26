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

namespace Apache.Ignite.Core.Cache.Affinity
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Specifies cache key field to be used to determine a node on which given cache key will be stored.
    /// Only one field or property can be marked with this attribute.
    /// <para />
    /// This attribute is an alternative to <see cref="BinaryTypeConfiguration.AffinityKeyFieldName"/> setting.
    /// This attribute has lower priority than <see cref="BinaryTypeConfiguration.AffinityKeyFieldName"/> setting.
    /// <para />
    /// One of the major use cases for this attribute is the routing of grid computations
    /// to the nodes where the data for this computation is cached, the concept otherwise known as 
    /// Colocation Of Computations And Data.
    /// <para />
    /// For example, if a Person object is always accessed together with a Company object for which this person 
    /// is an employee, then for better performance and scalability it makes sense to colocate Person objects 
    /// together with their Company object when storing them in cache. 
    /// To achieve that, cache key used to cache Person objects should have a field or property marked with
    /// <see cref="AffinityKeyMappedAttribute"/> attribute, which will provide the value of 
    /// the company key for which that person works.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public sealed class AffinityKeyMappedAttribute : Attribute
    {
        /// <summary>
        /// Gets the affinity key field name from attribute.
        /// </summary>
        public static string GetFieldNameFromAttribute(Type type)
        {
            Debug.Assert(type != null);

            var res = ReflectionUtils.GetFieldsAndProperties(type)
                .Select(x => x.Key)
                .Where(x => x.GetCustomAttributes(false).OfType<AffinityKeyMappedAttribute>().Any())
                .Select(x => x.Name).ToArray();

            if (res.Length > 1)
            {
                throw new BinaryObjectException(string.Format(
                    "Multiple '{0}' attributes found on type '{1}'. There can be only one affinity field.",
                    typeof(AffinityKeyMappedAttribute).Name, type));
            }

            return res.SingleOrDefault();
        }
    }
}
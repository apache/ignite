/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
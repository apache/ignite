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

namespace Apache.Ignite.Linq.Impl
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// SQL type mapping.
    /// </summary>
    internal static class SqlTypes
    {
        /** */
        private static readonly Dictionary<Type, string> NetToSql = new Dictionary<Type, string>
        {
            {typeof (bool), "boolean"},
            {typeof (byte), "smallint"},
            {typeof (sbyte), "tinyint"},
            {typeof (short), "smallint"},
            {typeof (ushort), "int"},
            {typeof (int), "int"},
            {typeof (uint), "bigint"},
            {typeof (long), "bigint"},
            {typeof (ulong), "bigint"},
            {typeof (float), "real"},
            {typeof (double), "double"},
            {typeof (string), "nvarchar"},
            {typeof (decimal), "decimal"},
            {typeof (Guid), "uuid"},
            {typeof (DateTime), "timestamp"},
        };

        /** */
        private static readonly HashSet<Type> NotSupportedTypes = new HashSet<Type>(new []
        {
            typeof(char)
        }); 

        /// <summary>
        /// Gets the corresponding Java type name.
        /// </summary>
        public static string GetSqlTypeName(Type type)
        {
            if (type == null)
                return null;

            type = Nullable.GetUnderlyingType(type) ?? type;

            if (NotSupportedTypes.Contains(type))
            {
                throw new NotSupportedException("Type is not supported for SQL mapping: " + type);
            }

            string res;

            return NetToSql.TryGetValue(type, out res) ? res : null;
        }
    }
}

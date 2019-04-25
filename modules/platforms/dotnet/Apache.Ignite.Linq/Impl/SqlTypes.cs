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

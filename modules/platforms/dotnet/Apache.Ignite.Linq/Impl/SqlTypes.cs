﻿/*
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
            {typeof (char), "nvarchar(1)"},
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
            {typeof (DateTime?), "timestamp"},
        };

        /// <summary>
        /// Gets the corresponding Java type name.
        /// </summary>
        public static string GetSqlTypeName(Type type)
        {
            if (type == null)
                return null;

            string res;

            return !NetToSql.TryGetValue(type, out res) ? null : res;
        }
    }
}

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

namespace Apache.Ignite.Linq.Impl
{
    using System;
    using System.Reflection;

    /// <summary>
    /// Helper class with MethodInfos for methods that are supported by LINQ provider.
    /// </summary>
    internal static class Methods
    {
        /** */
        public static readonly MethodInfo StringContains = typeof(string).GetMethod("Contains");

        /** */
        public static readonly MethodInfo StringStartsWith = typeof(string).GetMethod("StartsWith",
            new[] { typeof(string) });

        /** */
        public static readonly MethodInfo StringEndsWith = typeof(string).GetMethod("EndsWith",
            new[] { typeof(string) });

        /** */
        public static readonly MethodInfo StringToLower = typeof(string).GetMethod("ToLower", new Type[0]);

        /** */
        public static readonly MethodInfo StringToUpper = typeof(string).GetMethod("ToUpper", new Type[0]);

        /** */
        public static readonly MethodInfo DateTimeToString = typeof (DateTime).GetMethod("ToString",
            new[] {typeof (string)});
    }
}
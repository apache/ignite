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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Parses .NET-style type names and deconstructs them into parts.
    /// </summary>
    internal class TypeNameParser
    {
        public static Result Parse(string typeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            var res = new Result();

            for (int i = 0; i < typeName.Length; i++)
            {
                var ch = typeName[i];

                switch (ch)
                {
                    case '.':
                        res.NameStart = i;
                        break;

                    case '`':
                        res.NameEnd = i;
                        ParseGeneric(typeName, i);
                        break;

                    case ',':
                        res.AssemblyIndex = i;
                        if (res.NameEnd < 0)
                        {
                            res.NameEnd = i;
                        }
                        break;
                }

                i++;
            }

            return res;
        }

        private static void ParseGeneric(string typeName, int i)
        {
            Debug.Assert(typeName[i] == '`');

            // Skip to types
            while (typeName[i] != '[')
                i++;
        }

        public class Result
        {
            public Result()
            {
                NameStart = -1;
                NameEnd = -1;
                AssemblyIndex = -1;
            }

            public int NameStart { get; set; }
            
            public int NameEnd { get; set; }

            public int AssemblyIndex { get; set; }
        }
    }
}

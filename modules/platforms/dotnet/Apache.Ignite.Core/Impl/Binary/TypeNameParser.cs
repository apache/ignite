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
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Parses .NET-style type names and deconstructs them into parts.
    /// </summary>
    internal static class TypeNameParser
    {
        public static Result Parse(string typeName)
        {
            int i = 0;

            return Parse(typeName, ref i);
        }

        private static Result Parse(string typeName, ref int i)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            var res = new Result(typeName, i);

            int bracket = 0;

            for (; i < typeName.Length; i++)
            {
                var ch = typeName[i];

                switch (ch)
                {
                    case '.':
                        res.NameStart = i + 1;
                        break;

                    case '`':
                        res.NameEnd = i - 1;
                        res.Generics = ParseGenerics(typeName, ref i);
                        break;

                    case ',':
                        if (bracket > 0)
                            break;

                        res.AssemblyIndex = i + 1;
                        if (res.NameEnd < 0)
                        {
                            res.NameEnd = i - 1;
                        }
                        return res;

                    case '[':
                        bracket++;
                        break;

                    case ']':
                        bracket--;

                        if (bracket < 0)
                            return res;

                        break;
                }
            }

            if (res.NameEnd < 0)
                res.NameEnd = typeName.Length - 1;

            return res;
        }

        private static List<Result> ParseGenerics(string typeName, ref int i)
        {
            // TODO: Out of bounds checks
            Debug.Assert(typeName[i] == '`');

            int start = i + 1;

            // Skip to types
            while (typeName[i] != '[')
                i++;

            int count = int.Parse(typeName.Substring(start, i - start));

            var res = new List<Result>(count);

            i++;

            while (true)  // TODO: Invalid char detection.
            {
                if (typeName[i] == ',' || typeName[i] == ' ')
                {
                    i++;
                }

                if (typeName[i] == '[')
                {
                    i++;

                    res.Add(Parse(typeName, ref i));

                    while (typeName[i] != ']')
                    {
                        i++;
                    }

                    i++;
                }

                if (typeName[i] == ']')
                {
                    i++;
                    return res;
                }
            }
        }

        public class Result
        {
            private readonly string _typeName;
            private readonly int _start;

            public Result(string typeName, int start)
            {
                _typeName = typeName;
                _start = start;

                NameStart = -1;
                NameEnd = -1;
                AssemblyIndex = -1;
            }

            public int NameStart { get; set; }
            
            public int NameEnd { get; set; }

            public int AssemblyIndex { get; set; }

            public ICollection<Result> Generics { get; set; }

            public string GetName()
            {
                return _typeName.Substring(NameStart, NameEnd - NameStart + 1);
            }

            public string GetFullName()
            {
                return _typeName.Substring(_start, NameEnd - _start + 1);
            }
        }
    }
}

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
    internal class TypeNameParser
    {
        private int _pos;

        private readonly string _typeName;

        private TypeNameParser(string typeName)
        {
            _typeName = typeName;
        }

        public static Result Parse(string typeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            return new TypeNameParser(typeName).Parse();
        }

        private char Char
        {
            get { return _typeName[_pos]; }
        }

        private Result Parse()
        {
            var res = new Result(_typeName, _pos);

            int bracket = 0;

            for (; _pos < _typeName.Length; _pos++)
            {
                var ch = Char;

                switch (ch)
                {
                    case '.':
                        res.NameStart = _pos + 1;
                        break;

                    case '`':
                        res.NameEnd = _pos - 1;
                        res.Generics = ParseGenerics();
                        break;

                    case ',':
                        if (bracket > 0)
                            break;

                        res.AssemblyIndex = _pos + 1;
                        if (res.NameEnd < 0)
                        {
                            res.NameEnd = _pos - 1;
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
                res.NameEnd = _typeName.Length - 1;

            return res;
        }

        private List<Result> ParseGenerics()
        {
            // TODO: Out of bounds checks
            Debug.Assert(Char == '`');

            int start = _pos + 1;

            // Skip to types
            while (Char != '[')
                _pos++;

            int count = int.Parse(_typeName.Substring(start, _pos - start));

            var res = new List<Result>(count);

            _pos++;

            while (true)  // TODO: Invalid char detection.
            {
                if (Char == ',' || Char == ' ')
                {
                    _pos++;
                }

                if (Char == '[')
                {
                    _pos++;

                    res.Add(Parse());

                    while (Char != ']')
                    {
                        _pos++;
                    }

                    _pos++;
                }

                if (Char == ']')
                {
                    _pos++;
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

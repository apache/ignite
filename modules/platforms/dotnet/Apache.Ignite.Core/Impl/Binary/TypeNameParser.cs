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
        private readonly int _start;

        private readonly string _typeName;

        private int _pos;

        private TypeNameParser(string typeName, int start = 0)
        {
            _typeName = typeName;
            _start = start;

            NameEnd = -1;
            NameStart = -1;
            AssemblyIndex = -1;

            Parse();
        }

        public static TypeNameParser Parse(string typeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            return new TypeNameParser(typeName);
        }

        public int NameStart { get; private set; }

        public int NameEnd { get; private set; }

        public int AssemblyIndex { get; private set; }

        public ICollection<TypeNameParser> Generics { get; private set; }

        public string GetName()
        {
            return _typeName.Substring(NameStart, NameEnd - NameStart + 1);
        }

        public string GetFullName()
        {
            return _typeName.Substring(_start, NameEnd - _start + 1);
        }


        private void Parse()
        {
            // Example:
            // System.Collections.Generic.List`1[[System.Int32[], mscorlib, Version=4.0.0.0, Culture=neutral,
            // PublicKeyToken =b77a5c561934e089]][], mscorlib, Version=4.0.0.0, Culture=neutral,
            // PublicKeyToken =b77a5c561934e089

            // 1) Namespace+name, ends with '`' or '[' or ','
            // 2) Generic, starts with '`'
            // 3) Array, starts with '['
            // 4) Assembly, starts with ',', ends with EOL or `]`

            int bracket = 0;

            for (; _pos < _typeName.Length; _pos++)
            {
                var ch = Char;

                switch (ch)
                {
                    case '.':
                        NameStart = _pos + 1;
                        break;

                    case '`':
                        NameEnd = _pos - 1;
                        ParseGenerics();
                        break;

                    case ',':
                        if (bracket > 0)
                            break;

                        AssemblyIndex = _pos + 1;
                        if (NameEnd < 0)
                        {
                            NameEnd = _pos - 1;
                        }
                        return;

                    case '[':
                        bracket++;
                        break;

                    case ']':
                        bracket--;

                        if (bracket < 0)
                            return;

                        break;
                }
            }

            if (NameEnd < 0)
                NameEnd = _typeName.Length - 1;
        }

        private void ParseTypeName()
        {
            
        }

        private void ParseAssemblyName()
        {
            
        }

        private void ParseArrayDefinition()
        {
            
        }

        private void ParseGenerics()
        {
            // TODO: Out of bounds checks
            Debug.Assert(Char == '`');

            int start = _pos + 1;

            // Skip to types
            while (Char != '[')
                _pos++;

            int count = int.Parse(_typeName.Substring(start, _pos - start));

            Generics = new List<TypeNameParser>(count);

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

                    Generics.Add(new TypeNameParser(_typeName, _pos));

                    while (Char != ']')
                    {
                        _pos++;
                    }

                    _pos++;
                }

                if (Char == ']')
                {
                    _pos++;
                    return ;
                }
            }
        }

        private char Char
        {
            get { return _typeName[_pos]; }
        }
    }
}

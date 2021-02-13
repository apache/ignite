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
    using System.Globalization;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Parses .NET-style type names and deconstructs them into parts.
    /// </summary>
    internal class TypeNameParser
    {
        /** */
        private readonly int _start;

        /** */
        private readonly char[] _typeNameArr;

        /** */
        private readonly bool _forceJavaNamingConventions;

        /** */
        private int _pos;

        /// <summary>
        /// Initializes a new instance of the <see cref="TypeNameParser" /> class.
        /// </summary>
        private TypeNameParser(string typeName, ref int pos, bool forceJavaNamingConventions)
        {
            _typeNameArr = typeName.ToCharArray();
            _start = pos;
            _pos = _start;
            _forceJavaNamingConventions = forceJavaNamingConventions;

            NameEnd = -1;
            NameStart = 0;
            AssemblyStart = -1;
            AssemblyEnd = -1;
            ArrayStart = -1;

            Parse();

            pos = _pos;
        }

        /// <summary>
        /// Parses the specified type name.
        /// </summary>
        public static TypeNameParser Parse(string typeName, bool forceJavaNamingConventions = false)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            int pos = 0;

            return new TypeNameParser(typeName, ref pos, forceJavaNamingConventions);
        }

        /// <summary>
        /// Gets the name start.
        /// </summary>
        public int NameStart { get; private set; }

        /// <summary>
        /// Gets the name end.
        /// </summary>
        public int NameEnd { get; private set; }

        /// <summary>
        /// Gets the name end.
        /// </summary>
        public int FullNameEnd { get; private set; }

        /// <summary>
        /// Gets the start of the assembly name.
        /// </summary>
        public int AssemblyStart { get; private set; }

        /// <summary>
        /// Gets the start of the assembly name.
        /// </summary>
        public int AssemblyEnd { get; private set; }

        /// <summary>
        /// Gets the start of the array definition.
        /// </summary>
        public int ArrayStart { get; private set; }

        /// <summary>
        /// Gets the start of the array definition.
        /// </summary>
        public int ArrayEnd { get; private set; }

        /// <summary>
        /// Gets the generics.
        /// </summary>
        public ICollection<TypeNameParser> Generics { get; private set; }

        /// <summary>
        /// Gets the type name (without namespace).
        /// </summary>
        public string GetName()
        {
            if (NameEnd < 0)
                return null;

            return Substring(NameStart, NameEnd - NameStart + 1);
        }

        /// <summary>
        /// Gets the full type name (with namespace).
        /// </summary>
        public string GetNameWithNamespace()
        {
            if (NameEnd < 0)
                return null;

            return Substring(_start, NameEnd - _start + 1);
        }

        /// <summary>
        /// Gets the full name (with namespace, generics and arrays).
        /// </summary>
        public string GetFullName()
        {
            return Substring(_start, FullNameEnd - _start + 1);
        }

        /// <summary>
        /// Gets the array part.
        /// </summary>
        public string GetArray()
        {
            if (ArrayStart < 0)
                return null;

            return Substring(ArrayStart, ArrayEnd - ArrayStart + 1);
        }

        /// <summary>
        /// Gets assembly name part.
        /// </summary>
        public string GetAssemblyName()
        {
            if (AssemblyStart < 0)
                return null;

            return Substring(AssemblyStart, AssemblyEnd - AssemblyStart + 1);
        }

        /// <summary>
        /// Parses this instance.
        /// </summary>
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

            ParseTypeName();
            ParseGeneric();
            ParseArrayDefinition();
            FullNameEnd = End ? _pos : _pos - 1;
            ParseAssemblyName();
        }

        /// <summary>
        /// Parses the type name with namespace.
        /// </summary>
        private void ParseTypeName()
        {
            NameStart = _pos;

            while (Shift())
            {
                if (Char == '.' || Char == '+')
                {
                    if (_forceJavaNamingConventions)
                        _typeNameArr[NameStart] =
                            System.Char.ToLower(_typeNameArr[NameStart], CultureInfo.CurrentCulture);

                    NameStart = _pos + 1;
                }

                if (Char == '`')
                {
                    // Non-null list indicates detected generic type.
                    Generics = Generics ?? new List<TypeNameParser>();
                }

                if (Char == '[' || Char == ']' || Char == ',' || Char == ' ')
                    break;
            }

            NameEnd = End ? _pos : _pos - 1;
        }

        /// <summary>
        /// Parses the generic part.
        /// </summary>
        private void ParseGeneric()
        {
            // Generics can be nested:
            // UserQuery+Gen`1+Gen2`1[[System.Int32, mscorlib],[System.String, mscorlib]]

            if (Generics == null)
            {
                return;
            }

            if (End || Char == ',')
            {
                // Open (unbound) generic.
                return;
            }

            if (Char != '[')
            {
                throw new IgniteException("Invalid generic type name, number must be followed by '[': " + TypeName);
            }

            while (true)
            {
                RequireShift();

                if (Char != '[')
                {
                    throw new IgniteException("Invalid generic type name, '[' must be followed by '[': " + TypeName);
                }

                RequireShift();

                Generics.Add(new TypeNameParser(TypeName, ref _pos, _forceJavaNamingConventions));

                if (Char != ']')
                {
                    throw new IgniteException("Invalid generic type name, no matching ']': " + TypeName);
                }

                RequireShift();

                if (Char == ']')
                {
                    Shift();
                    return;
                }

                if (Char != ',')
                {
                    throw new IgniteException("Invalid generic type name, expected ',': " + TypeName);
                }
            }
        }

        /// <summary>
        /// Parses the array definition.
        /// </summary>
        private void ParseArrayDefinition()
        {
            if (Char != '[')
                return;

            ArrayStart = _pos;

            var bracket = true;

            RequireShift();
            
            while (true)
            {
                if (Char == '[')
                {
                    if (bracket)
                    {
                        throw new IgniteException("Invalid array specification: " + TypeName);
                    }

                    bracket = true;
                }
                else if (Char == ']')
                {
                    if (!bracket)
                    {
                        ArrayEnd = _pos - 1;
                        return;
                    }

                    bracket = false;
                }
                else if (Char == ',' || Char == '*')
                {
                    if (!bracket)
                    {
                        break;
                    }
                }
                else
                {
                    if (bracket)
                    {
                        throw new IgniteException("Invalid array specification: " + TypeName);
                    }

                    break;
                }

                if (!Shift())
                    break;
            }

            ArrayEnd = Char == ']' ? _pos : _pos - 1;
        }

        /// <summary>
        /// Parses assembly name part.
        /// </summary>
        private void ParseAssemblyName()
        {
            if (Char != ',')
                return;

            RequireShift();

            SkipSpaces();

            AssemblyStart = _pos;

            while (Char != ']' && Shift())
            {
                // No-op.
            }

            AssemblyEnd = End ? _pos : _pos - 1;
        }

        /// <summary>
        /// Shifts the position forward.
        /// </summary>
        private bool Shift()
        {
            if (_pos < _typeNameArr.Length - 1)
            {
                _pos++;
                return true;
            }

            return false;
        }

        /// <summary>
        /// Requires position shift or throws an error.
        /// </summary>
        private void RequireShift()
        {
            if (!Shift())
            {
                throw new IgniteException("Invalid type name - not enough data: " + TypeName);
            }
        }

        /// <summary>
        /// Skips the spaces.
        /// </summary>
        private void SkipSpaces()
        {
            while (Char == ' ' && Shift())
            {
                // No-op.
            }
        }

        /// <summary>
        /// Gets a value indicating whether we are at the end of the string.
        /// </summary>
        private bool End
        {
            get { return _pos >= _typeNameArr.Length - 1; }
        }

        /// <summary>
        /// Gets the current character.
        /// </summary>
        private char Char
        {
            get { return _typeNameArr[_pos]; }
        }

        private string TypeName
        {
            get { return new string(_typeNameArr); }
        }

        private string Substring(int start, int length)
        {
            return new string(_typeNameArr, start, length); 
        }

        /** <inheritdoc /> */
        public override string ToString()
        {
            return TypeName;
        }
    }
}

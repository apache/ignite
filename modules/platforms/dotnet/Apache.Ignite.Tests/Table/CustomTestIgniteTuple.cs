/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Tests.Table
{
    using Ignite.Table;

    /// <summary>
    /// Custom tuple implementation for tests.
    /// </summary>
    public class CustomTestIgniteTuple : IIgniteTuple
    {
        public const int Key = 42;

        public const string Value = "Val1";

        public int FieldCount => 2;

        public object? this[int ordinal]
        {
            get => ordinal switch { 0 => Key, _ => Value };
            set => throw new System.NotImplementedException();
        }

        public object? this[string name]
        {
            get => name switch { "key" => Key, _ => Value };
            set => throw new System.NotImplementedException();
        }

        public string GetName(int ordinal) => ordinal switch { 0 => "key", _ => "val" };

        public int GetOrdinal(string name) => name switch { "key" => 0, _ => 1 };
    }
}

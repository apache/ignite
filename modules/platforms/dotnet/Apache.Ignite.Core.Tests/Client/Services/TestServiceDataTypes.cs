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

namespace Apache.Ignite.Core.Tests.Client.Services
{
    using System.Linq;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Service for testing various data types passing.
    /// </summary>
    public class TestServiceDataTypes : ITestServiceDataTypes, IService
    {
        /** <inheritdoc /> */
        public byte GetByte(byte x)
        {
            return (byte) (x + 1);
        }

        /** <inheritdoc /> */
        public byte[] GetByteArray(byte[] x)
        {
            return x.Select(b => (byte) (b + 1)).ToArray();
        }

        /** <inheritdoc /> */
        public sbyte GetSbyte(sbyte x)
        {
            return (sbyte) (x + 1);
        }

        /** <inheritdoc /> */
        public sbyte[] GetSbyteArray(sbyte[] x)
        {
            return x.Select(b => (sbyte) (b + 1)).ToArray();
        }

        /** <inheritdoc /> */
        public char GetChar(char x)
        {
            return (char) (x + 1);
        }

        /** <inheritdoc /> */
        public char[] GetCharArray(char[] x)
        {
            return x.Select(b => (char) (b + 1)).ToArray();
        }

        /** <inheritdoc /> */
        public void Init(IServiceContext context)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void Execute(IServiceContext context)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void Cancel(IServiceContext context)
        {
            // No-op.
        }
    }
}
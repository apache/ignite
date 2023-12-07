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

namespace Apache.Ignite.Core.Tests.Services
{
    using Apache.Ignite.Core.Services;
    using NUnit.Framework;

    /// <summary>
    /// Services async tests.
    /// </summary>
    [TestFixture]
    [Category(TestUtils.CategoryIntensive)]
    public class ServicesTestAsync : ServicesTest
    {
        /** <inheritdoc /> */
        protected override IServices Services
        {
            get { return new ServicesAsyncWrapper(Grid1.GetServices()); }
        }

        /** */
        public ServicesTestAsync()
        {
            // No-op.
        }

        /** */
        public ServicesTestAsync(bool useBinaryArray) : base(useBinaryArray)
        {
            // No-op.
        }
    }

    /// <summary> Tests with UseBinaryArray = true. </summary>
    public class ServicesTestAsyncBinaryArrays : ServicesTestAsync
    {
        /** */
        public ServicesTestAsyncBinaryArrays() : base(true)
        {
            // No-op.
        }
    }
}

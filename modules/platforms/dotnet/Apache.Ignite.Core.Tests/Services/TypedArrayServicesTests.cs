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

// ReSharper disable NonReadonlyMemberInGetHashCode
namespace Apache.Ignite.Core.Tests.Services
{
    using Apache.Ignite.Core.Impl.Services;
    using NUnit.Framework;

    /// <summary>
    /// Tests calling platform service from java.
    /// </summary>
    public class CallPlatformServiceTestTypedArrays : CallPlatformServiceTest
    {
        /// <summary>Start grids and deploy test service.</summary>
        [SetUp]
        public void SetUp0()
        {
            TestUtils.UseTypedArray = !TestUtils.DfltUseTypedArray;
        }

        /// <summary>Executes after each test.</summary>
        [TearDown]
        public void TearDown0()
        {
            TestUtils.UseTypedArray = TestUtils.DfltUseTypedArray;
        }
    }

    /// <summary>
    /// Tests <see cref="ServiceProxySerializer"/> functionality with keepBinary mode enabled on server.
    /// </summary>
    public class ServiceProxyTestKeepBinaryServerTypedArrays : ServiceProxyTestKeepBinaryServer
    {
        /// <summary>Start grids and deploy test service.</summary>
        [SetUp]
        public void SetUp0()
        {
            TestUtils.UseTypedArray = !TestUtils.DfltUseTypedArray;
        }

        /// <summary>Executes after each test.</summary>
        [TearDown]
        public void TearDown0()
        {
            TestUtils.UseTypedArray = TestUtils.DfltUseTypedArray;
        }
    }

    /// <summary>
    /// Services tests.
    /// </summary>
    public class ServicesTestTypedArrays : ServicesTest
    {
        /// <summary>Start grids and deploy test service.</summary>
        [SetUp]
        public void SetUp0()
        {
            TestUtils.UseTypedArray = !TestUtils.DfltUseTypedArray;
        }

        /// <summary>Executes after each test.</summary>
        [TearDown]
        public void TearDown0()
        {
            TestUtils.UseTypedArray = TestUtils.DfltUseTypedArray;
        }
    }

    /// <summary>
    /// Services async tests.
    /// </summary>
    public class ServicesTestAsyncTypedArrays : ServicesTestAsync
    {
        /// <summary>Start grids and deploy test service.</summary>
        [SetUp]
        public void SetUp0()
        {
            TestUtils.UseTypedArray = !TestUtils.DfltUseTypedArray;
        }

        /// <summary>Executes after each test.</summary>
        [TearDown]
        public void TearDown0()
        {
            TestUtils.UseTypedArray = TestUtils.DfltUseTypedArray;
        }
    }

    /// <summary>
    /// Services test with compact footers disabled.
    /// </summary>
    public class ServicesTestFullFooterTypedArrays : ServicesTestFullFooter
    {
        /// <summary>Start grids and deploy test service.</summary>
        [SetUp]
        public void SetUp0()
        {
            TestUtils.UseTypedArray = !TestUtils.DfltUseTypedArray;
        }

        /// <summary>Executes after each test.</summary>
        [TearDown]
        public void TearDown0()
        {
            TestUtils.UseTypedArray = TestUtils.DfltUseTypedArray;
        }
    }

    /// <summary>
    /// Tests checks ability to execute service method without explicit registration of parameter type.
    /// </summary>
    public class ServicesTypeAutoResolveTestTypedArrays : ServicesTypeAutoResolveTest
    {
        /// <summary>Start grids and deploy test service.</summary>
        [SetUp]
        public void SetUp0()
        {
            TestUtils.UseTypedArray = !TestUtils.DfltUseTypedArray;
        }

        /// <summary>Executes after each test.</summary>
        [TearDown]
        public void TearDown0()
        {
            TestUtils.UseTypedArray = TestUtils.DfltUseTypedArray;
        }
    }
}

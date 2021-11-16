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
    using NUnit.Framework;

    /// <summary>
    /// Tests calling platform service from java.
    /// </summary>
    public class CallPlatformServiceTestTypedArrays : CallPlatformServiceTest
    {
        [SetUp]
        public override void SetUp()
        {
            TestUtils.UseTypedArray = !TestUtils.DfltUseTypedArray;

            base.SetUp();
        }

        [TearDown]
        public override void TearDown()
        {
            base.TearDown();

            TestUtils.UseTypedArray = TestUtils.DfltUseTypedArray;
        }
    }

    /// <summary>
    /// Services tests.
    /// </summary>
    public class ServicesTestTypedArrays : ServicesTest
    {
        [SetUp]
        public override void SetUp()
        {
            TestUtils.UseTypedArray = !TestUtils.DfltUseTypedArray;

            base.SetUp();
        }

        [TestFixtureTearDown]
        public override void FixtureTearDown()
        {
            base.FixtureTearDown();
            TestUtils.UseTypedArray = TestUtils.DfltUseTypedArray;
        }
    }

    /// <summary>
    /// Services async tests.
    /// </summary>
    public class ServicesTestAsyncTypedArrays : ServicesTestAsync
    {
        [SetUp]
        public override void SetUp()
        {
            TestUtils.UseTypedArray = !TestUtils.DfltUseTypedArray;

            base.SetUp();
        }

        [TestFixtureTearDown]
        public override void FixtureTearDown()
        {
            base.FixtureTearDown();
            TestUtils.UseTypedArray = TestUtils.DfltUseTypedArray;
        }
    }

    /// <summary>
    /// Services test with compact footers disabled.
    /// </summary>
    public class ServicesTestFullFooterTypedArrays : ServicesTestFullFooter
    {
        [SetUp]
        public override void SetUp()
        {
            TestUtils.UseTypedArray = !TestUtils.DfltUseTypedArray;

            base.SetUp();
        }

        [TestFixtureTearDown]
        public override void FixtureTearDown()
        {
            base.FixtureTearDown();
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
        public override void SetUp()
        {
            TestUtils.UseTypedArray = !TestUtils.DfltUseTypedArray;

            base.SetUp();
        }

        [TestFixtureTearDown]
        public override void FixtureTearDown()
        {
            base.FixtureTearDown();

            TestUtils.UseTypedArray = TestUtils.DfltUseTypedArray;
        }
    }
}

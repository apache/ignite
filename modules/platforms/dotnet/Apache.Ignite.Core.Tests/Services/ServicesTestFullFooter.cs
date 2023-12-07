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
    using NUnit.Framework;

    /// <summary>
    /// Services test with compact footers disabled.
    /// </summary>
    [TestFixture]
    [Category(TestUtils.CategoryIntensive)]
    public class ServicesTestFullFooter : ServicesTest
    {
        /// <summary>
        /// Gets a value indicating whether compact footers should be used.
        /// </summary>
        protected override bool CompactFooter
        {
            get { return false; }
        }

        /** */
        public ServicesTestFullFooter()
        {
            // No-op.
        }

        /** */
        public ServicesTestFullFooter(bool useBinaryArray) : base(useBinaryArray)
        {
            // No-op.
        }
    }

    /// <summary> Tests with UseBinaryArray = true. </summary>
    public class ServicesTestFullFooterBinaryArrays : ServicesTestFullFooter
    {
        /** */
        public ServicesTestFullFooterBinaryArrays() : base(true)
        {
            // No-op.
        }
    }
}

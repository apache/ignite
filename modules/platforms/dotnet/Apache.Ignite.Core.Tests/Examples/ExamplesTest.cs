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

namespace Apache.Ignite.Core.Tests.Examples
{
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests all examples in various modes.
    /// </summary>
    [Category(TestUtils.CategoryExamples)]
    public class ExamplesTest
    {
        /** */
        private static readonly Example[] AllExamples = Example.GetExamples().ToArray();

        /** */
        private static readonly Example[] ThickExamples = AllExamples.Where(e => !e.IsThin).ToArray();

        /** */
        private static readonly Example[] ThinExamples = AllExamples.Where(e => e.IsThin).ToArray();

        /// <summary>
        /// Tests the example in a single node mode.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource(nameof(ThickExamples))]
        public void TestThick(Example example)
        {
            example.Run();
        }

        /// <summary>
        /// Tests the example in a single node mode.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource(nameof(ThinExamples))]
        public void TestThin(Example example)
        {
            example.Run();
        }
    }
}

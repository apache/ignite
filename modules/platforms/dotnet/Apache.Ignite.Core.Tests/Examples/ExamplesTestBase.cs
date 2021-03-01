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
    using System;
    using System.IO;
    using System.Text;
    using NUnit.Framework;

    /// <summary>
    /// Base class for example tests.
    /// </summary>
    public abstract class ExamplesTestBase
    {
        /** */
        private StringBuilder _outSb;

        /** */
        private TextWriter _oldOut;

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            _oldOut = Console.Out;
            _outSb = new StringBuilder();
            Console.SetOut(new StringWriter(_outSb));
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Console.SetOut(_oldOut);
            Console.WriteLine(_outSb);

            StringAssert.Contains(">>> Example finished, press any key to exit ...", GetOutput());
        }

        /// <summary>
        /// Gets the example output.
        /// </summary>
        protected string GetOutput()
        {
            return _outSb.ToString();
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="example"></param>
        /// <param name="requiredLines"></param>
        protected void CheckOutput(Example example, params string[] requiredLines)
        {
            var output = GetOutput();

            foreach (var line in requiredLines)
            {
                StringAssert.Contains(line, output);
            }

            var expectedOutputFile = Path.Combine(ExamplePaths.ExpectedOutputDir, example.Name) + ".txt";

            // TODO: Make this required
            if (File.Exists(expectedOutputFile))
            {
                var expectedOutput = File.ReadAllText(expectedOutputFile);

                StringAssert.Contains(expectedOutput, output);
            }
        }
    }
}

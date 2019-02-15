/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.Examples
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;
    using Apache.Ignite.Examples.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Represents an Example to be tested.
    /// </summary>
    public class Example
    {
        /** Execute action */
        private Action _runAction;

        /** Config url */
        public string ConfigPath { get; private set; }

        /** Dll load flag */
        public bool NeedsTestDll { get; private set; }

        /** Name */
        public Type ExampleType { get; private set; }

        /// <summary>
        /// Runs this example.
        /// </summary>
        public void Run()
        {
            try
            {
                _runAction();
            }
            catch (InvalidOperationException ex)
            {
                // Each example has a ReadKey at the end, which throws an exception in test environment.
                if (ex.Message != "Cannot read keys when either application does not have a console or " +
                    "when console input has been redirected from a file. Try Console.Read.")
                {
                    throw;
                }

                return;
            }

            throw new Exception("ReadKey missing at the end of the example.");
        }

        /// <summary>
        /// Gets all examples.
        /// </summary>
        public static IEnumerable<Example> GetExamples()
        {
            var examplesAsm = typeof (ClosureExample).Assembly;

            var sourceFiles = Directory.GetFiles(PathUtil.ExamplesSourcePath, "*.cs", SearchOption.AllDirectories)
                .Where(x => !x.Contains("dotnetcore")).ToArray();

            Assert.IsTrue(sourceFiles.Any());

            var types = examplesAsm.GetTypes().Where(x => x.GetMethod("Main") != null).OrderBy(x => x.Name).ToArray();

            Assert.IsTrue(types.Any());

            foreach (var type in types)
            {
                var sourceFile = sourceFiles.Single(x => x.EndsWith(string.Format("\\{0}.cs", type.Name)));

                var sourceCode = File.ReadAllText(sourceFile);

                yield return new Example
                {
                    ConfigPath = GetConfigPath(sourceCode),
                    NeedsTestDll = sourceCode.Contains("-assembly="),
                    _runAction = GetRunAction(type),
                    ExampleType = type
                };
            }
        }

        /// <summary>
        /// Gets the run action.
        /// </summary>
        private static Action GetRunAction(Type type)
        {
            var mainMethod = type.GetMethod("Main");
            Assert.IsNotNull(mainMethod);
            return (Action) Delegate.CreateDelegate(typeof (Action), mainMethod);
        }

        /// <summary>
        /// Gets the spring configuration URL.
        /// </summary>
        private static string GetConfigPath(string code)
        {
            var match = Regex.Match(code, "-configFileName=(.*?.config)");

            return match.Success ? match.Groups[1].Value : null;
        }

        /** <inheritdoc /> */
        public override string ToString()
        {
            // This will be displayed in TeamCity and R# test runner
            return ExampleType.Name;
        }
    }
}
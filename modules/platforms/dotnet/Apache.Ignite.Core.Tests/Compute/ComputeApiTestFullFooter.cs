/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests.Compute
{
    using System;
    using System.IO;
    using NUnit.Framework;

    /// <summary>
    /// Compute API test with compact footers disabled.
    /// </summary>
    [TestFixture]
    public class ComputeApiTestFullFooter : ComputeApiTest
    {
        /// <summary>
        /// Gets the expected compact footers setting.
        /// </summary>
        protected override bool CompactFooter
        {
            get { return false; }
        }

        /// <summary>
        /// Gets the configs.
        /// </summary>
        protected override Tuple<string, string, string> GetConfigs()
        {
            var baseConfigs = base.GetConfigs();

            return Tuple.Create(
                ReplaceFooterSetting(baseConfigs.Item1),
                ReplaceFooterSetting(baseConfigs.Item2),
                ReplaceFooterSetting(baseConfigs.Item3));
        }

        /// <summary>
        /// Replaces the footer setting.
        /// </summary>
        internal static string ReplaceFooterSetting(string path)
        {
            var text = File.ReadAllText(path).Replace(
                "property name=\"compactFooter\" value=\"true\"",
                "property name=\"compactFooter\" value=\"false\"");

            path += "_fullFooter";

            File.WriteAllText(path, text);

            Assert.IsTrue(File.Exists(path));

            return path;
        }
    }
}

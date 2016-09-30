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

namespace Apache.Ignite.Core.Tests.Plugin
{
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Plugin;

    /// <summary>
    /// Test Ignite plugin.
    /// </summary>
    public class TestPlugin
    {
        /** Target. */
        private readonly IPluginTarget _target;

        /// <summary>
        /// Initializes a new instance of the <see cref="TestPlugin"/> class.
        /// </summary>
        /// <param name="target">The target.</param>
        public TestPlugin(IPluginTarget target)
        {
            IgniteArgumentCheck.NotNull(target, "target");

            _target = target;
        }

        /// <summary>
        /// Gets the target.
        /// </summary>
        public IPluginTarget Target
        {
            get { return _target; }
        }
    }

    /// <summary>
    /// Expected Ignite plugin entry point is IIgnite extension method,
    /// so users simply say ignite.GetTestPlugin().
    /// </summary>
    public static class IgniteTestPluginExtensions
    {
        private const string PluginName = "PlatformTestPlugin";

        public static TestPlugin GetTestPlugin(this IIgnite ignite)
        {
            var target = ignite.GetPluginTarget(PluginName);

            return new TestPlugin(target);
        }
    }
}

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

namespace Apache.Ignite.Core.Tests.Deployment
{
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Function that returns parent process name.
    /// </summary>
    public class ProcessNameFunc : IComputeFunc<string>
    {
        /// <summary>
        /// Gets or sets the argument.
        /// </summary>
        public object Arg { get; set; }

        /** <inheritdoc /> */
        public string Invoke()
        {
            return System.Diagnostics.Process.GetCurrentProcess().ProcessName + Arg;
        }
    }

    /// <summary>
    /// Function that returns parent process name.
    /// </summary>
    public class ProcessNameArgFunc : IComputeFunc<object, string>
    {
        /** <inheritdoc /> */
        public string Invoke(object arg)
        {
            return System.Diagnostics.Process.GetCurrentProcess().ProcessName + arg;
        }
    }
}
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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Environment variable utilities.
    /// </summary>
    public static class EnvVar
    {
        /// <summary>
        /// Sets the environment variable and returns an object that resets the variable back when disposed.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <param name="value">Value.</param>
        /// <returns>Object that resets environment variable to previous value when disposed.</returns>
        public static IDisposable Set(string name, string value)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            var oldValue = Environment.GetEnvironmentVariable(name);
            Environment.SetEnvironmentVariable(name, value);

            return new EnvVarDisposableRestore(name, oldValue);
        }

        /// <summary>
        /// Disposable helper.
        /// </summary>
        private class EnvVarDisposableRestore : IDisposable
        {
            /** */
            private readonly string _name;

            /** */
            private readonly string _value;

            /// <summary>
            /// Ctor.
            /// </summary>
            /// <param name="name">Name.</param>
            /// <param name="value">Value.</param>
            public EnvVarDisposableRestore(string name, string value)
            {
                _name = name;
                _value = value;
            }

            /** <inheritDoc /> */
            public void Dispose()
            {
                Environment.SetEnvironmentVariable(_name, _value);
            }
        }
    }
}

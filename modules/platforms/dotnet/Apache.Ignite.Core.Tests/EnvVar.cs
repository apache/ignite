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

            return new DisposeAction(() => Environment.SetEnvironmentVariable(name, oldValue));
        }
    }
}

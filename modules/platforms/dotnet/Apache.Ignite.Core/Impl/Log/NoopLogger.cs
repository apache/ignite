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

namespace Apache.Ignite.Core.Impl.Log
{
    using System;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Logger that does not do anything - for convenience to avoid null checks.
    /// </summary>
    internal class NoopLogger : ILogger
    {
        /// <summary>
        /// Singleton instance.
        /// </summary>
        public static readonly NoopLogger Instance = new NoopLogger();
        
        /// <summary>
        /// Initializes a new instance of <see cref="NoopLogger"/> class.
        /// </summary>
        private NoopLogger()
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, string category,
            string nativeErrorInfo, Exception ex)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public bool IsEnabled(LogLevel level)
        {
            return false;
        }
    }
}
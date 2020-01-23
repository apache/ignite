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

using System;

namespace Apache.Ignite.Core.Impl.Unmanaged.Jni
{
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Console writer.
    /// </summary>
    internal sealed class ConsoleWriter : MarshalByRefObject
    {
        /** Environment variable: whether to suppress stderr warnings from Java 11. */
        private const string EnvIgniteNetSuppressJavaIllegalAccessWarnings =
            "IGNITE_NET_SUPPRESS_JAVA_ILLEGAL_ACCESS_WARNINGS";

        /** Warnings to suppress. */
        private static readonly string[] JavaIllegalAccessWarnings =
        {
            "WARNING: An illegal reflective access operation has occurred",
            "WARNING: Illegal reflective access by org.apache.ignite.internal.util.GridUnsafe$2",
            "WARNING: Please consider reporting this to the maintainers of",
            "WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations",
            "WARNING: All illegal access operations will be denied in a future release"
        };

        /** Flag: whether to suppress stderr warnings from Java 11. */
        private readonly bool _suppressIllegalAccessWarnings;

        public ConsoleWriter()
        {
            _suppressIllegalAccessWarnings =
                Environment.GetEnvironmentVariable(EnvIgniteNetSuppressJavaIllegalAccessWarnings) == "true";
        }

        /// <summary>
        /// Writes the specified message to console.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic",
            Justification = "Only instance methods can be called across AppDomain boundaries.")]
        public void Write(string message, bool isError)
        {
            if (_suppressIllegalAccessWarnings && isError && IsKnownWarning(message))
            {
                return;
            }

            var target = isError ? Console.Error : Console.Out;
            target.Write(message);
        }

        /** <inheritdoc /> */
        public override object InitializeLifetimeService()
        {
            // Ensure that cross-AppDomain reference lives forever.
            return null;
        }

        /// <summary>
        /// Returns a value indicating whether provided message is a known warning.
        /// </summary>
        private static bool IsKnownWarning(string message)
        {
            foreach (var warning in JavaIllegalAccessWarnings)
            {
                if (message.StartsWith(warning, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }
    }
}

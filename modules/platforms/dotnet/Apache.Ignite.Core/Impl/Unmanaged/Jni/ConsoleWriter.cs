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

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

namespace Apache.Ignite.Config
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Linq;

    /// <summary>
    /// Configurator which uses application configuration.
    /// </summary>
    internal static class AppSettingsConfigurator
    {
        /** Common configuration property prefix. */
        private const string CfgPrefix = "Ignite.";

        /** Configuration property: Ignite home. */
        private const string CfgHome = "Home";

        /** Configuration property: JVM option prefix. */
        private const string CfgJvmOptPrefix = "JvmOption";

        /** Configuration property: assembly prefix. */
        private const string CfgAssemblyPrefix = "Assembly";

        /// <summary>
        /// Gets the arguments in split form.
        /// </summary>
        public static IEnumerable<Tuple<string, string>> GetArgs(NameValueCollection args)
        {
            return args.AllKeys
                .Where(x => x.StartsWith(CfgPrefix, StringComparison.OrdinalIgnoreCase))
                .Select(k => Tuple.Create(Replace(k), args[k]));
        }

        /// <summary>
        /// Replaces the appsettings-specific keys with common arg name.
        /// </summary>
        private static string Replace(string key)
        {
            key = key.Substring(CfgPrefix.Length);

            key = key.Equals(CfgHome, StringComparison.OrdinalIgnoreCase) ? Configurator.CmdIgniteHome : key;
            key = key.StartsWith(CfgJvmOptPrefix, StringComparison.OrdinalIgnoreCase) ? Configurator.CmdJvmOpt : key;
            key = key.StartsWith(CfgAssemblyPrefix, StringComparison.OrdinalIgnoreCase) ? Configurator.CmdAssembly : key;

            return key;
        }
    }
}

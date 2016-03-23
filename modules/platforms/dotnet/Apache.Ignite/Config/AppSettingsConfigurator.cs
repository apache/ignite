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

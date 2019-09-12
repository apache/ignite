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

namespace Apache.Ignite.Config
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core;

    /// <summary>
    /// Loads required assemblies at runtime.
    /// </summary>
    internal static class ArgsAssemblyLoader
    {
        /// <summary>
        /// Load assemblies if assembly key is present in arguments.
        /// This is useful for plugin configuration before parsing the app.config file.
        /// </summary>
        /// <param name="args">Application arguments in split form.</param>
        public static void LoadAssemblies(IEnumerable<Tuple<string, string>> args)
        {
            var assemblies = args
                .Where(x => x.Item1.StartsWith(Configurator.CmdAssembly, StringComparison.InvariantCultureIgnoreCase))
                .Select(Configurator.ValidateArgValue);

            Ignition.LoadAssemblies(assemblies);
        }
    }
}

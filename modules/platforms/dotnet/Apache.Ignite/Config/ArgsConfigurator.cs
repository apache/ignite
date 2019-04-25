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

    /// <summary>
    /// Configurator which uses arguments array.
    /// </summary>
    internal static class ArgsConfigurator
    {
        /// <summary>
        /// Gets the arguments in split form.
        /// </summary>
        public static IEnumerable<Tuple<string, string>> GetArgs(IEnumerable<string> args)
        {
            return args
                .Select(x => x.Trim().TrimStart('-'))
                .Select(x => x.StartsWith(Configurator.CmdJvmOpt + "-")
                    ? new[] {Configurator.CmdJvmOpt, x.Substring(Configurator.CmdJvmOpt.Length)}
                    : x.Split(new[] {'='}, 2))
                .Select(x => Tuple.Create(x[0], x.Length > 1 ? x[1] : string.Empty));
        }
    }
}

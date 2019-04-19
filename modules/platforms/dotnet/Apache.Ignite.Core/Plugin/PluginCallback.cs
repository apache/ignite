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

namespace Apache.Ignite.Core.Plugin
{
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Plugin callback delegate.
    /// </summary>
    /// <param name="input">Input reader. May be null.</param>
    /// <param name="output">Output writer. May be null.</param>
    /// <returns>Result code.</returns>
    public delegate long PluginCallback(IBinaryRawReader input, IBinaryRawWriter output);
}

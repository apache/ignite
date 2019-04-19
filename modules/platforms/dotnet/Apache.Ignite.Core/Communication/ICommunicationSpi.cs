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

namespace Apache.Ignite.Core.Communication
{
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Communication.Tcp;

    /// <summary>
    /// Communication SPI is responsible for data exchange between nodes. 
    /// <para />
    /// Communication SPI is one of the most important SPIs in Ignite. It is used
    /// heavily throughout the system and provides means for all data exchanges
    /// between nodes, such as internal implementation details and user driven messages.
    /// <para />
    /// Only predefined implementation is supported now: <see cref="TcpCommunicationSpi"/>.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1040:AvoidEmptyInterfaces")]
    public interface ICommunicationSpi
    {
        // No-op.
    }
}

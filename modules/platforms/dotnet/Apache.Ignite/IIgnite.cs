/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite
{
    using System;
    using Table;

    /// <summary>
    /// Ignite API entry point.
    /// <para />
    /// Implementation can be a thin client (see <see cref="IIgniteClient"/> and <see cref="IgniteClient.StartAsync"/>),
    /// or a direct IPC connection for server-side functionality like compute.
    /// </summary>
    public interface IIgnite : IDisposable
    {
        /// <summary>
        /// Gets the tables API.
        /// </summary>
        ITables Tables { get; }
    }
}

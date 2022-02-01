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

namespace Apache.Ignite.Core.Client.Cache.Query.Continuous
{
    using System;

    /// <summary>
    /// Represents a continuous query handle. Call <see cref="IDisposable.Dispose"/> to stop the continuous query.
    /// </summary>
    public interface IContinuousQueryHandleClient : IDisposable
    {
        /// <summary>
        /// Occurs when continuous query gets disconnected.
        /// Disconnect can be caused by a server node failure or a network failure.
        /// </summary>
        event EventHandler<ContinuousQueryClientDisconnectedEventArgs> Disconnected;
    }
}

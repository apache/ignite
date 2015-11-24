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

namespace Apache.Ignite.Core.Messaging
{
    using System;

    /// <summary>
    /// Represents messaging filter predicate.
    /// </summary>
    public interface IMessageListener<in T>
    {
        /// <summary>
        /// Invokes the message listener when a message arrives.
        /// </summary>
        /// <param name="nodeId">Message source node identifier.</param>
        /// <param name="message">Message.</param>
        /// <returns>
        /// Value indicating whether this instance should remain subscribed. 
        /// Returning <c>false</c> will unsubscribe this message listener from further notifications.
        /// </returns>
        bool Invoke(Guid nodeId, T message);
    }
}
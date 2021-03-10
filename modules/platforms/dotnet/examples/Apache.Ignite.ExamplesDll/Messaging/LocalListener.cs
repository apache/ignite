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

namespace Apache.Ignite.ExamplesDll.Messaging
{
    using System;
    using System.Threading;
    using Apache.Ignite.Core.Messaging;

    /// <summary>
    /// Local message listener which signals countdown event on each received message.
    /// </summary>
    public class LocalListener : IMessageListener<int>
    {
        /** Countdown event. */
        private readonly CountdownEvent _countdown;

        /// <summary>
        /// Initializes a new instance of the <see cref="LocalListener"/> class.
        /// </summary>
        /// <param name="countdown">The countdown event.</param>
        public LocalListener(CountdownEvent countdown)
        {
            if (countdown == null)
                throw new ArgumentNullException("countdown");

            _countdown = countdown;
        }

        /// <summary>
        /// Receives a message and returns a value 
        /// indicating whether provided message and node id satisfy this predicate.
        /// Returning false will unsubscribe this listener from future notifications.
        /// </summary>
        /// <param name="nodeId">Node identifier.</param>
        /// <param name="message">Message.</param>
        /// <returns>Value indicating whether provided message and node id satisfy this predicate.</returns>
        public bool Invoke(Guid nodeId, int message)
        {
            _countdown.Signal();

            return !_countdown.IsSet;
        }
    }
}

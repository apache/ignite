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
using Apache.Ignite.Core.Events;

namespace Apache.Ignite.ExamplesDll.Events
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Lambda expression event listeners.
    /// </summary>
    public static class LambdaListener
    {
        /// <summary>
        /// Creates a local listener using a lambda expression.
        /// </summary>
        /// <param name="events">The events.</param>
        /// <param name="types">The event types.</param>
        /// <returns>Local listener.</returns>
        public static IEventListener<IEvent> LocalListen(IEvents events, IEnumerable<int> types)
        {
            return events.LocalListen<IEvent>(evt =>
            {
                Console.WriteLine("Local listener received an event [evt={0}]", evt.Name);
                return true;
            }, types);
        }
    }
}

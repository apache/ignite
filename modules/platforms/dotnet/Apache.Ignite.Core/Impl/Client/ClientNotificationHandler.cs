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

namespace Apache.Ignite.Core.Impl.Client
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Wraps client notification handler together with the message queue.
    /// Client notifications can be sent by the server sooner than we subscribe to them.
    /// This class handles the problem: when _handler is not present, we add messages to the _queue.
    /// When _handler is added, we drain the queue and also route new messages to the handler directly.
    /// </summary>
    internal class ClientNotificationHandler
    {
        /** Logger. */
        private readonly ILogger _logger;

        /** Nested handler. */
        private Action<IBinaryStream> _handler;

        /** Queue. */
        private List<IBinaryStream> _queue;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientNotificationHandler"/>.
        /// </summary>
        public ClientNotificationHandler(ILogger logger, Action<IBinaryStream> handler = null)
        {
            Debug.Assert(logger != null);
            
            _logger = logger;
            _handler = handler;
        }

        /// <summary>
        /// Handles the notification.
        /// </summary>
        /// <param name="stream">Notification data.</param>
        public void Handle(IBinaryStream stream)
        {
            // ClientSocket handles notifications in a single receiver thread,
            // but handler will be set from another thread.
            lock (this)
            {
                if (_handler != null)
                {
                    _handler(stream);
                }
                else
                {
                    // NOTE: Back pressure control should be added here when needed (e.g. for Continuous Queries). 
                    _queue = _queue ?? new List<IBinaryStream>();
                    _queue.Add(stream);
                }
            }
        }

        /// <summary>
        /// Sets the handler.
        /// </summary>
        /// <param name="handler">Handler.</param>
        public ClientNotificationHandler SetHandler(Action<IBinaryStream> handler)
        {
            Debug.Assert(handler != null);
            
            lock (this)
            {
                _handler = handler;

                if (_queue != null)
                {
                    var queue = _queue;
                    _queue = null;

                    ThreadPool.QueueUserWorkItem(_ => Drain(handler, queue));
                }
            }

            return this;
        }

        /// <summary>
        /// Drains the queue.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Thread root must catch all exceptions to avoid crashing the process.")]
        private void Drain(Action<IBinaryStream> handler, List<IBinaryStream> queue)
        {
            try
            {
                queue.ForEach(handler);
            }
            catch (Exception e)
            {
                _logger.Error("Failed to handle client notification", e);
            }
        }
    }
}
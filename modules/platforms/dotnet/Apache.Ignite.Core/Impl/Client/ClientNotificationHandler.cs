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
        /** Handler delegate. */
        public delegate void Handler(IBinaryStream stream, Exception ex);

        /** Logger. */
        private readonly ILogger _logger;

        /** Nested handler. */
        private volatile Handler _handler;

        /** Queue. */
        private List<KeyValuePair<IBinaryStream, Exception>> _queue;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientNotificationHandler"/>.
        /// </summary>
        public ClientNotificationHandler(ILogger logger, Handler handler = null)
        {
            Debug.Assert(logger != null);

            _logger = logger;
            _handler = handler;
        }

        /// <summary>
        /// Gets a value indicating whether handler is set for this instance.
        /// </summary>
        public bool HasHandler
        {
            get { return _handler != null; }
        }

        /// <summary>
        /// Handles the notification.
        /// </summary>
        /// <param name="stream">Notification data.</param>
        /// <param name="exception">Exception. Can be null.</param>
        public void Handle(IBinaryStream stream, Exception exception)
        {
            // We are in the socket receiver thread here.
            // However, handler is set from another thread, so lock is required.
            lock (this)
            {
                // NOTE: Back pressure control should be added here when needed (e.g. for Continuous Queries).
                var handler = _handler;

                if (handler != null)
                {
                    ThreadPool.QueueUserWorkItem(_ => Handle(handler, stream, exception));
                }
                else
                {
                    _queue = _queue ?? new List<KeyValuePair<IBinaryStream, Exception>>();
                    _queue.Add(new KeyValuePair<IBinaryStream, Exception>(stream, exception));
                }
            }
        }

        /// <summary>
        /// Sets the handler.
        /// </summary>
        /// <param name="handler">Handler.</param>
        public void SetHandler(Handler handler)
        {
            Debug.Assert(handler != null);
            Debug.Assert(_handler == null);

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
        }

        /// <summary>
        /// Drains the queue.
        /// </summary>
        private void Drain(Handler handler, List<KeyValuePair<IBinaryStream, Exception>> queue)
        {
            foreach (var stream in queue)
            {
                Handle(handler, stream.Key, stream.Value);
            }
        }

        /// <summary>
        /// Handles the notification.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Thread root must catch all exceptions to avoid crashing the process.")]
        private void Handle(Handler handler, IBinaryStream stream, Exception exception)
        {
            try
            {
                handler(stream, exception);
            }
            catch (Exception e)
            {
                _logger.Error(e, "Failed to handle client notification");
            }
        }
    }
}

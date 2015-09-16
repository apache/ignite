/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;
using System.Threading;
using GridGain.Messaging;

namespace GridGain.Examples.Messaging
{
    /// <summary>
    /// Local message listener which signals countdown event on each received message.
    /// </summary>
    public class LocalListener : IMessageFilter<int>
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

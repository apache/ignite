/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Log4Net
{
    using System;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Log;
    using global::log4net;
    using global::log4net.Core;
    using global::log4net.Util;
    using ILogger = Apache.Ignite.Core.Log.ILogger;

    /// <summary>
    /// Ignite log4net integration.
    /// </summary>
    public class IgniteLog4NetLogger : ILogger
    {
        /** Wrapped log4net log. */
        private readonly ILog _log;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteLog4NetLogger"/> class.
        /// </summary>
        public IgniteLog4NetLogger() : this (LogManager.GetLogger(typeof(IgniteLog4NetLogger)))
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteLog4NetLogger"/> class.
        /// </summary>
        /// <param name="log">The log.</param>
        public IgniteLog4NetLogger(ILog log)
        {
            IgniteArgumentCheck.NotNull(log, "log");

            _log = log;
        }

        /// <summary>
        /// Logs the specified message.
        /// </summary>
        /// <param name="level">The level.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments to format <paramref name="message" />.
        /// Can be null (formatting will not occur).</param>
        /// <param name="formatProvider">The format provider. Can be null if <paramref name="args" /> is null.</param>
        /// <param name="category">The logging category name.</param>
        /// <param name="nativeErrorInfo">The native error information.</param>
        /// <param name="ex">The exception. Can be null.</param>
        public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, 
            string category, string nativeErrorInfo, Exception ex)
        {
            var logLevel = ConvertLogLevel(level);

            var repo = _log.Logger.Repository;

            var messageObject = args == null 
                ? (object) message 
                : new SystemStringFormat(formatProvider, message, args);

            var evt = new LoggingEvent(GetType(), repo, category, logLevel, messageObject, ex);

            if (nativeErrorInfo != null)
                evt.Properties["nativeErrorInfo"] = nativeErrorInfo;

            _log.Logger.Log(evt);
        }

        /// <summary>
        /// Determines whether the specified log level is enabled.
        /// </summary>
        /// <param name="level">The level.</param>
        /// <returns>
        /// Value indicating whether the specified log level is enabled
        /// </returns>
        public bool IsEnabled(LogLevel level)
        {
            var logLevel = ConvertLogLevel(level);

            return _log.Logger.IsEnabledFor(logLevel);
        }

        /// <summary>
        /// Converts the Ignite LogLevel to the log4net log level.
        /// </summary>
        /// <param name="level">The Ignite log level.</param>
        /// <returns>Corresponding log4net log level.</returns>
        public static Level ConvertLogLevel(LogLevel level)
        {
            switch (level)
            {
                case LogLevel.Trace:
                    return Level.Trace;
                case LogLevel.Debug:
                    return Level.Debug;
                case LogLevel.Info:
                    return Level.Info;
                case LogLevel.Warn:
                    return Level.Warn;
                case LogLevel.Error:
                    return Level.Error;
                default:
                    throw new ArgumentOutOfRangeException("level", level, null);
            }
        }
    }
}

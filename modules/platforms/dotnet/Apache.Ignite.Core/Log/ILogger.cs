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

namespace Apache.Ignite.Core.Log
{
    using System;
    using System.ComponentModel;
    using System.Globalization;

    /// <summary>
    /// Defines Ignite logging interface.
    /// </summary>
    public interface ILogger
    {
        // TODO: Logger should only have the simplest methods
        // Convenience overloads go to extension class!

        /// <summary>
        /// Logs the specified message.
        /// </summary>
        /// <param name="level">The level.</param>
        /// <param name="ex">The ex.</param>
        /// <param name="formatProvider">The format provider.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        void Log(LogLevel level, Exception ex, IFormatProvider formatProvider, [Localizable(false)] string message,
            object[] args);
    }

    public static class LoggerExtensions
    {
        public static void LogError(this ILogger logger, string message)
        {
            logger.Log(LogLevel.Error, null, CultureInfo.InvariantCulture, message, null);
        }
    }

    // TODO: LogEventInfo?, LogLevel - see NLog
    public enum LogLevel
    {
        Error
    }
}

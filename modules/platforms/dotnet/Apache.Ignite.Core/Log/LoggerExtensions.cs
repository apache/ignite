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
    using System.Globalization;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Extension methods for <see cref="ILogger" />
    /// </summary>
    public static class LoggerExtensions
    {
        // 4 overloads per level (message, message+args, ex+message, ex+message+args)

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Trace"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="message">The message.</param>
        public static void Trace(this ILogger logger, string message)
        {
            Log(logger, LogLevel.Trace, message);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Trace"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        public static void Trace(this ILogger logger, string message, params object[] args)
        {
            Log(logger, LogLevel.Trace, message, args);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Trace"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="ex">The exception.</param>
        /// <param name="message">The message.</param>
        public static void Trace(this ILogger logger, Exception ex, string message)
        {
            Log(logger, LogLevel.Trace, ex, message);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Trace"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="ex">The exception.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        public static void Trace(this ILogger logger, Exception ex, string message, params object[] args)
        {
            Log(logger, LogLevel.Trace, ex, message, args);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Debug"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="message">The message.</param>
        public static void Debug(this ILogger logger, string message)
        {
            Log(logger, LogLevel.Debug, message);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Debug"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        public static void Debug(this ILogger logger, string message, params object[] args)
        {
            Log(logger, LogLevel.Debug, message, args);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Debug"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="ex">The exception.</param>
        /// <param name="message">The message.</param>
        public static void Debug(this ILogger logger, Exception ex, string message)
        {
            Log(logger, LogLevel.Debug, ex, message);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Debug"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="ex">The exception.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        public static void Debug(this ILogger logger, Exception ex, string message, params object[] args)
        {
            Log(logger, LogLevel.Debug, ex, message, args);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Info"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="message">The message.</param>
        public static void Info(this ILogger logger, string message)
        {
            Log(logger, LogLevel.Info, message);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Info"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        public static void Info(this ILogger logger, string message, params object[] args)
        {
            Log(logger, LogLevel.Info, message, args);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Info"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="ex">The exception.</param>
        /// <param name="message">The message.</param>
        public static void Info(this ILogger logger, Exception ex, string message)
        {
            Log(logger, LogLevel.Info, ex, message);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Info"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="ex">The exception.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        public static void Info(this ILogger logger, Exception ex, string message, params object[] args)
        {
            Log(logger, LogLevel.Info, ex, message, args);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Warn"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="message">The message.</param>
        public static void Warn(this ILogger logger, string message)
        {
            Log(logger, LogLevel.Warn, message);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Warn"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        public static void Warn(this ILogger logger, string message, params object[] args)
        {
            Log(logger, LogLevel.Warn, message, args);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Warn"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="ex">The exception.</param>
        /// <param name="message">The message.</param>
        public static void Warn(this ILogger logger, Exception ex, string message)
        {
            Log(logger, LogLevel.Warn, ex, message);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Warn"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="ex">The exception.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        public static void Warn(this ILogger logger, Exception ex, string message, params object[] args)
        {
            Log(logger, LogLevel.Warn, ex, message, args);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Error"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="message">The message.</param>
        public static void Error(this ILogger logger, string message)
        {
            Log(logger, LogLevel.Error, message);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Error"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        public static void Error(this ILogger logger, string message, params object[] args)
        {
            Log(logger, LogLevel.Error, message, args);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Error"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="ex">The exception.</param>
        /// <param name="message">The message.</param>
        public static void Error(this ILogger logger, Exception ex, string message)
        {
            Log(logger, LogLevel.Error, ex, message);
        }

        /// <summary>
        /// Logs the message with <see cref="LogLevel.Error"/> level.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="ex">The exception.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        public static void Error(this ILogger logger, Exception ex, string message, params object[] args)
        {
            Log(logger, LogLevel.Error, ex, message, args);
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="level">The level.</param>
        /// <param name="message">The message.</param>
        public static void Log(this ILogger logger, LogLevel level, string message)
        {
            IgniteArgumentCheck.NotNull(logger, "logger");

            logger.Log(level, message, null, null, null, null, null);
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="level">The level.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        public static void Log(this ILogger logger, LogLevel level, string message, params object[] args)
        {
            IgniteArgumentCheck.NotNull(logger, "logger");

            logger.Log(level, message, args, CultureInfo.InvariantCulture, null, null, null);
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="level">The level.</param>
        /// <param name="ex">The exception.</param>
        /// <param name="message">The message.</param>
        public static void Log(this ILogger logger, LogLevel level, Exception ex, string message)
        {
            IgniteArgumentCheck.NotNull(logger, "logger");

            logger.Log(level, message, null, null, null, null, ex);
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="level">The level.</param>
        /// <param name="ex">The exception.</param>
        /// <param name="message">The message.</param>
        /// <param name="args">The arguments.</param>
        public static void Log(this ILogger logger, LogLevel level, Exception ex, string message, params object[] args)
        {
            IgniteArgumentCheck.NotNull(logger, "logger");

            logger.Log(level, message, args, CultureInfo.InvariantCulture, null, null, ex);
        }

        /// <summary>
        /// Gets the <see cref="CategoryLogger"/> with a specified category that wraps provided logger.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="category">The category.</param>
        /// <returns>Logger that uses specified category when no other category is provided.</returns>
        public static ILogger GetLogger(this ILogger logger, string category)
        {
            IgniteArgumentCheck.NotNull(logger, "logger");

            return new CategoryLogger(logger, category);
        }

        /// <summary>
        /// Gets the <see cref="CategoryLogger"/> with a specified category that wraps provided logger.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="category">The category as a type.</param>
        /// <returns>Logger that uses specified category when no other category is provided.</returns>
        public static ILogger GetLogger(this ILogger logger, Type category)
        {
            IgniteArgumentCheck.NotNull(logger, "logger");
            IgniteArgumentCheck.NotNull(category, "category");

            return new CategoryLogger(logger, category.Name);
        }
    }
}
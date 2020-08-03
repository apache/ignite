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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Log;
    using NUnit.Framework;

    /// <summary>
    /// Stores log entries in a list.
    /// </summary>
    public class ListLogger : ILogger
    {
        /** */
        private readonly List<Entry> _entries = new List<Entry>();

        /** */
        private readonly object _lock = new object();

        /** */
        private readonly ILogger _wrappedLogger;

        /// <summary>
        /// Initializes a new instance of <see cref="ListLogger"/> class.
        /// </summary>
        public ListLogger(ILogger wrappedLogger = null)
        {
            _wrappedLogger = wrappedLogger;
            EnabledLevels = new[] {LogLevel.Debug, LogLevel.Warn, LogLevel.Error};
        }
        
        /// <summary>
        /// Gets or sets enabled levels.
        /// </summary>
        public LogLevel[] EnabledLevels { get; set; }

        /// <summary>
        /// Gets the entries.
        /// </summary>
        public List<Entry> Entries
        {
            get
            {
                lock (_lock)
                {
                    return _entries.ToList();
                }
            }
        }

        /// <summary>
        /// Clears the entries.
        /// </summary>
        public void Clear()
        {
            lock (_lock)
            {
                _entries.Clear();
            }
        }

        /** <inheritdoc /> */
        public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, string category,
            string nativeErrorInfo, Exception ex)
        {
            Assert.NotNull(message);

            if (!IsEnabled(level))
            {
                return;
            }
            
            if (_wrappedLogger != null)
            {
                _wrappedLogger.Log(level, message, args, formatProvider, category, nativeErrorInfo, ex);
            }
            
            lock (_lock)
            {
                if (args != null)
                {
                    message = string.Format(formatProvider, message, args);
                }

                if (ex != null)
                {
                    message += Environment.NewLine + ex;
                }
                
                _entries.Add(new Entry(message, level, category));
            }
        }

        /** <inheritdoc /> */
        public bool IsEnabled(LogLevel level)
        {
            return EnabledLevels.Contains(level);
        }

        /// <summary>
        /// Log entry.
        /// </summary>
        public class Entry
        {
            /** */
            private readonly string _message;
            
            /** */
            private readonly LogLevel _level;
            
            /** */
            private readonly string _category;

            /// <summary>
            /// Initializes a new instance of <see cref="Entry"/> class.
            /// </summary>
            public Entry(string message, LogLevel level, string category)
            {
                Assert.NotNull(message);
                
                _message = message;
                _level = level;
                _category = category;
            }

            /// <summary>
            /// Gets the message.
            /// </summary>
            public string Message
            {
                get { return _message; }
            }
            
            /// <summary>
            /// Gets the level.
            /// </summary>
            public LogLevel Level
            {
                get { return _level; }
            }

            /// <summary>
            /// Gets the category.
            /// </summary>
            public string Category
            {
                get { return _category; }
            }

            /** <inheritdoc /> */
            public override string ToString()
            {
                return string.Format("{0} [Level={1}, Message={2}]", GetType().Name, Level, Message);
            }
        }
    }
}

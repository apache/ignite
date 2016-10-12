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

namespace Apache.Ignite.Core.Plugin
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Event arguments for plugin callback.
    /// </summary>
    public class PluginCallbackEventArgs : EventArgs
    {
        /** */
        private readonly IBinaryRawReader _reader;

        /** */
        private readonly IBinaryRawWriter _writer;

        /// <summary>
        /// Initializes a new instance of the <see cref="PluginCallbackEventArgs" /> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <param name="writer">The writer.</param>
        internal PluginCallbackEventArgs(IBinaryRawReader reader, IBinaryRawWriter writer)
        {
            Debug.Assert(reader != null);
            Debug.Assert(writer != null);

            _reader = reader;
            _writer = writer;
        }

        /// <summary>
        /// Gets the reader for the stream with event data.
        /// </summary>
        public IBinaryRawReader Reader
        {
            get { return _reader; }
        }

        /// <summary>
        /// Gets the writer for event handing results.
        /// </summary>
        public IBinaryRawWriter Writer
        {
            get { return _writer; }
        }
    }
}
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

namespace Apache.Ignite.Core.Tests.Plugin
{
    using System;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Test custom-mapped plugin exception.
    /// </summary>
    public class TestIgnitePluginException : IgniteException
    {
        /** */
        private readonly string _className;

        /** */
        private readonly IIgnite _ignite;

        /// <summary>
        /// Initializes a new instance of the <see cref="TestIgnitePluginException" /> class.
        /// </summary>
        /// <param name="className">Name of the class.</param>
        /// <param name="message">Message.</param>
        /// <param name="ignite">Ignite.</param>
        /// <param name="cause">Cause.</param>
        public TestIgnitePluginException(string className, string message, IIgnite ignite, Exception cause) 
            : base(message, cause)
        {
            _className = className;
            _ignite = ignite;
        }

        /// <summary>
        /// Gets the name of the class.
        /// </summary>
        public string ClassName
        {
            get { return _className; }
        }

        /// <summary>
        /// Gets the ignite.
        /// </summary>
        public IIgnite Ignite
        {
            get { return _ignite; }
        }
    }
}

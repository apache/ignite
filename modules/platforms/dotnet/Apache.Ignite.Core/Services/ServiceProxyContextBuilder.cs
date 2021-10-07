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

namespace Apache.Ignite.Core.Services
{
    using System.Collections;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Impl.Services;

    /// <summary>
    /// Service proxy context builder.
    /// </summary>
    public class ServiceProxyContextBuilder
    {
        /** Context attributes. */
        private readonly Hashtable _values;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ServiceProxyContextBuilder()
        {
            _values = new Hashtable();
        }
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Context attribute name.</param>
        /// <param name="value">Context attribute value.</param>
        public ServiceProxyContextBuilder(string name, object value)
        {
            _values = new Hashtable {{name, value}};
        }
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="values">Context attributes.</param>
        public ServiceProxyContextBuilder(Dictionary<string, object> values)
        {
            _values = new Hashtable(values);
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Context attribute name.</param>
        /// <param name="value">Context attribute value.</param>
        public ServiceProxyContextBuilder Add(string name, object value)
        {
            _values.Add(name, value);

            return this;
        }

        /// <summary>
        /// Create an instance of service proxy context.
        /// </summary>
        /// <returns>Service proxy context.</returns>
        internal ServiceProxyContext Build()
        {
            return new ServiceProxyContextImpl(_values);
        }
    }
}
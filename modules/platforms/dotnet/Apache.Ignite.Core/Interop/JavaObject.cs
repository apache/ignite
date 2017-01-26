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

namespace Apache.Ignite.Core.Interop
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Represents a Java object wrapper.
    /// <para />
    /// <see cref="JavaObject"/> can be converted to Ignite filters and predicates 
    /// which can be used on non-.NET Ignite nodes.
    /// <para />
    /// Workflow is as follows:
    /// Instantiate specified Java class;
    /// Set property values;
    /// If the resulting object implements PlatformJavaObjectFactory, call create() method and use the result,
    /// otherwise use the original object.
    /// </summary>
    public class JavaObject
    {
        /** Java class name. */
        private readonly string _className;

        /** Properties. */
        private readonly IDictionary<string, object> _properties = new Dictionary<string, object>();

        /// <summary>
        /// Initializes a new instance of the <see cref="JavaObject"/> class.
        /// </summary>
        /// <param name="className">Name of the Java class.</param>
        public JavaObject(string className)
        {
            IgniteArgumentCheck.NotNullOrEmpty(className, "className");

            _className = className;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JavaObject"/> class.
        /// </summary>
        /// <param name="className">Name of the Java class.</param>
        /// <param name="properties">The properties to set on the Java object.</param>
        public JavaObject(string className, IDictionary<string, object> properties) : this(className)
        {
            _properties = properties ?? _properties;
        }

        /// <summary>
        /// Gets the Java class name.
        /// </summary>
        public string ClassName
        {
            get { return _className; }
        }

        /// <summary>
        /// Gets the properties to be set on the Java object.
        /// </summary>
        public IDictionary<string, object> Properties
        {
            get { return _properties; }
        }
    }
}

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

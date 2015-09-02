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

namespace Apache.Ignite.Core.Impl.Portable
{
    using System;

    /// <summary>
    /// Portable builder field.
    /// </summary>
    internal class PortableBuilderField
    {
        /** Remove marker object. */
        public static readonly object RmvMarkerObj = new object();

        /** Remove marker. */
        public static readonly PortableBuilderField RmvMarker = 
            new PortableBuilderField(null, RmvMarkerObj);

        /** Type. */
        private readonly Type _typ;

        /** Value. */
        private readonly object _val;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typ">Type.</param>
        /// <param name="val">Value.</param>
        public PortableBuilderField(Type typ, object val)
        {
            _typ = typ;
            _val = val;
        }

        /// <summary>
        /// Type.
        /// </summary>
        public Type Type
        {
            get
            {
                return _typ;
            }
        }

        /// <summary>
        /// Value.
        /// </summary>
        public object Value
        {
            get
            {
                return _val;
            }
        }
    }
}

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

namespace Apache.Ignite.AspNet.Impl
{
    using System;
    using System.Runtime.Serialization;
    using System.Web;
    using System.Web.SessionState;

    /// <summary>
    /// Serializable store data.
    /// </summary>
    [Serializable]
    internal class IgniteSessionStateStoreData : SessionStateStoreData, ISerializable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteSessionStateStoreData"/> class.
        /// </summary>
        /// <param name="sessionItems">The session variables and values for the current session.</param>
        /// <param name="staticObjects">The <see cref="T:System.Web.HttpStaticObjectsCollection" /> 
        /// for the current session.</param>
        /// <param name="timeout">The <see cref="P:System.Web.SessionState.SessionStateStoreData.Timeout" /> 
        /// for the current session.</param>
        public IgniteSessionStateStoreData(ISessionStateItemCollection sessionItems,
            HttpStaticObjectsCollection staticObjects, int timeout) : base(sessionItems, staticObjects, timeout)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteSessionStateStoreData"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected IgniteSessionStateStoreData(SerializationInfo info, StreamingContext ctx)
            : base(ReadSessionItems(info), ReadStaticObjects(info), ReadTimeout(info))
        {
            // TODO
        }

        /** <inheritdoc /> */
        private static int ReadTimeout(SerializationInfo info)
        {
            // TODO
            return 0;
        }

        /** <inheritdoc /> */
        private static HttpStaticObjectsCollection ReadStaticObjects(SerializationInfo info)
        {
            // TODO
            return new HttpStaticObjectsCollection();
        }

        /** <inheritdoc /> */
        private static ISessionStateItemCollection ReadSessionItems(SerializationInfo info)
        {
            // TODO
            return new SessionStateItemCollection();
        }


        /** <inheritdoc /> */
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            // TODO
        }
    }
}

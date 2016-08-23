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
    using System.Diagnostics;
    using System.Web.SessionState;

    /// <summary>
    /// <see cref="SessionStateStoreData"/> serializer.
    /// </summary>
    internal static class SessionStateStoreDataSerializer
    {
        public static byte[] Serialize(SessionStateStoreData data)
        {
            Debug.Assert(data != null);

            return null;
        }

        public static SessionStateStoreData Deserialize(byte[] bytes)
        {
            Debug.Assert(bytes != null);

            return null;
        }
    }
}

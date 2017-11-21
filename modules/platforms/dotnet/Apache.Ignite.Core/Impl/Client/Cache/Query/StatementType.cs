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

namespace Apache.Ignite.Core.Impl.Client.Cache.Query
{
    /// <summary>
    /// Query request type.
    /// <para />
    /// When the client knows expected kind of query, we can fail earlier on server.
    /// </summary>
    internal enum StatementType : byte
    {
        /// <summary>
        /// Any query, SQL or DML.
        /// </summary>
        Any = 0,

        /// <summary>
        /// Select query, "SELECT .. FROM".
        /// </summary>
        Select = 1,

        /// <summary>
        /// Update (DML) query, "UPDATE .. ", "INSERT INTO ..".
        /// </summary>
        Update = 2
    }
}

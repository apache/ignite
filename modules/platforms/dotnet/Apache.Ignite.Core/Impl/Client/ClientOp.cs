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

namespace Apache.Ignite.Core.Impl.Client
{
    /// <summary>
    /// Client op code.
    /// </summary>
    internal enum ClientOp : short
    {
        // General purpose.
        ResourceClose = 0,

        // Cache.
        CacheGet = 1000,
        CachePut = 1001,
        CachePutIfAbsent = 1002,
        CacheGetAll = 1003,
        CachePutAll = 1004,
        CacheGetAndPut = 1005,
        CacheGetAndReplace = 1006,
        CacheGetAndRemove = 1007,
        CacheGetAndPutIfAbsent = 1008,
        CacheReplace = 1009,
        CacheReplaceIfEquals = 1010,
        CacheContainsKey = 1011,
        CacheContainsKeys = 1012,
        CacheClear = 1013,
        CacheClearKey = 1014,
        CacheClearKeys = 1015,
        CacheRemoveKey = 1016,
        CacheRemoveIfEquals = 1017,
        CacheRemoveKeys = 1018,
        CacheRemoveAll = 1019,
        CacheGetSize = 1020,
        CacheGetNames = 1050,
        CacheCreateWithName = 1051,
        CacheGetOrCreateWithName = 1052,
        CacheCreateWithConfiguration = 1053,
        CacheGetOrCreateWithConfiguration = 1054,
        CacheGetConfiguration = 1055,
        CacheDestroy = 1056,
        
        // Queries.
        QueryScan = 2000,
        QueryScanCursorGetPage = 2001,
        QuerySql = 2002,
        QuerySqlCursorGetPage = 2003,
        QuerySqlFields = 2004,
        QuerySqlFieldsCursorGetPage = 2005,

        // Metadata.
        BinaryTypeNameGet = 3000,
        BinaryTypeNamePut = 3001,
        BinaryTypeGet = 3002,
        BinaryTypePut = 3003
    }
}

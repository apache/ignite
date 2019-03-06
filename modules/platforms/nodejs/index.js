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

'use strict';

module.exports = require('./lib/IgniteClient');
module.exports.ObjectType = require('./lib/ObjectType').ObjectType;
module.exports.MapObjectType = require('./lib/ObjectType').MapObjectType;
module.exports.CollectionObjectType = require('./lib/ObjectType').CollectionObjectType;
module.exports.ComplexObjectType = require('./lib/ObjectType').ComplexObjectType;
module.exports.ObjectArrayType = require('./lib/ObjectType').ObjectArrayType;
module.exports.BinaryObject = require('./lib/BinaryObject');
module.exports.Timestamp = require('./lib/Timestamp');
module.exports.EnumItem = require('./lib/EnumItem');
module.exports.Decimal = require('decimal.js');
module.exports.Errors = require('./lib/Errors');
module.exports.IgniteClientConfiguration = require('./lib/IgniteClientConfiguration');
module.exports.CacheClient = require('./lib/CacheClient');
module.exports.CacheEntry = require('./lib/CacheClient').CacheEntry;
module.exports.CacheConfiguration = require('./lib/CacheConfiguration');
module.exports.QueryEntity = require('./lib/CacheConfiguration').QueryEntity;
module.exports.QueryField = require('./lib/CacheConfiguration').QueryField;
module.exports.QueryIndex = require('./lib/CacheConfiguration').QueryIndex;
module.exports.CacheKeyConfiguration = require('./lib/CacheConfiguration').CacheKeyConfiguration;
module.exports.SqlQuery = require('./lib/Query').SqlQuery;
module.exports.SqlFieldsQuery = require('./lib/Query').SqlFieldsQuery;
module.exports.ScanQuery = require('./lib/Query').ScanQuery;
module.exports.Cursor = require('./lib/Cursor').Cursor;
module.exports.SqlFieldsCursor = require('./lib/Cursor').SqlFieldsCursor;

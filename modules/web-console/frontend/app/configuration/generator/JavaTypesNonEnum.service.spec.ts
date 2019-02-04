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

import {JavaTypesNonEnum} from './JavaTypesNonEnum.service';

import ClusterDflts from './generator/defaults/Cluster.service';
import CacheDflts from './generator/defaults/Cache.service';
import IgfsDflts from './generator/defaults/IGFS.service';
import JavaTypes from 'app/services/JavaTypes.service';

const instance = new JavaTypesNonEnum(new ClusterDflts(), new CacheDflts(), new IgfsDflts(), new JavaTypes());

import { assert } from 'chai';

suite('JavaTypesNonEnum', () => {
    test('nonEnum', () => {
        assert.equal(instance.nonEnum('org.apache.ignite.cache.CacheMode'), false);
        assert.equal(instance.nonEnum('org.apache.ignite.transactions.TransactionConcurrency'), false);
        assert.equal(instance.nonEnum('org.apache.ignite.cache.CacheWriteSynchronizationMode'), false);
        assert.equal(instance.nonEnum('org.apache.ignite.igfs.IgfsIpcEndpointType'), false);
        assert.equal(instance.nonEnum('java.io.Serializable'), true);
        assert.equal(instance.nonEnum('BigDecimal'), true);
    });
});

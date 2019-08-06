/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {suite, test} from 'mocha';
import {assert} from 'chai';

import Worker from './decompress.worker';
import SimpleWorkerPool from '../../utils/SimpleWorkerPool';

suite('Decompress worker', () => {
    const pool = new SimpleWorkerPool('decompressor', Worker, 4);

    test('Big number in properties', async() => {
        const data = '{"str":"ok","str_num":"sd 9223372036854775807dsds","small_num":12345678901234,"small_num_float":0.123400000000000,"big_num":9223372036854775807}';

        const res = await pool.postMessage(data);

        assert.isString(res.str, 'has str string');
        assert.isString(res.str_num, 'has str_num string');
        assert.isNumber(res.small_num, 'has small_num number');
        assert.isNumber(res.small_num_float, 'has small_num_float number');
        assert.isString(res.big_num, 'has big_num string');
    });

    test('Big number in array', async() => {
        const data = '{"array":[9223372036854775807, "ok","sd 9223372036854775807dsds",12345678901234,0.123400000000000,9223372036854775807]}';

        const {array} = await pool.postMessage(data);

        assert.isString(array[0], 'has str big_num');
        assert.isString(array[1], 'has str string');
        assert.isString(array[2], 'has str_num string');
        assert.isNumber(array[3], 'has small_num number');
        assert.isNumber(array[4], 'has small_num_float number');
        assert.isString(array[5], 'has big_num string');
    });
});

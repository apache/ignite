

import {suite, test} from 'mocha';
import {assert} from 'chai';

import Worker from './decompress.worker';
import SimpleWorkerPool from '../../utils/SimpleWorkerPool';

suite('Decompress worker', () => {
    const pool = new SimpleWorkerPool('decompressor', Worker, 4);

    test('Big number in properties', async() => {
        const data = '{"str":"ok","str_num":"sd 9223372036854775807dsds","small_num":12345678901234,"small_num_float":0.123400000000000,"big_num":9223372036854775807, "big_negative_number":-9223372036847675801}';

        const res = await pool.postMessage(data);

        assert.strictEqual(res.str, 'ok', 'has str string');
        assert.strictEqual(res.str_num, 'sd 9223372036854775807dsds', 'has str_num string');
        assert.strictEqual(res.small_num, 12345678901234, 'has small_num number');
        assert.strictEqual(res.small_num_float, 0.123400000000000, 'has small_num_float number');
        assert.strictEqual(res.big_num, '9223372036854775807', 'has big_num string');
        assert.strictEqual(res.big_negative_number, '-9223372036847675801', 'has big_negative_number string');
    });

    test('Big number in array', async() => {
        const data = '{"array":[9223372036854775807, "ok","sd 9223372036854775807dsds",12345678901234,0.123400000000000,-9223372036847675801]}';

        const {array} = await pool.postMessage(data);

        assert.strictEqual(array[0], '9223372036854775807', 'has big_num string');
        assert.strictEqual(array[1], 'ok', 'has str string');
        assert.strictEqual(array[2], 'sd 9223372036854775807dsds', 'has str_num string');
        assert.strictEqual(array[3], 12345678901234, 'has small_num number');
        assert.strictEqual(array[4], 0.123400000000000, 'has small_num_float number');
        assert.strictEqual(array[5], '-9223372036847675801', 'has big_negative_number string');
    });
});

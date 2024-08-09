

import bytesFilter from './bytes.filter';

import { suite, test } from 'mocha';
import { assert } from 'chai';

const bytesFilterInstance = bytesFilter();

suite('bytes filter', () => {
    test('bytes filter', () => {
        assert.equal(bytesFilterInstance(0), '0 bytes');
        assert.equal(bytesFilterInstance(1000), '1000.0 bytes');
        assert.equal(bytesFilterInstance(1024), '1.0 kB');
        assert.equal(bytesFilterInstance(5000), '4.9 kB');
        assert.equal(bytesFilterInstance(1048576), '1.0 MB');
        assert.equal(bytesFilterInstance(104857600), '100.0 MB');
        assert.equal(bytesFilterInstance(1073741824), '1.0 GB');
        assert.equal(bytesFilterInstance(1099511627776), '1.0 TB');
    });
});

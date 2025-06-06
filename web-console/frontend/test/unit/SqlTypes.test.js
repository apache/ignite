

import SqlTypes from '../../app/services/SqlTypes.service';

const INSTANCE = new SqlTypes();

import { suite, test } from 'mocha';
import { assert } from 'chai';

suite('SqlTypesTestsSuite', () => {
    test('validIdentifier', () => {
        assert.equal(INSTANCE.validIdentifier('myIdent'), true);
        assert.equal(INSTANCE.validIdentifier('java.math.BigDecimal'), false);
        assert.equal(INSTANCE.validIdentifier('2Demo'), false);
        assert.equal(INSTANCE.validIdentifier('abra kadabra'), false);
        assert.equal(INSTANCE.validIdentifier(), false);
        assert.equal(INSTANCE.validIdentifier(null), false);
        assert.equal(INSTANCE.validIdentifier(''), false);
        assert.equal(INSTANCE.validIdentifier(' '), false);
    });

    test('isKeyword', () => {
        assert.equal(INSTANCE.isKeyword('group'), true);
        assert.equal(INSTANCE.isKeyword('Group'), true);
        assert.equal(INSTANCE.isKeyword('select'), true);
        assert.equal(INSTANCE.isKeyword('abra kadabra'), false);
        assert.equal(INSTANCE.isKeyword(), false);
        assert.equal(INSTANCE.isKeyword(null), false);
        assert.equal(INSTANCE.isKeyword(''), false);
        assert.equal(INSTANCE.isKeyword(' '), false);
    });

    test('findJdbcType', () => {
        assert.equal(INSTANCE.findJdbcType(0).dbName, 'NULL');
        assert.equal(INSTANCE.findJdbcType(5555).dbName, 'Unknown');
    });
});

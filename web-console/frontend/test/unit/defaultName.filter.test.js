

import defaultName from '../../app/filters/default-name.filter';

import { assert } from 'chai';

const instance = defaultName();

suite('defaultName', () => {
    test('defaultName filter', () => {
        let undef;

        assert.equal(instance(''), '<default>');
        assert.equal(instance(null), '<default>');
        assert.equal(instance(), '<default>');
        assert.equal(instance('', false), '<default>');
        assert.equal(instance(null, false), '<default>');
        assert.equal(instance(undef, false), '<default>');
        assert.equal(instance('', true), '&lt;default&gt;');
        assert.equal(instance(null, true), '&lt;default&gt;');
        assert.equal(instance(undef, true), '&lt;default&gt;');
        assert.equal(instance('name', false), 'name');
        assert.equal(instance('name', true), 'name');
    });
});

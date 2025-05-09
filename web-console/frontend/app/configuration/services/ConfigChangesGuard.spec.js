

import {assert} from 'chai';
import {IgniteObjectDiffer} from './ConfigChangesGuard';

suite('Config changes guard', () => {
    test('Object differ', () => {
        const differ = new IgniteObjectDiffer();

        assert.isUndefined(
            differ.diff({a: void 0}, {a: false}),
            'No changes when boolean values changes from undefined to false'
        );

        assert.isUndefined(
            differ.diff({a: void 0}, {a: null}),
            'No changes when undefined value changes to null'
        );

        assert.isUndefined(
            differ.diff({a: void 0}, {a: ''}),
            'No changes when undefined value changes to an empty string'
        );
    });
});

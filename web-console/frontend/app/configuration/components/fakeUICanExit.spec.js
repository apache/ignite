

import {spy} from 'sinon';
import {assert} from 'chai';
import {FakeUiCanExitController} from './fakeUICanExit';

suite('Page configuration fakeUIcanExit directive', () => {
    test('It unsubscribes from state events when destroyed', () => {
        const $element = {data: () => [{uiCanExit: () => {}}]};
        const off = spy();
        const $transitions = {onBefore: () => off};
        const i = new FakeUiCanExitController($element, $transitions);
        i.$onInit();
        i.$onDestroy();
        assert.ok(off.calledOnce, 'Calls off when destroyed');
    });
});

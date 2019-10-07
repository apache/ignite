/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

import { assert } from 'chai';
import { NotebookCtrl } from './controller';

const inject = ($inject: string[], impls: {[symbol: string]: any}) => $inject.map(symbol => impls[symbol] || {});
// assert.doesNotHrow does not support async functions
const doesNotThrowAsync = async(fn, message: string) => {
    try {
        await fn();
    } catch (e) {
        assert.notOk(e, message);
    }
};

suite('Query Notebook component', () => {
    suite('Close open queries', () => {
        // Regression test for https://ggsystems.atlassian.net/browse/GG-19358
        test('This in closeOpenedQueries callback', async() => {
            const IgniteConfirm = {confirm: () => Promise.resolve(true)};
            const $translate = {instant() {}};
            const notebook = {paragraphs: [{loading: true}]};
            const i = new NotebookCtrl(...inject(NotebookCtrl.$inject, {
                IgniteLegacyUtils: {mkOptions: () => {}},
                $scope: {notebook},
                IgniteConfirm,
                $translate,
                $filter: () => {},
                IgniteNotebook: {find() {
                    return Promise.resolve(notebook);
                }},
                $state: {params: {}},
                $transitions: {onBefore() {}},
                $window: {addEventListener() {}},
                AgentManager: {addClusterSwitchListener() {}},
                IgniteLoading: {finish() {}}

            }));

            await doesNotThrowAsync(i.closeOpenedQueries.bind(null), 'It should not throw errors with incorrect "this" context');
        });
    });
});

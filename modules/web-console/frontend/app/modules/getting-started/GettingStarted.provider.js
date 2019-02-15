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

import angular from 'angular';

/**
 * @typedef GettingStartedItem
 * @prop {string} title
 * @prop {Array<string>} message
 */

/**
 * @typedef {Array<GettingStartedItem>} GettingStartedItems
 */

import PAGES from 'app/data/getting-started.json';
import templateUrl from 'views/templates/getting-started.tpl.pug';

export function provider() {
    /**
     * Getting started pages.
     * @type {GettingStartedItems}
     */
    const items = PAGES;

    this.push = (before, data) => {
        const idx = _.findIndex(items, {title: before});

        if (idx < 0)
            items.push(data);
        else
            items.splice(idx, 0, data);
    };

    this.update = (before, data) => {
        const idx = _.findIndex(items, {title: before});

        if (idx >= 0)
            items[idx] = data;
    };

    this.$get = function() {
        return items;
    };

    return this;
}

/**
 * @param {ng.IRootScopeService} $root
 * @param {mgcrea.ngStrap.modal.IModalService} $modal
 * @param {GettingStartedItems} igniteGettingStarted
 */
export function service($root, $modal, igniteGettingStarted) {
    const _model = igniteGettingStarted;

    let _page = 0;

    const scope = $root.$new();

    scope.ui = {
        dontShowGettingStarted: false
    };

    function _fillPage() {
        if (_page === 0)
            scope.title = `${_model[_page].title}`;
        else
            scope.title = `${_page}. ${_model[_page].title}`;

        scope.message = _model[_page].message.join(' ');
    }

    scope.isFirst = () => _page === 0;

    scope.isLast = () => _page === _model.length - 1;

    scope.next = () => {
        _page += 1;

        _fillPage();
    };

    scope.prev = () => {
        _page -= 1;

        _fillPage();
    };

    const dialog = $modal({ templateUrl, scope, show: false, backdrop: 'static'});

    scope.close = () => {
        try {
            localStorage.showGettingStarted = !scope.ui.dontShowGettingStarted;
        }
        catch (ignore) {
            // No-op.
        }

        dialog.hide();
    };

    return {
        /**
         * @param {boolean} force
         */
        tryShow: (force) => {
            try {
                scope.ui.dontShowGettingStarted = !(_.isNil(localStorage.showGettingStarted)
                        || localStorage.showGettingStarted === 'true');
            }
            catch (ignore) {
                // No-op.
            }

            if (force || !scope.ui.dontShowGettingStarted) {
                _page = 0;

                _fillPage();

                dialog.$promise.then(dialog.show);
            }
        }
    };
}

service.$inject = ['$rootScope', '$modal', 'igniteGettingStarted'];

export default angular
    .module('ignite-console.getting-started', [])
    .provider('igniteGettingStarted', provider)
    .service('gettingStarted', service);

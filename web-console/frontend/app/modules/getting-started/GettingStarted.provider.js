

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
        catch (ignored) {
            // No-op.
        }

        dialog.hide();
    };

    return {
        tryShow: (force = false) => {
            try {
                scope.ui.dontShowGettingStarted = !(_.isNil(localStorage.showGettingStarted)
                        || localStorage.showGettingStarted === 'true');
            }
            catch (ignored) {
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

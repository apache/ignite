/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import angular from 'angular';

// Getting started pages.
import PAGES from 'app/data/getting-started.json';
import templateUrl from 'views/templates/getting-started.tpl.pug';

angular
    .module('ignite-console.getting-started', [])
    .provider('igniteGettingStarted', function() {
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

        this.$get = [function() {
            return items;
        }];
    })
    .service('gettingStarted', ['$rootScope', '$modal', 'igniteGettingStarted', function($root, $modal, igniteGettingStarted) {
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
    }]);

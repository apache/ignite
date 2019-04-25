/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import _ from 'lodash';

/**
 * @param {ng.ITimeoutService} $timeout
 */
export default function factory($timeout) {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} el
     * @param {ng.IAttributes} attrs
     */
    const link = (scope, el, attrs) => {
        if (_.isUndefined(attrs.igniteFormFieldInputAutofocus) || attrs.igniteFormFieldInputAutofocus !== 'true')
            return;

        $timeout(() => el.focus(), 100);
    };

    return {
        restrict: 'A',
        link
    };
}

factory.$inject = ['$timeout'];

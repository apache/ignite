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

const template = `<i class='fa' ng-class='isOpen ? "fa-chevron-circle-down" : "fa-chevron-circle-right"'></i>`; // eslint-disable-line quotes

export default ['igniteFormPanelChevron', [() => {
    const controller = [() => {}];

    const link = ($scope, $element, $attrs, [bsCollapseCtrl]) => {
        const $target = $element.parent().parent().find('.panel-collapse');

        bsCollapseCtrl.$viewChangeListeners.push(function() {
            const index = bsCollapseCtrl.$targets.reduce((acc, el, i) => {
                if (el[0] === $target[0])
                    acc.push(i);

                return acc;
            }, [])[0];

            $scope.isOpen = false;

            const active = bsCollapseCtrl.$activeIndexes();

            if ((active instanceof Array) && active.indexOf(index) !== -1 || active === index)
                $scope.isOpen = true;
        });
    };

    return {
        restrict: 'E',
        scope: {},
        link,
        template,
        controller,
        replace: true,
        transclude: true,
        require: ['^bsCollapse']
    };
}]];

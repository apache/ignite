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

const template = `
    <svg
        class='copy-input-value-button'
        ignite-icon='copy'
        ignite-copy-to-clipboard='{{ $ctrl.value }}'
        bs-tooltip=''
        data-title='{{::$ctrl.title}}'
        ng-show='$ctrl.value'
    ></svg>
`;

class CopyInputValueButtonController {
    /** @type {ng.INgModelController} */
    ngModel;

    /**
     * Tooltip title
     * @type {string}
     */
    title;

    static $inject = ['$element', '$compile', '$scope'];

    /**
     * @param {JQLite} $element
     * @param {ng.ICompileService} $compile
     * @param {ng.IScope} $scope
     */
    constructor($element, $compile, $scope) {
        this.$element = $element;
        this.$compile = $compile;
        this.$scope = $scope;
    }

    $postLink() {
        this.buttonScope = this.$scope.$new(true);
        this.buttonScope.$ctrl = this;
        this.$compile(template)(this.buttonScope, (clone) => {
            this.$element[0].parentElement.appendChild(clone[0]);
            this.buttonElement = clone;
        });
    }

    $onDestroy() {
        this.buttonScope.$ctrl = null;
        this.buttonScope.$destroy();
        this.buttonElement.remove();
        this.buttonElement = this.$element = this.ngModel = null;
    }

    get value() {
        return this.ngModel
            ? this.ngModel.$modelValue
            : void 0;
    }
}

export function directive() {
    return {
        scope: false,
        bindToController: {
            title: '@copyInputValueButton'
        },
        controller: CopyInputValueButtonController,
        require: {
            ngModel: 'ngModel'
        }
    };
}

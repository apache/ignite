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

export class IgniteFormField {
    static animName = 'ignite-form-field__error-blink';
    static eventName = 'webkitAnimationEnd oAnimationEnd msAnimationEnd animationend';
    static $inject = ['$element', '$scope'];
    constructor($element, $scope) {
        Object.assign(this, {$element});
        this.$scope = $scope;
    }
    $postLink() {
        this.onAnimEnd = () => this.$element.removeClass(IgniteFormField.animName);
        this.$element.on(IgniteFormField.eventName, this.onAnimEnd);
    }
    $onDestroy() {
        this.$element.off(IgniteFormField.eventName, this.onAnimEnd);
        this.$element = this.onAnimEnd = null;
    }
    notifyAboutError() {
        if (this.$element) this.$element.addClass(IgniteFormField.animName);
    }
    /**
     * Exposes control in $scope
     * @param {ng.INgModelController} control
     */
    exposeControl(control, name = '$input') {
        this.$scope[name] = control;
        this.$scope.$on('$destroy', () => this.$scope[name] = null);
    }
}

export default angular.module('ignite-console.page-configure.validation', [])
    .directive('pcNotInCollection', function() {
        class Controller {
            /** @type {ng.INgModelController} */
            ngModel;
            /** @type {Array} */
            items;

            $onInit() {
                this.ngModel.$validators.notInCollection = (item) => {
                    if (!this.items) return true;
                    return !this.items.includes(item);
                };
            }

            $onChanges() {
                this.ngModel.$validate();
            }
        }

        return {
            controller: Controller,
            require: {
                ngModel: 'ngModel'
            },
            bindToController: {
                items: '<pcNotInCollection'
            }
        };
    })
    .directive('pcInCollection', function() {
        class Controller {
            /** @type {ng.INgModelController} */
            ngModel;
            /** @type {Array} */
            items;
            /** @type {string} */
            pluck;

            $onInit() {
                this.ngModel.$validators.inCollection = (item) => {
                    if (!this.items) return false;
                    const items = this.pluck ? this.items.map((i) => i[this.pluck]) : this.items;
                    return Array.isArray(item)
                        ? item.every((i) => items.includes(i))
                        : items.includes(item);
                };
            }

            $onChanges() {
                this.ngModel.$validate();
            }
        }

        return {
            controller: Controller,
            require: {
                ngModel: 'ngModel'
            },
            bindToController: {
                items: '<pcInCollection',
                pluck: '@?pcInCollectionPluck'
            }
        };
    })
    .directive('pcPowerOfTwo', function() {
        class Controller {
            /** @type {ng.INgModelController} */
            ngModel;
            $onInit() {
                this.ngModel.$validators.powerOfTwo = (value) => {
                    return !value || ((value & -value) === value);
                };
            }
        }

        return {
            controller: Controller,
            require: {
                ngModel: 'ngModel'
            },
            bindToController: true
        };
    })
    .directive('bsCollapseTarget', function() {
        return {
            require: {
                bsCollapse: '^^bsCollapse'
            },
            bindToController: true,
            controller: ['$element', '$scope', function($element, $scope) {
                this.open = function() {
                    const index = this.bsCollapse.$targets.indexOf($element);
                    const isActive = this.bsCollapse.$targets.$active.includes(index);
                    if (!isActive) this.bsCollapse.$setActive(index);
                };
                this.$onDestroy = () => this.open = $element = null;
            }]
        };
    })
    .directive('ngModel', ['$timeout', function($timeout) {
        return {
            require: ['ngModel', '?^^bsCollapseTarget', '?^^igniteFormField', '?^^panelCollapsible'],
            link(scope, el, attr, [ngModel, bsCollapseTarget, igniteFormField, panelCollapsible]) {
                const off = scope.$on('$showValidationError', (e, target) => {
                    if (target !== ngModel) return;
                    ngModel.$setTouched();
                    bsCollapseTarget && bsCollapseTarget.open();
                    panelCollapsible && panelCollapsible.open();
                    $timeout(() => {
                        if (el[0].scrollIntoViewIfNeeded)
                            el[0].scrollIntoViewIfNeeded();
                        else
                            el[0].scrollIntoView();

                        if (!attr.bsSelect) $timeout(() => el[0].focus());
                        igniteFormField && igniteFormField.notifyAboutError();
                    });
                });
            }
        };
    }])
    .directive('igniteFormField', function() {
        return {
            restrict: 'C',
            controller: IgniteFormField,
            scope: true
        };
    })
    .directive('isValidJavaIdentifier', ['IgniteLegacyUtils', function(LegacyUtils) {
        return {
            link(scope, el, attr, ngModel) {
                ngModel.$validators.isValidJavaIdentifier = (value) => LegacyUtils.VALID_JAVA_IDENTIFIER.test(value);
            },
            require: 'ngModel'
        };
    }])
    .directive('notJavaReservedWord', ['IgniteLegacyUtils', function(LegacyUtils) {
        return {
            link(scope, el, attr, ngModel) {
                ngModel.$validators.notJavaReservedWord = (value) => !LegacyUtils.JAVA_KEYWORDS.includes(value);
            },
            require: 'ngModel'
        };
    }]);

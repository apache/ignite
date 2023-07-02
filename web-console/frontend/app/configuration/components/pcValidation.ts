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

import LegacyUtilsFactory from 'app/services/LegacyUtils.service';

export default angular.module('ignite-console.page-configure.validation', [])
    .directive('pcNotInCollection', function() {
        class Controller<T> {
            ngModel: ng.INgModelController;
            items: T[];

            $onInit() {
                this.ngModel.$validators.notInCollection = (item: T) => {
                    if (!this.items)
                        return true;

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
        class Controller<T> {
            ngModel: ng.INgModelController;
            items: T[];
            pluck?: string;

            $onInit() {
                this.ngModel.$validators.inCollection = (item: T) => {
                    if (!this.items)
                        return false;

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
            ngModel: ng.INgModelController;
            $onInit() {
                this.ngModel.$validators.powerOfTwo = (value: number) => {
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
    .directive('isValidJavaIdentifier', ['IgniteLegacyUtils', function(LegacyUtils: ReturnType<typeof LegacyUtilsFactory>) {
        return {
            link(scope, el, attr, ngModel: ng.INgModelController) {
                ngModel.$validators.isValidJavaIdentifier = (value: string) => LegacyUtils.VALID_JAVA_IDENTIFIER.test(value);
            },
            require: 'ngModel'
        };
    }])
    .directive('notJavaReservedWord', ['IgniteLegacyUtils', function(LegacyUtils: ReturnType<typeof LegacyUtilsFactory>) {
        return {
            link(scope, el, attr, ngModel: ng.INgModelController) {
                ngModel.$validators.notJavaReservedWord = (value: string) => !LegacyUtils.JAVA_KEYWORDS.includes(value);
            },
            require: 'ngModel'
        };
    }]);

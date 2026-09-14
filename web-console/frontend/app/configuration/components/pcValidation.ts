

import LegacyUtilsFactory from 'app/services/LegacyUtils.service';
import JavaTypes from 'app/services/JavaTypes.service';

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
    .directive('isValidJavaIdentifier', ['JavaTypes', function(JavaTypes: JavaTypes) {
        return {
            link(scope, el, attr, ngModel: ng.INgModelController) {
                ngModel.$validators.isValidJavaIdentifier = (value: string) => JavaTypes.isValidJavaIdentifier(value);
            },
            require: 'ngModel'
        };
    }])
    .directive('notJavaReservedWord', ['JavaTypes', function(JavaTypes: JavaTypes) {
        return {
            link(scope, el, attr, ngModel: ng.INgModelController) {
                ngModel.$validators.notJavaReservedWord = (value: string) => !JavaTypes.isKeyword(value);
            },
            require: 'ngModel'
        };
    }]);

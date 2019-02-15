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

import negate from 'lodash/negate';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import mixin from 'lodash/mixin';

import {user as userAction, register as registerStore} from './store';
const nonNil = negate(isNil);
const nonEmpty = negate(isEmpty);

mixin({
    nonNil,
    nonEmpty
});

import alertTemplateUrl from 'views/templates/alert.tpl.pug';
import dropdownTemplateUrl from 'views/templates/dropdown.tpl.pug';
import validationTemplateUrl from 'views/templates/validation-error.tpl.pug';

const igniteConsoleCfg = angular.module('ignite-console.config', ['ngAnimate', 'mgcrea.ngStrap']);

igniteConsoleCfg.run(registerStore);

// Configure AngularJS animation: do not animate fa-spin.
igniteConsoleCfg.config(['$animateProvider', ($animateProvider) => {
    $animateProvider.classNameFilter(/^((?!(fa-spin|ng-animate-disabled)).)*$/);
}]);

// AngularStrap modal popup configuration.
igniteConsoleCfg.config(['$modalProvider', ($modalProvider) => {
    Object.assign($modalProvider.defaults, {
        animation: 'am-fade-and-scale',
        placement: 'center',
        html: true
    });
}]);

// AngularStrap popover configuration.
igniteConsoleCfg.config(['$popoverProvider', ($popoverProvider) => {
    Object.assign($popoverProvider.defaults, {
        trigger: 'manual',
        placement: 'right',
        container: 'body',
        templateUrl: validationTemplateUrl
    });
}]);

// AngularStrap tooltips configuration.
igniteConsoleCfg.config(['$tooltipProvider', ($tooltipProvider) => {
    Object.assign($tooltipProvider.defaults, {
        container: 'body',
        delay: {show: 150, hide: 150},
        placement: 'right',
        html: 'true',
        trigger: 'click hover'
    });
}]);

// AngularStrap select (combobox) configuration.
igniteConsoleCfg.config(['$selectProvider', ($selectProvider) => {
    Object.assign($selectProvider.defaults, {
        container: 'body',
        maxLength: '5',
        allText: 'Select All',
        noneText: 'Clear All',
        template: '<bs-select-menu></bs-select-menu>',
        iconCheckmark: 'fa fa-check',
        caretHtml: '',
        animation: ''
    });
}]);

// AngularStrap alerts configuration.
igniteConsoleCfg.config(['$alertProvider', ($alertProvider) => {
    Object.assign($alertProvider.defaults, {
        container: 'body',
        placement: 'top-right',
        duration: '5',
        templateUrl: alertTemplateUrl,
        type: 'danger'
    });
}]);


// AngularStrap dropdowns () configuration.
igniteConsoleCfg.config(['$dropdownProvider', ($dropdownProvider) => {
    Object.assign($dropdownProvider.defaults, {
        templateUrl: dropdownTemplateUrl,
        animation: ''
    });
}]);

// AngularStrap dropdowns () configuration.
igniteConsoleCfg.config(['$datepickerProvider', ($datepickerProvider) => {
    Object.assign($datepickerProvider.defaults, {
        autoclose: true,
        iconLeft: 'icon-datepicker-left',
        iconRight: 'icon-datepicker-right'
    });
}]);

igniteConsoleCfg.config(['$translateProvider', ($translateProvider) => {
    $translateProvider.useSanitizeValueStrategy('sanitize');
}]);

// Restores pre 4.3.0 ui-grid getSelectedRows method behavior
// ui-grid 4.4+ getSelectedRows additionally skips entries without $$hashKey,
// which breaks most of out code that works with selected rows.
igniteConsoleCfg.directive('uiGridSelection', function() {
    function legacyGetSelectedRows() {
        return this.rows.filter((row) => row.isSelected).map((row) => row.entity);
    }
    return {
        require: '^uiGrid',
        restrict: 'A',
        link(scope, el, attr, ctrl) {
            ctrl.grid.api.registerMethodsFromObject({selection: {legacyGetSelectedRows}});
        }
    };
});

igniteConsoleCfg.run(['$rootScope', 'Store', ($root, store) => {
    $root.$on('user', (event, user) => store.dispatch(userAction({...user})));
}]);

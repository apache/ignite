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

import negate from 'lodash/negate';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import mixin from 'lodash/mixin';

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

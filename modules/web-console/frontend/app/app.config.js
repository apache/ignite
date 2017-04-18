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

import _ from 'lodash';
import angular from 'angular';

const nonNil = _.negate(_.isNil);
const nonEmpty = _.negate(_.isEmpty);

_.mixin({
    nonNil,
    nonEmpty
});

import alertTemplateUrl from 'views/templates/alert.tpl.pug';
import selectTemplateUrl from 'views/templates/select.tpl.pug';
import dropdownTemplateUrl from 'views/templates/dropdown.tpl.pug';
import validationTemplateUrl from 'views/templates/validation-error.tpl.pug';

const igniteConsoleCfg = angular.module('ignite-console.config', ['ngAnimate', 'mgcrea.ngStrap']);

// Configure AngularJS animation: do not animate fa-spin.
igniteConsoleCfg.config(['$animateProvider', ($animateProvider) => {
    $animateProvider.classNameFilter(/^((?!(fa-spin)).)*$/);
}]);

// AngularStrap modal popup configuration.
igniteConsoleCfg.config(['$modalProvider', ($modalProvider) => {
    angular.extend($modalProvider.defaults, {
        animation: 'am-fade-and-scale',
        placement: 'center',
        html: true
    });
}]);

// AngularStrap popover configuration.
igniteConsoleCfg.config(['$popoverProvider', ($popoverProvider) => {
    angular.extend($popoverProvider.defaults, {
        trigger: 'manual',
        placement: 'right',
        container: 'body',
        templateUrl: validationTemplateUrl
    });
}]);

// AngularStrap tooltips configuration.
igniteConsoleCfg.config(['$tooltipProvider', ($tooltipProvider) => {
    angular.extend($tooltipProvider.defaults, {
        container: 'body',
        delay: {show: 150, hide: 150},
        placement: 'right',
        html: 'true',
        trigger: 'click hover'
    });
}]);

// AngularStrap select (combobox) configuration.
igniteConsoleCfg.config(['$selectProvider', ($selectProvider) => {
    angular.extend($selectProvider.defaults, {
        container: 'body',
        maxLength: '5',
        allText: 'Select All',
        noneText: 'Clear All',
        templateUrl: selectTemplateUrl,
        iconCheckmark: 'fa fa-check',
        caretHtml: ''
    });
}]);

// AngularStrap alerts configuration.
igniteConsoleCfg.config(['$alertProvider', ($alertProvider) => {
    angular.extend($alertProvider.defaults, {
        container: 'body',
        placement: 'top-right',
        duration: '5',
        templateUrl: alertTemplateUrl,
        type: 'danger'
    });
}]);


// AngularStrap dropdowns () configuration.
igniteConsoleCfg.config(['$dropdownProvider', ($dropdownProvider) => {
    angular.extend($dropdownProvider.defaults, {
        templateUrl: dropdownTemplateUrl
    });
}]);

// AngularStrap dropdowns () configuration.
igniteConsoleCfg.config(['$datepickerProvider', ($datepickerProvider) => {
    angular.extend($datepickerProvider.defaults, {
        autoclose: true,
        iconLeft: 'icon-datepicker-left',
        iconRight: 'icon-datepicker-right'
    });
}]);

igniteConsoleCfg.config(['$translateProvider', ($translateProvider) => {
    $translateProvider.useSanitizeValueStrategy('sanitize');
}]);

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

import jQuery from 'jquery'
import _ from 'lodash'
import ace from 'ace'
import angular from 'angular';

import 'angular-ui-router'
import 'angular-strap'
import 'angular-ui-ace'
import 'angular-tree-control'
import 'angular-smart-table'
import 'angular-animate'
import 'angular-sanitize'
import 'angular-ag-grid'
import 'angular-loading'
import 'angular-drag-and-drop-lists'
import 'angular-nvd3'

import 'bootstrap-carousel'
import 'file-saver'
import 'jszip'
import 'query-command-supported'

import 'public/stylesheets/style.css!'

import 'nvd3/build/nv.d3.css!'
import 'angular-tree-control/css/tree-control-attribute.css!'
import 'angular-tree-control/css/tree-control.css!'
import 'angular-ag-grid/dist/ag-grid.css!'
import 'angular-loading/angular-loading.css!'
import 'angular-motion/dist/angular-motion.css!'

window._ = _;
window.ace = ace;
window.require = ace.require;
window.angular = angular;

import './modules/User/index'
import './modules/Auth/index'

import './modules/states/login/index'
import './modules/states/logout/index'
import './modules/states/password-reset/index'
import './modules/states/configuration/index'
import './modules/states/sql/index'
import './modules/states/profile/index'
import './modules/states/admin/index'

// ignite:modules
import './modules/navbar/main'
import './modules/userbar/main'
import './modules/configuration-sidebar/main'
// endignite

angular
.module('ignite-console', [
	'ui.router',
	// services
	'ignite-console.Auth',
	'ignite-console.User',
	// states
	'ignite-console.states.login',
	'ignite-console.states.logout',
	'ignite-console.states.password',
	'ignite-console.states.configuration',
	'ignite-console.states.sql',
	'ignite-console.states.profile',
	'ignite-console.states.admin',
	//
	'ignite-console.navbar',
	'ignite-console.userbar',
    'ignite-console.configuration-sidebar'
])
.config(['$stateProvider', '$locationProvider', '$urlRouterProvider', function($stateProvider, $locationProvider, $urlRouterProvider) {
	// set up the states
	$stateProvider
	.state('base', {
		url: '',
		abstract: true,
		templateUrl: '/base.html'
	});

	$urlRouterProvider.when('/', '/configuration/clusters');

	$locationProvider.html5Mode(true)
}]);

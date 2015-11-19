import jQuery from 'jquery'
import _ from 'lodash'
import ace from 'ace'
import angular from 'angular';

import 'angular-strap'
import 'angular-ui-ace'
import 'angular-tree-control'
import 'angular-drag-and-drop-lists'
import 'angular-smart-table'
import 'angular-animate'
import 'angular-sanitize'
import 'angular-grid'
import 'angular-loading'
import 'angular-nvd3'
import 'angular-ui-router'

import 'angular-grid/dist/ag-grid.css!'
import 'angular-loading/angular-loading.css!'
import 'angular-tree-control/css/tree-control.css!'
import 'angular-tree-control/css/tree-control-attribute.css!'
import 'angular-motion/dist/angular-motion.css!'

import 'nvd3/build/nv.d3.css!'

import 'spinjs'
import 'query-command-supported'
import 'jszip'
import 'FileSaver'
import 'Blob'

window._ = _;
window.ace = ace;
window.require = ace.require;
window.angular = angular;

import './modules/states/login/index'
import './modules/states/configuration/index'
import './modules/states/sql/index'
import './modules/states/profile/index'
import './modules/states/admin/index'

angular
.module('ignite-console', [
	'ui.router',
	// states
	'ignite-console.states.login',
	'ignite-console.states.configuration',
	'ignite-console.states.sql',
	'ignite-console.states.profile',
	'ignite-console.states.admin'
])
.config(function($stateProvider, $locationProvider) {
	// set up the states
	$stateProvider
	.state('base', {
		url: '',
		abstract: true,
		templateUrl: '/base.html'
	});

	$locationProvider.html5Mode(true)
});

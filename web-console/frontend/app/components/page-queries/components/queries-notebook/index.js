

import angular from 'angular';
import templateUrl from './template.tpl.pug';
import { NotebookCtrl } from './controller';
import NotebookData from '../../notebook.data';
import {component as actions} from './components/query-actions-button/component';
import {default as igniteInformation} from './components/ignite-information/information.directive';
import './style.scss';

export default angular.module('ignite-console.sql.notebook', [])
    .directive('igniteInformation', igniteInformation)
    .component('queryActionsButton', actions)
    .component('queriesNotebook', {
        controller: NotebookCtrl,
        templateUrl
    })
    .service('IgniteNotebookData', NotebookData);

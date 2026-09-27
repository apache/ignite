

import angular from 'angular';
import templateUrl from './template.tpl.pug';
import { NotebooksListCtrl } from './controller';
import './style.scss';

export default angular.module('ignite-console.sql.notebooks-list', [])
    .component('queriesNotebooksList', {
        controller: NotebooksListCtrl,
        templateUrl,
        transclude: true
    });

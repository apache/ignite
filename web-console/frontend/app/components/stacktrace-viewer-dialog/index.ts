

import angular from 'angular';
import './style.scss';

import StacktraceViewerDialog from './service';

import stacktraceTree from './components/stacktrace-tree';
import stacktraceItem from './components/stacktrace-item';

export default angular
    .module('ignite-console.stacktrace-viewer-dialog', [])
    .service('StacktraceViewerDialog', StacktraceViewerDialog)
    .component('stacktraceTree', stacktraceTree)
    .component('stacktraceItem', stacktraceItem);



import angular from 'angular';

import './style.scss';
import template from './template.pug';
import controller from './controller';

import connectedClustersDialog from '../connected-clusters-dialog';

export default angular
    .module('ignite-console.connected-clusters-badge', [
        connectedClustersDialog.name
    ])
    .component('connectedClusters', {
        template,
        controller
    });

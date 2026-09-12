

import angular from 'angular';

import './nodes-dialog.scss';

import Nodes from './Nodes.service';
import nodesDialogController from './nodes-dialog.controller';

angular.module('ignite-console.nodes', [])
    .service('IgniteNodes', Nodes)
    .controller('nodesDialogController', nodesDialogController);

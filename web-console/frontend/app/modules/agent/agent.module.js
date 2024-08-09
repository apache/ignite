

import angular from 'angular';

import AgentModal from './AgentModal.service';
import AgentManager from './AgentManager.service';

import clusterLogin from './components/cluster-login';

angular
    .module('ignite-console.agent', [
        clusterLogin.name
    ])
    .service('AgentModal', AgentModal)
    .service('AgentManager', AgentManager);

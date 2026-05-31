

import template from './template.pug';

class controller {
    /** @type {string} */
    clusterId;

    static $inject = ['AgentManager'];

    /**
     * @param {import('app/modules/agent/AgentManager.service').default} agentMgr
     */
    constructor(agentMgr) {
        this.agentMgr = agentMgr;
    }

    logout() {
        this.agentMgr.clustersSecrets.reset(this.clusterId);
    }
}

export default {
    controller,
    template,
    bindings: {
        clusterId: '<'
    }
};

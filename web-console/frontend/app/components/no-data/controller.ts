

import _ from 'lodash';
import {of} from 'rxjs';
import {distinctUntilChanged, switchMap} from 'rxjs/operators';

import AgentManager from 'app/modules/agent/AgentManager.service';
import AgentModal from 'app/modules/agent/AgentModal.service';

export default class NoDataCmpCtrl {
    static $inject = ['$translate', 'AgentManager', 'AgentModal'];

    connectionState$ = this.AgentManager.connectionSbj.pipe(
        switchMap((sbj) => {
            if (!this.AgentManager.isDemoMode() && !_.isNil(sbj.cluster) && sbj.cluster.active === false)
                return of('CLUSTER_INACTIVE');

            return of(sbj.state);
        }),
        distinctUntilChanged()
    );

    backState = '.';
    backText = this.$translate.instant('closeButtonLabel');

    constructor(
        private $translate: ng.translate.ITranslateService,
        private AgentManager: AgentManager,
        private agentModal: AgentModal
    ) {}

    openAgentMissingDialog() {
        this.agentModal.agentDisconnected(this.backText, this.backState);
    }

    openNodeMissingDialog() {
        this.agentModal.clusterDisconnected(this.backText, this.backState);
    }
}

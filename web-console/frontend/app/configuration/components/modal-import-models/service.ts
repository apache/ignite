

import {UIRouter, StateDeclaration, StateService} from '@uirouter/angularjs';
import AgentManager from 'app/modules/agent/AgentManager.service';

export default class ModalImportModels {
    static $inject = ['$modal', '$q', '$uiRouter', 'AgentManager'];

    deferred: ng.IDeferred<true>;
    private _state: StateDeclaration;
    private _modal: mgcrea.ngStrap.modal.IModal;

    constructor(
        private $modal: mgcrea.ngStrap.modal.IModalService,
        private $q: ng.IQService,
        private $uiRouter: UIRouter,
        private AgentManager: AgentManager
    ) {}

    _goToDynamicState() {
        if (this.deferred)
            return this.deferred.promise;

        this.deferred = this.$q.defer();

        if (this._state)
            this.$uiRouter.stateRegistry.deregister(this._state);

        this._state = this.$uiRouter.stateRegistry.register({
            name: 'importModels',
            parent: this.$uiRouter.stateService.current,
            onEnter: () => {
                this._open();
            },
            onExit: () => {
                this._modal && this._modal.hide();
            }
        });

        return this.$uiRouter.stateService.go(this._state, this.$uiRouter.stateService.params)
            .catch(() => {
                this.deferred.reject(false);
                this.deferred = null;
            });
    }

    _open() {
        const self = this;

        this._modal = this.$modal({
            template: `
                <modal-import-models
                    on-hide='$ctrl.onHide()'
                    cluster-id='$ctrl.$state.params.clusterID'
                ></modal-import-models>
            `,
            controller: ['$state', function($state: StateService) {
                this.$state = $state;

                this.onHide = () => {
                    self.deferred.resolve(true);

                    this.$state.go('^');
                };
            }],
            controllerAs: '$ctrl',
            backdrop: 'static',
            show: false
        });

        return this._modal.$promise
            .then(() => this._modal.show())
            .then(() => this.deferred.promise)
            .finally(() => this.deferred = null);
    }

    open() {
        this._goToDynamicState();
    }
}

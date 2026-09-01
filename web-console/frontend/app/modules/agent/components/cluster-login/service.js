

import _ from 'lodash';

import {CancellationError} from 'app/errors/CancellationError';

export default class ClusterLoginService {
    static $inject = ['$modal', '$q', 'IgniteMessages'];

    deferred;

    /**
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     * @param {ng.IQService} $q
     */
    constructor($modal, $q, Messages) {
        this.$modal = $modal;
        this.$q = $q;
        this.Messages = Messages;
    }

    /**
     * @param {import('../../types/ClusterSecrets').ClusterSecrets} baseSecrets
     * @param {Error} reason Error that bring to opening of dialog.
     * @returns {ng.IPromise<import('../../types/ClusterSecrets').ClusterSecrets>}
     */
    askCredentials(baseSecrets, reason) {
        if (this.deferred)
            return this.deferred.promise;

        this.deferred = this.$q.defer();

        const self = this;

        const modal = this.$modal({
            template: `
                <cluster-login
                    secrets='$ctrl.secrets'
                    on-login='$ctrl.onLogin()'
                    on-hide='$ctrl.onHide()'
                ></cluster-login>
            `,
            controller: [function() {
                this.secrets = _.clone(baseSecrets);

                this.onLogin = () => {
                    self.deferred.resolve(this.secrets);
                };

                this.onHide = () => {
                    self.deferred.reject(new CancellationError());
                };
            }],
            controllerAs: '$ctrl',
            backdrop: 'static',
            show: true
        });

        if (reason)
            this.Messages.showError('Authentication failed: ', reason);

        return modal.$promise
            .then(() => this.deferred.promise)
            .finally(() => {
                this.deferred = null;

                modal.hide();
            });
    }

    cancel() {
        if (this.deferred)
            this.deferred.reject(new CancellationError());
    }
}

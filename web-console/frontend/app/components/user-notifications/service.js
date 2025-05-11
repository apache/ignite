

import _ from 'lodash';

import controller from './controller';
import templateUrl from './template.tpl.pug';

export default class UserNotificationsService {
    static $inject = ['$http', '$modal', '$q', 'IgniteMessages'];

    /** @type {ng.IQService} */
    $q;

    /**
     * @param {ng.IHttpService} $http
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     * @param {ng.IQService} $q
     * @param {ReturnType<typeof import('app/services/Messages.service').default>} Messages
     */
    constructor($http, $modal, $q, Messages) {
        this.$http = $http;
        this.$modal = $modal;
        this.$q = $q;
        this.Messages = Messages;

        this.message = null;
        this.visible = false;
    }

    set announcement(ann) {
        this.message = _.get(ann, 'message');
        this.visible = _.get(ann, 'visible');
    }

    editor() {
        const deferred = this.$q.defer();

        const modal = this.$modal({
            templateUrl,
            resolve: {
                deferred: () => deferred,
                message: () => this.message,
                visible: () => this.visible
            },
            controller,
            controllerAs: '$ctrl'
        });

        return deferred.promise
            .then((ann) => {
                this.$http.put('/api/v1/admin/announcement', ann)
                    .catch(this.Messages.showError);
            })
            .finally(modal.hide);
    }
}

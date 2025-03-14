

import {StateService} from '@uirouter/angularjs';
import {default as MessagesFactory} from 'app/services/Messages.service';

export default class {
    static $inject = ['$modal', '$http', '$state', 'IgniteMessages', '$element', '$translate'];

    ui?: {email: string, token: string, password?: string};

    constructor(
        $modal: mgcrea.ngStrap.modal.IModalService,
        private $http,
        private $state: StateService,
        private Messages: ReturnType<typeof MessagesFactory>,
        private el: JQLite,
        private $translate: ng.translate.ITranslateService
    ) {}

    $postLink() {
        this.el.addClass('public-page');
    }

    $onInit() {
        this.ui = {
            email: this.$state.params.email,
            token: this.$state.params.token
        };
    }

    // Try to reset user password for provided token.
    resetPassword() {
        const resetParams = {
            email: this.ui.email,
            token: this.ui.token,
            password: this.ui.password
        };

        this.$http.post('/api/v1/password/reset', resetParams)
            .then(() => {
                this.$state.go('signin');

                this.Messages.showInfo(this.$translate.instant('passwordReset.successNotification'));
            })
            .catch(({data, state}) => {
                if (state === 503)
                    this.$state.go('signin');

                this.Messages.showError(data);
            });
    }
}

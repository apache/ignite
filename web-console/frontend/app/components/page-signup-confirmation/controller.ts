

import {default as Auth} from '../../modules/user/Auth.service';
import {default as MessagesFactory} from '../../services/Messages.service';

export default class PageSignupConfirmation {
    email: string;

    static $inject = ['Auth', 'IgniteMessages', '$element', '$translate'];

    constructor(
        private auth: Auth,
        private messages: ReturnType<typeof MessagesFactory>,
        private el: JQLite,
        private $translate: ng.translate.ITranslateService
    ) {}

    $postLink() {
        this.el.addClass('public-page');
    }

    async resendConfirmation() {
        try {
            await this.auth.resendSignupConfirmation(this.email);
            this.messages.showInfo(this.$translate.instant('signupConfirmation.successNotification'));
        }
        catch (e) {
            this.messages.showError(e);
        }
    }
}

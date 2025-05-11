

import IgniteAdminData from '../../core/admin/Admin.data';
import MessagesFactory from '../../services/Messages.service';
import FormUtilsFactoryFactory from '../../services/FormUtils.service';
import LoadingServiceFactory from '../../modules/loading/loading.service';
import {ISignupData} from '../form-signup';
import {UserService} from 'app/modules/user/User.service';

export class DialogAdminCreateUser {
    close: ng.ICompiledExpression;

    form: ng.IFormController;

    data: ISignupData = {
        email: null,
        password: null,
        firstName: null,
        lastName: null,
        company: null,
        country: null
    };

    serverError: string | null = null;
    loadingText = this.$translate.instant('admin.createUserDialog.loadingText', null, null, null, null)

    static $inject = ['User', 'IgniteAdminData', 'IgniteMessages', 'IgniteFormUtils', 'IgniteLoading', '$translate'];

    constructor(
        private User: UserService,
        private AdminData: IgniteAdminData,
        private IgniteMessages: ReturnType<typeof MessagesFactory>,
        private IgniteFormUtils: ReturnType<typeof FormUtilsFactoryFactory>,
        private loading: ReturnType<typeof LoadingServiceFactory>,
        private $translate: ng.translate.ITranslateService
    ) {}

    canSubmitForm(form: DialogAdminCreateUser['form']) {
        return form.$error.server ? true : !form.$invalid;
    }

    setServerError(error: DialogAdminCreateUser['serverError']) {
        this.serverError = error;
    }

    createUser() {
        this.IgniteFormUtils.triggerValidation(this.form);

        this.setServerError(null);

        if (!this.canSubmitForm(this.form))
            return;

        this.loading.start('createUser');

        this.AdminData.registerUser(this.data)
            .then(() => {
                this.User.created$.next(this.data);
                this.IgniteMessages.showInfo(this.$translate.instant('admin.createUserDialog.userCreatedSuccessNotification', {user: this.data}));
                this.close({});
            })
            .catch((err) => {
                this.loading.finish('createUser');
                this.IgniteMessages.showError(null, err);
                this.setServerError(err.message);
            });
    }
}

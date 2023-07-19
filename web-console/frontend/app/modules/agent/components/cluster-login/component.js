

import template from './template.pug';
import {ClusterSecrets} from '../../types/ClusterSecrets';

export const component = {
    bindings: {
        secrets: '=',
        onLogin: '&',
        onHide: '&'
    },
    controller: class {
        /** @type {ClusterSecrets} */
        secrets;
        /** @type {ng.ICompiledExpression} */
        onLogin;
        /** @type {ng.ICompiledExpression} */
        onHide;
        /** @type {ng.IFormController} */
        form;

        login() {
            if (this.form.$invalid)
                return;

            this.onLogin();
        }
    },
    template
};

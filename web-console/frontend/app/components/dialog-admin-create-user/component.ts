

import template from './template.pug';
import {DialogAdminCreateUser} from './controller';

export default {
    template,
    controller: DialogAdminCreateUser,
    bindings: {
        close: '&onHide'
    }
};

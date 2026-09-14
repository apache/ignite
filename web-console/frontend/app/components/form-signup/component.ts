

import template from './template.pug';
import './style.scss';
import {FormSignup} from './controller';

export const component: ng.IComponentOptions = {
    template,
    controller: FormSignup,
    bindings: {
        outerForm: '<',
        serverError: '<'
    },
    require: {
        ngModel: 'ngModel'
    }
};

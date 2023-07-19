

import template from './template.pug';
import './style.scss';

export const component = {
    template,
    bindings: {
        selectedAmount: '<',
        totalAmount: '<'
    }
};

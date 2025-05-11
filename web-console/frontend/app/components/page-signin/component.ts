

import template from './template.pug';
import controller from './controller';
import './style.scss';

/** @type {ng.IComponentOptions} */
export default {
    controller,
    template,
    bindings: {
        activationToken: '@?'
    }
};



import controller from './controller';
import template from './template.pug';
import './style.scss';

export default {
    template,
    controller,
    bindings: {
        isLoading: '<'
    }
} as ng.IComponentOptions;

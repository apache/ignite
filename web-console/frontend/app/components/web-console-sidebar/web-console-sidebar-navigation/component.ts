

import './style.scss';
import template from './template.pug';
import controller from './controller';

export const component = {
    controller,
    template,
    bindings: {
        opened: '<'
    }
};

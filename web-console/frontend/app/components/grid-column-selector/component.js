

import template from './template.pug';
import controller from './controller';
import './style.scss';

export default {
    template,
    controller,
    transclude: true,
    bindings: {
        gridApi: '<'
    }
};

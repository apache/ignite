

import template from './template.pug';
import './style.scss';
import controller from './controller';

export default {
    name: 'modalPreviewProject',
    template,
    controller,
    bindings: {
        onHide: '&',
        cluster: '<',
        isDemo: '<'
    }
};

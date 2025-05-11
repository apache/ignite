

import './style.scss';
import controller from './controller';
import template from './template.pug';

export default {
    controller,
    template,
    bindings: {
        value: '<?'
    }
};

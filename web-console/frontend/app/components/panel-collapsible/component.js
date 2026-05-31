

import template from './template.pug';
import controller from './controller';
import './style.scss';

export default {
    template,
    controller,
    bindings: {
        opened: '<?',
        onOpen: '&?',
        onClose: '&?',
        title: '@?',
        description: '@?',
        disabled: '@?'
    },
    transclude: {
        title: '?panelTitle',
        description: '?panelDescription',
        actions: '?panelActions',
        content: 'panelContent'
    }
};

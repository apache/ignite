

import './style.scss';
import template from './template.pug';
import controller from './controller';

export default {
    controller,
    template,
    transclude: true,
    require: {
        ngModel: 'ngModel'
    },
    bindings: {
        label: '@',
        placeholder: '@',
        min: '@?',
        max: '@?',
        tip: '@',
        required: '<?',
        sizeType: '@?',
        sizeScaleLabel: '@?',
        onScaleChange: '&?',
        ngDisabled: '<?',
        autofocus: '<?',
        inputDebounce: '<?'
    }
};

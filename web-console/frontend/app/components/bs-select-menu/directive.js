

import template from './template.pug';
import controller from './controller';
import './style.scss';

export default function bsSelectMenu() {
    return {
        template,
        controller,
        controllerAs: '$ctrl',
        restrict: 'E',
        replace: true // Workaround: without [replace: true] bs-select detects incorrect menu size.
    };
}

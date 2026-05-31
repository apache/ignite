

import './information.scss';
import template from './information.pug';

export default function() {
    return {
        scope: {
            title: '@'
        },
        restrict: 'E',
        template,
        replace: true,
        transclude: true
    };
}

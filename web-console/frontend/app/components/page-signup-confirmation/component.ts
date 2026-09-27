

import controller from './controller';
import templateUrl from './template.tpl.pug';
import './style.scss';

export const component: ng.IComponentOptions = {
    controller,
    templateUrl,
    bindings: {
        email: '@'
    }
};

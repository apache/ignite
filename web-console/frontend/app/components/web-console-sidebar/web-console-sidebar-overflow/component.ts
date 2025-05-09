

import controller from './controller';
import template from './template.pug';
import './style.scss';

export const component: ng.IComponentOptions =  {
    controller,
    transclude: true,
    template
};



import template from './template.pug';
import './style.scss';
import {TimedRedirectionCtrl} from './controller';

export const component: ng.IComponentOptions = {
    template,
    controller: TimedRedirectionCtrl,
    bindings: {
        headerText: '<',
        subHeaderText: '<'
    }
};

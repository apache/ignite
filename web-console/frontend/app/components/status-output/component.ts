

import {Status} from './controller';
import templateUrl from './template.tpl.pug';
import './style.scss';

const component: ng.IComponentOptions = {
    templateUrl,
    bindings: {
        options: '<',
        value: '<'
    },
    controller: Status
};

export {component};

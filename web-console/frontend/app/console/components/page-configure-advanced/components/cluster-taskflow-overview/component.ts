

import templateUrl from './template.tpl.pug';
import './style.scss';
import controller from './controller';

export default {
    templateUrl,
    controller,
    bindings: {
        clusterId: '<',
        cluster: '<',
        models: '<',
        targetCaches: '=' 
    }
};

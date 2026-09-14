

import templateUrl from './template.tpl.pug';
import controller from './controller';

import './style.scss';

export default {
    controller,
    templateUrl,
    transclude: true,
    bindings: {
        resultDataStatus: '<',
        handleClusterInactive: '<'
    }
} as ng.IComponentOptions;

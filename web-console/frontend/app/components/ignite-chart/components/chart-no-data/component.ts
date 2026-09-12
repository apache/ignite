

import IgniteChartNoDataCtrl from './controller';
import templateUrl from './template.tpl.pug';

export default {
    controller: IgniteChartNoDataCtrl,
    templateUrl,
    require: {
        igniteChart: '^igniteChart'
    },
    bindings: {
        resultDataStatus: '<',
        handleClusterInactive: '<'
    }
} as ng.IComponentOptions;



import { IgniteChartController } from './controller';
import templateUrl from './template.tpl.pug';

export default {
    controller: IgniteChartController,
    templateUrl,
    bindings: {
        chartOptions: '<',
        chartDataPoint: '<',
        chartHistory: '<',
        chartTitle: '<',
        chartColors: '<',
        chartHeaderText: '<',
        refreshRate: '<',
        resultDataStatus: '<?'
    },
    transclude: true
};

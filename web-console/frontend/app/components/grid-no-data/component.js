

import './style.scss';
import controller from './controller';

export default {
    template: `
        <div ng-if='$ctrl.noData' ng-transclude></div>
        <div ng-if='$ctrl.noDataFiltered' ng-transclude='noDataFiltered'></div>
    `,
    controller,
    bindings: {
        gridApi: '<'
    },
    transclude: {
        noDataFiltered: '?gridNoDataFiltered'
    }
};

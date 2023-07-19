

import template from './template.pug';
import './style.scss';
import controller from './controller';

export default {
    template,
    controller,
    transclude: {
        footerSlot: '?footerSlot'
    },
    bindings: {
        items: '<',
        onVisibleRowsChange: '&?',
        onSortChanged: '&?',
        onFilterChanged: '&?',

        hideHeader: '<?',
        rowIdentityKey: '@?',

        columnDefs: '<',
        tableTitle: '<',
        selectedRowId: '<?',
        maxRowsToShow: '@?',
        onSelectionChange: '&?',
        oneWaySelection: '<?',
        incomingActionsMenu: '<?actionsMenu'
    }
};

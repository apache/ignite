

import './style.scss';
import template from './template.pug';
import controller from './controller';

export default {
    template,
    controller,
    bindings: {
        gridApi: '=?',
        gridTreeView: '<?',
        gridGrouping: '<?',
        gridThin: '<?',
        gridHeight: '<?',
        tabName: '<?',
        tableTitle: '<?',
        maxRowsToShow: '<?',

        // Input Events.
        items: '<',
        columnDefs: '<',
        categories: '<?',
        singleSelect: '<?',
        oneWaySelection: '<?',
        rowIdentityKey: '@?',
        selectedRows: '<?',
        selectedRowsId: '<?',

        // Output events.
        onSelectionChange: '&?',
        onApiRegistered: '&?'
    }
};

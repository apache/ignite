

import {componentFactory, StatusLevel} from 'app/components/status-output';

export default componentFactory([
    {
        level: StatusLevel.GREEN,
        value: true,
        label: 'connectedClustersDialog.status.active'
    },
    {
        level: StatusLevel.RED,
        value: false,
        label: 'connectedClustersDialog.status.inactive'
    }
]);



export const columnDefsFn = ($translate: ng.translate.ITranslateService) => [
    {
        name: 'name',
        displayName: $translate.instant('connectedClustersDialog.gridColumnTitles.name'),
        field: 'name',
        cellTemplate: `
            <div class='ui-grid-cell-contents'>
                <cluster-security-icon
                    secured='row.entity.secured'
                ></cluster-security-icon>
                {{ COL_FIELD }}
            </div>
        `,
        width: 240,
        minWidth: 240
    },
    {
        name: 'nids',
        displayName: $translate.instant('connectedClustersDialog.gridColumnTitles.nids'),
        field: 'size',
        cellClass: 'ui-grid-number-cell',
        width: 180,
        minWidth: 180
    },
    {
        name: 'status',
        displayName: $translate.instant('connectedClustersDialog.gridColumnTitles.status'),
        field: 'active',
        cellTemplate: `
            <div class='ui-grid-cell-contents ui-grid-cell--status'>
                <connected-clusters-cell-status
                    value='COL_FIELD'
                ></connected-clusters-cell-status>
                <connected-clusters-cell-logout
                    ng-if='row.entity.secured && grid.appScope.$ctrl.agentMgr.hasCredentials(row.entity.id)'
                    cluster-id='row.entity.id'
                ></connected-clusters-cell-logout>
            </div>
        `,
        minWidth: 140
    }
];

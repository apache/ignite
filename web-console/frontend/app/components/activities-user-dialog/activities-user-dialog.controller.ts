

export default class ActivitiesCtrl {
    static $inject = ['user', '$translate'];

    columnDefs = [
        { displayName: this.$translate.instant('admin.userActivitiesDialog.grid.columns.description.title'), field: 'action', enableFiltering: false, cellFilter: 'translate', minWidth: 120, width: '43%'},
        { displayName: this.$translate.instant('admin.userActivitiesDialog.grid.columns.action.title'), field: 'action', enableFiltering: false, minWidth: 120, width: '43%'},
        { displayName: this.$translate.instant('admin.userActivitiesDialog.grid.columns.visited.title'), field: 'amount', enableFiltering: false, minWidth: 80}
    ];

    data;

    constructor(private user, private $translate: ng.translate.ITranslateService) {
        this.data = user.activitiesDetail;
    }
}


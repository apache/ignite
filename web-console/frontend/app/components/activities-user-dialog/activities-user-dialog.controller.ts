// Import angular-translate module
import 'angular-translate';

// Declare types for $translate if necessary
declare module 'angular' {
    namespace translate {
        interface ITranslateService {
            instant(key: string, interpolateParams?: object): string;
        }
    }
}

export default class ActivitiesCtrl {
    static $inject = ['user', '$translate'];

    columnDefs = [
        { 
            displayName: this.$translate.instant('admin.userActivitiesDialog.grid.columns.description.title'), 
            field: 'action', 
            enableFiltering: false, 
            cellFilter: 'translate', 
            minWidth: 120, 
            width: '43%'
        },
        { 
            displayName: this.$translate.instant('admin.userActivitiesDialog.grid.columns.action.title'), 
            field: 'action', 
            enableFiltering: false, 
            minWidth: 120, 
            width: '43%' 
        },
        { 
            displayName: this.$translate.instant('admin.userActivitiesDialog.grid.columns.visited.title'), 
            field: 'amount', 
            enableFiltering: false, 
            minWidth: 80 
        }
    ];

    data;

    constructor(private user: any, private $translate: angular.translate.ITranslateService) {
        this.data = user.activitiesDetail;
    }
}



import angular from 'angular';

import IgniteAdminData from './admin/Admin.data';
import IgniteActivitiesData from './activities/Activities.data';

angular.module('ignite-console.core', [])
    .service('IgniteAdminData', IgniteAdminData)
    .service('IgniteActivitiesData', IgniteActivitiesData);

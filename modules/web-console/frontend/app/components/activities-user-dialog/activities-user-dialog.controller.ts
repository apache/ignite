/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

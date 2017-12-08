/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export default class PageConfigureAdvancedController {
    static $inject = ['$scope'];

    static menuItems = [
        { text: 'Clusters', sref: 'base.configuration.tabs.advanced.clusters' },
        { text: 'Model', sref: 'base.configuration.tabs.advanced.domains' },
        { text: 'Caches', sref: 'base.configuration.tabs.advanced.caches' },
        { text: 'IGFS', sref: 'base.configuration.tabs.advanced.igfs' },
        { text: 'Summary', sref: 'base.configuration.tabs.advanced.summary' }
    ];

    constructor($scope) {
        Object.assign(this, {$scope});
    }

    $onInit() {
        this.menuItems = this.constructor.menuItems;
    }
}

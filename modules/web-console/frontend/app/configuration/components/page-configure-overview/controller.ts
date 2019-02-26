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

import {Subject, Observable} from 'rxjs';
import {map} from 'rxjs/operators';
import naturalCompare from 'natural-compare-lite';

const cellTemplate = (state) => `
    <div class="ui-grid-cell-contents">
        <a
            class="link-success"
            ui-sref="${state}({clusterID: row.entity._id})"
            title='Click to edit'
        >{{ row.entity[col.field] }}</a>
    </div>
`;

import {default as ConfigureState} from '../../services/ConfigureState';
import {default as ConfigSelectors} from '../../store/selectors';
import {default as Clusters} from '../../services/Clusters';
import {default as ModalPreviewProject} from '../../components/modal-preview-project/service';
import {default as ConfigurationDownload} from '../../services/ConfigurationDownload';

import {confirmClustersRemoval} from '../../store/actionCreators';

import {UIRouter} from '@uirouter/angularjs';
import {ShortCluster} from '../../types';
import {IColumnDefOf} from 'ui-grid';

export default class PageConfigureOverviewController {
    static $inject = [
        '$uiRouter',
        'ModalPreviewProject',
        'Clusters',
        'ConfigureState',
        'ConfigSelectors',
        'ConfigurationDownload'
    ];

    constructor(
        private $uiRouter: UIRouter,
        private ModalPreviewProject: ModalPreviewProject,
        private Clusters: Clusters,
        private ConfigureState: ConfigureState,
        private ConfigSelectors: ConfigSelectors,
        private ConfigurationDownload: ConfigurationDownload
    ) {}

    shortClusters$: Observable<Array<ShortCluster>>;
    clustersColumnDefs: Array<IColumnDefOf<ShortCluster>>;
    selectedRows$: Subject<Array<ShortCluster>>;
    selectedRowsIDs$: Observable<Array<string>>;

    $onDestroy() {
        this.selectedRows$.complete();
    }

    removeClusters(clusters: Array<ShortCluster>) {
        this.ConfigureState.dispatchAction(confirmClustersRemoval(clusters.map((c) => c._id)));

        // TODO: Implement storing selected rows in store to share this data between other components.
        this.selectedRows$.next([]);
    }

    editCluster(cluster: ShortCluster) {
        return this.$uiRouter.stateService.go('^.edit', {clusterID: cluster._id});
    }

    $onInit() {
        this.shortClusters$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectShortClustersValue());

        this.clustersColumnDefs = [
            {
                name: 'name',
                displayName: 'Name',
                field: 'name',
                enableHiding: false,
                filter: {
                    placeholder: 'Filter by name…'
                },
                sort: {direction: 'asc', priority: 0},
                sortingAlgorithm: naturalCompare,
                cellTemplate: cellTemplate('base.configuration.edit'),
                minWidth: 165
            },
            {
                name: 'discovery',
                displayName: 'Discovery',
                field: 'discovery',
                multiselectFilterOptions: this.Clusters.discoveries,
                width: 150
            },
            {
                name: 'caches',
                displayName: 'Caches',
                field: 'cachesCount',
                cellClass: 'ui-grid-number-cell',
                cellTemplate: cellTemplate('base.configuration.edit.advanced.caches'),
                enableFiltering: false,
                type: 'number',
                width: 95
            },
            {
                name: 'models',
                displayName: 'Models',
                field: 'modelsCount',
                cellClass: 'ui-grid-number-cell',
                cellTemplate: cellTemplate('base.configuration.edit.advanced.models'),
                enableFiltering: false,
                type: 'number',
                width: 95
            },
            {
                name: 'igfs',
                displayName: 'IGFS',
                field: 'igfsCount',
                cellClass: 'ui-grid-number-cell',
                cellTemplate: cellTemplate('base.configuration.edit.advanced.igfs'),
                enableFiltering: false,
                type: 'number',
                width: 80
            }
        ];

        this.selectedRows$ = new Subject();

        this.selectedRowsIDs$ = this.selectedRows$.pipe(map((selectedClusters) => selectedClusters.map((cluster) => cluster._id)));

        this.actions$ = this.selectedRows$.pipe(map((selectedClusters) => [
            {
                action: 'Edit',
                click: () => this.editCluster(selectedClusters[0]),
                available: selectedClusters.length === 1
            },
            {
                action: 'See project structure',
                click: () => this.ModalPreviewProject.open(selectedClusters[0]),
                available: selectedClusters.length === 1
            },
            {
                action: 'Download project',
                click: () => this.ConfigurationDownload.downloadClusterConfiguration(selectedClusters[0]),
                available: selectedClusters.length === 1
            },
            {
                action: 'Delete',
                click: () => this.removeClusters(selectedClusters),
                available: true
            }
        ]));
    }
}

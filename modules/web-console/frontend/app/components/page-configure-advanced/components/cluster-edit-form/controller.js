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

import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import isEqual from 'lodash/isEqual';
import _ from 'lodash';

export default class ClusterEditFormController {
    /** @type {Array<ig.config.cache.ShortCache>} */
    caches;
    /** @type {ig.menu<string>} */
    cachesMenu;
    /** @type {ng.ICompiledExpression} */
    onSave;

    static $inject = ['IgniteLegacyUtils', 'IgniteEventGroups', 'IgniteConfirm', 'IgniteVersion', '$scope', 'Clusters', 'IgniteFormUtils'];
    /**
     * @param {import('app/services/Clusters').default} Clusters
     */
    constructor(IgniteLegacyUtils, IgniteEventGroups, IgniteConfirm, IgniteVersion, $scope, Clusters, IgniteFormUtils) {
        Object.assign(this, {IgniteLegacyUtils, IgniteEventGroups, IgniteConfirm, IgniteVersion, $scope, IgniteFormUtils});
        this.Clusters = Clusters;
    }

    $onDestroy() {
        this.subscription.unsubscribe();
    }

    $onInit() {
        this.available = this.IgniteVersion.available.bind(this.IgniteVersion);

        let __original_value;

        const rebuildDropdowns = () => {
            this.eventStorage = [
                {value: 'Memory', label: 'Memory'},
                {value: 'Custom', label: 'Custom'}
            ];

            this.marshallerVariant = [
                {value: 'JdkMarshaller', label: 'JdkMarshaller'},
                {value: null, label: 'Default'}
            ];

            if (this.available('2.0.0')) {
                this.eventStorage.push({value: null, label: 'Disabled'});

                this.eventGroups = _.filter(this.IgniteEventGroups, ({value}) => value !== 'EVTS_SWAPSPACE');
            }
            else {
                this.eventGroups = this.IgniteEventGroups;

                this.marshallerVariant.splice(0, 0, {value: 'OptimizedMarshaller', label: 'OptimizedMarshaller'});
            }
        };

        rebuildDropdowns();

        const filterModel = (cluster) => {
            if (cluster) {
                if (this.available('2.0.0')) {
                    const evtGrps = _.map(this.eventGroups, 'value');

                    _.remove(cluster.includeEventTypes, (evtGrp) => !_.includes(evtGrps, evtGrp));

                    if (_.get(cluster, 'marshaller.kind') === 'OptimizedMarshaller')
                        cluster.marshaller.kind = null;
                }
                else if (cluster && !_.get(cluster, 'eventStorage.kind'))
                    _.set(cluster, 'eventStorage.kind', 'Memory');
            }
        };

        this.subscription = this.IgniteVersion.currentSbj
            .do(rebuildDropdowns)
            .do(() => filterModel(this.clonedCluster))
            .subscribe();

        this.supportedJdbcTypes = this.IgniteLegacyUtils.mkOptions(this.IgniteLegacyUtils.SUPPORTED_JDBC_TYPES);

        this.$scope.ui = this.IgniteFormUtils.formUI();
        this.$scope.ui.loadedPanels = ['checkpoint', 'serviceConfiguration', 'odbcConfiguration'];

        this.formActions = [
            {text: 'Save', icon: 'checkmark', click: () => this.save()},
            {text: 'Save and Download', icon: 'download', click: () => this.save(true)}
        ];
    }

    $onChanges(changes) {
        if ('cluster' in changes && this.shouldOverwriteValue(this.cluster, this.clonedCluster)) {
            this.clonedCluster = cloneDeep(changes.cluster.currentValue);
            if (this.$scope.ui && this.$scope.ui.inputForm) {
                this.$scope.ui.inputForm.$setPristine();
                this.$scope.ui.inputForm.$setUntouched();
            }
        }
        if ('caches' in changes)
            this.cachesMenu = (changes.caches.currentValue || []).map((c) => ({label: c.name, value: c._id}));
    }

    /**
     * The form should accept incoming cluster value if:
     * 1. It has different _id ("new" to real id).
     * 2. Different caches or models (imported from DB).
     * @param {Object} a Incoming value.
     * @param {Object} b Current value.
     */
    shouldOverwriteValue(a, b) {
        return get(a, '_id') !== get(b, '_id') ||
            !isEqual(get(a, 'caches'), get(b, 'caches')) ||
            !isEqual(get(a, 'models'), get(b, 'models'));
    }

    getValuesToCompare() {
        return [this.cluster, this.clonedCluster].map(this.Clusters.normalize);
    }

    save(download) {
        if (this.$scope.ui.inputForm.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.$scope.ui.inputForm, this.$scope);
        this.onSave({$event: {cluster: cloneDeep(this.clonedCluster), download}});
    }

    reset = () => this.clonedCluster = cloneDeep(this.cluster);
    confirmAndReset() {
        return this.IgniteConfirm.confirm('Are you sure you want to undo all changes for current cluster?')
        .then(this.reset);
    }
}

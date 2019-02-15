/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import isEqual from 'lodash/isEqual';
import _ from 'lodash';
import {tap} from 'rxjs/operators';
import {ShortCache} from '../../../../types';
import {Menu} from 'app/types';
import Clusters from '../../../../services/Clusters';
import LegacyUtils from 'app/services/LegacyUtils.service';
import IgniteEventGroups from '../../../../generator/generator/defaults/Event-groups.service';
import LegacyConfirm from 'app/services/Confirm.service';
import Version from 'app/services/Version.service';
import FormUtils from 'app/services/FormUtils.service';

export default class ClusterEditFormController {
    caches: ShortCache[];
    cachesMenu: Menu<string>;
    servicesCachesMenu: Menu<string>;
    onSave: ng.ICompiledExpression;

    static $inject = ['IgniteLegacyUtils', 'IgniteEventGroups', 'IgniteConfirm', 'IgniteVersion', '$scope', 'Clusters', 'IgniteFormUtils'];

    constructor(
        private IgniteLegacyUtils: ReturnType<typeof LegacyUtils>,
        private IgniteEventGroups: IgniteEventGroups,
        private IgniteConfirm: ReturnType<typeof LegacyConfirm>,
        private IgniteVersion: Version,
        private $scope: ng.IScope,
        private Clusters: Clusters,
        private IgniteFormUtils: ReturnType<typeof FormUtils>
    ) {}

    $onDestroy() {
        this.subscription.unsubscribe();
    }

    $onInit() {
        this.available = this.IgniteVersion.available.bind(this.IgniteVersion);

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

        this.subscription = this.IgniteVersion.currentSbj.pipe(
            tap(rebuildDropdowns),
            tap(() => filterModel(this.clonedCluster))
        )
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

        if ('caches' in changes) {
            this.cachesMenu = (changes.caches.currentValue || []).map((c) => ({label: c.name, value: c._id}));
            this.servicesCachesMenu = [{label: 'Key-affinity not used', value: null}].concat(this.cachesMenu);
        }
    }

    /**
     * The form should accept incoming cluster value if:
     * 1. It has different _id ("new" to real id).
     * 2. Different caches or models (imported from DB).
     * @param a Incoming value.
     * @param b Current value.
     */
    shouldOverwriteValue<T>(a: T, b: T) {
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

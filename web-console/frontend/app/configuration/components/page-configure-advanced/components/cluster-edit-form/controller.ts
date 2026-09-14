

import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import isEqual from 'lodash/isEqual';
import _ from 'lodash';
import {tap} from 'rxjs/operators';
import {ShortCache} from '../../../../types';
import {Menu} from 'app/types';
import Clusters from '../../../../services/Clusters';
import SqlTypes from 'app/services/SqlTypes.service';
import IgniteEventGroups from '../../../../generator/generator/defaults/Event-groups.service';
import LegacyConfirm from 'app/services/Confirm.service';
import Version from 'app/services/Version.service';
import FormUtils from 'app/services/FormUtils.service';

export default class ClusterEditFormController {
    cluster: any;
    caches: ShortCache[];
    cachesMenu: Menu<string>;
    onSave: ng.ICompiledExpression;

    static $inject = ['SqlTypes', 'IgniteEventGroups', 'IgniteConfirm', 'IgniteVersion', '$scope', 'Clusters', 'IgniteFormUtils'];

    constructor(
        private SqlTypes: SqlTypes,
        private IgniteEventGroups: IgniteEventGroups,
        private IgniteConfirm: ReturnType<typeof LegacyConfirm>,
        private IgniteVersion: Version,
        private $scope: ng.IScope,
        private Clusters: Clusters,
        private IgniteFormUtils: ReturnType<typeof FormUtils>
    ) {}

    $onInit() {
        this.available = this.IgniteVersion.available.bind(this.IgniteVersion);

        const rebuildDropdowns = () => {
            this.eventStorage = [
                {value: 'Memory', label: 'Memory'},
                {value: 'Custom', label: 'Custom'}
            ];            

            this.failureHandlerVariant = [
                {value: 'RestartProcess', label: 'Restart process'},
                {value: 'StopNodeOnHalt', label: 'Try stop with timeout'},
                {value: 'StopNode', label: 'Stop on critical error'},
                {value: 'Noop', label: 'Disabled'},
                {value: 'Custom', label: 'Custom'},
                {value: null, label: 'Default'}
            ];

            this.ignoredFailureTypes = [
                {value: 'SEGMENTATION', label: 'SEGMENTATION'},
                {value: 'SYSTEM_WORKER_TERMINATION', label: 'SYSTEM_WORKER_TERMINATION'},
                {value: 'SYSTEM_WORKER_BLOCKED', label: 'SYSTEM_WORKER_BLOCKED'},
                {value: 'CRITICAL_ERROR', label: 'CRITICAL_ERROR'},
                {value: 'SYSTEM_CRITICAL_OPERATION_TIMEOUT', label: 'SYSTEM_CRITICAL_OPERATION_TIMEOUT'}
            ];

            
            this.eventStorage.push({value: null, label: 'Disabled'});

            this.eventGroups = _.filter(this.IgniteEventGroups, ({value}) => value !== 'EVTS_SWAPSPACE');

            _.forEach(this.eventGroups, (grp) => grp.events = _.filter(grp.events, (evt) => evt.indexOf('SWAP') < 0));
                      

            this.eventTypes = [];

            _.forEach(this.eventGroups, (grp) => {
                _.forEach(grp.events, (e) => {
                    const newVal = {value: e, label: e};

                    if (!_.find(this.eventTypes, newVal))
                        this.eventTypes.push(newVal);
                });
            });
        };

        rebuildDropdowns();

        const filterModel = (cluster) => {
            if (cluster) {
                const evtGrps = _.map(this.eventGroups, 'value');
                _.remove(cluster.includeEventTypes, (evtGrp) => !_.includes(evtGrps, evtGrp));             
            }
        };

        this.subscription = this.IgniteVersion.currentSbj.pipe(
            tap(rebuildDropdowns),
            tap(() => filterModel(this.clonedCluster))
        )
        .subscribe();

        this.supportedJdbcTypes = this.SqlTypes.mkJdbcTypeOptions();

        this.$scope.ui = this.IgniteFormUtils.formUI();
        this.$scope.ui.loadedPanels = ['checkpoint'];

        this.formActions = [
            {text: 'Save', icon: 'checkmark', click: () => this.save(false)},
            {text: 'Save and Download', icon: 'download', click: () => this.save(true)}
        ];
    }

    $onDestroy() {
        this.subscription.unsubscribe();
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
            this.cachesMenu = (changes.caches.currentValue || []).map((c) => ({label: c.name, value: c.id}));            
        }
    }

    /**
     * The form should accept incoming cluster value if:
     * 1. It has different id ("new" to real id).
     * 2. Different caches or models (imported from DB).
     * @param a Incoming value.
     * @param b Current value.
     */
    shouldOverwriteValue<T>(a: T, b: T) {
        return get(a, 'id') !== get(b, 'id') ||
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
        return this.IgniteConfirm
            .confirm('Are you sure you want to undo all changes for current cluster?')
            .then(this.reset);
    }
}

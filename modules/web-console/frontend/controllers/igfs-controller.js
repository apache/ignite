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

// Controller for IGFS screen.
export default ['igfsController', [
    '$scope', '$http', '$state', '$filter', '$timeout', 'IgniteLegacyUtils', 'IgniteMessages', 'IgniteConfirm', 'IgniteClone', 'IgniteLoading', 'IgniteModelNormalizer', 'IgniteUnsavedChangesGuard', 'IgniteLegacyTable', 'IgniteConfigurationResource', 'IgniteErrorPopover', 'IgniteFormUtils',
    function($scope, $http, $state, $filter, $timeout, LegacyUtils, Messages, Confirm, Clone, Loading, ModelNormalizer, UnsavedChangesGuard, LegacyTable, Resource, ErrorPopover, FormUtils) {
        UnsavedChangesGuard.install($scope);

        const emptyIgfs = {empty: true};

        let __original_value;

        const blank = {
            ipcEndpointConfiguration: {},
            secondaryFileSystem: {}
        };

        // We need to initialize backupItem with empty object in order to properly used from angular directives.
        $scope.backupItem = emptyIgfs;

        $scope.ui = FormUtils.formUI();
        $scope.ui.activePanels = [0];
        $scope.ui.topPanels = [0];

        $scope.compactJavaName = FormUtils.compactJavaName;
        $scope.widthIsSufficient = FormUtils.widthIsSufficient;
        $scope.saveBtnTipText = FormUtils.saveBtnTipText;

        $scope.tableSave = function(field, index, stopEdit) {
            if (field.type === 'pathModes' && LegacyTable.tablePairSaveVisible(field, index))
                return LegacyTable.tablePairSave($scope.tablePairValid, $scope.backupItem, field, index, stopEdit);

            return true;
        };

        $scope.tableReset = (trySave) => {
            const field = LegacyTable.tableField();

            if (trySave && LegacyUtils.isDefined(field) && !$scope.tableSave(field, LegacyTable.tableEditedRowIndex(), true))
                return false;

            LegacyTable.tableReset();

            return true;
        };

        $scope.tableNewItem = function(field) {
            if ($scope.tableReset(true))
                LegacyTable.tableNewItem(field);
        };

        $scope.tableNewItemActive = LegacyTable.tableNewItemActive;

        $scope.tableStartEdit = function(item, field, index) {
            if ($scope.tableReset(true))
                LegacyTable.tableStartEdit(item, field, index, $scope.tableSave);
        };

        $scope.tableEditing = LegacyTable.tableEditing;
        $scope.tablePairSave = LegacyTable.tablePairSave;
        $scope.tablePairSaveVisible = LegacyTable.tablePairSaveVisible;

        $scope.tableRemove = function(item, field, index) {
            if ($scope.tableReset(true))
                LegacyTable.tableRemove(item, field, index);
        };

        $scope.tablePairValid = function(item, field, index, stopEdit) {
            const pairValue = LegacyTable.tablePairValue(field, index);

            const model = item[field.model];

            if (LegacyUtils.isDefined(model)) {
                const idx = _.findIndex(model, function(pair) {
                    return pair.path === pairValue.key;
                });

                // Found duplicate.
                if (idx >= 0 && idx !== index) {
                    if (stopEdit)
                        return false;

                    return ErrorPopover.show(LegacyTable.tableFieldId(index, 'KeyPathMode'), 'Such path already exists!', $scope.ui, 'misc');
                }
            }

            return true;
        };

        $scope.tblPathModes = {
            type: 'pathModes',
            model: 'pathModes',
            focusId: 'PathMode',
            ui: 'table-pair',
            keyName: 'path',
            valueName: 'mode',
            save: $scope.tableSave
        };

        $scope.igfsModes = LegacyUtils.mkOptions(['PRIMARY', 'PROXY', 'DUAL_SYNC', 'DUAL_ASYNC']);

        $scope.contentVisible = function() {
            const item = $scope.backupItem;

            return !item.empty && (!item._id || _.find($scope.displayedRows, {_id: item._id}));
        };

        $scope.toggleExpanded = function() {
            $scope.ui.expanded = !$scope.ui.expanded;

            ErrorPopover.hide();
        };

        $scope.igfss = [];
        $scope.clusters = [];

        function selectFirstItem() {
            if ($scope.igfss.length > 0)
                $scope.selectItem($scope.igfss[0]);
        }

        Loading.start('loadingIgfsScreen');

        // When landing on the page, get IGFSs and show them.
        Resource.read()
            .then(({spaces, clusters, igfss}) => {
                $scope.spaces = spaces;

                $scope.igfss = igfss || [];

                // For backward compatibility set colocateMetadata and relaxedConsistency default values.
                _.forEach($scope.igfss, (igfs) => {
                    if (_.isUndefined(igfs.colocateMetadata))
                        igfs.colocateMetadata = true;

                    if (_.isUndefined(igfs.relaxedConsistency))
                        igfs.relaxedConsistency = true;
                });

                $scope.clusters = _.map(clusters || [], (cluster) => ({
                    label: cluster.name,
                    value: cluster._id
                }));

                if ($state.params.linkId)
                    $scope.createItem($state.params.linkId);
                else {
                    const lastSelectedIgfs = angular.fromJson(sessionStorage.lastSelectedIgfs);

                    if (lastSelectedIgfs) {
                        const idx = _.findIndex($scope.igfss, function(igfs) {
                            return igfs._id === lastSelectedIgfs;
                        });

                        if (idx >= 0)
                            $scope.selectItem($scope.igfss[idx]);
                        else {
                            sessionStorage.removeItem('lastSelectedIgfs');

                            selectFirstItem();
                        }
                    }
                    else
                        selectFirstItem();
                }

                $scope.$watch('ui.inputForm.$valid', function(valid) {
                    if (valid && ModelNormalizer.isEqual(__original_value, $scope.backupItem))
                        $scope.ui.inputForm.$dirty = false;
                });

                $scope.$watch('backupItem', function(val) {
                    if (!$scope.ui.inputForm)
                        return;

                    const form = $scope.ui.inputForm;

                    if (form.$valid && ModelNormalizer.isEqual(__original_value, val))
                        form.$setPristine();
                    else
                        form.$setDirty();
                }, true);

                $scope.$watch('ui.activePanels.length', () => {
                    ErrorPopover.hide();
                });
            })
            .catch(Messages.showError)
            .then(() => {
                $scope.ui.ready = true;
                $scope.ui.inputForm && $scope.ui.inputForm.$setPristine();

                Loading.finish('loadingIgfsScreen');
            });

        $scope.selectItem = function(item, backup) {
            function selectItem() {
                LegacyTable.tableReset();

                $scope.selectedItem = item;

                try {
                    if (item && item._id)
                        sessionStorage.lastSelectedIgfs = angular.toJson(item._id);
                    else
                        sessionStorage.removeItem('lastSelectedIgfs');
                }
                catch (ignored) {
                    // No-op.
                }

                if (backup)
                    $scope.backupItem = backup;
                else if (item)
                    $scope.backupItem = angular.copy(item);
                else
                    $scope.backupItem = emptyIgfs;

                $scope.backupItem = _.merge({}, blank, $scope.backupItem);

                if ($scope.ui.inputForm) {
                    $scope.ui.inputForm.$error = {};
                    $scope.ui.inputForm.$setPristine();
                }

                __original_value = ModelNormalizer.normalize($scope.backupItem);

                if (LegacyUtils.getQueryVariable('new'))
                    $state.go('base.configuration.igfs');
            }

            FormUtils.confirmUnsavedChanges($scope.backupItem && $scope.ui.inputForm && $scope.ui.inputForm.$dirty, selectItem);
        };

        $scope.linkId = () => $scope.backupItem._id ? $scope.backupItem._id : 'create';

        function prepareNewItem(linkId) {
            return {
                space: $scope.spaces[0]._id,
                ipcEndpointEnabled: true,
                fragmentizerEnabled: true,
                colocateMetadata: true,
                relaxedConsistency: true,
                clusters: linkId && _.find($scope.clusters, {value: linkId}) ? [linkId] :
                    (_.isEmpty($scope.clusters) ? [] : [$scope.clusters[0].value])
            };
        }

        // Add new IGFS.
        $scope.createItem = function(linkId) {
            if ($scope.tableReset(true)) {
                $timeout(() => FormUtils.ensureActivePanel($scope.ui, 'general', 'igfsNameInput'));

                $scope.selectItem(null, prepareNewItem(linkId));
            }
        };

        // Check IGFS logical consistency.
        function validate(item) {
            ErrorPopover.hide();

            if (LegacyUtils.isEmptyString(item.name))
                return ErrorPopover.show('igfsNameInput', 'IGFS name should not be empty!', $scope.ui, 'general');

            if (!LegacyUtils.checkFieldValidators($scope.ui))
                return false;

            if (!item.secondaryFileSystemEnabled && (item.defaultMode === 'PROXY'))
                return ErrorPopover.show('secondaryFileSystem-title', 'Secondary file system should be configured for "PROXY" IGFS mode!', $scope.ui, 'secondaryFileSystem');

            if (item.pathModes) {
                for (let pathIx = 0; pathIx < item.pathModes.length; pathIx++) {
                    if (!item.secondaryFileSystemEnabled && item.pathModes[pathIx].mode === 'PROXY')
                        return ErrorPopover.show('secondaryFileSystem-title', 'Secondary file system should be configured for "PROXY" path mode!', $scope.ui, 'secondaryFileSystem');
                }
            }

            return true;
        }

        // Save IGFS in database.
        function save(item) {
            $http.post('/api/v1/configuration/igfs/save', item)
                .then(({data}) => {
                    const _id = data;

                    $scope.ui.inputForm.$setPristine();

                    const idx = _.findIndex($scope.igfss, {_id});

                    if (idx >= 0)
                        _.assign($scope.igfss[idx], item);
                    else {
                        item._id = _id;
                        $scope.igfss.push(item);
                    }

                    $scope.selectItem(item);

                    Messages.showInfo(`IGFS "${item.name}" saved.`);
                })
                .catch(Messages.showError);
        }

        // Save IGFS.
        $scope.saveItem = function() {
            if ($scope.tableReset(true)) {
                const item = $scope.backupItem;

                if (validate(item))
                    save(item);
            }
        };

        function _igfsNames() {
            return _.map($scope.igfss, function(igfs) {
                return igfs.name;
            });
        }

        // Clone IGFS with new name.
        $scope.cloneItem = function() {
            if ($scope.tableReset(true) && validate($scope.backupItem)) {
                Clone.confirm($scope.backupItem.name, _igfsNames()).then(function(newName) {
                    const item = angular.copy($scope.backupItem);

                    delete item._id;

                    item.name = newName;

                    save(item);
                });
            }
        };

        // Remove IGFS from db.
        $scope.removeItem = function() {
            LegacyTable.tableReset();

            const selectedItem = $scope.selectedItem;

            Confirm.confirm('Are you sure you want to remove IGFS: "' + selectedItem.name + '"?')
                .then(function() {
                    const _id = selectedItem._id;

                    $http.post('/api/v1/configuration/igfs/remove', {_id})
                        .then(() => {
                            Messages.showInfo('IGFS has been removed: ' + selectedItem.name);

                            const igfss = $scope.igfss;

                            const idx = _.findIndex(igfss, function(igfs) {
                                return igfs._id === _id;
                            });

                            if (idx >= 0) {
                                igfss.splice(idx, 1);

                                $scope.ui.inputForm.$setPristine();

                                if (igfss.length > 0)
                                    $scope.selectItem(igfss[0]);
                                else
                                    $scope.backupItem = emptyIgfs;
                            }
                        })
                        .catch(Messages.showError);
                });
        };

        // Remove all IGFS from db.
        $scope.removeAllItems = function() {
            LegacyTable.tableReset();

            Confirm.confirm('Are you sure you want to remove all IGFS?')
                .then(function() {
                    $http.post('/api/v1/configuration/igfs/remove/all')
                        .then(() => {
                            Messages.showInfo('All IGFS have been removed');

                            $scope.igfss = [];
                            $scope.backupItem = emptyIgfs;
                            $scope.ui.inputForm.$error = {};
                            $scope.ui.inputForm.$setPristine();
                        })
                        .catch(Messages.showError);
                });
        };

        $scope.resetAll = function() {
            LegacyTable.tableReset();

            Confirm.confirm('Are you sure you want to undo all changes for current IGFS?')
                .then(function() {
                    $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();
                    $scope.ui.inputForm.$error = {};
                    $scope.ui.inputForm.$setPristine();
                });
        };
    }
]];

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
import consoleModule from 'controllers/common-module';

consoleModule.controller('igfsController', [
    '$scope', '$http', '$state', '$filter', '$timeout', '$common', '$confirm', '$clone', '$loading', '$cleanup', '$unsavedChangesGuard', '$table',
    function($scope, $http, $state, $filter, $timeout, $common, $confirm, $clone, $loading, $cleanup, $unsavedChangesGuard, $table) {
        $unsavedChangesGuard.install($scope);

        const emptyIgfs = {empty: true};

        let __original_value;

        const blank = {
            ipcEndpointConfiguration: {},
            secondaryFileSystem: {}
        };

        // We need to initialize backupItem with empty object in order to properly used from angular directives.
        $scope.backupItem = emptyIgfs;

        $scope.ui = $common.formUI();
        $scope.ui.activePanels = [0];
        $scope.ui.topPanels = [0];

        $scope.compactJavaName = $common.compactJavaName;
        $scope.widthIsSufficient = $common.widthIsSufficient;
        $scope.saveBtnTipText = $common.saveBtnTipText;

        const showPopoverMessage = $common.showPopoverMessage;

        // TODO LEGACY start
        $scope.tableSave = function(field, index, stopEdit) {
            if (field.type === 'pathModes' && $table.tablePairSaveVisible(field, index))
                return $table.tablePairSave($scope.tablePairValid, $scope.backupItem, field, index, stopEdit);

            return true;
        };

        $scope.tableReset = (trySave) => {
            const field = $table.tableField();

            if (trySave && $common.isDefined(field) && !$scope.tableSave(field, $table.tableEditedRowIndex(), true))
                return false;

            $table.tableReset();

            return true;
        };

        $scope.tableNewItem = function(field) {
            if ($scope.tableReset(true))
                $table.tableNewItem(field);
        };

        $scope.tableNewItemActive = $table.tableNewItemActive;

        $scope.tableStartEdit = function(item, field, index) {
            if ($scope.tableReset(true))
                $table.tableStartEdit(item, field, index, $scope.tableSave);
        };

        $scope.tableEditing = $table.tableEditing;
        $scope.tablePairSave = $table.tablePairSave;
        $scope.tablePairSaveVisible = $table.tablePairSaveVisible;

        $scope.tableRemove = function(item, field, index) {
            if ($scope.tableReset(true))
                $table.tableRemove(item, field, index);
        };

        $scope.tablePairValid = function(item, field, index) {
            const pairValue = $table.tablePairValue(field, index);

            const model = item[field.model];

            if ($common.isDefined(model)) {
                const idx = _.findIndex(model, function(pair) {
                    return pair.path === pairValue.key;
                });

                // Found duplicate.
                if (idx >= 0 && idx !== index)
                    return showPopoverMessage($scope.ui, 'misc', $table.tableFieldId(index, 'KeyPathMode'), 'Such path already exists!');
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

        $scope.igfsModes = $common.mkOptions(['PRIMARY', 'PROXY', 'DUAL_SYNC', 'DUAL_ASYNC']);
        // TODO LEGACY start - end

        $scope.contentVisible = function() {
            const item = $scope.backupItem;

            return !item.empty && (!item._id || _.find($scope.displayedRows, {_id: item._id}));
        };

        $scope.toggleExpanded = function() {
            $scope.ui.expanded = !$scope.ui.expanded;

            $common.hidePopover();
        };

        $scope.igfss = [];
        $scope.clusters = [];

        function selectFirstItem() {
            if ($scope.igfss.length > 0)
                $scope.selectItem($scope.igfss[0]);
        }

        $loading.start('loadingIgfsScreen');

        // When landing on the page, get IGFSs and show them.
        $http.post('/api/v1/configuration/igfs/list')
            .success(function(data) {
                $scope.spaces = data.spaces;

                $scope.igfss = data.igfss || [];

                // For backward compatibility set colocateMetadata and relaxedConsistency default values.
                _.forEach($scope.igfss, (igfs) => {
                    if (_.isUndefined(igfs.colocateMetadata))
                        igfs.colocateMetadata = true;

                    if (_.isUndefined(igfs.relaxedConsistency))
                        igfs.relaxedConsistency = true;
                });

                $scope.clusters = _.map(data.clusters || [], function(cluster) {
                    return {
                        value: cluster._id,
                        label: cluster.name
                    };
                });

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
                    if (valid && _.isEqual(__original_value, $cleanup($scope.backupItem)))
                        $scope.ui.inputForm.$dirty = false;
                });

                $scope.$watch('backupItem', function(val) {
                    const form = $scope.ui.inputForm;

                    if (form.$pristine || (form.$valid && _.isEqual(__original_value, $cleanup(val))))
                        form.$setPristine();
                    else
                        form.$setDirty();
                }, true);
            })
            .catch(function(errMsg) {
                $common.showError(errMsg);
            })
            .finally(function() {
                $scope.ui.ready = true;
                $scope.ui.inputForm.$setPristine();
                $loading.finish('loadingIgfsScreen');
            });

        $scope.selectItem = function(item, backup) {
            function selectItem() {
                $table.tableReset(); // TODO LEGACY

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

                $scope.backupItem = angular.merge({}, blank, $scope.backupItem);

                __original_value = $cleanup($scope.backupItem);

                if ($common.getQueryVariable('new'))
                    $state.go('base.configuration.igfs');
            }

            $common.confirmUnsavedChanges($scope.backupItem && $scope.ui.inputForm.$dirty, selectItem);
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
            if ($scope.tableReset(true)) { // TODO LEGACY
                $timeout(() => $common.ensureActivePanel($scope.ui, 'general', 'igfsName'));

                $scope.selectItem(null, prepareNewItem(linkId));
            }
        };

        // Check IGFS logical consistency.
        function validate(item) {
            $common.hidePopover();

            if ($common.isEmptyString(item.name))
                return showPopoverMessage($scope.ui, 'general', 'igfsName', 'IGFS name should not be empty!');

            if (!$common.checkFieldValidators($scope.ui))
                return false;

            if (!item.secondaryFileSystemEnabled && (item.defaultMode === 'PROXY'))
                return showPopoverMessage($scope.ui, 'secondaryFileSystem', 'secondaryFileSystem-title', 'Secondary file system should be configured for "PROXY" IGFS mode!');

            if (item.pathModes) {
                for (let pathIx = 0; pathIx < item.pathModes.length; pathIx++) {
                    if (!item.secondaryFileSystemEnabled && item.pathModes[pathIx].mode === 'PROXY')
                        return showPopoverMessage($scope.ui, 'secondaryFileSystem', 'secondaryFileSystem-title', 'Secondary file system should be configured for "PROXY" path mode!');
                }
            }

            return true;
        }

        // Save IGFS in database.
        function save(item) {
            $http.post('/api/v1/configuration/igfs/save', item)
                .success(function(_id) {
                    $scope.ui.inputForm.$setPristine();

                    const idx = _.findIndex($scope.igfss, function(igfs) {
                        return igfs._id === _id;
                    });

                    if (idx >= 0)
                        angular.merge($scope.igfss[idx], item);
                    else {
                        item._id = _id;
                        $scope.igfss.push(item);
                    }

                    $scope.selectItem(item);

                    $common.showInfo('IGFS "' + item.name + '" saved.');
                })
                .error(function(errMsg) {
                    $common.showError(errMsg);
                });
        }

        // Save IGFS.
        $scope.saveItem = function() {
            if ($scope.tableReset(true)) { // TODO LEGACY
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
            if ($scope.tableReset(true) && validate($scope.backupItem)) { // TODO LEGACY
                $clone.confirm($scope.backupItem.name, _igfsNames()).then(function(newName) {
                    const item = angular.copy($scope.backupItem);

                    delete item._id;

                    item.name = newName;

                    save(item);
                });
            }
        };

        // Remove IGFS from db.
        $scope.removeItem = function() {
            $table.tableReset(); // TODO LEGACY

            const selectedItem = $scope.selectedItem;

            $confirm.confirm('Are you sure you want to remove IGFS: "' + selectedItem.name + '"?')
                .then(function() {
                    const _id = selectedItem._id;

                    $http.post('/api/v1/configuration/igfs/remove', {_id})
                        .success(function() {
                            $common.showInfo('IGFS has been removed: ' + selectedItem.name);

                            const igfss = $scope.igfss;

                            const idx = _.findIndex(igfss, function(igfs) {
                                return igfs._id === _id;
                            });

                            if (idx >= 0) {
                                igfss.splice(idx, 1);

                                if (igfss.length > 0)
                                    $scope.selectItem(igfss[0]);
                                else {
                                    $scope.backupItem = emptyIgfs;
                                    $scope.ui.inputForm.$setPristine();
                                }
                            }
                        })
                        .error(function(errMsg) {
                            $common.showError(errMsg);
                        });
                });
        };

        // Remove all IGFS from db.
        $scope.removeAllItems = function() {
            $table.tableReset(); // TODO LEGACY

            $confirm.confirm('Are you sure you want to remove all IGFS?')
                .then(function() {
                    $http.post('/api/v1/configuration/igfs/remove/all')
                        .success(function() {
                            $common.showInfo('All IGFS have been removed');

                            $scope.igfss = [];
                            $scope.backupItem = emptyIgfs;
                            $scope.ui.inputForm.$setPristine();
                        })
                        .error(function(errMsg) {
                            $common.showError(errMsg);
                        });
                });
        };

        $scope.resetAll = function() {
            $table.tableReset(); // TODO LEGACY

            $confirm.confirm('Are you sure you want to undo all changes for current IGFS?')
                .then(function() {
                    $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();
                    $scope.ui.inputForm.$setPristine();
                });
        };
    }]
);

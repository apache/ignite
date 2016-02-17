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
consoleModule.controller('igfsController', [
    '$scope', '$state', '$controller', '$filter', '$http', '$timeout', '$common', '$focus', '$confirm', '$clone', '$table', '$preview', '$loading', '$unsavedChangesGuard',
    function ($scope, $state, $controller, $filter, $http, $timeout, $common, $focus, $confirm, $clone, $table, $preview, $loading, $unsavedChangesGuard) {
            $unsavedChangesGuard.install($scope);

            // Initialize the super class and extend it.
            angular.extend(this, $controller('save-remove', {$scope: $scope}));

            $scope.ui = $common.formUI();
            $scope.ui.activePanels = [0];
            $scope.ui.topPanels = [0];

            $scope.joinTip = $common.joinTip;
            $scope.getModel = $common.getModel;
            $scope.javaBuiltInClasses = $common.javaBuiltInClasses;
            $scope.compactJavaName = $common.compactJavaName;
            $scope.widthIsSufficient = $common.widthIsSufficient;
            $scope.saveBtnTipText = $common.saveBtnTipText;
            $scope.panelExpanded = $common.panelExpanded;

            $scope.tableVisibleRow = $table.tableVisibleRow;

            $scope.tableSave = function (field, index, stopEdit) {
                switch (field.type) {
                    case 'pathModes':
                        if ($table.tablePairSaveVisible(field, index))
                            return $table.tablePairSave($scope.tablePairValid, $scope.backupItem, field, index, stopEdit);

                        break;
                }

                return true;
            };

            $scope.tableReset = function (save) {
                var field = $table.tableField();

                if (!save || !$common.isDefined(field) || $scope.tableSave(field, $table.tableEditedRowIndex(), true)) {
                    $table.tableReset();

                    return true;
                }

                return false;
            };

            $scope.tableNewItem = function (field) {
                if ($scope.tableReset(true))
                    $table.tableNewItem(field);
            };

            $scope.tableNewItemActive = $table.tableNewItemActive;

            $scope.tableStartEdit = function (item, field, index) {
                if ($scope.tableReset(true))
                    $table.tableStartEdit(item, field, index);
            };

            $scope.tableEditing = $table.tableEditing;

            $scope.tableRemove = function (item, field, index) {
                if ($scope.tableReset(true))
                    $table.tableRemove(item, field, index);
            };

            $scope.tableSimpleSave = $table.tableSimpleSave;
            $scope.tableSimpleSaveVisible = $table.tableSimpleSaveVisible;

            $scope.tableSimpleUp = function (item, field, index) {
                if ($scope.tableReset(true))
                    $table.tableSimpleUp(item, field, index);
            };

            $scope.tableSimpleDown = function (item, field, index) {
                if ($scope.tableReset(true))
                    $table.tableSimpleDown(item, field, index);
            };

            $scope.tableSimpleDownVisible = $table.tableSimpleDownVisible;

            $scope.tablePairSave = $table.tablePairSave;
            $scope.tablePairSaveVisible = $table.tablePairSaveVisible;

            var previews = [];

            $scope.previewInit = function (preview) {
                previews.push(preview);

                $preview.previewInit(preview);
            };

            $scope.previewChanged = $preview.previewChanged;

            $scope.hidePopover = $common.hidePopover;

            var showPopoverMessage = $common.showPopoverMessage;

            $scope.igfsModes = $common.mkOptions(['PRIMARY', 'PROXY', 'DUAL_SYNC', 'DUAL_ASYNC']);

            $scope.ipcTypes = $common.mkOptions(['SHMEM', 'TCP']);

            $scope.toggleExpanded = function () {
                $scope.ui.expanded = !$scope.ui.expanded;

                $common.hidePopover();
            };

            $scope.general = [];
            $scope.advanced = [];
            $scope.igfss = [];
            $scope.clusters = [];

            $scope.preview = {
                general: {xml: '', java: '', allDefaults: true},
                ipc: {xml: '', java: '', allDefaults: true},
                fragmentizer: {xml: '', java: '', allDefaults: true},
                dualMode: {xml: '', java: '', allDefaults: true},
                secondaryFileSystem: {xml: '', java: '', allDefaults: true},
                misc: {xml: '', java: '', allDefaults: true}
            };

            $scope.tablePairValid = function (item, field, index) {
                var pairValue = $table.tablePairValue(field, index);

                var model = item[field.model];

                if ($common.isDefined(model)) {
                    var idx = _.findIndex(model, function (pair) {
                        return pair.path === pairValue.key;
                    });

                    // Found duplicate.
                    if (idx >= 0 && idx !== index)
                        return showPopoverMessage($scope.ui, 'misc', $table.tableFieldId(index, 'KeyPathMode'), 'Such path already exists!');
                }

                return true;
            };

            function selectFirstItem() {
                if ($scope.igfss.length > 0)
                    $scope.selectItem($scope.igfss[0]);
            }

            $loading.start('loadingIgfsScreen');

            // When landing on the page, get IGFSs and show them.
            $http.post('/api/v1/configuration/igfs/list')
                .success(function (data) {
                    $scope.spaces = data.spaces;
                    $scope.igfss = data.igfss;
                    $scope.clusters = _.map(data.clusters, function (cluster) {
                        return {
                            value: cluster._id,
                            label: cluster.name
                        };
                    });

                    // Load page descriptor.
                    $http.get('/models/igfs.json')
                        .success(function (data) {
                            $scope.general = data.general;
                            $scope.advanced = data.advanced;

                            $scope.ui.addGroups(data.general, data.advanced);

                            if ($state.params.id)
                                $scope.createItem($state.params.id);
                            else {
                                var lastSelectedIgfs = angular.fromJson(sessionStorage.lastSelectedIgfs);

                                if (lastSelectedIgfs) {
                                    var idx = _.findIndex($scope.igfss, function (igfs) {
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

                            $scope.$watch('backupItem', function (val) {
                                if (val) {
                                    var srcItem = $scope.selectedItem ? $scope.selectedItem : prepareNewItem();

                                    $scope.ui.checkDirty(val, srcItem);

                                    var varName = $commonUtils.toJavaName('igfs', val.name);

                                    $scope.preview.general.xml = $generatorXml.igfsGeneral(val).asString();
                                    $scope.preview.general.java = $generatorJava.igfsGeneral(val, varName).asString();
                                    $scope.preview.general.allDefaults = $common.isEmptyString($scope.preview.general.xml);

                                    $scope.preview.ipc.xml = $generatorXml.igfsIPC(val).asString();
                                    $scope.preview.ipc.java = $generatorJava.igfsIPC(val, varName).asString();
                                    $scope.preview.ipc.allDefaults = $common.isEmptyString($scope.preview.ipc.xml);

                                    $scope.preview.fragmentizer.xml = $generatorXml.igfsFragmentizer(val).asString();
                                    $scope.preview.fragmentizer.java = $generatorJava.igfsFragmentizer(val, varName).asString();
                                    $scope.preview.fragmentizer.allDefaults = $common.isEmptyString($scope.preview.fragmentizer.xml);

                                    $scope.preview.dualMode.xml = $generatorXml.igfsDualMode(val).asString();
                                    $scope.preview.dualMode.java = $generatorJava.igfsDualMode(val, varName).asString();
                                    $scope.preview.dualMode.allDefaults = $common.isEmptyString($scope.preview.dualMode.xml);

                                    $scope.preview.secondaryFileSystem.xml = $generatorXml.igfsSecondFS(val).asString();
                                    $scope.preview.secondaryFileSystem.java = $generatorJava.igfsSecondFS(val, varName).asString();
                                    $scope.preview.secondaryFileSystem.allDefaults = $common.isEmptyString($scope.preview.secondaryFileSystem.xml);

                                    $scope.preview.misc.xml = $generatorXml.igfsMisc(val).asString();
                                    $scope.preview.misc.java = $generatorJava.igfsMisc(val, varName).asString();
                                    $scope.preview.misc.allDefaults = $common.isEmptyString($scope.preview.misc.xml);
                                }
                            }, true);
                        })
                        .error(function (errMsg) {
                            $common.showError(errMsg);
                        });
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                })
                .finally(function () {
                    $scope.ui.ready = true;
                    $loading.finish('loadingIgfsScreen');
                });

            $scope.selectItem = function (item, backup) {
                function selectItem() {
                    $table.tableReset();

                    $scope.selectedItem = angular.copy(item);

                    try {
                        if (item)
                            sessionStorage.lastSelectedIgfs = angular.toJson(item._id);
                        else
                            sessionStorage.removeItem('lastSelectedIgfs');
                    }
                    catch (error) { }

                    _.forEach(previews, function(preview) {
                        preview.attractAttention = false;
                    });

                    if (backup)
                        $scope.backupItem = backup;
                    else if (item)
                        $scope.backupItem = angular.copy(item);
                    else
                        $scope.backupItem = undefined;

                    if ($common.getQueryVariable('new'))
                        $state.go('base.configuration.igfs');
                }

                $common.confirmUnsavedChanges($scope.ui.isDirty(), selectItem);
            };

            function prepareNewItem(id) {
                return {
                    space: $scope.spaces[0]._id,
                    ipcEndpointEnabled: true,
                    fragmentizerEnabled: true,
                    clusters: id && _.find($scope.clusters, {value: id}) ? [id] :
                        (!$common.isEmptyArray($scope.clusters) ? [$scope.clusters[0].value] : [])
                };
            }

            // Add new IGFS.
            $scope.createItem = function (id) {
                if ($scope.tableReset(true)) {
                    $timeout(function () {
                        $common.ensureActivePanel($scope.ui, 'general', 'igfsName');
                    });

                    $scope.selectItem(undefined, prepareNewItem(id));
                }
            };

            // Check IGFS logical consistency.
            function validate(item) {
                if ($common.isEmptyString(item.name))
                    return showPopoverMessage($scope.ui, 'general', 'igfsName', 'Name should not be empty');

                if (!$common.isEmptyString(item.dualModePutExecutorService) &&
                    !$common.isValidJavaClass('Put executor service', item.dualModePutExecutorService, false, 'dualModePutExecutorService', false, $scope.ui, 'dualMode'))
                    return false;

                if (!item.secondaryFileSystemEnabled && (item.defaultMode === 'PROXY'))
                    return showPopoverMessage($scope.ui, 'secondaryFileSystem', 'secondaryFileSystem-title', 'Secondary file system should be configured for "PROXY" IGFS mode');

                if (item.pathModes) {
                    for (var pathIx = 0; pathIx < item.pathModes.length; pathIx ++) {
                        if (!item.secondaryFileSystemEnabled && item.pathModes[pathIx].mode === 'PROXY')
                            return showPopoverMessage($scope.ui, 'misc', 'secondaryFileSystem-title', 'Secondary file system should be configured for "PROXY" path mode');
                    }
                }

                return true;
            }

            // Save IGFS into database.
            function save(item) {
                $http.post('/api/v1/configuration/igfs/save', item)
                    .success(function (_id) {
                        $scope.ui.markPristine();

                        var idx = _.findIndex($scope.igfss, function (igfs) {
                            return igfs._id === _id;
                        });

                        if (idx >= 0)
                            angular.extend($scope.igfss[idx], item);
                        else {
                            item._id = _id;
                            $scope.igfss.push(item);
                        }

                        $scope.selectItem(item);

                        $common.showInfo('IGFS "' + item.name + '" saved.');
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    });
            }

            // Save IGFS.
            $scope.saveItem = function () {
                if ($scope.tableReset(true)) {
                    var item = $scope.backupItem;

                    if (validate(item))
                        save(item);
                }
            };

            function _igfsNames() {
                return _.map($scope.igfss, function (igfs) {
                    return igfs.name;
                });
            }

            // Save IGFS with new name.
            $scope.cloneItem = function () {
                if ($scope.tableReset(true) && validate($scope.backupItem)) {
                    $clone.confirm($scope.backupItem.name, _igfsNames()).then(function (newName) {
                        var item = angular.copy($scope.backupItem);

                        delete item._id;
                        item.name = newName;

                        save(item);
                    });
                }
            };

            // Remove IGFS from db.
            $scope.removeItem = function () {
                $table.tableReset();

                var selectedItem = $scope.selectedItem;

                $confirm.confirm('Are you sure you want to remove IGFS: "' + selectedItem.name + '"?')
                    .then(function () {
                        var _id = selectedItem._id;

                            $http.post('/api/v1/configuration/igfs/remove', {_id: _id})
                                .success(function () {
                                    $common.showInfo('IGFS has been removed: ' + selectedItem.name);

                                var igfss = $scope.igfss;

                                var idx = _.findIndex(igfss, function (igfs) {
                                    return igfs._id === _id;
                                });

                                if (idx >= 0) {
                                    igfss.splice(idx, 1);

                                    if (igfss.length > 0)
                                        $scope.selectItem(igfss[0]);
                                    else
                                        $scope.selectItem(undefined, undefined);
                                }
                            })
                            .error(function (errMsg) {
                                $common.showError(errMsg);
                            });
                    });
            };

            // Remove all igfss from db.
            $scope.removeAllItems = function () {
                $table.tableReset();

                $confirm.confirm('Are you sure you want to remove all IGFS?')
                    .then(function () {
                            $http.post('/api/v1/configuration/igfs/remove/all')
                                .success(function () {
                                    $common.showInfo('All IGFS have been removed');

                                $scope.igfss = [];
                                $scope.ui.markPristine();
                                $scope.selectItem(undefined, undefined);
                            })
                            .error(function (errMsg) {
                                $common.showError(errMsg);
                            });
                    });
            };

            $scope.resetItem = function (group) {
                var resetTo = $scope.selectedItem;

                if (!$common.isDefined(resetTo))
                    resetTo = prepareNewItem();

                $common.resetItem($scope.backupItem, resetTo, $scope.general, group);
                $common.resetItem($scope.backupItem, resetTo, $scope.advanced, group);
            };

            $scope.resetAll = function() {
                $table.tableReset();

                $confirm.confirm('Are you sure you want to undo all changes for current IGFS?')
                    .then(function() {
                        $scope.backupItem = $scope.selectedItem ? angular.copy($scope.selectedItem) : prepareNewItem();
                        $scope.ui.markPristine();
                    });
            };
        }]
);

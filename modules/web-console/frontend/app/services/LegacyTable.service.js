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

// TODO: Refactor this service for legacy tables with more than one input field.
export default ['IgniteLegacyTable',
    ['IgniteLegacyUtils', 'IgniteFocus', 'IgniteErrorPopover', function(LegacyUtils, Focus, ErrorPopover) {
        function _model(item, field) {
            let path = field.path;

            if (_.isNil(path) || _.isNil(item))
                return item;

            path = path.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
            path = path.replace(/^\./, ''); // strip a leading dot

            const segs = path.split('.');
            let root = item;

            while (segs.length > 0) {
                const pathStep = segs.shift();

                if (typeof root[pathStep] === 'undefined')
                    root[pathStep] = {};

                root = root[pathStep];
            }

            return root;
        }

        const table = {name: 'none', editIndex: -1};

        function _tableReset() {
            delete table.field;
            table.name = 'none';
            table.editIndex = -1;

            ErrorPopover.hide();
        }

        function _tableSaveAndReset() {
            const field = table.field;

            const save = LegacyUtils.isDefined(field) && LegacyUtils.isDefined(field.save);

            if (!save || !LegacyUtils.isDefined(field) || field.save(field, table.editIndex, true)) {
                _tableReset();

                return true;
            }

            return false;
        }

        function _tableState(field, editIndex, specName) {
            table.field = field;
            table.name = specName || field.model;
            table.editIndex = editIndex;
        }

        function _tableUI(tbl) {
            const ui = tbl.ui;

            return ui ? ui : tbl.type;
        }

        function _tableFocus(focusId, index) {
            Focus.move((index < 0 ? 'new' : 'cur') + focusId + (index >= 0 ? index : ''));
        }

        function _tablePairValue(filed, index) {
            return index < 0 ? {key: filed.newKey, value: filed.newValue} : {
                key: filed.curKey,
                value: filed.curValue
            };
        }

        function _tableStartEdit(item, tbl, index, save) {
            _tableState(tbl, index);

            const val = _.get(_model(item, tbl), tbl.model)[index];

            const ui = _tableUI(tbl);

            tbl.save = save;

            if (ui === 'table-pair') {
                tbl.curKey = val[tbl.keyName];
                tbl.curValue = val[tbl.valueName];

                _tableFocus('Key' + tbl.focusId, index);
            }
            else if (ui === 'table-db-fields') {
                tbl.curDatabaseFieldName = val.databaseFieldName;
                tbl.curDatabaseFieldType = val.databaseFieldType;
                tbl.curJavaFieldName = val.javaFieldName;
                tbl.curJavaFieldType = val.javaFieldType;

                _tableFocus('DatabaseFieldName' + tbl.focusId, index);
            }
            else if (ui === 'table-indexes') {
                tbl.curIndexName = val.name;
                tbl.curIndexType = val.indexType;
                tbl.curIndexFields = val.fields;

                _tableFocus(tbl.focusId, index);
            }
        }

        function _tableNewItem(tbl) {
            _tableState(tbl, -1);

            const ui = _tableUI(tbl);

            if (ui === 'table-pair') {
                tbl.newKey = null;
                tbl.newValue = null;

                _tableFocus('Key' + tbl.focusId, -1);
            }
            else if (ui === 'table-db-fields') {
                tbl.newDatabaseFieldName = null;
                tbl.newDatabaseFieldType = null;
                tbl.newJavaFieldName = null;
                tbl.newJavaFieldType = null;

                _tableFocus('DatabaseFieldName' + tbl.focusId, -1);
            }
            else if (ui === 'table-indexes') {
                tbl.newIndexName = null;
                tbl.newIndexType = 'SORTED';
                tbl.newIndexFields = null;

                _tableFocus(tbl.focusId, -1);
            }
        }

        return {
            tableState: _tableState,
            tableReset: _tableReset,
            tableSaveAndReset: _tableSaveAndReset,
            tableNewItem: _tableNewItem,
            tableNewItemActive(tbl) {
                return table.name === tbl.model && table.editIndex < 0;
            },
            tableEditing(tbl, index) {
                return table.name === tbl.model && table.editIndex === index;
            },
            tableEditedRowIndex() {
                return table.editIndex;
            },
            tableField() {
                return table.field;
            },
            tableStartEdit: _tableStartEdit,
            tableRemove(item, field, index) {
                _tableReset();

                _.get(_model(item, field), field.model).splice(index, 1);
            },
            tablePairValue: _tablePairValue,
            tablePairSave(pairValid, item, field, index, stopEdit) {
                const valid = pairValid(item, field, index, stopEdit);

                if (valid) {
                    const pairValue = _tablePairValue(field, index);

                    let pairModel = {};

                    const container = _.get(item, field.model);

                    if (index < 0) {
                        pairModel[field.keyName] = pairValue.key;
                        pairModel[field.valueName] = pairValue.value;

                        if (container)
                            container.push(pairModel);
                        else
                            _.set(item, field.model, [pairModel]);

                        if (!stopEdit)
                            _tableNewItem(field);
                    }
                    else {
                        pairModel = container[index];

                        pairModel[field.keyName] = pairValue.key;
                        pairModel[field.valueName] = pairValue.value;

                        if (!stopEdit) {
                            if (index < container.length - 1)
                                _tableStartEdit(item, field, index + 1);
                            else
                                _tableNewItem(field);
                        }
                    }
                }

                return valid || stopEdit;
            },
            tablePairSaveVisible(field, index) {
                const pairValue = _tablePairValue(field, index);

                return !LegacyUtils.isEmptyString(pairValue.key) && !LegacyUtils.isEmptyString(pairValue.value);
            },
            tableFocusInvalidField(index, id) {
                _tableFocus(id, index);

                return false;
            },
            tableFieldId(index, id) {
                return (index < 0 ? 'new' : 'cur') + id + (index >= 0 ? index : '');
            }
        };
    }]];

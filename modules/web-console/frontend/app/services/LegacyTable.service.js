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

// TODO: Refactor this service for legacy tables with more than one input field.
/**
 * @param {ReturnType<typeof import('./LegacyUtils.service').default>} LegacyUtils
 * @param {ReturnType<typeof import('./Focus.service').default>} Focus
 * @param {import('./ErrorPopover.service').default} ErrorPopover
 */
export default function service(LegacyUtils, Focus, ErrorPopover) {
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
}

service.$inject = ['IgniteLegacyUtils', 'IgniteFocus', 'IgniteErrorPopover'];

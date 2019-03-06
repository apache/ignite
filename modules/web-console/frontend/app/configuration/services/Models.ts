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

import ObjectID from 'bson-objectid';
import omit from 'lodash/fp/omit';
import {DomainModel, IndexField, ShortDomainModel, Index, Field, KeyField, ValueField} from '../types';

export default class Models {
    static $inject = ['$http'];

    constructor(private $http: ng.IHttpService) {}

    getModel(modelID: string) {
        return this.$http.get<{data: DomainModel[]}>(`/api/v1/configuration/domains/${modelID}`);
    }

    getBlankModel(): DomainModel {
        return {
            _id: ObjectID.generate(),
            generatePojo: true,
            caches: [],
            queryKeyFields: [],
            queryMetadata: 'Configuration'
        };
    }

    queryMetadata = {
        values: [
            {label: 'Annotations', value: 'Annotations'},
            {label: 'Configuration', value: 'Configuration'}
        ]
    };

    indexType = {
        values: [
            {label: 'SORTED', value: 'SORTED'},
            {label: 'FULLTEXT', value: 'FULLTEXT'},
            {label: 'GEOSPATIAL', value: 'GEOSPATIAL'}
        ]
    };

    indexSortDirection = {
        values: [
            {value: true, label: 'ASC'},
            {value: false, label: 'DESC'}
        ],
        default: true
    };

    normalize = omit(['__v', 'space']);

    addIndexField(fields: IndexField[]) {
        return fields[fields.push({_id: ObjectID.generate(), direction: true}) - 1];
    }

    addIndex(model: DomainModel) {
        if (!model)
            return;

        if (!model.indexes)
            model.indexes = [];

        model.indexes.push({
            _id: ObjectID.generate(),
            name: '',
            indexType: 'SORTED',
            fields: []
        });

        return model.indexes[model.indexes.length - 1];
    }

    hasIndex(model: DomainModel) {
        return model.queryMetadata === 'Configuration'
            ? !!(model.keyFields && model.keyFields.length)
            : (!model.generatePojo || !model.databaseSchema && !model.databaseTable);
    }

    toShortModel(model: DomainModel): ShortDomainModel {
        return {
            _id: model._id,
            keyType: model.keyType,
            valueType: model.valueType,
            hasIndex: this.hasIndex(model)
        };
    }

    queryIndexes = {
        /**
         * Validates query indexes for completeness
         */
        complete: ($value: Index[] = []) => $value.every((index) => (
            index.name && index.indexType &&
            index.fields && index.fields.length && index.fields.every((field) => !!field.name))
        ),
        /**
         * Checks if field names used in indexes exist
         */
        fieldsExist: ($value: Index[] = [], fields: Field[] = []) => {
            const names = new Set(fields.map((field) => field.name));
            return $value.every((index) => index.fields && index.fields.every((field) => names.has(field.name)));
        },
        /**
         * Check if fields of query indexes have unique names
         */
        indexFieldsHaveUniqueNames: ($value: Index[] = []) => {
            return $value.every((index) => {
                if (!index.fields)
                    return true;

                const uniqueNames = new Set(index.fields.map((ec) => ec.name));
                return uniqueNames.size === index.fields.length;
            });
        }
    };

    /**
     * Removes instances of removed fields from queryKeyFields and index fields
     */
    removeInvalidFields(model: DomainModel): DomainModel {
        if (!model)
            return model;

        const fieldNames = new Set((model.fields || []).map((f) => f.name));
        return {
            ...model,
            queryKeyFields: (model.queryKeyFields || []).filter((queryKeyField) => fieldNames.has(queryKeyField)),
            indexes: (model.indexes || []).map((index) => ({
                ...index,
                fields: (index.fields || []).filter((indexField) => fieldNames.has(indexField.name))
            }))
        };
    }

    /**
     * Checks that collection of DB fields has unique DB and Java field names
     */
    storeKeyDBFieldsUnique(DBFields: (KeyField|ValueField)[] = []) {
        return ['databaseFieldName', 'javaFieldName'].every((key) => {
            const items = new Set(DBFields.map((field) => field[key]));
            return items.size === DBFields.length;
        });
    }
}

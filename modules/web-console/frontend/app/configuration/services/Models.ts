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

    inlineSizeTypes = [
        {label: 'Auto', value: -1},
        {label: 'Custom', value: 1},
        {label: 'Disabled', value: 0}
    ];

    inlineSizeType = {
        _val(queryIndex) {
            return (queryIndex.inlineSizeType === null || queryIndex.inlineSizeType === void 0) ? -1 : queryIndex.inlineSizeType;
        },
        onChange: (queryIndex) => {
            const inlineSizeType = this.inlineSizeType._val(queryIndex);
            switch (inlineSizeType) {
                case 1:
                    return queryIndex.inlineSize = queryIndex.inlineSize > 0 ? queryIndex.inlineSize : null;
                case 0:
                case -1:
                    return queryIndex.inlineSize = queryIndex.inlineSizeType;
                default: break;
            }
        },
        default: 'Auto'
    };

    fieldProperties = {
        typesWithPrecision: ['BigDecimal', 'String', 'byte[]'],
        fieldPresentation: (entity, available) => {
            if (!entity)
                return '';

            const precision = available('2.7.0') && this.fieldProperties.precisionAvailable(entity);
            const scale = available('2.7.0') && this.fieldProperties.scaleAvailable(entity);

            return `${entity.name || ''} ${entity.className || ''}${precision && entity.precision ? ' (' + entity.precision : ''}\
${scale && entity.precision && entity.scale ? ',' + entity.scale : ''}${precision && entity.precision ? ')' : ''}\
${available('2.3.0') && entity.notNull ? ' Not NULL' : ''}${available('2.4.0') && entity.defaultValue ? ' DEFAULT ' + entity.defaultValue : ''}`;
        },
        precisionAvailable: (entity) => entity && this.fieldProperties.typesWithPrecision.includes(entity.className),
        scaleAvailable: (entity) => entity && entity.className === 'BigDecimal'
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

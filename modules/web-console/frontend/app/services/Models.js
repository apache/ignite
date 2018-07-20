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

export default class Models {
    static $inject = ['$http'];

    /**
     * @param {ng.IHttpService} $http
     */
    constructor($http) {
        this.$http = $http;
    }

    /**
     * @param {string} modelID
     * @returns {ng.IPromise<ng.IHttpResponse<{data: ig.config.model.DomainModel}>>}
     */
    getModel(modelID) {
        return this.$http.get(`/api/v1/configuration/domains/${modelID}`);
    }

    /**
     * @returns {ig.config.model.DomainModel}
     */
    getBlankModel() {
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

    /**
     * @param {Array<ig.config.model.IndexField>} fields
     */
    addIndexField(fields) {
        return fields[fields.push({_id: ObjectID.generate(), direction: true}) - 1];
    }

    /**
     * @param {ig.config.model.DomainModel} model
     */
    addIndex(model) {
        if (!model) return;
        if (!model.indexes) model.indexes = [];
        model.indexes.push({
            _id: ObjectID.generate(),
            name: '',
            indexType: 'SORTED',
            fields: []
        });
        return model.indexes[model.indexes.length - 1];
    }

    /**
     * @param {ig.config.model.DomainModel} model
     */
    hasIndex(model) {
        return model.queryMetadata === 'Configuration'
            ? !!(model.keyFields && model.keyFields.length)
            : (!model.generatePojo || !model.databaseSchema && !model.databaseTable);
    }

    /**
     * @param {ig.config.model.DomainModel} model
     * @returns {ig.config.model.ShortDomainModel}
     */
    toShortModel(model) {
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
         * @param {Array<ig.config.model.Index>} $value
         */
        complete: ($value = []) => $value.every((index) => (
            index.name && index.indexType &&
            index.fields && index.fields.length && index.fields.every((field) => !!field.name))
        ),
        /**
         * Checks if field names used in indexes exist
         * @param {Array<ig.config.model.Index>} $value
         * @param {Array<ig.config.model.Field>} fields
         */
        fieldsExist: ($value = [], fields = []) => {
            const names = new Set(fields.map((field) => field.name));
            return $value.every((index) => index.fields && index.fields.every((field) => names.has(field.name)));
        },
        /**
         * Check if fields of query indexes have unique names
         * @param {Array<ig.config.model.Index>} $value
         */
        indexFieldsHaveUniqueNames: ($value = []) => {
            return $value.every((index) => {
                if (!index.fields) return true;
                const uniqueNames = new Set(index.fields.map((ec) => ec.name));
                return uniqueNames.size === index.fields.length;
            });
        }
    };

    /**
     * Removes instances of removed fields from queryKeyFields and index fields
     * 
     * @param {ig.config.model.DomainModel} model
     * @returns {ig.config.model.DomainModel}
     */
    removeInvalidFields(model) {
        if (!model) return model;
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
     * @param {Array<ig.config.model.KeyField|ig.config.model.ValueField>} DBFields
     */
    storeKeyDBFieldsUnique(DBFields = []) {
        return ['databaseFieldName', 'javaFieldName'].every((key) => {
            const items = new Set(DBFields.map((field) => field[key]));
            return items.size === DBFields.length;
        });
    }
}

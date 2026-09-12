import omit from 'lodash/fp/omit';
import uuidv4 from 'uuid/v4';

import {DomainModel, IndexField, ShortDomainModel, Index, Field, KeyField, ValueField, InlineSizeType} from '../types';

export default class Models {
    static $inject = ['$http'];

    constructor(private $http: ng.IHttpService) {}

    getModel(modelID: string) {
        return this.$http.get<{data: DomainModel[]}>(`/api/v1/configuration/domains/${modelID}`);
    }

    getBlankModel(): DomainModel {
        return {
            id: uuidv4(),
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
        _val(queryIndex: Index): InlineSizeType {
            return (queryIndex.inlineSizeType === null || queryIndex.inlineSizeType === void 0) ? -1 : queryIndex.inlineSizeType;
        },
        onChange: (queryIndex?: Index): void => {
            // Undefined queryIndex did not happen until native selects were introduced
            if (!queryIndex) return;
            const inlineSizeType = this.inlineSizeType._val(queryIndex);
            switch (inlineSizeType) {
                case 1:
                    queryIndex.inlineSize = queryIndex.inlineSize > 0 ? queryIndex.inlineSize : null;
                    break;
                case 0:
                case -1:
                    queryIndex.inlineSize = queryIndex.inlineSizeType;
                    break;
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

            const precision = this.fieldProperties.precisionAvailable(entity);
            const scale = this.fieldProperties.scaleAvailable(entity);

            return `${entity.name || ''} ${entity.className || ''}${precision && entity.precision ? ' (' + entity.precision : ''}\
               ${scale && entity.precision && entity.scale ? ',' + entity.scale : ''}${precision && entity.precision ? ')' : ''}\
               ${entity.notNull ? ' Not NULL' : ''} ${entity.defaultValue ? ' DEFAULT ' + entity.defaultValue : ''}`;
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
        return fields[fields.push({id: uuidv4(), direction: true}) - 1];
    }

    addIndex(model: DomainModel) {
        if (!model)
            return;

        if (!model.indexes)
            model.indexes = [];

        model.indexes.push({
            id: uuidv4(),
            name: '',
            indexType: 'SORTED',
            fields: [],
            inlineSize: null,
            inlineSizeType: -1
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
            id: model.id,
            keyType: model.keyType,
            valueType: model.valueType,
            tableComment: model.tableComment,
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

import templateUrl from './importer_from_template.tpl.pug';
import './style.scss';

import _ from 'lodash';
import find from 'lodash/fp/find';
import get from 'lodash/fp/get';
import naturalCompare from 'natural-compare-lite';
import {Subject,combineLatest, EMPTY, from, merge, of, race, timer} from 'rxjs';
import {distinctUntilChanged, exhaustMap, filter, map, pluck, switchMap, take, tap, catchError} from 'rxjs/operators';
import {uniqueName} from 'app/utils/uniqueName';
import {defaultNames} from '../../defaultNames';
import {DemoService} from 'app/modules/demo/Demo.module';
import uuidv4 from 'uuid/v4';

// eslint-disable-next-line
import {UIRouter} from '@uirouter/angularjs'
import {default as IgniteConfirmBatch} from 'app/services/ConfirmBatch.service';
import {Confirm as IgniteConfirm} from 'app/services/Confirm.service';
import {default as IgniteFocus} from 'app/services/Focus.service';
import IgniteMessages from 'app/services/Messages.service';
import IgniteLoading from 'app/modules/loading/loading.service';
import {default as ConfigSelectors} from '../../store/selectors';
import {default as ConfigEffects} from '../../store/effects';
import {default as ConfigureState} from '../../services/ConfigureState';
// eslint-disable-next-line
import {default as AgentManager} from 'app/modules/agent/AgentManager.service'
import {default as SqlTypes} from 'app/services/SqlTypes.service';
import {default as JavaTypes} from 'app/services/JavaTypes.service';
import IgniteLegacyUtils from 'app/services/LegacyUtils.service';
import Version from 'app/services/Version.service';
import IgniteFormUtils from 'app/services/FormUtils.service';

// eslint-disable-next-line
import {default as ActivitiesData} from 'app/core/activities/Activities.data';
import {UserService} from 'app/modules/user/User.service';
import Datasource from 'app/datasource/services/Datasource';

function _mapCaches(caches = []) {
    return caches.map((cache) => {
        return {label: cache.name, value: cache.id, cache};
    });
}

const INFO_CONNECT_TO_DB = 'Configure connection to database';
const INFO_SELECT_SCHEMAS = 'Select schemas to load tables from';
const INFO_SELECT_TABLES = 'Select tables to import as domain model';
const INFO_SELECT_OPTIONS = 'Select import domain model options';
const LOADING_DATA_SOURCES = {text: 'Loading data sources...'};
const LOADING_SCHEMAS = {text: 'Loading schemas...'};
const LOADING_TABLES = {text: 'Loading tables...'};
const SAVING_DOMAINS = {text: 'Saving domain model...'};

const IMPORT_DM_NEW_CACHE = 1;
const IMPORT_DM_ASSOCIATE_CACHE = 2;

const DFLT_PARTITIONED_CACHE = {
    label: 'PARTITIONED',
    value: -1,
    cache: {
        name: 'PARTITIONED',
        cacheMode: 'PARTITIONED',
        atomicityMode: 'ATOMIC',
        readThrough: false,
        writeThrough: false
    }
};

const DFLT_REPLICATED_CACHE = {
    label: 'REPLICATED',
    value: -2,
    cache: {
        name: 'REPLICATED',
        cacheMode: 'REPLICATED',
        atomicityMode: 'ATOMIC',
        readThrough: false,
        writeThrough: false
    }
};

const CACHE_TEMPLATES = [DFLT_PARTITIONED_CACHE, DFLT_REPLICATED_CACHE];

export class ModalImportModelsFromTemplate {
    /**
     * Cluster ID to import models into
     * @type {string}
     */
    clusterID;

    /** @type {ng.ICompiledExpression} */
    onHide;

    loadedCaches: Map<string, any>;

    static $inject = ['$uiRouter','Datasource', 'ConfigSelectors', 'ConfigEffects', 'ConfigureState', 'IgniteConfirm', 'IgniteConfirmBatch', 'IgniteFocus', 'SqlTypes', 'JavaTypes', 'IgniteMessages', 
    '$scope', 'Demo', 'AgentManager', 'IgniteActivitiesData', 'IgniteLoading', 'IgniteFormUtils', 'IgniteLegacyUtils', 'IgniteVersion', 'User'];

    /**
     * @param {UIRouter} $uiRouter
     * @param {ConfigSelectors} ConfigSelectors
     * @param {ConfigEffects} ConfigEffects
     * @param {ConfigureState} ConfigureState
     * @param {IgniteConfirmBatch} ConfirmBatch
     * @param {SqlTypes} SqlTypes
     * @param {JavaTypes} JavaTypes
     * @param {ng.IScope} $scope
     * @param {AgentManager} agentMgr
     * @param {ActivitiesData} ActivitiesData
     */
    constructor(
        private $uiRouter:UIRouter, 
        private Datasource:Datasource, 
        private ConfigSelectors:ConfigSelectors, 
        private ConfigEffects:ConfigEffects, 
        private ConfigureState:ConfigureState, 
        private Confirm:IgniteConfirm, 
        private ConfirmBatch:IgniteConfirmBatch, 
        private Focus:ReturnType<typeof IgniteFocus>,
        private SqlTypes:SqlTypes, 
        private JavaTypes:JavaTypes, 
        private Messages:ReturnType<typeof IgniteMessages>, 
        private $scope:ng.IScope, 
        private Demo: DemoService, 
        private agentMgr:AgentManager, 
        private ActivitiesData:ActivitiesData, 
        private Loading:ReturnType<typeof IgniteLoading>, 
        private FormUtils:ReturnType<typeof IgniteFormUtils>, 
        private LegacyUtils:ReturnType<typeof IgniteLegacyUtils>, 
        private IgniteVersion:Version, 
        private User: UserService) {        
        
    }

    loadData() {
        return of(this.clusterID).pipe(
            switchMap((id = 'new') => {
                return this.ConfigureState.state$.pipe(this.ConfigSelectors.selectClusterToEdit(id, defaultNames.importedCluster));
            }),
            switchMap((cluster) => {
                return (!(cluster.caches || []).length && !(cluster.models || []).length)
                    ? of({
                        cluster,
                        caches: [],
                        models: []
                    })
                    : from(Promise.all([
                        this.ConfigEffects.etp('LOAD_SHORT_CACHES', {ids: cluster.caches || [], clusterID: cluster.id}),
                        this.ConfigEffects.etp('LOAD_SHORT_MODELS', {ids: cluster.models || [], clusterID: cluster.id})
                    ])).pipe(switchMap(() => {
                        return combineLatest(
                            this.ConfigureState.state$.pipe(this.ConfigSelectors.selectShortCachesValue()),
                            this.ConfigureState.state$.pipe(this.ConfigSelectors.selectShortModelsValue()),
                            (caches, models) => ({
                                cluster,
                                caches,
                                models
                            })
                        ).pipe(take(1));
                    }));
            }),
            take(1)
        );
    }

    saveBatch(batch) {
        if (!batch.length)
            return;

        this.$scope.importDomain.loadingOptions = SAVING_DOMAINS;
        this.Loading.start('importDomainFromTemplate');

        this.ConfigureState.dispatchAction({
            type: 'ADVANCED_SAVE_COMPLETE_CONFIGURATION',
            changedItems: this.batchActionsToRequestBody(batch),
            prevActions: []
        });

        this.saveSubscription = race(
            this.ConfigureState.actions$.pipe(
                filter((a) => a.type === 'ADVANCED_SAVE_COMPLETE_CONFIGURATION_OK'),
                tap(() => this.onHide())
            ),
            this.ConfigureState.actions$.pipe(
                filter((a) => a.type === 'ADVANCED_SAVE_COMPLETE_CONFIGURATION_ERR')
            )
        ).pipe(
            take(1),
            tap(() => {
                this.Loading.finish('importDomainFromTemplate');
            })
        )
        .subscribe();
    }

    batchActionsToRequestBody(batch) {
        const result = batch.reduce((req, action) => {
            return {
                ...req,
                cluster: {
                    ...req.cluster,
                    models: [...req.cluster.models, action.newDomainModel.id],
                    caches: [...req.cluster.caches, ...action.newDomainModel.caches]
                },
                models: [...req.models, action.newDomainModel],
                caches: action.newCache
                    ? [...req.caches, action.newCache]
                    : action.cacheStoreChanges
                        ? [...req.caches, {
                            ...this.loadedCaches[action.cacheStoreChanges[0].cacheId],
                            ...action.cacheStoreChanges[0].change
                        }]
                        : req.caches
            };
        }, {cluster: this.cluster, models: [], caches: []});
        result.cluster.models = [...new Set(result.cluster.models)];
        result.cluster.caches = [...new Set(result.cluster.caches)];
        return result;
    }

    onDatasourceSelectionChange(selected) {
        this.$scope.$applyAsync(() => {
            if(selected.length>0){
                this.$scope.selectedPreset = selected[0]                
            }         
           
            this.selectedDatasourcesIDs = selected.map((i) => i.id);
        });
    }

    onTableSelectionChange(selected) {
        this.$scope.$applyAsync(() => {
            this.$scope.importDomain.tablesToUse = selected;
            this.selectedTablesIDs = selected.map((t) => t.id);
        });
    }

    onMetaTableSelectionChange(selected) {
        this.$scope.$applyAsync(() => {
            if(this.$scope.importDomain.tablesFromMetaCollection){
                const tables = this.$scope.importDomain.tables;
                const meta_tables = tables.filter((t) => {
                    let hasmeta = 0;
                    t.columns.forEach((col) => {
                        if(["TABLE_NAME","COLUMN_NAME"].includes(col.name)){
                            hasmeta+=1;
                        }
                    });
                    return hasmeta >= 2;
                });

                this.$scope.importDomain.original_tables = tables;
                this.$scope.importDomain.tables = meta_tables;
                this.$scope.importDomain.tablesToUse = meta_tables;
                this.selectedTablesIDs = [];
                this.$scope.importDomain.action = 'meta_tables';
            }
            else{
                this.$scope.importDomain.tables = this.$scope.importDomain.original_tables;
                let tablesToUse = this.$scope.importDomain.tables.filter((t) => this.LegacyUtils.isDefined(t.idIndex));                    
                this.selectedTablesIDs = tablesToUse.map((t) => t.name);
                this.$scope.importDomain.action = 'tables';
            }
            
        });
    }

    onSchemaSelectionChange(selected) {
        this.$scope.$applyAsync(() => {
            this.$scope.importDomain.schemasToUse = selected;
            this.selectedSchemasIDs = selected.map((i) => i.name);
        });
    }

    onVisibleRowsChange(rows) {
        return this.visibleTables = rows.map((r) => r.entity);
    }

    onCacheSelect(cacheID) {
        if (cacheID < 0)
            return;

        if (this.loadedCaches[cacheID])
            return;

        return this.onCacheSelectSubcription = merge(
            timer(0, 1).pipe(
                take(1),
                tap(() => this.ConfigureState.dispatchAction({type: 'LOAD_CACHE', cacheID}))
            ),
            race(
                this.ConfigureState.actions$.pipe(
                    filter((a) => a.type === 'LOAD_CACHE_OK' && a.cache.id === cacheID),
                    pluck('cache'),
                    tap((cache) => {
                        this.loadedCaches[cacheID] = cache;
                    })
                ),
                this.ConfigureState.actions$.pipe(
                    filter((a) => a.type === 'LOAD_CACHE_ERR' && a.action.cacheID === cacheID)
                )
            ).pipe(take(1))
        )
        .subscribe();
    }

    $onDestroy() {
        this.subscribers$.unsubscribe();        
        if (this.onCacheSelectSubcription) this.onCacheSelectSubcription.unsubscribe();
        if (this.saveSubscription) this.saveSubscription.unsubscribe();
        
    }

    async $onInit() {
        // Restores old behavior
        const {Confirm, Datasource, ConfirmBatch, Focus, SqlTypes, JavaTypes, Messages, $scope, Demo, agentMgr, ActivitiesData, Loading, FormUtils, LegacyUtils} = this;

        /**
         * Convert some name to valid java package name.
         *
         * @param name to convert.
         * @returns {string} Valid java package name.
         */
        const _toJavaPackage = (name) => {
            return name ? name.replace(/[^A-Za-z_0-9/.]+/g, '_') : 'org';
        };

        const _makeDefaultPackageName = (user) => user
            ? _toJavaPackage(`${user.email.replace('@', '.').split('.').reverse().join('.')}.model`)
            : void 0;

        this.$scope.ui = {
            generatePojo: true,
            builtinKeys: true,
            generateKeyFields: true,
            usePrimitives: true,
            generateTypeAliases: true,
            generateFieldAliases: true,
            packageNameUserInput: _makeDefaultPackageName(await this.User.current$.pipe(take(1)).toPromise())
        };

        this.$scope.$hide = this.onHide;

        this.$scope.importCommon = {};

        this.subscription = this.loadData().pipe(tap((data) => {
            this.$scope.caches = _mapCaches(data.caches);
            this.$scope.domains = data.models;
            this.caches = data.caches;
            this.cluster = data.cluster;

            if (!_.isEmpty(this.$scope.caches)) {
                this.$scope.importActions.push({
                    label: 'Associate with existing cache',
                    shortLabel: 'Associate',
                    value: IMPORT_DM_ASSOCIATE_CACHE
                });
            }
            this.$scope.$watch('importCommon.action', this._fillCommonCachesOrTemplates(this.$scope.importCommon), true);
            this.$scope.importCommon.action = IMPORT_DM_NEW_CACHE;
        }));

        // New
        this.loadedCaches = {
            ...CACHE_TEMPLATES.reduce((a, c) => ({...a, [c.value]: c.cache}), {})
        };

        this.actions = [
            {value: 'connect', label: this.Demo.enabled ? 'Description' : 'Connection'},
            {value: 'schemas', label: 'Schemas'},
            {value: 'tables', label: 'Tables'},
            {value: 'options', label: 'Options'}
        ];

        // Legacy
        $scope.ui.invalidKeyFieldsTooltip = 'Found key types without configured key fields<br/>' +
            'It may be a result of import tables from database without primary keys<br/>' +
            'Key field for such key types should be configured manually';

        $scope.indexType = LegacyUtils.mkOptions(['SORTED', 'FULLTEXT', 'VECTORTEXT', 'GEOSPATIAL']);

        $scope.importActions = [{
            label: 'Create new cache by template',
            shortLabel: 'Create',
            value: IMPORT_DM_NEW_CACHE
        }];
        
        this.dataSourceList$ = from(Datasource.getDatasourceList()).pipe(            
            switchMap(({data}) => of(
                data
            )),            
            catchError((error) => of({
                type: `DATASOURCE_ERR`,
                error: {
                    message: `Failed to load datasoure:  ${error.data.message}`
                },
                action: {}
            }))           
        ).subscribe((data)=> {
            $scope.dataSourceList = data //.map((data)=> { return { id:data.id, jndiName:data.jndiName, jdbcUrl:data.jdbcUrl, schemaName:data.schemaName }; })
        }); 
        
        this.selectedDatasourcesIDs = [];

        $scope.selectedPreset = {
            db: 'MaongoDB',
            jdbcDriverJar: '',
            jdbcDriverClass: '',
            jdbcUrl: null,
            jndiName: null,
            user: 'sa',
            password: '',
            tablesOnly: true,
            importSamples: false
        };

        $scope.demoConnection = {
            db: 'MaongoDB',
            jdbcDriverClass: '',
            jdbcUrl: 'mongodb://localhost:2701/demo-db',
            user: 'sa',
            password: '',
            tablesOnly: true,
            importSamples: false
        };
        

        /**
         * Load list of database schemas.
         */
        const _loadSchemas = () => {
            this.Datasource.getDatabaseList($scope.selectedPreset.id).then(({data})=> {
                $scope.importDomain.loadingOptions = LOADING_SCHEMAS;
                Loading.start('importDomainFromTemplate');
                const schemaInfo = data;
                const catelog = $scope.selectedPreset.jndiName
                if(schemaInfo) {
                    $scope.importDomain.action = 'schemas';
                    $scope.importDomain.info = INFO_SELECT_SCHEMAS;
                    $scope.importDomain.catalog = JavaTypes.toJavaIdentifier(catelog);
                    $scope.importDomain.schemas = schemaInfo;
                    $scope.importDomain.schemasToUse = $scope.importDomain.schemas;
                    this.selectedSchemasIDs = $scope.importDomain.schemas.map((s) => s.name);

                    if ($scope.importDomain.schemas.length === 0)
                        $scope.importDomainNext();
                }
                
            })
            .catch(Messages.showError)
            .then(() => Loading.finish('importDomainFromTemplate'));
        }


        this._importCachesOrTemplates = [];

        $scope.tableActionView = (tbl) => {
            const cacheName = get('label')(find({value: tbl.cacheOrTemplate}));

            if (tbl.action === IMPORT_DM_NEW_CACHE)
                return 'Create ' + tbl.generatedCacheName + ' (' + cacheName + ')';

            return 'Associate with ' + cacheName;
        };

        /**
         * Load list of database tables.
         */
        const _loadTables = () => {
            $scope.importDomain.loadingOptions = LOADING_TABLES;
            Loading.start('importDomainFromTemplate');

            $scope.importDomain.allTablesSelected = false;
            $scope.importDomain.tablesFromMetaCollection = false;
            $scope.importDomain.tablesFromMetaCollectionHide = false;
            this.selectedTables = [];

            const preset = $scope.importDomain.demo ? $scope.demoConnection : $scope.selectedPreset;

            preset.schemas = $scope.importDomain.schemasToUse.map((s) => s.name);
            
            const schema = preset.schemas[0];
            this.Datasource.getCollectionList($scope.selectedPreset.id,schema)
                .then(({data}) => {
                    this._importCachesOrTemplates = CACHE_TEMPLATES.concat($scope.caches);

                    this._fillCommonCachesOrTemplates($scope.importCommon)($scope.importCommon.action);

                    const tables: Array<any> = data;
                    _.forEach(tables, (tbl, idx) => {
                        tbl.id = idx;
                        tbl.action = IMPORT_DM_NEW_CACHE;
                        tbl.table = tbl.name;
                        tbl.schema = schema;              
                        tbl.generatedCacheName = uniqueName(SqlTypes.toJdbcIdentifier(tbl.name), this.caches);
                        tbl.cacheOrTemplate = DFLT_PARTITIONED_CACHE.value;
                        tbl.label = tbl.schema + '.' + tbl.name;
                        tbl.edit = false;
                        
                        if (tbl.idIndex) {
                            tbl.keyType = 'String'
                            tbl.keyFields = ['_id'];
                        }
                    });

                    $scope.importDomain.action = 'tables';
                    $scope.importDomain.tables = tables;
                    let tablesToUse = tables.filter((t) => LegacyUtils.isDefined(t.idIndex));
                    
                    this.selectedTablesIDs = tablesToUse.map((t) => t.name);
                    this.$scope.importDomain.tablesToUse = tablesToUse;                    

                    $scope.importDomain.info = INFO_SELECT_TABLES;
                })
                .catch(Messages.showError)
                .then(() => Loading.finish('importDomainFromTemplate'));
        };        

        const _loadTablesFromMetaCollection = () => {
            $scope.importDomain.loadingOptions = LOADING_TABLES;
            Loading.start('importDomainFromTemplate');

            $scope.importDomain.allTablesSelected = false;
            $scope.importDomain.tablesFromMetaCollection = false;
            $scope.importDomain.tablesFromMetaCollectionHide = true;
            this.selectedTables = [];

            const preset = $scope.importDomain.demo ? $scope.demoConnection : $scope.selectedPreset;

            preset.schemas = $scope.importDomain.schemasToUse.map((s) => s.name);
            const selectedTables = this.$scope.importDomain.tablesToUse.map((s) => s.name);
            const schema = preset.schemas[0];
            this.Datasource.getCollectionListFromMetaCollection($scope.selectedPreset.id,schema,selectedTables[0])
                .then(({data}) => {
                    this._importCachesOrTemplates = CACHE_TEMPLATES.concat($scope.caches);

                    this._fillCommonCachesOrTemplates($scope.importCommon)($scope.importCommon.action);

                    const tables: Array<any> = data;
                    _.forEach(tables, (tbl, idx) => {
                        tbl.id = idx;
                        tbl.action = IMPORT_DM_NEW_CACHE;
                        tbl.table = tbl.name;
                        tbl.schema = tbl.schema || schema;                                
                        tbl.generatedCacheName = uniqueName(SqlTypes.toJdbcIdentifier(tbl.name), this.caches);
                        tbl.cacheOrTemplate = DFLT_PARTITIONED_CACHE.value;
                        tbl.label = tbl.schema + '.' + tbl.name;
                        tbl.edit = false;
                        
                        if (tbl.idIndex) {
                            tbl.keyType = 'String'
                            tbl.keyFields = ['_id'];
                        }
                    });

                    $scope.importDomain.action = 'tables';
                    $scope.importDomain.tables = tables;
                    let tablesToUse = tables; //.filter((t) => LegacyUtils.isDefined(t.idIndex));
                    
                    this.selectedTablesIDs = tablesToUse.map((t) => t.name);
                    this.$scope.importDomain.tablesToUse = tablesToUse;                    

                    $scope.importDomain.info = INFO_SELECT_TABLES;
                })
                .catch(Messages.showError)
                .then(() => Loading.finish('importDomainFromTemplate'));
        };

        $scope.applyDefaults = () => {
            _.forEach(this.visibleTables, (table) => {
                table.edit = false;
                table.action = $scope.importCommon.action;
                table.cacheOrTemplate = $scope.importCommon.cacheOrTemplate;
            });
        };

        $scope._curDbTable = null;

        $scope.startEditDbTableCache = (tbl) => {
            if ($scope._curDbTable) {
                $scope._curDbTable.edit = false;

                if ($scope._curDbTable.actionWatch) {
                    $scope._curDbTable.actionWatch();
                    $scope._curDbTable.actionWatch = null;
                }
            }

            $scope._curDbTable = tbl;

            const _fillFn = this._fillCommonCachesOrTemplates($scope._curDbTable);

            _fillFn($scope._curDbTable.action);

            $scope._curDbTable.actionWatch = $scope.$watch('_curDbTable.action', _fillFn, true);

            $scope._curDbTable.edit = true;
        };

        /**
         * Show page with import domain models options.
         */
        function _selectOptions() {
            $scope.importDomain.action = 'options';
            $scope.importDomain.button = 'Save';
            $scope.importDomain.info = INFO_SELECT_OPTIONS;

            Focus.move('domainPackageName');
        }

        const _saveDomainModel = (optionsForm) => {
            if (optionsForm.$invalid)
                return this.FormUtils.triggerValidation(optionsForm, this.$scope);

            const generatePojo = $scope.ui.generatePojo;
            const packageName = $scope.ui.packageName;

            const batch = [];
            const checkedCaches = [];

            let containKey = true;
            let containDup = false;

            function dbField(name, jdbcType, nullable, unsigned) {
                const javaTypes = (unsigned && jdbcType.unsigned) ? jdbcType.unsigned : jdbcType.signed;
                const javaFieldType = (!nullable && javaTypes.primitiveType && $scope.ui.usePrimitives) ? javaTypes.primitiveType : javaTypes.javaType;

                return {
                    databaseFieldName: name,
                    databaseFieldType: jdbcType.dbType==1111? 'OTHER' : jdbcType.dbName,
                    javaType: javaTypes.javaType,
                    javaFieldName: generatePojo ? JavaTypes.toJavaFieldName(name) : name,
                    javaFieldType
                };
            }

            _.forEach($scope.importDomain.tablesToUse, (table, curIx, tablesToUse) => {
                const qryFields = [];
                const indexes = [];
                const keyFields = [];
                const valFields = [];
                const aliases = [];
                
                const tableName = table.table;
                // this is cacheName
                let typeName = generatePojo? JavaTypes.toJavaClassName(tableName) : tableName;

                if (_.find($scope.importDomain.tablesToUse,
                        (tbl, ix) => ix !== curIx && tableName === tbl.table)) {
                    typeName = typeName + '_' + JavaTypes.toJavaClassName(table.schema);

                    containDup = true;
                }

                let valType = tableName;
                let typeAlias;

                if (generatePojo) {
                    if ($scope.ui.generateTypeAliases && tableName.toLowerCase() !== typeName.toLowerCase())
                        typeAlias = tableName;

                    valType = _toJavaPackage(packageName) + '.' + typeName;
                }

                let _containKey = false;

                _.forEach(table.columns, function(col) {
                    col.typeName = SqlTypes.findJdbcTypeName(col.typeName);
                    const fld = dbField(col.name, SqlTypes.findJdbcType(col.type,col.typeName), col.nullable, col.unsigned);

                    qryFields.push({name: fld.javaFieldName, className: fld.javaType, comment: col.comment});

                    const dbName = fld.databaseFieldName;

                    if (generatePojo && $scope.ui.generateFieldAliases &&
                        SqlTypes.isValidSqlIdentifier(dbName) &&
                        !_.find(aliases, {field: fld.javaFieldName}) &&
                        fld.javaFieldName.toUpperCase() !== dbName.toUpperCase()){
                            
                        aliases.push({field: fld.javaFieldName, alias: dbName});
                    }                        

                    if (col.key || col.name==='_id' || col.name==='_key') {
                        keyFields.push(fld);

                        _containKey = true;
                    }
                    else
                        valFields.push(fld);
                });

                containKey &= _containKey;
                if (table.indexes) {
                    _.forEach(table.indexes, (idx) => {
                        const idxFields = _.map(idx.fields, (idxFld) => ({
                            name: generatePojo?JavaTypes.toJavaFieldName(idxFld.name):idxFld.name,
                            direction: idxFld.sortOrder
                        }));

                        indexes.push({
                            name: idx.name,
                            indexType: 'SORTED',
                            fields: idxFields
                        });
                    });
                }

                if(!containKey){
                    const fld = dbField("_id", SqlTypes.findJdbcType(1111,"UUID"), false, false);
                    qryFields.push({name: fld.javaFieldName, className: fld.javaType, comment: ''});
                    keyFields.push(fld); 
                }

                const domainFound = _.find($scope.domains, (domain) => domain.valueType === valType);

                const batchAction = {
                    confirm: false,
                    skip: false,
                    table,
                    newDomainModel: {
                        id: uuidv4(),
                        caches: [],
                        generatePojo
                    }
                };

                if (LegacyUtils.isDefined(domainFound)) {
                    batchAction.newDomainModel.id = domainFound.id;
                    // Don't touch original caches value
                    delete batchAction.newDomainModel.caches;
                    batchAction.confirm = true;
                }

                Object.assign(batchAction.newDomainModel, {
                    tableName: typeAlias,
                    tableComment: table.comment,
                    keyType: valType + (generatePojo? 'Key':'_key'),
                    valueType: valType,
                    queryMetadata: 'Configuration',
                    databaseSchema: table.schema,
                    databaseTable: tableName,
                    fields: qryFields,
                    queryKeyFields: _.map(keyFields, (field) => field.javaFieldName),
                    indexes,
                    keyFields,
                    aliases,
                    valueFields: _.isEmpty(valFields) ? keyFields.slice() : valFields
                });

                // Use Java built-in type for key.
                if ($scope.ui.builtinKeys && batchAction.newDomainModel.keyFields.length === 1) {
                    const newDomain = batchAction.newDomainModel;
                    const keyField = newDomain.keyFields[0];

                    newDomain.keyType = keyField.javaType;
                    newDomain.keyFieldName = keyField.javaFieldName;

                    if (!$scope.ui.generateKeyFields) {
                        // Exclude key column from query fields.
                        newDomain.fields = _.filter(newDomain.fields, (field) => field.name !== keyField.javaFieldName);

                        newDomain.queryKeyFields = [];
                    }

                    // Exclude key column from indexes.
                    _.forEach(newDomain.indexes, (index) => {
                        index.fields = _.filter(index.fields, (field) => field.name !== keyField.javaFieldName);
                    });

                    newDomain.indexes = _.filter(newDomain.indexes, (index) => !_.isEmpty(index.fields));
                }

                // Prepare caches for generation.
                if (table.action === IMPORT_DM_NEW_CACHE) {
                    const newCache = _.cloneDeep(this.loadedCaches[table.cacheOrTemplate]);

                    batchAction.newCache = newCache;                    
                    newCache.id = uuidv4();
                    newCache.name = table.generatedCacheName;
                    newCache.domains = [batchAction.newDomainModel.id];
                    // add@byron
                    newCache.sqlSchema = table.schema;
                    batchAction.newDomainModel.caches = [newCache.id];

                    // POJO store factory is not defined in template.
                    if (!newCache.cacheStoreFactory || newCache.cacheStoreFactory.kind !== 'CacheJdbcPojoStoreFactory') {
                        const dialect = $scope.selectedPreset.db;

                        const catalog = $scope.importDomain.catalog;

                        const dsFactoryBean = {
                            dataSourceBean: $scope.selectedPreset.jndiName,
                            dialect,
                            implementationVersion: $scope.selectedPreset.jdbcDriverImplementationVersion
                        };

                        const mongoFactoryBean = {
                            dataSrc: $scope.selectedPreset.jdbcUrl,
                            idField: '_id',                            
                        };

                        newCache.cacheStoreFactory = {
                            kind: dialect === 'MongoDB' ? 'DocumentLoadOnlyStoreFactory' : 'CacheJdbcPojoStoreFactory',
                            DocumentLoadOnlyStoreFactory: mongoFactoryBean,
                            CacheJdbcPojoStoreFactory: dsFactoryBean,
                            CacheJdbcBlobStoreFactory: { connectVia: 'DataSource' }
                        };
                    }

                    /** disable@byron
                    if (!newCache.readThrough && !newCache.writeThrough) {
                        newCache.readThrough = true;
                        newCache.writeThrough = true;
                    }
                    */
                    newCache.readThrough = false;
                    newCache.writeThrough = false;
                }
                else {
                    const newDomain = batchAction.newDomainModel;
                    const cacheId = table.cacheOrTemplate;

                    batchAction.newDomainModel.caches = [cacheId];

                    if (!_.includes(checkedCaches, cacheId)) {
                        const cache = _.find($scope.caches, {value: cacheId}).cache;

                        // TODO: move elsewhere, make sure it still works
                        const change = LegacyUtils.autoCacheStoreConfiguration(cache, [newDomain], $scope.selectedPreset.jndiName,$scope.selectedPreset.db);

                        if (change)
                            batchAction.cacheStoreChanges = [{cacheId, change}];

                        checkedCaches.push(cacheId);
                    }
                }

                batch.push(batchAction);
            });

            /**
             * Generate message to show on confirm dialog.
             *
             * @param meta Object to confirm.
             * @returns {string} Generated message.
             */
            function overwriteMessage(meta) {
                return `
                    Domain model with name &quot;${meta.newDomainModel.databaseTable}&quot; already exists.
                    Are you sure you want to overwrite it?
                `;
            }

            const itemsToConfirm = _.filter(batch, (item) => item.confirm);

            const checkOverwrite = () => {
                if (itemsToConfirm.length > 0) {
                    return ConfirmBatch.confirm(overwriteMessage, itemsToConfirm)
                        .then(() => this.saveBatch(_.filter(batch, (item) => !item.skip)))
                        .catch(() => Messages.showError('Importing of domain models interrupted by user.'));
                }
                return this.saveBatch(batch);
            };

            const checkDuplicate = () => {
                if (containDup) {
                    Confirm.confirm('Some tables have the same name.<br/>' +
                        'Name of types for that tables will contain schema name too.')
                        .then(() => checkOverwrite());
                }
                else
                    checkOverwrite();
            };

            if (containKey)
                checkDuplicate();
            else {
                Confirm.confirm('Some tables have no primary key.<br/>' +
                    'You will need to configure key type and key fields for such tables after import complete.')
                    .then(() => checkDuplicate());
            }
        };


        $scope.importDomainNext = (form) => {
            if (!$scope.importDomainNextAvailable())
                return;

            const act = $scope.importDomain.action;
            
            if (act === 'connect')
                _loadSchemas();
            else if (act === 'schemas')
                _loadTables();
            else if (act === 'meta_tables')
                _loadTablesFromMetaCollection();
            else if (act === 'tables')
                _selectOptions();
            else if (act === 'options')
                _saveDomainModel(form);
        };

        $scope.nextTooltipText = function() {
            const importDomainNextAvailable = $scope.importDomainNextAvailable();

            const act = $scope.importDomain.action;


            if (act === 'connect' && _.isNil($scope.selectedPreset.jdbcUrl))
                return 'Input valid JDBC URL';

            if (act === 'connect' || act === 'drivers')
                return 'Click to load list of schemas from database';

            if (act === 'schemas')
                return importDomainNextAvailable ? 'Click to load list of tables from database' : 'Select schemas to continue';

            if (act === 'meta_tables')
                return importDomainNextAvailable ? 'Click to show import tables' : 'Select one meta table to continue';

            if (act === 'tables')
                return importDomainNextAvailable ? 'Click to show import options' : 'Select tables to continue';

            if (act === 'options')
                return 'Click to import domain model for selected tables';

            return 'Click to continue';
        };

        $scope.prevTooltipText = function() {
            const act = $scope.importDomain.action;

            if (act === 'schemas')
                return $scope.importDomain.demo ? 'Click to return on demo description step' : 'Click to return on connection configuration step';

            if (act === 'meta_tables')
                return 'Click to return on meta table selection step';

            if (act === 'tables')
                return 'Click to return on schemas selection step';

            if (act === 'options')
                return 'Click to return on tables selection step';
        };

        $scope.importDomainNextAvailable = function() {
            switch ($scope.importDomain.action) {
                case 'connect':
                    return !_.isNil($scope.selectedPreset.jdbcUrl);

                case 'schemas':
                    return _.isEmpty($scope.importDomain.schemas) || !!get('importDomain.schemasToUse.length')($scope);

                case 'tables':
                    return !_.isNil($scope.importDomain.tablesToUse) && !!$scope.importDomain.tablesToUse.length;

                case 'meta_tables':
                    return !_.isNil($scope.importDomain.tablesToUse) && !!$scope.importDomain.tablesToUse.length;

                default:
                    return true;
            }
        };

        $scope.importDomainPrev = function() {
            $scope.importDomain.button = 'Next';

            if ($scope.importDomain.action === 'options') {
                $scope.importDomain.action = 'tables';
                $scope.importDomain.info = INFO_SELECT_TABLES;
            }
            else if ($scope.importDomain.action === 'tables' && $scope.importDomain.schemas.length > 0) {
                $scope.importDomain.action = 'schemas';
                $scope.importDomain.info = INFO_SELECT_SCHEMAS;
            }
            else if ($scope.importDomain.action === 'meta_tables' && $scope.importDomain.schemas.length > 0) {
                $scope.importDomain.action = 'schemas';
                $scope.importDomain.info = INFO_SELECT_SCHEMAS;
            }
            else {
                $scope.importDomain.action = 'connect';
                $scope.importDomain.info = INFO_CONNECT_TO_DB;
            }
        };

        const demo = Demo.enabled;

        $scope.importDomain = {
            demo,
            action: 'connect',
            jdbcDriversNotFound: false,
            schemas: [],
            allSchemasSelected: false,
            tables: [],
            allTablesSelected: false,
            button: 'Next',
            info: ''
        };

        $scope.importDomain.loadingOptions = LOADING_DATA_SOURCES;

        const fetchDomainData = () => {
            if (demo) {
                $scope.ui.packageNameUserInput = $scope.ui.packageName;
                $scope.ui.packageName = 'model';

                return;
            }

            // Get available JDBC drivers via agent.
            Loading.start('importDomainFromTemplate');

            $scope.importDomain.action = 'connect';
            $scope.importDomain.tables = [];
            this.selectedTables = [];

            $scope.importDomain.info = INFO_CONNECT_TO_DB;

            Loading.finish('importDomainFromTemplate');
        };
        

        fetchDomainData();

        this.subscribers$ = this.subscription.subscribe();
        
    }

    _fillCommonCachesOrTemplates(item) {
        return (action) => {
            if (item.cachesOrTemplates)
                item.cachesOrTemplates.length = 0;
            else
                item.cachesOrTemplates = [];

            if (action === IMPORT_DM_NEW_CACHE)
                item.cachesOrTemplates.push(...CACHE_TEMPLATES);

            if (!_.isEmpty(this.$scope.caches)) {
                item.cachesOrTemplates.push(...this.$scope.caches);
                this.onCacheSelect(item.cachesOrTemplates[0].value);
            }

            if (
                !_.find(item.cachesOrTemplates, {value: item.cacheOrTemplate}) &&
                item.cachesOrTemplates.length
            )
                item.cacheOrTemplate = item.cachesOrTemplates[0].value;
        };
    }

    datasourcesColumnDefs = [
        {
            name: 'id',
            displayName: 'Id',
            field: 'id',
            enableHiding: true,            
            visible: false,
            enableFiltering: false,          
            minWidth: 40
        },
        {
            name: 'jndiName',
            displayName: 'JNDI Name',
            field: 'jndiName',
            enableHiding: false,
            sort: {direction: 'asc', priority: 0},            
            visible: true,
            sortingAlgorithm: naturalCompare,
            enableFiltering: false,
            minWidth: 100
        },
        {
            name: 'jdbcUrl',
            displayName: 'JDBC URL',
            field: 'jdbcUrl',
            enableHiding: false,            
            visible: true,
            enableFiltering: false,      
            minWidth: 400
        },
        {
            name: 'schemaName',
            displayName: 'Schema Name',
            field: 'schemaName',
            enableHiding: false,            
            visible: true,
            enableFiltering: false,         
            minWidth: 100
        }
    ];

    schemasColumnDefs = [
        {
            name: 'name',
            displayName: 'Name',
            field: 'name',
            enableHiding: false,
            sort: {direction: 'asc', priority: 0},
            filter: {
                placeholder: 'Filter by Name…'
            },
            visible: true,
            sortingAlgorithm: naturalCompare,
            minWidth: 165
        }
    ];

    tablesColumnDefs = [
        {
            name: 'schema',
            displayName: 'Schema',
            field: 'schema',
            enableHiding: false,
            enableFiltering: true,
            filter: {
                placeholder: 'Filter by schema…'
            },
            sort: {direction: 'asc', priority: 0},
            visible: true,
            sortingAlgorithm: naturalCompare,
            minWidth: 100
        },
        {
            name: 'table',
            displayName: 'Table',
            field: 'table',
            enableHiding: false,
            enableFiltering: true,
            filter: {
                placeholder: 'Filter by Table…'
            },
            visible: true,
            sortingAlgorithm: naturalCompare,
            minWidth: 200
        },
        {
            name: 'comment',
            displayName: 'Table comment',
            field: 'comment',
            enableHiding: true,
            enableFiltering: true,
            filter: {
                placeholder: 'Filter by Table…'
            },
            visible: true,            
            minWidth: 250
        },
        {
            name: 'action',
            displayName: 'Action',
            field: 'action',
            enableHiding: false,
            enableFiltering: false,
            cellTemplate: `
                <tables-action-cell
                    table='row.entity'
                    on-edit-start='grid.appScope.$ctrl.$scope.startEditDbTableCache($event)'
                    on-cache-select='grid.appScope.$ctrl.onCacheSelect($event)'
                    caches='grid.appScope.$ctrl._importCachesOrTemplates'
                    import-actions='grid.appScope.$ctrl.$scope.importActions'
                ></tables-action-cell>
            `,
            visible: true,
            minWidth: 350
        }
    ];
}

export const component = {
    name: 'modalImportModelsFromTemplate',
    controller: ModalImportModelsFromTemplate,
    templateUrl,
    bindings: {
        onHide: '&',
        clusterID: '<clusterId'
    }
};

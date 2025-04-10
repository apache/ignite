

import _ from 'lodash';
import {nonEmpty, nonNil} from 'app/utils/lodashMixins';
import id8 from 'app/utils/id8';
import {Subject, defer, from, of, merge, timer, EMPTY, combineLatest} from 'rxjs';
import {catchError, distinctUntilChanged, expand, exhaustMap, filter, finalize, first, ignoreElements, map, pluck, switchMap, takeUntil, takeWhile, take, tap} from 'rxjs/operators';

import {default as Notebook} from '../../notebook.service';
import {default as MessagesServiceFactory} from 'app/services/Messages.service';
import {default as LegacyConfirmServiceFactory} from 'app/services/Confirm.service';
import {default as InputDialog} from 'app/components/input-dialog/input-dialog.service';
import {QueryActions} from './components/query-actions-button/controller';
import {CancellationError} from 'app/errors/CancellationError';
import {DemoService} from 'app/modules/demo/Demo.module';

// Time line X axis descriptor.
export const TIME_LINE = {value: -1, type: 'java.sql.Date', label: 'TIME_LINE'};

// Row index X axis descriptor.
export const ROW_IDX = {value: -2, type: 'java.lang.Integer', label: 'ROW_IDX'};


const _fullColName = (col) => {
    const res = [];

    if (col.schemaName)
        res.push(col.schemaName);

    if (col.typeName)
        res.push(col.typeName);

    res.push(col.fieldName);

    return res.join('.');
}

let paragraphId = 0;

export class Paragraph {
    id: string;
    name: string;
    queryType: 'SCAN' | 'SQL_FIELDS' | 'GREMLIN';
    meta: Array<any>;
    result: string;
    rows: Array<any>=[];
    chartColumns: Array<any> = [];
    chartKeyCols: Array<any> = [];
    chartValCols: Array<any> = [];
    columnFilter: (col) => boolean;

    constructor($animate, $timeout, JavaTypes, errorParser, paragraph, private $translate: ng.translate.ITranslateService) {
        const self = this;

        self.id = 'paragraph-' + paragraphId++;
        self.queryType = paragraph.queryType || 'SQL_FIELDS';
        self.maxPages = 0;
        self.filter = '';
        self.useAsDefaultSchema = false;
        self.localQueryMode = false;
        self.csvIsPreparing = false;
        self.scanningInProgress = false;

        self.cancelQuerySubject = new Subject();
        self.cancelExportSubject = new Subject();

        _.assign(this, paragraph);

        Object.defineProperty(this, 'gridOptions', {value: {
            enableGridMenu: false,
            enableColumnMenus: false,
            flatEntityAccess: true,
            fastWatch: true,
            categories: [],
            rebuildColumns() {
                if (_.isNil(this.api))
                    return;

                this.categories.length = 0;

                this.columnDefs = _.reduce(self.meta, (cols, col, idx) => {
                    cols.push({
                        displayName: col.fieldName,
                        headerTooltip: _fullColName(col),
                        field: idx.toString(),
                        minWidth: 50,
                        cellClass: 'cell-left',
                        visible: self.columnFilter(col)
                    });

                    this.categories.push({
                        name: col.fieldName,
                        visible: self.columnFilter(col),
                        enableHiding: true
                    });

                    return cols;
                }, []);

                $timeout(() => this.api.core.notifyDataChange('column'));
            },
            adjustHeight() {
                if (_.isNil(this.api))
                    return;

                this.data = self.rows;

                const height = Math.min(self.rows.length, 15) * 30 + 47;

                // Remove header height.
                this.api.grid.element.css('height', height + 'px');

                $timeout(() => this.api.core.handleWindowResize());
            },
            onRegisterApi(api) {
                $animate.enabled(api.grid.element, false);

                this.api = api;

                this.rebuildColumns();

                this.adjustHeight();
            }
        }});

        Object.defineProperty(this, 'chartHistory', {value: []});

        Object.defineProperty(this, 'error', {value: {
            root: {},
            message: ''
        }});

        this.showLoading = (enable) => {
            if (this.queryType === 'SCAN')
                this.scanningInProgress = enable;

            this.loading = enable;
        };

        this.setError = (err) => {
            const parsedErr = errorParser.parse(err);

            this.error.root = err;
            this.error.message = parsedErr.message;

            let cause = err;

            while (nonNil(cause)) {
                if (nonEmpty(cause.className) &&
                    _.includes(['SQLException', 'JdbcSQLException', 'QueryCancelledException'], JavaTypes.shortClassName(cause.className))) {
                    const parsedCause = errorParser.parse(cause.message || cause.className);

                    this.error.message = parsedCause.message;

                    break;
                }

                cause = cause.cause;
            }

            if (_.isEmpty(this.error.message) && nonEmpty(err.className)) {
                this.error.message = this.$translate.instant('queries.notebook.internalClusterErrorMessagePrefix');

                if (nonEmpty(err.className))
                    this.error.message += ': ' + err.className;
            }
        };
    }

    resultType() {
        if (_.isNil(this.queryArgs))
            return null;

        if (nonEmpty(this.error.message))
            return 'error';

        if (_.isEmpty(this.rows))
            return 'empty';

        return this.result === 'TABLE' ? 'table' : 'chart';
    }

    nonRefresh() {
        return _.isNil(this.rate) || _.isNil(this.rate.stopTime);
    }

    table() {
        return this.result === 'TABLE';
    }

    chart() {
        return this.result !== 'TABLE' && this.result !== 'NONE';
    }

    nonEmpty() {
        return this.rows && this.rows.length > 0;
    }

    queryExecuted() {
        return nonEmpty(this.meta) || nonEmpty(this.error.message);
    }

    scanExplain() {
        return this.queryExecuted() && (this.queryType === 'SCAN' || this.queryArgs.query.startsWith('EXPLAIN '));
    }

    timeLineSupported() {
        return this.result !== 'PIE';
    }

    chartColumnsConfigured() {
        return nonEmpty(this.chartKeyCols) && nonEmpty(this.chartValCols);
    }

    chartTimeLineEnabled() {
        return nonEmpty(this.chartKeyCols) && _.eq(this.chartKeyCols[0], TIME_LINE);
    }

    executionInProgress(showLocal = false) {
        return this.loading && (this.localQueryMode === showLocal);
    }

    checkScanInProgress(showLocal = false) {
        return this.scanningInProgress && (this.localQueryMode === showLocal);
    }

    cancelRefresh($interval) {
        if (this.rate && this.rate.stopTime) {
            $interval.cancel(this.rate.stopTime);

            delete this.rate.stopTime;
        }
    }

    reset($interval) {
        this.meta = [];
        this.chartColumns = [];
        this.chartKeyCols = [];
        this.chartValCols = [];
        this.error.root = {};
        this.error.message = '';
        this.rows = [];
        this.duration = 0;

        this.cancelRefresh($interval);
    }

    toJSON() {
        return {
            name: this.name,
            query: this.query,
            result: this.result,
            pageSize: this.pageSize,
            timeLineSpan: this.timeLineSpan,
            maxPages: this.maxPages,
            cacheName: this.cacheName,
            useAsDefaultSchema: this.useAsDefaultSchema,
            chartsOptions: this.chartsOptions,
            rate: this.rate,
            queryType: this.queryType,
            nonCollocatedJoins: this.nonCollocatedJoins,
            enforceJoinOrder: this.enforceJoinOrder,
            lazy: this.lazy,
            collocated: this.collocated,
            keepBinary: this.keepBinary
        };
    }
}

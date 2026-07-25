
import _ from 'lodash';
import debounce from 'lodash/debounce';
import headerTemplate from 'app/primitives/ui-grid-header/index.tpl.pug';

import ResizeObserver from 'resize-observer-polyfill';

export default class IgniteUiGrid {
    /** @type {import('ui-grid').IGridOptions} */
    grid;

    /** @type {import('ui-grid').IGridApi} */
    gridApi;

    /** @type */
    gridThin;

    /** @type */
    gridHeight;

    /** @type */
    items;

    /** @type */
    columnDefs;

    /** @type */
    categories;

    /** @type {boolean} */
    singleSelect;

    /** @type */
    onSelectionChange;

    /** @type */
    selectedRows;

    /** @type */
    selectedRowsId;

    /** @type */
    _selected;

    static $inject = ['$scope', '$element', '$timeout', 'gridUtil'];

    /**
     * @param {ng.IScope} $scope
     * @param $element
     * @param $timeout
     * @param gridUtil
     */
    constructor($scope, $element, $timeout, gridUtil) {
        this.$scope = $scope;
        this.$element = $element;
        this.$timeout = $timeout;
        this.gridUtil = gridUtil;

        this.rowIdentityKey = 'id';

        this.rowHeight = 48;
        this.headerRowHeight = 70;
    }

    $onInit() {
        this.SCROLLBAR_WIDTH = this.gridUtil.getScrollbarWidth();

        if (this.gridThin) {
            this.rowHeight = 36;
            this.headerRowHeight = 48;
        }

        this.grid = {
            appScopeProvider: this.$scope.$parent,
            data: this.items,
            columnDefs: this.columnDefs,
            categories: this.categories,
            rowHeight: this.rowHeight,
            multiSelect: !this.singleSelect,
            enableSelectAll: !this.singleSelect,
            headerRowHeight: this.headerRowHeight,
            columnVirtualizationThreshold: 30,
            enableColumnMenus: false,
            enableFullRowSelection: true,
            enableFiltering: true,
            enableRowHashing: false,
            fastWatch: true,
            showTreeExpandNoChildren: false,
            modifierKeysToMultiSelect: true,
            selectionRowHeaderWidth: 52,
            exporterCsvColumnSeparator: ';',
            onRegisterApi: (api) => {
                this.gridApi = api;

                const _isExpandedTree = (tree) => {
                    return !_.find(tree, (node) => {
                        return !_.isEmpty(node.children) && (node.state === 'collapsed' || !_isExpandedTree(node.children));
                    });
                };

                api.core.on.rowsRendered(this.$scope, () => {
                    const tree = _.get(api, 'grid.treeBase.tree');

                    if (tree)
                        api.grid.treeBase.expandAll = _isExpandedTree(tree);
                });

                api.core.on.rowsVisibleChanged(this.$scope, () => {
                    this.adjustHeight();

                    // Without property existence check non-set selectedRows or selectedRowsId
                    // binding might cause unwanted behavior,
                    // like unchecking rows during any items change,
                    // even if nothing really changed.
                    if (this._selected && this._selected.length && this.onSelectionChange) {
                        this.applyIncomingSelectionRows(this._selected);

                        // Change selected rows if filter was changed.
                        this.onRowsSelectionChange([]);
                    }
                });

                if (this.onSelectionChange) {
                    api.selection.on.rowSelectionChanged(this.$scope, (row, e) => {
                        this.onRowsSelectionChange([row], e);
                    });

                    api.selection.on.rowSelectionChangedBatch(this.$scope, (rows, e) => {
                        this.onRowsSelectionChange(rows, e);
                    });
                }

                api.core.on.filterChanged(this.$scope, (column) => {
                    this.onFilterChange(column);
                });

                this.$timeout(() => {
                    if (this.selectedRowsId) this.applyIncomingSelectionRowsId(this.selectedRowsId);
                });

                this.resizeObserver = new ResizeObserver(() => api.core.handleWindowResize());
                this.resizeObserver.observe(this.$element[0]);

                if (this.onApiRegistered)
                    this.onApiRegistered({$event: api});
            }
        };

        if (this.grid.categories)
            this.grid.headerTemplate = headerTemplate;
    }

    $onChanges(changes) {
        const hasChanged = (binding) =>
            binding in changes && changes[binding].currentValue !== changes[binding].previousValue;

        if (hasChanged('items') && this.grid)
            this.grid.data = changes.items.currentValue;

        if (hasChanged('selectedRows') && this.grid && this.grid.data && this.onSelectionChange)
            this.applyIncomingSelectionRows(changes.selectedRows.currentValue);

        if (hasChanged('selectedRowsId') && this.grid && this.grid.data)
            this.applyIncomingSelectionRowsId(changes.selectedRowsId.currentValue);

        if (hasChanged('gridHeight') && this.grid)
            this.adjustHeight();
    }

    $onDestroy() {
        if (this.resizeObserver)
            this.resizeObserver.disconnect();
    }

    applyIncomingSelectionRows = (selected = []) => {
        this.gridApi.selection.clearSelectedRows({ ignore: true });

        const visibleRows = this.gridApi.core.getVisibleRows(this.gridApi.grid)
            .map(({ entity }) => entity);

        const rows = visibleRows.filter((r) =>
            selected.map((row) => row[this.rowIdentityKey]).includes(r[this.rowIdentityKey]));

        rows.forEach((r) => {
            this.gridApi.selection.selectRow(r, { ignore: true });
        });
    };

    applyIncomingSelectionRowsId = (selected = []) => {
        if (this.onSelectionChange) {
            this.gridApi.selection.clearSelectedRows({ ignore: true });

            const visibleRows = this.gridApi.core.getVisibleRows(this.gridApi.grid)
                .map(({ entity }) => entity);

            const rows = visibleRows.filter((r) =>
                selected.includes(r[this.rowIdentityKey]));

            rows.forEach((r) => {
                this.gridApi.selection.selectRow(r, { ignore: true });
            });
        }
    };

    onRowsSelectionChange = (rows, e = {}) => {
        this.selectGroupHeaders(this.gridApi.grid.treeBase && this.gridApi.grid.treeBase.tree || []);
        this.debounceSelectionChange(rows, e);
    };

    debounceSelectionChange = debounce((rows, e) => {
        if (e.ignore)
            return;

        this._selected = this.gridApi.selection.legacyGetSelectedRows();

        if (this.onSelectionChange)
            this.onSelectionChange({ $event: this._selected });
    })

    selectGroupHeaders = (rows) => {
        const groupHeaders = rows.filter((row) => row.row.groupHeader);
        if (!groupHeaders.length) return;

        groupHeaders.forEach((groupHeader) => {
            const allChildrenSelected = groupHeader.children.every((child) => {
                if (child.row.groupHeader) this.selectGroupHeaders(child.children);
                return child.row.isSelected;
            });
            if (groupHeader.row.isSelected !== allChildrenSelected) groupHeader.row.setSelected(allChildrenSelected);
        });
    }

    onFilterChange = debounce((column) => {
        if (!this.gridApi.selection)
            return;

        if (this.selectedRows && this.onSelectionChange)
            this.applyIncomingSelectionRows(this.selectedRows);

        if (this.selectedRowsId)
            this.applyIncomingSelectionRowsId(this.selectedRowsId);
    });

    adjustHeight() {
        let height = this.gridHeight;

        if (!height) {
            const maxRowsToShow = this.maxRowsToShow || 5;
            const headerBorder = 1;
            const visibleRows = this.gridApi.core.getVisibleRows().length;
            const header = this.grid.headerRowHeight + headerBorder;
            const optionalScroll = (visibleRows ? this.gridUtil.getScrollbarWidth() : 0);

            height = Math.min(visibleRows, maxRowsToShow) * this.grid.rowHeight + header + optionalScroll;
        }

        this.gridApi.grid.element.css('height', height + 'px');
        this.gridApi.core.handleWindowResize();
    }
}

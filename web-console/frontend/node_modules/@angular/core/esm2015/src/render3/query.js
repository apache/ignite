/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
// We are temporarily importing the existing viewEngine_from core so we can be sure we are
// correctly implementing its interfaces for backwards compatibility.
import { ElementRef as ViewEngine_ElementRef } from '../linker/element_ref';
import { QueryList } from '../linker/query_list';
import { TemplateRef as ViewEngine_TemplateRef } from '../linker/template_ref';
import { ViewContainerRef } from '../linker/view_container_ref';
import { assertDataInRange, assertDefined, throwError } from '../util/assert';
import { stringify } from '../util/stringify';
import { assertFirstTemplatePass, assertLContainer } from './assert';
import { getNodeInjectable, locateDirectiveOrProvider } from './di';
import { storeCleanupWithContext } from './instructions/shared';
import { CONTAINER_HEADER_OFFSET, MOVED_VIEWS } from './interfaces/container';
import { unusedValueExportToPlacateAjd as unused1 } from './interfaces/definition';
import { unusedValueExportToPlacateAjd as unused2 } from './interfaces/injector';
import { unusedValueExportToPlacateAjd as unused3 } from './interfaces/node';
import { unusedValueExportToPlacateAjd as unused4 } from './interfaces/query';
import { DECLARATION_LCONTAINER, PARENT, QUERIES, TVIEW } from './interfaces/view';
import { assertNodeOfPossibleTypes } from './node_assert';
import { getCurrentQueryIndex, getLView, getPreviousOrParentTNode, isCreationMode, setCurrentQueryIndex } from './state';
import { createContainerRef, createElementRef, createTemplateRef } from './view_engine_compatibility';
/** @type {?} */
const unusedValueToPlacateAjd = unused1 + unused2 + unused3 + unused4;
/**
 * @template T
 */
class LQuery_ {
    /**
     * @param {?} queryList
     */
    constructor(queryList) {
        this.queryList = queryList;
        this.matches = null;
    }
    /**
     * @return {?}
     */
    clone() { return new LQuery_(this.queryList); }
    /**
     * @return {?}
     */
    setDirty() { this.queryList.setDirty(); }
}
if (false) {
    /** @type {?} */
    LQuery_.prototype.matches;
    /** @type {?} */
    LQuery_.prototype.queryList;
}
class LQueries_ {
    /**
     * @param {?=} queries
     */
    constructor(queries = []) {
        this.queries = queries;
    }
    /**
     * @param {?} tView
     * @return {?}
     */
    createEmbeddedView(tView) {
        /** @type {?} */
        const tQueries = tView.queries;
        if (tQueries !== null) {
            /** @type {?} */
            const noOfInheritedQueries = tView.contentQueries !== null ? tView.contentQueries[0] : tQueries.length;
            /** @type {?} */
            const viewLQueries = new Array(noOfInheritedQueries);
            // An embedded view has queries propagated from a declaration view at the beginning of the
            // TQueries collection and up until a first content query declared in the embedded view. Only
            // propagated LQueries are created at this point (LQuery corresponding to declared content
            // queries will be instantiated from the content query instructions for each directive).
            for (let i = 0; i < noOfInheritedQueries; i++) {
                /** @type {?} */
                const tQuery = tQueries.getByIndex(i);
                /** @type {?} */
                const parentLQuery = this.queries[tQuery.indexInDeclarationView];
                viewLQueries[i] = parentLQuery.clone();
            }
            return new LQueries_(viewLQueries);
        }
        return null;
    }
    /**
     * @param {?} tView
     * @return {?}
     */
    insertView(tView) { this.dirtyQueriesWithMatches(tView); }
    /**
     * @param {?} tView
     * @return {?}
     */
    detachView(tView) { this.dirtyQueriesWithMatches(tView); }
    /**
     * @private
     * @param {?} tView
     * @return {?}
     */
    dirtyQueriesWithMatches(tView) {
        for (let i = 0; i < this.queries.length; i++) {
            if (getTQuery(tView, i).matches !== null) {
                this.queries[i].setDirty();
            }
        }
    }
}
if (false) {
    /** @type {?} */
    LQueries_.prototype.queries;
}
class TQueryMetadata_ {
    /**
     * @param {?} predicate
     * @param {?} descendants
     * @param {?} isStatic
     * @param {?=} read
     */
    constructor(predicate, descendants, isStatic, read = null) {
        this.predicate = predicate;
        this.descendants = descendants;
        this.isStatic = isStatic;
        this.read = read;
    }
}
if (false) {
    /** @type {?} */
    TQueryMetadata_.prototype.predicate;
    /** @type {?} */
    TQueryMetadata_.prototype.descendants;
    /** @type {?} */
    TQueryMetadata_.prototype.isStatic;
    /** @type {?} */
    TQueryMetadata_.prototype.read;
}
class TQueries_ {
    /**
     * @param {?=} queries
     */
    constructor(queries = []) {
        this.queries = queries;
    }
    /**
     * @param {?} tView
     * @param {?} tNode
     * @return {?}
     */
    elementStart(tView, tNode) {
        ngDevMode && assertFirstTemplatePass(tView, 'Queries should collect results on the first template pass only');
        for (let query of this.queries) {
            query.elementStart(tView, tNode);
        }
    }
    /**
     * @param {?} tNode
     * @return {?}
     */
    elementEnd(tNode) {
        for (let query of this.queries) {
            query.elementEnd(tNode);
        }
    }
    /**
     * @param {?} tNode
     * @return {?}
     */
    embeddedTView(tNode) {
        /** @type {?} */
        let queriesForTemplateRef = null;
        for (let i = 0; i < this.length; i++) {
            /** @type {?} */
            const childQueryIndex = queriesForTemplateRef !== null ? queriesForTemplateRef.length : 0;
            /** @type {?} */
            const tqueryClone = this.getByIndex(i).embeddedTView(tNode, childQueryIndex);
            if (tqueryClone) {
                tqueryClone.indexInDeclarationView = i;
                if (queriesForTemplateRef !== null) {
                    queriesForTemplateRef.push(tqueryClone);
                }
                else {
                    queriesForTemplateRef = [tqueryClone];
                }
            }
        }
        return queriesForTemplateRef !== null ? new TQueries_(queriesForTemplateRef) : null;
    }
    /**
     * @param {?} tView
     * @param {?} tNode
     * @return {?}
     */
    template(tView, tNode) {
        ngDevMode && assertFirstTemplatePass(tView, 'Queries should collect results on the first template pass only');
        for (let query of this.queries) {
            query.template(tView, tNode);
        }
    }
    /**
     * @param {?} index
     * @return {?}
     */
    getByIndex(index) {
        ngDevMode && assertDataInRange(this.queries, index);
        return this.queries[index];
    }
    /**
     * @return {?}
     */
    get length() { return this.queries.length; }
    /**
     * @param {?} tquery
     * @return {?}
     */
    track(tquery) { this.queries.push(tquery); }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    TQueries_.prototype.queries;
}
class TQuery_ {
    /**
     * @param {?} metadata
     * @param {?=} nodeIndex
     */
    constructor(metadata, nodeIndex = -1) {
        this.metadata = metadata;
        this.matches = null;
        this.indexInDeclarationView = -1;
        this.crossesNgTemplate = false;
        /**
         * A flag indicating if a given query still applies to nodes it is crossing. We use this flag
         * (alongside with _declarationNodeIndex) to know when to stop applying content queries to
         * elements in a template.
         */
        this._appliesToNextNode = true;
        this._declarationNodeIndex = nodeIndex;
    }
    /**
     * @param {?} tView
     * @param {?} tNode
     * @return {?}
     */
    elementStart(tView, tNode) {
        if (this.isApplyingToNode(tNode)) {
            this.matchTNode(tView, tNode);
        }
    }
    /**
     * @param {?} tNode
     * @return {?}
     */
    elementEnd(tNode) {
        if (this._declarationNodeIndex === tNode.index) {
            this._appliesToNextNode = false;
        }
    }
    /**
     * @param {?} tView
     * @param {?} tNode
     * @return {?}
     */
    template(tView, tNode) { this.elementStart(tView, tNode); }
    /**
     * @param {?} tNode
     * @param {?} childQueryIndex
     * @return {?}
     */
    embeddedTView(tNode, childQueryIndex) {
        if (this.isApplyingToNode(tNode)) {
            this.crossesNgTemplate = true;
            // A marker indicating a `<ng-template>` element (a placeholder for query results from
            // embedded views created based on this `<ng-template>`).
            this.addMatch(-tNode.index, childQueryIndex);
            return new TQuery_(this.metadata);
        }
        return null;
    }
    /**
     * @private
     * @param {?} tNode
     * @return {?}
     */
    isApplyingToNode(tNode) {
        if (this._appliesToNextNode && this.metadata.descendants === false) {
            return this._declarationNodeIndex === (tNode.parent ? tNode.parent.index : -1);
        }
        return this._appliesToNextNode;
    }
    /**
     * @private
     * @param {?} tView
     * @param {?} tNode
     * @return {?}
     */
    matchTNode(tView, tNode) {
        if (Array.isArray(this.metadata.predicate)) {
            /** @type {?} */
            const localNames = (/** @type {?} */ (this.metadata.predicate));
            for (let i = 0; i < localNames.length; i++) {
                this.matchTNodeWithReadOption(tView, tNode, getIdxOfMatchingSelector(tNode, localNames[i]));
            }
        }
        else {
            /** @type {?} */
            const typePredicate = (/** @type {?} */ (this.metadata.predicate));
            if (typePredicate === ViewEngine_TemplateRef) {
                if (tNode.type === 0 /* Container */) {
                    this.matchTNodeWithReadOption(tView, tNode, -1);
                }
            }
            else {
                this.matchTNodeWithReadOption(tView, tNode, locateDirectiveOrProvider(tNode, tView, typePredicate, false, false));
            }
        }
    }
    /**
     * @private
     * @param {?} tView
     * @param {?} tNode
     * @param {?} nodeMatchIdx
     * @return {?}
     */
    matchTNodeWithReadOption(tView, tNode, nodeMatchIdx) {
        if (nodeMatchIdx !== null) {
            /** @type {?} */
            const read = this.metadata.read;
            if (read !== null) {
                if (read === ViewEngine_ElementRef || read === ViewContainerRef ||
                    read === ViewEngine_TemplateRef && tNode.type === 0 /* Container */) {
                    this.addMatch(tNode.index, -2);
                }
                else {
                    /** @type {?} */
                    const directiveOrProviderIdx = locateDirectiveOrProvider(tNode, tView, read, false, false);
                    if (directiveOrProviderIdx !== null) {
                        this.addMatch(tNode.index, directiveOrProviderIdx);
                    }
                }
            }
            else {
                this.addMatch(tNode.index, nodeMatchIdx);
            }
        }
    }
    /**
     * @private
     * @param {?} tNodeIdx
     * @param {?} matchIdx
     * @return {?}
     */
    addMatch(tNodeIdx, matchIdx) {
        if (this.matches === null) {
            this.matches = [tNodeIdx, matchIdx];
        }
        else {
            this.matches.push(tNodeIdx, matchIdx);
        }
    }
}
if (false) {
    /** @type {?} */
    TQuery_.prototype.matches;
    /** @type {?} */
    TQuery_.prototype.indexInDeclarationView;
    /** @type {?} */
    TQuery_.prototype.crossesNgTemplate;
    /**
     * A node index on which a query was declared (-1 for view queries and ones inherited from the
     * declaration template). We use this index (alongside with _appliesToNextNode flag) to know
     * when to apply content queries to elements in a template.
     * @type {?}
     * @private
     */
    TQuery_.prototype._declarationNodeIndex;
    /**
     * A flag indicating if a given query still applies to nodes it is crossing. We use this flag
     * (alongside with _declarationNodeIndex) to know when to stop applying content queries to
     * elements in a template.
     * @type {?}
     * @private
     */
    TQuery_.prototype._appliesToNextNode;
    /** @type {?} */
    TQuery_.prototype.metadata;
}
/**
 * Iterates over local names for a given node and returns directive index
 * (or -1 if a local name points to an element).
 *
 * @param {?} tNode static data of a node to check
 * @param {?} selector selector to match
 * @return {?} directive index, -1 or null if a selector didn't match any of the local names
 */
function getIdxOfMatchingSelector(tNode, selector) {
    /** @type {?} */
    const localNames = tNode.localNames;
    if (localNames !== null) {
        for (let i = 0; i < localNames.length; i += 2) {
            if (localNames[i] === selector) {
                return (/** @type {?} */ (localNames[i + 1]));
            }
        }
    }
    return null;
}
/**
 * @param {?} tNode
 * @param {?} currentView
 * @return {?}
 */
function createResultByTNodeType(tNode, currentView) {
    if (tNode.type === 3 /* Element */ || tNode.type === 4 /* ElementContainer */) {
        return createElementRef(ViewEngine_ElementRef, tNode, currentView);
    }
    else if (tNode.type === 0 /* Container */) {
        return createTemplateRef(ViewEngine_TemplateRef, ViewEngine_ElementRef, tNode, currentView);
    }
    return null;
}
/**
 * @param {?} lView
 * @param {?} tNode
 * @param {?} matchingIdx
 * @param {?} read
 * @return {?}
 */
function createResultForNode(lView, tNode, matchingIdx, read) {
    if (matchingIdx === -1) {
        // if read token and / or strategy is not specified, detect it using appropriate tNode type
        return createResultByTNodeType(tNode, lView);
    }
    else if (matchingIdx === -2) {
        // read a special token from a node injector
        return createSpecialToken(lView, tNode, read);
    }
    else {
        // read a token
        return getNodeInjectable(lView[TVIEW].data, lView, matchingIdx, (/** @type {?} */ (tNode)));
    }
}
/**
 * @param {?} lView
 * @param {?} tNode
 * @param {?} read
 * @return {?}
 */
function createSpecialToken(lView, tNode, read) {
    if (read === ViewEngine_ElementRef) {
        return createElementRef(ViewEngine_ElementRef, tNode, lView);
    }
    else if (read === ViewEngine_TemplateRef) {
        return createTemplateRef(ViewEngine_TemplateRef, ViewEngine_ElementRef, tNode, lView);
    }
    else if (read === ViewContainerRef) {
        ngDevMode && assertNodeOfPossibleTypes(tNode, 3 /* Element */, 0 /* Container */, 4 /* ElementContainer */);
        return createContainerRef(ViewContainerRef, ViewEngine_ElementRef, (/** @type {?} */ (tNode)), lView);
    }
    else {
        ngDevMode &&
            throwError(`Special token to read should be one of ElementRef, TemplateRef or ViewContainerRef but got ${stringify(read)}.`);
    }
}
/**
 * A helper function that creates query results for a given view. This function is meant to do the
 * processing once and only once for a given view instance (a set of results for a given view
 * doesn't change).
 * @template T
 * @param {?} lView
 * @param {?} tQuery
 * @param {?} queryIndex
 * @return {?}
 */
function materializeViewResults(lView, tQuery, queryIndex) {
    /** @type {?} */
    const lQuery = (/** @type {?} */ ((/** @type {?} */ (lView[QUERIES])).queries))[queryIndex];
    if (lQuery.matches === null) {
        /** @type {?} */
        const tViewData = lView[TVIEW].data;
        /** @type {?} */
        const tQueryMatches = (/** @type {?} */ (tQuery.matches));
        /** @type {?} */
        const result = new Array(tQueryMatches.length / 2);
        for (let i = 0; i < tQueryMatches.length; i += 2) {
            /** @type {?} */
            const matchedNodeIdx = tQueryMatches[i];
            if (matchedNodeIdx < 0) {
                // we at the <ng-template> marker which might have results in views created based on this
                // <ng-template> - those results will be in separate views though, so here we just leave
                // null as a placeholder
                result[i / 2] = null;
            }
            else {
                ngDevMode && assertDataInRange(tViewData, matchedNodeIdx);
                /** @type {?} */
                const tNode = (/** @type {?} */ (tViewData[matchedNodeIdx]));
                result[i / 2] =
                    createResultForNode(lView, tNode, tQueryMatches[i + 1], tQuery.metadata.read);
            }
        }
        lQuery.matches = result;
    }
    return lQuery.matches;
}
/**
 * A helper function that collects (already materialized) query results from a tree of views,
 * starting with a provided LView.
 * @template T
 * @param {?} lView
 * @param {?} queryIndex
 * @param {?} result
 * @return {?}
 */
function collectQueryResults(lView, queryIndex, result) {
    /** @type {?} */
    const tQuery = (/** @type {?} */ (lView[TVIEW].queries)).getByIndex(queryIndex);
    /** @type {?} */
    const tQueryMatches = tQuery.matches;
    if (tQueryMatches !== null) {
        /** @type {?} */
        const lViewResults = materializeViewResults(lView, tQuery, queryIndex);
        for (let i = 0; i < tQueryMatches.length; i += 2) {
            /** @type {?} */
            const tNodeIdx = tQueryMatches[i];
            if (tNodeIdx > 0) {
                /** @type {?} */
                const viewResult = lViewResults[i / 2];
                ngDevMode && assertDefined(viewResult, 'materialized query result should be defined');
                result.push((/** @type {?} */ (viewResult)));
            }
            else {
                /** @type {?} */
                const childQueryIndex = tQueryMatches[i + 1];
                /** @type {?} */
                const declarationLContainer = (/** @type {?} */ (lView[-tNodeIdx]));
                ngDevMode && assertLContainer(declarationLContainer);
                // collect matches for views inserted in this container
                for (let i = CONTAINER_HEADER_OFFSET; i < declarationLContainer.length; i++) {
                    /** @type {?} */
                    const embeddedLView = declarationLContainer[i];
                    if (embeddedLView[DECLARATION_LCONTAINER] === embeddedLView[PARENT]) {
                        collectQueryResults(embeddedLView, childQueryIndex, result);
                    }
                }
                // collect matches for views created from this declaration container and inserted into
                // different containers
                if (declarationLContainer[MOVED_VIEWS] !== null) {
                    for (let embeddedLView of (/** @type {?} */ (declarationLContainer[MOVED_VIEWS]))) {
                        collectQueryResults(embeddedLView, childQueryIndex, result);
                    }
                }
            }
        }
    }
    return result;
}
/**
 * Refreshes a query by combining matches from all active views and removing matches from deleted
 * views.
 *
 * \@codeGenApi
 * @param {?} queryList
 * @return {?} `true` if a query got dirty during change detection or if this is a static query
 * resolving in creation mode, `false` otherwise.
 *
 */
export function ɵɵqueryRefresh(queryList) {
    /** @type {?} */
    const lView = getLView();
    /** @type {?} */
    const queryIndex = getCurrentQueryIndex();
    setCurrentQueryIndex(queryIndex + 1);
    /** @type {?} */
    const tQuery = getTQuery(lView[TVIEW], queryIndex);
    if (queryList.dirty && (isCreationMode() === tQuery.metadata.isStatic)) {
        if (tQuery.matches === null) {
            queryList.reset([]);
        }
        else {
            /** @type {?} */
            const result = tQuery.crossesNgTemplate ? collectQueryResults(lView, queryIndex, []) :
                materializeViewResults(lView, tQuery, queryIndex);
            queryList.reset(result);
            queryList.notifyOnChanges();
        }
        return true;
    }
    return false;
}
/**
 * Creates new QueryList for a static view query.
 *
 * \@codeGenApi
 * @template T
 * @param {?} predicate The type for which the query will search
 * @param {?} descend Whether or not to descend into children
 * @param {?=} read What to save in the query
 *
 * @return {?}
 */
export function ɵɵstaticViewQuery(predicate, descend, read) {
    viewQueryInternal(getLView(), predicate, descend, read, true);
}
/**
 * Creates new QueryList, stores the reference in LView and returns QueryList.
 *
 * \@codeGenApi
 * @template T
 * @param {?} predicate The type for which the query will search
 * @param {?} descend Whether or not to descend into children
 * @param {?=} read What to save in the query
 *
 * @return {?}
 */
export function ɵɵviewQuery(predicate, descend, read) {
    viewQueryInternal(getLView(), predicate, descend, read, false);
}
/**
 * @template T
 * @param {?} lView
 * @param {?} predicate
 * @param {?} descend
 * @param {?} read
 * @param {?} isStatic
 * @return {?}
 */
function viewQueryInternal(lView, predicate, descend, read, isStatic) {
    /** @type {?} */
    const tView = lView[TVIEW];
    if (tView.firstTemplatePass) {
        createTQuery(tView, new TQueryMetadata_(predicate, descend, isStatic, read), -1);
        if (isStatic) {
            tView.staticViewQueries = true;
        }
    }
    createLQuery(lView);
}
/**
 * Loads a QueryList corresponding to the current view query.
 *
 * \@codeGenApi
 * @template T
 * @return {?}
 */
export function ɵɵloadViewQuery() {
    return loadQueryInternal(getLView(), getCurrentQueryIndex());
}
/**
 * Registers a QueryList, associated with a content query, for later refresh (part of a view
 * refresh).
 *
 * \@codeGenApi
 * @template T
 * @param {?} directiveIndex Current directive index
 * @param {?} predicate The type for which the query will search
 * @param {?} descend Whether or not to descend into children
 * @param {?=} read What to save in the query
 * @return {?} QueryList<T>
 *
 */
export function ɵɵcontentQuery(directiveIndex, predicate, descend, read) {
    contentQueryInternal(getLView(), predicate, descend, read, false, getPreviousOrParentTNode(), directiveIndex);
}
/**
 * Registers a QueryList, associated with a static content query, for later refresh
 * (part of a view refresh).
 *
 * \@codeGenApi
 * @template T
 * @param {?} directiveIndex Current directive index
 * @param {?} predicate The type for which the query will search
 * @param {?} descend Whether or not to descend into children
 * @param {?=} read What to save in the query
 * @return {?} QueryList<T>
 *
 */
export function ɵɵstaticContentQuery(directiveIndex, predicate, descend, read) {
    contentQueryInternal(getLView(), predicate, descend, read, true, getPreviousOrParentTNode(), directiveIndex);
}
/**
 * @template T
 * @param {?} lView
 * @param {?} predicate
 * @param {?} descend
 * @param {?} read
 * @param {?} isStatic
 * @param {?} tNode
 * @param {?} directiveIndex
 * @return {?}
 */
function contentQueryInternal(lView, predicate, descend, read, isStatic, tNode, directiveIndex) {
    /** @type {?} */
    const tView = lView[TVIEW];
    if (tView.firstTemplatePass) {
        createTQuery(tView, new TQueryMetadata_(predicate, descend, isStatic, read), tNode.index);
        saveContentQueryAndDirectiveIndex(tView, directiveIndex);
        if (isStatic) {
            tView.staticContentQueries = true;
        }
    }
    createLQuery(lView);
}
/**
 * Loads a QueryList corresponding to the current content query.
 *
 * \@codeGenApi
 * @template T
 * @return {?}
 */
export function ɵɵloadContentQuery() {
    return loadQueryInternal(getLView(), getCurrentQueryIndex());
}
/**
 * @template T
 * @param {?} lView
 * @param {?} queryIndex
 * @return {?}
 */
function loadQueryInternal(lView, queryIndex) {
    ngDevMode &&
        assertDefined(lView[QUERIES], 'LQueries should be defined when trying to load a query');
    ngDevMode && assertDataInRange((/** @type {?} */ (lView[QUERIES])).queries, queryIndex);
    return (/** @type {?} */ (lView[QUERIES])).queries[queryIndex].queryList;
}
/**
 * @template T
 * @param {?} lView
 * @return {?}
 */
function createLQuery(lView) {
    /** @type {?} */
    const queryList = new QueryList();
    storeCleanupWithContext(lView, queryList, queryList.destroy);
    if (lView[QUERIES] === null)
        lView[QUERIES] = new LQueries_();
    (/** @type {?} */ (lView[QUERIES])).queries.push(new LQuery_(queryList));
}
/**
 * @param {?} tView
 * @param {?} metadata
 * @param {?} nodeIndex
 * @return {?}
 */
function createTQuery(tView, metadata, nodeIndex) {
    if (tView.queries === null)
        tView.queries = new TQueries_();
    tView.queries.track(new TQuery_(metadata, nodeIndex));
}
/**
 * @param {?} tView
 * @param {?} directiveIndex
 * @return {?}
 */
function saveContentQueryAndDirectiveIndex(tView, directiveIndex) {
    /** @type {?} */
    const tViewContentQueries = tView.contentQueries || (tView.contentQueries = []);
    /** @type {?} */
    const lastSavedDirectiveIndex = tView.contentQueries.length ? tViewContentQueries[tViewContentQueries.length - 1] : -1;
    if (directiveIndex !== lastSavedDirectiveIndex) {
        tViewContentQueries.push((/** @type {?} */ (tView.queries)).length - 1, directiveIndex);
    }
}
/**
 * @param {?} tView
 * @param {?} index
 * @return {?}
 */
function getTQuery(tView, index) {
    ngDevMode && assertDefined(tView.queries, 'TQueries must be defined to retrieve a TQuery');
    return (/** @type {?} */ (tView.queries)).getByIndex(index);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicXVlcnkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL3F1ZXJ5LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7Ozs7QUFZQSxPQUFPLEVBQUMsVUFBVSxJQUFJLHFCQUFxQixFQUFDLE1BQU0sdUJBQXVCLENBQUM7QUFDMUUsT0FBTyxFQUFDLFNBQVMsRUFBQyxNQUFNLHNCQUFzQixDQUFDO0FBQy9DLE9BQU8sRUFBQyxXQUFXLElBQUksc0JBQXNCLEVBQUMsTUFBTSx3QkFBd0IsQ0FBQztBQUM3RSxPQUFPLEVBQUMsZ0JBQWdCLEVBQUMsTUFBTSw4QkFBOEIsQ0FBQztBQUM5RCxPQUFPLEVBQUMsaUJBQWlCLEVBQUUsYUFBYSxFQUFFLFVBQVUsRUFBQyxNQUFNLGdCQUFnQixDQUFDO0FBQzVFLE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUU1QyxPQUFPLEVBQUMsdUJBQXVCLEVBQUUsZ0JBQWdCLEVBQUMsTUFBTSxVQUFVLENBQUM7QUFDbkUsT0FBTyxFQUFDLGlCQUFpQixFQUFFLHlCQUF5QixFQUFDLE1BQU0sTUFBTSxDQUFDO0FBQ2xFLE9BQU8sRUFBQyx1QkFBdUIsRUFBQyxNQUFNLHVCQUF1QixDQUFDO0FBQzlELE9BQU8sRUFBQyx1QkFBdUIsRUFBYyxXQUFXLEVBQUMsTUFBTSx3QkFBd0IsQ0FBQztBQUN4RixPQUFPLEVBQUMsNkJBQTZCLElBQUksT0FBTyxFQUFDLE1BQU0seUJBQXlCLENBQUM7QUFDakYsT0FBTyxFQUFDLDZCQUE2QixJQUFJLE9BQU8sRUFBQyxNQUFNLHVCQUF1QixDQUFDO0FBQy9FLE9BQU8sRUFBd0UsNkJBQTZCLElBQUksT0FBTyxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFDbEosT0FBTyxFQUFxRCw2QkFBNkIsSUFBSSxPQUFPLEVBQUMsTUFBTSxvQkFBb0IsQ0FBQztBQUNoSSxPQUFPLEVBQUMsc0JBQXNCLEVBQVMsTUFBTSxFQUFFLE9BQU8sRUFBRSxLQUFLLEVBQVEsTUFBTSxtQkFBbUIsQ0FBQztBQUMvRixPQUFPLEVBQUMseUJBQXlCLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFDeEQsT0FBTyxFQUFDLG9CQUFvQixFQUFFLFFBQVEsRUFBRSx3QkFBd0IsRUFBRSxjQUFjLEVBQUUsb0JBQW9CLEVBQUMsTUFBTSxTQUFTLENBQUM7QUFDdkgsT0FBTyxFQUFDLGtCQUFrQixFQUFFLGdCQUFnQixFQUFFLGlCQUFpQixFQUFDLE1BQU0sNkJBQTZCLENBQUM7O01BRTlGLHVCQUF1QixHQUFHLE9BQU8sR0FBRyxPQUFPLEdBQUcsT0FBTyxHQUFHLE9BQU87Ozs7QUFFckUsTUFBTSxPQUFPOzs7O0lBRVgsWUFBbUIsU0FBdUI7UUFBdkIsY0FBUyxHQUFULFNBQVMsQ0FBYztRQUQxQyxZQUFPLEdBQW9CLElBQUksQ0FBQztJQUNhLENBQUM7Ozs7SUFDOUMsS0FBSyxLQUFnQixPQUFPLElBQUksT0FBTyxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQyxDQUFDLENBQUM7Ozs7SUFDMUQsUUFBUSxLQUFXLElBQUksQ0FBQyxTQUFTLENBQUMsUUFBUSxFQUFFLENBQUMsQ0FBQyxDQUFDO0NBQ2hEOzs7SUFKQywwQkFBZ0M7O0lBQ3BCLDRCQUE4Qjs7QUFLNUMsTUFBTSxTQUFTOzs7O0lBQ2IsWUFBbUIsVUFBeUIsRUFBRTtRQUEzQixZQUFPLEdBQVAsT0FBTyxDQUFvQjtJQUFHLENBQUM7Ozs7O0lBRWxELGtCQUFrQixDQUFDLEtBQVk7O2NBQ3ZCLFFBQVEsR0FBRyxLQUFLLENBQUMsT0FBTztRQUM5QixJQUFJLFFBQVEsS0FBSyxJQUFJLEVBQUU7O2tCQUNmLG9CQUFvQixHQUN0QixLQUFLLENBQUMsY0FBYyxLQUFLLElBQUksQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLGNBQWMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsUUFBUSxDQUFDLE1BQU07O2tCQUN2RSxZQUFZLEdBQWtCLElBQUksS0FBSyxDQUFDLG9CQUFvQixDQUFDO1lBRW5FLDBGQUEwRjtZQUMxRiw2RkFBNkY7WUFDN0YsMEZBQTBGO1lBQzFGLHdGQUF3RjtZQUN4RixLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsb0JBQW9CLEVBQUUsQ0FBQyxFQUFFLEVBQUU7O3NCQUN2QyxNQUFNLEdBQUcsUUFBUSxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUM7O3NCQUMvQixZQUFZLEdBQUcsSUFBSSxDQUFDLE9BQU8sQ0FBQyxNQUFNLENBQUMsc0JBQXNCLENBQUM7Z0JBQ2hFLFlBQVksQ0FBQyxDQUFDLENBQUMsR0FBRyxZQUFZLENBQUMsS0FBSyxFQUFFLENBQUM7YUFDeEM7WUFFRCxPQUFPLElBQUksU0FBUyxDQUFDLFlBQVksQ0FBQyxDQUFDO1NBQ3BDO1FBRUQsT0FBTyxJQUFJLENBQUM7SUFDZCxDQUFDOzs7OztJQUVELFVBQVUsQ0FBQyxLQUFZLElBQVUsSUFBSSxDQUFDLHVCQUF1QixDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQzs7Ozs7SUFFdkUsVUFBVSxDQUFDLEtBQVksSUFBVSxJQUFJLENBQUMsdUJBQXVCLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7Ozs7SUFFL0QsdUJBQXVCLENBQUMsS0FBWTtRQUMxQyxLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsSUFBSSxDQUFDLE9BQU8sQ0FBQyxNQUFNLEVBQUUsQ0FBQyxFQUFFLEVBQUU7WUFDNUMsSUFBSSxTQUFTLENBQUMsS0FBSyxFQUFFLENBQUMsQ0FBQyxDQUFDLE9BQU8sS0FBSyxJQUFJLEVBQUU7Z0JBQ3hDLElBQUksQ0FBQyxPQUFPLENBQUMsQ0FBQyxDQUFDLENBQUMsUUFBUSxFQUFFLENBQUM7YUFDNUI7U0FDRjtJQUNILENBQUM7Q0FDRjs7O0lBcENhLDRCQUFrQzs7QUFzQ2hELE1BQU0sZUFBZTs7Ozs7OztJQUNuQixZQUNXLFNBQTZCLEVBQVMsV0FBb0IsRUFBUyxRQUFpQixFQUNwRixPQUFZLElBQUk7UUFEaEIsY0FBUyxHQUFULFNBQVMsQ0FBb0I7UUFBUyxnQkFBVyxHQUFYLFdBQVcsQ0FBUztRQUFTLGFBQVEsR0FBUixRQUFRLENBQVM7UUFDcEYsU0FBSSxHQUFKLElBQUksQ0FBWTtJQUFHLENBQUM7Q0FDaEM7OztJQUZLLG9DQUFvQzs7SUFBRSxzQ0FBMkI7O0lBQUUsbUNBQXdCOztJQUMzRiwrQkFBdUI7O0FBRzdCLE1BQU0sU0FBUzs7OztJQUNiLFlBQW9CLFVBQW9CLEVBQUU7UUFBdEIsWUFBTyxHQUFQLE9BQU8sQ0FBZTtJQUFHLENBQUM7Ozs7OztJQUU5QyxZQUFZLENBQUMsS0FBWSxFQUFFLEtBQVk7UUFDckMsU0FBUyxJQUFJLHVCQUF1QixDQUNuQixLQUFLLEVBQUUsZ0VBQWdFLENBQUMsQ0FBQztRQUMxRixLQUFLLElBQUksS0FBSyxJQUFJLElBQUksQ0FBQyxPQUFPLEVBQUU7WUFDOUIsS0FBSyxDQUFDLFlBQVksQ0FBQyxLQUFLLEVBQUUsS0FBSyxDQUFDLENBQUM7U0FDbEM7SUFDSCxDQUFDOzs7OztJQUNELFVBQVUsQ0FBQyxLQUFZO1FBQ3JCLEtBQUssSUFBSSxLQUFLLElBQUksSUFBSSxDQUFDLE9BQU8sRUFBRTtZQUM5QixLQUFLLENBQUMsVUFBVSxDQUFDLEtBQUssQ0FBQyxDQUFDO1NBQ3pCO0lBQ0gsQ0FBQzs7Ozs7SUFDRCxhQUFhLENBQUMsS0FBWTs7WUFDcEIscUJBQXFCLEdBQWtCLElBQUk7UUFFL0MsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLElBQUksQ0FBQyxNQUFNLEVBQUUsQ0FBQyxFQUFFLEVBQUU7O2tCQUM5QixlQUFlLEdBQUcscUJBQXFCLEtBQUssSUFBSSxDQUFDLENBQUMsQ0FBQyxxQkFBcUIsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUM7O2tCQUNuRixXQUFXLEdBQUcsSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUMsQ0FBQyxhQUFhLENBQUMsS0FBSyxFQUFFLGVBQWUsQ0FBQztZQUU1RSxJQUFJLFdBQVcsRUFBRTtnQkFDZixXQUFXLENBQUMsc0JBQXNCLEdBQUcsQ0FBQyxDQUFDO2dCQUN2QyxJQUFJLHFCQUFxQixLQUFLLElBQUksRUFBRTtvQkFDbEMscUJBQXFCLENBQUMsSUFBSSxDQUFDLFdBQVcsQ0FBQyxDQUFDO2lCQUN6QztxQkFBTTtvQkFDTCxxQkFBcUIsR0FBRyxDQUFDLFdBQVcsQ0FBQyxDQUFDO2lCQUN2QzthQUNGO1NBQ0Y7UUFFRCxPQUFPLHFCQUFxQixLQUFLLElBQUksQ0FBQyxDQUFDLENBQUMsSUFBSSxTQUFTLENBQUMscUJBQXFCLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDO0lBQ3RGLENBQUM7Ozs7OztJQUVELFFBQVEsQ0FBQyxLQUFZLEVBQUUsS0FBWTtRQUNqQyxTQUFTLElBQUksdUJBQXVCLENBQ25CLEtBQUssRUFBRSxnRUFBZ0UsQ0FBQyxDQUFDO1FBQzFGLEtBQUssSUFBSSxLQUFLLElBQUksSUFBSSxDQUFDLE9BQU8sRUFBRTtZQUM5QixLQUFLLENBQUMsUUFBUSxDQUFDLEtBQUssRUFBRSxLQUFLLENBQUMsQ0FBQztTQUM5QjtJQUNILENBQUM7Ozs7O0lBRUQsVUFBVSxDQUFDLEtBQWE7UUFDdEIsU0FBUyxJQUFJLGlCQUFpQixDQUFDLElBQUksQ0FBQyxPQUFPLEVBQUUsS0FBSyxDQUFDLENBQUM7UUFDcEQsT0FBTyxJQUFJLENBQUMsT0FBTyxDQUFDLEtBQUssQ0FBQyxDQUFDO0lBQzdCLENBQUM7Ozs7SUFFRCxJQUFJLE1BQU0sS0FBYSxPQUFPLElBQUksQ0FBQyxPQUFPLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQzs7Ozs7SUFFcEQsS0FBSyxDQUFDLE1BQWMsSUFBVSxJQUFJLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUM7Q0FDM0Q7Ozs7OztJQWxEYSw0QkFBOEI7O0FBb0Q1QyxNQUFNLE9BQU87Ozs7O0lBbUJYLFlBQW1CLFFBQXdCLEVBQUUsWUFBb0IsQ0FBQyxDQUFDO1FBQWhELGFBQVEsR0FBUixRQUFRLENBQWdCO1FBbEIzQyxZQUFPLEdBQWtCLElBQUksQ0FBQztRQUM5QiwyQkFBc0IsR0FBRyxDQUFDLENBQUMsQ0FBQztRQUM1QixzQkFBaUIsR0FBRyxLQUFLLENBQUM7Ozs7OztRQWNsQix1QkFBa0IsR0FBRyxJQUFJLENBQUM7UUFHaEMsSUFBSSxDQUFDLHFCQUFxQixHQUFHLFNBQVMsQ0FBQztJQUN6QyxDQUFDOzs7Ozs7SUFFRCxZQUFZLENBQUMsS0FBWSxFQUFFLEtBQVk7UUFDckMsSUFBSSxJQUFJLENBQUMsZ0JBQWdCLENBQUMsS0FBSyxDQUFDLEVBQUU7WUFDaEMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxLQUFLLEVBQUUsS0FBSyxDQUFDLENBQUM7U0FDL0I7SUFDSCxDQUFDOzs7OztJQUVELFVBQVUsQ0FBQyxLQUFZO1FBQ3JCLElBQUksSUFBSSxDQUFDLHFCQUFxQixLQUFLLEtBQUssQ0FBQyxLQUFLLEVBQUU7WUFDOUMsSUFBSSxDQUFDLGtCQUFrQixHQUFHLEtBQUssQ0FBQztTQUNqQztJQUNILENBQUM7Ozs7OztJQUVELFFBQVEsQ0FBQyxLQUFZLEVBQUUsS0FBWSxJQUFVLElBQUksQ0FBQyxZQUFZLENBQUMsS0FBSyxFQUFFLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQzs7Ozs7O0lBRS9FLGFBQWEsQ0FBQyxLQUFZLEVBQUUsZUFBdUI7UUFDakQsSUFBSSxJQUFJLENBQUMsZ0JBQWdCLENBQUMsS0FBSyxDQUFDLEVBQUU7WUFDaEMsSUFBSSxDQUFDLGlCQUFpQixHQUFHLElBQUksQ0FBQztZQUM5QixzRkFBc0Y7WUFDdEYseURBQXlEO1lBQ3pELElBQUksQ0FBQyxRQUFRLENBQUMsQ0FBQyxLQUFLLENBQUMsS0FBSyxFQUFFLGVBQWUsQ0FBQyxDQUFDO1lBQzdDLE9BQU8sSUFBSSxPQUFPLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxDQUFDO1NBQ25DO1FBQ0QsT0FBTyxJQUFJLENBQUM7SUFDZCxDQUFDOzs7Ozs7SUFFTyxnQkFBZ0IsQ0FBQyxLQUFZO1FBQ25DLElBQUksSUFBSSxDQUFDLGtCQUFrQixJQUFJLElBQUksQ0FBQyxRQUFRLENBQUMsV0FBVyxLQUFLLEtBQUssRUFBRTtZQUNsRSxPQUFPLElBQUksQ0FBQyxxQkFBcUIsS0FBSyxDQUFDLEtBQUssQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQyxNQUFNLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO1NBQ2hGO1FBQ0QsT0FBTyxJQUFJLENBQUMsa0JBQWtCLENBQUM7SUFDakMsQ0FBQzs7Ozs7OztJQUVPLFVBQVUsQ0FBQyxLQUFZLEVBQUUsS0FBWTtRQUMzQyxJQUFJLEtBQUssQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxTQUFTLENBQUMsRUFBRTs7a0JBQ3BDLFVBQVUsR0FBRyxtQkFBQSxJQUFJLENBQUMsUUFBUSxDQUFDLFNBQVMsRUFBWTtZQUN0RCxLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsVUFBVSxDQUFDLE1BQU0sRUFBRSxDQUFDLEVBQUUsRUFBRTtnQkFDMUMsSUFBSSxDQUFDLHdCQUF3QixDQUFDLEtBQUssRUFBRSxLQUFLLEVBQUUsd0JBQXdCLENBQUMsS0FBSyxFQUFFLFVBQVUsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7YUFDN0Y7U0FDRjthQUFNOztrQkFDQyxhQUFhLEdBQUcsbUJBQUEsSUFBSSxDQUFDLFFBQVEsQ0FBQyxTQUFTLEVBQU87WUFDcEQsSUFBSSxhQUFhLEtBQUssc0JBQXNCLEVBQUU7Z0JBQzVDLElBQUksS0FBSyxDQUFDLElBQUksc0JBQXdCLEVBQUU7b0JBQ3RDLElBQUksQ0FBQyx3QkFBd0IsQ0FBQyxLQUFLLEVBQUUsS0FBSyxFQUFFLENBQUMsQ0FBQyxDQUFDLENBQUM7aUJBQ2pEO2FBQ0Y7aUJBQU07Z0JBQ0wsSUFBSSxDQUFDLHdCQUF3QixDQUN6QixLQUFLLEVBQUUsS0FBSyxFQUFFLHlCQUF5QixDQUFDLEtBQUssRUFBRSxLQUFLLEVBQUUsYUFBYSxFQUFFLEtBQUssRUFBRSxLQUFLLENBQUMsQ0FBQyxDQUFDO2FBQ3pGO1NBQ0Y7SUFDSCxDQUFDOzs7Ozs7OztJQUVPLHdCQUF3QixDQUFDLEtBQVksRUFBRSxLQUFZLEVBQUUsWUFBeUI7UUFDcEYsSUFBSSxZQUFZLEtBQUssSUFBSSxFQUFFOztrQkFDbkIsSUFBSSxHQUFHLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSTtZQUMvQixJQUFJLElBQUksS0FBSyxJQUFJLEVBQUU7Z0JBQ2pCLElBQUksSUFBSSxLQUFLLHFCQUFxQixJQUFJLElBQUksS0FBSyxnQkFBZ0I7b0JBQzNELElBQUksS0FBSyxzQkFBc0IsSUFBSSxLQUFLLENBQUMsSUFBSSxzQkFBd0IsRUFBRTtvQkFDekUsSUFBSSxDQUFDLFFBQVEsQ0FBQyxLQUFLLENBQUMsS0FBSyxFQUFFLENBQUMsQ0FBQyxDQUFDLENBQUM7aUJBQ2hDO3FCQUFNOzswQkFDQyxzQkFBc0IsR0FDeEIseUJBQXlCLENBQUMsS0FBSyxFQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsS0FBSyxFQUFFLEtBQUssQ0FBQztvQkFDL0QsSUFBSSxzQkFBc0IsS0FBSyxJQUFJLEVBQUU7d0JBQ25DLElBQUksQ0FBQyxRQUFRLENBQUMsS0FBSyxDQUFDLEtBQUssRUFBRSxzQkFBc0IsQ0FBQyxDQUFDO3FCQUNwRDtpQkFDRjthQUNGO2lCQUFNO2dCQUNMLElBQUksQ0FBQyxRQUFRLENBQUMsS0FBSyxDQUFDLEtBQUssRUFBRSxZQUFZLENBQUMsQ0FBQzthQUMxQztTQUNGO0lBQ0gsQ0FBQzs7Ozs7OztJQUVPLFFBQVEsQ0FBQyxRQUFnQixFQUFFLFFBQWdCO1FBQ2pELElBQUksSUFBSSxDQUFDLE9BQU8sS0FBSyxJQUFJLEVBQUU7WUFDekIsSUFBSSxDQUFDLE9BQU8sR0FBRyxDQUFDLFFBQVEsRUFBRSxRQUFRLENBQUMsQ0FBQztTQUNyQzthQUFNO1lBQ0wsSUFBSSxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsUUFBUSxFQUFFLFFBQVEsQ0FBQyxDQUFDO1NBQ3ZDO0lBQ0gsQ0FBQztDQUNGOzs7SUFwR0MsMEJBQThCOztJQUM5Qix5Q0FBNEI7O0lBQzVCLG9DQUEwQjs7Ozs7Ozs7SUFPMUIsd0NBQXNDOzs7Ozs7OztJQU90QyxxQ0FBa0M7O0lBRXRCLDJCQUErQjs7Ozs7Ozs7OztBQTRGN0MsU0FBUyx3QkFBd0IsQ0FBQyxLQUFZLEVBQUUsUUFBZ0I7O1VBQ3hELFVBQVUsR0FBRyxLQUFLLENBQUMsVUFBVTtJQUNuQyxJQUFJLFVBQVUsS0FBSyxJQUFJLEVBQUU7UUFDdkIsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLFVBQVUsQ0FBQyxNQUFNLEVBQUUsQ0FBQyxJQUFJLENBQUMsRUFBRTtZQUM3QyxJQUFJLFVBQVUsQ0FBQyxDQUFDLENBQUMsS0FBSyxRQUFRLEVBQUU7Z0JBQzlCLE9BQU8sbUJBQUEsVUFBVSxDQUFDLENBQUMsR0FBRyxDQUFDLENBQUMsRUFBVSxDQUFDO2FBQ3BDO1NBQ0Y7S0FDRjtJQUNELE9BQU8sSUFBSSxDQUFDO0FBQ2QsQ0FBQzs7Ozs7O0FBR0QsU0FBUyx1QkFBdUIsQ0FBQyxLQUFZLEVBQUUsV0FBa0I7SUFDL0QsSUFBSSxLQUFLLENBQUMsSUFBSSxvQkFBc0IsSUFBSSxLQUFLLENBQUMsSUFBSSw2QkFBK0IsRUFBRTtRQUNqRixPQUFPLGdCQUFnQixDQUFDLHFCQUFxQixFQUFFLEtBQUssRUFBRSxXQUFXLENBQUMsQ0FBQztLQUNwRTtTQUFNLElBQUksS0FBSyxDQUFDLElBQUksc0JBQXdCLEVBQUU7UUFDN0MsT0FBTyxpQkFBaUIsQ0FBQyxzQkFBc0IsRUFBRSxxQkFBcUIsRUFBRSxLQUFLLEVBQUUsV0FBVyxDQUFDLENBQUM7S0FDN0Y7SUFDRCxPQUFPLElBQUksQ0FBQztBQUNkLENBQUM7Ozs7Ozs7O0FBR0QsU0FBUyxtQkFBbUIsQ0FBQyxLQUFZLEVBQUUsS0FBWSxFQUFFLFdBQW1CLEVBQUUsSUFBUztJQUNyRixJQUFJLFdBQVcsS0FBSyxDQUFDLENBQUMsRUFBRTtRQUN0QiwyRkFBMkY7UUFDM0YsT0FBTyx1QkFBdUIsQ0FBQyxLQUFLLEVBQUUsS0FBSyxDQUFDLENBQUM7S0FDOUM7U0FBTSxJQUFJLFdBQVcsS0FBSyxDQUFDLENBQUMsRUFBRTtRQUM3Qiw0Q0FBNEM7UUFDNUMsT0FBTyxrQkFBa0IsQ0FBQyxLQUFLLEVBQUUsS0FBSyxFQUFFLElBQUksQ0FBQyxDQUFDO0tBQy9DO1NBQU07UUFDTCxlQUFlO1FBQ2YsT0FBTyxpQkFBaUIsQ0FBQyxLQUFLLENBQUMsS0FBSyxDQUFDLENBQUMsSUFBSSxFQUFFLEtBQUssRUFBRSxXQUFXLEVBQUUsbUJBQUEsS0FBSyxFQUFnQixDQUFDLENBQUM7S0FDeEY7QUFDSCxDQUFDOzs7Ozs7O0FBRUQsU0FBUyxrQkFBa0IsQ0FBQyxLQUFZLEVBQUUsS0FBWSxFQUFFLElBQVM7SUFDL0QsSUFBSSxJQUFJLEtBQUsscUJBQXFCLEVBQUU7UUFDbEMsT0FBTyxnQkFBZ0IsQ0FBQyxxQkFBcUIsRUFBRSxLQUFLLEVBQUUsS0FBSyxDQUFDLENBQUM7S0FDOUQ7U0FBTSxJQUFJLElBQUksS0FBSyxzQkFBc0IsRUFBRTtRQUMxQyxPQUFPLGlCQUFpQixDQUFDLHNCQUFzQixFQUFFLHFCQUFxQixFQUFFLEtBQUssRUFBRSxLQUFLLENBQUMsQ0FBQztLQUN2RjtTQUFNLElBQUksSUFBSSxLQUFLLGdCQUFnQixFQUFFO1FBQ3BDLFNBQVMsSUFBSSx5QkFBeUIsQ0FDckIsS0FBSywrREFBcUUsQ0FBQztRQUM1RixPQUFPLGtCQUFrQixDQUNyQixnQkFBZ0IsRUFBRSxxQkFBcUIsRUFDdkMsbUJBQUEsS0FBSyxFQUF5RCxFQUFFLEtBQUssQ0FBQyxDQUFDO0tBQzVFO1NBQU07UUFDTCxTQUFTO1lBQ0wsVUFBVSxDQUNOLDhGQUE4RixTQUFTLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQyxDQUFDO0tBQzNIO0FBQ0gsQ0FBQzs7Ozs7Ozs7Ozs7QUFPRCxTQUFTLHNCQUFzQixDQUFJLEtBQVksRUFBRSxNQUFjLEVBQUUsVUFBa0I7O1VBQzNFLE1BQU0sR0FBRyxtQkFBQSxtQkFBQSxLQUFLLENBQUMsT0FBTyxDQUFDLEVBQUUsQ0FBQyxPQUFPLEVBQUUsQ0FBQyxVQUFVLENBQUM7SUFDckQsSUFBSSxNQUFNLENBQUMsT0FBTyxLQUFLLElBQUksRUFBRTs7Y0FDckIsU0FBUyxHQUFHLEtBQUssQ0FBQyxLQUFLLENBQUMsQ0FBQyxJQUFJOztjQUM3QixhQUFhLEdBQUcsbUJBQUEsTUFBTSxDQUFDLE9BQU8sRUFBRTs7Y0FDaEMsTUFBTSxHQUFhLElBQUksS0FBSyxDQUFDLGFBQWEsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxDQUFDO1FBQzVELEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxhQUFhLENBQUMsTUFBTSxFQUFFLENBQUMsSUFBSSxDQUFDLEVBQUU7O2tCQUMxQyxjQUFjLEdBQUcsYUFBYSxDQUFDLENBQUMsQ0FBQztZQUN2QyxJQUFJLGNBQWMsR0FBRyxDQUFDLEVBQUU7Z0JBQ3RCLHlGQUF5RjtnQkFDekYsd0ZBQXdGO2dCQUN4Rix3QkFBd0I7Z0JBQ3hCLE1BQU0sQ0FBQyxDQUFDLEdBQUcsQ0FBQyxDQUFDLEdBQUcsSUFBSSxDQUFDO2FBQ3RCO2lCQUFNO2dCQUNMLFNBQVMsSUFBSSxpQkFBaUIsQ0FBQyxTQUFTLEVBQUUsY0FBYyxDQUFDLENBQUM7O3NCQUNwRCxLQUFLLEdBQUcsbUJBQUEsU0FBUyxDQUFDLGNBQWMsQ0FBQyxFQUFTO2dCQUNoRCxNQUFNLENBQUMsQ0FBQyxHQUFHLENBQUMsQ0FBQztvQkFDVCxtQkFBbUIsQ0FBQyxLQUFLLEVBQUUsS0FBSyxFQUFFLGFBQWEsQ0FBQyxDQUFDLEdBQUcsQ0FBQyxDQUFDLEVBQUUsTUFBTSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUMsQ0FBQzthQUNuRjtTQUNGO1FBQ0QsTUFBTSxDQUFDLE9BQU8sR0FBRyxNQUFNLENBQUM7S0FDekI7SUFFRCxPQUFPLE1BQU0sQ0FBQyxPQUFPLENBQUM7QUFDeEIsQ0FBQzs7Ozs7Ozs7OztBQU1ELFNBQVMsbUJBQW1CLENBQUksS0FBWSxFQUFFLFVBQWtCLEVBQUUsTUFBVzs7VUFDckUsTUFBTSxHQUFHLG1CQUFBLEtBQUssQ0FBQyxLQUFLLENBQUMsQ0FBQyxPQUFPLEVBQUUsQ0FBQyxVQUFVLENBQUMsVUFBVSxDQUFDOztVQUN0RCxhQUFhLEdBQUcsTUFBTSxDQUFDLE9BQU87SUFDcEMsSUFBSSxhQUFhLEtBQUssSUFBSSxFQUFFOztjQUNwQixZQUFZLEdBQUcsc0JBQXNCLENBQUksS0FBSyxFQUFFLE1BQU0sRUFBRSxVQUFVLENBQUM7UUFFekUsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLGFBQWEsQ0FBQyxNQUFNLEVBQUUsQ0FBQyxJQUFJLENBQUMsRUFBRTs7a0JBQzFDLFFBQVEsR0FBRyxhQUFhLENBQUMsQ0FBQyxDQUFDO1lBQ2pDLElBQUksUUFBUSxHQUFHLENBQUMsRUFBRTs7c0JBQ1YsVUFBVSxHQUFHLFlBQVksQ0FBQyxDQUFDLEdBQUcsQ0FBQyxDQUFDO2dCQUN0QyxTQUFTLElBQUksYUFBYSxDQUFDLFVBQVUsRUFBRSw2Q0FBNkMsQ0FBQyxDQUFDO2dCQUN0RixNQUFNLENBQUMsSUFBSSxDQUFDLG1CQUFBLFVBQVUsRUFBSyxDQUFDLENBQUM7YUFDOUI7aUJBQU07O3NCQUNDLGVBQWUsR0FBRyxhQUFhLENBQUMsQ0FBQyxHQUFHLENBQUMsQ0FBQzs7c0JBRXRDLHFCQUFxQixHQUFHLG1CQUFBLEtBQUssQ0FBQyxDQUFDLFFBQVEsQ0FBQyxFQUFjO2dCQUM1RCxTQUFTLElBQUksZ0JBQWdCLENBQUMscUJBQXFCLENBQUMsQ0FBQztnQkFFckQsdURBQXVEO2dCQUN2RCxLQUFLLElBQUksQ0FBQyxHQUFHLHVCQUF1QixFQUFFLENBQUMsR0FBRyxxQkFBcUIsQ0FBQyxNQUFNLEVBQUUsQ0FBQyxFQUFFLEVBQUU7OzBCQUNyRSxhQUFhLEdBQUcscUJBQXFCLENBQUMsQ0FBQyxDQUFDO29CQUM5QyxJQUFJLGFBQWEsQ0FBQyxzQkFBc0IsQ0FBQyxLQUFLLGFBQWEsQ0FBQyxNQUFNLENBQUMsRUFBRTt3QkFDbkUsbUJBQW1CLENBQUMsYUFBYSxFQUFFLGVBQWUsRUFBRSxNQUFNLENBQUMsQ0FBQztxQkFDN0Q7aUJBQ0Y7Z0JBRUQsc0ZBQXNGO2dCQUN0Rix1QkFBdUI7Z0JBQ3ZCLElBQUkscUJBQXFCLENBQUMsV0FBVyxDQUFDLEtBQUssSUFBSSxFQUFFO29CQUMvQyxLQUFLLElBQUksYUFBYSxJQUFJLG1CQUFBLHFCQUFxQixDQUFDLFdBQVcsQ0FBQyxFQUFFLEVBQUU7d0JBQzlELG1CQUFtQixDQUFDLGFBQWEsRUFBRSxlQUFlLEVBQUUsTUFBTSxDQUFDLENBQUM7cUJBQzdEO2lCQUNGO2FBQ0Y7U0FDRjtLQUNGO0lBQ0QsT0FBTyxNQUFNLENBQUM7QUFDaEIsQ0FBQzs7Ozs7Ozs7Ozs7QUFXRCxNQUFNLFVBQVUsY0FBYyxDQUFDLFNBQXlCOztVQUNoRCxLQUFLLEdBQUcsUUFBUSxFQUFFOztVQUNsQixVQUFVLEdBQUcsb0JBQW9CLEVBQUU7SUFFekMsb0JBQW9CLENBQUMsVUFBVSxHQUFHLENBQUMsQ0FBQyxDQUFDOztVQUUvQixNQUFNLEdBQUcsU0FBUyxDQUFDLEtBQUssQ0FBQyxLQUFLLENBQUMsRUFBRSxVQUFVLENBQUM7SUFDbEQsSUFBSSxTQUFTLENBQUMsS0FBSyxJQUFJLENBQUMsY0FBYyxFQUFFLEtBQUssTUFBTSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQUMsRUFBRTtRQUN0RSxJQUFJLE1BQU0sQ0FBQyxPQUFPLEtBQUssSUFBSSxFQUFFO1lBQzNCLFNBQVMsQ0FBQyxLQUFLLENBQUMsRUFBRSxDQUFDLENBQUM7U0FDckI7YUFBTTs7a0JBQ0MsTUFBTSxHQUFHLE1BQU0sQ0FBQyxpQkFBaUIsQ0FBQyxDQUFDLENBQUMsbUJBQW1CLENBQUMsS0FBSyxFQUFFLFVBQVUsRUFBRSxFQUFFLENBQUMsQ0FBQyxDQUFDO2dCQUM1QyxzQkFBc0IsQ0FBQyxLQUFLLEVBQUUsTUFBTSxFQUFFLFVBQVUsQ0FBQztZQUMzRixTQUFTLENBQUMsS0FBSyxDQUFDLE1BQU0sQ0FBQyxDQUFDO1lBQ3hCLFNBQVMsQ0FBQyxlQUFlLEVBQUUsQ0FBQztTQUM3QjtRQUNELE9BQU8sSUFBSSxDQUFDO0tBQ2I7SUFFRCxPQUFPLEtBQUssQ0FBQztBQUNmLENBQUM7Ozs7Ozs7Ozs7OztBQVdELE1BQU0sVUFBVSxpQkFBaUIsQ0FDN0IsU0FBOEIsRUFBRSxPQUFnQixFQUFFLElBQVU7SUFDOUQsaUJBQWlCLENBQUMsUUFBUSxFQUFFLEVBQUUsU0FBUyxFQUFFLE9BQU8sRUFBRSxJQUFJLEVBQUUsSUFBSSxDQUFDLENBQUM7QUFDaEUsQ0FBQzs7Ozs7Ozs7Ozs7O0FBV0QsTUFBTSxVQUFVLFdBQVcsQ0FBSSxTQUE4QixFQUFFLE9BQWdCLEVBQUUsSUFBVTtJQUN6RixpQkFBaUIsQ0FBQyxRQUFRLEVBQUUsRUFBRSxTQUFTLEVBQUUsT0FBTyxFQUFFLElBQUksRUFBRSxLQUFLLENBQUMsQ0FBQztBQUNqRSxDQUFDOzs7Ozs7Ozs7O0FBRUQsU0FBUyxpQkFBaUIsQ0FDdEIsS0FBWSxFQUFFLFNBQThCLEVBQUUsT0FBZ0IsRUFBRSxJQUFTLEVBQ3pFLFFBQWlCOztVQUNiLEtBQUssR0FBRyxLQUFLLENBQUMsS0FBSyxDQUFDO0lBQzFCLElBQUksS0FBSyxDQUFDLGlCQUFpQixFQUFFO1FBQzNCLFlBQVksQ0FBQyxLQUFLLEVBQUUsSUFBSSxlQUFlLENBQUMsU0FBUyxFQUFFLE9BQU8sRUFBRSxRQUFRLEVBQUUsSUFBSSxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUNqRixJQUFJLFFBQVEsRUFBRTtZQUNaLEtBQUssQ0FBQyxpQkFBaUIsR0FBRyxJQUFJLENBQUM7U0FDaEM7S0FDRjtJQUNELFlBQVksQ0FBSSxLQUFLLENBQUMsQ0FBQztBQUN6QixDQUFDOzs7Ozs7OztBQU9ELE1BQU0sVUFBVSxlQUFlO0lBQzdCLE9BQU8saUJBQWlCLENBQUksUUFBUSxFQUFFLEVBQUUsb0JBQW9CLEVBQUUsQ0FBQyxDQUFDO0FBQ2xFLENBQUM7Ozs7Ozs7Ozs7Ozs7O0FBY0QsTUFBTSxVQUFVLGNBQWMsQ0FDMUIsY0FBc0IsRUFBRSxTQUE4QixFQUFFLE9BQWdCLEVBQUUsSUFBVTtJQUN0RixvQkFBb0IsQ0FDaEIsUUFBUSxFQUFFLEVBQUUsU0FBUyxFQUFFLE9BQU8sRUFBRSxJQUFJLEVBQUUsS0FBSyxFQUFFLHdCQUF3QixFQUFFLEVBQUUsY0FBYyxDQUFDLENBQUM7QUFDL0YsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7QUFjRCxNQUFNLFVBQVUsb0JBQW9CLENBQ2hDLGNBQXNCLEVBQUUsU0FBOEIsRUFBRSxPQUFnQixFQUFFLElBQVU7SUFDdEYsb0JBQW9CLENBQ2hCLFFBQVEsRUFBRSxFQUFFLFNBQVMsRUFBRSxPQUFPLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSx3QkFBd0IsRUFBRSxFQUFFLGNBQWMsQ0FBQyxDQUFDO0FBQzlGLENBQUM7Ozs7Ozs7Ozs7OztBQUVELFNBQVMsb0JBQW9CLENBQ3pCLEtBQVksRUFBRSxTQUE4QixFQUFFLE9BQWdCLEVBQUUsSUFBUyxFQUFFLFFBQWlCLEVBQzVGLEtBQVksRUFBRSxjQUFzQjs7VUFDaEMsS0FBSyxHQUFHLEtBQUssQ0FBQyxLQUFLLENBQUM7SUFDMUIsSUFBSSxLQUFLLENBQUMsaUJBQWlCLEVBQUU7UUFDM0IsWUFBWSxDQUFDLEtBQUssRUFBRSxJQUFJLGVBQWUsQ0FBQyxTQUFTLEVBQUUsT0FBTyxFQUFFLFFBQVEsRUFBRSxJQUFJLENBQUMsRUFBRSxLQUFLLENBQUMsS0FBSyxDQUFDLENBQUM7UUFDMUYsaUNBQWlDLENBQUMsS0FBSyxFQUFFLGNBQWMsQ0FBQyxDQUFDO1FBQ3pELElBQUksUUFBUSxFQUFFO1lBQ1osS0FBSyxDQUFDLG9CQUFvQixHQUFHLElBQUksQ0FBQztTQUNuQztLQUNGO0lBRUQsWUFBWSxDQUFJLEtBQUssQ0FBQyxDQUFDO0FBQ3pCLENBQUM7Ozs7Ozs7O0FBT0QsTUFBTSxVQUFVLGtCQUFrQjtJQUNoQyxPQUFPLGlCQUFpQixDQUFJLFFBQVEsRUFBRSxFQUFFLG9CQUFvQixFQUFFLENBQUMsQ0FBQztBQUNsRSxDQUFDOzs7Ozs7O0FBRUQsU0FBUyxpQkFBaUIsQ0FBSSxLQUFZLEVBQUUsVUFBa0I7SUFDNUQsU0FBUztRQUNMLGFBQWEsQ0FBQyxLQUFLLENBQUMsT0FBTyxDQUFDLEVBQUUsd0RBQXdELENBQUMsQ0FBQztJQUM1RixTQUFTLElBQUksaUJBQWlCLENBQUMsbUJBQUEsS0FBSyxDQUFDLE9BQU8sQ0FBQyxFQUFFLENBQUMsT0FBTyxFQUFFLFVBQVUsQ0FBQyxDQUFDO0lBQ3JFLE9BQU8sbUJBQUEsS0FBSyxDQUFDLE9BQU8sQ0FBQyxFQUFFLENBQUMsT0FBTyxDQUFDLFVBQVUsQ0FBQyxDQUFDLFNBQVMsQ0FBQztBQUN4RCxDQUFDOzs7Ozs7QUFFRCxTQUFTLFlBQVksQ0FBSSxLQUFZOztVQUM3QixTQUFTLEdBQUcsSUFBSSxTQUFTLEVBQUs7SUFDcEMsdUJBQXVCLENBQUMsS0FBSyxFQUFFLFNBQVMsRUFBRSxTQUFTLENBQUMsT0FBTyxDQUFDLENBQUM7SUFFN0QsSUFBSSxLQUFLLENBQUMsT0FBTyxDQUFDLEtBQUssSUFBSTtRQUFFLEtBQUssQ0FBQyxPQUFPLENBQUMsR0FBRyxJQUFJLFNBQVMsRUFBRSxDQUFDO0lBQzlELG1CQUFBLEtBQUssQ0FBQyxPQUFPLENBQUMsRUFBRSxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsSUFBSSxPQUFPLENBQUMsU0FBUyxDQUFDLENBQUMsQ0FBQztBQUN4RCxDQUFDOzs7Ozs7O0FBRUQsU0FBUyxZQUFZLENBQUMsS0FBWSxFQUFFLFFBQXdCLEVBQUUsU0FBaUI7SUFDN0UsSUFBSSxLQUFLLENBQUMsT0FBTyxLQUFLLElBQUk7UUFBRSxLQUFLLENBQUMsT0FBTyxHQUFHLElBQUksU0FBUyxFQUFFLENBQUM7SUFDNUQsS0FBSyxDQUFDLE9BQU8sQ0FBQyxLQUFLLENBQUMsSUFBSSxPQUFPLENBQUMsUUFBUSxFQUFFLFNBQVMsQ0FBQyxDQUFDLENBQUM7QUFDeEQsQ0FBQzs7Ozs7O0FBRUQsU0FBUyxpQ0FBaUMsQ0FBQyxLQUFZLEVBQUUsY0FBc0I7O1VBQ3ZFLG1CQUFtQixHQUFHLEtBQUssQ0FBQyxjQUFjLElBQUksQ0FBQyxLQUFLLENBQUMsY0FBYyxHQUFHLEVBQUUsQ0FBQzs7VUFDekUsdUJBQXVCLEdBQ3pCLEtBQUssQ0FBQyxjQUFjLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxtQkFBbUIsQ0FBQyxtQkFBbUIsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztJQUMxRixJQUFJLGNBQWMsS0FBSyx1QkFBdUIsRUFBRTtRQUM5QyxtQkFBbUIsQ0FBQyxJQUFJLENBQUMsbUJBQUEsS0FBSyxDQUFDLE9BQU8sRUFBRSxDQUFDLE1BQU0sR0FBRyxDQUFDLEVBQUUsY0FBYyxDQUFDLENBQUM7S0FDdEU7QUFDSCxDQUFDOzs7Ozs7QUFFRCxTQUFTLFNBQVMsQ0FBQyxLQUFZLEVBQUUsS0FBYTtJQUM1QyxTQUFTLElBQUksYUFBYSxDQUFDLEtBQUssQ0FBQyxPQUFPLEVBQUUsK0NBQStDLENBQUMsQ0FBQztJQUMzRixPQUFPLG1CQUFBLEtBQUssQ0FBQyxPQUFPLEVBQUUsQ0FBQyxVQUFVLENBQUMsS0FBSyxDQUFDLENBQUM7QUFDM0MsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLy8gV2UgYXJlIHRlbXBvcmFyaWx5IGltcG9ydGluZyB0aGUgZXhpc3Rpbmcgdmlld0VuZ2luZV9mcm9tIGNvcmUgc28gd2UgY2FuIGJlIHN1cmUgd2UgYXJlXG4vLyBjb3JyZWN0bHkgaW1wbGVtZW50aW5nIGl0cyBpbnRlcmZhY2VzIGZvciBiYWNrd2FyZHMgY29tcGF0aWJpbGl0eS5cblxuaW1wb3J0IHtUeXBlfSBmcm9tICcuLi9pbnRlcmZhY2UvdHlwZSc7XG5pbXBvcnQge0VsZW1lbnRSZWYgYXMgVmlld0VuZ2luZV9FbGVtZW50UmVmfSBmcm9tICcuLi9saW5rZXIvZWxlbWVudF9yZWYnO1xuaW1wb3J0IHtRdWVyeUxpc3R9IGZyb20gJy4uL2xpbmtlci9xdWVyeV9saXN0JztcbmltcG9ydCB7VGVtcGxhdGVSZWYgYXMgVmlld0VuZ2luZV9UZW1wbGF0ZVJlZn0gZnJvbSAnLi4vbGlua2VyL3RlbXBsYXRlX3JlZic7XG5pbXBvcnQge1ZpZXdDb250YWluZXJSZWZ9IGZyb20gJy4uL2xpbmtlci92aWV3X2NvbnRhaW5lcl9yZWYnO1xuaW1wb3J0IHthc3NlcnREYXRhSW5SYW5nZSwgYXNzZXJ0RGVmaW5lZCwgdGhyb3dFcnJvcn0gZnJvbSAnLi4vdXRpbC9hc3NlcnQnO1xuaW1wb3J0IHtzdHJpbmdpZnl9IGZyb20gJy4uL3V0aWwvc3RyaW5naWZ5JztcblxuaW1wb3J0IHthc3NlcnRGaXJzdFRlbXBsYXRlUGFzcywgYXNzZXJ0TENvbnRhaW5lcn0gZnJvbSAnLi9hc3NlcnQnO1xuaW1wb3J0IHtnZXROb2RlSW5qZWN0YWJsZSwgbG9jYXRlRGlyZWN0aXZlT3JQcm92aWRlcn0gZnJvbSAnLi9kaSc7XG5pbXBvcnQge3N0b3JlQ2xlYW51cFdpdGhDb250ZXh0fSBmcm9tICcuL2luc3RydWN0aW9ucy9zaGFyZWQnO1xuaW1wb3J0IHtDT05UQUlORVJfSEVBREVSX09GRlNFVCwgTENvbnRhaW5lciwgTU9WRURfVklFV1N9IGZyb20gJy4vaW50ZXJmYWNlcy9jb250YWluZXInO1xuaW1wb3J0IHt1bnVzZWRWYWx1ZUV4cG9ydFRvUGxhY2F0ZUFqZCBhcyB1bnVzZWQxfSBmcm9tICcuL2ludGVyZmFjZXMvZGVmaW5pdGlvbic7XG5pbXBvcnQge3VudXNlZFZhbHVlRXhwb3J0VG9QbGFjYXRlQWpkIGFzIHVudXNlZDJ9IGZyb20gJy4vaW50ZXJmYWNlcy9pbmplY3Rvcic7XG5pbXBvcnQge1RDb250YWluZXJOb2RlLCBURWxlbWVudENvbnRhaW5lck5vZGUsIFRFbGVtZW50Tm9kZSwgVE5vZGUsIFROb2RlVHlwZSwgdW51c2VkVmFsdWVFeHBvcnRUb1BsYWNhdGVBamQgYXMgdW51c2VkM30gZnJvbSAnLi9pbnRlcmZhY2VzL25vZGUnO1xuaW1wb3J0IHtMUXVlcmllcywgTFF1ZXJ5LCBUUXVlcmllcywgVFF1ZXJ5LCBUUXVlcnlNZXRhZGF0YSwgdW51c2VkVmFsdWVFeHBvcnRUb1BsYWNhdGVBamQgYXMgdW51c2VkNH0gZnJvbSAnLi9pbnRlcmZhY2VzL3F1ZXJ5JztcbmltcG9ydCB7REVDTEFSQVRJT05fTENPTlRBSU5FUiwgTFZpZXcsIFBBUkVOVCwgUVVFUklFUywgVFZJRVcsIFRWaWV3fSBmcm9tICcuL2ludGVyZmFjZXMvdmlldyc7XG5pbXBvcnQge2Fzc2VydE5vZGVPZlBvc3NpYmxlVHlwZXN9IGZyb20gJy4vbm9kZV9hc3NlcnQnO1xuaW1wb3J0IHtnZXRDdXJyZW50UXVlcnlJbmRleCwgZ2V0TFZpZXcsIGdldFByZXZpb3VzT3JQYXJlbnRUTm9kZSwgaXNDcmVhdGlvbk1vZGUsIHNldEN1cnJlbnRRdWVyeUluZGV4fSBmcm9tICcuL3N0YXRlJztcbmltcG9ydCB7Y3JlYXRlQ29udGFpbmVyUmVmLCBjcmVhdGVFbGVtZW50UmVmLCBjcmVhdGVUZW1wbGF0ZVJlZn0gZnJvbSAnLi92aWV3X2VuZ2luZV9jb21wYXRpYmlsaXR5JztcblxuY29uc3QgdW51c2VkVmFsdWVUb1BsYWNhdGVBamQgPSB1bnVzZWQxICsgdW51c2VkMiArIHVudXNlZDMgKyB1bnVzZWQ0O1xuXG5jbGFzcyBMUXVlcnlfPFQ+IGltcGxlbWVudHMgTFF1ZXJ5PFQ+IHtcbiAgbWF0Y2hlczogKFR8bnVsbClbXXxudWxsID0gbnVsbDtcbiAgY29uc3RydWN0b3IocHVibGljIHF1ZXJ5TGlzdDogUXVlcnlMaXN0PFQ+KSB7fVxuICBjbG9uZSgpOiBMUXVlcnk8VD4geyByZXR1cm4gbmV3IExRdWVyeV8odGhpcy5xdWVyeUxpc3QpOyB9XG4gIHNldERpcnR5KCk6IHZvaWQgeyB0aGlzLnF1ZXJ5TGlzdC5zZXREaXJ0eSgpOyB9XG59XG5cbmNsYXNzIExRdWVyaWVzXyBpbXBsZW1lbnRzIExRdWVyaWVzIHtcbiAgY29uc3RydWN0b3IocHVibGljIHF1ZXJpZXM6IExRdWVyeTxhbnk+W10gPSBbXSkge31cblxuICBjcmVhdGVFbWJlZGRlZFZpZXcodFZpZXc6IFRWaWV3KTogTFF1ZXJpZXN8bnVsbCB7XG4gICAgY29uc3QgdFF1ZXJpZXMgPSB0Vmlldy5xdWVyaWVzO1xuICAgIGlmICh0UXVlcmllcyAhPT0gbnVsbCkge1xuICAgICAgY29uc3Qgbm9PZkluaGVyaXRlZFF1ZXJpZXMgPVxuICAgICAgICAgIHRWaWV3LmNvbnRlbnRRdWVyaWVzICE9PSBudWxsID8gdFZpZXcuY29udGVudFF1ZXJpZXNbMF0gOiB0UXVlcmllcy5sZW5ndGg7XG4gICAgICBjb25zdCB2aWV3TFF1ZXJpZXM6IExRdWVyeTxhbnk+W10gPSBuZXcgQXJyYXkobm9PZkluaGVyaXRlZFF1ZXJpZXMpO1xuXG4gICAgICAvLyBBbiBlbWJlZGRlZCB2aWV3IGhhcyBxdWVyaWVzIHByb3BhZ2F0ZWQgZnJvbSBhIGRlY2xhcmF0aW9uIHZpZXcgYXQgdGhlIGJlZ2lubmluZyBvZiB0aGVcbiAgICAgIC8vIFRRdWVyaWVzIGNvbGxlY3Rpb24gYW5kIHVwIHVudGlsIGEgZmlyc3QgY29udGVudCBxdWVyeSBkZWNsYXJlZCBpbiB0aGUgZW1iZWRkZWQgdmlldy4gT25seVxuICAgICAgLy8gcHJvcGFnYXRlZCBMUXVlcmllcyBhcmUgY3JlYXRlZCBhdCB0aGlzIHBvaW50IChMUXVlcnkgY29ycmVzcG9uZGluZyB0byBkZWNsYXJlZCBjb250ZW50XG4gICAgICAvLyBxdWVyaWVzIHdpbGwgYmUgaW5zdGFudGlhdGVkIGZyb20gdGhlIGNvbnRlbnQgcXVlcnkgaW5zdHJ1Y3Rpb25zIGZvciBlYWNoIGRpcmVjdGl2ZSkuXG4gICAgICBmb3IgKGxldCBpID0gMDsgaSA8IG5vT2ZJbmhlcml0ZWRRdWVyaWVzOyBpKyspIHtcbiAgICAgICAgY29uc3QgdFF1ZXJ5ID0gdFF1ZXJpZXMuZ2V0QnlJbmRleChpKTtcbiAgICAgICAgY29uc3QgcGFyZW50TFF1ZXJ5ID0gdGhpcy5xdWVyaWVzW3RRdWVyeS5pbmRleEluRGVjbGFyYXRpb25WaWV3XTtcbiAgICAgICAgdmlld0xRdWVyaWVzW2ldID0gcGFyZW50TFF1ZXJ5LmNsb25lKCk7XG4gICAgICB9XG5cbiAgICAgIHJldHVybiBuZXcgTFF1ZXJpZXNfKHZpZXdMUXVlcmllcyk7XG4gICAgfVxuXG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICBpbnNlcnRWaWV3KHRWaWV3OiBUVmlldyk6IHZvaWQgeyB0aGlzLmRpcnR5UXVlcmllc1dpdGhNYXRjaGVzKHRWaWV3KTsgfVxuXG4gIGRldGFjaFZpZXcodFZpZXc6IFRWaWV3KTogdm9pZCB7IHRoaXMuZGlydHlRdWVyaWVzV2l0aE1hdGNoZXModFZpZXcpOyB9XG5cbiAgcHJpdmF0ZSBkaXJ0eVF1ZXJpZXNXaXRoTWF0Y2hlcyh0VmlldzogVFZpZXcpIHtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHRoaXMucXVlcmllcy5sZW5ndGg7IGkrKykge1xuICAgICAgaWYgKGdldFRRdWVyeSh0VmlldywgaSkubWF0Y2hlcyAhPT0gbnVsbCkge1xuICAgICAgICB0aGlzLnF1ZXJpZXNbaV0uc2V0RGlydHkoKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cbn1cblxuY2xhc3MgVFF1ZXJ5TWV0YWRhdGFfIGltcGxlbWVudHMgVFF1ZXJ5TWV0YWRhdGEge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyBwcmVkaWNhdGU6IFR5cGU8YW55PnxzdHJpbmdbXSwgcHVibGljIGRlc2NlbmRhbnRzOiBib29sZWFuLCBwdWJsaWMgaXNTdGF0aWM6IGJvb2xlYW4sXG4gICAgICBwdWJsaWMgcmVhZDogYW55ID0gbnVsbCkge31cbn1cblxuY2xhc3MgVFF1ZXJpZXNfIGltcGxlbWVudHMgVFF1ZXJpZXMge1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIHF1ZXJpZXM6IFRRdWVyeVtdID0gW10pIHt9XG5cbiAgZWxlbWVudFN0YXJ0KHRWaWV3OiBUVmlldywgdE5vZGU6IFROb2RlKTogdm9pZCB7XG4gICAgbmdEZXZNb2RlICYmIGFzc2VydEZpcnN0VGVtcGxhdGVQYXNzKFxuICAgICAgICAgICAgICAgICAgICAgdFZpZXcsICdRdWVyaWVzIHNob3VsZCBjb2xsZWN0IHJlc3VsdHMgb24gdGhlIGZpcnN0IHRlbXBsYXRlIHBhc3Mgb25seScpO1xuICAgIGZvciAobGV0IHF1ZXJ5IG9mIHRoaXMucXVlcmllcykge1xuICAgICAgcXVlcnkuZWxlbWVudFN0YXJ0KHRWaWV3LCB0Tm9kZSk7XG4gICAgfVxuICB9XG4gIGVsZW1lbnRFbmQodE5vZGU6IFROb2RlKTogdm9pZCB7XG4gICAgZm9yIChsZXQgcXVlcnkgb2YgdGhpcy5xdWVyaWVzKSB7XG4gICAgICBxdWVyeS5lbGVtZW50RW5kKHROb2RlKTtcbiAgICB9XG4gIH1cbiAgZW1iZWRkZWRUVmlldyh0Tm9kZTogVE5vZGUpOiBUUXVlcmllc3xudWxsIHtcbiAgICBsZXQgcXVlcmllc0ZvclRlbXBsYXRlUmVmOiBUUXVlcnlbXXxudWxsID0gbnVsbDtcblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgdGhpcy5sZW5ndGg7IGkrKykge1xuICAgICAgY29uc3QgY2hpbGRRdWVyeUluZGV4ID0gcXVlcmllc0ZvclRlbXBsYXRlUmVmICE9PSBudWxsID8gcXVlcmllc0ZvclRlbXBsYXRlUmVmLmxlbmd0aCA6IDA7XG4gICAgICBjb25zdCB0cXVlcnlDbG9uZSA9IHRoaXMuZ2V0QnlJbmRleChpKS5lbWJlZGRlZFRWaWV3KHROb2RlLCBjaGlsZFF1ZXJ5SW5kZXgpO1xuXG4gICAgICBpZiAodHF1ZXJ5Q2xvbmUpIHtcbiAgICAgICAgdHF1ZXJ5Q2xvbmUuaW5kZXhJbkRlY2xhcmF0aW9uVmlldyA9IGk7XG4gICAgICAgIGlmIChxdWVyaWVzRm9yVGVtcGxhdGVSZWYgIT09IG51bGwpIHtcbiAgICAgICAgICBxdWVyaWVzRm9yVGVtcGxhdGVSZWYucHVzaCh0cXVlcnlDbG9uZSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcXVlcmllc0ZvclRlbXBsYXRlUmVmID0gW3RxdWVyeUNsb25lXTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIHJldHVybiBxdWVyaWVzRm9yVGVtcGxhdGVSZWYgIT09IG51bGwgPyBuZXcgVFF1ZXJpZXNfKHF1ZXJpZXNGb3JUZW1wbGF0ZVJlZikgOiBudWxsO1xuICB9XG5cbiAgdGVtcGxhdGUodFZpZXc6IFRWaWV3LCB0Tm9kZTogVE5vZGUpOiB2b2lkIHtcbiAgICBuZ0Rldk1vZGUgJiYgYXNzZXJ0Rmlyc3RUZW1wbGF0ZVBhc3MoXG4gICAgICAgICAgICAgICAgICAgICB0VmlldywgJ1F1ZXJpZXMgc2hvdWxkIGNvbGxlY3QgcmVzdWx0cyBvbiB0aGUgZmlyc3QgdGVtcGxhdGUgcGFzcyBvbmx5Jyk7XG4gICAgZm9yIChsZXQgcXVlcnkgb2YgdGhpcy5xdWVyaWVzKSB7XG4gICAgICBxdWVyeS50ZW1wbGF0ZSh0VmlldywgdE5vZGUpO1xuICAgIH1cbiAgfVxuXG4gIGdldEJ5SW5kZXgoaW5kZXg6IG51bWJlcik6IFRRdWVyeSB7XG4gICAgbmdEZXZNb2RlICYmIGFzc2VydERhdGFJblJhbmdlKHRoaXMucXVlcmllcywgaW5kZXgpO1xuICAgIHJldHVybiB0aGlzLnF1ZXJpZXNbaW5kZXhdO1xuICB9XG5cbiAgZ2V0IGxlbmd0aCgpOiBudW1iZXIgeyByZXR1cm4gdGhpcy5xdWVyaWVzLmxlbmd0aDsgfVxuXG4gIHRyYWNrKHRxdWVyeTogVFF1ZXJ5KTogdm9pZCB7IHRoaXMucXVlcmllcy5wdXNoKHRxdWVyeSk7IH1cbn1cblxuY2xhc3MgVFF1ZXJ5XyBpbXBsZW1lbnRzIFRRdWVyeSB7XG4gIG1hdGNoZXM6IG51bWJlcltdfG51bGwgPSBudWxsO1xuICBpbmRleEluRGVjbGFyYXRpb25WaWV3ID0gLTE7XG4gIGNyb3NzZXNOZ1RlbXBsYXRlID0gZmFsc2U7XG5cbiAgLyoqXG4gICAqIEEgbm9kZSBpbmRleCBvbiB3aGljaCBhIHF1ZXJ5IHdhcyBkZWNsYXJlZCAoLTEgZm9yIHZpZXcgcXVlcmllcyBhbmQgb25lcyBpbmhlcml0ZWQgZnJvbSB0aGVcbiAgICogZGVjbGFyYXRpb24gdGVtcGxhdGUpLiBXZSB1c2UgdGhpcyBpbmRleCAoYWxvbmdzaWRlIHdpdGggX2FwcGxpZXNUb05leHROb2RlIGZsYWcpIHRvIGtub3dcbiAgICogd2hlbiB0byBhcHBseSBjb250ZW50IHF1ZXJpZXMgdG8gZWxlbWVudHMgaW4gYSB0ZW1wbGF0ZS5cbiAgICovXG4gIHByaXZhdGUgX2RlY2xhcmF0aW9uTm9kZUluZGV4OiBudW1iZXI7XG5cbiAgLyoqXG4gICAqIEEgZmxhZyBpbmRpY2F0aW5nIGlmIGEgZ2l2ZW4gcXVlcnkgc3RpbGwgYXBwbGllcyB0byBub2RlcyBpdCBpcyBjcm9zc2luZy4gV2UgdXNlIHRoaXMgZmxhZ1xuICAgKiAoYWxvbmdzaWRlIHdpdGggX2RlY2xhcmF0aW9uTm9kZUluZGV4KSB0byBrbm93IHdoZW4gdG8gc3RvcCBhcHBseWluZyBjb250ZW50IHF1ZXJpZXMgdG9cbiAgICogZWxlbWVudHMgaW4gYSB0ZW1wbGF0ZS5cbiAgICovXG4gIHByaXZhdGUgX2FwcGxpZXNUb05leHROb2RlID0gdHJ1ZTtcblxuICBjb25zdHJ1Y3RvcihwdWJsaWMgbWV0YWRhdGE6IFRRdWVyeU1ldGFkYXRhLCBub2RlSW5kZXg6IG51bWJlciA9IC0xKSB7XG4gICAgdGhpcy5fZGVjbGFyYXRpb25Ob2RlSW5kZXggPSBub2RlSW5kZXg7XG4gIH1cblxuICBlbGVtZW50U3RhcnQodFZpZXc6IFRWaWV3LCB0Tm9kZTogVE5vZGUpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5pc0FwcGx5aW5nVG9Ob2RlKHROb2RlKSkge1xuICAgICAgdGhpcy5tYXRjaFROb2RlKHRWaWV3LCB0Tm9kZSk7XG4gICAgfVxuICB9XG5cbiAgZWxlbWVudEVuZCh0Tm9kZTogVE5vZGUpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5fZGVjbGFyYXRpb25Ob2RlSW5kZXggPT09IHROb2RlLmluZGV4KSB7XG4gICAgICB0aGlzLl9hcHBsaWVzVG9OZXh0Tm9kZSA9IGZhbHNlO1xuICAgIH1cbiAgfVxuXG4gIHRlbXBsYXRlKHRWaWV3OiBUVmlldywgdE5vZGU6IFROb2RlKTogdm9pZCB7IHRoaXMuZWxlbWVudFN0YXJ0KHRWaWV3LCB0Tm9kZSk7IH1cblxuICBlbWJlZGRlZFRWaWV3KHROb2RlOiBUTm9kZSwgY2hpbGRRdWVyeUluZGV4OiBudW1iZXIpOiBUUXVlcnl8bnVsbCB7XG4gICAgaWYgKHRoaXMuaXNBcHBseWluZ1RvTm9kZSh0Tm9kZSkpIHtcbiAgICAgIHRoaXMuY3Jvc3Nlc05nVGVtcGxhdGUgPSB0cnVlO1xuICAgICAgLy8gQSBtYXJrZXIgaW5kaWNhdGluZyBhIGA8bmctdGVtcGxhdGU+YCBlbGVtZW50IChhIHBsYWNlaG9sZGVyIGZvciBxdWVyeSByZXN1bHRzIGZyb21cbiAgICAgIC8vIGVtYmVkZGVkIHZpZXdzIGNyZWF0ZWQgYmFzZWQgb24gdGhpcyBgPG5nLXRlbXBsYXRlPmApLlxuICAgICAgdGhpcy5hZGRNYXRjaCgtdE5vZGUuaW5kZXgsIGNoaWxkUXVlcnlJbmRleCk7XG4gICAgICByZXR1cm4gbmV3IFRRdWVyeV8odGhpcy5tZXRhZGF0YSk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgcHJpdmF0ZSBpc0FwcGx5aW5nVG9Ob2RlKHROb2RlOiBUTm9kZSk6IGJvb2xlYW4ge1xuICAgIGlmICh0aGlzLl9hcHBsaWVzVG9OZXh0Tm9kZSAmJiB0aGlzLm1ldGFkYXRhLmRlc2NlbmRhbnRzID09PSBmYWxzZSkge1xuICAgICAgcmV0dXJuIHRoaXMuX2RlY2xhcmF0aW9uTm9kZUluZGV4ID09PSAodE5vZGUucGFyZW50ID8gdE5vZGUucGFyZW50LmluZGV4IDogLTEpO1xuICAgIH1cbiAgICByZXR1cm4gdGhpcy5fYXBwbGllc1RvTmV4dE5vZGU7XG4gIH1cblxuICBwcml2YXRlIG1hdGNoVE5vZGUodFZpZXc6IFRWaWV3LCB0Tm9kZTogVE5vZGUpOiB2b2lkIHtcbiAgICBpZiAoQXJyYXkuaXNBcnJheSh0aGlzLm1ldGFkYXRhLnByZWRpY2F0ZSkpIHtcbiAgICAgIGNvbnN0IGxvY2FsTmFtZXMgPSB0aGlzLm1ldGFkYXRhLnByZWRpY2F0ZSBhcyBzdHJpbmdbXTtcbiAgICAgIGZvciAobGV0IGkgPSAwOyBpIDwgbG9jYWxOYW1lcy5sZW5ndGg7IGkrKykge1xuICAgICAgICB0aGlzLm1hdGNoVE5vZGVXaXRoUmVhZE9wdGlvbih0VmlldywgdE5vZGUsIGdldElkeE9mTWF0Y2hpbmdTZWxlY3Rvcih0Tm9kZSwgbG9jYWxOYW1lc1tpXSkpO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICBjb25zdCB0eXBlUHJlZGljYXRlID0gdGhpcy5tZXRhZGF0YS5wcmVkaWNhdGUgYXMgYW55O1xuICAgICAgaWYgKHR5cGVQcmVkaWNhdGUgPT09IFZpZXdFbmdpbmVfVGVtcGxhdGVSZWYpIHtcbiAgICAgICAgaWYgKHROb2RlLnR5cGUgPT09IFROb2RlVHlwZS5Db250YWluZXIpIHtcbiAgICAgICAgICB0aGlzLm1hdGNoVE5vZGVXaXRoUmVhZE9wdGlvbih0VmlldywgdE5vZGUsIC0xKTtcbiAgICAgICAgfVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5tYXRjaFROb2RlV2l0aFJlYWRPcHRpb24oXG4gICAgICAgICAgICB0VmlldywgdE5vZGUsIGxvY2F0ZURpcmVjdGl2ZU9yUHJvdmlkZXIodE5vZGUsIHRWaWV3LCB0eXBlUHJlZGljYXRlLCBmYWxzZSwgZmFsc2UpKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBwcml2YXRlIG1hdGNoVE5vZGVXaXRoUmVhZE9wdGlvbih0VmlldzogVFZpZXcsIHROb2RlOiBUTm9kZSwgbm9kZU1hdGNoSWR4OiBudW1iZXJ8bnVsbCk6IHZvaWQge1xuICAgIGlmIChub2RlTWF0Y2hJZHggIT09IG51bGwpIHtcbiAgICAgIGNvbnN0IHJlYWQgPSB0aGlzLm1ldGFkYXRhLnJlYWQ7XG4gICAgICBpZiAocmVhZCAhPT0gbnVsbCkge1xuICAgICAgICBpZiAocmVhZCA9PT0gVmlld0VuZ2luZV9FbGVtZW50UmVmIHx8IHJlYWQgPT09IFZpZXdDb250YWluZXJSZWYgfHxcbiAgICAgICAgICAgIHJlYWQgPT09IFZpZXdFbmdpbmVfVGVtcGxhdGVSZWYgJiYgdE5vZGUudHlwZSA9PT0gVE5vZGVUeXBlLkNvbnRhaW5lcikge1xuICAgICAgICAgIHRoaXMuYWRkTWF0Y2godE5vZGUuaW5kZXgsIC0yKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBjb25zdCBkaXJlY3RpdmVPclByb3ZpZGVySWR4ID1cbiAgICAgICAgICAgICAgbG9jYXRlRGlyZWN0aXZlT3JQcm92aWRlcih0Tm9kZSwgdFZpZXcsIHJlYWQsIGZhbHNlLCBmYWxzZSk7XG4gICAgICAgICAgaWYgKGRpcmVjdGl2ZU9yUHJvdmlkZXJJZHggIT09IG51bGwpIHtcbiAgICAgICAgICAgIHRoaXMuYWRkTWF0Y2godE5vZGUuaW5kZXgsIGRpcmVjdGl2ZU9yUHJvdmlkZXJJZHgpO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5hZGRNYXRjaCh0Tm9kZS5pbmRleCwgbm9kZU1hdGNoSWR4KTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGFkZE1hdGNoKHROb2RlSWR4OiBudW1iZXIsIG1hdGNoSWR4OiBudW1iZXIpIHtcbiAgICBpZiAodGhpcy5tYXRjaGVzID09PSBudWxsKSB7XG4gICAgICB0aGlzLm1hdGNoZXMgPSBbdE5vZGVJZHgsIG1hdGNoSWR4XTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5tYXRjaGVzLnB1c2godE5vZGVJZHgsIG1hdGNoSWR4KTtcbiAgICB9XG4gIH1cbn1cblxuLyoqXG4gKiBJdGVyYXRlcyBvdmVyIGxvY2FsIG5hbWVzIGZvciBhIGdpdmVuIG5vZGUgYW5kIHJldHVybnMgZGlyZWN0aXZlIGluZGV4XG4gKiAob3IgLTEgaWYgYSBsb2NhbCBuYW1lIHBvaW50cyB0byBhbiBlbGVtZW50KS5cbiAqXG4gKiBAcGFyYW0gdE5vZGUgc3RhdGljIGRhdGEgb2YgYSBub2RlIHRvIGNoZWNrXG4gKiBAcGFyYW0gc2VsZWN0b3Igc2VsZWN0b3IgdG8gbWF0Y2hcbiAqIEByZXR1cm5zIGRpcmVjdGl2ZSBpbmRleCwgLTEgb3IgbnVsbCBpZiBhIHNlbGVjdG9yIGRpZG4ndCBtYXRjaCBhbnkgb2YgdGhlIGxvY2FsIG5hbWVzXG4gKi9cbmZ1bmN0aW9uIGdldElkeE9mTWF0Y2hpbmdTZWxlY3Rvcih0Tm9kZTogVE5vZGUsIHNlbGVjdG9yOiBzdHJpbmcpOiBudW1iZXJ8bnVsbCB7XG4gIGNvbnN0IGxvY2FsTmFtZXMgPSB0Tm9kZS5sb2NhbE5hbWVzO1xuICBpZiAobG9jYWxOYW1lcyAhPT0gbnVsbCkge1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgbG9jYWxOYW1lcy5sZW5ndGg7IGkgKz0gMikge1xuICAgICAgaWYgKGxvY2FsTmFtZXNbaV0gPT09IHNlbGVjdG9yKSB7XG4gICAgICAgIHJldHVybiBsb2NhbE5hbWVzW2kgKyAxXSBhcyBudW1iZXI7XG4gICAgICB9XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufVxuXG5cbmZ1bmN0aW9uIGNyZWF0ZVJlc3VsdEJ5VE5vZGVUeXBlKHROb2RlOiBUTm9kZSwgY3VycmVudFZpZXc6IExWaWV3KTogYW55IHtcbiAgaWYgKHROb2RlLnR5cGUgPT09IFROb2RlVHlwZS5FbGVtZW50IHx8IHROb2RlLnR5cGUgPT09IFROb2RlVHlwZS5FbGVtZW50Q29udGFpbmVyKSB7XG4gICAgcmV0dXJuIGNyZWF0ZUVsZW1lbnRSZWYoVmlld0VuZ2luZV9FbGVtZW50UmVmLCB0Tm9kZSwgY3VycmVudFZpZXcpO1xuICB9IGVsc2UgaWYgKHROb2RlLnR5cGUgPT09IFROb2RlVHlwZS5Db250YWluZXIpIHtcbiAgICByZXR1cm4gY3JlYXRlVGVtcGxhdGVSZWYoVmlld0VuZ2luZV9UZW1wbGF0ZVJlZiwgVmlld0VuZ2luZV9FbGVtZW50UmVmLCB0Tm9kZSwgY3VycmVudFZpZXcpO1xuICB9XG4gIHJldHVybiBudWxsO1xufVxuXG5cbmZ1bmN0aW9uIGNyZWF0ZVJlc3VsdEZvck5vZGUobFZpZXc6IExWaWV3LCB0Tm9kZTogVE5vZGUsIG1hdGNoaW5nSWR4OiBudW1iZXIsIHJlYWQ6IGFueSk6IGFueSB7XG4gIGlmIChtYXRjaGluZ0lkeCA9PT0gLTEpIHtcbiAgICAvLyBpZiByZWFkIHRva2VuIGFuZCAvIG9yIHN0cmF0ZWd5IGlzIG5vdCBzcGVjaWZpZWQsIGRldGVjdCBpdCB1c2luZyBhcHByb3ByaWF0ZSB0Tm9kZSB0eXBlXG4gICAgcmV0dXJuIGNyZWF0ZVJlc3VsdEJ5VE5vZGVUeXBlKHROb2RlLCBsVmlldyk7XG4gIH0gZWxzZSBpZiAobWF0Y2hpbmdJZHggPT09IC0yKSB7XG4gICAgLy8gcmVhZCBhIHNwZWNpYWwgdG9rZW4gZnJvbSBhIG5vZGUgaW5qZWN0b3JcbiAgICByZXR1cm4gY3JlYXRlU3BlY2lhbFRva2VuKGxWaWV3LCB0Tm9kZSwgcmVhZCk7XG4gIH0gZWxzZSB7XG4gICAgLy8gcmVhZCBhIHRva2VuXG4gICAgcmV0dXJuIGdldE5vZGVJbmplY3RhYmxlKGxWaWV3W1RWSUVXXS5kYXRhLCBsVmlldywgbWF0Y2hpbmdJZHgsIHROb2RlIGFzIFRFbGVtZW50Tm9kZSk7XG4gIH1cbn1cblxuZnVuY3Rpb24gY3JlYXRlU3BlY2lhbFRva2VuKGxWaWV3OiBMVmlldywgdE5vZGU6IFROb2RlLCByZWFkOiBhbnkpOiBhbnkge1xuICBpZiAocmVhZCA9PT0gVmlld0VuZ2luZV9FbGVtZW50UmVmKSB7XG4gICAgcmV0dXJuIGNyZWF0ZUVsZW1lbnRSZWYoVmlld0VuZ2luZV9FbGVtZW50UmVmLCB0Tm9kZSwgbFZpZXcpO1xuICB9IGVsc2UgaWYgKHJlYWQgPT09IFZpZXdFbmdpbmVfVGVtcGxhdGVSZWYpIHtcbiAgICByZXR1cm4gY3JlYXRlVGVtcGxhdGVSZWYoVmlld0VuZ2luZV9UZW1wbGF0ZVJlZiwgVmlld0VuZ2luZV9FbGVtZW50UmVmLCB0Tm9kZSwgbFZpZXcpO1xuICB9IGVsc2UgaWYgKHJlYWQgPT09IFZpZXdDb250YWluZXJSZWYpIHtcbiAgICBuZ0Rldk1vZGUgJiYgYXNzZXJ0Tm9kZU9mUG9zc2libGVUeXBlcyhcbiAgICAgICAgICAgICAgICAgICAgIHROb2RlLCBUTm9kZVR5cGUuRWxlbWVudCwgVE5vZGVUeXBlLkNvbnRhaW5lciwgVE5vZGVUeXBlLkVsZW1lbnRDb250YWluZXIpO1xuICAgIHJldHVybiBjcmVhdGVDb250YWluZXJSZWYoXG4gICAgICAgIFZpZXdDb250YWluZXJSZWYsIFZpZXdFbmdpbmVfRWxlbWVudFJlZixcbiAgICAgICAgdE5vZGUgYXMgVEVsZW1lbnROb2RlIHwgVENvbnRhaW5lck5vZGUgfCBURWxlbWVudENvbnRhaW5lck5vZGUsIGxWaWV3KTtcbiAgfSBlbHNlIHtcbiAgICBuZ0Rldk1vZGUgJiZcbiAgICAgICAgdGhyb3dFcnJvcihcbiAgICAgICAgICAgIGBTcGVjaWFsIHRva2VuIHRvIHJlYWQgc2hvdWxkIGJlIG9uZSBvZiBFbGVtZW50UmVmLCBUZW1wbGF0ZVJlZiBvciBWaWV3Q29udGFpbmVyUmVmIGJ1dCBnb3QgJHtzdHJpbmdpZnkocmVhZCl9LmApO1xuICB9XG59XG5cbi8qKlxuICogQSBoZWxwZXIgZnVuY3Rpb24gdGhhdCBjcmVhdGVzIHF1ZXJ5IHJlc3VsdHMgZm9yIGEgZ2l2ZW4gdmlldy4gVGhpcyBmdW5jdGlvbiBpcyBtZWFudCB0byBkbyB0aGVcbiAqIHByb2Nlc3Npbmcgb25jZSBhbmQgb25seSBvbmNlIGZvciBhIGdpdmVuIHZpZXcgaW5zdGFuY2UgKGEgc2V0IG9mIHJlc3VsdHMgZm9yIGEgZ2l2ZW4gdmlld1xuICogZG9lc24ndCBjaGFuZ2UpLlxuICovXG5mdW5jdGlvbiBtYXRlcmlhbGl6ZVZpZXdSZXN1bHRzPFQ+KGxWaWV3OiBMVmlldywgdFF1ZXJ5OiBUUXVlcnksIHF1ZXJ5SW5kZXg6IG51bWJlcik6IChUIHwgbnVsbClbXSB7XG4gIGNvbnN0IGxRdWVyeSA9IGxWaWV3W1FVRVJJRVNdICEucXVlcmllcyAhW3F1ZXJ5SW5kZXhdO1xuICBpZiAobFF1ZXJ5Lm1hdGNoZXMgPT09IG51bGwpIHtcbiAgICBjb25zdCB0Vmlld0RhdGEgPSBsVmlld1tUVklFV10uZGF0YTtcbiAgICBjb25zdCB0UXVlcnlNYXRjaGVzID0gdFF1ZXJ5Lm1hdGNoZXMgITtcbiAgICBjb25zdCByZXN1bHQ6IFR8bnVsbFtdID0gbmV3IEFycmF5KHRRdWVyeU1hdGNoZXMubGVuZ3RoIC8gMik7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCB0UXVlcnlNYXRjaGVzLmxlbmd0aDsgaSArPSAyKSB7XG4gICAgICBjb25zdCBtYXRjaGVkTm9kZUlkeCA9IHRRdWVyeU1hdGNoZXNbaV07XG4gICAgICBpZiAobWF0Y2hlZE5vZGVJZHggPCAwKSB7XG4gICAgICAgIC8vIHdlIGF0IHRoZSA8bmctdGVtcGxhdGU+IG1hcmtlciB3aGljaCBtaWdodCBoYXZlIHJlc3VsdHMgaW4gdmlld3MgY3JlYXRlZCBiYXNlZCBvbiB0aGlzXG4gICAgICAgIC8vIDxuZy10ZW1wbGF0ZT4gLSB0aG9zZSByZXN1bHRzIHdpbGwgYmUgaW4gc2VwYXJhdGUgdmlld3MgdGhvdWdoLCBzbyBoZXJlIHdlIGp1c3QgbGVhdmVcbiAgICAgICAgLy8gbnVsbCBhcyBhIHBsYWNlaG9sZGVyXG4gICAgICAgIHJlc3VsdFtpIC8gMl0gPSBudWxsO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgbmdEZXZNb2RlICYmIGFzc2VydERhdGFJblJhbmdlKHRWaWV3RGF0YSwgbWF0Y2hlZE5vZGVJZHgpO1xuICAgICAgICBjb25zdCB0Tm9kZSA9IHRWaWV3RGF0YVttYXRjaGVkTm9kZUlkeF0gYXMgVE5vZGU7XG4gICAgICAgIHJlc3VsdFtpIC8gMl0gPVxuICAgICAgICAgICAgY3JlYXRlUmVzdWx0Rm9yTm9kZShsVmlldywgdE5vZGUsIHRRdWVyeU1hdGNoZXNbaSArIDFdLCB0UXVlcnkubWV0YWRhdGEucmVhZCk7XG4gICAgICB9XG4gICAgfVxuICAgIGxRdWVyeS5tYXRjaGVzID0gcmVzdWx0O1xuICB9XG5cbiAgcmV0dXJuIGxRdWVyeS5tYXRjaGVzO1xufVxuXG4vKipcbiAqIEEgaGVscGVyIGZ1bmN0aW9uIHRoYXQgY29sbGVjdHMgKGFscmVhZHkgbWF0ZXJpYWxpemVkKSBxdWVyeSByZXN1bHRzIGZyb20gYSB0cmVlIG9mIHZpZXdzLFxuICogc3RhcnRpbmcgd2l0aCBhIHByb3ZpZGVkIExWaWV3LlxuICovXG5mdW5jdGlvbiBjb2xsZWN0UXVlcnlSZXN1bHRzPFQ+KGxWaWV3OiBMVmlldywgcXVlcnlJbmRleDogbnVtYmVyLCByZXN1bHQ6IFRbXSk6IFRbXSB7XG4gIGNvbnN0IHRRdWVyeSA9IGxWaWV3W1RWSUVXXS5xdWVyaWVzICEuZ2V0QnlJbmRleChxdWVyeUluZGV4KTtcbiAgY29uc3QgdFF1ZXJ5TWF0Y2hlcyA9IHRRdWVyeS5tYXRjaGVzO1xuICBpZiAodFF1ZXJ5TWF0Y2hlcyAhPT0gbnVsbCkge1xuICAgIGNvbnN0IGxWaWV3UmVzdWx0cyA9IG1hdGVyaWFsaXplVmlld1Jlc3VsdHM8VD4obFZpZXcsIHRRdWVyeSwgcXVlcnlJbmRleCk7XG5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHRRdWVyeU1hdGNoZXMubGVuZ3RoOyBpICs9IDIpIHtcbiAgICAgIGNvbnN0IHROb2RlSWR4ID0gdFF1ZXJ5TWF0Y2hlc1tpXTtcbiAgICAgIGlmICh0Tm9kZUlkeCA+IDApIHtcbiAgICAgICAgY29uc3Qgdmlld1Jlc3VsdCA9IGxWaWV3UmVzdWx0c1tpIC8gMl07XG4gICAgICAgIG5nRGV2TW9kZSAmJiBhc3NlcnREZWZpbmVkKHZpZXdSZXN1bHQsICdtYXRlcmlhbGl6ZWQgcXVlcnkgcmVzdWx0IHNob3VsZCBiZSBkZWZpbmVkJyk7XG4gICAgICAgIHJlc3VsdC5wdXNoKHZpZXdSZXN1bHQgYXMgVCk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25zdCBjaGlsZFF1ZXJ5SW5kZXggPSB0UXVlcnlNYXRjaGVzW2kgKyAxXTtcblxuICAgICAgICBjb25zdCBkZWNsYXJhdGlvbkxDb250YWluZXIgPSBsVmlld1stdE5vZGVJZHhdIGFzIExDb250YWluZXI7XG4gICAgICAgIG5nRGV2TW9kZSAmJiBhc3NlcnRMQ29udGFpbmVyKGRlY2xhcmF0aW9uTENvbnRhaW5lcik7XG5cbiAgICAgICAgLy8gY29sbGVjdCBtYXRjaGVzIGZvciB2aWV3cyBpbnNlcnRlZCBpbiB0aGlzIGNvbnRhaW5lclxuICAgICAgICBmb3IgKGxldCBpID0gQ09OVEFJTkVSX0hFQURFUl9PRkZTRVQ7IGkgPCBkZWNsYXJhdGlvbkxDb250YWluZXIubGVuZ3RoOyBpKyspIHtcbiAgICAgICAgICBjb25zdCBlbWJlZGRlZExWaWV3ID0gZGVjbGFyYXRpb25MQ29udGFpbmVyW2ldO1xuICAgICAgICAgIGlmIChlbWJlZGRlZExWaWV3W0RFQ0xBUkFUSU9OX0xDT05UQUlORVJdID09PSBlbWJlZGRlZExWaWV3W1BBUkVOVF0pIHtcbiAgICAgICAgICAgIGNvbGxlY3RRdWVyeVJlc3VsdHMoZW1iZWRkZWRMVmlldywgY2hpbGRRdWVyeUluZGV4LCByZXN1bHQpO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuXG4gICAgICAgIC8vIGNvbGxlY3QgbWF0Y2hlcyBmb3Igdmlld3MgY3JlYXRlZCBmcm9tIHRoaXMgZGVjbGFyYXRpb24gY29udGFpbmVyIGFuZCBpbnNlcnRlZCBpbnRvXG4gICAgICAgIC8vIGRpZmZlcmVudCBjb250YWluZXJzXG4gICAgICAgIGlmIChkZWNsYXJhdGlvbkxDb250YWluZXJbTU9WRURfVklFV1NdICE9PSBudWxsKSB7XG4gICAgICAgICAgZm9yIChsZXQgZW1iZWRkZWRMVmlldyBvZiBkZWNsYXJhdGlvbkxDb250YWluZXJbTU9WRURfVklFV1NdICEpIHtcbiAgICAgICAgICAgIGNvbGxlY3RRdWVyeVJlc3VsdHMoZW1iZWRkZWRMVmlldywgY2hpbGRRdWVyeUluZGV4LCByZXN1bHQpO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuICByZXR1cm4gcmVzdWx0O1xufVxuXG4vKipcbiAqIFJlZnJlc2hlcyBhIHF1ZXJ5IGJ5IGNvbWJpbmluZyBtYXRjaGVzIGZyb20gYWxsIGFjdGl2ZSB2aWV3cyBhbmQgcmVtb3ZpbmcgbWF0Y2hlcyBmcm9tIGRlbGV0ZWRcbiAqIHZpZXdzLlxuICpcbiAqIEByZXR1cm5zIGB0cnVlYCBpZiBhIHF1ZXJ5IGdvdCBkaXJ0eSBkdXJpbmcgY2hhbmdlIGRldGVjdGlvbiBvciBpZiB0aGlzIGlzIGEgc3RhdGljIHF1ZXJ5XG4gKiByZXNvbHZpbmcgaW4gY3JlYXRpb24gbW9kZSwgYGZhbHNlYCBvdGhlcndpc2UuXG4gKlxuICogQGNvZGVHZW5BcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIMm1ybVxdWVyeVJlZnJlc2gocXVlcnlMaXN0OiBRdWVyeUxpc3Q8YW55Pik6IGJvb2xlYW4ge1xuICBjb25zdCBsVmlldyA9IGdldExWaWV3KCk7XG4gIGNvbnN0IHF1ZXJ5SW5kZXggPSBnZXRDdXJyZW50UXVlcnlJbmRleCgpO1xuXG4gIHNldEN1cnJlbnRRdWVyeUluZGV4KHF1ZXJ5SW5kZXggKyAxKTtcblxuICBjb25zdCB0UXVlcnkgPSBnZXRUUXVlcnkobFZpZXdbVFZJRVddLCBxdWVyeUluZGV4KTtcbiAgaWYgKHF1ZXJ5TGlzdC5kaXJ0eSAmJiAoaXNDcmVhdGlvbk1vZGUoKSA9PT0gdFF1ZXJ5Lm1ldGFkYXRhLmlzU3RhdGljKSkge1xuICAgIGlmICh0UXVlcnkubWF0Y2hlcyA9PT0gbnVsbCkge1xuICAgICAgcXVlcnlMaXN0LnJlc2V0KFtdKTtcbiAgICB9IGVsc2Uge1xuICAgICAgY29uc3QgcmVzdWx0ID0gdFF1ZXJ5LmNyb3NzZXNOZ1RlbXBsYXRlID8gY29sbGVjdFF1ZXJ5UmVzdWx0cyhsVmlldywgcXVlcnlJbmRleCwgW10pIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIG1hdGVyaWFsaXplVmlld1Jlc3VsdHMobFZpZXcsIHRRdWVyeSwgcXVlcnlJbmRleCk7XG4gICAgICBxdWVyeUxpc3QucmVzZXQocmVzdWx0KTtcbiAgICAgIHF1ZXJ5TGlzdC5ub3RpZnlPbkNoYW5nZXMoKTtcbiAgICB9XG4gICAgcmV0dXJuIHRydWU7XG4gIH1cblxuICByZXR1cm4gZmFsc2U7XG59XG5cbi8qKlxuICogQ3JlYXRlcyBuZXcgUXVlcnlMaXN0IGZvciBhIHN0YXRpYyB2aWV3IHF1ZXJ5LlxuICpcbiAqIEBwYXJhbSBwcmVkaWNhdGUgVGhlIHR5cGUgZm9yIHdoaWNoIHRoZSBxdWVyeSB3aWxsIHNlYXJjaFxuICogQHBhcmFtIGRlc2NlbmQgV2hldGhlciBvciBub3QgdG8gZGVzY2VuZCBpbnRvIGNoaWxkcmVuXG4gKiBAcGFyYW0gcmVhZCBXaGF0IHRvIHNhdmUgaW4gdGhlIHF1ZXJ5XG4gKlxuICogQGNvZGVHZW5BcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIMm1ybVzdGF0aWNWaWV3UXVlcnk8VD4oXG4gICAgcHJlZGljYXRlOiBUeXBlPGFueT58IHN0cmluZ1tdLCBkZXNjZW5kOiBib29sZWFuLCByZWFkPzogYW55KTogdm9pZCB7XG4gIHZpZXdRdWVyeUludGVybmFsKGdldExWaWV3KCksIHByZWRpY2F0ZSwgZGVzY2VuZCwgcmVhZCwgdHJ1ZSk7XG59XG5cbi8qKlxuICogQ3JlYXRlcyBuZXcgUXVlcnlMaXN0LCBzdG9yZXMgdGhlIHJlZmVyZW5jZSBpbiBMVmlldyBhbmQgcmV0dXJucyBRdWVyeUxpc3QuXG4gKlxuICogQHBhcmFtIHByZWRpY2F0ZSBUaGUgdHlwZSBmb3Igd2hpY2ggdGhlIHF1ZXJ5IHdpbGwgc2VhcmNoXG4gKiBAcGFyYW0gZGVzY2VuZCBXaGV0aGVyIG9yIG5vdCB0byBkZXNjZW5kIGludG8gY2hpbGRyZW5cbiAqIEBwYXJhbSByZWFkIFdoYXQgdG8gc2F2ZSBpbiB0aGUgcXVlcnlcbiAqXG4gKiBAY29kZUdlbkFwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gybXJtXZpZXdRdWVyeTxUPihwcmVkaWNhdGU6IFR5cGU8YW55Pnwgc3RyaW5nW10sIGRlc2NlbmQ6IGJvb2xlYW4sIHJlYWQ/OiBhbnkpOiB2b2lkIHtcbiAgdmlld1F1ZXJ5SW50ZXJuYWwoZ2V0TFZpZXcoKSwgcHJlZGljYXRlLCBkZXNjZW5kLCByZWFkLCBmYWxzZSk7XG59XG5cbmZ1bmN0aW9uIHZpZXdRdWVyeUludGVybmFsPFQ+KFxuICAgIGxWaWV3OiBMVmlldywgcHJlZGljYXRlOiBUeXBlPGFueT58IHN0cmluZ1tdLCBkZXNjZW5kOiBib29sZWFuLCByZWFkOiBhbnksXG4gICAgaXNTdGF0aWM6IGJvb2xlYW4pOiB2b2lkIHtcbiAgY29uc3QgdFZpZXcgPSBsVmlld1tUVklFV107XG4gIGlmICh0Vmlldy5maXJzdFRlbXBsYXRlUGFzcykge1xuICAgIGNyZWF0ZVRRdWVyeSh0VmlldywgbmV3IFRRdWVyeU1ldGFkYXRhXyhwcmVkaWNhdGUsIGRlc2NlbmQsIGlzU3RhdGljLCByZWFkKSwgLTEpO1xuICAgIGlmIChpc1N0YXRpYykge1xuICAgICAgdFZpZXcuc3RhdGljVmlld1F1ZXJpZXMgPSB0cnVlO1xuICAgIH1cbiAgfVxuICBjcmVhdGVMUXVlcnk8VD4obFZpZXcpO1xufVxuXG4vKipcbiAqIExvYWRzIGEgUXVlcnlMaXN0IGNvcnJlc3BvbmRpbmcgdG8gdGhlIGN1cnJlbnQgdmlldyBxdWVyeS5cbiAqXG4gKiBAY29kZUdlbkFwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gybXJtWxvYWRWaWV3UXVlcnk8VD4oKTogUXVlcnlMaXN0PFQ+IHtcbiAgcmV0dXJuIGxvYWRRdWVyeUludGVybmFsPFQ+KGdldExWaWV3KCksIGdldEN1cnJlbnRRdWVyeUluZGV4KCkpO1xufVxuXG4vKipcbiAqIFJlZ2lzdGVycyBhIFF1ZXJ5TGlzdCwgYXNzb2NpYXRlZCB3aXRoIGEgY29udGVudCBxdWVyeSwgZm9yIGxhdGVyIHJlZnJlc2ggKHBhcnQgb2YgYSB2aWV3XG4gKiByZWZyZXNoKS5cbiAqXG4gKiBAcGFyYW0gZGlyZWN0aXZlSW5kZXggQ3VycmVudCBkaXJlY3RpdmUgaW5kZXhcbiAqIEBwYXJhbSBwcmVkaWNhdGUgVGhlIHR5cGUgZm9yIHdoaWNoIHRoZSBxdWVyeSB3aWxsIHNlYXJjaFxuICogQHBhcmFtIGRlc2NlbmQgV2hldGhlciBvciBub3QgdG8gZGVzY2VuZCBpbnRvIGNoaWxkcmVuXG4gKiBAcGFyYW0gcmVhZCBXaGF0IHRvIHNhdmUgaW4gdGhlIHF1ZXJ5XG4gKiBAcmV0dXJucyBRdWVyeUxpc3Q8VD5cbiAqXG4gKiBAY29kZUdlbkFwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gybXJtWNvbnRlbnRRdWVyeTxUPihcbiAgICBkaXJlY3RpdmVJbmRleDogbnVtYmVyLCBwcmVkaWNhdGU6IFR5cGU8YW55Pnwgc3RyaW5nW10sIGRlc2NlbmQ6IGJvb2xlYW4sIHJlYWQ/OiBhbnkpOiB2b2lkIHtcbiAgY29udGVudFF1ZXJ5SW50ZXJuYWwoXG4gICAgICBnZXRMVmlldygpLCBwcmVkaWNhdGUsIGRlc2NlbmQsIHJlYWQsIGZhbHNlLCBnZXRQcmV2aW91c09yUGFyZW50VE5vZGUoKSwgZGlyZWN0aXZlSW5kZXgpO1xufVxuXG4vKipcbiAqIFJlZ2lzdGVycyBhIFF1ZXJ5TGlzdCwgYXNzb2NpYXRlZCB3aXRoIGEgc3RhdGljIGNvbnRlbnQgcXVlcnksIGZvciBsYXRlciByZWZyZXNoXG4gKiAocGFydCBvZiBhIHZpZXcgcmVmcmVzaCkuXG4gKlxuICogQHBhcmFtIGRpcmVjdGl2ZUluZGV4IEN1cnJlbnQgZGlyZWN0aXZlIGluZGV4XG4gKiBAcGFyYW0gcHJlZGljYXRlIFRoZSB0eXBlIGZvciB3aGljaCB0aGUgcXVlcnkgd2lsbCBzZWFyY2hcbiAqIEBwYXJhbSBkZXNjZW5kIFdoZXRoZXIgb3Igbm90IHRvIGRlc2NlbmQgaW50byBjaGlsZHJlblxuICogQHBhcmFtIHJlYWQgV2hhdCB0byBzYXZlIGluIHRoZSBxdWVyeVxuICogQHJldHVybnMgUXVlcnlMaXN0PFQ+XG4gKlxuICogQGNvZGVHZW5BcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIMm1ybVzdGF0aWNDb250ZW50UXVlcnk8VD4oXG4gICAgZGlyZWN0aXZlSW5kZXg6IG51bWJlciwgcHJlZGljYXRlOiBUeXBlPGFueT58IHN0cmluZ1tdLCBkZXNjZW5kOiBib29sZWFuLCByZWFkPzogYW55KTogdm9pZCB7XG4gIGNvbnRlbnRRdWVyeUludGVybmFsKFxuICAgICAgZ2V0TFZpZXcoKSwgcHJlZGljYXRlLCBkZXNjZW5kLCByZWFkLCB0cnVlLCBnZXRQcmV2aW91c09yUGFyZW50VE5vZGUoKSwgZGlyZWN0aXZlSW5kZXgpO1xufVxuXG5mdW5jdGlvbiBjb250ZW50UXVlcnlJbnRlcm5hbDxUPihcbiAgICBsVmlldzogTFZpZXcsIHByZWRpY2F0ZTogVHlwZTxhbnk+fCBzdHJpbmdbXSwgZGVzY2VuZDogYm9vbGVhbiwgcmVhZDogYW55LCBpc1N0YXRpYzogYm9vbGVhbixcbiAgICB0Tm9kZTogVE5vZGUsIGRpcmVjdGl2ZUluZGV4OiBudW1iZXIpOiB2b2lkIHtcbiAgY29uc3QgdFZpZXcgPSBsVmlld1tUVklFV107XG4gIGlmICh0Vmlldy5maXJzdFRlbXBsYXRlUGFzcykge1xuICAgIGNyZWF0ZVRRdWVyeSh0VmlldywgbmV3IFRRdWVyeU1ldGFkYXRhXyhwcmVkaWNhdGUsIGRlc2NlbmQsIGlzU3RhdGljLCByZWFkKSwgdE5vZGUuaW5kZXgpO1xuICAgIHNhdmVDb250ZW50UXVlcnlBbmREaXJlY3RpdmVJbmRleCh0VmlldywgZGlyZWN0aXZlSW5kZXgpO1xuICAgIGlmIChpc1N0YXRpYykge1xuICAgICAgdFZpZXcuc3RhdGljQ29udGVudFF1ZXJpZXMgPSB0cnVlO1xuICAgIH1cbiAgfVxuXG4gIGNyZWF0ZUxRdWVyeTxUPihsVmlldyk7XG59XG5cbi8qKlxuICogTG9hZHMgYSBRdWVyeUxpc3QgY29ycmVzcG9uZGluZyB0byB0aGUgY3VycmVudCBjb250ZW50IHF1ZXJ5LlxuICpcbiAqIEBjb2RlR2VuQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiDJtcm1bG9hZENvbnRlbnRRdWVyeTxUPigpOiBRdWVyeUxpc3Q8VD4ge1xuICByZXR1cm4gbG9hZFF1ZXJ5SW50ZXJuYWw8VD4oZ2V0TFZpZXcoKSwgZ2V0Q3VycmVudFF1ZXJ5SW5kZXgoKSk7XG59XG5cbmZ1bmN0aW9uIGxvYWRRdWVyeUludGVybmFsPFQ+KGxWaWV3OiBMVmlldywgcXVlcnlJbmRleDogbnVtYmVyKTogUXVlcnlMaXN0PFQ+IHtcbiAgbmdEZXZNb2RlICYmXG4gICAgICBhc3NlcnREZWZpbmVkKGxWaWV3W1FVRVJJRVNdLCAnTFF1ZXJpZXMgc2hvdWxkIGJlIGRlZmluZWQgd2hlbiB0cnlpbmcgdG8gbG9hZCBhIHF1ZXJ5Jyk7XG4gIG5nRGV2TW9kZSAmJiBhc3NlcnREYXRhSW5SYW5nZShsVmlld1tRVUVSSUVTXSAhLnF1ZXJpZXMsIHF1ZXJ5SW5kZXgpO1xuICByZXR1cm4gbFZpZXdbUVVFUklFU10gIS5xdWVyaWVzW3F1ZXJ5SW5kZXhdLnF1ZXJ5TGlzdDtcbn1cblxuZnVuY3Rpb24gY3JlYXRlTFF1ZXJ5PFQ+KGxWaWV3OiBMVmlldykge1xuICBjb25zdCBxdWVyeUxpc3QgPSBuZXcgUXVlcnlMaXN0PFQ+KCk7XG4gIHN0b3JlQ2xlYW51cFdpdGhDb250ZXh0KGxWaWV3LCBxdWVyeUxpc3QsIHF1ZXJ5TGlzdC5kZXN0cm95KTtcblxuICBpZiAobFZpZXdbUVVFUklFU10gPT09IG51bGwpIGxWaWV3W1FVRVJJRVNdID0gbmV3IExRdWVyaWVzXygpO1xuICBsVmlld1tRVUVSSUVTXSAhLnF1ZXJpZXMucHVzaChuZXcgTFF1ZXJ5XyhxdWVyeUxpc3QpKTtcbn1cblxuZnVuY3Rpb24gY3JlYXRlVFF1ZXJ5KHRWaWV3OiBUVmlldywgbWV0YWRhdGE6IFRRdWVyeU1ldGFkYXRhLCBub2RlSW5kZXg6IG51bWJlcik6IHZvaWQge1xuICBpZiAodFZpZXcucXVlcmllcyA9PT0gbnVsbCkgdFZpZXcucXVlcmllcyA9IG5ldyBUUXVlcmllc18oKTtcbiAgdFZpZXcucXVlcmllcy50cmFjayhuZXcgVFF1ZXJ5XyhtZXRhZGF0YSwgbm9kZUluZGV4KSk7XG59XG5cbmZ1bmN0aW9uIHNhdmVDb250ZW50UXVlcnlBbmREaXJlY3RpdmVJbmRleCh0VmlldzogVFZpZXcsIGRpcmVjdGl2ZUluZGV4OiBudW1iZXIpIHtcbiAgY29uc3QgdFZpZXdDb250ZW50UXVlcmllcyA9IHRWaWV3LmNvbnRlbnRRdWVyaWVzIHx8ICh0Vmlldy5jb250ZW50UXVlcmllcyA9IFtdKTtcbiAgY29uc3QgbGFzdFNhdmVkRGlyZWN0aXZlSW5kZXggPVxuICAgICAgdFZpZXcuY29udGVudFF1ZXJpZXMubGVuZ3RoID8gdFZpZXdDb250ZW50UXVlcmllc1t0Vmlld0NvbnRlbnRRdWVyaWVzLmxlbmd0aCAtIDFdIDogLTE7XG4gIGlmIChkaXJlY3RpdmVJbmRleCAhPT0gbGFzdFNhdmVkRGlyZWN0aXZlSW5kZXgpIHtcbiAgICB0Vmlld0NvbnRlbnRRdWVyaWVzLnB1c2godFZpZXcucXVlcmllcyAhLmxlbmd0aCAtIDEsIGRpcmVjdGl2ZUluZGV4KTtcbiAgfVxufVxuXG5mdW5jdGlvbiBnZXRUUXVlcnkodFZpZXc6IFRWaWV3LCBpbmRleDogbnVtYmVyKTogVFF1ZXJ5IHtcbiAgbmdEZXZNb2RlICYmIGFzc2VydERlZmluZWQodFZpZXcucXVlcmllcywgJ1RRdWVyaWVzIG11c3QgYmUgZGVmaW5lZCB0byByZXRyaWV2ZSBhIFRRdWVyeScpO1xuICByZXR1cm4gdFZpZXcucXVlcmllcyAhLmdldEJ5SW5kZXgoaW5kZXgpO1xufVxuIl19
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
/**
 * An object representing query metadata extracted from query annotations.
 * @record
 */
export function TQueryMetadata() { }
if (false) {
    /** @type {?} */
    TQueryMetadata.prototype.predicate;
    /** @type {?} */
    TQueryMetadata.prototype.descendants;
    /** @type {?} */
    TQueryMetadata.prototype.read;
    /** @type {?} */
    TQueryMetadata.prototype.isStatic;
}
/**
 * TQuery objects represent all the query-related data that remain the same from one view instance
 * to another and can be determined on the very first template pass. Most notably TQuery holds all
 * the matches for a given view.
 * @record
 */
export function TQuery() { }
if (false) {
    /**
     * Query metadata extracted from query annotations.
     * @type {?}
     */
    TQuery.prototype.metadata;
    /**
     * Index of a query in a declaration view in case of queries propagated to en embedded view, -1
     * for queries declared in a given view. We are storing this index so we can find a parent query
     * to clone for an embedded view (when an embedded view is created).
     * @type {?}
     */
    TQuery.prototype.indexInDeclarationView;
    /**
     * Matches collected on the first template pass. Each match is a pair of:
     * - TNode index;
     * - match index;
     *
     * A TNode index can be either:
     * - a positive number (the most common case) to indicate a matching TNode;
     * - a negative number to indicate that a given query is crossing a <ng-template> element and
     * results from views created based on TemplateRef should be inserted at this place.
     *
     * A match index is a number used to find an actual value (for a given node) when query results
     * are materialized. This index can have one of the following values:
     * - -2 - indicates that we need to read a special token (TemplateRef, ViewContainerRef etc.);
     * - -1 - indicates that we need to read a default value based on the node type (TemplateRef for
     * ng-template and ElementRef for other elements);
     * - a positive number - index of an injectable to be read from the element injector.
     * @type {?}
     */
    TQuery.prototype.matches;
    /**
     * A flag indicating if a given query crosses an <ng-template> element. This flag exists for
     * performance reasons: we can notice that queries not crossing any <ng-template> elements will
     * have matches from a given view only (and adapt processing accordingly).
     * @type {?}
     */
    TQuery.prototype.crossesNgTemplate;
    /**
     * A method call when a given query is crossing an element (or element container). This is where a
     * given TNode is matched against a query predicate.
     * @param {?} tView
     * @param {?} tNode
     * @return {?}
     */
    TQuery.prototype.elementStart = function (tView, tNode) { };
    /**
     * A method called when processing the elementEnd instruction - this is mostly useful to determine
     * if a given content query should match any nodes past this point.
     * @param {?} tNode
     * @return {?}
     */
    TQuery.prototype.elementEnd = function (tNode) { };
    /**
     * A method called when processing the template instruction. This is where a
     * given TContainerNode is matched against a query predicate.
     * @param {?} tView
     * @param {?} tNode
     * @return {?}
     */
    TQuery.prototype.template = function (tView, tNode) { };
    /**
     * A query-related method called when an embedded TView is created based on the content of a
     * <ng-template> element. We call this method to determine if a given query should be propagated
     * to the embedded view and if so - return a cloned TQuery for this embedded view.
     * @param {?} tNode
     * @param {?} childQueryIndex
     * @return {?}
     */
    TQuery.prototype.embeddedTView = function (tNode, childQueryIndex) { };
}
/**
 * TQueries represent a collection of individual TQuery objects tracked in a given view. Most of the
 * methods on this interface are simple proxy methods to the corresponding functionality on TQuery.
 * @record
 */
export function TQueries() { }
if (false) {
    /**
     * Returns the number of queries tracked in a given view.
     * @type {?}
     */
    TQueries.prototype.length;
    /**
     * Adds a new TQuery to a collection of queries tracked in a given view.
     * @param {?} tQuery
     * @return {?}
     */
    TQueries.prototype.track = function (tQuery) { };
    /**
     * Returns a TQuery instance for at the given index  in the queries array.
     * @param {?} index
     * @return {?}
     */
    TQueries.prototype.getByIndex = function (index) { };
    /**
     * A proxy method that iterates over all the TQueries in a given TView and calls the corresponding
     * `elementStart` on each and every TQuery.
     * @param {?} tView
     * @param {?} tNode
     * @return {?}
     */
    TQueries.prototype.elementStart = function (tView, tNode) { };
    /**
     * A proxy method that iterates over all the TQueries in a given TView and calls the corresponding
     * `elementEnd` on each and every TQuery.
     * @param {?} tNode
     * @return {?}
     */
    TQueries.prototype.elementEnd = function (tNode) { };
    /**
     * A proxy method that iterates over all the TQueries in a given TView and calls the corresponding
     * `template` on each and every TQuery.
     * @param {?} tView
     * @param {?} tNode
     * @return {?}
     */
    TQueries.prototype.template = function (tView, tNode) { };
    /**
     * A proxy method that iterates over all the TQueries in a given TView and calls the corresponding
     * `embeddedTView` on each and every TQuery.
     * @param {?} tNode
     * @return {?}
     */
    TQueries.prototype.embeddedTView = function (tNode) { };
}
/**
 * An interface that represents query-related information specific to a view instance. Most notably
 * it contains:
 * - materialized query matches;
 * - a pointer to a QueryList where materialized query results should be reported.
 * @record
 * @template T
 */
export function LQuery() { }
if (false) {
    /**
     * Materialized query matches for a given view only (!). Results are initialized lazily so the
     * array of matches is set to `null` initially.
     * @type {?}
     */
    LQuery.prototype.matches;
    /**
     * A QueryList where materialized query results should be reported.
     * @type {?}
     */
    LQuery.prototype.queryList;
    /**
     * Clones an LQuery for an embedded view. A cloned query shares the same `QueryList` but has a
     * separate collection of materialized matches.
     * @return {?}
     */
    LQuery.prototype.clone = function () { };
    /**
     * Called when an embedded view, impacting results of this query, is inserted or removed.
     * @return {?}
     */
    LQuery.prototype.setDirty = function () { };
}
/**
 * lQueries represent a collection of individual LQuery objects tracked in a given view.
 * @record
 */
export function LQueries() { }
if (false) {
    /**
     * A collection of queries tracked in a given view.
     * @type {?}
     */
    LQueries.prototype.queries;
    /**
     * A method called when a new embedded view is created. As a result a set of LQueries applicable
     * for a new embedded view is instantiated (cloned) from the declaration view.
     * @param {?} tView
     * @return {?}
     */
    LQueries.prototype.createEmbeddedView = function (tView) { };
    /**
     * A method called when an embedded view is inserted into a container. As a result all impacted
     * `LQuery` objects (and associated `QueryList`) are marked as dirty.
     * @param {?} tView
     * @return {?}
     */
    LQueries.prototype.insertView = function (tView) { };
    /**
     * A method called when an embedded view is detached from a container. As a result all impacted
     * `LQuery` objects (and associated `QueryList`) are marked as dirty.
     * @param {?} tView
     * @return {?}
     */
    LQueries.prototype.detachView = function (tView) { };
}
// Note: This hack is necessary so we don't erroneously get a circular dependency
// failure based on types.
/** @type {?} */
export const unusedValueExportToPlacateAjd = 1;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicXVlcnkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL2ludGVyZmFjZXMvcXVlcnkudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7Ozs7O0FBaUJBLG9DQUtDOzs7SUFKQyxtQ0FBOEI7O0lBQzlCLHFDQUFxQjs7SUFDckIsOEJBQVU7O0lBQ1Ysa0NBQWtCOzs7Ozs7OztBQVFwQiw0QkFzRUM7Ozs7OztJQWxFQywwQkFBeUI7Ozs7Ozs7SUFPekIsd0NBQStCOzs7Ozs7Ozs7Ozs7Ozs7Ozs7O0lBbUIvQix5QkFBdUI7Ozs7Ozs7SUFPdkIsbUNBQTJCOzs7Ozs7OztJQVEzQiw0REFBK0M7Ozs7Ozs7SUFPL0MsbURBQStCOzs7Ozs7OztJQVEvQix3REFBMkM7Ozs7Ozs7OztJQVMzQyx1RUFBa0U7Ozs7Ozs7QUFPcEUsOEJBK0NDOzs7Ozs7SUEvQkMsMEJBQWU7Ozs7OztJQVhmLGlEQUE0Qjs7Ozs7O0lBTTVCLHFEQUFrQzs7Ozs7Ozs7SUFhbEMsOERBQStDOzs7Ozs7O0lBTy9DLHFEQUErQjs7Ozs7Ozs7SUFRL0IsMERBQTJDOzs7Ozs7O0lBTzNDLHdEQUEyQzs7Ozs7Ozs7OztBQVM3Qyw0QkFzQkM7Ozs7Ozs7SUFqQkMseUJBQXlCOzs7OztJQUt6QiwyQkFBd0I7Ozs7OztJQU14Qix5Q0FBbUI7Ozs7O0lBS25CLDRDQUFpQjs7Ozs7O0FBTW5CLDhCQTBCQzs7Ozs7O0lBdEJDLDJCQUF1Qjs7Ozs7OztJQU92Qiw2REFBZ0Q7Ozs7Ozs7SUFPaEQscURBQStCOzs7Ozs7O0lBTy9CLHFEQUErQjs7Ozs7QUFNakMsTUFBTSxPQUFPLDZCQUE2QixHQUFHLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7VHlwZX0gZnJvbSAnLi4vLi4vaW50ZXJmYWNlL3R5cGUnO1xuaW1wb3J0IHtRdWVyeUxpc3R9IGZyb20gJy4uLy4uL2xpbmtlcic7XG5cbmltcG9ydCB7VE5vZGV9IGZyb20gJy4vbm9kZSc7XG5pbXBvcnQge1RWaWV3fSBmcm9tICcuL3ZpZXcnO1xuXG4vKipcbiAqIEFuIG9iamVjdCByZXByZXNlbnRpbmcgcXVlcnkgbWV0YWRhdGEgZXh0cmFjdGVkIGZyb20gcXVlcnkgYW5ub3RhdGlvbnMuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgVFF1ZXJ5TWV0YWRhdGEge1xuICBwcmVkaWNhdGU6IFR5cGU8YW55PnxzdHJpbmdbXTtcbiAgZGVzY2VuZGFudHM6IGJvb2xlYW47XG4gIHJlYWQ6IGFueTtcbiAgaXNTdGF0aWM6IGJvb2xlYW47XG59XG5cbi8qKlxuICogVFF1ZXJ5IG9iamVjdHMgcmVwcmVzZW50IGFsbCB0aGUgcXVlcnktcmVsYXRlZCBkYXRhIHRoYXQgcmVtYWluIHRoZSBzYW1lIGZyb20gb25lIHZpZXcgaW5zdGFuY2VcbiAqIHRvIGFub3RoZXIgYW5kIGNhbiBiZSBkZXRlcm1pbmVkIG9uIHRoZSB2ZXJ5IGZpcnN0IHRlbXBsYXRlIHBhc3MuIE1vc3Qgbm90YWJseSBUUXVlcnkgaG9sZHMgYWxsXG4gKiB0aGUgbWF0Y2hlcyBmb3IgYSBnaXZlbiB2aWV3LlxuICovXG5leHBvcnQgaW50ZXJmYWNlIFRRdWVyeSB7XG4gIC8qKlxuICAgKiBRdWVyeSBtZXRhZGF0YSBleHRyYWN0ZWQgZnJvbSBxdWVyeSBhbm5vdGF0aW9ucy5cbiAgICovXG4gIG1ldGFkYXRhOiBUUXVlcnlNZXRhZGF0YTtcblxuICAvKipcbiAgICogSW5kZXggb2YgYSBxdWVyeSBpbiBhIGRlY2xhcmF0aW9uIHZpZXcgaW4gY2FzZSBvZiBxdWVyaWVzIHByb3BhZ2F0ZWQgdG8gZW4gZW1iZWRkZWQgdmlldywgLTFcbiAgICogZm9yIHF1ZXJpZXMgZGVjbGFyZWQgaW4gYSBnaXZlbiB2aWV3LiBXZSBhcmUgc3RvcmluZyB0aGlzIGluZGV4IHNvIHdlIGNhbiBmaW5kIGEgcGFyZW50IHF1ZXJ5XG4gICAqIHRvIGNsb25lIGZvciBhbiBlbWJlZGRlZCB2aWV3ICh3aGVuIGFuIGVtYmVkZGVkIHZpZXcgaXMgY3JlYXRlZCkuXG4gICAqL1xuICBpbmRleEluRGVjbGFyYXRpb25WaWV3OiBudW1iZXI7XG5cbiAgLyoqXG4gICAqIE1hdGNoZXMgY29sbGVjdGVkIG9uIHRoZSBmaXJzdCB0ZW1wbGF0ZSBwYXNzLiBFYWNoIG1hdGNoIGlzIGEgcGFpciBvZjpcbiAgICogLSBUTm9kZSBpbmRleDtcbiAgICogLSBtYXRjaCBpbmRleDtcbiAgICpcbiAgICogQSBUTm9kZSBpbmRleCBjYW4gYmUgZWl0aGVyOlxuICAgKiAtIGEgcG9zaXRpdmUgbnVtYmVyICh0aGUgbW9zdCBjb21tb24gY2FzZSkgdG8gaW5kaWNhdGUgYSBtYXRjaGluZyBUTm9kZTtcbiAgICogLSBhIG5lZ2F0aXZlIG51bWJlciB0byBpbmRpY2F0ZSB0aGF0IGEgZ2l2ZW4gcXVlcnkgaXMgY3Jvc3NpbmcgYSA8bmctdGVtcGxhdGU+IGVsZW1lbnQgYW5kXG4gICAqIHJlc3VsdHMgZnJvbSB2aWV3cyBjcmVhdGVkIGJhc2VkIG9uIFRlbXBsYXRlUmVmIHNob3VsZCBiZSBpbnNlcnRlZCBhdCB0aGlzIHBsYWNlLlxuICAgKlxuICAgKiBBIG1hdGNoIGluZGV4IGlzIGEgbnVtYmVyIHVzZWQgdG8gZmluZCBhbiBhY3R1YWwgdmFsdWUgKGZvciBhIGdpdmVuIG5vZGUpIHdoZW4gcXVlcnkgcmVzdWx0c1xuICAgKiBhcmUgbWF0ZXJpYWxpemVkLiBUaGlzIGluZGV4IGNhbiBoYXZlIG9uZSBvZiB0aGUgZm9sbG93aW5nIHZhbHVlczpcbiAgICogLSAtMiAtIGluZGljYXRlcyB0aGF0IHdlIG5lZWQgdG8gcmVhZCBhIHNwZWNpYWwgdG9rZW4gKFRlbXBsYXRlUmVmLCBWaWV3Q29udGFpbmVyUmVmIGV0Yy4pO1xuICAgKiAtIC0xIC0gaW5kaWNhdGVzIHRoYXQgd2UgbmVlZCB0byByZWFkIGEgZGVmYXVsdCB2YWx1ZSBiYXNlZCBvbiB0aGUgbm9kZSB0eXBlIChUZW1wbGF0ZVJlZiBmb3JcbiAgICogbmctdGVtcGxhdGUgYW5kIEVsZW1lbnRSZWYgZm9yIG90aGVyIGVsZW1lbnRzKTtcbiAgICogLSBhIHBvc2l0aXZlIG51bWJlciAtIGluZGV4IG9mIGFuIGluamVjdGFibGUgdG8gYmUgcmVhZCBmcm9tIHRoZSBlbGVtZW50IGluamVjdG9yLlxuICAgKi9cbiAgbWF0Y2hlczogbnVtYmVyW118bnVsbDtcblxuICAvKipcbiAgICogQSBmbGFnIGluZGljYXRpbmcgaWYgYSBnaXZlbiBxdWVyeSBjcm9zc2VzIGFuIDxuZy10ZW1wbGF0ZT4gZWxlbWVudC4gVGhpcyBmbGFnIGV4aXN0cyBmb3JcbiAgICogcGVyZm9ybWFuY2UgcmVhc29uczogd2UgY2FuIG5vdGljZSB0aGF0IHF1ZXJpZXMgbm90IGNyb3NzaW5nIGFueSA8bmctdGVtcGxhdGU+IGVsZW1lbnRzIHdpbGxcbiAgICogaGF2ZSBtYXRjaGVzIGZyb20gYSBnaXZlbiB2aWV3IG9ubHkgKGFuZCBhZGFwdCBwcm9jZXNzaW5nIGFjY29yZGluZ2x5KS5cbiAgICovXG4gIGNyb3NzZXNOZ1RlbXBsYXRlOiBib29sZWFuO1xuXG4gIC8qKlxuICAgKiBBIG1ldGhvZCBjYWxsIHdoZW4gYSBnaXZlbiBxdWVyeSBpcyBjcm9zc2luZyBhbiBlbGVtZW50IChvciBlbGVtZW50IGNvbnRhaW5lcikuIFRoaXMgaXMgd2hlcmUgYVxuICAgKiBnaXZlbiBUTm9kZSBpcyBtYXRjaGVkIGFnYWluc3QgYSBxdWVyeSBwcmVkaWNhdGUuXG4gICAqIEBwYXJhbSB0Vmlld1xuICAgKiBAcGFyYW0gdE5vZGVcbiAgICovXG4gIGVsZW1lbnRTdGFydCh0VmlldzogVFZpZXcsIHROb2RlOiBUTm9kZSk6IHZvaWQ7XG5cbiAgLyoqXG4gICAqIEEgbWV0aG9kIGNhbGxlZCB3aGVuIHByb2Nlc3NpbmcgdGhlIGVsZW1lbnRFbmQgaW5zdHJ1Y3Rpb24gLSB0aGlzIGlzIG1vc3RseSB1c2VmdWwgdG8gZGV0ZXJtaW5lXG4gICAqIGlmIGEgZ2l2ZW4gY29udGVudCBxdWVyeSBzaG91bGQgbWF0Y2ggYW55IG5vZGVzIHBhc3QgdGhpcyBwb2ludC5cbiAgICogQHBhcmFtIHROb2RlXG4gICAqL1xuICBlbGVtZW50RW5kKHROb2RlOiBUTm9kZSk6IHZvaWQ7XG5cbiAgLyoqXG4gICAqIEEgbWV0aG9kIGNhbGxlZCB3aGVuIHByb2Nlc3NpbmcgdGhlIHRlbXBsYXRlIGluc3RydWN0aW9uLiBUaGlzIGlzIHdoZXJlIGFcbiAgICogZ2l2ZW4gVENvbnRhaW5lck5vZGUgaXMgbWF0Y2hlZCBhZ2FpbnN0IGEgcXVlcnkgcHJlZGljYXRlLlxuICAgKiBAcGFyYW0gdFZpZXdcbiAgICogQHBhcmFtIHROb2RlXG4gICAqL1xuICB0ZW1wbGF0ZSh0VmlldzogVFZpZXcsIHROb2RlOiBUTm9kZSk6IHZvaWQ7XG5cbiAgLyoqXG4gICAqIEEgcXVlcnktcmVsYXRlZCBtZXRob2QgY2FsbGVkIHdoZW4gYW4gZW1iZWRkZWQgVFZpZXcgaXMgY3JlYXRlZCBiYXNlZCBvbiB0aGUgY29udGVudCBvZiBhXG4gICAqIDxuZy10ZW1wbGF0ZT4gZWxlbWVudC4gV2UgY2FsbCB0aGlzIG1ldGhvZCB0byBkZXRlcm1pbmUgaWYgYSBnaXZlbiBxdWVyeSBzaG91bGQgYmUgcHJvcGFnYXRlZFxuICAgKiB0byB0aGUgZW1iZWRkZWQgdmlldyBhbmQgaWYgc28gLSByZXR1cm4gYSBjbG9uZWQgVFF1ZXJ5IGZvciB0aGlzIGVtYmVkZGVkIHZpZXcuXG4gICAqIEBwYXJhbSB0Tm9kZVxuICAgKiBAcGFyYW0gY2hpbGRRdWVyeUluZGV4XG4gICAqL1xuICBlbWJlZGRlZFRWaWV3KHROb2RlOiBUTm9kZSwgY2hpbGRRdWVyeUluZGV4OiBudW1iZXIpOiBUUXVlcnl8bnVsbDtcbn1cblxuLyoqXG4gKiBUUXVlcmllcyByZXByZXNlbnQgYSBjb2xsZWN0aW9uIG9mIGluZGl2aWR1YWwgVFF1ZXJ5IG9iamVjdHMgdHJhY2tlZCBpbiBhIGdpdmVuIHZpZXcuIE1vc3Qgb2YgdGhlXG4gKiBtZXRob2RzIG9uIHRoaXMgaW50ZXJmYWNlIGFyZSBzaW1wbGUgcHJveHkgbWV0aG9kcyB0byB0aGUgY29ycmVzcG9uZGluZyBmdW5jdGlvbmFsaXR5IG9uIFRRdWVyeS5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBUUXVlcmllcyB7XG4gIC8qKlxuICAgKiBBZGRzIGEgbmV3IFRRdWVyeSB0byBhIGNvbGxlY3Rpb24gb2YgcXVlcmllcyB0cmFja2VkIGluIGEgZ2l2ZW4gdmlldy5cbiAgICogQHBhcmFtIHRRdWVyeVxuICAgKi9cbiAgdHJhY2sodFF1ZXJ5OiBUUXVlcnkpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBSZXR1cm5zIGEgVFF1ZXJ5IGluc3RhbmNlIGZvciBhdCB0aGUgZ2l2ZW4gaW5kZXggIGluIHRoZSBxdWVyaWVzIGFycmF5LlxuICAgKiBAcGFyYW0gaW5kZXhcbiAgICovXG4gIGdldEJ5SW5kZXgoaW5kZXg6IG51bWJlcik6IFRRdWVyeTtcblxuICAvKipcbiAgICogUmV0dXJucyB0aGUgbnVtYmVyIG9mIHF1ZXJpZXMgdHJhY2tlZCBpbiBhIGdpdmVuIHZpZXcuXG4gICAqL1xuICBsZW5ndGg6IG51bWJlcjtcblxuICAvKipcbiAgICogQSBwcm94eSBtZXRob2QgdGhhdCBpdGVyYXRlcyBvdmVyIGFsbCB0aGUgVFF1ZXJpZXMgaW4gYSBnaXZlbiBUVmlldyBhbmQgY2FsbHMgdGhlIGNvcnJlc3BvbmRpbmdcbiAgICogYGVsZW1lbnRTdGFydGAgb24gZWFjaCBhbmQgZXZlcnkgVFF1ZXJ5LlxuICAgKiBAcGFyYW0gdFZpZXdcbiAgICogQHBhcmFtIHROb2RlXG4gICAqL1xuICBlbGVtZW50U3RhcnQodFZpZXc6IFRWaWV3LCB0Tm9kZTogVE5vZGUpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBBIHByb3h5IG1ldGhvZCB0aGF0IGl0ZXJhdGVzIG92ZXIgYWxsIHRoZSBUUXVlcmllcyBpbiBhIGdpdmVuIFRWaWV3IGFuZCBjYWxscyB0aGUgY29ycmVzcG9uZGluZ1xuICAgKiBgZWxlbWVudEVuZGAgb24gZWFjaCBhbmQgZXZlcnkgVFF1ZXJ5LlxuICAgKiBAcGFyYW0gdE5vZGVcbiAgICovXG4gIGVsZW1lbnRFbmQodE5vZGU6IFROb2RlKTogdm9pZDtcblxuICAvKipcbiAgICogQSBwcm94eSBtZXRob2QgdGhhdCBpdGVyYXRlcyBvdmVyIGFsbCB0aGUgVFF1ZXJpZXMgaW4gYSBnaXZlbiBUVmlldyBhbmQgY2FsbHMgdGhlIGNvcnJlc3BvbmRpbmdcbiAgICogYHRlbXBsYXRlYCBvbiBlYWNoIGFuZCBldmVyeSBUUXVlcnkuXG4gICAqIEBwYXJhbSB0Vmlld1xuICAgKiBAcGFyYW0gdE5vZGVcbiAgICovXG4gIHRlbXBsYXRlKHRWaWV3OiBUVmlldywgdE5vZGU6IFROb2RlKTogdm9pZDtcblxuICAvKipcbiAgKiBBIHByb3h5IG1ldGhvZCB0aGF0IGl0ZXJhdGVzIG92ZXIgYWxsIHRoZSBUUXVlcmllcyBpbiBhIGdpdmVuIFRWaWV3IGFuZCBjYWxscyB0aGUgY29ycmVzcG9uZGluZ1xuICAgKiBgZW1iZWRkZWRUVmlld2Agb24gZWFjaCBhbmQgZXZlcnkgVFF1ZXJ5LlxuICAgKiBAcGFyYW0gdE5vZGVcbiAgICovXG4gIGVtYmVkZGVkVFZpZXcodE5vZGU6IFROb2RlKTogVFF1ZXJpZXN8bnVsbDtcbn1cblxuLyoqXG4gKiBBbiBpbnRlcmZhY2UgdGhhdCByZXByZXNlbnRzIHF1ZXJ5LXJlbGF0ZWQgaW5mb3JtYXRpb24gc3BlY2lmaWMgdG8gYSB2aWV3IGluc3RhbmNlLiBNb3N0IG5vdGFibHlcbiAqIGl0IGNvbnRhaW5zOlxuICogLSBtYXRlcmlhbGl6ZWQgcXVlcnkgbWF0Y2hlcztcbiAqIC0gYSBwb2ludGVyIHRvIGEgUXVlcnlMaXN0IHdoZXJlIG1hdGVyaWFsaXplZCBxdWVyeSByZXN1bHRzIHNob3VsZCBiZSByZXBvcnRlZC5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBMUXVlcnk8VD4ge1xuICAvKipcbiAgICogTWF0ZXJpYWxpemVkIHF1ZXJ5IG1hdGNoZXMgZm9yIGEgZ2l2ZW4gdmlldyBvbmx5ICghKS4gUmVzdWx0cyBhcmUgaW5pdGlhbGl6ZWQgbGF6aWx5IHNvIHRoZVxuICAgKiBhcnJheSBvZiBtYXRjaGVzIGlzIHNldCB0byBgbnVsbGAgaW5pdGlhbGx5LlxuICAgKi9cbiAgbWF0Y2hlczogKFR8bnVsbClbXXxudWxsO1xuXG4gIC8qKlxuICAgKiBBIFF1ZXJ5TGlzdCB3aGVyZSBtYXRlcmlhbGl6ZWQgcXVlcnkgcmVzdWx0cyBzaG91bGQgYmUgcmVwb3J0ZWQuXG4gICAqL1xuICBxdWVyeUxpc3Q6IFF1ZXJ5TGlzdDxUPjtcblxuICAvKipcbiAgICogQ2xvbmVzIGFuIExRdWVyeSBmb3IgYW4gZW1iZWRkZWQgdmlldy4gQSBjbG9uZWQgcXVlcnkgc2hhcmVzIHRoZSBzYW1lIGBRdWVyeUxpc3RgIGJ1dCBoYXMgYVxuICAgKiBzZXBhcmF0ZSBjb2xsZWN0aW9uIG9mIG1hdGVyaWFsaXplZCBtYXRjaGVzLlxuICAgKi9cbiAgY2xvbmUoKTogTFF1ZXJ5PFQ+O1xuXG4gIC8qKlxuICAgKiBDYWxsZWQgd2hlbiBhbiBlbWJlZGRlZCB2aWV3LCBpbXBhY3RpbmcgcmVzdWx0cyBvZiB0aGlzIHF1ZXJ5LCBpcyBpbnNlcnRlZCBvciByZW1vdmVkLlxuICAgKi9cbiAgc2V0RGlydHkoKTogdm9pZDtcbn1cblxuLyoqXG4gKiBsUXVlcmllcyByZXByZXNlbnQgYSBjb2xsZWN0aW9uIG9mIGluZGl2aWR1YWwgTFF1ZXJ5IG9iamVjdHMgdHJhY2tlZCBpbiBhIGdpdmVuIHZpZXcuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgTFF1ZXJpZXMge1xuICAvKipcbiAgICogQSBjb2xsZWN0aW9uIG9mIHF1ZXJpZXMgdHJhY2tlZCBpbiBhIGdpdmVuIHZpZXcuXG4gICAqL1xuICBxdWVyaWVzOiBMUXVlcnk8YW55PltdO1xuXG4gIC8qKlxuICAgKiBBIG1ldGhvZCBjYWxsZWQgd2hlbiBhIG5ldyBlbWJlZGRlZCB2aWV3IGlzIGNyZWF0ZWQuIEFzIGEgcmVzdWx0IGEgc2V0IG9mIExRdWVyaWVzIGFwcGxpY2FibGVcbiAgICogZm9yIGEgbmV3IGVtYmVkZGVkIHZpZXcgaXMgaW5zdGFudGlhdGVkIChjbG9uZWQpIGZyb20gdGhlIGRlY2xhcmF0aW9uIHZpZXcuXG4gICAqIEBwYXJhbSB0Vmlld1xuICAgKi9cbiAgY3JlYXRlRW1iZWRkZWRWaWV3KHRWaWV3OiBUVmlldyk6IExRdWVyaWVzfG51bGw7XG5cbiAgLyoqXG4gICAqIEEgbWV0aG9kIGNhbGxlZCB3aGVuIGFuIGVtYmVkZGVkIHZpZXcgaXMgaW5zZXJ0ZWQgaW50byBhIGNvbnRhaW5lci4gQXMgYSByZXN1bHQgYWxsIGltcGFjdGVkXG4gICAqIGBMUXVlcnlgIG9iamVjdHMgKGFuZCBhc3NvY2lhdGVkIGBRdWVyeUxpc3RgKSBhcmUgbWFya2VkIGFzIGRpcnR5LlxuICAgKiBAcGFyYW0gdFZpZXdcbiAgICovXG4gIGluc2VydFZpZXcodFZpZXc6IFRWaWV3KTogdm9pZDtcblxuICAvKipcbiAgICogQSBtZXRob2QgY2FsbGVkIHdoZW4gYW4gZW1iZWRkZWQgdmlldyBpcyBkZXRhY2hlZCBmcm9tIGEgY29udGFpbmVyLiBBcyBhIHJlc3VsdCBhbGwgaW1wYWN0ZWRcbiAgICogYExRdWVyeWAgb2JqZWN0cyAoYW5kIGFzc29jaWF0ZWQgYFF1ZXJ5TGlzdGApIGFyZSBtYXJrZWQgYXMgZGlydHkuXG4gICAqIEBwYXJhbSB0Vmlld1xuICAgKi9cbiAgZGV0YWNoVmlldyh0VmlldzogVFZpZXcpOiB2b2lkO1xufVxuXG5cbi8vIE5vdGU6IFRoaXMgaGFjayBpcyBuZWNlc3Nhcnkgc28gd2UgZG9uJ3QgZXJyb25lb3VzbHkgZ2V0IGEgY2lyY3VsYXIgZGVwZW5kZW5jeVxuLy8gZmFpbHVyZSBiYXNlZCBvbiB0eXBlcy5cbmV4cG9ydCBjb25zdCB1bnVzZWRWYWx1ZUV4cG9ydFRvUGxhY2F0ZUFqZCA9IDE7XG4iXX0=
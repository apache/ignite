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
import { ɵɵdefineInjectable } from '../../di/interface/defs';
import { Optional, SkipSelf } from '../../di/metadata';
import { DefaultIterableDifferFactory } from '../differs/default_iterable_differ';
/**
 * A strategy for tracking changes over time to an iterable. Used by {\@link NgForOf} to
 * respond to changes in an iterable by effecting equivalent changes in the DOM.
 *
 * \@publicApi
 * @record
 * @template V
 */
export function IterableDiffer() { }
if (false) {
    /**
     * Compute a difference between the previous state and the new `object` state.
     *
     * @param {?} object containing the new value.
     * @return {?} an object describing the difference. The return value is only valid until the next
     * `diff()` invocation.
     */
    IterableDiffer.prototype.diff = function (object) { };
}
/**
 * An object describing the changes in the `Iterable` collection since last time
 * `IterableDiffer#diff()` was invoked.
 *
 * \@publicApi
 * @record
 * @template V
 */
export function IterableChanges() { }
if (false) {
    /**
     * Iterate over all changes. `IterableChangeRecord` will contain information about changes
     * to each item.
     * @param {?} fn
     * @return {?}
     */
    IterableChanges.prototype.forEachItem = function (fn) { };
    /**
     * Iterate over a set of operations which when applied to the original `Iterable` will produce the
     * new `Iterable`.
     *
     * NOTE: These are not necessarily the actual operations which were applied to the original
     * `Iterable`, rather these are a set of computed operations which may not be the same as the
     * ones applied.
     *
     * @param {?} fn
     * @return {?}
     */
    IterableChanges.prototype.forEachOperation = function (fn) { };
    /**
     * Iterate over changes in the order of original `Iterable` showing where the original items
     * have moved.
     * @param {?} fn
     * @return {?}
     */
    IterableChanges.prototype.forEachPreviousItem = function (fn) { };
    /**
     * Iterate over all added items.
     * @param {?} fn
     * @return {?}
     */
    IterableChanges.prototype.forEachAddedItem = function (fn) { };
    /**
     * Iterate over all moved items.
     * @param {?} fn
     * @return {?}
     */
    IterableChanges.prototype.forEachMovedItem = function (fn) { };
    /**
     * Iterate over all removed items.
     * @param {?} fn
     * @return {?}
     */
    IterableChanges.prototype.forEachRemovedItem = function (fn) { };
    /**
     * Iterate over all items which had their identity (as computed by the `TrackByFunction`)
     * changed.
     * @param {?} fn
     * @return {?}
     */
    IterableChanges.prototype.forEachIdentityChange = function (fn) { };
}
/**
 * Record representing the item change information.
 *
 * \@publicApi
 * @record
 * @template V
 */
export function IterableChangeRecord() { }
if (false) {
    /**
     * Current index of the item in `Iterable` or null if removed.
     * @type {?}
     */
    IterableChangeRecord.prototype.currentIndex;
    /**
     * Previous index of the item in `Iterable` or null if added.
     * @type {?}
     */
    IterableChangeRecord.prototype.previousIndex;
    /**
     * The item.
     * @type {?}
     */
    IterableChangeRecord.prototype.item;
    /**
     * Track by identity as computed by the `TrackByFunction`.
     * @type {?}
     */
    IterableChangeRecord.prototype.trackById;
}
/**
 * @deprecated v4.0.0 - Use IterableChangeRecord instead.
 * \@publicApi
 * @record
 * @template V
 */
export function CollectionChangeRecord() { }
/**
 * An optional function passed into the `NgForOf` directive that defines how to track
 * changes for items in an iterable.
 * The function takes the iteration index and item ID.
 * When supplied, Angular tracks changes by the return value of the function.
 *
 * \@publicApi
 * @record
 * @template T
 */
export function TrackByFunction() { }
/**
 * Provides a factory for {\@link IterableDiffer}.
 *
 * \@publicApi
 * @record
 */
export function IterableDifferFactory() { }
if (false) {
    /**
     * @param {?} objects
     * @return {?}
     */
    IterableDifferFactory.prototype.supports = function (objects) { };
    /**
     * @template V
     * @param {?=} trackByFn
     * @return {?}
     */
    IterableDifferFactory.prototype.create = function (trackByFn) { };
}
/**
 * A repository of different iterable diffing strategies used by NgFor, NgClass, and others.
 *
 * \@publicApi
 */
export class IterableDiffers {
    /**
     * @param {?} factories
     */
    constructor(factories) { this.factories = factories; }
    /**
     * @param {?} factories
     * @param {?=} parent
     * @return {?}
     */
    static create(factories, parent) {
        if (parent != null) {
            /** @type {?} */
            const copied = parent.factories.slice();
            factories = factories.concat(copied);
        }
        return new IterableDiffers(factories);
    }
    /**
     * Takes an array of {\@link IterableDifferFactory} and returns a provider used to extend the
     * inherited {\@link IterableDiffers} instance with the provided factories and return a new
     * {\@link IterableDiffers} instance.
     *
     * \@usageNotes
     * ### Example
     *
     * The following example shows how to extend an existing list of factories,
     * which will only be applied to the injector for this component and its children.
     * This step is all that's required to make a new {\@link IterableDiffer} available.
     *
     * ```
     * \@Component({
     *   viewProviders: [
     *     IterableDiffers.extend([new ImmutableListDiffer()])
     *   ]
     * })
     * ```
     * @param {?} factories
     * @return {?}
     */
    static extend(factories) {
        return {
            provide: IterableDiffers,
            useFactory: (/**
             * @param {?} parent
             * @return {?}
             */
            (parent) => {
                if (!parent) {
                    // Typically would occur when calling IterableDiffers.extend inside of dependencies passed
                    // to
                    // bootstrap(), which would override default pipes instead of extending them.
                    throw new Error('Cannot extend IterableDiffers without a parent injector');
                }
                return IterableDiffers.create(factories, parent);
            }),
            // Dependency technically isn't optional, but we can provide a better error message this way.
            deps: [[IterableDiffers, new SkipSelf(), new Optional()]]
        };
    }
    /**
     * @param {?} iterable
     * @return {?}
     */
    find(iterable) {
        /** @type {?} */
        const factory = this.factories.find((/**
         * @param {?} f
         * @return {?}
         */
        f => f.supports(iterable)));
        if (factory != null) {
            return factory;
        }
        else {
            throw new Error(`Cannot find a differ supporting object '${iterable}' of type '${getTypeNameForDebugging(iterable)}'`);
        }
    }
}
/** @nocollapse */
/** @nocollapse */ IterableDiffers.ngInjectableDef = ɵɵdefineInjectable({
    token: IterableDiffers,
    providedIn: 'root',
    factory: (/**
     * @nocollapse @return {?}
     */
    () => new IterableDiffers([new DefaultIterableDifferFactory()]))
});
if (false) {
    /**
     * @nocollapse
     * @type {?}
     */
    IterableDiffers.ngInjectableDef;
    /**
     * @deprecated v4.0.0 - Should be private
     * @type {?}
     */
    IterableDiffers.prototype.factories;
}
/**
 * @param {?} type
 * @return {?}
 */
export function getTypeNameForDebugging(type) {
    return type['name'] || typeof type;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaXRlcmFibGVfZGlmZmVycy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL2NoYW5nZV9kZXRlY3Rpb24vZGlmZmVycy9pdGVyYWJsZV9kaWZmZXJzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLGtCQUFrQixFQUFDLE1BQU0seUJBQXlCLENBQUM7QUFFM0QsT0FBTyxFQUFDLFFBQVEsRUFBRSxRQUFRLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUNyRCxPQUFPLEVBQUMsNEJBQTRCLEVBQUMsTUFBTSxvQ0FBb0MsQ0FBQzs7Ozs7Ozs7O0FBaUJoRixvQ0FTQzs7Ozs7Ozs7O0lBREMsc0RBQXFEOzs7Ozs7Ozs7O0FBU3ZELHFDQThDQzs7Ozs7Ozs7SUF6Q0MsMERBQWlFOzs7Ozs7Ozs7Ozs7SUFrQmpFLCtEQUdtRDs7Ozs7OztJQU1uRCxrRUFBeUU7Ozs7OztJQUd6RSwrREFBc0U7Ozs7OztJQUd0RSwrREFBc0U7Ozs7OztJQUd0RSxpRUFBd0U7Ozs7Ozs7SUFJeEUsb0VBQTJFOzs7Ozs7Ozs7QUFRN0UsMENBWUM7Ozs7OztJQVZDLDRDQUFtQzs7Ozs7SUFHbkMsNkNBQW9DOzs7OztJQUdwQyxvQ0FBaUI7Ozs7O0lBR2pCLHlDQUF3Qjs7Ozs7Ozs7QUFPMUIsNENBQTZFOzs7Ozs7Ozs7OztBQVU3RSxxQ0FBc0U7Ozs7Ozs7QUFPdEUsMkNBR0M7Ozs7OztJQUZDLGtFQUFnQzs7Ozs7O0lBQ2hDLGtFQUE2RDs7Ozs7OztBQVEvRCxNQUFNLE9BQU8sZUFBZTs7OztJQVkxQixZQUFZLFNBQWtDLElBQUksSUFBSSxDQUFDLFNBQVMsR0FBRyxTQUFTLENBQUMsQ0FBQyxDQUFDOzs7Ozs7SUFFL0UsTUFBTSxDQUFDLE1BQU0sQ0FBQyxTQUFrQyxFQUFFLE1BQXdCO1FBQ3hFLElBQUksTUFBTSxJQUFJLElBQUksRUFBRTs7a0JBQ1osTUFBTSxHQUFHLE1BQU0sQ0FBQyxTQUFTLENBQUMsS0FBSyxFQUFFO1lBQ3ZDLFNBQVMsR0FBRyxTQUFTLENBQUMsTUFBTSxDQUFDLE1BQU0sQ0FBQyxDQUFDO1NBQ3RDO1FBRUQsT0FBTyxJQUFJLGVBQWUsQ0FBQyxTQUFTLENBQUMsQ0FBQztJQUN4QyxDQUFDOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztJQXNCRCxNQUFNLENBQUMsTUFBTSxDQUFDLFNBQWtDO1FBQzlDLE9BQU87WUFDTCxPQUFPLEVBQUUsZUFBZTtZQUN4QixVQUFVOzs7O1lBQUUsQ0FBQyxNQUF1QixFQUFFLEVBQUU7Z0JBQ3RDLElBQUksQ0FBQyxNQUFNLEVBQUU7b0JBQ1gsMEZBQTBGO29CQUMxRixLQUFLO29CQUNMLDZFQUE2RTtvQkFDN0UsTUFBTSxJQUFJLEtBQUssQ0FBQyx5REFBeUQsQ0FBQyxDQUFDO2lCQUM1RTtnQkFDRCxPQUFPLGVBQWUsQ0FBQyxNQUFNLENBQUMsU0FBUyxFQUFFLE1BQU0sQ0FBQyxDQUFDO1lBQ25ELENBQUMsQ0FBQTs7WUFFRCxJQUFJLEVBQUUsQ0FBQyxDQUFDLGVBQWUsRUFBRSxJQUFJLFFBQVEsRUFBRSxFQUFFLElBQUksUUFBUSxFQUFFLENBQUMsQ0FBQztTQUMxRCxDQUFDO0lBQ0osQ0FBQzs7Ozs7SUFFRCxJQUFJLENBQUMsUUFBYTs7Y0FDVixPQUFPLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxJQUFJOzs7O1FBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUMsUUFBUSxDQUFDLFFBQVEsQ0FBQyxFQUFDO1FBQzlELElBQUksT0FBTyxJQUFJLElBQUksRUFBRTtZQUNuQixPQUFPLE9BQU8sQ0FBQztTQUNoQjthQUFNO1lBQ0wsTUFBTSxJQUFJLEtBQUssQ0FDWCwyQ0FBMkMsUUFBUSxjQUFjLHVCQUF1QixDQUFDLFFBQVEsQ0FBQyxHQUFHLENBQUMsQ0FBQztTQUM1RztJQUNILENBQUM7OztBQWxFTSwrQkFBZSxHQUFHLGtCQUFrQixDQUFDO0lBQzFDLEtBQUssRUFBRSxlQUFlO0lBQ3RCLFVBQVUsRUFBRSxNQUFNO0lBQ2xCLE9BQU87OztJQUFFLEdBQUcsRUFBRSxDQUFDLElBQUksZUFBZSxDQUFDLENBQUMsSUFBSSw0QkFBNEIsRUFBRSxDQUFDLENBQUMsQ0FBQTtDQUN6RSxDQUFDLENBQUM7Ozs7OztJQUpILGdDQUlHOzs7OztJQUtILG9DQUFtQzs7Ozs7O0FBNERyQyxNQUFNLFVBQVUsdUJBQXVCLENBQUMsSUFBUztJQUMvQyxPQUFPLElBQUksQ0FBQyxNQUFNLENBQUMsSUFBSSxPQUFPLElBQUksQ0FBQztBQUNyQyxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge8m1ybVkZWZpbmVJbmplY3RhYmxlfSBmcm9tICcuLi8uLi9kaS9pbnRlcmZhY2UvZGVmcyc7XG5pbXBvcnQge1N0YXRpY1Byb3ZpZGVyfSBmcm9tICcuLi8uLi9kaS9pbnRlcmZhY2UvcHJvdmlkZXInO1xuaW1wb3J0IHtPcHRpb25hbCwgU2tpcFNlbGZ9IGZyb20gJy4uLy4uL2RpL21ldGFkYXRhJztcbmltcG9ydCB7RGVmYXVsdEl0ZXJhYmxlRGlmZmVyRmFjdG9yeX0gZnJvbSAnLi4vZGlmZmVycy9kZWZhdWx0X2l0ZXJhYmxlX2RpZmZlcic7XG5cblxuXG4vKipcbiAqIEEgdHlwZSBkZXNjcmliaW5nIHN1cHBvcnRlZCBpdGVyYWJsZSB0eXBlcy5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCB0eXBlIE5nSXRlcmFibGU8VD4gPSBBcnJheTxUPnwgSXRlcmFibGU8VD47XG5cbi8qKlxuICogQSBzdHJhdGVneSBmb3IgdHJhY2tpbmcgY2hhbmdlcyBvdmVyIHRpbWUgdG8gYW4gaXRlcmFibGUuIFVzZWQgYnkge0BsaW5rIE5nRm9yT2Z9IHRvXG4gKiByZXNwb25kIHRvIGNoYW5nZXMgaW4gYW4gaXRlcmFibGUgYnkgZWZmZWN0aW5nIGVxdWl2YWxlbnQgY2hhbmdlcyBpbiB0aGUgRE9NLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBJdGVyYWJsZURpZmZlcjxWPiB7XG4gIC8qKlxuICAgKiBDb21wdXRlIGEgZGlmZmVyZW5jZSBiZXR3ZWVuIHRoZSBwcmV2aW91cyBzdGF0ZSBhbmQgdGhlIG5ldyBgb2JqZWN0YCBzdGF0ZS5cbiAgICpcbiAgICogQHBhcmFtIG9iamVjdCBjb250YWluaW5nIHRoZSBuZXcgdmFsdWUuXG4gICAqIEByZXR1cm5zIGFuIG9iamVjdCBkZXNjcmliaW5nIHRoZSBkaWZmZXJlbmNlLiBUaGUgcmV0dXJuIHZhbHVlIGlzIG9ubHkgdmFsaWQgdW50aWwgdGhlIG5leHRcbiAgICogYGRpZmYoKWAgaW52b2NhdGlvbi5cbiAgICovXG4gIGRpZmYob2JqZWN0OiBOZ0l0ZXJhYmxlPFY+KTogSXRlcmFibGVDaGFuZ2VzPFY+fG51bGw7XG59XG5cbi8qKlxuICogQW4gb2JqZWN0IGRlc2NyaWJpbmcgdGhlIGNoYW5nZXMgaW4gdGhlIGBJdGVyYWJsZWAgY29sbGVjdGlvbiBzaW5jZSBsYXN0IHRpbWVcbiAqIGBJdGVyYWJsZURpZmZlciNkaWZmKClgIHdhcyBpbnZva2VkLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBJdGVyYWJsZUNoYW5nZXM8Vj4ge1xuICAvKipcbiAgICogSXRlcmF0ZSBvdmVyIGFsbCBjaGFuZ2VzLiBgSXRlcmFibGVDaGFuZ2VSZWNvcmRgIHdpbGwgY29udGFpbiBpbmZvcm1hdGlvbiBhYm91dCBjaGFuZ2VzXG4gICAqIHRvIGVhY2ggaXRlbS5cbiAgICovXG4gIGZvckVhY2hJdGVtKGZuOiAocmVjb3JkOiBJdGVyYWJsZUNoYW5nZVJlY29yZDxWPikgPT4gdm9pZCk6IHZvaWQ7XG5cbiAgLyoqXG4gICAqIEl0ZXJhdGUgb3ZlciBhIHNldCBvZiBvcGVyYXRpb25zIHdoaWNoIHdoZW4gYXBwbGllZCB0byB0aGUgb3JpZ2luYWwgYEl0ZXJhYmxlYCB3aWxsIHByb2R1Y2UgdGhlXG4gICAqIG5ldyBgSXRlcmFibGVgLlxuICAgKlxuICAgKiBOT1RFOiBUaGVzZSBhcmUgbm90IG5lY2Vzc2FyaWx5IHRoZSBhY3R1YWwgb3BlcmF0aW9ucyB3aGljaCB3ZXJlIGFwcGxpZWQgdG8gdGhlIG9yaWdpbmFsXG4gICAqIGBJdGVyYWJsZWAsIHJhdGhlciB0aGVzZSBhcmUgYSBzZXQgb2YgY29tcHV0ZWQgb3BlcmF0aW9ucyB3aGljaCBtYXkgbm90IGJlIHRoZSBzYW1lIGFzIHRoZVxuICAgKiBvbmVzIGFwcGxpZWQuXG4gICAqXG4gICAqIEBwYXJhbSByZWNvcmQgQSBjaGFuZ2Ugd2hpY2ggbmVlZHMgdG8gYmUgYXBwbGllZFxuICAgKiBAcGFyYW0gcHJldmlvdXNJbmRleCBUaGUgYEl0ZXJhYmxlQ2hhbmdlUmVjb3JkI3ByZXZpb3VzSW5kZXhgIG9mIHRoZSBgcmVjb3JkYCByZWZlcnMgdG8gdGhlXG4gICAqICAgICAgICBvcmlnaW5hbCBgSXRlcmFibGVgIGxvY2F0aW9uLCB3aGVyZSBhcyBgcHJldmlvdXNJbmRleGAgcmVmZXJzIHRvIHRoZSB0cmFuc2llbnQgbG9jYXRpb25cbiAgICogICAgICAgIG9mIHRoZSBpdGVtLCBhZnRlciBhcHBseWluZyB0aGUgb3BlcmF0aW9ucyB1cCB0byB0aGlzIHBvaW50LlxuICAgKiBAcGFyYW0gY3VycmVudEluZGV4IFRoZSBgSXRlcmFibGVDaGFuZ2VSZWNvcmQjY3VycmVudEluZGV4YCBvZiB0aGUgYHJlY29yZGAgcmVmZXJzIHRvIHRoZVxuICAgKiAgICAgICAgb3JpZ2luYWwgYEl0ZXJhYmxlYCBsb2NhdGlvbiwgd2hlcmUgYXMgYGN1cnJlbnRJbmRleGAgcmVmZXJzIHRvIHRoZSB0cmFuc2llbnQgbG9jYXRpb25cbiAgICogICAgICAgIG9mIHRoZSBpdGVtLCBhZnRlciBhcHBseWluZyB0aGUgb3BlcmF0aW9ucyB1cCB0byB0aGlzIHBvaW50LlxuICAgKi9cbiAgZm9yRWFjaE9wZXJhdGlvbihcbiAgICAgIGZuOlxuICAgICAgICAgIChyZWNvcmQ6IEl0ZXJhYmxlQ2hhbmdlUmVjb3JkPFY+LCBwcmV2aW91c0luZGV4OiBudW1iZXJ8bnVsbCxcbiAgICAgICAgICAgY3VycmVudEluZGV4OiBudW1iZXJ8bnVsbCkgPT4gdm9pZCk6IHZvaWQ7XG5cbiAgLyoqXG4gICAqIEl0ZXJhdGUgb3ZlciBjaGFuZ2VzIGluIHRoZSBvcmRlciBvZiBvcmlnaW5hbCBgSXRlcmFibGVgIHNob3dpbmcgd2hlcmUgdGhlIG9yaWdpbmFsIGl0ZW1zXG4gICAqIGhhdmUgbW92ZWQuXG4gICAqL1xuICBmb3JFYWNoUHJldmlvdXNJdGVtKGZuOiAocmVjb3JkOiBJdGVyYWJsZUNoYW5nZVJlY29yZDxWPikgPT4gdm9pZCk6IHZvaWQ7XG5cbiAgLyoqIEl0ZXJhdGUgb3ZlciBhbGwgYWRkZWQgaXRlbXMuICovXG4gIGZvckVhY2hBZGRlZEl0ZW0oZm46IChyZWNvcmQ6IEl0ZXJhYmxlQ2hhbmdlUmVjb3JkPFY+KSA9PiB2b2lkKTogdm9pZDtcblxuICAvKiogSXRlcmF0ZSBvdmVyIGFsbCBtb3ZlZCBpdGVtcy4gKi9cbiAgZm9yRWFjaE1vdmVkSXRlbShmbjogKHJlY29yZDogSXRlcmFibGVDaGFuZ2VSZWNvcmQ8Vj4pID0+IHZvaWQpOiB2b2lkO1xuXG4gIC8qKiBJdGVyYXRlIG92ZXIgYWxsIHJlbW92ZWQgaXRlbXMuICovXG4gIGZvckVhY2hSZW1vdmVkSXRlbShmbjogKHJlY29yZDogSXRlcmFibGVDaGFuZ2VSZWNvcmQ8Vj4pID0+IHZvaWQpOiB2b2lkO1xuXG4gIC8qKiBJdGVyYXRlIG92ZXIgYWxsIGl0ZW1zIHdoaWNoIGhhZCB0aGVpciBpZGVudGl0eSAoYXMgY29tcHV0ZWQgYnkgdGhlIGBUcmFja0J5RnVuY3Rpb25gKVxuICAgKiBjaGFuZ2VkLiAqL1xuICBmb3JFYWNoSWRlbnRpdHlDaGFuZ2UoZm46IChyZWNvcmQ6IEl0ZXJhYmxlQ2hhbmdlUmVjb3JkPFY+KSA9PiB2b2lkKTogdm9pZDtcbn1cblxuLyoqXG4gKiBSZWNvcmQgcmVwcmVzZW50aW5nIHRoZSBpdGVtIGNoYW5nZSBpbmZvcm1hdGlvbi5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgSXRlcmFibGVDaGFuZ2VSZWNvcmQ8Vj4ge1xuICAvKiogQ3VycmVudCBpbmRleCBvZiB0aGUgaXRlbSBpbiBgSXRlcmFibGVgIG9yIG51bGwgaWYgcmVtb3ZlZC4gKi9cbiAgcmVhZG9ubHkgY3VycmVudEluZGV4OiBudW1iZXJ8bnVsbDtcblxuICAvKiogUHJldmlvdXMgaW5kZXggb2YgdGhlIGl0ZW0gaW4gYEl0ZXJhYmxlYCBvciBudWxsIGlmIGFkZGVkLiAqL1xuICByZWFkb25seSBwcmV2aW91c0luZGV4OiBudW1iZXJ8bnVsbDtcblxuICAvKiogVGhlIGl0ZW0uICovXG4gIHJlYWRvbmx5IGl0ZW06IFY7XG5cbiAgLyoqIFRyYWNrIGJ5IGlkZW50aXR5IGFzIGNvbXB1dGVkIGJ5IHRoZSBgVHJhY2tCeUZ1bmN0aW9uYC4gKi9cbiAgcmVhZG9ubHkgdHJhY2tCeUlkOiBhbnk7XG59XG5cbi8qKlxuICogQGRlcHJlY2F0ZWQgdjQuMC4wIC0gVXNlIEl0ZXJhYmxlQ2hhbmdlUmVjb3JkIGluc3RlYWQuXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgQ29sbGVjdGlvbkNoYW5nZVJlY29yZDxWPiBleHRlbmRzIEl0ZXJhYmxlQ2hhbmdlUmVjb3JkPFY+IHt9XG5cbi8qKlxuICogQW4gb3B0aW9uYWwgZnVuY3Rpb24gcGFzc2VkIGludG8gdGhlIGBOZ0Zvck9mYCBkaXJlY3RpdmUgdGhhdCBkZWZpbmVzIGhvdyB0byB0cmFja1xuICogY2hhbmdlcyBmb3IgaXRlbXMgaW4gYW4gaXRlcmFibGUuXG4gKiBUaGUgZnVuY3Rpb24gdGFrZXMgdGhlIGl0ZXJhdGlvbiBpbmRleCBhbmQgaXRlbSBJRC5cbiAqIFdoZW4gc3VwcGxpZWQsIEFuZ3VsYXIgdHJhY2tzIGNoYW5nZXMgYnkgdGhlIHJldHVybiB2YWx1ZSBvZiB0aGUgZnVuY3Rpb24uXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgaW50ZXJmYWNlIFRyYWNrQnlGdW5jdGlvbjxUPiB7IChpbmRleDogbnVtYmVyLCBpdGVtOiBUKTogYW55OyB9XG5cbi8qKlxuICogUHJvdmlkZXMgYSBmYWN0b3J5IGZvciB7QGxpbmsgSXRlcmFibGVEaWZmZXJ9LlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBJdGVyYWJsZURpZmZlckZhY3Rvcnkge1xuICBzdXBwb3J0cyhvYmplY3RzOiBhbnkpOiBib29sZWFuO1xuICBjcmVhdGU8Vj4odHJhY2tCeUZuPzogVHJhY2tCeUZ1bmN0aW9uPFY+KTogSXRlcmFibGVEaWZmZXI8Vj47XG59XG5cbi8qKlxuICogQSByZXBvc2l0b3J5IG9mIGRpZmZlcmVudCBpdGVyYWJsZSBkaWZmaW5nIHN0cmF0ZWdpZXMgdXNlZCBieSBOZ0ZvciwgTmdDbGFzcywgYW5kIG90aGVycy5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBJdGVyYWJsZURpZmZlcnMge1xuICAvKiogQG5vY29sbGFwc2UgKi9cbiAgc3RhdGljIG5nSW5qZWN0YWJsZURlZiA9IMm1ybVkZWZpbmVJbmplY3RhYmxlKHtcbiAgICB0b2tlbjogSXRlcmFibGVEaWZmZXJzLFxuICAgIHByb3ZpZGVkSW46ICdyb290JyxcbiAgICBmYWN0b3J5OiAoKSA9PiBuZXcgSXRlcmFibGVEaWZmZXJzKFtuZXcgRGVmYXVsdEl0ZXJhYmxlRGlmZmVyRmFjdG9yeSgpXSlcbiAgfSk7XG5cbiAgLyoqXG4gICAqIEBkZXByZWNhdGVkIHY0LjAuMCAtIFNob3VsZCBiZSBwcml2YXRlXG4gICAqL1xuICBmYWN0b3JpZXM6IEl0ZXJhYmxlRGlmZmVyRmFjdG9yeVtdO1xuICBjb25zdHJ1Y3RvcihmYWN0b3JpZXM6IEl0ZXJhYmxlRGlmZmVyRmFjdG9yeVtdKSB7IHRoaXMuZmFjdG9yaWVzID0gZmFjdG9yaWVzOyB9XG5cbiAgc3RhdGljIGNyZWF0ZShmYWN0b3JpZXM6IEl0ZXJhYmxlRGlmZmVyRmFjdG9yeVtdLCBwYXJlbnQ/OiBJdGVyYWJsZURpZmZlcnMpOiBJdGVyYWJsZURpZmZlcnMge1xuICAgIGlmIChwYXJlbnQgIT0gbnVsbCkge1xuICAgICAgY29uc3QgY29waWVkID0gcGFyZW50LmZhY3Rvcmllcy5zbGljZSgpO1xuICAgICAgZmFjdG9yaWVzID0gZmFjdG9yaWVzLmNvbmNhdChjb3BpZWQpO1xuICAgIH1cblxuICAgIHJldHVybiBuZXcgSXRlcmFibGVEaWZmZXJzKGZhY3Rvcmllcyk7XG4gIH1cblxuICAvKipcbiAgICogVGFrZXMgYW4gYXJyYXkgb2Yge0BsaW5rIEl0ZXJhYmxlRGlmZmVyRmFjdG9yeX0gYW5kIHJldHVybnMgYSBwcm92aWRlciB1c2VkIHRvIGV4dGVuZCB0aGVcbiAgICogaW5oZXJpdGVkIHtAbGluayBJdGVyYWJsZURpZmZlcnN9IGluc3RhbmNlIHdpdGggdGhlIHByb3ZpZGVkIGZhY3RvcmllcyBhbmQgcmV0dXJuIGEgbmV3XG4gICAqIHtAbGluayBJdGVyYWJsZURpZmZlcnN9IGluc3RhbmNlLlxuICAgKlxuICAgKiBAdXNhZ2VOb3Rlc1xuICAgKiAjIyMgRXhhbXBsZVxuICAgKlxuICAgKiBUaGUgZm9sbG93aW5nIGV4YW1wbGUgc2hvd3MgaG93IHRvIGV4dGVuZCBhbiBleGlzdGluZyBsaXN0IG9mIGZhY3RvcmllcyxcbiAgICogd2hpY2ggd2lsbCBvbmx5IGJlIGFwcGxpZWQgdG8gdGhlIGluamVjdG9yIGZvciB0aGlzIGNvbXBvbmVudCBhbmQgaXRzIGNoaWxkcmVuLlxuICAgKiBUaGlzIHN0ZXAgaXMgYWxsIHRoYXQncyByZXF1aXJlZCB0byBtYWtlIGEgbmV3IHtAbGluayBJdGVyYWJsZURpZmZlcn0gYXZhaWxhYmxlLlxuICAgKlxuICAgKiBgYGBcbiAgICogQENvbXBvbmVudCh7XG4gICAqICAgdmlld1Byb3ZpZGVyczogW1xuICAgKiAgICAgSXRlcmFibGVEaWZmZXJzLmV4dGVuZChbbmV3IEltbXV0YWJsZUxpc3REaWZmZXIoKV0pXG4gICAqICAgXVxuICAgKiB9KVxuICAgKiBgYGBcbiAgICovXG4gIHN0YXRpYyBleHRlbmQoZmFjdG9yaWVzOiBJdGVyYWJsZURpZmZlckZhY3RvcnlbXSk6IFN0YXRpY1Byb3ZpZGVyIHtcbiAgICByZXR1cm4ge1xuICAgICAgcHJvdmlkZTogSXRlcmFibGVEaWZmZXJzLFxuICAgICAgdXNlRmFjdG9yeTogKHBhcmVudDogSXRlcmFibGVEaWZmZXJzKSA9PiB7XG4gICAgICAgIGlmICghcGFyZW50KSB7XG4gICAgICAgICAgLy8gVHlwaWNhbGx5IHdvdWxkIG9jY3VyIHdoZW4gY2FsbGluZyBJdGVyYWJsZURpZmZlcnMuZXh0ZW5kIGluc2lkZSBvZiBkZXBlbmRlbmNpZXMgcGFzc2VkXG4gICAgICAgICAgLy8gdG9cbiAgICAgICAgICAvLyBib290c3RyYXAoKSwgd2hpY2ggd291bGQgb3ZlcnJpZGUgZGVmYXVsdCBwaXBlcyBpbnN0ZWFkIG9mIGV4dGVuZGluZyB0aGVtLlxuICAgICAgICAgIHRocm93IG5ldyBFcnJvcignQ2Fubm90IGV4dGVuZCBJdGVyYWJsZURpZmZlcnMgd2l0aG91dCBhIHBhcmVudCBpbmplY3RvcicpO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiBJdGVyYWJsZURpZmZlcnMuY3JlYXRlKGZhY3RvcmllcywgcGFyZW50KTtcbiAgICAgIH0sXG4gICAgICAvLyBEZXBlbmRlbmN5IHRlY2huaWNhbGx5IGlzbid0IG9wdGlvbmFsLCBidXQgd2UgY2FuIHByb3ZpZGUgYSBiZXR0ZXIgZXJyb3IgbWVzc2FnZSB0aGlzIHdheS5cbiAgICAgIGRlcHM6IFtbSXRlcmFibGVEaWZmZXJzLCBuZXcgU2tpcFNlbGYoKSwgbmV3IE9wdGlvbmFsKCldXVxuICAgIH07XG4gIH1cblxuICBmaW5kKGl0ZXJhYmxlOiBhbnkpOiBJdGVyYWJsZURpZmZlckZhY3Rvcnkge1xuICAgIGNvbnN0IGZhY3RvcnkgPSB0aGlzLmZhY3Rvcmllcy5maW5kKGYgPT4gZi5zdXBwb3J0cyhpdGVyYWJsZSkpO1xuICAgIGlmIChmYWN0b3J5ICE9IG51bGwpIHtcbiAgICAgIHJldHVybiBmYWN0b3J5O1xuICAgIH0gZWxzZSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgYENhbm5vdCBmaW5kIGEgZGlmZmVyIHN1cHBvcnRpbmcgb2JqZWN0ICcke2l0ZXJhYmxlfScgb2YgdHlwZSAnJHtnZXRUeXBlTmFtZUZvckRlYnVnZ2luZyhpdGVyYWJsZSl9J2ApO1xuICAgIH1cbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0VHlwZU5hbWVGb3JEZWJ1Z2dpbmcodHlwZTogYW55KTogc3RyaW5nIHtcbiAgcmV0dXJuIHR5cGVbJ25hbWUnXSB8fCB0eXBlb2YgdHlwZTtcbn1cbiJdfQ==
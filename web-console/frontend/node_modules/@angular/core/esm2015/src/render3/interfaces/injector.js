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
/** @type {?} */
export const TNODE = 8;
/** @type {?} */
export const PARENT_INJECTOR = 8;
/** @type {?} */
export const INJECTOR_BLOOM_PARENT_SIZE = 9;
/**
 * Represents a relative location of parent injector.
 *
 * The interfaces encodes number of parents `LView`s to traverse and index in the `LView`
 * pointing to the parent injector.
 * @record
 */
export function RelativeInjectorLocation() { }
if (false) {
    /** @type {?} */
    RelativeInjectorLocation.prototype.__brand__;
}
/** @enum {number} */
const RelativeInjectorLocationFlags = {
    InjectorIndexMask: 32767,
    ViewOffsetShift: 16,
    NO_PARENT: -1,
};
export { RelativeInjectorLocationFlags };
/** @type {?} */
export const NO_PARENT_INJECTOR = (/** @type {?} */ (-1));
/**
 * Each injector is saved in 9 contiguous slots in `LView` and 9 contiguous slots in
 * `TView.data`. This allows us to store information about the current node's tokens (which
 * can be shared in `TView`) as well as the tokens of its ancestor nodes (which cannot be
 * shared, so they live in `LView`).
 *
 * Each of these slots (aside from the last slot) contains a bloom filter. This bloom filter
 * determines whether a directive is available on the associated node or not. This prevents us
 * from searching the directives array at this level unless it's probable the directive is in it.
 *
 * See: https://en.wikipedia.org/wiki/Bloom_filter for more about bloom filters.
 *
 * Because all injectors have been flattened into `LView` and `TViewData`, they cannot typed
 * using interfaces as they were previously. The start index of each `LInjector` and `TInjector`
 * will differ based on where it is flattened into the main array, so it's not possible to know
 * the indices ahead of time and save their types here. The interfaces are still included here
 * for documentation purposes.
 *
 * export interface LInjector extends Array<any> {
 *
 *    // Cumulative bloom for directive IDs 0-31  (IDs are % BLOOM_SIZE)
 *    [0]: number;
 *
 *    // Cumulative bloom for directive IDs 32-63
 *    [1]: number;
 *
 *    // Cumulative bloom for directive IDs 64-95
 *    [2]: number;
 *
 *    // Cumulative bloom for directive IDs 96-127
 *    [3]: number;
 *
 *    // Cumulative bloom for directive IDs 128-159
 *    [4]: number;
 *
 *    // Cumulative bloom for directive IDs 160 - 191
 *    [5]: number;
 *
 *    // Cumulative bloom for directive IDs 192 - 223
 *    [6]: number;
 *
 *    // Cumulative bloom for directive IDs 224 - 255
 *    [7]: number;
 *
 *    // We need to store a reference to the injector's parent so DI can keep looking up
 *    // the injector tree until it finds the dependency it's looking for.
 *    [PARENT_INJECTOR]: number;
 * }
 *
 * export interface TInjector extends Array<any> {
 *
 *    // Shared node bloom for directive IDs 0-31  (IDs are % BLOOM_SIZE)
 *    [0]: number;
 *
 *    // Shared node bloom for directive IDs 32-63
 *    [1]: number;
 *
 *    // Shared node bloom for directive IDs 64-95
 *    [2]: number;
 *
 *    // Shared node bloom for directive IDs 96-127
 *    [3]: number;
 *
 *    // Shared node bloom for directive IDs 128-159
 *    [4]: number;
 *
 *    // Shared node bloom for directive IDs 160 - 191
 *    [5]: number;
 *
 *    // Shared node bloom for directive IDs 192 - 223
 *    [6]: number;
 *
 *    // Shared node bloom for directive IDs 224 - 255
 *    [7]: number;
 *
 *    // Necessary to find directive indices for a particular node.
 *    [TNODE]: TElementNode|TElementContainerNode|TContainerNode;
 *  }
 */
/**
 * Factory for creating instances of injectors in the NodeInjector.
 *
 * This factory is complicated by the fact that it can resolve `multi` factories as well.
 *
 * NOTE: Some of the fields are optional which means that this class has two hidden classes.
 * - One without `multi` support (most common)
 * - One with `multi` values, (rare).
 *
 * Since VMs can cache up to 4 inline hidden classes this is OK.
 *
 * - Single factory: Only `resolving` and `factory` is defined.
 * - `providers` factory: `componentProviders` is a number and `index = -1`.
 * - `viewProviders` factory: `componentProviders` is a number and `index` points to `providers`.
 */
export class NodeInjectorFactory {
    /**
     * @param {?} factory
     * @param {?} isViewProvider
     * @param {?} injectImplementation
     */
    constructor(factory, 
    /**
     * Set to `true` if the token is declared in `viewProviders` (or if it is component).
     */
    isViewProvider, injectImplementation) {
        this.factory = factory;
        /**
         * Marker set to true during factory invocation to see if we get into recursive loop.
         * Recursive loop causes an error to be displayed.
         */
        this.resolving = false;
        this.canSeeViewProviders = isViewProvider;
        this.injectImpl = injectImplementation;
    }
}
if (false) {
    /**
     * The inject implementation to be activated when using the factory.
     * @type {?}
     */
    NodeInjectorFactory.prototype.injectImpl;
    /**
     * Marker set to true during factory invocation to see if we get into recursive loop.
     * Recursive loop causes an error to be displayed.
     * @type {?}
     */
    NodeInjectorFactory.prototype.resolving;
    /**
     * Marks that the token can see other Tokens declared in `viewProviders` on the same node.
     * @type {?}
     */
    NodeInjectorFactory.prototype.canSeeViewProviders;
    /**
     * An array of factories to use in case of `multi` provider.
     * @type {?}
     */
    NodeInjectorFactory.prototype.multi;
    /**
     * Number of `multi`-providers which belong to the component.
     *
     * This is needed because when multiple components and directives declare the `multi` provider
     * they have to be concatenated in the correct order.
     *
     * Example:
     *
     * If we have a component and directive active an a single element as declared here
     * ```
     * component:
     *   provides: [ {provide: String, useValue: 'component', multi: true} ],
     *   viewProvides: [ {provide: String, useValue: 'componentView', multi: true} ],
     *
     * directive:
     *   provides: [ {provide: String, useValue: 'directive', multi: true} ],
     * ```
     *
     * Then the expected results are:
     *
     * ```
     * providers: ['component', 'directive']
     * viewProviders: ['component', 'componentView', 'directive']
     * ```
     *
     * The way to think about it is that the `viewProviders` have been inserted after the component
     * but before the directives, which is why we need to know how many `multi`s have been declared by
     * the component.
     * @type {?}
     */
    NodeInjectorFactory.prototype.componentProviders;
    /**
     * Current index of the Factory in the `data`. Needed for `viewProviders` and `providers` merging.
     * See `providerFactory`.
     * @type {?}
     */
    NodeInjectorFactory.prototype.index;
    /**
     * Because the same `multi` provider can be declared in `provides` and `viewProvides` it is
     * possible for `viewProvides` to shadow the `provides`. For this reason we store the
     * `provideFactory` of the `providers` so that `providers` can be extended with `viewProviders`.
     *
     * Example:
     *
     * Given:
     * ```
     * provides: [ {provide: String, useValue: 'all', multi: true} ],
     * viewProvides: [ {provide: String, useValue: 'viewOnly', multi: true} ],
     * ```
     *
     * We have to return `['all']` in case of content injection, but `['all', 'viewOnly']` in case
     * of view injection. We further have to make sure that the shared instances (in our case
     * `all`) are the exact same instance in both the content as well as the view injection. (We
     * have to make sure that we don't double instantiate.) For this reason the `viewProvides`
     * `Factory` has a pointer to the shadowed `provides` factory so that it can instantiate the
     * `providers` (`['all']`) and then extend it with `viewProviders` (`['all'] + ['viewOnly'] =
     * ['all', 'viewOnly']`).
     * @type {?}
     */
    NodeInjectorFactory.prototype.providerFactory;
    /**
     * Factory to invoke in order to create a new instance.
     * @type {?}
     */
    NodeInjectorFactory.prototype.factory;
}
/**
 * @param {?} obj
 * @return {?}
 */
export function isFactory(obj) {
    // See: https://jsperf.com/instanceof-vs-getprototypeof
    return obj !== null && typeof obj == 'object' &&
        Object.getPrototypeOf(obj) == NodeInjectorFactory.prototype;
}
// Note: This hack is necessary so we don't erroneously get a circular dependency
// failure based on types.
/** @type {?} */
export const unusedValueExportToPlacateAjd = 1;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5qZWN0b3IuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL2ludGVyZmFjZXMvaW5qZWN0b3IudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7O0FBZUEsTUFBTSxPQUFPLEtBQUssR0FBRyxDQUFDOztBQUN0QixNQUFNLE9BQU8sZUFBZSxHQUFHLENBQUM7O0FBQ2hDLE1BQU0sT0FBTywwQkFBMEIsR0FBRyxDQUFDOzs7Ozs7OztBQVEzQyw4Q0FBeUY7OztJQUE3Qyw2Q0FBMkM7Ozs7SUFHckYsd0JBQXFDO0lBQ3JDLG1CQUFvQjtJQUNwQixhQUFjOzs7O0FBR2hCLE1BQU0sT0FBTyxrQkFBa0IsR0FBNkIsbUJBQUEsQ0FBQyxDQUFDLEVBQU87Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBaUdyRSxNQUFNLE9BQU8sbUJBQW1COzs7Ozs7SUFtRjlCLFlBSVcsT0FleUI7SUFDaEM7O09BRUc7SUFDSCxjQUF1QixFQUFFLG9CQUN3QztRQXBCMUQsWUFBTyxHQUFQLE9BQU8sQ0Fla0I7Ozs7O1FBNUZwQyxjQUFTLEdBQUcsS0FBSyxDQUFDO1FBa0doQixJQUFJLENBQUMsbUJBQW1CLEdBQUcsY0FBYyxDQUFDO1FBQzFDLElBQUksQ0FBQyxVQUFVLEdBQUcsb0JBQW9CLENBQUM7SUFDekMsQ0FBQztDQUNGOzs7Ozs7SUEzR0MseUNBQW1GOzs7Ozs7SUFNbkYsd0NBQWtCOzs7OztJQUtsQixrREFBNkI7Ozs7O0lBSzdCLG9DQUF5Qjs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztJQStCekIsaURBQTRCOzs7Ozs7SUFNNUIsb0NBQWU7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0lBdUJmLDhDQUEyQzs7Ozs7SUFPdkMsc0NBZWdDOzs7Ozs7QUFXdEMsTUFBTSxVQUFVLFNBQVMsQ0FBQyxHQUFRO0lBQ2hDLHVEQUF1RDtJQUN2RCxPQUFPLEdBQUcsS0FBSyxJQUFJLElBQUksT0FBTyxHQUFHLElBQUksUUFBUTtRQUN6QyxNQUFNLENBQUMsY0FBYyxDQUFDLEdBQUcsQ0FBQyxJQUFJLG1CQUFtQixDQUFDLFNBQVMsQ0FBQztBQUNsRSxDQUFDOzs7O0FBSUQsTUFBTSxPQUFPLDZCQUE2QixHQUFHLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SW5qZWN0aW9uVG9rZW59IGZyb20gJy4uLy4uL2RpL2luamVjdGlvbl90b2tlbic7XG5pbXBvcnQge0luamVjdEZsYWdzfSBmcm9tICcuLi8uLi9kaS9pbnRlcmZhY2UvaW5qZWN0b3InO1xuaW1wb3J0IHtUeXBlfSBmcm9tICcuLi8uLi9pbnRlcmZhY2UvdHlwZSc7XG5cbmltcG9ydCB7VEVsZW1lbnROb2RlfSBmcm9tICcuL25vZGUnO1xuaW1wb3J0IHtMVmlldywgVERhdGF9IGZyb20gJy4vdmlldyc7XG5cbmV4cG9ydCBjb25zdCBUTk9ERSA9IDg7XG5leHBvcnQgY29uc3QgUEFSRU5UX0lOSkVDVE9SID0gODtcbmV4cG9ydCBjb25zdCBJTkpFQ1RPUl9CTE9PTV9QQVJFTlRfU0laRSA9IDk7XG5cbi8qKlxuICogUmVwcmVzZW50cyBhIHJlbGF0aXZlIGxvY2F0aW9uIG9mIHBhcmVudCBpbmplY3Rvci5cbiAqXG4gKiBUaGUgaW50ZXJmYWNlcyBlbmNvZGVzIG51bWJlciBvZiBwYXJlbnRzIGBMVmlld2BzIHRvIHRyYXZlcnNlIGFuZCBpbmRleCBpbiB0aGUgYExWaWV3YFxuICogcG9pbnRpbmcgdG8gdGhlIHBhcmVudCBpbmplY3Rvci5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBSZWxhdGl2ZUluamVjdG9yTG9jYXRpb24geyBfX2JyYW5kX186ICdSZWxhdGl2ZUluamVjdG9yTG9jYXRpb25GbGFncyc7IH1cblxuZXhwb3J0IGNvbnN0IGVudW0gUmVsYXRpdmVJbmplY3RvckxvY2F0aW9uRmxhZ3Mge1xuICBJbmplY3RvckluZGV4TWFzayA9IDBiMTExMTExMTExMTExMTExLFxuICBWaWV3T2Zmc2V0U2hpZnQgPSAxNixcbiAgTk9fUEFSRU5UID0gLTEsXG59XG5cbmV4cG9ydCBjb25zdCBOT19QQVJFTlRfSU5KRUNUT1I6IFJlbGF0aXZlSW5qZWN0b3JMb2NhdGlvbiA9IC0xIGFzIGFueTtcblxuLyoqXG4gKiBFYWNoIGluamVjdG9yIGlzIHNhdmVkIGluIDkgY29udGlndW91cyBzbG90cyBpbiBgTFZpZXdgIGFuZCA5IGNvbnRpZ3VvdXMgc2xvdHMgaW5cbiAqIGBUVmlldy5kYXRhYC4gVGhpcyBhbGxvd3MgdXMgdG8gc3RvcmUgaW5mb3JtYXRpb24gYWJvdXQgdGhlIGN1cnJlbnQgbm9kZSdzIHRva2VucyAod2hpY2hcbiAqIGNhbiBiZSBzaGFyZWQgaW4gYFRWaWV3YCkgYXMgd2VsbCBhcyB0aGUgdG9rZW5zIG9mIGl0cyBhbmNlc3RvciBub2RlcyAod2hpY2ggY2Fubm90IGJlXG4gKiBzaGFyZWQsIHNvIHRoZXkgbGl2ZSBpbiBgTFZpZXdgKS5cbiAqXG4gKiBFYWNoIG9mIHRoZXNlIHNsb3RzIChhc2lkZSBmcm9tIHRoZSBsYXN0IHNsb3QpIGNvbnRhaW5zIGEgYmxvb20gZmlsdGVyLiBUaGlzIGJsb29tIGZpbHRlclxuICogZGV0ZXJtaW5lcyB3aGV0aGVyIGEgZGlyZWN0aXZlIGlzIGF2YWlsYWJsZSBvbiB0aGUgYXNzb2NpYXRlZCBub2RlIG9yIG5vdC4gVGhpcyBwcmV2ZW50cyB1c1xuICogZnJvbSBzZWFyY2hpbmcgdGhlIGRpcmVjdGl2ZXMgYXJyYXkgYXQgdGhpcyBsZXZlbCB1bmxlc3MgaXQncyBwcm9iYWJsZSB0aGUgZGlyZWN0aXZlIGlzIGluIGl0LlxuICpcbiAqIFNlZTogaHR0cHM6Ly9lbi53aWtpcGVkaWEub3JnL3dpa2kvQmxvb21fZmlsdGVyIGZvciBtb3JlIGFib3V0IGJsb29tIGZpbHRlcnMuXG4gKlxuICogQmVjYXVzZSBhbGwgaW5qZWN0b3JzIGhhdmUgYmVlbiBmbGF0dGVuZWQgaW50byBgTFZpZXdgIGFuZCBgVFZpZXdEYXRhYCwgdGhleSBjYW5ub3QgdHlwZWRcbiAqIHVzaW5nIGludGVyZmFjZXMgYXMgdGhleSB3ZXJlIHByZXZpb3VzbHkuIFRoZSBzdGFydCBpbmRleCBvZiBlYWNoIGBMSW5qZWN0b3JgIGFuZCBgVEluamVjdG9yYFxuICogd2lsbCBkaWZmZXIgYmFzZWQgb24gd2hlcmUgaXQgaXMgZmxhdHRlbmVkIGludG8gdGhlIG1haW4gYXJyYXksIHNvIGl0J3Mgbm90IHBvc3NpYmxlIHRvIGtub3dcbiAqIHRoZSBpbmRpY2VzIGFoZWFkIG9mIHRpbWUgYW5kIHNhdmUgdGhlaXIgdHlwZXMgaGVyZS4gVGhlIGludGVyZmFjZXMgYXJlIHN0aWxsIGluY2x1ZGVkIGhlcmVcbiAqIGZvciBkb2N1bWVudGF0aW9uIHB1cnBvc2VzLlxuICpcbiAqIGV4cG9ydCBpbnRlcmZhY2UgTEluamVjdG9yIGV4dGVuZHMgQXJyYXk8YW55PiB7XG4gKlxuICogICAgLy8gQ3VtdWxhdGl2ZSBibG9vbSBmb3IgZGlyZWN0aXZlIElEcyAwLTMxICAoSURzIGFyZSAlIEJMT09NX1NJWkUpXG4gKiAgICBbMF06IG51bWJlcjtcbiAqXG4gKiAgICAvLyBDdW11bGF0aXZlIGJsb29tIGZvciBkaXJlY3RpdmUgSURzIDMyLTYzXG4gKiAgICBbMV06IG51bWJlcjtcbiAqXG4gKiAgICAvLyBDdW11bGF0aXZlIGJsb29tIGZvciBkaXJlY3RpdmUgSURzIDY0LTk1XG4gKiAgICBbMl06IG51bWJlcjtcbiAqXG4gKiAgICAvLyBDdW11bGF0aXZlIGJsb29tIGZvciBkaXJlY3RpdmUgSURzIDk2LTEyN1xuICogICAgWzNdOiBudW1iZXI7XG4gKlxuICogICAgLy8gQ3VtdWxhdGl2ZSBibG9vbSBmb3IgZGlyZWN0aXZlIElEcyAxMjgtMTU5XG4gKiAgICBbNF06IG51bWJlcjtcbiAqXG4gKiAgICAvLyBDdW11bGF0aXZlIGJsb29tIGZvciBkaXJlY3RpdmUgSURzIDE2MCAtIDE5MVxuICogICAgWzVdOiBudW1iZXI7XG4gKlxuICogICAgLy8gQ3VtdWxhdGl2ZSBibG9vbSBmb3IgZGlyZWN0aXZlIElEcyAxOTIgLSAyMjNcbiAqICAgIFs2XTogbnVtYmVyO1xuICpcbiAqICAgIC8vIEN1bXVsYXRpdmUgYmxvb20gZm9yIGRpcmVjdGl2ZSBJRHMgMjI0IC0gMjU1XG4gKiAgICBbN106IG51bWJlcjtcbiAqXG4gKiAgICAvLyBXZSBuZWVkIHRvIHN0b3JlIGEgcmVmZXJlbmNlIHRvIHRoZSBpbmplY3RvcidzIHBhcmVudCBzbyBESSBjYW4ga2VlcCBsb29raW5nIHVwXG4gKiAgICAvLyB0aGUgaW5qZWN0b3IgdHJlZSB1bnRpbCBpdCBmaW5kcyB0aGUgZGVwZW5kZW5jeSBpdCdzIGxvb2tpbmcgZm9yLlxuICogICAgW1BBUkVOVF9JTkpFQ1RPUl06IG51bWJlcjtcbiAqIH1cbiAqXG4gKiBleHBvcnQgaW50ZXJmYWNlIFRJbmplY3RvciBleHRlbmRzIEFycmF5PGFueT4ge1xuICpcbiAqICAgIC8vIFNoYXJlZCBub2RlIGJsb29tIGZvciBkaXJlY3RpdmUgSURzIDAtMzEgIChJRHMgYXJlICUgQkxPT01fU0laRSlcbiAqICAgIFswXTogbnVtYmVyO1xuICpcbiAqICAgIC8vIFNoYXJlZCBub2RlIGJsb29tIGZvciBkaXJlY3RpdmUgSURzIDMyLTYzXG4gKiAgICBbMV06IG51bWJlcjtcbiAqXG4gKiAgICAvLyBTaGFyZWQgbm9kZSBibG9vbSBmb3IgZGlyZWN0aXZlIElEcyA2NC05NVxuICogICAgWzJdOiBudW1iZXI7XG4gKlxuICogICAgLy8gU2hhcmVkIG5vZGUgYmxvb20gZm9yIGRpcmVjdGl2ZSBJRHMgOTYtMTI3XG4gKiAgICBbM106IG51bWJlcjtcbiAqXG4gKiAgICAvLyBTaGFyZWQgbm9kZSBibG9vbSBmb3IgZGlyZWN0aXZlIElEcyAxMjgtMTU5XG4gKiAgICBbNF06IG51bWJlcjtcbiAqXG4gKiAgICAvLyBTaGFyZWQgbm9kZSBibG9vbSBmb3IgZGlyZWN0aXZlIElEcyAxNjAgLSAxOTFcbiAqICAgIFs1XTogbnVtYmVyO1xuICpcbiAqICAgIC8vIFNoYXJlZCBub2RlIGJsb29tIGZvciBkaXJlY3RpdmUgSURzIDE5MiAtIDIyM1xuICogICAgWzZdOiBudW1iZXI7XG4gKlxuICogICAgLy8gU2hhcmVkIG5vZGUgYmxvb20gZm9yIGRpcmVjdGl2ZSBJRHMgMjI0IC0gMjU1XG4gKiAgICBbN106IG51bWJlcjtcbiAqXG4gKiAgICAvLyBOZWNlc3NhcnkgdG8gZmluZCBkaXJlY3RpdmUgaW5kaWNlcyBmb3IgYSBwYXJ0aWN1bGFyIG5vZGUuXG4gKiAgICBbVE5PREVdOiBURWxlbWVudE5vZGV8VEVsZW1lbnRDb250YWluZXJOb2RlfFRDb250YWluZXJOb2RlO1xuICogIH1cbiAqL1xuXG4vKipcbiogRmFjdG9yeSBmb3IgY3JlYXRpbmcgaW5zdGFuY2VzIG9mIGluamVjdG9ycyBpbiB0aGUgTm9kZUluamVjdG9yLlxuKlxuKiBUaGlzIGZhY3RvcnkgaXMgY29tcGxpY2F0ZWQgYnkgdGhlIGZhY3QgdGhhdCBpdCBjYW4gcmVzb2x2ZSBgbXVsdGlgIGZhY3RvcmllcyBhcyB3ZWxsLlxuKlxuKiBOT1RFOiBTb21lIG9mIHRoZSBmaWVsZHMgYXJlIG9wdGlvbmFsIHdoaWNoIG1lYW5zIHRoYXQgdGhpcyBjbGFzcyBoYXMgdHdvIGhpZGRlbiBjbGFzc2VzLlxuKiAtIE9uZSB3aXRob3V0IGBtdWx0aWAgc3VwcG9ydCAobW9zdCBjb21tb24pXG4qIC0gT25lIHdpdGggYG11bHRpYCB2YWx1ZXMsIChyYXJlKS5cbipcbiogU2luY2UgVk1zIGNhbiBjYWNoZSB1cCB0byA0IGlubGluZSBoaWRkZW4gY2xhc3NlcyB0aGlzIGlzIE9LLlxuKlxuKiAtIFNpbmdsZSBmYWN0b3J5OiBPbmx5IGByZXNvbHZpbmdgIGFuZCBgZmFjdG9yeWAgaXMgZGVmaW5lZC5cbiogLSBgcHJvdmlkZXJzYCBmYWN0b3J5OiBgY29tcG9uZW50UHJvdmlkZXJzYCBpcyBhIG51bWJlciBhbmQgYGluZGV4ID0gLTFgLlxuKiAtIGB2aWV3UHJvdmlkZXJzYCBmYWN0b3J5OiBgY29tcG9uZW50UHJvdmlkZXJzYCBpcyBhIG51bWJlciBhbmQgYGluZGV4YCBwb2ludHMgdG8gYHByb3ZpZGVyc2AuXG4qL1xuZXhwb3J0IGNsYXNzIE5vZGVJbmplY3RvckZhY3Rvcnkge1xuICAvKipcbiAgICogVGhlIGluamVjdCBpbXBsZW1lbnRhdGlvbiB0byBiZSBhY3RpdmF0ZWQgd2hlbiB1c2luZyB0aGUgZmFjdG9yeS5cbiAgICovXG4gIGluamVjdEltcGw6IG51bGx8KDxUPih0b2tlbjogVHlwZTxUPnxJbmplY3Rpb25Ub2tlbjxUPiwgZmxhZ3M/OiBJbmplY3RGbGFncykgPT4gVCk7XG5cbiAgLyoqXG4gICAqIE1hcmtlciBzZXQgdG8gdHJ1ZSBkdXJpbmcgZmFjdG9yeSBpbnZvY2F0aW9uIHRvIHNlZSBpZiB3ZSBnZXQgaW50byByZWN1cnNpdmUgbG9vcC5cbiAgICogUmVjdXJzaXZlIGxvb3AgY2F1c2VzIGFuIGVycm9yIHRvIGJlIGRpc3BsYXllZC5cbiAgICovXG4gIHJlc29sdmluZyA9IGZhbHNlO1xuXG4gIC8qKlxuICAgKiBNYXJrcyB0aGF0IHRoZSB0b2tlbiBjYW4gc2VlIG90aGVyIFRva2VucyBkZWNsYXJlZCBpbiBgdmlld1Byb3ZpZGVyc2Agb24gdGhlIHNhbWUgbm9kZS5cbiAgICovXG4gIGNhblNlZVZpZXdQcm92aWRlcnM6IGJvb2xlYW47XG5cbiAgLyoqXG4gICAqIEFuIGFycmF5IG9mIGZhY3RvcmllcyB0byB1c2UgaW4gY2FzZSBvZiBgbXVsdGlgIHByb3ZpZGVyLlxuICAgKi9cbiAgbXVsdGk/OiBBcnJheTwoKSA9PiBhbnk+O1xuXG4gIC8qKlxuICAgKiBOdW1iZXIgb2YgYG11bHRpYC1wcm92aWRlcnMgd2hpY2ggYmVsb25nIHRvIHRoZSBjb21wb25lbnQuXG4gICAqXG4gICAqIFRoaXMgaXMgbmVlZGVkIGJlY2F1c2Ugd2hlbiBtdWx0aXBsZSBjb21wb25lbnRzIGFuZCBkaXJlY3RpdmVzIGRlY2xhcmUgdGhlIGBtdWx0aWAgcHJvdmlkZXJcbiAgICogdGhleSBoYXZlIHRvIGJlIGNvbmNhdGVuYXRlZCBpbiB0aGUgY29ycmVjdCBvcmRlci5cbiAgICpcbiAgICogRXhhbXBsZTpcbiAgICpcbiAgICogSWYgd2UgaGF2ZSBhIGNvbXBvbmVudCBhbmQgZGlyZWN0aXZlIGFjdGl2ZSBhbiBhIHNpbmdsZSBlbGVtZW50IGFzIGRlY2xhcmVkIGhlcmVcbiAgICogYGBgXG4gICAqIGNvbXBvbmVudDpcbiAgICogICBwcm92aWRlczogWyB7cHJvdmlkZTogU3RyaW5nLCB1c2VWYWx1ZTogJ2NvbXBvbmVudCcsIG11bHRpOiB0cnVlfSBdLFxuICAgKiAgIHZpZXdQcm92aWRlczogWyB7cHJvdmlkZTogU3RyaW5nLCB1c2VWYWx1ZTogJ2NvbXBvbmVudFZpZXcnLCBtdWx0aTogdHJ1ZX0gXSxcbiAgICpcbiAgICogZGlyZWN0aXZlOlxuICAgKiAgIHByb3ZpZGVzOiBbIHtwcm92aWRlOiBTdHJpbmcsIHVzZVZhbHVlOiAnZGlyZWN0aXZlJywgbXVsdGk6IHRydWV9IF0sXG4gICAqIGBgYFxuICAgKlxuICAgKiBUaGVuIHRoZSBleHBlY3RlZCByZXN1bHRzIGFyZTpcbiAgICpcbiAgICogYGBgXG4gICAqIHByb3ZpZGVyczogWydjb21wb25lbnQnLCAnZGlyZWN0aXZlJ11cbiAgICogdmlld1Byb3ZpZGVyczogWydjb21wb25lbnQnLCAnY29tcG9uZW50VmlldycsICdkaXJlY3RpdmUnXVxuICAgKiBgYGBcbiAgICpcbiAgICogVGhlIHdheSB0byB0aGluayBhYm91dCBpdCBpcyB0aGF0IHRoZSBgdmlld1Byb3ZpZGVyc2AgaGF2ZSBiZWVuIGluc2VydGVkIGFmdGVyIHRoZSBjb21wb25lbnRcbiAgICogYnV0IGJlZm9yZSB0aGUgZGlyZWN0aXZlcywgd2hpY2ggaXMgd2h5IHdlIG5lZWQgdG8ga25vdyBob3cgbWFueSBgbXVsdGlgcyBoYXZlIGJlZW4gZGVjbGFyZWQgYnlcbiAgICogdGhlIGNvbXBvbmVudC5cbiAgICovXG4gIGNvbXBvbmVudFByb3ZpZGVycz86IG51bWJlcjtcblxuICAvKipcbiAgICogQ3VycmVudCBpbmRleCBvZiB0aGUgRmFjdG9yeSBpbiB0aGUgYGRhdGFgLiBOZWVkZWQgZm9yIGB2aWV3UHJvdmlkZXJzYCBhbmQgYHByb3ZpZGVyc2AgbWVyZ2luZy5cbiAgICogU2VlIGBwcm92aWRlckZhY3RvcnlgLlxuICAgKi9cbiAgaW5kZXg/OiBudW1iZXI7XG5cbiAgLyoqXG4gICAqIEJlY2F1c2UgdGhlIHNhbWUgYG11bHRpYCBwcm92aWRlciBjYW4gYmUgZGVjbGFyZWQgaW4gYHByb3ZpZGVzYCBhbmQgYHZpZXdQcm92aWRlc2AgaXQgaXNcbiAgICogcG9zc2libGUgZm9yIGB2aWV3UHJvdmlkZXNgIHRvIHNoYWRvdyB0aGUgYHByb3ZpZGVzYC4gRm9yIHRoaXMgcmVhc29uIHdlIHN0b3JlIHRoZVxuICAgKiBgcHJvdmlkZUZhY3RvcnlgIG9mIHRoZSBgcHJvdmlkZXJzYCBzbyB0aGF0IGBwcm92aWRlcnNgIGNhbiBiZSBleHRlbmRlZCB3aXRoIGB2aWV3UHJvdmlkZXJzYC5cbiAgICpcbiAgICogRXhhbXBsZTpcbiAgICpcbiAgICogR2l2ZW46XG4gICAqIGBgYFxuICAgKiBwcm92aWRlczogWyB7cHJvdmlkZTogU3RyaW5nLCB1c2VWYWx1ZTogJ2FsbCcsIG11bHRpOiB0cnVlfSBdLFxuICAgKiB2aWV3UHJvdmlkZXM6IFsge3Byb3ZpZGU6IFN0cmluZywgdXNlVmFsdWU6ICd2aWV3T25seScsIG11bHRpOiB0cnVlfSBdLFxuICAgKiBgYGBcbiAgICpcbiAgICogV2UgaGF2ZSB0byByZXR1cm4gYFsnYWxsJ11gIGluIGNhc2Ugb2YgY29udGVudCBpbmplY3Rpb24sIGJ1dCBgWydhbGwnLCAndmlld09ubHknXWAgaW4gY2FzZVxuICAgKiBvZiB2aWV3IGluamVjdGlvbi4gV2UgZnVydGhlciBoYXZlIHRvIG1ha2Ugc3VyZSB0aGF0IHRoZSBzaGFyZWQgaW5zdGFuY2VzIChpbiBvdXIgY2FzZVxuICAgKiBgYWxsYCkgYXJlIHRoZSBleGFjdCBzYW1lIGluc3RhbmNlIGluIGJvdGggdGhlIGNvbnRlbnQgYXMgd2VsbCBhcyB0aGUgdmlldyBpbmplY3Rpb24uIChXZVxuICAgKiBoYXZlIHRvIG1ha2Ugc3VyZSB0aGF0IHdlIGRvbid0IGRvdWJsZSBpbnN0YW50aWF0ZS4pIEZvciB0aGlzIHJlYXNvbiB0aGUgYHZpZXdQcm92aWRlc2BcbiAgICogYEZhY3RvcnlgIGhhcyBhIHBvaW50ZXIgdG8gdGhlIHNoYWRvd2VkIGBwcm92aWRlc2AgZmFjdG9yeSBzbyB0aGF0IGl0IGNhbiBpbnN0YW50aWF0ZSB0aGVcbiAgICogYHByb3ZpZGVyc2AgKGBbJ2FsbCddYCkgYW5kIHRoZW4gZXh0ZW5kIGl0IHdpdGggYHZpZXdQcm92aWRlcnNgIChgWydhbGwnXSArIFsndmlld09ubHknXSA9XG4gICAqIFsnYWxsJywgJ3ZpZXdPbmx5J11gKS5cbiAgICovXG4gIHByb3ZpZGVyRmFjdG9yeT86IE5vZGVJbmplY3RvckZhY3Rvcnl8bnVsbDtcblxuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgLyoqXG4gICAgICAgKiBGYWN0b3J5IHRvIGludm9rZSBpbiBvcmRlciB0byBjcmVhdGUgYSBuZXcgaW5zdGFuY2UuXG4gICAgICAgKi9cbiAgICAgIHB1YmxpYyBmYWN0b3J5OlxuICAgICAgICAgICh0aGlzOiBOb2RlSW5qZWN0b3JGYWN0b3J5LCBfOiB1bmRlZmluZWQsXG4gICAgICAgICAgIC8qKlxuICAgICAgICAgICAgKiBhcnJheSB3aGVyZSBpbmplY3RhYmxlcyB0b2tlbnMgYXJlIHN0b3JlZC4gVGhpcyBpcyB1c2VkIGluXG4gICAgICAgICAgICAqIGNhc2Ugb2YgYW4gZXJyb3IgcmVwb3J0aW5nIHRvIHByb2R1Y2UgZnJpZW5kbGllciBlcnJvcnMuXG4gICAgICAgICAgICAqL1xuICAgICAgICAgICB0RGF0YTogVERhdGEsXG4gICAgICAgICAgIC8qKlxuICAgICAgICAgICAgKiBhcnJheSB3aGVyZSBleGlzdGluZyBpbnN0YW5jZXMgb2YgaW5qZWN0YWJsZXMgYXJlIHN0b3JlZC4gVGhpcyBpcyB1c2VkIGluIGNhc2VcbiAgICAgICAgICAgICogb2YgbXVsdGkgc2hhZG93IGlzIG5lZWRlZC4gU2VlIGBtdWx0aWAgZmllbGQgZG9jdW1lbnRhdGlvbi5cbiAgICAgICAgICAgICovXG4gICAgICAgICAgIGxWaWV3OiBMVmlldyxcbiAgICAgICAgICAgLyoqXG4gICAgICAgICAgICAqIFRoZSBUTm9kZSBvZiB0aGUgc2FtZSBlbGVtZW50IGluamVjdG9yLlxuICAgICAgICAgICAgKi9cbiAgICAgICAgICAgdE5vZGU6IFRFbGVtZW50Tm9kZSkgPT4gYW55LFxuICAgICAgLyoqXG4gICAgICAgKiBTZXQgdG8gYHRydWVgIGlmIHRoZSB0b2tlbiBpcyBkZWNsYXJlZCBpbiBgdmlld1Byb3ZpZGVyc2AgKG9yIGlmIGl0IGlzIGNvbXBvbmVudCkuXG4gICAgICAgKi9cbiAgICAgIGlzVmlld1Byb3ZpZGVyOiBib29sZWFuLCBpbmplY3RJbXBsZW1lbnRhdGlvbjogbnVsbHxcbiAgICAgICg8VD4odG9rZW46IFR5cGU8VD58SW5qZWN0aW9uVG9rZW48VD4sIGZsYWdzPzogSW5qZWN0RmxhZ3MpID0+IFQpKSB7XG4gICAgdGhpcy5jYW5TZWVWaWV3UHJvdmlkZXJzID0gaXNWaWV3UHJvdmlkZXI7XG4gICAgdGhpcy5pbmplY3RJbXBsID0gaW5qZWN0SW1wbGVtZW50YXRpb247XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzRmFjdG9yeShvYmo6IGFueSk6IG9iaiBpcyBOb2RlSW5qZWN0b3JGYWN0b3J5IHtcbiAgLy8gU2VlOiBodHRwczovL2pzcGVyZi5jb20vaW5zdGFuY2VvZi12cy1nZXRwcm90b3R5cGVvZlxuICByZXR1cm4gb2JqICE9PSBudWxsICYmIHR5cGVvZiBvYmogPT0gJ29iamVjdCcgJiZcbiAgICAgIE9iamVjdC5nZXRQcm90b3R5cGVPZihvYmopID09IE5vZGVJbmplY3RvckZhY3RvcnkucHJvdG90eXBlO1xufVxuXG4vLyBOb3RlOiBUaGlzIGhhY2sgaXMgbmVjZXNzYXJ5IHNvIHdlIGRvbid0IGVycm9uZW91c2x5IGdldCBhIGNpcmN1bGFyIGRlcGVuZGVuY3lcbi8vIGZhaWx1cmUgYmFzZWQgb24gdHlwZXMuXG5leHBvcnQgY29uc3QgdW51c2VkVmFsdWVFeHBvcnRUb1BsYWNhdGVBamQgPSAxO1xuIl19
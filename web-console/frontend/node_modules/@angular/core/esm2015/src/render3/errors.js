/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { stringify } from '../util/stringify';
/**
 * Called when directives inject each other (creating a circular dependency)
 * @param {?} token
 * @return {?}
 */
export function throwCyclicDependencyError(token) {
    throw new Error(`Cannot instantiate cyclic dependency! ${token}`);
}
/**
 * Called when there are multiple component selectors that match a given node
 * @param {?} tNode
 * @return {?}
 */
export function throwMultipleComponentError(tNode) {
    throw new Error(`Multiple components match node with tagname ${tNode.tagName}`);
}
/**
 * Throws an ExpressionChangedAfterChecked error if checkNoChanges mode is on.
 * @param {?} creationMode
 * @param {?} oldValue
 * @param {?} currValue
 * @return {?}
 */
export function throwErrorIfNoChangesMode(creationMode, oldValue, currValue) {
    /** @type {?} */
    let msg = `ExpressionChangedAfterItHasBeenCheckedError: Expression has changed after it was checked. Previous value: '${oldValue}'. Current value: '${currValue}'.`;
    if (creationMode) {
        msg +=
            ` It seems like the view has been created after its parent and its children have been dirty checked.` +
                ` Has it been created in a change detection hook ?`;
    }
    // TODO: include debug context
    throw new Error(msg);
}
/**
 * @return {?}
 */
export function throwMixedMultiProviderError() {
    throw new Error(`Cannot mix multi providers and regular providers`);
}
/**
 * @param {?=} ngModuleType
 * @param {?=} providers
 * @param {?=} provider
 * @return {?}
 */
export function throwInvalidProviderError(ngModuleType, providers, provider) {
    /** @type {?} */
    let ngModuleDetail = '';
    if (ngModuleType && providers) {
        /** @type {?} */
        const providerDetail = providers.map((/**
         * @param {?} v
         * @return {?}
         */
        v => v == provider ? '?' + provider + '?' : '...'));
        ngModuleDetail =
            ` - only instances of Provider and Type are allowed, got: [${providerDetail.join(', ')}]`;
    }
    throw new Error(`Invalid provider for the NgModule '${stringify(ngModuleType)}'` + ngModuleDetail);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXJyb3JzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvcmVuZGVyMy9lcnJvcnMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQVNBLE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQzs7Ozs7O0FBTTVDLE1BQU0sVUFBVSwwQkFBMEIsQ0FBQyxLQUFVO0lBQ25ELE1BQU0sSUFBSSxLQUFLLENBQUMseUNBQXlDLEtBQUssRUFBRSxDQUFDLENBQUM7QUFDcEUsQ0FBQzs7Ozs7O0FBR0QsTUFBTSxVQUFVLDJCQUEyQixDQUFDLEtBQVk7SUFDdEQsTUFBTSxJQUFJLEtBQUssQ0FBQywrQ0FBK0MsS0FBSyxDQUFDLE9BQU8sRUFBRSxDQUFDLENBQUM7QUFDbEYsQ0FBQzs7Ozs7Ozs7QUFHRCxNQUFNLFVBQVUseUJBQXlCLENBQ3JDLFlBQXFCLEVBQUUsUUFBYSxFQUFFLFNBQWM7O1FBQ2xELEdBQUcsR0FDSCw4R0FBOEcsUUFBUSxzQkFBc0IsU0FBUyxJQUFJO0lBQzdKLElBQUksWUFBWSxFQUFFO1FBQ2hCLEdBQUc7WUFDQyxxR0FBcUc7Z0JBQ3JHLG1EQUFtRCxDQUFDO0tBQ3pEO0lBQ0QsOEJBQThCO0lBQzlCLE1BQU0sSUFBSSxLQUFLLENBQUMsR0FBRyxDQUFDLENBQUM7QUFDdkIsQ0FBQzs7OztBQUVELE1BQU0sVUFBVSw0QkFBNEI7SUFDMUMsTUFBTSxJQUFJLEtBQUssQ0FBQyxrREFBa0QsQ0FBQyxDQUFDO0FBQ3RFLENBQUM7Ozs7Ozs7QUFFRCxNQUFNLFVBQVUseUJBQXlCLENBQ3JDLFlBQWdDLEVBQUUsU0FBaUIsRUFBRSxRQUFjOztRQUNqRSxjQUFjLEdBQUcsRUFBRTtJQUN2QixJQUFJLFlBQVksSUFBSSxTQUFTLEVBQUU7O2NBQ3ZCLGNBQWMsR0FBRyxTQUFTLENBQUMsR0FBRzs7OztRQUFDLENBQUMsQ0FBQyxFQUFFLENBQUMsQ0FBQyxJQUFJLFFBQVEsQ0FBQyxDQUFDLENBQUMsR0FBRyxHQUFHLFFBQVEsR0FBRyxHQUFHLENBQUMsQ0FBQyxDQUFDLEtBQUssRUFBQztRQUN2RixjQUFjO1lBQ1YsNkRBQTZELGNBQWMsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQztLQUMvRjtJQUVELE1BQU0sSUFBSSxLQUFLLENBQ1gsc0NBQXNDLFNBQVMsQ0FBQyxZQUFZLENBQUMsR0FBRyxHQUFHLGNBQWMsQ0FBQyxDQUFDO0FBQ3pGLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyJcbi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cbmltcG9ydCB7SW5qZWN0b3JUeXBlfSBmcm9tICcuLi9kaS9pbnRlcmZhY2UvZGVmcyc7XG5pbXBvcnQge3N0cmluZ2lmeX0gZnJvbSAnLi4vdXRpbC9zdHJpbmdpZnknO1xuXG5pbXBvcnQge1ROb2RlfSBmcm9tICcuL2ludGVyZmFjZXMvbm9kZSc7XG5cblxuLyoqIENhbGxlZCB3aGVuIGRpcmVjdGl2ZXMgaW5qZWN0IGVhY2ggb3RoZXIgKGNyZWF0aW5nIGEgY2lyY3VsYXIgZGVwZW5kZW5jeSkgKi9cbmV4cG9ydCBmdW5jdGlvbiB0aHJvd0N5Y2xpY0RlcGVuZGVuY3lFcnJvcih0b2tlbjogYW55KTogbmV2ZXIge1xuICB0aHJvdyBuZXcgRXJyb3IoYENhbm5vdCBpbnN0YW50aWF0ZSBjeWNsaWMgZGVwZW5kZW5jeSEgJHt0b2tlbn1gKTtcbn1cblxuLyoqIENhbGxlZCB3aGVuIHRoZXJlIGFyZSBtdWx0aXBsZSBjb21wb25lbnQgc2VsZWN0b3JzIHRoYXQgbWF0Y2ggYSBnaXZlbiBub2RlICovXG5leHBvcnQgZnVuY3Rpb24gdGhyb3dNdWx0aXBsZUNvbXBvbmVudEVycm9yKHROb2RlOiBUTm9kZSk6IG5ldmVyIHtcbiAgdGhyb3cgbmV3IEVycm9yKGBNdWx0aXBsZSBjb21wb25lbnRzIG1hdGNoIG5vZGUgd2l0aCB0YWduYW1lICR7dE5vZGUudGFnTmFtZX1gKTtcbn1cblxuLyoqIFRocm93cyBhbiBFeHByZXNzaW9uQ2hhbmdlZEFmdGVyQ2hlY2tlZCBlcnJvciBpZiBjaGVja05vQ2hhbmdlcyBtb2RlIGlzIG9uLiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHRocm93RXJyb3JJZk5vQ2hhbmdlc01vZGUoXG4gICAgY3JlYXRpb25Nb2RlOiBib29sZWFuLCBvbGRWYWx1ZTogYW55LCBjdXJyVmFsdWU6IGFueSk6IG5ldmVyfHZvaWQge1xuICBsZXQgbXNnID1cbiAgICAgIGBFeHByZXNzaW9uQ2hhbmdlZEFmdGVySXRIYXNCZWVuQ2hlY2tlZEVycm9yOiBFeHByZXNzaW9uIGhhcyBjaGFuZ2VkIGFmdGVyIGl0IHdhcyBjaGVja2VkLiBQcmV2aW91cyB2YWx1ZTogJyR7b2xkVmFsdWV9Jy4gQ3VycmVudCB2YWx1ZTogJyR7Y3VyclZhbHVlfScuYDtcbiAgaWYgKGNyZWF0aW9uTW9kZSkge1xuICAgIG1zZyArPVxuICAgICAgICBgIEl0IHNlZW1zIGxpa2UgdGhlIHZpZXcgaGFzIGJlZW4gY3JlYXRlZCBhZnRlciBpdHMgcGFyZW50IGFuZCBpdHMgY2hpbGRyZW4gaGF2ZSBiZWVuIGRpcnR5IGNoZWNrZWQuYCArXG4gICAgICAgIGAgSGFzIGl0IGJlZW4gY3JlYXRlZCBpbiBhIGNoYW5nZSBkZXRlY3Rpb24gaG9vayA/YDtcbiAgfVxuICAvLyBUT0RPOiBpbmNsdWRlIGRlYnVnIGNvbnRleHRcbiAgdGhyb3cgbmV3IEVycm9yKG1zZyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB0aHJvd01peGVkTXVsdGlQcm92aWRlckVycm9yKCkge1xuICB0aHJvdyBuZXcgRXJyb3IoYENhbm5vdCBtaXggbXVsdGkgcHJvdmlkZXJzIGFuZCByZWd1bGFyIHByb3ZpZGVyc2ApO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdGhyb3dJbnZhbGlkUHJvdmlkZXJFcnJvcihcbiAgICBuZ01vZHVsZVR5cGU/OiBJbmplY3RvclR5cGU8YW55PiwgcHJvdmlkZXJzPzogYW55W10sIHByb3ZpZGVyPzogYW55KSB7XG4gIGxldCBuZ01vZHVsZURldGFpbCA9ICcnO1xuICBpZiAobmdNb2R1bGVUeXBlICYmIHByb3ZpZGVycykge1xuICAgIGNvbnN0IHByb3ZpZGVyRGV0YWlsID0gcHJvdmlkZXJzLm1hcCh2ID0+IHYgPT0gcHJvdmlkZXIgPyAnPycgKyBwcm92aWRlciArICc/JyA6ICcuLi4nKTtcbiAgICBuZ01vZHVsZURldGFpbCA9XG4gICAgICAgIGAgLSBvbmx5IGluc3RhbmNlcyBvZiBQcm92aWRlciBhbmQgVHlwZSBhcmUgYWxsb3dlZCwgZ290OiBbJHtwcm92aWRlckRldGFpbC5qb2luKCcsICcpfV1gO1xuICB9XG5cbiAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgYEludmFsaWQgcHJvdmlkZXIgZm9yIHRoZSBOZ01vZHVsZSAnJHtzdHJpbmdpZnkobmdNb2R1bGVUeXBlKX0nYCArIG5nTW9kdWxlRGV0YWlsKTtcbn1cbiJdfQ==
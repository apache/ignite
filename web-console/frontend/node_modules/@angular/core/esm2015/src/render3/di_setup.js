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
import { resolveForwardRef } from '../di/forward_ref';
import { isClassProvider, isTypeProvider, providerToFactory } from '../di/r3_injector';
import { diPublicInInjector, getNodeInjectable, getOrCreateNodeInjectorForNode } from './di';
import { ɵɵdirectiveInject } from './instructions/all';
import { NodeInjectorFactory } from './interfaces/injector';
import { isComponentDef } from './interfaces/type_checks';
import { TVIEW } from './interfaces/view';
import { getLView, getPreviousOrParentTNode } from './state';
/**
 * Resolves the providers which are defined in the DirectiveDef.
 *
 * When inserting the tokens and the factories in their respective arrays, we can assume that
 * this method is called first for the component (if any), and then for other directives on the same
 * node.
 * As a consequence,the providers are always processed in that order:
 * 1) The view providers of the component
 * 2) The providers of the component
 * 3) The providers of the other directives
 * This matches the structure of the injectables arrays of a view (for each node).
 * So the tokens and the factories can be pushed at the end of the arrays, except
 * in one case for multi providers.
 *
 * @template T
 * @param {?} def the directive definition
 * @param {?} providers
 * @param {?} viewProviders
 * @return {?}
 */
export function providersResolver(def, providers, viewProviders) {
    /** @type {?} */
    const lView = getLView();
    /** @type {?} */
    const tView = lView[TVIEW];
    if (tView.firstTemplatePass) {
        /** @type {?} */
        const isComponent = isComponentDef(def);
        // The list of view providers is processed first, and the flags are updated
        resolveProvider(viewProviders, tView.data, tView.blueprint, isComponent, true);
        // Then, the list of providers is processed, and the flags are updated
        resolveProvider(providers, tView.data, tView.blueprint, isComponent, false);
    }
}
/**
 * Resolves a provider and publishes it to the DI system.
 * @param {?} provider
 * @param {?} tInjectables
 * @param {?} lInjectablesBlueprint
 * @param {?} isComponent
 * @param {?} isViewProvider
 * @return {?}
 */
function resolveProvider(provider, tInjectables, lInjectablesBlueprint, isComponent, isViewProvider) {
    provider = resolveForwardRef(provider);
    if (Array.isArray(provider)) {
        // Recursively call `resolveProvider`
        // Recursion is OK in this case because this code will not be in hot-path once we implement
        // cloning of the initial state.
        for (let i = 0; i < provider.length; i++) {
            resolveProvider(provider[i], tInjectables, lInjectablesBlueprint, isComponent, isViewProvider);
        }
    }
    else {
        /** @type {?} */
        const lView = getLView();
        /** @type {?} */
        const tView = lView[TVIEW];
        /** @type {?} */
        let token = isTypeProvider(provider) ? provider : resolveForwardRef(provider.provide);
        /** @type {?} */
        let providerFactory = providerToFactory(provider);
        /** @type {?} */
        const tNode = getPreviousOrParentTNode();
        /** @type {?} */
        const beginIndex = tNode.providerIndexes & 65535 /* ProvidersStartIndexMask */;
        /** @type {?} */
        const endIndex = tNode.directiveStart;
        /** @type {?} */
        const cptViewProvidersCount = tNode.providerIndexes >> 16 /* CptViewProvidersCountShift */;
        if (isClassProvider(provider) || isTypeProvider(provider)) {
            /** @type {?} */
            const prototype = (((/** @type {?} */ (provider))).useClass || provider).prototype;
            /** @type {?} */
            const ngOnDestroy = prototype.ngOnDestroy;
            if (ngOnDestroy) {
                (tView.destroyHooks || (tView.destroyHooks = [])).push(tInjectables.length, ngOnDestroy);
            }
        }
        if (isTypeProvider(provider) || !provider.multi) {
            // Single provider case: the factory is created and pushed immediately
            /** @type {?} */
            const factory = new NodeInjectorFactory(providerFactory, isViewProvider, ɵɵdirectiveInject);
            /** @type {?} */
            const existingFactoryIndex = indexOf(token, tInjectables, isViewProvider ? beginIndex : beginIndex + cptViewProvidersCount, endIndex);
            if (existingFactoryIndex == -1) {
                diPublicInInjector(getOrCreateNodeInjectorForNode((/** @type {?} */ (tNode)), lView), tView, token);
                tInjectables.push(token);
                tNode.directiveStart++;
                tNode.directiveEnd++;
                if (isViewProvider) {
                    tNode.providerIndexes += 65536 /* CptViewProvidersCountShifter */;
                }
                lInjectablesBlueprint.push(factory);
                lView.push(factory);
            }
            else {
                lInjectablesBlueprint[existingFactoryIndex] = factory;
                lView[existingFactoryIndex] = factory;
            }
        }
        else {
            // Multi provider case:
            // We create a multi factory which is going to aggregate all the values.
            // Since the output of such a factory depends on content or view injection,
            // we create two of them, which are linked together.
            //
            // The first one (for view providers) is always in the first block of the injectables array,
            // and the second one (for providers) is always in the second block.
            // This is important because view providers have higher priority. When a multi token
            // is being looked up, the view providers should be found first.
            // Note that it is not possible to have a multi factory in the third block (directive block).
            //
            // The algorithm to process multi providers is as follows:
            // 1) If the multi provider comes from the `viewProviders` of the component:
            //   a) If the special view providers factory doesn't exist, it is created and pushed.
            //   b) Else, the multi provider is added to the existing multi factory.
            // 2) If the multi provider comes from the `providers` of the component or of another
            // directive:
            //   a) If the multi factory doesn't exist, it is created and provider pushed into it.
            //      It is also linked to the multi factory for view providers, if it exists.
            //   b) Else, the multi provider is added to the existing multi factory.
            /** @type {?} */
            const existingProvidersFactoryIndex = indexOf(token, tInjectables, beginIndex + cptViewProvidersCount, endIndex);
            /** @type {?} */
            const existingViewProvidersFactoryIndex = indexOf(token, tInjectables, beginIndex, beginIndex + cptViewProvidersCount);
            /** @type {?} */
            const doesProvidersFactoryExist = existingProvidersFactoryIndex >= 0 &&
                lInjectablesBlueprint[existingProvidersFactoryIndex];
            /** @type {?} */
            const doesViewProvidersFactoryExist = existingViewProvidersFactoryIndex >= 0 &&
                lInjectablesBlueprint[existingViewProvidersFactoryIndex];
            if (isViewProvider && !doesViewProvidersFactoryExist ||
                !isViewProvider && !doesProvidersFactoryExist) {
                // Cases 1.a and 2.a
                diPublicInInjector(getOrCreateNodeInjectorForNode((/** @type {?} */ (tNode)), lView), tView, token);
                /** @type {?} */
                const factory = multiFactory(isViewProvider ? multiViewProvidersFactoryResolver : multiProvidersFactoryResolver, lInjectablesBlueprint.length, isViewProvider, isComponent, providerFactory);
                if (!isViewProvider && doesViewProvidersFactoryExist) {
                    lInjectablesBlueprint[existingViewProvidersFactoryIndex].providerFactory = factory;
                }
                tInjectables.push(token);
                tNode.directiveStart++;
                tNode.directiveEnd++;
                if (isViewProvider) {
                    tNode.providerIndexes += 65536 /* CptViewProvidersCountShifter */;
                }
                lInjectablesBlueprint.push(factory);
                lView.push(factory);
            }
            else {
                // Cases 1.b and 2.b
                multiFactoryAdd((/** @type {?} */ (lInjectablesBlueprint))[isViewProvider ? existingViewProvidersFactoryIndex : existingProvidersFactoryIndex], providerFactory, !isViewProvider && isComponent);
            }
            if (!isViewProvider && isComponent && doesViewProvidersFactoryExist) {
                (/** @type {?} */ (lInjectablesBlueprint[existingViewProvidersFactoryIndex].componentProviders))++;
            }
        }
    }
}
/**
 * Add a factory in a multi factory.
 * @param {?} multiFactory
 * @param {?} factory
 * @param {?} isComponentProvider
 * @return {?}
 */
function multiFactoryAdd(multiFactory, factory, isComponentProvider) {
    (/** @type {?} */ (multiFactory.multi)).push(factory);
    if (isComponentProvider) {
        (/** @type {?} */ (multiFactory.componentProviders))++;
    }
}
/**
 * Returns the index of item in the array, but only in the begin to end range.
 * @param {?} item
 * @param {?} arr
 * @param {?} begin
 * @param {?} end
 * @return {?}
 */
function indexOf(item, arr, begin, end) {
    for (let i = begin; i < end; i++) {
        if (arr[i] === item)
            return i;
    }
    return -1;
}
/**
 * Use this with `multi` `providers`.
 * @this {?}
 * @param {?} _
 * @param {?} tData
 * @param {?} lData
 * @param {?} tNode
 * @return {?}
 */
function multiProvidersFactoryResolver(_, tData, lData, tNode) {
    return multiResolve((/** @type {?} */ (this.multi)), []);
}
/**
 * Use this with `multi` `viewProviders`.
 *
 * This factory knows how to concatenate itself with the existing `multi` `providers`.
 * @this {?}
 * @param {?} _
 * @param {?} tData
 * @param {?} lData
 * @param {?} tNode
 * @return {?}
 */
function multiViewProvidersFactoryResolver(_, tData, lData, tNode) {
    /** @type {?} */
    const factories = (/** @type {?} */ (this.multi));
    /** @type {?} */
    let result;
    if (this.providerFactory) {
        /** @type {?} */
        const componentCount = (/** @type {?} */ (this.providerFactory.componentProviders));
        /** @type {?} */
        const multiProviders = getNodeInjectable(tData, lData, (/** @type {?} */ ((/** @type {?} */ (this.providerFactory)).index)), tNode);
        // Copy the section of the array which contains `multi` `providers` from the component
        result = multiProviders.slice(0, componentCount);
        // Insert the `viewProvider` instances.
        multiResolve(factories, result);
        // Copy the section of the array which contains `multi` `providers` from other directives
        for (let i = componentCount; i < multiProviders.length; i++) {
            result.push(multiProviders[i]);
        }
    }
    else {
        result = [];
        // Insert the `viewProvider` instances.
        multiResolve(factories, result);
    }
    return result;
}
/**
 * Maps an array of factories into an array of values.
 * @param {?} factories
 * @param {?} result
 * @return {?}
 */
function multiResolve(factories, result) {
    for (let i = 0; i < factories.length; i++) {
        /** @type {?} */
        const factory = (/** @type {?} */ ((/** @type {?} */ (factories[i]))));
        result.push(factory());
    }
    return result;
}
/**
 * Creates a multi factory.
 * @param {?} factoryFn
 * @param {?} index
 * @param {?} isViewProvider
 * @param {?} isComponent
 * @param {?} f
 * @return {?}
 */
function multiFactory(factoryFn, index, isViewProvider, isComponent, f) {
    /** @type {?} */
    const factory = new NodeInjectorFactory(factoryFn, isViewProvider, ɵɵdirectiveInject);
    factory.multi = [];
    factory.index = index;
    factory.componentProviders = 0;
    multiFactoryAdd(factory, f, isComponent && !isViewProvider);
    return factory;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGlfc2V0dXAuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL2RpX3NldHVwLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBU0EsT0FBTyxFQUFDLGlCQUFpQixFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFFcEQsT0FBTyxFQUFDLGVBQWUsRUFBRSxjQUFjLEVBQUUsaUJBQWlCLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUVyRixPQUFPLEVBQUMsa0JBQWtCLEVBQUUsaUJBQWlCLEVBQUUsOEJBQThCLEVBQUMsTUFBTSxNQUFNLENBQUM7QUFDM0YsT0FBTyxFQUFDLGlCQUFpQixFQUFDLE1BQU0sb0JBQW9CLENBQUM7QUFFckQsT0FBTyxFQUFDLG1CQUFtQixFQUFDLE1BQU0sdUJBQXVCLENBQUM7QUFFMUQsT0FBTyxFQUFDLGNBQWMsRUFBQyxNQUFNLDBCQUEwQixDQUFDO0FBQ3hELE9BQU8sRUFBZSxLQUFLLEVBQVEsTUFBTSxtQkFBbUIsQ0FBQztBQUM3RCxPQUFPLEVBQUMsUUFBUSxFQUFFLHdCQUF3QixFQUFDLE1BQU0sU0FBUyxDQUFDOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFzQjNELE1BQU0sVUFBVSxpQkFBaUIsQ0FDN0IsR0FBb0IsRUFBRSxTQUFxQixFQUFFLGFBQXlCOztVQUNsRSxLQUFLLEdBQUcsUUFBUSxFQUFFOztVQUNsQixLQUFLLEdBQVUsS0FBSyxDQUFDLEtBQUssQ0FBQztJQUNqQyxJQUFJLEtBQUssQ0FBQyxpQkFBaUIsRUFBRTs7Y0FDckIsV0FBVyxHQUFHLGNBQWMsQ0FBQyxHQUFHLENBQUM7UUFFdkMsMkVBQTJFO1FBQzNFLGVBQWUsQ0FBQyxhQUFhLEVBQUUsS0FBSyxDQUFDLElBQUksRUFBRSxLQUFLLENBQUMsU0FBUyxFQUFFLFdBQVcsRUFBRSxJQUFJLENBQUMsQ0FBQztRQUUvRSxzRUFBc0U7UUFDdEUsZUFB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LENBQUM7S0FDakM7SUFDRCxPQUFPLE1BQU0sQ0FBQztBQUNoQixDQUFDOzs7Ozs7O0FBS0QsU0FBUyxZQUFZLENBQUMsU0FBMkIsRUFBRSxNQUFhO0lBQzlELEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxTQUFTLENBQUMsTUFBTSxFQUFFLENBQUMsRUFBRSxFQUFFOztjQUNuQyxPQUFPLEdBQUcsbUJBQUEsbUJBQUEsU0FBUyxDQUFDLENBQUMsQ0FBQyxFQUFFLEVBQVk7UUFDMUMsTUFBTSxDQUFDLElBQUksQ0FBQyxPQUFPLEVBQUUsQ0FBQyxDQUFDO0tBQ3hCO0lBQ0QsT0FBTyxNQUFNLENBQUM7QUFDaEIsQ0FBQzs7Ozs7Ozs7OztBQUtELFNBQVMsWUFBWSxDQUNqQixTQUVPLEVBQ1AsS0FBYSxFQUFFLGNBQXVCLEVBQUUsV0FBb0IsRUFDNUQsQ0FBWTs7VUFDUixPQUFPLEdBQUcsSUFBSSxtQkFBbUIsQ0FBQyxTQUFTLEVBQUUsY0FBYyxFQUFFLGlCQUFpQixDQUFDO0lBQ3JGLE9BQU8sQ0FBQyxLQUFLLEdBQUcsRUFBRSxDQUFDO0lBQ25CLE9BQU8sQ0FBQyxLQUFLLEdBQUcsS0FBSyxDQUFDO0lBQ3RCLE9BQU8sQ0FBQyxrQkFBa0IsR0FBRyxDQUFDLENBQUM7SUFDL0IsZUFBZSxDQUFDLE9BQU8sRUFBRSxDQUFDLEVBQUUsV0FBVyxJQUFJLENBQUMsY0FBYyxDQUFDLENBQUM7SUFDNUQsT0FBTyxPQUFPLENBQUM7QUFDakIsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuXG5pbXBvcnQge3Jlc29sdmVGb3J3YXJkUmVmfSBmcm9tICcuLi9kaS9mb3J3YXJkX3JlZic7XG5pbXBvcnQge0NsYXNzUHJvdmlkZXIsIFByb3ZpZGVyfSBmcm9tICcuLi9kaS9pbnRlcmZhY2UvcHJvdmlkZXInO1xuaW1wb3J0IHtpc0NsYXNzUHJvdmlkZXIsIGlzVHlwZVByb3ZpZGVyLCBwcm92aWRlclRvRmFjdG9yeX0gZnJvbSAnLi4vZGkvcjNfaW5qZWN0b3InO1xuXG5pbXBvcnQge2RpUHVibGljSW5JbmplY3RvciwgZ2V0Tm9kZUluamVjdGFibGUsIGdldE9yQ3JlYXRlTm9kZUluamVjdG9yRm9yTm9kZX0gZnJvbSAnLi9kaSc7XG5pbXBvcnQge8m1ybVkaXJlY3RpdmVJbmplY3R9IGZyb20gJy4vaW5zdHJ1Y3Rpb25zL2FsbCc7XG5pbXBvcnQge0RpcmVjdGl2ZURlZn0gZnJvbSAnLi9pbnRlcmZhY2VzL2RlZmluaXRpb24nO1xuaW1wb3J0IHtOb2RlSW5qZWN0b3JGYWN0b3J5fSBmcm9tICcuL2ludGVyZmFjZXMvaW5qZWN0b3InO1xuaW1wb3J0IHtUQ29udGFpbmVyTm9kZSwgVEVsZW1lbnRDb250YWluZXJOb2RlLCBURWxlbWVudE5vZGUsIFROb2RlUHJvdmlkZXJJbmRleGVzfSBmcm9tICcuL2ludGVyZmFjZXMvbm9kZSc7XG5pbXBvcnQge2lzQ29tcG9uZW50RGVmfSBmcm9tICcuL2ludGVyZmFjZXMvdHlwZV9jaGVja3MnO1xuaW1wb3J0IHtMVmlldywgVERhdGEsIFRWSUVXLCBUVmlld30gZnJvbSAnLi9pbnRlcmZhY2VzL3ZpZXcnO1xuaW1wb3J0IHtnZXRMVmlldywgZ2V0UHJldmlvdXNPclBhcmVudFROb2RlfSBmcm9tICcuL3N0YXRlJztcblxuXG5cbi8qKlxuICogUmVzb2x2ZXMgdGhlIHByb3ZpZGVycyB3aGljaCBhcmUgZGVmaW5lZCBpbiB0aGUgRGlyZWN0aXZlRGVmLlxuICpcbiAqIFdoZW4gaW5zZXJ0aW5nIHRoZSB0b2tlbnMgYW5kIHRoZSBmYWN0b3JpZXMgaW4gdGhlaXIgcmVzcGVjdGl2ZSBhcnJheXMsIHdlIGNhbiBhc3N1bWUgdGhhdFxuICogdGhpcyBtZXRob2QgaXMgY2FsbGVkIGZpcnN0IGZvciB0aGUgY29tcG9uZW50IChpZiBhbnkpLCBhbmQgdGhlbiBmb3Igb3RoZXIgZGlyZWN0aXZlcyBvbiB0aGUgc2FtZVxuICogbm9kZS5cbiAqIEFzIGEgY29uc2VxdWVuY2UsdGhlIHByb3ZpZGVycyBhcmUgYWx3YXlzIHByb2Nlc3NlZCBpbiB0aGF0IG9yZGVyOlxuICogMSkgVGhlIHZpZXcgcHJvdmlkZXJzIG9mIHRoZSBjb21wb25lbnRcbiAqIDIpIFRoZSBwcm92aWRlcnMgb2YgdGhlIGNvbXBvbmVudFxuICogMykgVGhlIHByb3ZpZGVycyBvZiB0aGUgb3RoZXIgZGlyZWN0aXZlc1xuICogVGhpcyBtYXRjaGVzIHRoZSBzdHJ1Y3R1cmUgb2YgdGhlIGluamVjdGFibGVzIGFycmF5cyBvZiBhIHZpZXcgKGZvciBlYWNoIG5vZGUpLlxuICogU28gdGhlIHRva2VucyBhbmQgdGhlIGZhY3RvcmllcyBjYW4gYmUgcHVzaGVkIGF0IHRoZSBlbmQgb2YgdGhlIGFycmF5cywgZXhjZXB0XG4gKiBpbiBvbmUgY2FzZSBmb3IgbXVsdGkgcHJvdmlkZXJzLlxuICpcbiAqIEBwYXJhbSBkZWYgdGhlIGRpcmVjdGl2ZSBkZWZpbml0aW9uXG4gKiBAcGFyYW0gcHJvdmlkZXJzOiBBcnJheSBvZiBgcHJvdmlkZXJzYC5cbiAqIEBwYXJhbSB2aWV3UHJvdmlkZXJzOiBBcnJheSBvZiBgdmlld1Byb3ZpZGVyc2AuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBwcm92aWRlcnNSZXNvbHZlcjxUPihcbiAgICBkZWY6IERpcmVjdGl2ZURlZjxUPiwgcHJvdmlkZXJzOiBQcm92aWRlcltdLCB2aWV3UHJvdmlkZXJzOiBQcm92aWRlcltdKTogdm9pZCB7XG4gIGNvbnN0IGxWaWV3ID0gZ2V0TFZpZXcoKTtcbiAgY29uc3QgdFZpZXc6IFRWaWV3ID0gbFZpZXdbVFZJRVddO1xuICBpZiAodFZpZXcuZmlyc3RUZW1wbGF0ZVBhc3MpIHtcbiAgICBjb25zdCBpc0NvbXBvbmVudCA9IGlzQ29tcG9uZW50RGVmKGRlZik7XG5cbiAgICAvLyBUaGUgbGlzdCBvZiB2aWV3IHByb3ZpZGVycyBpcyBwcm9jZXNzZWQgZmlyc3QsIGFuZCB0aGUgZmxhZ3MgYXJlIHVwZGF0ZWRcbiAgICByZXNvbHZlUHJvdmlkZXIodmlld1Byb3ZpZGVycywgdFZpZXcuZGF0YSwgdFZpZXcuYmx1ZXByaW50LCBpc0NvbXBvbmVudCwgdHJ1ZSk7XG5cbiAgICAvLyBUaGVuLCB0aGUgbGlzdCBvZiBwcm92aWRlcnMgaXMgcHJvY2Vzc2VkLCBhbmQgdGhlIGZsYWdzIGFyZSB1cGRhdGVkXG4gICAgcmVzb2x2ZVByb3ZpZGVyKHByb3ZpZGVycywgdFZpZXcuZGF0YSwgdFZpZXcuYmx1ZXByaW50LCBpc0NvbXBvbmVudCwgZmFsc2UpO1xuICB9XG59XG5cbi8qKlxuICogUmVzb2x2ZXMgYSBwcm92aWRlciBhbmQgcHVibGlzaGVzIGl0IHRvIHRoZSBESSBzeXN0ZW0uXG4gKi9cbmZ1bmN0aW9uIHJlc29sdmVQcm92aWRlcihcbiAgICBwcm92aWRlcjogUHJvdmlkZXIsIHRJbmplY3RhYmxlczogVERhdGEsIGxJbmplY3RhYmxlc0JsdWVwcmludDogTm9kZUluamVjdG9yRmFjdG9yeVtdLFxuICAgIGlzQ29tcG9uZW50OiBib29sZWFuLCBpc1ZpZXdQcm92aWRlcjogYm9vbGVhbik6IHZvaWQge1xuICBwcm92aWRlciA9IHJlc29sdmVGb3J3YXJkUmVmKHByb3ZpZGVyKTtcbiAgaWYgKEFycmF5LmlzQXJyYXkocHJvdmlkZXIpKSB7XG4gICAgLy8gUmVjdXJzaXZlbHkgY2FsbCBgcmVzb2x2ZVByb3ZpZGVyYFxuICAgIC8vIFJlY3Vyc2lvbiBpcyBPSyBpbiB0aGlzIGNhc2UgYmVjYXVzZSB0aGlzIGNvZGUgd2lsbCBub3QgYmUgaW4gaG90LXBhdGggb25jZSB3ZSBpbXBsZW1lbnRcbiAgICAvLyBjbG9uaW5nIG9mIHRoZSBpbml0aWFsIHN0YXRlLlxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgcHJvdmlkZXIubGVuZ3RoOyBpKyspIHtcbiAgICAgIHJlc29sdmVQcm92aWRlcihcbiAgICAgICAgICBwcm92aWRlcltpXSwgdEluamVjdGFibGVzLCBsSW5qZWN0YWJsZXNCbHVlcHJpbnQsIGlzQ29tcG9uZW50LCBpc1ZpZXdQcm92aWRlcik7XG4gICAgfVxuICB9IGVsc2Uge1xuICAgIGNvbnN0IGxWaWV3ID0gZ2V0TFZpZXcoKTtcbiAgICBjb25zdCB0VmlldyA9IGxWaWV3W1RWSUVXXTtcbiAgICBsZXQgdG9rZW46IGFueSA9IGlzVHlwZVByb3ZpZGVyKHByb3ZpZGVyKSA/IHByb3ZpZGVyIDogcmVzb2x2ZUZvcndhcmRSZWYocHJvdmlkZXIucHJvdmlkZSk7XG4gICAgbGV0IHByb3ZpZGVyRmFjdG9yeTogKCkgPT4gYW55ID0gcHJvdmlkZXJUb0ZhY3RvcnkocHJvdmlkZXIpO1xuXG4gICAgY29uc3QgdE5vZGUgPSBnZXRQcmV2aW91c09yUGFyZW50VE5vZGUoKTtcbiAgICBjb25zdCBiZWdpbkluZGV4ID0gdE5vZGUucHJvdmlkZXJJbmRleGVzICYgVE5vZGVQcm92aWRlckluZGV4ZXMuUHJvdmlkZXJzU3RhcnRJbmRleE1hc2s7XG4gICAgY29uc3QgZW5kSW5kZXggPSB0Tm9kZS5kaXJlY3RpdmVTdGFydDtcbiAgICBjb25zdCBjcHRWaWV3UHJvdmlkZXJzQ291bnQgPVxuICAgICAgICB0Tm9kZS5wcm92aWRlckluZGV4ZXMgPj4gVE5vZGVQcm92aWRlckluZGV4ZXMuQ3B0Vmlld1Byb3ZpZGVyc0NvdW50U2hpZnQ7XG5cbiAgICBpZiAoaXNDbGFzc1Byb3ZpZGVyKHByb3ZpZGVyKSB8fCBpc1R5cGVQcm92aWRlcihwcm92aWRlcikpIHtcbiAgICAgIGNvbnN0IHByb3RvdHlwZSA9ICgocHJvdmlkZXIgYXMgQ2xhc3NQcm92aWRlcikudXNlQ2xhc3MgfHwgcHJvdmlkZXIpLnByb3RvdHlwZTtcbiAgICAgIGNvbnN0IG5nT25EZXN0cm95ID0gcHJvdG90eXBlLm5nT25EZXN0cm95O1xuXG4gICAgICBpZiAobmdPbkRlc3Ryb3kpIHtcbiAgICAgICAgKHRWaWV3LmRlc3Ryb3lIb29rcyB8fCAodFZpZXcuZGVzdHJveUhvb2tzID0gW10pKS5wdXNoKHRJbmplY3RhYmxlcy5sZW5ndGgsIG5nT25EZXN0cm95KTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAoaXNUeXBlUHJvdmlkZXIocHJvdmlkZXIpIHx8ICFwcm92aWRlci5tdWx0aSkge1xuICAgICAgLy8gU2luZ2xlIHByb3ZpZGVyIGNhc2U6IHRoZSBmYWN0b3J5IGlzIGNyZWF0ZWQgYW5kIHB1c2hlZCBpbW1lZGlhdGVseVxuICAgICAgY29uc3QgZmFjdG9yeSA9IG5ldyBOb2RlSW5qZWN0b3JGYWN0b3J5KHByb3ZpZGVyRmFjdG9yeSwgaXNWaWV3UHJvdmlkZXIsIMm1ybVkaXJlY3RpdmVJbmplY3QpO1xuICAgICAgY29uc3QgZXhpc3RpbmdGYWN0b3J5SW5kZXggPSBpbmRleE9mKFxuICAgICAgICAgIHRva2VuLCB0SW5qZWN0YWJsZXMsIGlzVmlld1Byb3ZpZGVyID8gYmVnaW5JbmRleCA6IGJlZ2luSW5kZXggKyBjcHRWaWV3UHJvdmlkZXJzQ291bnQsXG4gICAgICAgICAgZW5kSW5kZXgpO1xuICAgICAgaWYgKGV4aXN0aW5nRmFjdG9yeUluZGV4ID09IC0xKSB7XG4gICAgICAgIGRpUHVibGljSW5JbmplY3RvcihcbiAgICAgICAgICAgIGdldE9yQ3JlYXRlTm9kZUluamVjdG9yRm9yTm9kZShcbiAgICAgICAgICAgICAgICB0Tm9kZSBhcyBURWxlbWVudE5vZGUgfCBUQ29udGFpbmVyTm9kZSB8IFRFbGVtZW50Q29udGFpbmVyTm9kZSwgbFZpZXcpLFxuICAgICAgICAgICAgdFZpZXcsIHRva2VuKTtcbiAgICAgICAgdEluamVjdGFibGVzLnB1c2godG9rZW4pO1xuICAgICAgICB0Tm9kZS5kaXJlY3RpdmVTdGFydCsrO1xuICAgICAgICB0Tm9kZS5kaXJlY3RpdmVFbmQrKztcbiAgICAgICAgaWYgKGlzVmlld1Byb3ZpZGVyKSB7XG4gICAgICAgICAgdE5vZGUucHJvdmlkZXJJbmRleGVzICs9IFROb2RlUHJvdmlkZXJJbmRleGVzLkNwdFZpZXdQcm92aWRlcnNDb3VudFNoaWZ0ZXI7XG4gICAgICAgIH1cbiAgICAgICAgbEluamVjdGFibGVzQmx1ZXByaW50LnB1c2goZmFjdG9yeSk7XG4gICAgICAgIGxWaWV3LnB1c2goZmFjdG9yeSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBsSW5qZWN0YWJsZXNCbHVlcHJpbnRbZXhpc3RpbmdGYWN0b3J5SW5kZXhdID0gZmFjdG9yeTtcbiAgICAgICAgbFZpZXdbZXhpc3RpbmdGYWN0b3J5SW5kZXhdID0gZmFjdG9yeTtcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgLy8gTXVsdGkgcHJvdmlkZXIgY2FzZTpcbiAgICAgIC8vIFdlIGNyZWF0ZSBhIG11bHRpIGZhY3Rvcnkgd2hpY2ggaXMgZ29pbmcgdG8gYWdncmVnYXRlIGFsbCB0aGUgdmFsdWVzLlxuICAgICAgLy8gU2luY2UgdGhlIG91dHB1dCBvZiBzdWNoIGEgZmFjdG9yeSBkZXBlbmRzIG9uIGNvbnRlbnQgb3IgdmlldyBpbmplY3Rpb24sXG4gICAgICAvLyB3ZSBjcmVhdGUgdHdvIG9mIHRoZW0sIHdoaWNoIGFyZSBsaW5rZWQgdG9nZXRoZXIuXG4gICAgICAvL1xuICAgICAgLy8gVGhlIGZpcnN0IG9uZSAoZm9yIHZpZXcgcHJvdmlkZXJzKSBpcyBhbHdheXMgaW4gdGhlIGZpcnN0IGJsb2NrIG9mIHRoZSBpbmplY3RhYmxlcyBhcnJheSxcbiAgICAgIC8vIGFuZCB0aGUgc2Vjb25kIG9uZSAoZm9yIHByb3ZpZGVycykgaXMgYWx3YXlzIGluIHRoZSBzZWNvbmQgYmxvY2suXG4gICAgICAvLyBUaGlzIGlzIGltcG9ydGFudCBiZWNhdXNlIHZpZXcgcHJvdmlkZXJzIGhhdmUgaGlnaGVyIHByaW9yaXR5LiBXaGVuIGEgbXVsdGkgdG9rZW5cbiAgICAgIC8vIGlzIGJlaW5nIGxvb2tlZCB1cCwgdGhlIHZpZXcgcHJvdmlkZXJzIHNob3VsZCBiZSBmb3VuZCBmaXJzdC5cbiAgICAgIC8vIE5vdGUgdGhhdCBpdCBpcyBub3QgcG9zc2libGUgdG8gaGF2ZSBhIG11bHRpIGZhY3RvcnkgaW4gdGhlIHRoaXJkIGJsb2NrIChkaXJlY3RpdmUgYmxvY2spLlxuICAgICAgLy9cbiAgICAgIC8vIFRoZSBhbGdvcml0aG0gdG8gcHJvY2VzcyBtdWx0aSBwcm92aWRlcnMgaXMgYXMgZm9sbG93czpcbiAgICAgIC8vIDEpIElmIHRoZSBtdWx0aSBwcm92aWRlciBjb21lcyBmcm9tIHRoZSBgdmlld1Byb3ZpZGVyc2Agb2YgdGhlIGNvbXBvbmVudDpcbiAgICAgIC8vICAgYSkgSWYgdGhlIHNwZWNpYWwgdmlldyBwcm92aWRlcnMgZmFjdG9yeSBkb2Vzbid0IGV4aXN0LCBpdCBpcyBjcmVhdGVkIGFuZCBwdXNoZWQuXG4gICAgICAvLyAgIGIpIEVsc2UsIHRoZSBtdWx0aSBwcm92aWRlciBpcyBhZGRlZCB0byB0aGUgZXhpc3RpbmcgbXVsdGkgZmFjdG9yeS5cbiAgICAgIC8vIDIpIElmIHRoZSBtdWx0aSBwcm92aWRlciBjb21lcyBmcm9tIHRoZSBgcHJvdmlkZXJzYCBvZiB0aGUgY29tcG9uZW50IG9yIG9mIGFub3RoZXJcbiAgICAgIC8vIGRpcmVjdGl2ZTpcbiAgICAgIC8vICAgYSkgSWYgdGhlIG11bHRpIGZhY3RvcnkgZG9lc24ndCBleGlzdCwgaXQgaXMgY3JlYXRlZCBhbmQgcHJvdmlkZXIgcHVzaGVkIGludG8gaXQuXG4gICAgICAvLyAgICAgIEl0IGlzIGFsc28gbGlua2VkIHRvIHRoZSBtdWx0aSBmYWN0b3J5IGZvciB2aWV3IHByb3ZpZGVycywgaWYgaXQgZXhpc3RzLlxuICAgICAgLy8gICBiKSBFbHNlLCB0aGUgbXVsdGkgcHJvdmlkZXIgaXMgYWRkZWQgdG8gdGhlIGV4aXN0aW5nIG11bHRpIGZhY3RvcnkuXG5cbiAgICAgIGNvbnN0IGV4aXN0aW5nUHJvdmlkZXJzRmFjdG9yeUluZGV4ID1cbiAgICAgICAgICBpbmRleE9mKHRva2VuLCB0SW5qZWN0YWJsZXMsIGJlZ2luSW5kZXggKyBjcHRWaWV3UHJvdmlkZXJzQ291bnQsIGVuZEluZGV4KTtcbiAgICAgIGNvbnN0IGV4aXN0aW5nVmlld1Byb3ZpZGVyc0ZhY3RvcnlJbmRleCA9XG4gICAgICAgICAgaW5kZXhPZih0b2tlbiwgdEluamVjdGFibGVzLCBiZWdpbkluZGV4LCBiZWdpbkluZGV4ICsgY3B0Vmlld1Byb3ZpZGVyc0NvdW50KTtcbiAgICAgIGNvbnN0IGRvZXNQcm92aWRlcnNGYWN0b3J5RXhpc3QgPSBleGlzdGluZ1Byb3ZpZGVyc0ZhY3RvcnlJbmRleCA+PSAwICYmXG4gICAgICAgICAgbEluamVjdGFibGVzQmx1ZXByaW50W2V4aXN0aW5nUHJvdmlkZXJzRmFjdG9yeUluZGV4XTtcbiAgICAgIGNvbnN0IGRvZXNWaWV3UHJvdmlkZXJzRmFjdG9yeUV4aXN0ID0gZXhpc3RpbmdWaWV3UHJvdmlkZXJzRmFjdG9yeUluZGV4ID49IDAgJiZcbiAgICAgICAgICBsSW5qZWN0YWJsZXNCbHVlcHJpbnRbZXhpc3RpbmdWaWV3UHJvdmlkZXJzRmFjdG9yeUluZGV4XTtcblxuICAgICAgaWYgKGlzVmlld1Byb3ZpZGVyICYmICFkb2VzVmlld1Byb3ZpZGVyc0ZhY3RvcnlFeGlzdCB8fFxuICAgICAgICAgICFpc1ZpZXdQcm92aWRlciAmJiAhZG9lc1Byb3ZpZGVyc0ZhY3RvcnlFeGlzdCkge1xuICAgICAgICAvLyBDYXNlcyAxLmEgYW5kIDIuYVxuICAgICAgICBkaVB1YmxpY0luSW5qZWN0b3IoXG4gICAgICAgICAgICBnZXRPckNyZWF0ZU5vZGVJbmplY3RvckZvck5vZGUoXG4gICAgICAgICAgICAgICAgdE5vZGUgYXMgVEVsZW1lbnROb2RlIHwgVENvbnRhaW5lck5vZGUgfCBURWxlbWVudENvbnRhaW5lck5vZGUsIGxWaWV3KSxcbiAgICAgICAgICAgIHRWaWV3LCB0b2tlbik7XG4gICAgICAgIGNvbnN0IGZhY3RvcnkgPSBtdWx0aUZhY3RvcnkoXG4gICAgICAgICAgICBpc1ZpZXdQcm92aWRlciA/IG11bHRpVmlld1Byb3ZpZGVyc0ZhY3RvcnlSZXNvbHZlciA6IG11bHRpUHJvdmlkZXJzRmFjdG9yeVJlc29sdmVyLFxuICAgICAgICAgICAgbEluamVjdGFibGVzQmx1ZXByaW50Lmxlbmd0aCwgaXNWaWV3UHJvdmlkZXIsIGlzQ29tcG9uZW50LCBwcm92aWRlckZhY3RvcnkpO1xuICAgICAgICBpZiAoIWlzVmlld1Byb3ZpZGVyICYmIGRvZXNWaWV3UHJvdmlkZXJzRmFjdG9yeUV4aXN0KSB7XG4gICAgICAgICAgbEluamVjdGFibGVzQmx1ZXByaW50W2V4aXN0aW5nVmlld1Byb3ZpZGVyc0ZhY3RvcnlJbmRleF0ucHJvdmlkZXJGYWN0b3J5ID0gZmFjdG9yeTtcbiAgICAgICAgfVxuICAgICAgICB0SW5qZWN0YWJsZXMucHVzaCh0b2tlbik7XG4gICAgICAgIHROb2RlLmRpcmVjdGl2ZVN0YXJ0Kys7XG4gICAgICAgIHROb2RlLmRpcmVjdGl2ZUVuZCsrO1xuICAgICAgICBpZiAoaXNWaWV3UHJvdmlkZXIpIHtcbiAgICAgICAgICB0Tm9kZS5wcm92aWRlckluZGV4ZXMgKz0gVE5vZGVQcm92aWRlckluZGV4ZXMuQ3B0Vmlld1Byb3ZpZGVyc0NvdW50U2hpZnRlcjtcbiAgICAgICAgfVxuICAgICAgICBsSW5qZWN0YWJsZXNCbHVlcHJpbnQucHVzaChmYWN0b3J5KTtcbiAgICAgICAgbFZpZXcucHVzaChmYWN0b3J5KTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIC8vIENhc2VzIDEuYiBhbmQgMi5iXG4gICAgICAgIG11bHRpRmFjdG9yeUFkZChcbiAgICAgICAgICAgIGxJbmplY3RhYmxlc0JsdWVwcmludCAhW2lzVmlld1Byb3ZpZGVyID8gZXhpc3RpbmdWaWV3UHJvdmlkZXJzRmFjdG9yeUluZGV4IDogZXhpc3RpbmdQcm92aWRlcnNGYWN0b3J5SW5kZXhdLFxuICAgICAgICAgICAgcHJvdmlkZXJGYWN0b3J5LCAhaXNWaWV3UHJvdmlkZXIgJiYgaXNDb21wb25lbnQpO1xuICAgICAgfVxuICAgICAgaWYgKCFpc1ZpZXdQcm92aWRlciAmJiBpc0NvbXBvbmVudCAmJiBkb2VzVmlld1Byb3ZpZGVyc0ZhY3RvcnlFeGlzdCkge1xuICAgICAgICBsSW5qZWN0YWJsZXNCbHVlcHJpbnRbZXhpc3RpbmdWaWV3UHJvdmlkZXJzRmFjdG9yeUluZGV4XS5jb21wb25lbnRQcm92aWRlcnMgISsrO1xuICAgICAgfVxuICAgIH1cbiAgfVxufVxuXG4vKipcbiAqIEFkZCBhIGZhY3RvcnkgaW4gYSBtdWx0aSBmYWN0b3J5LlxuICovXG5mdW5jdGlvbiBtdWx0aUZhY3RvcnlBZGQoXG4gICAgbXVsdGlGYWN0b3J5OiBOb2RlSW5qZWN0b3JGYWN0b3J5LCBmYWN0b3J5OiAoKSA9PiBhbnksIGlzQ29tcG9uZW50UHJvdmlkZXI6IGJvb2xlYW4pOiB2b2lkIHtcbiAgbXVsdGlGYWN0b3J5Lm11bHRpICEucHVzaChmYWN0b3J5KTtcbiAgaWYgKGlzQ29tcG9uZW50UHJvdmlkZXIpIHtcbiAgICBtdWx0aUZhY3RvcnkuY29tcG9uZW50UHJvdmlkZXJzICErKztcbiAgfVxufVxuXG4vKipcbiAqIFJldHVybnMgdGhlIGluZGV4IG9mIGl0ZW0gaW4gdGhlIGFycmF5LCBidXQgb25seSBpbiB0aGUgYmVnaW4gdG8gZW5kIHJhbmdlLlxuICovXG5mdW5jdGlvbiBpbmRleE9mKGl0ZW06IGFueSwgYXJyOiBhbnlbXSwgYmVnaW46IG51bWJlciwgZW5kOiBudW1iZXIpIHtcbiAgZm9yIChsZXQgaSA9IGJlZ2luOyBpIDwgZW5kOyBpKyspIHtcbiAgICBpZiAoYXJyW2ldID09PSBpdGVtKSByZXR1cm4gaTtcbiAgfVxuICByZXR1cm4gLTE7XG59XG5cbi8qKlxuICogVXNlIHRoaXMgd2l0aCBgbXVsdGlgIGBwcm92aWRlcnNgLlxuICovXG5mdW5jdGlvbiBtdWx0aVByb3ZpZGVyc0ZhY3RvcnlSZXNvbHZlcihcbiAgICB0aGlzOiBOb2RlSW5qZWN0b3JGYWN0b3J5LCBfOiB1bmRlZmluZWQsIHREYXRhOiBURGF0YSwgbERhdGE6IExWaWV3LFxuICAgIHROb2RlOiBURWxlbWVudE5vZGUpOiBhbnlbXSB7XG4gIHJldHVybiBtdWx0aVJlc29sdmUodGhpcy5tdWx0aSAhLCBbXSk7XG59XG5cbi8qKlxuICogVXNlIHRoaXMgd2l0aCBgbXVsdGlgIGB2aWV3UHJvdmlkZXJzYC5cbiAqXG4gKiBUaGlzIGZhY3Rvcnkga25vd3MgaG93IHRvIGNvbmNhdGVuYXRlIGl0c2VsZiB3aXRoIHRoZSBleGlzdGluZyBgbXVsdGlgIGBwcm92aWRlcnNgLlxuICovXG5mdW5jdGlvbiBtdWx0aVZpZXdQcm92aWRlcnNGYWN0b3J5UmVzb2x2ZXIoXG4gICAgdGhpczogTm9kZUluamVjdG9yRmFjdG9yeSwgXzogdW5kZWZpbmVkLCB0RGF0YTogVERhdGEsIGxEYXRhOiBMVmlldyxcbiAgICB0Tm9kZTogVEVsZW1lbnROb2RlKTogYW55W10ge1xuICBjb25zdCBmYWN0b3JpZXMgPSB0aGlzLm11bHRpICE7XG4gIGxldCByZXN1bHQ6IGFueVtdO1xuICBpZiAodGhpcy5wcm92aWRlckZhY3RvcnkpIHtcbiAgICBjb25zdCBjb21wb25lbnRDb3VudCA9IHRoaXMucHJvdmlkZXJGYWN0b3J5LmNvbXBvbmVudFByb3ZpZGVycyAhO1xuICAgIGNvbnN0IG11bHRpUHJvdmlkZXJzID0gZ2V0Tm9kZUluamVjdGFibGUodERhdGEsIGxEYXRhLCB0aGlzLnByb3ZpZGVyRmFjdG9yeSAhLmluZGV4ICEsIHROb2RlKTtcbiAgICAvLyBDb3B5IHRoZSBzZWN0aW9uIG9mIHRoZSBhcnJheSB3aGljaCBjb250YWlucyBgbXVsdGlgIGBwcm92aWRlcnNgIGZyb20gdGhlIGNvbXBvbmVudFxuICAgIHJlc3VsdCA9IG11bHRpUHJvdmlkZXJzLnNsaWNlKDAsIGNvbXBvbmVudENvdW50KTtcbiAgICAvLyBJbnNlcnQgdGhlIGB2aWV3UHJvdmlkZXJgIGluc3RhbmNlcy5cbiAgICBtdWx0aVJlc29sdmUoZmFjdG9yaWVzLCByZXN1bHQpO1xuICAgIC8vIENvcHkgdGhlIHNlY3Rpb24gb2YgdGhlIGFycmF5IHdoaWNoIGNvbnRhaW5zIGBtdWx0aWAgYHByb3ZpZGVyc2AgZnJvbSBvdGhlciBkaXJlY3RpdmVzXG4gICAgZm9yIChsZXQgaSA9IGNvbXBvbmVudENvdW50OyBpIDwgbXVsdGlQcm92aWRlcnMubGVuZ3RoOyBpKyspIHtcbiAgICAgIHJlc3VsdC5wdXNoKG11bHRpUHJvdmlkZXJzW2ldKTtcbiAgICB9XG4gIH0gZWxzZSB7XG4gICAgcmVzdWx0ID0gW107XG4gICAgLy8gSW5zZXJ0IHRoZSBgdmlld1Byb3ZpZGVyYCBpbnN0YW5jZXMuXG4gICAgbXVsdGlSZXNvbHZlKGZhY3RvcmllcywgcmVzdWx0KTtcbiAgfVxuICByZXR1cm4gcmVzdWx0O1xufVxuXG4vKipcbiAqIE1hcHMgYW4gYXJyYXkgb2YgZmFjdG9yaWVzIGludG8gYW4gYXJyYXkgb2YgdmFsdWVzLlxuICovXG5mdW5jdGlvbiBtdWx0aVJlc29sdmUoZmFjdG9yaWVzOiBBcnJheTwoKSA9PiBhbnk+LCByZXN1bHQ6IGFueVtdKTogYW55W10ge1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGZhY3Rvcmllcy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IGZhY3RvcnkgPSBmYWN0b3JpZXNbaV0gIWFzKCkgPT4gbnVsbDtcbiAgICByZXN1bHQucHVzaChmYWN0b3J5KCkpO1xuICB9XG4gIHJldHVybiByZXN1bHQ7XG59XG5cbi8qKlxuICogQ3JlYXRlcyBhIG11bHRpIGZhY3RvcnkuXG4gKi9cbmZ1bmN0aW9uIG11bHRpRmFjdG9yeShcbiAgICBmYWN0b3J5Rm46IChcbiAgICAgICAgdGhpczogTm9kZUluamVjdG9yRmFjdG9yeSwgXzogdW5kZWZpbmVkLCB0RGF0YTogVERhdGEsIGxEYXRhOiBMVmlldywgdE5vZGU6IFRFbGVtZW50Tm9kZSkgPT5cbiAgICAgICAgYW55LFxuICAgIGluZGV4OiBudW1iZXIsIGlzVmlld1Byb3ZpZGVyOiBib29sZWFuLCBpc0NvbXBvbmVudDogYm9vbGVhbixcbiAgICBmOiAoKSA9PiBhbnkpOiBOb2RlSW5qZWN0b3JGYWN0b3J5IHtcbiAgY29uc3QgZmFjdG9yeSA9IG5ldyBOb2RlSW5qZWN0b3JGYWN0b3J5KGZhY3RvcnlGbiwgaXNWaWV3UHJvdmlkZXIsIMm1ybVkaXJlY3RpdmVJbmplY3QpO1xuICBmYWN0b3J5Lm11bHRpID0gW107XG4gIGZhY3RvcnkuaW5kZXggPSBpbmRleDtcbiAgZmFjdG9yeS5jb21wb25lbnRQcm92aWRlcnMgPSAwO1xuICBtdWx0aUZhY3RvcnlBZGQoZmFjdG9yeSwgZiwgaXNDb21wb25lbnQgJiYgIWlzVmlld1Byb3ZpZGVyKTtcbiAgcmV0dXJuIGZhY3Rvcnk7XG59XG4iXX0=
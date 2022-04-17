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
import { Type } from '../interface/type';
import { reflector } from '../reflection/reflection';
import { resolveForwardRef } from './forward_ref';
import { InjectionToken } from './injection_token';
import { Inject, Optional, Self, SkipSelf } from './metadata';
import { invalidProviderError, mixingMultiProvidersWithRegularProvidersError, noAnnotationError } from './reflective_errors';
import { ReflectiveKey } from './reflective_key';
/**
 * @record
 */
function NormalizedProvider() { }
/**
 * `Dependency` is used by the framework to extend DI.
 * This is internal to Angular and should not be used directly.
 */
export class ReflectiveDependency {
    /**
     * @param {?} key
     * @param {?} optional
     * @param {?} visibility
     */
    constructor(key, optional, visibility) {
        this.key = key;
        this.optional = optional;
        this.visibility = visibility;
    }
    /**
     * @param {?} key
     * @return {?}
     */
    static fromKey(key) {
        return new ReflectiveDependency(key, false, null);
    }
}
if (false) {
    /** @type {?} */
    ReflectiveDependency.prototype.key;
    /** @type {?} */
    ReflectiveDependency.prototype.optional;
    /** @type {?} */
    ReflectiveDependency.prototype.visibility;
}
/** @type {?} */
const _EMPTY_LIST = [];
/**
 * An internal resolved representation of a `Provider` used by the `Injector`.
 *
 * \@usageNotes
 * This is usually created automatically by `Injector.resolveAndCreate`.
 *
 * It can be created manually, as follows:
 *
 * ### Example
 *
 * ```typescript
 * var resolvedProviders = Injector.resolve([{ provide: 'message', useValue: 'Hello' }]);
 * var injector = Injector.fromResolvedProviders(resolvedProviders);
 *
 * expect(injector.get('message')).toEqual('Hello');
 * ```
 *
 * \@publicApi
 * @record
 */
export function ResolvedReflectiveProvider() { }
if (false) {
    /**
     * A key, usually a `Type<any>`.
     * @type {?}
     */
    ResolvedReflectiveProvider.prototype.key;
    /**
     * Factory function which can return an instance of an object represented by a key.
     * @type {?}
     */
    ResolvedReflectiveProvider.prototype.resolvedFactories;
    /**
     * Indicates if the provider is a multi-provider or a regular provider.
     * @type {?}
     */
    ResolvedReflectiveProvider.prototype.multiProvider;
}
export class ResolvedReflectiveProvider_ {
    /**
     * @param {?} key
     * @param {?} resolvedFactories
     * @param {?} multiProvider
     */
    constructor(key, resolvedFactories, multiProvider) {
        this.key = key;
        this.resolvedFactories = resolvedFactories;
        this.multiProvider = multiProvider;
        this.resolvedFactory = this.resolvedFactories[0];
    }
}
if (false) {
    /** @type {?} */
    ResolvedReflectiveProvider_.prototype.resolvedFactory;
    /** @type {?} */
    ResolvedReflectiveProvider_.prototype.key;
    /** @type {?} */
    ResolvedReflectiveProvider_.prototype.resolvedFactories;
    /** @type {?} */
    ResolvedReflectiveProvider_.prototype.multiProvider;
}
/**
 * An internal resolved representation of a factory function created by resolving `Provider`.
 * \@publicApi
 */
export class ResolvedReflectiveFactory {
    /**
     * @param {?} factory
     * @param {?} dependencies
     */
    constructor(factory, dependencies) {
        this.factory = factory;
        this.dependencies = dependencies;
    }
}
if (false) {
    /**
     * Factory function which can return an instance of an object represented by a key.
     * @type {?}
     */
    ResolvedReflectiveFactory.prototype.factory;
    /**
     * Arguments (dependencies) to the `factory` function.
     * @type {?}
     */
    ResolvedReflectiveFactory.prototype.dependencies;
}
/**
 * Resolve a single provider.
 * @param {?} provider
 * @return {?}
 */
function resolveReflectiveFactory(provider) {
    /** @type {?} */
    let factoryFn;
    /** @type {?} */
    let resolvedDeps;
    if (provider.useClass) {
        /** @type {?} */
        const useClass = resolveForwardRef(provider.useClass);
        factoryFn = reflector.factory(useClass);
        resolvedDeps = _dependenciesFor(useClass);
    }
    else if (provider.useExisting) {
        factoryFn = (/**
         * @param {?} aliasInstance
         * @return {?}
         */
        (aliasInstance) => aliasInstance);
        resolvedDeps = [ReflectiveDependency.fromKey(ReflectiveKey.get(provider.useExisting))];
    }
    else if (provider.useFactory) {
        factoryFn = provider.useFactory;
        resolvedDeps = constructDependencies(provider.useFactory, provider.deps);
    }
    else {
        factoryFn = (/**
         * @return {?}
         */
        () => provider.useValue);
        resolvedDeps = _EMPTY_LIST;
    }
    return new ResolvedReflectiveFactory(factoryFn, resolvedDeps);
}
/**
 * Converts the `Provider` into `ResolvedProvider`.
 *
 * `Injector` internally only uses `ResolvedProvider`, `Provider` contains convenience provider
 * syntax.
 * @param {?} provider
 * @return {?}
 */
function resolveReflectiveProvider(provider) {
    return new ResolvedReflectiveProvider_(ReflectiveKey.get(provider.provide), [resolveReflectiveFactory(provider)], provider.multi || false);
}
/**
 * Resolve a list of Providers.
 * @param {?} providers
 * @return {?}
 */
export function resolveReflectiveProviders(providers) {
    /** @type {?} */
    const normalized = _normalizeProviders(providers, []);
    /** @type {?} */
    const resolved = normalized.map(resolveReflectiveProvider);
    /** @type {?} */
    const resolvedProviderMap = mergeResolvedReflectiveProviders(resolved, new Map());
    return Array.from(resolvedProviderMap.values());
}
/**
 * Merges a list of ResolvedProviders into a list where each key is contained exactly once and
 * multi providers have been merged.
 * @param {?} providers
 * @param {?} normalizedProvidersMap
 * @return {?}
 */
export function mergeResolvedReflectiveProviders(providers, normalizedProvidersMap) {
    for (let i = 0; i < providers.length; i++) {
        /** @type {?} */
        const provider = providers[i];
        /** @type {?} */
        const existing = normalizedProvidersMap.get(provider.key.id);
        if (existing) {
            if (provider.multiProvider !== existing.multiProvider) {
                throw mixingMultiProvidersWithRegularProvidersError(existing, provider);
            }
            if (provider.multiProvider) {
                for (let j = 0; j < provider.resolvedFactories.length; j++) {
                    existing.resolvedFactories.push(provider.resolvedFactories[j]);
                }
            }
            else {
                normalizedProvidersMap.set(provider.key.id, provider);
            }
        }
        else {
            /** @type {?} */
            let resolvedProvider;
            if (provider.multiProvider) {
                resolvedProvider = new ResolvedReflectiveProvider_(provider.key, provider.resolvedFactories.slice(), provider.multiProvider);
            }
            else {
                resolvedProvider = provider;
            }
            normalizedProvidersMap.set(provider.key.id, resolvedProvider);
        }
    }
    return normalizedProvidersMap;
}
/**
 * @param {?} providers
 * @param {?} res
 * @return {?}
 */
function _normalizeProviders(providers, res) {
    providers.forEach((/**
     * @param {?} b
     * @return {?}
     */
    b => {
        if (b instanceof Type) {
            res.push((/** @type {?} */ ({ provide: b, useClass: b })));
        }
        else if (b && typeof b == 'object' && ((/** @type {?} */ (b))).provide !== undefined) {
            res.push((/** @type {?} */ (b)));
        }
        else if (b instanceof Array) {
            _normalizeProviders(b, res);
        }
        else {
            throw invalidProviderError(b);
        }
    }));
    return res;
}
/**
 * @param {?} typeOrFunc
 * @param {?=} dependencies
 * @return {?}
 */
export function constructDependencies(typeOrFunc, dependencies) {
    if (!dependencies) {
        return _dependenciesFor(typeOrFunc);
    }
    else {
        /** @type {?} */
        const params = dependencies.map((/**
         * @param {?} t
         * @return {?}
         */
        t => [t]));
        return dependencies.map((/**
         * @param {?} t
         * @return {?}
         */
        t => _extractToken(typeOrFunc, t, params)));
    }
}
/**
 * @param {?} typeOrFunc
 * @return {?}
 */
function _dependenciesFor(typeOrFunc) {
    /** @type {?} */
    const params = reflector.parameters(typeOrFunc);
    if (!params)
        return [];
    if (params.some((/**
     * @param {?} p
     * @return {?}
     */
    p => p == null))) {
        throw noAnnotationError(typeOrFunc, params);
    }
    return params.map((/**
     * @param {?} p
     * @return {?}
     */
    p => _extractToken(typeOrFunc, p, params)));
}
/**
 * @param {?} typeOrFunc
 * @param {?} metadata
 * @param {?} params
 * @return {?}
 */
function _extractToken(typeOrFunc, metadata, params) {
    /** @type {?} */
    let token = null;
    /** @type {?} */
    let optional = false;
    if (!Array.isArray(metadata)) {
        if (metadata instanceof Inject) {
            return _createDependency(metadata.token, optional, null);
        }
        else {
            return _createDependency(metadata, optional, null);
        }
    }
    /** @type {?} */
    let visibility = null;
    for (let i = 0; i < metadata.length; ++i) {
        /** @type {?} */
        const paramMetadata = metadata[i];
        if (paramMetadata instanceof Type) {
            token = paramMetadata;
        }
        else if (paramMetadata instanceof Inject) {
            token = paramMetadata.token;
        }
        else if (paramMetadata instanceof Optional) {
            optional = true;
        }
        else if (paramMetadata instanceof Self || paramMetadata instanceof SkipSelf) {
            visibility = paramMetadata;
        }
        else if (paramMetadata instanceof InjectionToken) {
            token = paramMetadata;
        }
    }
    token = resolveForwardRef(token);
    if (token != null) {
        return _createDependency(token, optional, visibility);
    }
    else {
        throw noAnnotationError(typeOrFunc, params);
    }
}
/**
 * @param {?} token
 * @param {?} optional
 * @param {?} visibility
 * @return {?}
 */
function _createDependency(token, optional, visibility) {
    return new ReflectiveDependency(ReflectiveKey.get(token), optional, visibility);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVmbGVjdGl2ZV9wcm92aWRlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL2RpL3JlZmxlY3RpdmVfcHJvdmlkZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsSUFBSSxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFDdkMsT0FBTyxFQUFDLFNBQVMsRUFBQyxNQUFNLDBCQUEwQixDQUFDO0FBRW5ELE9BQU8sRUFBQyxpQkFBaUIsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUNoRCxPQUFPLEVBQUMsY0FBYyxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFFakQsT0FBTyxFQUFDLE1BQU0sRUFBRSxRQUFRLEVBQUUsSUFBSSxFQUFFLFFBQVEsRUFBQyxNQUFNLFlBQVksQ0FBQztBQUM1RCxPQUFPLEVBQUMsb0JBQW9CLEVBQUUsNkNBQTZDLEVBQUUsaUJBQWlCLEVBQUMsTUFBTSxxQkFBcUIsQ0FBQztBQUMzSCxPQUFPLEVBQUMsYUFBYSxFQUFDLE1BQU0sa0JBQWtCLENBQUM7Ozs7QUFHL0MsaUNBQ3NCOzs7OztBQU10QixNQUFNLE9BQU8sb0JBQW9COzs7Ozs7SUFDL0IsWUFDVyxHQUFrQixFQUFTLFFBQWlCLEVBQVMsVUFBOEI7UUFBbkYsUUFBRyxHQUFILEdBQUcsQ0FBZTtRQUFTLGFBQVEsR0FBUixRQUFRLENBQVM7UUFBUyxlQUFVLEdBQVYsVUFBVSxDQUFvQjtJQUFHLENBQUM7Ozs7O0lBRWxHLE1BQU0sQ0FBQyxPQUFPLENBQUMsR0FBa0I7UUFDL0IsT0FBTyxJQUFJLG9CQUFvQixDQUFDLEdBQUcsRUFBRSxLQUFLLEVBQUUsSUFBSSxDQUFDLENBQUM7SUFDcEQsQ0FBQztDQUNGOzs7SUFMSyxtQ0FBeUI7O0lBQUUsd0NBQXdCOztJQUFFLDBDQUFxQzs7O01BTzFGLFdBQVcsR0FBVSxFQUFFOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7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QUFJLGFBQWEsWUFBWSxNQUFNLEVBQUU7WUFDMUMsS0FBSyxHQUFHLGFBQWEsQ0FBQyxLQUFLLENBQUM7U0FFN0I7YUFBTSxJQUFJLGFBQWEsWUFBWSxRQUFRLEVBQUU7WUFDNUMsUUFBUSxHQUFHLElBQUksQ0FBQztTQUVqQjthQUFNLElBQUksYUFBYSxZQUFZLElBQUksSUFBSSxhQUFhLFlBQVksUUFBUSxFQUFFO1lBQzdFLFVBQVUsR0FBRyxhQUFhLENBQUM7U0FDNUI7YUFBTSxJQUFJLGFBQWEsWUFBWSxjQUFjLEVBQUU7WUFDbEQsS0FBSyxHQUFHLGFBQWEsQ0FBQztTQUN2QjtLQUNGO0lBRUQsS0FBSyxHQUFHLGlCQUFpQixDQUFDLEtBQUssQ0FBQyxDQUFDO0lBRWpDLElBQUksS0FBSyxJQUFJLElBQUksRUFBRTtRQUNqQixPQUFPLGlCQUFpQixDQUFDLEtBQUssRUFBRSxRQUFRLEVBQUUsVUFBVSxDQUFDLENBQUM7S0FDdkQ7U0FBTTtRQUNMLE1BQU0saUJBQWlCLENBQUMsVUFBVSxFQUFFLE1BQU0sQ0FBQyxDQUFDO0tBQzdDO0FBQ0gsQ0FBQzs7Ozs7OztBQUVELFNBQVMsaUJBQWlCLENBQ3RCLEtBQVUsRUFBRSxRQUFpQixFQUFFLFVBQWtDO0lBQ25FLE9BQU8sSUFBSSxvQkFBb0IsQ0FBQyxhQUFhLENBQUMsR0FBRyxDQUFDLEtBQUssQ0FBQyxFQUFFLFFBQVEsRUFBRSxVQUFVLENBQUMsQ0FBQztBQUNsRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1R5cGV9IGZyb20gJy4uL2ludGVyZmFjZS90eXBlJztcbmltcG9ydCB7cmVmbGVjdG9yfSBmcm9tICcuLi9yZWZsZWN0aW9uL3JlZmxlY3Rpb24nO1xuXG5pbXBvcnQge3Jlc29sdmVGb3J3YXJkUmVmfSBmcm9tICcuL2ZvcndhcmRfcmVmJztcbmltcG9ydCB7SW5qZWN0aW9uVG9rZW59IGZyb20gJy4vaW5qZWN0aW9uX3Rva2VuJztcbmltcG9ydCB7Q2xhc3NQcm92aWRlciwgRXhpc3RpbmdQcm92aWRlciwgRmFjdG9yeVByb3ZpZGVyLCBQcm92aWRlciwgVHlwZVByb3ZpZGVyLCBWYWx1ZVByb3ZpZGVyfSBmcm9tICcuL2ludGVyZmFjZS9wcm92aWRlcic7XG5pbXBvcnQge0luamVjdCwgT3B0aW9uYWwsIFNlbGYsIFNraXBTZWxmfSBmcm9tICcuL21ldGFkYXRhJztcbmltcG9ydCB7aW52YWxpZFByb3ZpZGVyRXJyb3IsIG1peGluZ011bHRpUHJvdmlkZXJzV2l0aFJlZ3VsYXJQcm92aWRlcnNFcnJvciwgbm9Bbm5vdGF0aW9uRXJyb3J9IGZyb20gJy4vcmVmbGVjdGl2ZV9lcnJvcnMnO1xuaW1wb3J0IHtSZWZsZWN0aXZlS2V5fSBmcm9tICcuL3JlZmxlY3RpdmVfa2V5JztcblxuXG5pbnRlcmZhY2UgTm9ybWFsaXplZFByb3ZpZGVyIGV4dGVuZHMgVHlwZVByb3ZpZGVyLCBWYWx1ZVByb3ZpZGVyLCBDbGFzc1Byb3ZpZGVyLCBFeGlzdGluZ1Byb3ZpZGVyLFxuICAgIEZhY3RvcnlQcm92aWRlciB7fVxuXG4vKipcbiAqIGBEZXBlbmRlbmN5YCBpcyB1c2VkIGJ5IHRoZSBmcmFtZXdvcmsgdG8gZXh0ZW5kIERJLlxuICogVGhpcyBpcyBpbnRlcm5hbCB0byBBbmd1bGFyIGFuZCBzaG91bGQgbm90IGJlIHVzZWQgZGlyZWN0bHkuXG4gKi9cbmV4cG9ydCBjbGFzcyBSZWZsZWN0aXZlRGVwZW5kZW5jeSB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIGtleTogUmVmbGVjdGl2ZUtleSwgcHVibGljIG9wdGlvbmFsOiBib29sZWFuLCBwdWJsaWMgdmlzaWJpbGl0eTogU2VsZnxTa2lwU2VsZnxudWxsKSB7fVxuXG4gIHN0YXRpYyBmcm9tS2V5KGtleTogUmVmbGVjdGl2ZUtleSk6IFJlZmxlY3RpdmVEZXBlbmRlbmN5IHtcbiAgICByZXR1cm4gbmV3IFJlZmxlY3RpdmVEZXBlbmRlbmN5KGtleSwgZmFsc2UsIG51bGwpO1xuICB9XG59XG5cbmNvbnN0IF9FTVBUWV9MSVNUOiBhbnlbXSA9IFtdO1xuXG4vKipcbiAqIEFuIGludGVybmFsIHJlc29sdmVkIHJlcHJlc2VudGF0aW9uIG9mIGEgYFByb3ZpZGVyYCB1c2VkIGJ5IHRoZSBgSW5qZWN0b3JgLlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKiBUaGlzIGlzIHVzdWFsbHkgY3JlYXRlZCBhdXRvbWF0aWNhbGx5IGJ5IGBJbmplY3Rvci5yZXNvbHZlQW5kQ3JlYXRlYC5cbiAqXG4gKiBJdCBjYW4gYmUgY3JlYXRlZCBtYW51YWxseSwgYXMgZm9sbG93czpcbiAqXG4gKiAjIyMgRXhhbXBsZVxuICpcbiAqIGBgYHR5cGVzY3JpcHRcbiAqIHZhciByZXNvbHZlZFByb3ZpZGVycyA9IEluamVjdG9yLnJlc29sdmUoW3sgcHJvdmlkZTogJ21lc3NhZ2UnLCB1c2VWYWx1ZTogJ0hlbGxvJyB9XSk7XG4gKiB2YXIgaW5qZWN0b3IgPSBJbmplY3Rvci5mcm9tUmVzb2x2ZWRQcm92aWRlcnMocmVzb2x2ZWRQcm92aWRlcnMpO1xuICpcbiAqIGV4cGVjdChpbmplY3Rvci5nZXQoJ21lc3NhZ2UnKSkudG9FcXVhbCgnSGVsbG8nKTtcbiAqIGBgYFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBSZXNvbHZlZFJlZmxlY3RpdmVQcm92aWRlciB7XG4gIC8qKlxuICAgKiBBIGtleSwgdXN1YWxseSBhIGBUeXBlPGFueT5gLlxuICAgKi9cbiAga2V5OiBSZWZsZWN0aXZlS2V5O1xuXG4gIC8qKlxuICAgKiBGYWN0b3J5IGZ1bmN0aW9uIHdoaWNoIGNhbiByZXR1cm4gYW4gaW5zdGFuY2Ugb2YgYW4gb2JqZWN0IHJlcHJlc2VudGVkIGJ5IGEga2V5LlxuICAgKi9cbiAgcmVzb2x2ZWRGYWN0b3JpZXM6IFJlc29sdmVkUmVmbGVjdGl2ZUZhY3RvcnlbXTtcblxuICAvKipcbiAgICogSW5kaWNhdGVzIGlmIHRoZSBwcm92aWRlciBpcyBhIG11bHRpLXByb3ZpZGVyIG9yIGEgcmVndWxhciBwcm92aWRlci5cbiAgICovXG4gIG11bHRpUHJvdmlkZXI6IGJvb2xlYW47XG59XG5cbmV4cG9ydCBjbGFzcyBSZXNvbHZlZFJlZmxlY3RpdmVQcm92aWRlcl8gaW1wbGVtZW50cyBSZXNvbHZlZFJlZmxlY3RpdmVQcm92aWRlciB7XG4gIHJlYWRvbmx5IHJlc29sdmVkRmFjdG9yeTogUmVzb2x2ZWRSZWZsZWN0aXZlRmFjdG9yeTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyBrZXk6IFJlZmxlY3RpdmVLZXksIHB1YmxpYyByZXNvbHZlZEZhY3RvcmllczogUmVzb2x2ZWRSZWZsZWN0aXZlRmFjdG9yeVtdLFxuICAgICAgcHVibGljIG11bHRpUHJvdmlkZXI6IGJvb2xlYW4pIHtcbiAgICB0aGlzLnJlc29sdmVkRmFjdG9yeSA9IHRoaXMucmVzb2x2ZWRGYWN0b3JpZXNbMF07XG4gIH1cbn1cblxuLyoqXG4gKiBBbiBpbnRlcm5hbCByZXNvbHZlZCByZXByZXNlbnRhdGlvbiBvZiBhIGZhY3RvcnkgZnVuY3Rpb24gY3JlYXRlZCBieSByZXNvbHZpbmcgYFByb3ZpZGVyYC5cbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIFJlc29sdmVkUmVmbGVjdGl2ZUZhY3Rvcnkge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIC8qKlxuICAgICAgICogRmFjdG9yeSBmdW5jdGlvbiB3aGljaCBjYW4gcmV0dXJuIGFuIGluc3RhbmNlIG9mIGFuIG9iamVjdCByZXByZXNlbnRlZCBieSBhIGtleS5cbiAgICAgICAqL1xuICAgICAgcHVibGljIGZhY3Rvcnk6IEZ1bmN0aW9uLFxuXG4gICAgICAvKipcbiAgICAgICAqIEFyZ3VtZW50cyAoZGVwZW5kZW5jaWVzKSB0byB0aGUgYGZhY3RvcnlgIGZ1bmN0aW9uLlxuICAgICAgICovXG4gICAgICBwdWJsaWMgZGVwZW5kZW5jaWVzOiBSZWZsZWN0aXZlRGVwZW5kZW5jeVtdKSB7fVxufVxuXG5cbi8qKlxuICogUmVzb2x2ZSBhIHNpbmdsZSBwcm92aWRlci5cbiAqL1xuZnVuY3Rpb24gcmVzb2x2ZVJlZmxlY3RpdmVGYWN0b3J5KHByb3ZpZGVyOiBOb3JtYWxpemVkUHJvdmlkZXIpOiBSZXNvbHZlZFJlZmxlY3RpdmVGYWN0b3J5IHtcbiAgbGV0IGZhY3RvcnlGbjogRnVuY3Rpb247XG4gIGxldCByZXNvbHZlZERlcHM6IFJlZmxlY3RpdmVEZXBlbmRlbmN5W107XG4gIGlmIChwcm92aWRlci51c2VDbGFzcykge1xuICAgIGNvbnN0IHVzZUNsYXNzID0gcmVzb2x2ZUZvcndhcmRSZWYocHJvdmlkZXIudXNlQ2xhc3MpO1xuICAgIGZhY3RvcnlGbiA9IHJlZmxlY3Rvci5mYWN0b3J5KHVzZUNsYXNzKTtcbiAgICByZXNvbHZlZERlcHMgPSBfZGVwZW5kZW5jaWVzRm9yKHVzZUNsYXNzKTtcbiAgfSBlbHNlIGlmIChwcm92aWRlci51c2VFeGlzdGluZykge1xuICAgIGZhY3RvcnlGbiA9IChhbGlhc0luc3RhbmNlOiBhbnkpID0+IGFsaWFzSW5zdGFuY2U7XG4gICAgcmVzb2x2ZWREZXBzID0gW1JlZmxlY3RpdmVEZXBlbmRlbmN5LmZyb21LZXkoUmVmbGVjdGl2ZUtleS5nZXQocHJvdmlkZXIudXNlRXhpc3RpbmcpKV07XG4gIH0gZWxzZSBpZiAocHJvdmlkZXIudXNlRmFjdG9yeSkge1xuICAgIGZhY3RvcnlGbiA9IHByb3ZpZGVyLnVzZUZhY3Rvcnk7XG4gICAgcmVzb2x2ZWREZXBzID0gY29uc3RydWN0RGVwZW5kZW5jaWVzKHByb3ZpZGVyLnVzZUZhY3RvcnksIHByb3ZpZGVyLmRlcHMpO1xuICB9IGVsc2Uge1xuICAgIGZhY3RvcnlGbiA9ICgpID0+IHByb3ZpZGVyLnVzZVZhbHVlO1xuICAgIHJlc29sdmVkRGVwcyA9IF9FTVBUWV9MSVNUO1xuICB9XG4gIHJldHVybiBuZXcgUmVzb2x2ZWRSZWZsZWN0aXZlRmFjdG9yeShmYWN0b3J5Rm4sIHJlc29sdmVkRGVwcyk7XG59XG5cbi8qKlxuICogQ29udmVydHMgdGhlIGBQcm92aWRlcmAgaW50byBgUmVzb2x2ZWRQcm92aWRlcmAuXG4gKlxuICogYEluamVjdG9yYCBpbnRlcm5hbGx5IG9ubHkgdXNlcyBgUmVzb2x2ZWRQcm92aWRlcmAsIGBQcm92aWRlcmAgY29udGFpbnMgY29udmVuaWVuY2UgcHJvdmlkZXJcbiAqIHN5bnRheC5cbiAqL1xuZnVuY3Rpb24gcmVzb2x2ZVJlZmxlY3RpdmVQcm92aWRlcihwcm92aWRlcjogTm9ybWFsaXplZFByb3ZpZGVyKTogUmVzb2x2ZWRSZWZsZWN0aXZlUHJvdmlkZXIge1xuICByZXR1cm4gbmV3IFJlc29sdmVkUmVmbGVjdGl2ZVByb3ZpZGVyXyhcbiAgICAgIFJlZmxlY3RpdmVLZXkuZ2V0KHByb3ZpZGVyLnByb3ZpZGUpLCBbcmVzb2x2ZVJlZmxlY3RpdmVGYWN0b3J5KHByb3ZpZGVyKV0sXG4gICAgICBwcm92aWRlci5tdWx0aSB8fCBmYWxzZSk7XG59XG5cbi8qKlxuICogUmVzb2x2ZSBhIGxpc3Qgb2YgUHJvdmlkZXJzLlxuICovXG5leHBvcnQgZnVuY3Rpb24gcmVzb2x2ZVJlZmxlY3RpdmVQcm92aWRlcnMocHJvdmlkZXJzOiBQcm92aWRlcltdKTogUmVzb2x2ZWRSZWZsZWN0aXZlUHJvdmlkZXJbXSB7XG4gIGNvbnN0IG5vcm1hbGl6ZWQgPSBfbm9ybWFsaXplUHJvdmlkZXJzKHByb3ZpZGVycywgW10pO1xuICBjb25zdCByZXNvbHZlZCA9IG5vcm1hbGl6ZWQubWFwKHJlc29sdmVSZWZsZWN0aXZlUHJvdmlkZXIpO1xuICBjb25zdCByZXNvbHZlZFByb3ZpZGVyTWFwID0gbWVyZ2VSZXNvbHZlZFJlZmxlY3RpdmVQcm92aWRlcnMocmVzb2x2ZWQsIG5ldyBNYXAoKSk7XG4gIHJldHVybiBBcnJheS5mcm9tKHJlc29sdmVkUHJvdmlkZXJNYXAudmFsdWVzKCkpO1xufVxuXG4vKipcbiAqIE1lcmdlcyBhIGxpc3Qgb2YgUmVzb2x2ZWRQcm92aWRlcnMgaW50byBhIGxpc3Qgd2hlcmUgZWFjaCBrZXkgaXMgY29udGFpbmVkIGV4YWN0bHkgb25jZSBhbmRcbiAqIG11bHRpIHByb3ZpZGVycyBoYXZlIGJlZW4gbWVyZ2VkLlxuICovXG5leHBvcnQgZnVuY3Rpb24gbWVyZ2VSZXNvbHZlZFJlZmxlY3RpdmVQcm92aWRlcnMoXG4gICAgcHJvdmlkZXJzOiBSZXNvbHZlZFJlZmxlY3RpdmVQcm92aWRlcltdLFxuICAgIG5vcm1hbGl6ZWRQcm92aWRlcnNNYXA6IE1hcDxudW1iZXIsIFJlc29sdmVkUmVmbGVjdGl2ZVByb3ZpZGVyPik6XG4gICAgTWFwPG51bWJlciwgUmVzb2x2ZWRSZWZsZWN0aXZlUHJvdmlkZXI+IHtcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBwcm92aWRlcnMubGVuZ3RoOyBpKyspIHtcbiAgICBjb25zdCBwcm92aWRlciA9IHByb3ZpZGVyc1tpXTtcbiAgICBjb25zdCBleGlzdGluZyA9IG5vcm1hbGl6ZWRQcm92aWRlcnNNYXAuZ2V0KHByb3ZpZGVyLmtleS5pZCk7XG4gICAgaWYgKGV4aXN0aW5nKSB7XG4gICAgICBpZiAocHJvdmlkZXIubXVsdGlQcm92aWRlciAhPT0gZXhpc3RpbmcubXVsdGlQcm92aWRlcikge1xuICAgICAgICB0aHJvdyBtaXhpbmdNdWx0aVByb3ZpZGVyc1dpdGhSZWd1bGFyUHJvdmlkZXJzRXJyb3IoZXhpc3RpbmcsIHByb3ZpZGVyKTtcbiAgICAgIH1cbiAgICAgIGlmIChwcm92aWRlci5tdWx0aVByb3ZpZGVyKSB7XG4gICAgICAgIGZvciAobGV0IGogPSAwOyBqIDwgcHJvdmlkZXIucmVzb2x2ZWRGYWN0b3JpZXMubGVuZ3RoOyBqKyspIHtcbiAgICAgICAgICBleGlzdGluZy5yZXNvbHZlZEZhY3Rvcmllcy5wdXNoKHByb3ZpZGVyLnJlc29sdmVkRmFjdG9yaWVzW2pdKTtcbiAgICAgICAgfVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgbm9ybWFsaXplZFByb3ZpZGVyc01hcC5zZXQocHJvdmlkZXIua2V5LmlkLCBwcm92aWRlcik7XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIGxldCByZXNvbHZlZFByb3ZpZGVyOiBSZXNvbHZlZFJlZmxlY3RpdmVQcm92aWRlcjtcbiAgICAgIGlmIChwcm92aWRlci5tdWx0aVByb3ZpZGVyKSB7XG4gICAgICAgIHJlc29sdmVkUHJvdmlkZXIgPSBuZXcgUmVzb2x2ZWRSZWZsZWN0aXZlUHJvdmlkZXJfKFxuICAgICAgICAgICAgcHJvdmlkZXIua2V5LCBwcm92aWRlci5yZXNvbHZlZEZhY3Rvcmllcy5zbGljZSgpLCBwcm92aWRlci5tdWx0aVByb3ZpZGVyKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHJlc29sdmVkUHJvdmlkZXIgPSBwcm92aWRlcjtcbiAgICAgIH1cbiAgICAgIG5vcm1hbGl6ZWRQcm92aWRlcnNNYXAuc2V0KHByb3ZpZGVyLmtleS5pZCwgcmVzb2x2ZWRQcm92aWRlcik7XG4gICAgfVxuICB9XG4gIHJldHVybiBub3JtYWxpemVkUHJvdmlkZXJzTWFwO1xufVxuXG5mdW5jdGlvbiBfbm9ybWFsaXplUHJvdmlkZXJzKFxuICAgIHByb3ZpZGVyczogUHJvdmlkZXJbXSwgcmVzOiBOb3JtYWxpemVkUHJvdmlkZXJbXSk6IE5vcm1hbGl6ZWRQcm92aWRlcltdIHtcbiAgcHJvdmlkZXJzLmZvckVhY2goYiA9PiB7XG4gICAgaWYgKGIgaW5zdGFuY2VvZiBUeXBlKSB7XG4gICAgICByZXMucHVzaCh7IHByb3ZpZGU6IGIsIHVzZUNsYXNzOiBiIH0gYXMgTm9ybWFsaXplZFByb3ZpZGVyKTtcblxuICAgIH0gZWxzZSBpZiAoYiAmJiB0eXBlb2YgYiA9PSAnb2JqZWN0JyAmJiAoYiBhcyBhbnkpLnByb3ZpZGUgIT09IHVuZGVmaW5lZCkge1xuICAgICAgcmVzLnB1c2goYiBhcyBOb3JtYWxpemVkUHJvdmlkZXIpO1xuXG4gICAgfSBlbHNlIGlmIChiIGluc3RhbmNlb2YgQXJyYXkpIHtcbiAgICAgIF9ub3JtYWxpemVQcm92aWRlcnMoYiwgcmVzKTtcblxuICAgIH0gZWxzZSB7XG4gICAgICB0aHJvdyBpbnZhbGlkUHJvdmlkZXJFcnJvcihiKTtcbiAgICB9XG4gIH0pO1xuXG4gIHJldHVybiByZXM7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjb25zdHJ1Y3REZXBlbmRlbmNpZXMoXG4gICAgdHlwZU9yRnVuYzogYW55LCBkZXBlbmRlbmNpZXM/OiBhbnlbXSk6IFJlZmxlY3RpdmVEZXBlbmRlbmN5W10ge1xuICBpZiAoIWRlcGVuZGVuY2llcykge1xuICAgIHJldHVybiBfZGVwZW5kZW5jaWVzRm9yKHR5cGVPckZ1bmMpO1xuICB9IGVsc2Uge1xuICAgIGNvbnN0IHBhcmFtczogYW55W11bXSA9IGRlcGVuZGVuY2llcy5tYXAodCA9PiBbdF0pO1xuICAgIHJldHVybiBkZXBlbmRlbmNpZXMubWFwKHQgPT4gX2V4dHJhY3RUb2tlbih0eXBlT3JGdW5jLCB0LCBwYXJhbXMpKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBfZGVwZW5kZW5jaWVzRm9yKHR5cGVPckZ1bmM6IGFueSk6IFJlZmxlY3RpdmVEZXBlbmRlbmN5W10ge1xuICBjb25zdCBwYXJhbXMgPSByZWZsZWN0b3IucGFyYW1ldGVycyh0eXBlT3JGdW5jKTtcblxuICBpZiAoIXBhcmFtcykgcmV0dXJuIFtdO1xuICBpZiAocGFyYW1zLnNvbWUocCA9PiBwID09IG51bGwpKSB7XG4gICAgdGhyb3cgbm9Bbm5vdGF0aW9uRXJyb3IodHlwZU9yRnVuYywgcGFyYW1zKTtcbiAgfVxuICByZXR1cm4gcGFyYW1zLm1hcChwID0+IF9leHRyYWN0VG9rZW4odHlwZU9yRnVuYywgcCwgcGFyYW1zKSk7XG59XG5cbmZ1bmN0aW9uIF9leHRyYWN0VG9rZW4oXG4gICAgdHlwZU9yRnVuYzogYW55LCBtZXRhZGF0YTogYW55W10gfCBhbnksIHBhcmFtczogYW55W11bXSk6IFJlZmxlY3RpdmVEZXBlbmRlbmN5IHtcbiAgbGV0IHRva2VuOiBhbnkgPSBudWxsO1xuICBsZXQgb3B0aW9uYWwgPSBmYWxzZTtcblxuICBpZiAoIUFycmF5LmlzQXJyYXkobWV0YWRhdGEpKSB7XG4gICAgaWYgKG1ldGFkYXRhIGluc3RhbmNlb2YgSW5qZWN0KSB7XG4gICAgICByZXR1cm4gX2NyZWF0ZURlcGVuZGVuY3kobWV0YWRhdGEudG9rZW4sIG9wdGlvbmFsLCBudWxsKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIF9jcmVhdGVEZXBlbmRlbmN5KG1ldGFkYXRhLCBvcHRpb25hbCwgbnVsbCk7XG4gICAgfVxuICB9XG5cbiAgbGV0IHZpc2liaWxpdHk6IFNlbGZ8U2tpcFNlbGZ8bnVsbCA9IG51bGw7XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBtZXRhZGF0YS5sZW5ndGg7ICsraSkge1xuICAgIGNvbnN0IHBhcmFtTWV0YWRhdGEgPSBtZXRhZGF0YVtpXTtcblxuICAgIGlmIChwYXJhbU1ldGFkYXRhIGluc3RhbmNlb2YgVHlwZSkge1xuICAgICAgdG9rZW4gPSBwYXJhbU1ldGFkYXRhO1xuXG4gICAgfSBlbHNlIGlmIChwYXJhbU1ldGFkYXRhIGluc3RhbmNlb2YgSW5qZWN0KSB7XG4gICAgICB0b2tlbiA9IHBhcmFtTWV0YWRhdGEudG9rZW47XG5cbiAgICB9IGVsc2UgaWYgKHBhcmFtTWV0YWRhdGEgaW5zdGFuY2VvZiBPcHRpb25hbCkge1xuICAgICAgb3B0aW9uYWwgPSB0cnVlO1xuXG4gICAgfSBlbHNlIGlmIChwYXJhbU1ldGFkYXRhIGluc3RhbmNlb2YgU2VsZiB8fCBwYXJhbU1ldGFkYXRhIGluc3RhbmNlb2YgU2tpcFNlbGYpIHtcbiAgICAgIHZpc2liaWxpdHkgPSBwYXJhbU1ldGFkYXRhO1xuICAgIH0gZWxzZSBpZiAocGFyYW1NZXRhZGF0YSBpbnN0YW5jZW9mIEluamVjdGlvblRva2VuKSB7XG4gICAgICB0b2tlbiA9IHBhcmFtTWV0YWRhdGE7XG4gICAgfVxuICB9XG5cbiAgdG9rZW4gPSByZXNvbHZlRm9yd2FyZFJlZih0b2tlbik7XG5cbiAgaWYgKHRva2VuICE9IG51bGwpIHtcbiAgICByZXR1cm4gX2NyZWF0ZURlcGVuZGVuY3kodG9rZW4sIG9wdGlvbmFsLCB2aXNpYmlsaXR5KTtcbiAgfSBlbHNlIHtcbiAgICB0aHJvdyBub0Fubm90YXRpb25FcnJvcih0eXBlT3JGdW5jLCBwYXJhbXMpO1xuICB9XG59XG5cbmZ1bmN0aW9uIF9jcmVhdGVEZXBlbmRlbmN5KFxuICAgIHRva2VuOiBhbnksIG9wdGlvbmFsOiBib29sZWFuLCB2aXNpYmlsaXR5OiBTZWxmIHwgU2tpcFNlbGYgfCBudWxsKTogUmVmbGVjdGl2ZURlcGVuZGVuY3kge1xuICByZXR1cm4gbmV3IFJlZmxlY3RpdmVEZXBlbmRlbmN5KFJlZmxlY3RpdmVLZXkuZ2V0KHRva2VuKSwgb3B0aW9uYWwsIHZpc2liaWxpdHkpO1xufVxuIl19
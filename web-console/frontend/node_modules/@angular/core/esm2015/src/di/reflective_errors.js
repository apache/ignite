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
import { ERROR_ORIGINAL_ERROR, wrappedError } from '../util/errors';
import { stringify } from '../util/stringify';
/**
 * @param {?} keys
 * @return {?}
 */
function findFirstClosedCycle(keys) {
    /** @type {?} */
    const res = [];
    for (let i = 0; i < keys.length; ++i) {
        if (res.indexOf(keys[i]) > -1) {
            res.push(keys[i]);
            return res;
        }
        res.push(keys[i]);
    }
    return res;
}
/**
 * @param {?} keys
 * @return {?}
 */
function constructResolvingPath(keys) {
    if (keys.length > 1) {
        /** @type {?} */
        const reversed = findFirstClosedCycle(keys.slice().reverse());
        /** @type {?} */
        const tokenStrs = reversed.map((/**
         * @param {?} k
         * @return {?}
         */
        k => stringify(k.token)));
        return ' (' + tokenStrs.join(' -> ') + ')';
    }
    return '';
}
/**
 * @record
 */
export function InjectionError() { }
if (false) {
    /** @type {?} */
    InjectionError.prototype.keys;
    /** @type {?} */
    InjectionError.prototype.injectors;
    /** @type {?} */
    InjectionError.prototype.constructResolvingMessage;
    /**
     * @param {?} injector
     * @param {?} key
     * @return {?}
     */
    InjectionError.prototype.addKey = function (injector, key) { };
}
/**
 * @param {?} injector
 * @param {?} key
 * @param {?} constructResolvingMessage
 * @param {?=} originalError
 * @return {?}
 */
function injectionError(injector, key, constructResolvingMessage, originalError) {
    /** @type {?} */
    const keys = [key];
    /** @type {?} */
    const errMsg = constructResolvingMessage(keys);
    /** @type {?} */
    const error = (/** @type {?} */ ((originalError ? wrappedError(errMsg, originalError) : Error(errMsg))));
    error.addKey = addKey;
    error.keys = keys;
    error.injectors = [injector];
    error.constructResolvingMessage = constructResolvingMessage;
    ((/** @type {?} */ (error)))[ERROR_ORIGINAL_ERROR] = originalError;
    return error;
}
/**
 * @this {?}
 * @param {?} injector
 * @param {?} key
 * @return {?}
 */
function addKey(injector, key) {
    this.injectors.push(injector);
    this.keys.push(key);
    // Note: This updated message won't be reflected in the `.stack` property
    this.message = this.constructResolvingMessage(this.keys);
}
/**
 * Thrown when trying to retrieve a dependency by key from {\@link Injector}, but the
 * {\@link Injector} does not have a {\@link Provider} for the given key.
 *
 * \@usageNotes
 * ### Example
 *
 * ```typescript
 * class A {
 *   constructor(b:B) {}
 * }
 *
 * expect(() => Injector.resolveAndCreate([A])).toThrowError();
 * ```
 * @param {?} injector
 * @param {?} key
 * @return {?}
 */
export function noProviderError(injector, key) {
    return injectionError(injector, key, (/**
     * @param {?} keys
     * @return {?}
     */
    function (keys) {
        /** @type {?} */
        const first = stringify(keys[0].token);
        return `No provider for ${first}!${constructResolvingPath(keys)}`;
    }));
}
/**
 * Thrown when dependencies form a cycle.
 *
 * \@usageNotes
 * ### Example
 *
 * ```typescript
 * var injector = Injector.resolveAndCreate([
 *   {provide: "one", useFactory: (two) => "two", deps: [[new Inject("two")]]},
 *   {provide: "two", useFactory: (one) => "one", deps: [[new Inject("one")]]}
 * ]);
 *
 * expect(() => injector.get("one")).toThrowError();
 * ```
 *
 * Retrieving `A` or `B` throws a `CyclicDependencyError` as the graph above cannot be constructed.
 * @param {?} injector
 * @param {?} key
 * @return {?}
 */
export function cyclicDependencyError(injector, key) {
    return injectionError(injector, key, (/**
     * @param {?} keys
     * @return {?}
     */
    function (keys) {
        return `Cannot instantiate cyclic dependency!${constructResolvingPath(keys)}`;
    }));
}
/**
 * Thrown when a constructing type returns with an Error.
 *
 * The `InstantiationError` class contains the original error plus the dependency graph which caused
 * this object to be instantiated.
 *
 * \@usageNotes
 * ### Example
 *
 * ```typescript
 * class A {
 *   constructor() {
 *     throw new Error('message');
 *   }
 * }
 *
 * var injector = Injector.resolveAndCreate([A]);
 * try {
 *   injector.get(A);
 * } catch (e) {
 *   expect(e instanceof InstantiationError).toBe(true);
 *   expect(e.originalException.message).toEqual("message");
 *   expect(e.originalStack).toBeDefined();
 * }
 * ```
 * @param {?} injector
 * @param {?} originalException
 * @param {?} originalStack
 * @param {?} key
 * @return {?}
 */
export function instantiationError(injector, originalException, originalStack, key) {
    return injectionError(injector, key, (/**
     * @param {?} keys
     * @return {?}
     */
    function (keys) {
        /** @type {?} */
        const first = stringify(keys[0].token);
        return `${originalException.message}: Error during instantiation of ${first}!${constructResolvingPath(keys)}.`;
    }), originalException);
}
/**
 * Thrown when an object other then {\@link Provider} (or `Type`) is passed to {\@link Injector}
 * creation.
 *
 * \@usageNotes
 * ### Example
 *
 * ```typescript
 * expect(() => Injector.resolveAndCreate(["not a type"])).toThrowError();
 * ```
 * @param {?} provider
 * @return {?}
 */
export function invalidProviderError(provider) {
    return Error(`Invalid provider - only instances of Provider and Type are allowed, got: ${provider}`);
}
/**
 * Thrown when the class has no annotation information.
 *
 * Lack of annotation information prevents the {\@link Injector} from determining which dependencies
 * need to be injected into the constructor.
 *
 * \@usageNotes
 * ### Example
 *
 * ```typescript
 * class A {
 *   constructor(b) {}
 * }
 *
 * expect(() => Injector.resolveAndCreate([A])).toThrowError();
 * ```
 *
 * This error is also thrown when the class not marked with {\@link Injectable} has parameter types.
 *
 * ```typescript
 * class B {}
 *
 * class A {
 *   constructor(b:B) {} // no information about the parameter types of A is available at runtime.
 * }
 *
 * expect(() => Injector.resolveAndCreate([A,B])).toThrowError();
 * ```
 *
 * @param {?} typeOrFunc
 * @param {?} params
 * @return {?}
 */
export function noAnnotationError(typeOrFunc, params) {
    /** @type {?} */
    const signature = [];
    for (let i = 0, ii = params.length; i < ii; i++) {
        /** @type {?} */
        const parameter = params[i];
        if (!parameter || parameter.length == 0) {
            signature.push('?');
        }
        else {
            signature.push(parameter.map(stringify).join(' '));
        }
    }
    return Error('Cannot resolve all parameters for \'' + stringify(typeOrFunc) + '\'(' +
        signature.join(', ') + '). ' +
        'Make sure that all the parameters are decorated with Inject or have valid type annotations and that \'' +
        stringify(typeOrFunc) + '\' is decorated with Injectable.');
}
/**
 * Thrown when getting an object by index.
 *
 * \@usageNotes
 * ### Example
 *
 * ```typescript
 * class A {}
 *
 * var injector = Injector.resolveAndCreate([A]);
 *
 * expect(() => injector.getAt(100)).toThrowError();
 * ```
 *
 * @param {?} index
 * @return {?}
 */
export function outOfBoundsError(index) {
    return Error(`Index ${index} is out-of-bounds.`);
}
// TODO: add a working example after alpha38 is released
/**
 * Thrown when a multi provider and a regular provider are bound to the same token.
 *
 * \@usageNotes
 * ### Example
 *
 * ```typescript
 * expect(() => Injector.resolveAndCreate([
 *   { provide: "Strings", useValue: "string1", multi: true},
 *   { provide: "Strings", useValue: "string2", multi: false}
 * ])).toThrowError();
 * ```
 * @param {?} provider1
 * @param {?} provider2
 * @return {?}
 */
export function mixingMultiProvidersWithRegularProvidersError(provider1, provider2) {
    return Error(`Cannot mix multi providers and regular providers, got: ${provider1} ${provider2}`);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVmbGVjdGl2ZV9lcnJvcnMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9kaS9yZWZsZWN0aXZlX2Vycm9ycy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVNBLE9BQU8sRUFBQyxvQkFBb0IsRUFBRSxZQUFZLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUNsRSxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sbUJBQW1CLENBQUM7Ozs7O0FBSzVDLFNBQVMsb0JBQW9CLENBQUMsSUFBVzs7VUFDakMsR0FBRyxHQUFVLEVBQUU7SUFDckIsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLElBQUksQ0FBQyxNQUFNLEVBQUUsRUFBRSxDQUFDLEVBQUU7UUFDcEMsSUFBSSxHQUFHLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxHQUFHLENBQUMsQ0FBQyxFQUFFO1lBQzdCLEdBQUcsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7WUFDbEIsT0FBTyxHQUFHLENBQUM7U0FDWjtRQUNELEdBQUcsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7S0FDbkI7SUFDRCxPQUFPLEdBQUcsQ0FBQztBQUNiLENBQUM7Ozs7O0FBRUQsU0FBUyxzQkFBc0IsQ0FBQyxJQUFXO0lBQ3pDLElBQUksSUFBSSxDQUFDLE1BQU0sR0FBRyxDQUFDLEVBQUU7O2NBQ2IsUUFBUSxHQUFHLG9CQUFvQixDQUFDLElBQUksQ0FBQyxLQUFLLEVBQUUsQ0FBQyxPQUFPLEVBQUUsQ0FBQzs7Y0FDdkQsU0FBUyxHQUFHLFFBQVEsQ0FBQyxHQUFHOzs7O1FBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxTQUFTLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQyxFQUFDO1FBQ3ZELE9BQU8sSUFB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bkcsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtUeXBlfSBmcm9tICcuLi9pbnRlcmZhY2UvdHlwZSc7XG5pbXBvcnQge0VSUk9SX09SSUdJTkFMX0VSUk9SLCB3cmFwcGVkRXJyb3J9IGZyb20gJy4uL3V0aWwvZXJyb3JzJztcbmltcG9ydCB7c3RyaW5naWZ5fSBmcm9tICcuLi91dGlsL3N0cmluZ2lmeSc7XG5cbmltcG9ydCB7UmVmbGVjdGl2ZUluamVjdG9yfSBmcm9tICcuL3JlZmxlY3RpdmVfaW5qZWN0b3InO1xuaW1wb3J0IHtSZWZsZWN0aXZlS2V5fSBmcm9tICcuL3JlZmxlY3RpdmVfa2V5JztcblxuZnVuY3Rpb24gZmluZEZpcnN0Q2xvc2VkQ3ljbGUoa2V5czogYW55W10pOiBhbnlbXSB7XG4gIGNvbnN0IHJlczogYW55W10gPSBbXTtcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBrZXlzLmxlbmd0aDsgKytpKSB7XG4gICAgaWYgKHJlcy5pbmRleE9mKGtleXNbaV0pID4gLTEpIHtcbiAgICAgIHJlcy5wdXNoKGtleXNbaV0pO1xuICAgICAgcmV0dXJuIHJlcztcbiAgICB9XG4gICAgcmVzLnB1c2goa2V5c1tpXSk7XG4gIH1cbiAgcmV0dXJuIHJlcztcbn1cblxuZnVuY3Rpb24gY29uc3RydWN0UmVzb2x2aW5nUGF0aChrZXlzOiBhbnlbXSk6IHN0cmluZyB7XG4gIGlmIChrZXlzLmxlbmd0aCA+IDEpIHtcbiAgICBjb25zdCByZXZlcnNlZCA9IGZpbmRGaXJzdENsb3NlZEN5Y2xlKGtleXMuc2xpY2UoKS5yZXZlcnNlKCkpO1xuICAgIGNvbnN0IHRva2VuU3RycyA9IHJldmVyc2VkLm1hcChrID0+IHN0cmluZ2lmeShrLnRva2VuKSk7XG4gICAgcmV0dXJuICcgKCcgKyB0b2tlblN0cnMuam9pbignIC0+ICcpICsgJyknO1xuICB9XG5cbiAgcmV0dXJuICcnO1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIEluamVjdGlvbkVycm9yIGV4dGVuZHMgRXJyb3Ige1xuICBrZXlzOiBSZWZsZWN0aXZlS2V5W107XG4gIGluamVjdG9yczogUmVmbGVjdGl2ZUluamVjdG9yW107XG4gIGNvbnN0cnVjdFJlc29sdmluZ01lc3NhZ2U6IChrZXlzOiBSZWZsZWN0aXZlS2V5W10pID0+IHN0cmluZztcbiAgYWRkS2V5KGluamVjdG9yOiBSZWZsZWN0aXZlSW5qZWN0b3IsIGtleTogUmVmbGVjdGl2ZUtleSk6IHZvaWQ7XG59XG5cbmZ1bmN0aW9uIGluamVjdGlvbkVycm9yKFxuICAgIGluamVjdG9yOiBSZWZsZWN0aXZlSW5qZWN0b3IsIGtleTogUmVmbGVjdGl2ZUtleSxcbiAgICBjb25zdHJ1Y3RSZXNvbHZpbmdNZXNzYWdlOiAoa2V5czogUmVmbGVjdGl2ZUtleVtdKSA9PiBzdHJpbmcsXG4gICAgb3JpZ2luYWxFcnJvcj86IEVycm9yKTogSW5qZWN0aW9uRXJyb3Ige1xuICBjb25zdCBrZXlzID0gW2tleV07XG4gIGNvbnN0IGVyck1zZyA9IGNvbnN0cnVjdFJlc29sdmluZ01lc3NhZ2Uoa2V5cyk7XG4gIGNvbnN0IGVycm9yID1cbiAgICAgIChvcmlnaW5hbEVycm9yID8gd3JhcHBlZEVycm9yKGVyck1zZywgb3JpZ2luYWxFcnJvcikgOiBFcnJvcihlcnJNc2cpKSBhcyBJbmplY3Rpb25FcnJvcjtcbiAgZXJyb3IuYWRkS2V5ID0gYWRkS2V5O1xuICBlcnJvci5rZXlzID0ga2V5cztcbiAgZXJyb3IuaW5qZWN0b3JzID0gW2luamVjdG9yXTtcbiAgZXJyb3IuY29uc3RydWN0UmVzb2x2aW5nTWVzc2FnZSA9IGNvbnN0cnVjdFJlc29sdmluZ01lc3NhZ2U7XG4gIChlcnJvciBhcyBhbnkpW0VSUk9SX09SSUdJTkFMX0VSUk9SXSA9IG9yaWdpbmFsRXJyb3I7XG4gIHJldHVybiBlcnJvcjtcbn1cblxuZnVuY3Rpb24gYWRkS2V5KHRoaXM6IEluamVjdGlvbkVycm9yLCBpbmplY3RvcjogUmVmbGVjdGl2ZUluamVjdG9yLCBrZXk6IFJlZmxlY3RpdmVLZXkpOiB2b2lkIHtcbiAgdGhpcy5pbmplY3RvcnMucHVzaChpbmplY3Rvcik7XG4gIHRoaXMua2V5cy5wdXNoKGtleSk7XG4gIC8vIE5vdGU6IFRoaXMgdXBkYXRlZCBtZXNzYWdlIHdvbid0IGJlIHJlZmxlY3RlZCBpbiB0aGUgYC5zdGFja2AgcHJvcGVydHlcbiAgdGhpcy5tZXNzYWdlID0gdGhpcy5jb25zdHJ1Y3RSZXNvbHZpbmdNZXNzYWdlKHRoaXMua2V5cyk7XG59XG5cbi8qKlxuICogVGhyb3duIHdoZW4gdHJ5aW5nIHRvIHJldHJpZXZlIGEgZGVwZW5kZW5jeSBieSBrZXkgZnJvbSB7QGxpbmsgSW5qZWN0b3J9LCBidXQgdGhlXG4gKiB7QGxpbmsgSW5qZWN0b3J9IGRvZXMgbm90IGhhdmUgYSB7QGxpbmsgUHJvdmlkZXJ9IGZvciB0aGUgZ2l2ZW4ga2V5LlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKiAjIyMgRXhhbXBsZVxuICpcbiAqIGBgYHR5cGVzY3JpcHRcbiAqIGNsYXNzIEEge1xuICogICBjb25zdHJ1Y3RvcihiOkIpIHt9XG4gKiB9XG4gKlxuICogZXhwZWN0KCgpID0+IEluamVjdG9yLnJlc29sdmVBbmRDcmVhdGUoW0FdKSkudG9UaHJvd0Vycm9yKCk7XG4gKiBgYGBcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIG5vUHJvdmlkZXJFcnJvcihpbmplY3RvcjogUmVmbGVjdGl2ZUluamVjdG9yLCBrZXk6IFJlZmxlY3RpdmVLZXkpOiBJbmplY3Rpb25FcnJvciB7XG4gIHJldHVybiBpbmplY3Rpb25FcnJvcihpbmplY3Rvciwga2V5LCBmdW5jdGlvbihrZXlzOiBSZWZsZWN0aXZlS2V5W10pIHtcbiAgICBjb25zdCBmaXJzdCA9IHN0cmluZ2lmeShrZXlzWzBdLnRva2VuKTtcbiAgICByZXR1cm4gYE5vIHByb3ZpZGVyIGZvciAke2ZpcnN0fSEke2NvbnN0cnVjdFJlc29sdmluZ1BhdGgoa2V5cyl9YDtcbiAgfSk7XG59XG5cbi8qKlxuICogVGhyb3duIHdoZW4gZGVwZW5kZW5jaWVzIGZvcm0gYSBjeWNsZS5cbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICogIyMjIEV4YW1wbGVcbiAqXG4gKiBgYGB0eXBlc2NyaXB0XG4gKiB2YXIgaW5qZWN0b3IgPSBJbmplY3Rvci5yZXNvbHZlQW5kQ3JlYXRlKFtcbiAqICAge3Byb3ZpZGU6IFwib25lXCIsIHVzZUZhY3Rvcnk6ICh0d28pID0+IFwidHdvXCIsIGRlcHM6IFtbbmV3IEluamVjdChcInR3b1wiKV1dfSxcbiAqICAge3Byb3ZpZGU6IFwidHdvXCIsIHVzZUZhY3Rvcnk6IChvbmUpID0+IFwib25lXCIsIGRlcHM6IFtbbmV3IEluamVjdChcIm9uZVwiKV1dfVxuICogXSk7XG4gKlxuICogZXhwZWN0KCgpID0+IGluamVjdG9yLmdldChcIm9uZVwiKSkudG9UaHJvd0Vycm9yKCk7XG4gKiBgYGBcbiAqXG4gKiBSZXRyaWV2aW5nIGBBYCBvciBgQmAgdGhyb3dzIGEgYEN5Y2xpY0RlcGVuZGVuY3lFcnJvcmAgYXMgdGhlIGdyYXBoIGFib3ZlIGNhbm5vdCBiZSBjb25zdHJ1Y3RlZC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGN5Y2xpY0RlcGVuZGVuY3lFcnJvcihcbiAgICBpbmplY3RvcjogUmVmbGVjdGl2ZUluamVjdG9yLCBrZXk6IFJlZmxlY3RpdmVLZXkpOiBJbmplY3Rpb25FcnJvciB7XG4gIHJldHVybiBpbmplY3Rpb25FcnJvcihpbmplY3Rvciwga2V5LCBmdW5jdGlvbihrZXlzOiBSZWZsZWN0aXZlS2V5W10pIHtcbiAgICByZXR1cm4gYENhbm5vdCBpbnN0YW50aWF0ZSBjeWNsaWMgZGVwZW5kZW5jeSEke2NvbnN0cnVjdFJlc29sdmluZ1BhdGgoa2V5cyl9YDtcbiAgfSk7XG59XG5cbi8qKlxuICogVGhyb3duIHdoZW4gYSBjb25zdHJ1Y3RpbmcgdHlwZSByZXR1cm5zIHdpdGggYW4gRXJyb3IuXG4gKlxuICogVGhlIGBJbnN0YW50aWF0aW9uRXJyb3JgIGNsYXNzIGNvbnRhaW5zIHRoZSBvcmlnaW5hbCBlcnJvciBwbHVzIHRoZSBkZXBlbmRlbmN5IGdyYXBoIHdoaWNoIGNhdXNlZFxuICogdGhpcyBvYmplY3QgdG8gYmUgaW5zdGFudGlhdGVkLlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKiAjIyMgRXhhbXBsZVxuICpcbiAqIGBgYHR5cGVzY3JpcHRcbiAqIGNsYXNzIEEge1xuICogICBjb25zdHJ1Y3RvcigpIHtcbiAqICAgICB0aHJvdyBuZXcgRXJyb3IoJ21lc3NhZ2UnKTtcbiAqICAgfVxuICogfVxuICpcbiAqIHZhciBpbmplY3RvciA9IEluamVjdG9yLnJlc29sdmVBbmRDcmVhdGUoW0FdKTtcblxuICogdHJ5IHtcbiAqICAgaW5qZWN0b3IuZ2V0KEEpO1xuICogfSBjYXRjaCAoZSkge1xuICogICBleHBlY3QoZSBpbnN0YW5jZW9mIEluc3RhbnRpYXRpb25FcnJvcikudG9CZSh0cnVlKTtcbiAqICAgZXhwZWN0KGUub3JpZ2luYWxFeGNlcHRpb24ubWVzc2FnZSkudG9FcXVhbChcIm1lc3NhZ2VcIik7XG4gKiAgIGV4cGVjdChlLm9yaWdpbmFsU3RhY2spLnRvQmVEZWZpbmVkKCk7XG4gKiB9XG4gKiBgYGBcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGluc3RhbnRpYXRpb25FcnJvcihcbiAgICBpbmplY3RvcjogUmVmbGVjdGl2ZUluamVjdG9yLCBvcmlnaW5hbEV4Y2VwdGlvbjogYW55LCBvcmlnaW5hbFN0YWNrOiBhbnksXG4gICAga2V5OiBSZWZsZWN0aXZlS2V5KTogSW5qZWN0aW9uRXJyb3Ige1xuICByZXR1cm4gaW5qZWN0aW9uRXJyb3IoaW5qZWN0b3IsIGtleSwgZnVuY3Rpb24oa2V5czogUmVmbGVjdGl2ZUtleVtdKSB7XG4gICAgY29uc3QgZmlyc3QgPSBzdHJpbmdpZnkoa2V5c1swXS50b2tlbik7XG4gICAgcmV0dXJuIGAke29yaWdpbmFsRXhjZXB0aW9uLm1lc3NhZ2V9OiBFcnJvciBkdXJpbmcgaW5zdGFudGlhdGlvbiBvZiAke2ZpcnN0fSEke2NvbnN0cnVjdFJlc29sdmluZ1BhdGgoa2V5cyl9LmA7XG4gIH0sIG9yaWdpbmFsRXhjZXB0aW9uKTtcbn1cblxuLyoqXG4gKiBUaHJvd24gd2hlbiBhbiBvYmplY3Qgb3RoZXIgdGhlbiB7QGxpbmsgUHJvdmlkZXJ9IChvciBgVHlwZWApIGlzIHBhc3NlZCB0byB7QGxpbmsgSW5qZWN0b3J9XG4gKiBjcmVhdGlvbi5cbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICogIyMjIEV4YW1wbGVcbiAqXG4gKiBgYGB0eXBlc2NyaXB0XG4gKiBleHBlY3QoKCkgPT4gSW5qZWN0b3IucmVzb2x2ZUFuZENyZWF0ZShbXCJub3QgYSB0eXBlXCJdKSkudG9UaHJvd0Vycm9yKCk7XG4gKiBgYGBcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGludmFsaWRQcm92aWRlckVycm9yKHByb3ZpZGVyOiBhbnkpIHtcbiAgcmV0dXJuIEVycm9yKFxuICAgICAgYEludmFsaWQgcHJvdmlkZXIgLSBvbmx5IGluc3RhbmNlcyBvZiBQcm92aWRlciBhbmQgVHlwZSBhcmUgYWxsb3dlZCwgZ290OiAke3Byb3ZpZGVyfWApO1xufVxuXG4vKipcbiAqIFRocm93biB3aGVuIHRoZSBjbGFzcyBoYXMgbm8gYW5ub3RhdGlvbiBpbmZvcm1hdGlvbi5cbiAqXG4gKiBMYWNrIG9mIGFubm90YXRpb24gaW5mb3JtYXRpb24gcHJldmVudHMgdGhlIHtAbGluayBJbmplY3Rvcn0gZnJvbSBkZXRlcm1pbmluZyB3aGljaCBkZXBlbmRlbmNpZXNcbiAqIG5lZWQgdG8gYmUgaW5qZWN0ZWQgaW50byB0aGUgY29uc3RydWN0b3IuXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqICMjIyBFeGFtcGxlXG4gKlxuICogYGBgdHlwZXNjcmlwdFxuICogY2xhc3MgQSB7XG4gKiAgIGNvbnN0cnVjdG9yKGIpIHt9XG4gKiB9XG4gKlxuICogZXhwZWN0KCgpID0+IEluamVjdG9yLnJlc29sdmVBbmRDcmVhdGUoW0FdKSkudG9UaHJvd0Vycm9yKCk7XG4gKiBgYGBcbiAqXG4gKiBUaGlzIGVycm9yIGlzIGFsc28gdGhyb3duIHdoZW4gdGhlIGNsYXNzIG5vdCBtYXJrZWQgd2l0aCB7QGxpbmsgSW5qZWN0YWJsZX0gaGFzIHBhcmFtZXRlciB0eXBlcy5cbiAqXG4gKiBgYGB0eXBlc2NyaXB0XG4gKiBjbGFzcyBCIHt9XG4gKlxuICogY2xhc3MgQSB7XG4gKiAgIGNvbnN0cnVjdG9yKGI6Qikge30gLy8gbm8gaW5mb3JtYXRpb24gYWJvdXQgdGhlIHBhcmFtZXRlciB0eXBlcyBvZiBBIGlzIGF2YWlsYWJsZSBhdCBydW50aW1lLlxuICogfVxuICpcbiAqIGV4cGVjdCgoKSA9PiBJbmplY3Rvci5yZXNvbHZlQW5kQ3JlYXRlKFtBLEJdKSkudG9UaHJvd0Vycm9yKCk7XG4gKiBgYGBcbiAqXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBub0Fubm90YXRpb25FcnJvcih0eXBlT3JGdW5jOiBUeXBlPGFueT58IEZ1bmN0aW9uLCBwYXJhbXM6IGFueVtdW10pOiBFcnJvciB7XG4gIGNvbnN0IHNpZ25hdHVyZTogc3RyaW5nW10gPSBbXTtcbiAgZm9yIChsZXQgaSA9IDAsIGlpID0gcGFyYW1zLmxlbmd0aDsgaSA8IGlpOyBpKyspIHtcbiAgICBjb25zdCBwYXJhbWV0ZXIgPSBwYXJhbXNbaV07XG4gICAgaWYgKCFwYXJhbWV0ZXIgfHwgcGFyYW1ldGVyLmxlbmd0aCA9PSAwKSB7XG4gICAgICBzaWduYXR1cmUucHVzaCgnPycpO1xuICAgIH0gZWxzZSB7XG4gICAgICBzaWduYXR1cmUucHVzaChwYXJhbWV0ZXIubWFwKHN0cmluZ2lmeSkuam9pbignICcpKTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIEVycm9yKFxuICAgICAgJ0Nhbm5vdCByZXNvbHZlIGFsbCBwYXJhbWV0ZXJzIGZvciBcXCcnICsgc3RyaW5naWZ5KHR5cGVPckZ1bmMpICsgJ1xcJygnICtcbiAgICAgIHNpZ25hdHVyZS5qb2luKCcsICcpICsgJykuICcgK1xuICAgICAgJ01ha2Ugc3VyZSB0aGF0IGFsbCB0aGUgcGFyYW1ldGVycyBhcmUgZGVjb3JhdGVkIHdpdGggSW5qZWN0IG9yIGhhdmUgdmFsaWQgdHlwZSBhbm5vdGF0aW9ucyBhbmQgdGhhdCBcXCcnICtcbiAgICAgIHN0cmluZ2lmeSh0eXBlT3JGdW5jKSArICdcXCcgaXMgZGVjb3JhdGVkIHdpdGggSW5qZWN0YWJsZS4nKTtcbn1cblxuLyoqXG4gKiBUaHJvd24gd2hlbiBnZXR0aW5nIGFuIG9iamVjdCBieSBpbmRleC5cbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICogIyMjIEV4YW1wbGVcbiAqXG4gKiBgYGB0eXBlc2NyaXB0XG4gKiBjbGFzcyBBIHt9XG4gKlxuICogdmFyIGluamVjdG9yID0gSW5qZWN0b3IucmVzb2x2ZUFuZENyZWF0ZShbQV0pO1xuICpcbiAqIGV4cGVjdCgoKSA9PiBpbmplY3Rvci5nZXRBdCgxMDApKS50b1Rocm93RXJyb3IoKTtcbiAqIGBgYFxuICpcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIG91dE9mQm91bmRzRXJyb3IoaW5kZXg6IG51bWJlcikge1xuICByZXR1cm4gRXJyb3IoYEluZGV4ICR7aW5kZXh9IGlzIG91dC1vZi1ib3VuZHMuYCk7XG59XG5cbi8vIFRPRE86IGFkZCBhIHdvcmtpbmcgZXhhbXBsZSBhZnRlciBhbHBoYTM4IGlzIHJlbGVhc2VkXG4vKipcbiAqIFRocm93biB3aGVuIGEgbXVsdGkgcHJvdmlkZXIgYW5kIGEgcmVndWxhciBwcm92aWRlciBhcmUgYm91bmQgdG8gdGhlIHNhbWUgdG9rZW4uXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqICMjIyBFeGFtcGxlXG4gKlxuICogYGBgdHlwZXNjcmlwdFxuICogZXhwZWN0KCgpID0+IEluamVjdG9yLnJlc29sdmVBbmRDcmVhdGUoW1xuICogICB7IHByb3ZpZGU6IFwiU3RyaW5nc1wiLCB1c2VWYWx1ZTogXCJzdHJpbmcxXCIsIG11bHRpOiB0cnVlfSxcbiAqICAgeyBwcm92aWRlOiBcIlN0cmluZ3NcIiwgdXNlVmFsdWU6IFwic3RyaW5nMlwiLCBtdWx0aTogZmFsc2V9XG4gKiBdKSkudG9UaHJvd0Vycm9yKCk7XG4gKiBgYGBcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIG1peGluZ011bHRpUHJvdmlkZXJzV2l0aFJlZ3VsYXJQcm92aWRlcnNFcnJvcihcbiAgICBwcm92aWRlcjE6IGFueSwgcHJvdmlkZXIyOiBhbnkpOiBFcnJvciB7XG4gIHJldHVybiBFcnJvcihgQ2Fubm90IG1peCBtdWx0aSBwcm92aWRlcnMgYW5kIHJlZ3VsYXIgcHJvdmlkZXJzLCBnb3Q6ICR7cHJvdmlkZXIxfSAke3Byb3ZpZGVyMn1gKTtcbn1cbiJdfQ==
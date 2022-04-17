/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { ERROR_ORIGINAL_ERROR, wrappedError } from '../util/errors';
import { stringify } from '../util/stringify';
function findFirstClosedCycle(keys) {
    var res = [];
    for (var i = 0; i < keys.length; ++i) {
        if (res.indexOf(keys[i]) > -1) {
            res.push(keys[i]);
            return res;
        }
        res.push(keys[i]);
    }
    return res;
}
function constructResolvingPath(keys) {
    if (keys.length > 1) {
        var reversed = findFirstClosedCycle(keys.slice().reverse());
        var tokenStrs = reversed.map(function (k) { return stringify(k.token); });
        return ' (' + tokenStrs.join(' -> ') + ')';
    }
    return '';
}
function injectionError(injector, key, constructResolvingMessage, originalError) {
    var keys = [key];
    var errMsg = constructResolvingMessage(keys);
    var error = (originalError ? wrappedError(errMsg, originalError) : Error(errMsg));
    error.addKey = addKey;
    error.keys = keys;
    error.injectors = [injector];
    error.constructResolvingMessage = constructResolvingMessage;
    error[ERROR_ORIGINAL_ERROR] = originalError;
    return error;
}
function addKey(injector, key) {
    this.injectors.push(injector);
    this.keys.push(key);
    // Note: This updated message won't be reflected in the `.stack` property
    this.message = this.constructResolvingMessage(this.keys);
}
/**
 * Thrown when trying to retrieve a dependency by key from {@link Injector}, but the
 * {@link Injector} does not have a {@link Provider} for the given key.
 *
 * @usageNotes
 * ### Example
 *
 * ```typescript
 * class A {
 *   constructor(b:B) {}
 * }
 *
 * expect(() => Injector.resolveAndCreate([A])).toThrowError();
 * ```
 */
export function noProviderError(injector, key) {
    return injectionError(injector, key, function (keys) {
        var first = stringify(keys[0].token);
        return "No provider for " + first + "!" + constructResolvingPath(keys);
    });
}
/**
 * Thrown when dependencies form a cycle.
 *
 * @usageNotes
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
 */
export function cyclicDependencyError(injector, key) {
    return injectionError(injector, key, function (keys) {
        return "Cannot instantiate cyclic dependency!" + constructResolvingPath(keys);
    });
}
/**
 * Thrown when a constructing type returns with an Error.
 *
 * The `InstantiationError` class contains the original error plus the dependency graph which caused
 * this object to be instantiated.
 *
 * @usageNotes
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
 */
export function instantiationError(injector, originalException, originalStack, key) {
    return injectionError(injector, key, function (keys) {
        var first = stringify(keys[0].token);
        return originalException.message + ": Error during instantiation of " + first + "!" + constructResolvingPath(keys) + ".";
    }, originalException);
}
/**
 * Thrown when an object other then {@link Provider} (or `Type`) is passed to {@link Injector}
 * creation.
 *
 * @usageNotes
 * ### Example
 *
 * ```typescript
 * expect(() => Injector.resolveAndCreate(["not a type"])).toThrowError();
 * ```
 */
export function invalidProviderError(provider) {
    return Error("Invalid provider - only instances of Provider and Type are allowed, got: " + provider);
}
/**
 * Thrown when the class has no annotation information.
 *
 * Lack of annotation information prevents the {@link Injector} from determining which dependencies
 * need to be injected into the constructor.
 *
 * @usageNotes
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
 * This error is also thrown when the class not marked with {@link Injectable} has parameter types.
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
 */
export function noAnnotationError(typeOrFunc, params) {
    var signature = [];
    for (var i = 0, ii = params.length; i < ii; i++) {
        var parameter = params[i];
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
 * @usageNotes
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
 */
export function outOfBoundsError(index) {
    return Error("Index " + index + " is out-of-bounds.");
}
// TODO: add a working example after alpha38 is released
/**
 * Thrown when a multi provider and a regular provider are bound to the same token.
 *
 * @usageNotes
 * ### Example
 *
 * ```typescript
 * expect(() => Injector.resolveAndCreate([
 *   { provide: "Strings", useValue: "string1", multi: true},
 *   { provide: "Strings", useValue: "string2", multi: false}
 * ])).toThrowError();
 * ```
 */
export function mixingMultiProvidersWithRegularProvidersError(provider1, provider2) {
    return Error("Cannot mix multi providers and regular providers, got: " + provider1 + " " + provider2);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVmbGVjdGl2ZV9lcnJvcnMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9kaS9yZWZsZWN0aXZlX2Vycm9ycy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFHSCxPQUFPLEVBQUMsb0JBQW9CLEVBQUUsWUFBWSxFQUFDLE1BQU0sZ0JBQWdCLENBQUM7QUFDbEUsT0FBTyxFQUFDLFNBQVMsRUFBQyxNQUFNLG1CQUFtQixDQUFDO0FBSzVDLFNBQVMsb0JBQW9CLENBQUMsSUFBVztJQUN2QyxJQUFNLEdBQUcsR0FBVSxFQUFFLENBQUM7SUFDdEIsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLElBQUksQ0FBQyxNQUFNLEVBQUUsRUFBRSxDQUFDLEVBQUU7UUFDcEMsSUFBSSxHQUFHLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxHQUFHLENBQUMsQ0FBQyxFQUFFO1lBQzdCLEdBQUcsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7WUFDbEIsT0FBTyxHQUFHLENBQUM7U0FDWjtRQUNELEdBQUcsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7S0FDbkI7SUFDRCxPQUFPLEdBQUcsQ0FBQztBQUNiLENBQUM7QUFFRCxTQUFTLHNCQUFzQixDQUFDLElBQVc7SUFDekMsSUFBSSxJQUFJLENBQUMsTUFBTSxHQUFHLENBQUMsRUFBRTtRQUNuQixJQUFNLFFBQVEsR0FBRyxvQkFBb0IsQ0FBQyxJQUFJLENBQUMsS0FBSyxFQUFFLENBQUMsT0FBTyxFQUFFLENBQUMsQ0FBQztRQUM5RCxJQUFNLFNBQVMsR0FBRyxRQUFRLENBQUMsR0FBRyxDQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsU0FBUyxDQUFDLENBQUMsQ0FBQyxLQUFL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QUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1R5cGV9IGZyb20gJy4uL2ludGVyZmFjZS90eXBlJztcbmltcG9ydCB7RVJST1JfT1JJR0lOQUxfRVJST1IsIHdyYXBwZWRFcnJvcn0gZnJvbSAnLi4vdXRpbC9lcnJvcnMnO1xuaW1wb3J0IHtzdHJpbmdpZnl9IGZyb20gJy4uL3V0aWwvc3RyaW5naWZ5JztcblxuaW1wb3J0IHtSZWZsZWN0aXZlSW5qZWN0b3J9IGZyb20gJy4vcmVmbGVjdGl2ZV9pbmplY3Rvcic7XG5pbXBvcnQge1JlZmxlY3RpdmVLZXl9IGZyb20gJy4vcmVmbGVjdGl2ZV9rZXknO1xuXG5mdW5jdGlvbiBmaW5kRmlyc3RDbG9zZWRDeWNsZShrZXlzOiBhbnlbXSk6IGFueVtdIHtcbiAgY29uc3QgcmVzOiBhbnlbXSA9IFtdO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGtleXMubGVuZ3RoOyArK2kpIHtcbiAgICBpZiAocmVzLmluZGV4T2Yoa2V5c1tpXSkgPiAtMSkge1xuICAgICAgcmVzLnB1c2goa2V5c1tpXSk7XG4gICAgICByZXR1cm4gcmVzO1xuICAgIH1cbiAgICByZXMucHVzaChrZXlzW2ldKTtcbiAgfVxuICByZXR1cm4gcmVzO1xufVxuXG5mdW5jdGlvbiBjb25zdHJ1Y3RSZXNvbHZpbmdQYXRoKGtleXM6IGFueVtdKTogc3RyaW5nIHtcbiAgaWYgKGtleXMubGVuZ3RoID4gMSkge1xuICAgIGNvbnN0IHJldmVyc2VkID0gZmluZEZpcnN0Q2xvc2VkQ3ljbGUoa2V5cy5zbGljZSgpLnJldmVyc2UoKSk7XG4gICAgY29uc3QgdG9rZW5TdHJzID0gcmV2ZXJzZWQubWFwKGsgPT4gc3RyaW5naWZ5KGsudG9rZW4pKTtcbiAgICByZXR1cm4gJyAoJyArIHRva2VuU3Rycy5qb2luKCcgLT4gJykgKyAnKSc7XG4gIH1cblxuICByZXR1cm4gJyc7XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgSW5qZWN0aW9uRXJyb3IgZXh0ZW5kcyBFcnJvciB7XG4gIGtleXM6IFJlZmxlY3RpdmVLZXlbXTtcbiAgaW5qZWN0b3JzOiBSZWZsZWN0aXZlSW5qZWN0b3JbXTtcbiAgY29uc3RydWN0UmVzb2x2aW5nTWVzc2FnZTogKGtleXM6IFJlZmxlY3RpdmVLZXlbXSkgPT4gc3RyaW5nO1xuICBhZGRLZXkoaW5qZWN0b3I6IFJlZmxlY3RpdmVJbmplY3Rvciwga2V5OiBSZWZsZWN0aXZlS2V5KTogdm9pZDtcbn1cblxuZnVuY3Rpb24gaW5qZWN0aW9uRXJyb3IoXG4gICAgaW5qZWN0b3I6IFJlZmxlY3RpdmVJbmplY3Rvciwga2V5OiBSZWZsZWN0aXZlS2V5LFxuICAgIGNvbnN0cnVjdFJlc29sdmluZ01lc3NhZ2U6IChrZXlzOiBSZWZsZWN0aXZlS2V5W10pID0+IHN0cmluZyxcbiAgICBvcmlnaW5hbEVycm9yPzogRXJyb3IpOiBJbmplY3Rpb25FcnJvciB7XG4gIGNvbnN0IGtleXMgPSBba2V5XTtcbiAgY29uc3QgZXJyTXNnID0gY29uc3RydWN0UmVzb2x2aW5nTWVzc2FnZShrZXlzKTtcbiAgY29uc3QgZXJyb3IgPVxuICAgICAgKG9yaWdpbmFsRXJyb3IgPyB3cmFwcGVkRXJyb3IoZXJyTXNnLCBvcmlnaW5hbEVycm9yKSA6IEVycm9yKGVyck1zZykpIGFzIEluamVjdGlvbkVycm9yO1xuICBlcnJvci5hZGRLZXkgPSBhZGRLZXk7XG4gIGVycm9yLmtleXMgPSBrZXlzO1xuICBlcnJvci5pbmplY3RvcnMgPSBbaW5qZWN0b3JdO1xuICBlcnJvci5jb25zdHJ1Y3RSZXNvbHZpbmdNZXNzYWdlID0gY29uc3RydWN0UmVzb2x2aW5nTWVzc2FnZTtcbiAgKGVycm9yIGFzIGFueSlbRVJST1JfT1JJR0lOQUxfRVJST1JdID0gb3JpZ2luYWxFcnJvcjtcbiAgcmV0dXJuIGVycm9yO1xufVxuXG5mdW5jdGlvbiBhZGRLZXkodGhpczogSW5qZWN0aW9uRXJyb3IsIGluamVjdG9yOiBSZWZsZWN0aXZlSW5qZWN0b3IsIGtleTogUmVmbGVjdGl2ZUtleSk6IHZvaWQge1xuICB0aGlzLmluamVjdG9ycy5wdXNoKGluamVjdG9yKTtcbiAgdGhpcy5rZXlzLnB1c2goa2V5KTtcbiAgLy8gTm90ZTogVGhpcyB1cGRhdGVkIG1lc3NhZ2Ugd29uJ3QgYmUgcmVmbGVjdGVkIGluIHRoZSBgLnN0YWNrYCBwcm9wZXJ0eVxuICB0aGlzLm1lc3NhZ2UgPSB0aGlzLmNvbnN0cnVjdFJlc29sdmluZ01lc3NhZ2UodGhpcy5rZXlzKTtcbn1cblxuLyoqXG4gKiBUaHJvd24gd2hlbiB0cnlpbmcgdG8gcmV0cmlldmUgYSBkZXBlbmRlbmN5IGJ5IGtleSBmcm9tIHtAbGluayBJbmplY3Rvcn0sIGJ1dCB0aGVcbiAqIHtAbGluayBJbmplY3Rvcn0gZG9lcyBub3QgaGF2ZSBhIHtAbGluayBQcm92aWRlcn0gZm9yIHRoZSBnaXZlbiBrZXkuXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqICMjIyBFeGFtcGxlXG4gKlxuICogYGBgdHlwZXNjcmlwdFxuICogY2xhc3MgQSB7XG4gKiAgIGNvbnN0cnVjdG9yKGI6Qikge31cbiAqIH1cbiAqXG4gKiBleHBlY3QoKCkgPT4gSW5qZWN0b3IucmVzb2x2ZUFuZENyZWF0ZShbQV0pKS50b1Rocm93RXJyb3IoKTtcbiAqIGBgYFxuICovXG5leHBvcnQgZnVuY3Rpb24gbm9Qcm92aWRlckVycm9yKGluamVjdG9yOiBSZWZsZWN0aXZlSW5qZWN0b3IsIGtleTogUmVmbGVjdGl2ZUtleSk6IEluamVjdGlvbkVycm9yIHtcbiAgcmV0dXJuIGluamVjdGlvbkVycm9yKGluamVjdG9yLCBrZXksIGZ1bmN0aW9uKGtleXM6IFJlZmxlY3RpdmVLZXlbXSkge1xuICAgIGNvbnN0IGZpcnN0ID0gc3RyaW5naWZ5KGtleXNbMF0udG9rZW4pO1xuICAgIHJldHVybiBgTm8gcHJvdmlkZXIgZm9yICR7Zmlyc3R9ISR7Y29uc3RydWN0UmVzb2x2aW5nUGF0aChrZXlzKX1gO1xuICB9KTtcbn1cblxuLyoqXG4gKiBUaHJvd24gd2hlbiBkZXBlbmRlbmNpZXMgZm9ybSBhIGN5Y2xlLlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKiAjIyMgRXhhbXBsZVxuICpcbiAqIGBgYHR5cGVzY3JpcHRcbiAqIHZhciBpbmplY3RvciA9IEluamVjdG9yLnJlc29sdmVBbmRDcmVhdGUoW1xuICogICB7cHJvdmlkZTogXCJvbmVcIiwgdXNlRmFjdG9yeTogKHR3bykgPT4gXCJ0d29cIiwgZGVwczogW1tuZXcgSW5qZWN0KFwidHdvXCIpXV19LFxuICogICB7cHJvdmlkZTogXCJ0d29cIiwgdXNlRmFjdG9yeTogKG9uZSkgPT4gXCJvbmVcIiwgZGVwczogW1tuZXcgSW5qZWN0KFwib25lXCIpXV19XG4gKiBdKTtcbiAqXG4gKiBleHBlY3QoKCkgPT4gaW5qZWN0b3IuZ2V0KFwib25lXCIpKS50b1Rocm93RXJyb3IoKTtcbiAqIGBgYFxuICpcbiAqIFJldHJpZXZpbmcgYEFgIG9yIGBCYCB0aHJvd3MgYSBgQ3ljbGljRGVwZW5kZW5jeUVycm9yYCBhcyB0aGUgZ3JhcGggYWJvdmUgY2Fubm90IGJlIGNvbnN0cnVjdGVkLlxuICovXG5leHBvcnQgZnVuY3Rpb24gY3ljbGljRGVwZW5kZW5jeUVycm9yKFxuICAgIGluamVjdG9yOiBSZWZsZWN0aXZlSW5qZWN0b3IsIGtleTogUmVmbGVjdGl2ZUtleSk6IEluamVjdGlvbkVycm9yIHtcbiAgcmV0dXJuIGluamVjdGlvbkVycm9yKGluamVjdG9yLCBrZXksIGZ1bmN0aW9uKGtleXM6IFJlZmxlY3RpdmVLZXlbXSkge1xuICAgIHJldHVybiBgQ2Fubm90IGluc3RhbnRpYXRlIGN5Y2xpYyBkZXBlbmRlbmN5ISR7Y29uc3RydWN0UmVzb2x2aW5nUGF0aChrZXlzKX1gO1xuICB9KTtcbn1cblxuLyoqXG4gKiBUaHJvd24gd2hlbiBhIGNvbnN0cnVjdGluZyB0eXBlIHJldHVybnMgd2l0aCBhbiBFcnJvci5cbiAqXG4gKiBUaGUgYEluc3RhbnRpYXRpb25FcnJvcmAgY2xhc3MgY29udGFpbnMgdGhlIG9yaWdpbmFsIGVycm9yIHBsdXMgdGhlIGRlcGVuZGVuY3kgZ3JhcGggd2hpY2ggY2F1c2VkXG4gKiB0aGlzIG9iamVjdCB0byBiZSBpbnN0YW50aWF0ZWQuXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqICMjIyBFeGFtcGxlXG4gKlxuICogYGBgdHlwZXNjcmlwdFxuICogY2xhc3MgQSB7XG4gKiAgIGNvbnN0cnVjdG9yKCkge1xuICogICAgIHRocm93IG5ldyBFcnJvcignbWVzc2FnZScpO1xuICogICB9XG4gKiB9XG4gKlxuICogdmFyIGluamVjdG9yID0gSW5qZWN0b3IucmVzb2x2ZUFuZENyZWF0ZShbQV0pO1xuXG4gKiB0cnkge1xuICogICBpbmplY3Rvci5nZXQoQSk7XG4gKiB9IGNhdGNoIChlKSB7XG4gKiAgIGV4cGVjdChlIGluc3RhbmNlb2YgSW5zdGFudGlhdGlvbkVycm9yKS50b0JlKHRydWUpO1xuICogICBleHBlY3QoZS5vcmlnaW5hbEV4Y2VwdGlvbi5tZXNzYWdlKS50b0VxdWFsKFwibWVzc2FnZVwiKTtcbiAqICAgZXhwZWN0KGUub3JpZ2luYWxTdGFjaykudG9CZURlZmluZWQoKTtcbiAqIH1cbiAqIGBgYFxuICovXG5leHBvcnQgZnVuY3Rpb24gaW5zdGFudGlhdGlvbkVycm9yKFxuICAgIGluamVjdG9yOiBSZWZsZWN0aXZlSW5qZWN0b3IsIG9yaWdpbmFsRXhjZXB0aW9uOiBhbnksIG9yaWdpbmFsU3RhY2s6IGFueSxcbiAgICBrZXk6IFJlZmxlY3RpdmVLZXkpOiBJbmplY3Rpb25FcnJvciB7XG4gIHJldHVybiBpbmplY3Rpb25FcnJvcihpbmplY3Rvciwga2V5LCBmdW5jdGlvbihrZXlzOiBSZWZsZWN0aXZlS2V5W10pIHtcbiAgICBjb25zdCBmaXJzdCA9IHN0cmluZ2lmeShrZXlzWzBdLnRva2VuKTtcbiAgICByZXR1cm4gYCR7b3JpZ2luYWxFeGNlcHRpb24ubWVzc2FnZX06IEVycm9yIGR1cmluZyBpbnN0YW50aWF0aW9uIG9mICR7Zmlyc3R9ISR7Y29uc3RydWN0UmVzb2x2aW5nUGF0aChrZXlzKX0uYDtcbiAgfSwgb3JpZ2luYWxFeGNlcHRpb24pO1xufVxuXG4vKipcbiAqIFRocm93biB3aGVuIGFuIG9iamVjdCBvdGhlciB0aGVuIHtAbGluayBQcm92aWRlcn0gKG9yIGBUeXBlYCkgaXMgcGFzc2VkIHRvIHtAbGluayBJbmplY3Rvcn1cbiAqIGNyZWF0aW9uLlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKiAjIyMgRXhhbXBsZVxuICpcbiAqIGBgYHR5cGVzY3JpcHRcbiAqIGV4cGVjdCgoKSA9PiBJbmplY3Rvci5yZXNvbHZlQW5kQ3JlYXRlKFtcIm5vdCBhIHR5cGVcIl0pKS50b1Rocm93RXJyb3IoKTtcbiAqIGBgYFxuICovXG5leHBvcnQgZnVuY3Rpb24gaW52YWxpZFByb3ZpZGVyRXJyb3IocHJvdmlkZXI6IGFueSkge1xuICByZXR1cm4gRXJyb3IoXG4gICAgICBgSW52YWxpZCBwcm92aWRlciAtIG9ubHkgaW5zdGFuY2VzIG9mIFByb3ZpZGVyIGFuZCBUeXBlIGFyZSBhbGxvd2VkLCBnb3Q6ICR7cHJvdmlkZXJ9YCk7XG59XG5cbi8qKlxuICogVGhyb3duIHdoZW4gdGhlIGNsYXNzIGhhcyBubyBhbm5vdGF0aW9uIGluZm9ybWF0aW9uLlxuICpcbiAqIExhY2sgb2YgYW5ub3RhdGlvbiBpbmZvcm1hdGlvbiBwcmV2ZW50cyB0aGUge0BsaW5rIEluamVjdG9yfSBmcm9tIGRldGVybWluaW5nIHdoaWNoIGRlcGVuZGVuY2llc1xuICogbmVlZCB0byBiZSBpbmplY3RlZCBpbnRvIHRoZSBjb25zdHJ1Y3Rvci5cbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICogIyMjIEV4YW1wbGVcbiAqXG4gKiBgYGB0eXBlc2NyaXB0XG4gKiBjbGFzcyBBIHtcbiAqICAgY29uc3RydWN0b3IoYikge31cbiAqIH1cbiAqXG4gKiBleHBlY3QoKCkgPT4gSW5qZWN0b3IucmVzb2x2ZUFuZENyZWF0ZShbQV0pKS50b1Rocm93RXJyb3IoKTtcbiAqIGBgYFxuICpcbiAqIFRoaXMgZXJyb3IgaXMgYWxzbyB0aHJvd24gd2hlbiB0aGUgY2xhc3Mgbm90IG1hcmtlZCB3aXRoIHtAbGluayBJbmplY3RhYmxlfSBoYXMgcGFyYW1ldGVyIHR5cGVzLlxuICpcbiAqIGBgYHR5cGVzY3JpcHRcbiAqIGNsYXNzIEIge31cbiAqXG4gKiBjbGFzcyBBIHtcbiAqICAgY29uc3RydWN0b3IoYjpCKSB7fSAvLyBubyBpbmZvcm1hdGlvbiBhYm91dCB0aGUgcGFyYW1ldGVyIHR5cGVzIG9mIEEgaXMgYXZhaWxhYmxlIGF0IHJ1bnRpbWUuXG4gKiB9XG4gKlxuICogZXhwZWN0KCgpID0+IEluamVjdG9yLnJlc29sdmVBbmRDcmVhdGUoW0EsQl0pKS50b1Rocm93RXJyb3IoKTtcbiAqIGBgYFxuICpcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIG5vQW5ub3RhdGlvbkVycm9yKHR5cGVPckZ1bmM6IFR5cGU8YW55PnwgRnVuY3Rpb24sIHBhcmFtczogYW55W11bXSk6IEVycm9yIHtcbiAgY29uc3Qgc2lnbmF0dXJlOiBzdHJpbmdbXSA9IFtdO1xuICBmb3IgKGxldCBpID0gMCwgaWkgPSBwYXJhbXMubGVuZ3RoOyBpIDwgaWk7IGkrKykge1xuICAgIGNvbnN0IHBhcmFtZXRlciA9IHBhcmFtc1tpXTtcbiAgICBpZiAoIXBhcmFtZXRlciB8fCBwYXJhbWV0ZXIubGVuZ3RoID09IDApIHtcbiAgICAgIHNpZ25hdHVyZS5wdXNoKCc/Jyk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHNpZ25hdHVyZS5wdXNoKHBhcmFtZXRlci5tYXAoc3RyaW5naWZ5KS5qb2luKCcgJykpO1xuICAgIH1cbiAgfVxuICByZXR1cm4gRXJyb3IoXG4gICAgICAnQ2Fubm90IHJlc29sdmUgYWxsIHBhcmFtZXRlcnMgZm9yIFxcJycgKyBzdHJpbmdpZnkodHlwZU9yRnVuYykgKyAnXFwnKCcgK1xuICAgICAgc2lnbmF0dXJlLmpvaW4oJywgJykgKyAnKS4gJyArXG4gICAgICAnTWFrZSBzdXJlIHRoYXQgYWxsIHRoZSBwYXJhbWV0ZXJzIGFyZSBkZWNvcmF0ZWQgd2l0aCBJbmplY3Qgb3IgaGF2ZSB2YWxpZCB0eXBlIGFubm90YXRpb25zIGFuZCB0aGF0IFxcJycgK1xuICAgICAgc3RyaW5naWZ5KHR5cGVPckZ1bmMpICsgJ1xcJyBpcyBkZWNvcmF0ZWQgd2l0aCBJbmplY3RhYmxlLicpO1xufVxuXG4vKipcbiAqIFRocm93biB3aGVuIGdldHRpbmcgYW4gb2JqZWN0IGJ5IGluZGV4LlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKiAjIyMgRXhhbXBsZVxuICpcbiAqIGBgYHR5cGVzY3JpcHRcbiAqIGNsYXNzIEEge31cbiAqXG4gKiB2YXIgaW5qZWN0b3IgPSBJbmplY3Rvci5yZXNvbHZlQW5kQ3JlYXRlKFtBXSk7XG4gKlxuICogZXhwZWN0KCgpID0+IGluamVjdG9yLmdldEF0KDEwMCkpLnRvVGhyb3dFcnJvcigpO1xuICogYGBgXG4gKlxuICovXG5leHBvcnQgZnVuY3Rpb24gb3V0T2ZCb3VuZHNFcnJvcihpbmRleDogbnVtYmVyKSB7XG4gIHJldHVybiBFcnJvcihgSW5kZXggJHtpbmRleH0gaXMgb3V0LW9mLWJvdW5kcy5gKTtcbn1cblxuLy8gVE9ETzogYWRkIGEgd29ya2luZyBleGFtcGxlIGFmdGVyIGFscGhhMzggaXMgcmVsZWFzZWRcbi8qKlxuICogVGhyb3duIHdoZW4gYSBtdWx0aSBwcm92aWRlciBhbmQgYSByZWd1bGFyIHByb3ZpZGVyIGFyZSBib3VuZCB0byB0aGUgc2FtZSB0b2tlbi5cbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICogIyMjIEV4YW1wbGVcbiAqXG4gKiBgYGB0eXBlc2NyaXB0XG4gKiBleHBlY3QoKCkgPT4gSW5qZWN0b3IucmVzb2x2ZUFuZENyZWF0ZShbXG4gKiAgIHsgcHJvdmlkZTogXCJTdHJpbmdzXCIsIHVzZVZhbHVlOiBcInN0cmluZzFcIiwgbXVsdGk6IHRydWV9LFxuICogICB7IHByb3ZpZGU6IFwiU3RyaW5nc1wiLCB1c2VWYWx1ZTogXCJzdHJpbmcyXCIsIG11bHRpOiBmYWxzZX1cbiAqIF0pKS50b1Rocm93RXJyb3IoKTtcbiAqIGBgYFxuICovXG5leHBvcnQgZnVuY3Rpb24gbWl4aW5nTXVsdGlQcm92aWRlcnNXaXRoUmVndWxhclByb3ZpZGVyc0Vycm9yKFxuICAgIHByb3ZpZGVyMTogYW55LCBwcm92aWRlcjI6IGFueSk6IEVycm9yIHtcbiAgcmV0dXJuIEVycm9yKGBDYW5ub3QgbWl4IG11bHRpIHByb3ZpZGVycyBhbmQgcmVndWxhciBwcm92aWRlcnMsIGdvdDogJHtwcm92aWRlcjF9ICR7cHJvdmlkZXIyfWApO1xufVxuIl19
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
import { asyncFallback } from './async_fallback';
/**
 * Wraps a test function in an asynchronous test zone. The test will automatically
 * complete when all asynchronous calls within this zone are done. Can be used
 * to wrap an {\@link inject} call.
 *
 * Example:
 *
 * ```
 * it('...', async(inject([AClass], (object) => {
 *   object.doSomething.then(() => {
 *     expect(...);
 *   })
 * });
 * ```
 *
 * \@publicApi
 * @param {?} fn
 * @return {?}
 */
export function async(fn) {
    /** @type {?} */
    const _Zone = typeof Zone !== 'undefined' ? Zone : null;
    if (!_Zone) {
        return (/**
         * @return {?}
         */
        function () {
            return Promise.reject('Zone is needed for the async() test helper but could not be found. ' +
                'Please make sure that your environment includes zone.js/dist/zone.js');
        });
    }
    /** @type {?} */
    const asyncTest = _Zone && _Zone[_Zone.__symbol__('asyncTest')];
    if (typeof asyncTest === 'function') {
        return asyncTest(fn);
    }
    // not using new version of zone.js
    // TODO @JiaLiPassion, remove this after all library updated to
    // newest version of zone.js(0.8.25)
    return asyncFallback(fn);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYXN5bmMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3Rlc3Rpbmcvc3JjL2FzeW5jLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLGFBQWEsRUFBQyxNQUFNLGtCQUFrQixDQUFDOzs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQW1CL0MsTUFBTSxVQUFVLEtBQUssQ0FBQyxFQUFZOztVQUMxQixLQUFLLEdBQVEsT0FBTyxJQUFJLEtBQUssV0FBVyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLElBQUk7SUFDNUQsSUFBSSxDQUFDLEtBQUssRUFBRTtRQUNWOzs7UUFBTztZQUNMLE9BQU8sT0FBTyxDQUFDLE1BQU0sQ0FDakIscUVBQXFFO2dCQUNyRSxzRUFBc0UsQ0FBQyxDQUFDO1FBQzlFLENBQUMsRUFBQztLQUNIOztVQUNLLFNBQVMsR0FBRyxLQUFLLElBQUksS0FBSyxDQUFDLEtBQUssQ0FBQyxVQUFVLENBQUMsV0FBVyxDQUFDLENBQUM7SUFDL0QsSUFBSSxPQUFPLFNBQVMsS0FBSyxVQUFVLEVBQUU7UUFDbkMsT0FBTyxTQUFTLENBQUMsRUFBRSxDQUFDLENBQUM7S0FDdEI7SUFDRCxtQ0FBbUM7SUFDbkMsK0RBQStEO0lBQy9ELG9DQUFvQztJQUNwQyxPQUFPLGFBQWEsQ0FBQyxFQUFFLENBQUMsQ0FBQztBQUMzQixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge2FzeW5jRmFsbGJhY2t9IGZyb20gJy4vYXN5bmNfZmFsbGJhY2snO1xuXG4vKipcbiAqIFdyYXBzIGEgdGVzdCBmdW5jdGlvbiBpbiBhbiBhc3luY2hyb25vdXMgdGVzdCB6b25lLiBUaGUgdGVzdCB3aWxsIGF1dG9tYXRpY2FsbHlcbiAqIGNvbXBsZXRlIHdoZW4gYWxsIGFzeW5jaHJvbm91cyBjYWxscyB3aXRoaW4gdGhpcyB6b25lIGFyZSBkb25lLiBDYW4gYmUgdXNlZFxuICogdG8gd3JhcCBhbiB7QGxpbmsgaW5qZWN0fSBjYWxsLlxuICpcbiAqIEV4YW1wbGU6XG4gKlxuICogYGBgXG4gKiBpdCgnLi4uJywgYXN5bmMoaW5qZWN0KFtBQ2xhc3NdLCAob2JqZWN0KSA9PiB7XG4gKiAgIG9iamVjdC5kb1NvbWV0aGluZy50aGVuKCgpID0+IHtcbiAqICAgICBleHBlY3QoLi4uKTtcbiAqICAgfSlcbiAqIH0pO1xuICogYGBgXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gYXN5bmMoZm46IEZ1bmN0aW9uKTogKGRvbmU6IGFueSkgPT4gYW55IHtcbiAgY29uc3QgX1pvbmU6IGFueSA9IHR5cGVvZiBab25lICE9PSAndW5kZWZpbmVkJyA/IFpvbmUgOiBudWxsO1xuICBpZiAoIV9ab25lKSB7XG4gICAgcmV0dXJuIGZ1bmN0aW9uKCkge1xuICAgICAgcmV0dXJuIFByb21pc2UucmVqZWN0KFxuICAgICAgICAgICdab25lIGlzIG5lZWRlZCBmb3IgdGhlIGFzeW5jKCkgdGVzdCBoZWxwZXIgYnV0IGNvdWxkIG5vdCBiZSBmb3VuZC4gJyArXG4gICAgICAgICAgJ1BsZWFzZSBtYWtlIHN1cmUgdGhhdCB5b3VyIGVudmlyb25tZW50IGluY2x1ZGVzIHpvbmUuanMvZGlzdC96b25lLmpzJyk7XG4gICAgfTtcbiAgfVxuICBjb25zdCBhc3luY1Rlc3QgPSBfWm9uZSAmJiBfWm9uZVtfWm9uZS5fX3N5bWJvbF9fKCdhc3luY1Rlc3QnKV07XG4gIGlmICh0eXBlb2YgYXN5bmNUZXN0ID09PSAnZnVuY3Rpb24nKSB7XG4gICAgcmV0dXJuIGFzeW5jVGVzdChmbik7XG4gIH1cbiAgLy8gbm90IHVzaW5nIG5ldyB2ZXJzaW9uIG9mIHpvbmUuanNcbiAgLy8gVE9ETyBASmlhTGlQYXNzaW9uLCByZW1vdmUgdGhpcyBhZnRlciBhbGwgbGlicmFyeSB1cGRhdGVkIHRvXG4gIC8vIG5ld2VzdCB2ZXJzaW9uIG9mIHpvbmUuanMoMC44LjI1KVxuICByZXR1cm4gYXN5bmNGYWxsYmFjayhmbik7XG59Il19
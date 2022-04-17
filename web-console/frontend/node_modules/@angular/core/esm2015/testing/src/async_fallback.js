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
const _global = (/** @type {?} */ ((typeof window === 'undefined' ? global : window)));
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
 *
 * @param {?} fn
 * @return {?}
 */
export function asyncFallback(fn) {
    // If we're running using the Jasmine test framework, adapt to call the 'done'
    // function when asynchronous activity is finished.
    if (_global.jasmine) {
        // Not using an arrow function to preserve context passed from call site
        return (/**
         * @this {?}
         * @param {?} done
         * @return {?}
         */
        function (done) {
            if (!done) {
                // if we run beforeEach in @angular/core/testing/testing_internal then we get no done
                // fake it here and assume sync.
                done = (/**
                 * @return {?}
                 */
                function () { });
                done.fail = (/**
                 * @param {?} e
                 * @return {?}
                 */
                function (e) { throw e; });
            }
            runInTestZone(fn, this, done, (/**
             * @param {?} err
             * @return {?}
             */
            (err) => {
                if (typeof err === 'string') {
                    return done.fail(new Error((/** @type {?} */ (err))));
                }
                else {
                    done.fail(err);
                }
            }));
        });
    }
    // Otherwise, return a promise which will resolve when asynchronous activity
    // is finished. This will be correctly consumed by the Mocha framework with
    // it('...', async(myFn)); or can be used in a custom framework.
    // Not using an arrow function to preserve context passed from call site
    return (/**
     * @this {?}
     * @return {?}
     */
    function () {
        return new Promise((/**
         * @param {?} finishCallback
         * @param {?} failCallback
         * @return {?}
         */
        (finishCallback, failCallback) => {
            runInTestZone(fn, this, finishCallback, failCallback);
        }));
    });
}
/**
 * @param {?} fn
 * @param {?} context
 * @param {?} finishCallback
 * @param {?} failCallback
 * @return {?}
 */
function runInTestZone(fn, context, finishCallback, failCallback) {
    /** @type {?} */
    const currentZone = Zone.current;
    /** @type {?} */
    const AsyncTestZoneSpec = ((/** @type {?} */ (Zone)))['AsyncTestZoneSpec'];
    if (AsyncTestZoneSpec === undefined) {
        throw new Error('AsyncTestZoneSpec is needed for the async() test helper but could not be found. ' +
            'Please make sure that your environment includes zone.js/dist/async-test.js');
    }
    /** @type {?} */
    const ProxyZoneSpec = (/** @type {?} */ (((/** @type {?} */ (Zone)))['ProxyZoneSpec']));
    if (ProxyZoneSpec === undefined) {
        throw new Error('ProxyZoneSpec is needed for the async() test helper but could not be found. ' +
            'Please make sure that your environment includes zone.js/dist/proxy.js');
    }
    /** @type {?} */
    const proxyZoneSpec = ProxyZoneSpec.get();
    ProxyZoneSpec.assertPresent();
    // We need to create the AsyncTestZoneSpec outside the ProxyZone.
    // If we do it in ProxyZone then we will get to infinite recursion.
    /** @type {?} */
    const proxyZone = Zone.current.getZoneWith('ProxyZoneSpec');
    /** @type {?} */
    const previousDelegate = proxyZoneSpec.getDelegate();
    (/** @type {?} */ ((/** @type {?} */ (proxyZone)).parent)).run((/**
     * @return {?}
     */
    () => {
        /** @type {?} */
        const testZoneSpec = new AsyncTestZoneSpec((/**
         * @return {?}
         */
        () => {
            // Need to restore the original zone.
            currentZone.run((/**
             * @return {?}
             */
            () => {
                if (proxyZoneSpec.getDelegate() == testZoneSpec) {
                    // Only reset the zone spec if it's sill this one. Otherwise, assume it's OK.
                    proxyZoneSpec.setDelegate(previousDelegate);
                }
                finishCallback();
            }));
        }), (/**
         * @param {?} error
         * @return {?}
         */
        (error) => {
            // Need to restore the original zone.
            currentZone.run((/**
             * @return {?}
             */
            () => {
                if (proxyZoneSpec.getDelegate() == testZoneSpec) {
                    // Only reset the zone spec if it's sill this one. Otherwise, assume it's OK.
                    proxyZoneSpec.setDelegate(previousDelegate);
                }
                failCallback(error);
            }));
        }), 'test');
        proxyZoneSpec.setDelegate(testZoneSpec);
    }));
    return Zone.current.runGuarded(fn, context);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYXN5bmNfZmFsbGJhY2suanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3Rlc3Rpbmcvc3JjL2FzeW5jX2ZhbGxiYWNrLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7OztNQWNNLE9BQU8sR0FBRyxtQkFBSyxDQUFDLE9BQU8sTUFBTSxLQUFLLFdBQVcsQ0FBQyxDQUFDLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxNQUFNLENBQUMsRUFBQTs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFtQnRFLE1BQU0sVUFBVSxhQUFhLENBQUMsRUFBWTtJQUN4Qyw4RUFBOEU7SUFDOUUsbURBQW1EO0lBQ25ELElBQUksT0FBTyxDQUFDLE9BQU8sRUFBRTtRQUNuQix3RUFBd0U7UUFDeEU7Ozs7O1FBQU8sVUFBd0IsSUFBUztZQUN0QyxJQUFJLENBQUMsSUFBSSxFQUFFO2dCQUNULHFGQUFxRjtnQkFDckYsZ0NBQWdDO2dCQUNoQyxJQUFJOzs7Z0JBQUcsY0FBWSxDQUFDLENBQUEsQ0FBQztnQkFDckIsSUFBSSxDQUFDLElBQUk7Ozs7Z0JBQUcsVUFBUyxDQUFNLElBQUksTUFBTSxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUEsQ0FBQzthQUMzQztZQUNELGFBQWEsQ0FBQyxFQUFFLEVBQUUsSUFBSSxFQUFFLElBQUk7Ozs7WUFBRSxDQUFDLEdBQVEsRUFBRSxFQUFFO2dCQUN6QyxJQUFJLE9BQU8sR0FBRyxLQUFLLFFBQVEsRUFBRTtvQkFDM0IsT0FBTyxJQUFJLENBQUMsSUFBSSxDQUFDLElBQUksS0FBSyxDQUFDLG1CQUFRLEdBQUcsRUFBQSxDQUFDLENBQUMsQ0FBQztpQkFDMUM7cUJBQU07b0JBQ0wsSUFBSSxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQztpQkFDaEI7WUFDSCxDQUFDLEVBQUMsQ0FBQztRQUNMLENBQUMsRUFBQztLQUNIO0lBQ0QsNEVBQTRFO0lBQzVFLDJFQUEyRTtJQUMzRSxnRUFBZ0U7SUFDaEUsd0VBQXdFO0lBQ3hFOzs7O0lBQU87UUFDTCxPQUFPLElBQUksT0FBTzs7Ozs7UUFBTyxDQUFDLGNBQWMsRUFBRSxZQUFZLEVBQUUsRUFBRTtZQUN4RCxhQUFhLENBQUMsRUFBRSxFQUFFLElBQUksRUFBRSxjQUFjLEVBQUUsWUFBWSxDQUFDLENBQUM7UUFDeEQsQ0FBQyxFQUFDLENBQUM7SUFDTCxDQUFDLEVBQUM7QUFDSixDQUFDOzs7Ozs7OztBQUVELFNBQVMsYUFBYSxDQUNsQixFQUFZLEVBQUUsT0FBWSxFQUFFLGNBQXdCLEVBQUUsWUFBc0I7O1VBQ3hFLFdBQVcsR0FBRyxJQUFJLENBQUMsT0FBTzs7VUFDMUIsaUJBQWlCLEdBQUcsQ0FBQyxtQkFBQSxJQUFJLEVBQU8sQ0FBQyxDQUFDLG1CQUFtQixDQUFDO0lBQzVELElBQUksaUJBQWlCLEtBQUssU0FBUyxFQUFFO1FBQ25DLE1BQU0sSUFBSSxLQUFLLENBQ1gsa0ZBQWtGO1lBQ2xGLDRFQUE0RSxDQUFDLENBQUM7S0FDbkY7O1VBQ0ssYUFBYSxHQUFHLG1CQUFBLENBQUMsbUJBQUEsSUFBSSxFQUFPLENBQUMsQ0FBQyxlQUFlLENBQUMsRUFHbkQ7SUFDRCxJQUFJLGFBQWEsS0FBSyxTQUFTLEVBQUU7UUFDL0IsTUFBTSxJQUFJLEtBQUssQ0FDWCw4RUFBOEU7WUFDOUUsdUVBQXVFLENBQUMsQ0FBQztLQUM5RTs7VUFDSyxhQUFhLEdBQUcsYUFBYSxDQUFDLEdBQUcsRUFBRTtJQUN6QyxhQUFhLENBQUMsYUFBYSxFQUFFLENBQUM7Ozs7VUFHeEIsU0FBUyxHQUFHLElBQUksQ0FBQyxPQUFPLENBQUMsV0FBVyxDQUFDLGVBQWUsQ0FBQzs7VUFDckQsZ0JBQWdCLEdBQUcsYUFBYSxDQUFDLFdBQVcsRUFBRTtJQUNwRCxtQkFBQSxtQkFBQSxTQUFTLEVBQUUsQ0FBQyxNQUFNLEVBQUUsQ0FBQyxHQUFHOzs7SUFBQyxHQUFHLEVBQUU7O2NBQ3RCLFlBQVksR0FBYSxJQUFJLGlCQUFpQjs7O1FBQ2hELEdBQUcsRUFBRTtZQUNILHFDQUFxQztZQUNyQyxXQUFXLENBQUMsR0FBRzs7O1lBQUMsR0FBRyxFQUFFO2dCQUNuQixJQUFJLGFBQWEsQ0FBQyxXQUFXLEVBQUUsSUFBSSxZQUFZLEVBQUU7b0JBQy9DLDZFQUE2RTtvQkFDN0UsYUFBYSxDQUFDLFdBQVcsQ0FBQyxnQkFBZ0IsQ0FBQyxDQUFDO2lCQUM3QztnQkFDRCxjQUFjLEVBQUUsQ0FBQztZQUNuQixDQUFDLEVBQUMsQ0FBQztRQUNMLENBQUM7Ozs7UUFDRCxDQUFDLEtBQVUsRUFBRSxFQUFFO1lBQ2IscUNBQXFDO1lBQ3JDLFdBQVcsQ0FBQyxHQUFHOzs7WUFBQyxHQUFHLEVBQUU7Z0JBQ25CLElBQUksYUFBYSxDQUFDLFdBQVcsRUFBRSxJQUFJLFlBQVksRUFBRTtvQkFDL0MsNkVBQTZFO29CQUM3RSxhQUFhLENBQUMsV0FBVyxDQUFDLGdCQUFnQixDQUFDLENBQUM7aUJBQzdDO2dCQUNELFlBQVksQ0FBQyxLQUFLLENBQUMsQ0FBQztZQUN0QixDQUFDLEVBQUMsQ0FBQztRQUNMLENBQUMsR0FDRCxNQUFNLENBQUM7UUFDWCxhQUFhLENBQUMsV0FBVyxDQUFDLFlBQVksQ0FBQyxDQUFDO0lBQzFDLENBQUMsRUFBQyxDQUFDO0lBQ0gsT0FBTyxJQUFJLENBQUMsT0FBTyxDQUFDLFVBQVUsQ0FBQyxFQUFFLEVBQUUsT0FBTyxDQUFDLENBQUM7QUFDOUMsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLyoqXG4gKiBhc3luYyBoYXMgYmVlbiBtb3ZlZCB0byB6b25lLmpzXG4gKiB0aGlzIGZpbGUgaXMgZm9yIGZhbGxiYWNrIGluIGNhc2Ugb2xkIHZlcnNpb24gb2Ygem9uZS5qcyBpcyB1c2VkXG4gKi9cbmRlY2xhcmUgdmFyIGdsb2JhbDogYW55O1xuXG5jb25zdCBfZ2xvYmFsID0gPGFueT4odHlwZW9mIHdpbmRvdyA9PT0gJ3VuZGVmaW5lZCcgPyBnbG9iYWwgOiB3aW5kb3cpO1xuXG4vKipcbiAqIFdyYXBzIGEgdGVzdCBmdW5jdGlvbiBpbiBhbiBhc3luY2hyb25vdXMgdGVzdCB6b25lLiBUaGUgdGVzdCB3aWxsIGF1dG9tYXRpY2FsbHlcbiAqIGNvbXBsZXRlIHdoZW4gYWxsIGFzeW5jaHJvbm91cyBjYWxscyB3aXRoaW4gdGhpcyB6b25lIGFyZSBkb25lLiBDYW4gYmUgdXNlZFxuICogdG8gd3JhcCBhbiB7QGxpbmsgaW5qZWN0fSBjYWxsLlxuICpcbiAqIEV4YW1wbGU6XG4gKlxuICogYGBgXG4gKiBpdCgnLi4uJywgYXN5bmMoaW5qZWN0KFtBQ2xhc3NdLCAob2JqZWN0KSA9PiB7XG4gKiAgIG9iamVjdC5kb1NvbWV0aGluZy50aGVuKCgpID0+IHtcbiAqICAgICBleHBlY3QoLi4uKTtcbiAqICAgfSlcbiAqIH0pO1xuICogYGBgXG4gKlxuICpcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGFzeW5jRmFsbGJhY2soZm46IEZ1bmN0aW9uKTogKGRvbmU6IGFueSkgPT4gYW55IHtcbiAgLy8gSWYgd2UncmUgcnVubmluZyB1c2luZyB0aGUgSmFzbWluZSB0ZXN0IGZyYW1ld29yaywgYWRhcHQgdG8gY2FsbCB0aGUgJ2RvbmUnXG4gIC8vIGZ1bmN0aW9uIHdoZW4gYXN5bmNocm9ub3VzIGFjdGl2aXR5IGlzIGZpbmlzaGVkLlxuICBpZiAoX2dsb2JhbC5qYXNtaW5lKSB7XG4gICAgLy8gTm90IHVzaW5nIGFuIGFycm93IGZ1bmN0aW9uIHRvIHByZXNlcnZlIGNvbnRleHQgcGFzc2VkIGZyb20gY2FsbCBzaXRlXG4gICAgcmV0dXJuIGZ1bmN0aW9uKHRoaXM6IHVua25vd24sIGRvbmU6IGFueSkge1xuICAgICAgaWYgKCFkb25lKSB7XG4gICAgICAgIC8vIGlmIHdlIHJ1biBiZWZvcmVFYWNoIGluIEBhbmd1bGFyL2NvcmUvdGVzdGluZy90ZXN0aW5nX2ludGVybmFsIHRoZW4gd2UgZ2V0IG5vIGRvbmVcbiAgICAgICAgLy8gZmFrZSBpdCBoZXJlIGFuZCBhc3N1bWUgc3luYy5cbiAgICAgICAgZG9uZSA9IGZ1bmN0aW9uKCkge307XG4gICAgICAgIGRvbmUuZmFpbCA9IGZ1bmN0aW9uKGU6IGFueSkgeyB0aHJvdyBlOyB9O1xuICAgICAgfVxuICAgICAgcnVuSW5UZXN0Wm9uZShmbiwgdGhpcywgZG9uZSwgKGVycjogYW55KSA9PiB7XG4gICAgICAgIGlmICh0eXBlb2YgZXJyID09PSAnc3RyaW5nJykge1xuICAgICAgICAgIHJldHVybiBkb25lLmZhaWwobmV3IEVycm9yKDxzdHJpbmc+ZXJyKSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgZG9uZS5mYWlsKGVycik7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH07XG4gIH1cbiAgLy8gT3RoZXJ3aXNlLCByZXR1cm4gYSBwcm9taXNlIHdoaWNoIHdpbGwgcmVzb2x2ZSB3aGVuIGFzeW5jaHJvbm91cyBhY3Rpdml0eVxuICAvLyBpcyBmaW5pc2hlZC4gVGhpcyB3aWxsIGJlIGNvcnJlY3RseSBjb25zdW1lZCBieSB0aGUgTW9jaGEgZnJhbWV3b3JrIHdpdGhcbiAgLy8gaXQoJy4uLicsIGFzeW5jKG15Rm4pKTsgb3IgY2FuIGJlIHVzZWQgaW4gYSBjdXN0b20gZnJhbWV3b3JrLlxuICAvLyBOb3QgdXNpbmcgYW4gYXJyb3cgZnVuY3Rpb24gdG8gcHJlc2VydmUgY29udGV4dCBwYXNzZWQgZnJvbSBjYWxsIHNpdGVcbiAgcmV0dXJuIGZ1bmN0aW9uKHRoaXM6IHVua25vd24pIHtcbiAgICByZXR1cm4gbmV3IFByb21pc2U8dm9pZD4oKGZpbmlzaENhbGxiYWNrLCBmYWlsQ2FsbGJhY2spID0+IHtcbiAgICAgIHJ1bkluVGVzdFpvbmUoZm4sIHRoaXMsIGZpbmlzaENhbGxiYWNrLCBmYWlsQ2FsbGJhY2spO1xuICAgIH0pO1xuICB9O1xufVxuXG5mdW5jdGlvbiBydW5JblRlc3Rab25lKFxuICAgIGZuOiBGdW5jdGlvbiwgY29udGV4dDogYW55LCBmaW5pc2hDYWxsYmFjazogRnVuY3Rpb24sIGZhaWxDYWxsYmFjazogRnVuY3Rpb24pIHtcbiAgY29uc3QgY3VycmVudFpvbmUgPSBab25lLmN1cnJlbnQ7XG4gIGNvbnN0IEFzeW5jVGVzdFpvbmVTcGVjID0gKFpvbmUgYXMgYW55KVsnQXN5bmNUZXN0Wm9uZVNwZWMnXTtcbiAgaWYgKEFzeW5jVGVzdFpvbmVTcGVjID09PSB1bmRlZmluZWQpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICdBc3luY1Rlc3Rab25lU3BlYyBpcyBuZWVkZWQgZm9yIHRoZSBhc3luYygpIHRlc3QgaGVscGVyIGJ1dCBjb3VsZCBub3QgYmUgZm91bmQuICcgK1xuICAgICAgICAnUGxlYXNlIG1ha2Ugc3VyZSB0aGF0IHlvdXIgZW52aXJvbm1lbnQgaW5jbHVkZXMgem9uZS5qcy9kaXN0L2FzeW5jLXRlc3QuanMnKTtcbiAgfVxuICBjb25zdCBQcm94eVpvbmVTcGVjID0gKFpvbmUgYXMgYW55KVsnUHJveHlab25lU3BlYyddIGFzIHtcbiAgICBnZXQoKToge3NldERlbGVnYXRlKHNwZWM6IFpvbmVTcGVjKTogdm9pZDsgZ2V0RGVsZWdhdGUoKTogWm9uZVNwZWM7fTtcbiAgICBhc3NlcnRQcmVzZW50OiAoKSA9PiB2b2lkO1xuICB9O1xuICBpZiAoUHJveHlab25lU3BlYyA9PT0gdW5kZWZpbmVkKSB7XG4gICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAnUHJveHlab25lU3BlYyBpcyBuZWVkZWQgZm9yIHRoZSBhc3luYygpIHRlc3QgaGVscGVyIGJ1dCBjb3VsZCBub3QgYmUgZm91bmQuICcgK1xuICAgICAgICAnUGxlYXNlIG1ha2Ugc3VyZSB0aGF0IHlvdXIgZW52aXJvbm1lbnQgaW5jbHVkZXMgem9uZS5qcy9kaXN0L3Byb3h5LmpzJyk7XG4gIH1cbiAgY29uc3QgcHJveHlab25lU3BlYyA9IFByb3h5Wm9uZVNwZWMuZ2V0KCk7XG4gIFByb3h5Wm9uZVNwZWMuYXNzZXJ0UHJlc2VudCgpO1xuICAvLyBXZSBuZWVkIHRvIGNyZWF0ZSB0aGUgQXN5bmNUZXN0Wm9uZVNwZWMgb3V0c2lkZSB0aGUgUHJveHlab25lLlxuICAvLyBJZiB3ZSBkbyBpdCBpbiBQcm94eVpvbmUgdGhlbiB3ZSB3aWxsIGdldCB0byBpbmZpbml0ZSByZWN1cnNpb24uXG4gIGNvbnN0IHByb3h5Wm9uZSA9IFpvbmUuY3VycmVudC5nZXRab25lV2l0aCgnUHJveHlab25lU3BlYycpO1xuICBjb25zdCBwcmV2aW91c0RlbGVnYXRlID0gcHJveHlab25lU3BlYy5nZXREZWxlZ2F0ZSgpO1xuICBwcm94eVpvbmUgIS5wYXJlbnQgIS5ydW4oKCkgPT4ge1xuICAgIGNvbnN0IHRlc3Rab25lU3BlYzogWm9uZVNwZWMgPSBuZXcgQXN5bmNUZXN0Wm9uZVNwZWMoXG4gICAgICAgICgpID0+IHtcbiAgICAgICAgICAvLyBOZWVkIHRvIHJlc3RvcmUgdGhlIG9yaWdpbmFsIHpvbmUuXG4gICAgICAgICAgY3VycmVudFpvbmUucnVuKCgpID0+IHtcbiAgICAgICAgICAgIGlmIChwcm94eVpvbmVTcGVjLmdldERlbGVnYXRlKCkgPT0gdGVzdFpvbmVTcGVjKSB7XG4gICAgICAgICAgICAgIC8vIE9ubHkgcmVzZXQgdGhlIHpvbmUgc3BlYyBpZiBpdCdzIHNpbGwgdGhpcyBvbmUuIE90aGVyd2lzZSwgYXNzdW1lIGl0J3MgT0suXG4gICAgICAgICAgICAgIHByb3h5Wm9uZVNwZWMuc2V0RGVsZWdhdGUocHJldmlvdXNEZWxlZ2F0ZSk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBmaW5pc2hDYWxsYmFjaygpO1xuICAgICAgICAgIH0pO1xuICAgICAgICB9LFxuICAgICAgICAoZXJyb3I6IGFueSkgPT4ge1xuICAgICAgICAgIC8vIE5lZWQgdG8gcmVzdG9yZSB0aGUgb3JpZ2luYWwgem9uZS5cbiAgICAgICAgICBjdXJyZW50Wm9uZS5ydW4oKCkgPT4ge1xuICAgICAgICAgICAgaWYgKHByb3h5Wm9uZVNwZWMuZ2V0RGVsZWdhdGUoKSA9PSB0ZXN0Wm9uZVNwZWMpIHtcbiAgICAgICAgICAgICAgLy8gT25seSByZXNldCB0aGUgem9uZSBzcGVjIGlmIGl0J3Mgc2lsbCB0aGlzIG9uZS4gT3RoZXJ3aXNlLCBhc3N1bWUgaXQncyBPSy5cbiAgICAgICAgICAgICAgcHJveHlab25lU3BlYy5zZXREZWxlZ2F0ZShwcmV2aW91c0RlbGVnYXRlKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGZhaWxDYWxsYmFjayhlcnJvcik7XG4gICAgICAgICAgfSk7XG4gICAgICAgIH0sXG4gICAgICAgICd0ZXN0Jyk7XG4gICAgcHJveHlab25lU3BlYy5zZXREZWxlZ2F0ZSh0ZXN0Wm9uZVNwZWMpO1xuICB9KTtcbiAgcmV0dXJuIFpvbmUuY3VycmVudC5ydW5HdWFyZGVkKGZuLCBjb250ZXh0KTtcbn1cbiJdfQ==
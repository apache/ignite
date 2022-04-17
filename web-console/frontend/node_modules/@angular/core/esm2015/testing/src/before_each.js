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
 * Public Test Library for unit testing Angular applications. Assumes that you are running
 * with Jasmine, Mocha, or a similar framework which exports a beforeEach function and
 * allows tests to be asynchronous by either returning a promise or using a 'done' parameter.
 */
import { resetFakeAsyncZone } from './fake_async';
import { TestBed } from './test_bed';
/** @type {?} */
const _global = (/** @type {?} */ ((typeof window === 'undefined' ? global : window)));
// Reset the test providers and the fake async zone before each test.
if (_global.beforeEach) {
    _global.beforeEach((/**
     * @return {?}
     */
    () => {
        TestBed.resetTestingModule();
        resetFakeAsyncZone();
    }));
}
// TODO(juliemr): remove this, only used because we need to export something to have compilation
// work.
/** @type {?} */
export const __core_private_testing_placeholder__ = '';
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYmVmb3JlX2VhY2guanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3Rlc3Rpbmcvc3JjL2JlZm9yZV9lYWNoLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7Ozs7Ozs7QUFjQSxPQUFPLEVBQUMsa0JBQWtCLEVBQUMsTUFBTSxjQUFjLENBQUM7QUFDaEQsT0FBTyxFQUFDLE9BQU8sRUFBQyxNQUFNLFlBQVksQ0FBQzs7TUFJN0IsT0FBTyxHQUFHLG1CQUFLLENBQUMsT0FBTyxNQUFNLEtBQUssV0FBVyxDQUFDLENBQUMsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLE1BQU0sQ0FBQyxFQUFBOztBQUd0RSxJQUFJLE9BQU8sQ0FBQyxVQUFVLEVBQUU7SUFDdEIsT0FBTyxDQUFDLFVBQVU7OztJQUFDLEdBQUcsRUFBRTtRQUN0QixPQUFPLENBQUMsa0JBQWtCLEVBQUUsQ0FBQztRQUM3QixrQkFBa0IsRUFBRSxDQUFDO0lBQ3ZCLENBQUMsRUFBQyxDQUFDO0NBQ0o7Ozs7QUFJRCxNQUFNLE9BQU8sb0NBQW9DLEdBQUcsRUFBRSIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLyoqXG4gKiBQdWJsaWMgVGVzdCBMaWJyYXJ5IGZvciB1bml0IHRlc3RpbmcgQW5ndWxhciBhcHBsaWNhdGlvbnMuIEFzc3VtZXMgdGhhdCB5b3UgYXJlIHJ1bm5pbmdcbiAqIHdpdGggSmFzbWluZSwgTW9jaGEsIG9yIGEgc2ltaWxhciBmcmFtZXdvcmsgd2hpY2ggZXhwb3J0cyBhIGJlZm9yZUVhY2ggZnVuY3Rpb24gYW5kXG4gKiBhbGxvd3MgdGVzdHMgdG8gYmUgYXN5bmNocm9ub3VzIGJ5IGVpdGhlciByZXR1cm5pbmcgYSBwcm9taXNlIG9yIHVzaW5nIGEgJ2RvbmUnIHBhcmFtZXRlci5cbiAqL1xuXG5pbXBvcnQge3Jlc2V0RmFrZUFzeW5jWm9uZX0gZnJvbSAnLi9mYWtlX2FzeW5jJztcbmltcG9ydCB7VGVzdEJlZH0gZnJvbSAnLi90ZXN0X2JlZCc7XG5cbmRlY2xhcmUgdmFyIGdsb2JhbDogYW55O1xuXG5jb25zdCBfZ2xvYmFsID0gPGFueT4odHlwZW9mIHdpbmRvdyA9PT0gJ3VuZGVmaW5lZCcgPyBnbG9iYWwgOiB3aW5kb3cpO1xuXG4vLyBSZXNldCB0aGUgdGVzdCBwcm92aWRlcnMgYW5kIHRoZSBmYWtlIGFzeW5jIHpvbmUgYmVmb3JlIGVhY2ggdGVzdC5cbmlmIChfZ2xvYmFsLmJlZm9yZUVhY2gpIHtcbiAgX2dsb2JhbC5iZWZvcmVFYWNoKCgpID0+IHtcbiAgICBUZXN0QmVkLnJlc2V0VGVzdGluZ01vZHVsZSgpO1xuICAgIHJlc2V0RmFrZUFzeW5jWm9uZSgpO1xuICB9KTtcbn1cblxuLy8gVE9ETyhqdWxpZW1yKTogcmVtb3ZlIHRoaXMsIG9ubHkgdXNlZCBiZWNhdXNlIHdlIG5lZWQgdG8gZXhwb3J0IHNvbWV0aGluZyB0byBoYXZlIGNvbXBpbGF0aW9uXG4vLyB3b3JrLlxuZXhwb3J0IGNvbnN0IF9fY29yZV9wcml2YXRlX3Rlc3RpbmdfcGxhY2Vob2xkZXJfXyA9ICcnO1xuIl19
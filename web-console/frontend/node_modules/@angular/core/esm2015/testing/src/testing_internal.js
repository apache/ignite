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
import { ɵisPromise as isPromise } from '@angular/core';
import { global } from '@angular/core/src/util/global';
import { AsyncTestCompleter } from './async_test_completer';
import { getTestBed } from './test_bed';
export { AsyncTestCompleter } from './async_test_completer';
export { inject } from './test_bed';
export { Log } from './logger';
export { MockNgZone } from './ng_zone_mock';
/** @type {?} */
export const proxy = (/**
 * @param {?} t
 * @return {?}
 */
(t) => t);
/** @type {?} */
const _global = (/** @type {?} */ ((typeof window === 'undefined' ? global : window)));
/** @type {?} */
export const afterEach = _global.afterEach;
/** @type {?} */
export const expect = _global.expect;
/** @type {?} */
const jsmBeforeEach = _global.beforeEach;
/** @type {?} */
const jsmDescribe = _global.describe;
/** @type {?} */
const jsmDDescribe = _global.fdescribe;
/** @type {?} */
const jsmXDescribe = _global.xdescribe;
/** @type {?} */
const jsmIt = _global.it;
/** @type {?} */
const jsmFIt = _global.fit;
/** @type {?} */
const jsmXIt = _global.xit;
/** @type {?} */
const runnerStack = [];
jasmine.DEFAULT_TIMEOUT_INTERVAL = 3000;
/** @type {?} */
const globalTimeOut = jasmine.DEFAULT_TIMEOUT_INTERVAL;
/** @type {?} */
const testBed = getTestBed();
/**
 * Mechanism to run `beforeEach()` functions of Angular tests.
 *
 * Note: Jasmine own `beforeEach` is used by this library to handle DI providers.
 */
class BeforeEachRunner {
    /**
     * @param {?} _parent
     */
    constructor(_parent) {
        this._parent = _parent;
        this._fns = [];
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    beforeEach(fn) { this._fns.push(fn); }
    /**
     * @return {?}
     */
    run() {
        if (this._parent)
            this._parent.run();
        this._fns.forEach((/**
         * @param {?} fn
         * @return {?}
         */
        (fn) => { fn(); }));
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    BeforeEachRunner.prototype._fns;
    /**
     * @type {?}
     * @private
     */
    BeforeEachRunner.prototype._parent;
}
// Reset the test providers before each test
jsmBeforeEach((/**
 * @return {?}
 */
() => { testBed.resetTestingModule(); }));
/**
 * @param {?} jsmFn
 * @param {...?} args
 * @return {?}
 */
function _describe(jsmFn, ...args) {
    /** @type {?} */
    const parentRunner = runnerStack.length === 0 ? null : runnerStack[runnerStack.length - 1];
    /** @type {?} */
    const runner = new BeforeEachRunner((/** @type {?} */ (parentRunner)));
    runnerStack.push(runner);
    /** @type {?} */
    const suite = jsmFn(...args);
    runnerStack.pop();
    return suite;
}
/**
 * @param {...?} args
 * @return {?}
 */
export function describe(...args) {
    return _describe(jsmDescribe, ...args);
}
/**
 * @param {...?} args
 * @return {?}
 */
export function ddescribe(...args) {
    return _describe(jsmDDescribe, ...args);
}
/**
 * @param {...?} args
 * @return {?}
 */
export function xdescribe(...args) {
    return _describe(jsmXDescribe, ...args);
}
/**
 * @param {?} fn
 * @return {?}
 */
export function beforeEach(fn) {
    if (runnerStack.length > 0) {
        // Inside a describe block, beforeEach() uses a BeforeEachRunner
        runnerStack[runnerStack.length - 1].beforeEach(fn);
    }
    else {
        // Top level beforeEach() are delegated to jasmine
        jsmBeforeEach(fn);
    }
}
/**
 * Allows overriding default providers defined in test_injector.js.
 *
 * The given function must return a list of DI providers.
 *
 * Example:
 *
 *   beforeEachProviders(() => [
 *     {provide: Compiler, useClass: MockCompiler},
 *     {provide: SomeToken, useValue: myValue},
 *   ]);
 * @param {?} fn
 * @return {?}
 */
export function beforeEachProviders(fn) {
    jsmBeforeEach((/**
     * @return {?}
     */
    () => {
        /** @type {?} */
        const providers = fn();
        if (!providers)
            return;
        testBed.configureTestingModule({ providers: providers });
    }));
}
/**
 * @param {?} jsmFn
 * @param {?} testName
 * @param {?} testFn
 * @param {?=} testTimeout
 * @return {?}
 */
function _it(jsmFn, testName, testFn, testTimeout = 0) {
    if (runnerStack.length == 0) {
        // This left here intentionally, as we should never get here, and it aids debugging.
        // tslint:disable-next-line
        debugger;
        throw new Error('Empty Stack!');
    }
    /** @type {?} */
    const runner = runnerStack[runnerStack.length - 1];
    /** @type {?} */
    const timeout = Math.max(globalTimeOut, testTimeout);
    jsmFn(testName, (/**
     * @param {?} done
     * @return {?}
     */
    (done) => {
        /** @type {?} */
        const completerProvider = {
            provide: AsyncTestCompleter,
            useFactory: (/**
             * @return {?}
             */
            () => {
                // Mark the test as async when an AsyncTestCompleter is injected in an it()
                return new AsyncTestCompleter();
            })
        };
        testBed.configureTestingModule({ providers: [completerProvider] });
        runner.run();
        if (testFn.length === 0) {
            // TypeScript doesn't infer the TestFn type without parameters here, so we
            // need to manually cast it.
            /** @type {?} */
            const retVal = ((/** @type {?} */ (testFn)))();
            if (isPromise(retVal)) {
                // Asynchronous test function that returns a Promise - wait for completion.
                retVal.then(done, done.fail);
            }
            else {
                // Synchronous test function - complete immediately.
                done();
            }
        }
        else {
            // Asynchronous test function that takes in 'done' parameter.
            testFn(done);
        }
    }), timeout);
}
/**
 * @param {?} expectation
 * @param {?} assertion
 * @param {?=} timeout
 * @return {?}
 */
export function it(expectation, assertion, timeout) {
    return _it(jsmIt, expectation, assertion, timeout);
}
/**
 * @param {?} expectation
 * @param {?} assertion
 * @param {?=} timeout
 * @return {?}
 */
export function fit(expectation, assertion, timeout) {
    return _it(jsmFIt, expectation, assertion, timeout);
}
/**
 * @param {?} expectation
 * @param {?} assertion
 * @param {?=} timeout
 * @return {?}
 */
export function xit(expectation, assertion, timeout) {
    return _it(jsmXIt, expectation, assertion, timeout);
}
export class SpyObject {
    /**
     * @param {?=} type
     */
    constructor(type) {
        if (type) {
            for (const prop in type.prototype) {
                /** @type {?} */
                let m = null;
                try {
                    m = type.prototype[prop];
                }
                catch (_a) {
                    // As we are creating spys for abstract classes,
                    // these classes might have getters that throw when they are accessed.
                    // As we are only auto creating spys for methods, this
                    // should not matter.
                }
                if (typeof m === 'function') {
                    this.spy(prop);
                }
            }
        }
    }
    /**
     * @param {?} name
     * @return {?}
     */
    spy(name) {
        if (!((/** @type {?} */ (this)))[name]) {
            ((/** @type {?} */ (this)))[name] = jasmine.createSpy(name);
        }
        return ((/** @type {?} */ (this)))[name];
    }
    /**
     * @param {?} name
     * @param {?} value
     * @return {?}
     */
    prop(name, value) { ((/** @type {?} */ (this)))[name] = value; }
    /**
     * @param {?=} object
     * @param {?=} config
     * @param {?=} overrides
     * @return {?}
     */
    static stub(object = null, config = null, overrides = null) {
        if (!(object instanceof SpyObject)) {
            overrides = config;
            config = object;
            object = new SpyObject();
        }
        /** @type {?} */
        const m = Object.assign({}, config, overrides);
        Object.keys(m).forEach((/**
         * @param {?} key
         * @return {?}
         */
        key => { object.spy(key).and.returnValue(m[key]); }));
        return object;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGVzdGluZ19pbnRlcm5hbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvdGVzdGluZy9zcmMvdGVzdGluZ19pbnRlcm5hbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxVQUFVLElBQUksU0FBUyxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBQ3RELE9BQU8sRUFBQyxNQUFNLEVBQUMsTUFBTSwrQkFBK0IsQ0FBQztBQUVyRCxPQUFPLEVBQUMsa0JBQWtCLEVBQUMsTUFBTSx3QkFBd0IsQ0FBQztBQUMxRCxPQUFPLEVBQUMsVUFBVSxFQUFTLE1BQU0sWUFBWSxDQUFDO0FBRTlDLE9BQU8sRUFBQyxrQkFBa0IsRUFBQyxNQUFNLHdCQUF3QixDQUFDO0FBQzFELE9BQU8sRUFBQyxNQUFNLEVBQUMsTUFBTSxZQUFZLENBQUM7QUFFbEMsb0JBQWMsVUFBVSxDQUFDO0FBQ3pCLDJCQUFjLGdCQUFnQixDQUFDOztBQUUvQixNQUFNLE9BQU8sS0FBSzs7OztBQUFtQixDQUFDLENBQU0sRUFBRSxFQUFFLENBQUMsQ0FBQyxDQUFBOztNQUU1QyxPQUFPLEdBQUcsbUJBQUssQ0FBQyxPQUFPLE1BQU0sS0FBSyxXQUFXLENBQUMsQ0FBQyxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsTUFBTSxDQUFDLEVBQUE7O0FBRXRFLE1BQU0sT0FBTyxTQUFTLEdBQWEsT0FBTyxDQUFDLFNBQVM7O0FBQ3BELE1BQU0sT0FBTyxNQUFNLEdBQTBDLE9BQU8sQ0FBQyxNQUFNOztNQUVyRSxhQUFhLEdBQUcsT0FBTyxDQUFDLFVBQVU7O01BQ2xDLFdBQVcsR0FBRyxPQUFPLENBQUMsUUFBUTs7TUFDOUIsWUFBWSxHQUFHLE9BQU8sQ0FBQyxTQUFTOztNQUNoQyxZQUFZLEdBQUcsT0FBTyxDQUFDLFNBQVM7O01BQ2hDLEtBQUssR0FBRyxPQUFPLENBQUMsRUFBRTs7TUFDbEIs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LElBQUksRUFBTyxDQUFDLENBQUMsSUFBSSxDQUFDLEVBQUU7WUFDeEIsQ0FBQyxtQkFBQSxJQUFJLEVBQU8sQ0FBQyxDQUFDLElBQUksQ0FBQyxHQUFHLE9BQU8sQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFDLENBQUM7U0FDL0M7UUFDRCxPQUFPLENBQUMsbUJBQUEsSUFBSSxFQUFPLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQztJQUM3QixDQUFDOzs7Ozs7SUFFRCxJQUFJLENBQUMsSUFBWSxFQUFFLEtBQVUsSUFBSSxDQUFDLG1CQUFBLElBQUksRUFBTyxDQUFDLENBQUMsSUFBSSxDQUFDLEdBQUcsS0FBSyxDQUFDLENBQUMsQ0FBQzs7Ozs7OztJQUUvRCxNQUFNLENBQUMsSUFBSSxDQUFDLFNBQWMsSUFBSSxFQUFFLFNBQWMsSUFBSSxFQUFFLFlBQWlCLElBQUk7UUFDdkUsSUFBSSxDQUFDLENBQUMsTUFBTSxZQUFZLFNBQVMsQ0FBQyxFQUFFO1lBQ2xDLFNBQVMsR0FBRyxNQUFNLENBQUM7WUFDbkIsTUFBTSxHQUFHLE1BQU0sQ0FBQztZQUNoQixNQUFNLEdBQUcsSUFBSSxTQUFTLEVBQUUsQ0FBQztTQUMxQjs7Y0FFSyxDQUFDLHFCQUFPLE1BQU0sRUFBSyxTQUFTLENBQUM7UUFDbkMsTUFBTSxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxPQUFPOzs7O1FBQUMsR0FBRyxDQUFDLEVBQUUsR0FBRyxNQUFNLENBQUMsR0FBRyxDQUFDLEdBQUcsQ0FBQyxDQUFDLEdBQUcsQ0FBQyxXQUFXLENBQUMsQ0FBQyxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLEVBQUMsQ0FBQztRQUM1RSxPQUFPLE1BQU0sQ0FBQztJQUNoQixDQUFDO0NBQ0YiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7ybVpc1Byb21pc2UgYXMgaXNQcm9taXNlfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7Z2xvYmFsfSBmcm9tICdAYW5ndWxhci9jb3JlL3NyYy91dGlsL2dsb2JhbCc7XG5cbmltcG9ydCB7QXN5bmNUZXN0Q29tcGxldGVyfSBmcm9tICcuL2FzeW5jX3Rlc3RfY29tcGxldGVyJztcbmltcG9ydCB7Z2V0VGVzdEJlZCwgaW5qZWN0fSBmcm9tICcuL3Rlc3RfYmVkJztcblxuZXhwb3J0IHtBc3luY1Rlc3RDb21wbGV0ZXJ9IGZyb20gJy4vYXN5bmNfdGVzdF9jb21wbGV0ZXInO1xuZXhwb3J0IHtpbmplY3R9IGZyb20gJy4vdGVzdF9iZWQnO1xuXG5leHBvcnQgKiBmcm9tICcuL2xvZ2dlcic7XG5leHBvcnQgKiBmcm9tICcuL25nX3pvbmVfbW9jayc7XG5cbmV4cG9ydCBjb25zdCBwcm94eTogQ2xhc3NEZWNvcmF0b3IgPSAodDogYW55KSA9PiB0O1xuXG5jb25zdCBfZ2xvYmFsID0gPGFueT4odHlwZW9mIHdpbmRvdyA9PT0gJ3VuZGVmaW5lZCcgPyBnbG9iYWwgOiB3aW5kb3cpO1xuXG5leHBvcnQgY29uc3QgYWZ0ZXJFYWNoOiBGdW5jdGlvbiA9IF9nbG9iYWwuYWZ0ZXJFYWNoO1xuZXhwb3J0IGNvbnN0IGV4cGVjdDogPFQ+KGFjdHVhbDogVCkgPT4gamFzbWluZS5NYXRjaGVyczxUPiA9IF9nbG9iYWwuZXhwZWN0O1xuXG5jb25zdCBqc21CZWZvcmVFYWNoID0gX2dsb2JhbC5iZWZvcmVFYWNoO1xuY29uc3QganNtRGVzY3JpYmUgPSBfZ2xvYmFsLmRlc2NyaWJlO1xuY29uc3QganNtRERlc2NyaWJlID0gX2dsb2JhbC5mZGVzY3JpYmU7XG5jb25zdCBqc21YRGVzY3JpYmUgPSBfZ2xvYmFsLnhkZXNjcmliZTtcbmNvbnN0IGpzbUl0ID0gX2dsb2JhbC5pdDtcbmNvbnN0IGpzbUZJdCA9IF9nbG9iYWwuZml0O1xuY29uc3QganNtWEl0ID0gX2dsb2JhbC54aXQ7XG5cbmNvbnN0IHJ1bm5lclN0YWNrOiBCZWZvcmVFYWNoUnVubmVyW10gPSBbXTtcbmphc21pbmUuREVGQVVMVF9USU1FT1VUX0lOVEVSVkFMID0gMzAwMDtcbmNvbnN0IGdsb2JhbFRpbWVPdXQgPSBqYXNtaW5lLkRFRkFVTFRfVElNRU9VVF9JTlRFUlZBTDtcblxuY29uc3QgdGVzdEJlZCA9IGdldFRlc3RCZWQoKTtcblxuZXhwb3J0IHR5cGUgVGVzdEZuID0gKChkb25lOiBEb25lRm4pID0+IGFueSkgfCAoKCkgPT4gYW55KTtcblxuLyoqXG4gKiBNZWNoYW5pc20gdG8gcnVuIGBiZWZvcmVFYWNoKClgIGZ1bmN0aW9ucyBvZiBBbmd1bGFyIHRlc3RzLlxuICpcbiAqIE5vdGU6IEphc21pbmUgb3duIGBiZWZvcmVFYWNoYCBpcyB1c2VkIGJ5IHRoaXMgbGlicmFyeSB0byBoYW5kbGUgREkgcHJvdmlkZXJzLlxuICovXG5jbGFzcyBCZWZvcmVFYWNoUnVubmVyIHtcbiAgcHJpdmF0ZSBfZm5zOiBBcnJheTxGdW5jdGlvbj4gPSBbXTtcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIF9wYXJlbnQ6IEJlZm9yZUVhY2hSdW5uZXIpIHt9XG5cbiAgYmVmb3JlRWFjaChmbjogRnVuY3Rpb24pOiB2b2lkIHsgdGhpcy5fZm5zLnB1c2goZm4pOyB9XG5cbiAgcnVuKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLl9wYXJlbnQpIHRoaXMuX3BhcmVudC5ydW4oKTtcbiAgICB0aGlzLl9mbnMuZm9yRWFjaCgoZm4pID0+IHsgZm4oKTsgfSk7XG4gIH1cbn1cblxuLy8gUmVzZXQgdGhlIHRlc3QgcHJvdmlkZXJzIGJlZm9yZSBlYWNoIHRlc3RcbmpzbUJlZm9yZUVhY2goKCkgPT4geyB0ZXN0QmVkLnJlc2V0VGVzdGluZ01vZHVsZSgpOyB9KTtcblxuZnVuY3Rpb24gX2Rlc2NyaWJlKGpzbUZuOiBGdW5jdGlvbiwgLi4uYXJnczogYW55W10pIHtcbiAgY29uc3QgcGFyZW50UnVubmVyID0gcnVubmVyU3RhY2subGVuZ3RoID09PSAwID8gbnVsbCA6IHJ1bm5lclN0YWNrW3J1bm5lclN0YWNrLmxlbmd0aCAtIDFdO1xuICBjb25zdCBydW5uZXIgPSBuZXcgQmVmb3JlRWFjaFJ1bm5lcihwYXJlbnRSdW5uZXIgISk7XG4gIHJ1bm5lclN0YWNrLnB1c2gocnVubmVyKTtcbiAgY29uc3Qgc3VpdGUgPSBqc21GbiguLi5hcmdzKTtcbiAgcnVubmVyU3RhY2sucG9wKCk7XG4gIHJldHVybiBzdWl0ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRlc2NyaWJlKC4uLmFyZ3M6IGFueVtdKTogdm9pZCB7XG4gIHJldHVybiBfZGVzY3JpYmUoanNtRGVzY3JpYmUsIC4uLmFyZ3MpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZGRlc2NyaWJlKC4uLmFyZ3M6IGFueVtdKTogdm9pZCB7XG4gIHJldHVybiBfZGVzY3JpYmUoanNtRERlc2NyaWJlLCAuLi5hcmdzKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHhkZXNjcmliZSguLi5hcmdzOiBhbnlbXSk6IHZvaWQge1xuICByZXR1cm4gX2Rlc2NyaWJlKGpzbVhEZXNjcmliZSwgLi4uYXJncyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBiZWZvcmVFYWNoKGZuOiBGdW5jdGlvbik6IHZvaWQge1xuICBpZiAocnVubmVyU3RhY2subGVuZ3RoID4gMCkge1xuICAgIC8vIEluc2lkZSBhIGRlc2NyaWJlIGJsb2NrLCBiZWZvcmVFYWNoKCkgdXNlcyBhIEJlZm9yZUVhY2hSdW5uZXJcbiAgICBydW5uZXJTdGFja1tydW5uZXJTdGFjay5sZW5ndGggLSAxXS5iZWZvcmVFYWNoKGZuKTtcbiAgfSBlbHNlIHtcbiAgICAvLyBUb3AgbGV2ZWwgYmVmb3JlRWFjaCgpIGFyZSBkZWxlZ2F0ZWQgdG8gamFzbWluZVxuICAgIGpzbUJlZm9yZUVhY2goZm4pO1xuICB9XG59XG5cbi8qKlxuICogQWxsb3dzIG92ZXJyaWRpbmcgZGVmYXVsdCBwcm92aWRlcnMgZGVmaW5lZCBpbiB0ZXN0X2luamVjdG9yLmpzLlxuICpcbiAqIFRoZSBnaXZlbiBmdW5jdGlvbiBtdXN0IHJldHVybiBhIGxpc3Qgb2YgREkgcHJvdmlkZXJzLlxuICpcbiAqIEV4YW1wbGU6XG4gKlxuICogICBiZWZvcmVFYWNoUHJvdmlkZXJzKCgpID0+IFtcbiAqICAgICB7cHJvdmlkZTogQ29tcGlsZXIsIHVzZUNsYXNzOiBNb2NrQ29tcGlsZXJ9LFxuICogICAgIHtwcm92aWRlOiBTb21lVG9rZW4sIHVzZVZhbHVlOiBteVZhbHVlfSxcbiAqICAgXSk7XG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBiZWZvcmVFYWNoUHJvdmlkZXJzKGZuOiBGdW5jdGlvbik6IHZvaWQge1xuICBqc21CZWZvcmVFYWNoKCgpID0+IHtcbiAgICBjb25zdCBwcm92aWRlcnMgPSBmbigpO1xuICAgIGlmICghcHJvdmlkZXJzKSByZXR1cm47XG4gICAgdGVzdEJlZC5jb25maWd1cmVUZXN0aW5nTW9kdWxlKHtwcm92aWRlcnM6IHByb3ZpZGVyc30pO1xuICB9KTtcbn1cblxuXG5mdW5jdGlvbiBfaXQoanNtRm46IEZ1bmN0aW9uLCB0ZXN0TmFtZTogc3RyaW5nLCB0ZXN0Rm46IFRlc3RGbiwgdGVzdFRpbWVvdXQgPSAwKTogdm9pZCB7XG4gIGlmIChydW5uZXJTdGFjay5sZW5ndGggPT0gMCkge1xuICAgIC8vIFRoaXMgbGVmdCBoZXJlIGludGVudGlvbmFsbHksIGFzIHdlIHNob3VsZCBuZXZlciBnZXQgaGVyZSwgYW5kIGl0IGFpZHMgZGVidWdnaW5nLlxuICAgIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZVxuICAgIGRlYnVnZ2VyO1xuICAgIHRocm93IG5ldyBFcnJvcignRW1wdHkgU3RhY2shJyk7XG4gIH1cbiAgY29uc3QgcnVubmVyID0gcnVubmVyU3RhY2tbcnVubmVyU3RhY2subGVuZ3RoIC0gMV07XG4gIGNvbnN0IHRpbWVvdXQgPSBNYXRoLm1heChnbG9iYWxUaW1lT3V0LCB0ZXN0VGltZW91dCk7XG5cbiAganNtRm4odGVzdE5hbWUsIChkb25lOiBEb25lRm4pID0+IHtcbiAgICBjb25zdCBjb21wbGV0ZXJQcm92aWRlciA9IHtcbiAgICAgIHByb3ZpZGU6IEFzeW5jVGVzdENvbXBsZXRlcixcbiAgICAgIHVzZUZhY3Rvcnk6ICgpID0+IHtcbiAgICAgICAgLy8gTWFyayB0aGUgdGVzdCBhcyBhc3luYyB3aGVuIGFuIEFzeW5jVGVzdENvbXBsZXRlciBpcyBpbmplY3RlZCBpbiBhbiBpdCgpXG4gICAgICAgIHJldHVybiBuZXcgQXN5bmNUZXN0Q29tcGxldGVyKCk7XG4gICAgICB9XG4gICAgfTtcbiAgICB0ZXN0QmVkLmNvbmZpZ3VyZVRlc3RpbmdNb2R1bGUoe3Byb3ZpZGVyczogW2NvbXBsZXRlclByb3ZpZGVyXX0pO1xuICAgIHJ1bm5lci5ydW4oKTtcblxuICAgIGlmICh0ZXN0Rm4ubGVuZ3RoID09PSAwKSB7XG4gICAgICAvLyBUeXBlU2NyaXB0IGRvZXNuJ3QgaW5mZXIgdGhlIFRlc3RGbiB0eXBlIHdpdGhvdXQgcGFyYW1ldGVycyBoZXJlLCBzbyB3ZVxuICAgICAgLy8gbmVlZCB0byBtYW51YWxseSBjYXN0IGl0LlxuICAgICAgY29uc3QgcmV0VmFsID0gKHRlc3RGbiBhcygpID0+IGFueSkoKTtcbiAgICAgIGlmIChpc1Byb21pc2UocmV0VmFsKSkge1xuICAgICAgICAvLyBBc3luY2hyb25vdXMgdGVzdCBmdW5jdGlvbiB0aGF0IHJldHVybnMgYSBQcm9taXNlIC0gd2FpdCBmb3IgY29tcGxldGlvbi5cbiAgICAgICAgcmV0VmFsLnRoZW4oZG9uZSwgZG9uZS5mYWlsKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIC8vIFN5bmNocm9ub3VzIHRlc3QgZnVuY3Rpb24gLSBjb21wbGV0ZSBpbW1lZGlhdGVseS5cbiAgICAgICAgZG9uZSgpO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICAvLyBBc3luY2hyb25vdXMgdGVzdCBmdW5jdGlvbiB0aGF0IHRha2VzIGluICdkb25lJyBwYXJhbWV0ZXIuXG4gICAgICB0ZXN0Rm4oZG9uZSk7XG4gICAgfVxuICB9LCB0aW1lb3V0KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGl0KGV4cGVjdGF0aW9uOiBzdHJpbmcsIGFzc2VydGlvbjogVGVzdEZuLCB0aW1lb3V0PzogbnVtYmVyKTogdm9pZCB7XG4gIHJldHVybiBfaXQoanNtSXQsIGV4cGVjdGF0aW9uLCBhc3NlcnRpb24sIHRpbWVvdXQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZml0KGV4cGVjdGF0aW9uOiBzdHJpbmcsIGFzc2VydGlvbjogVGVzdEZuLCB0aW1lb3V0PzogbnVtYmVyKTogdm9pZCB7XG4gIHJldHVybiBfaXQoanNtRkl0LCBleHBlY3RhdGlvbiwgYXNzZXJ0aW9uLCB0aW1lb3V0KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHhpdChleHBlY3RhdGlvbjogc3RyaW5nLCBhc3NlcnRpb246IFRlc3RGbiwgdGltZW91dD86IG51bWJlcik6IHZvaWQge1xuICByZXR1cm4gX2l0KGpzbVhJdCwgZXhwZWN0YXRpb24sIGFzc2VydGlvbiwgdGltZW91dCk7XG59XG5cbmV4cG9ydCBjbGFzcyBTcHlPYmplY3Qge1xuICBjb25zdHJ1Y3Rvcih0eXBlPzogYW55KSB7XG4gICAgaWYgKHR5cGUpIHtcbiAgICAgIGZvciAoY29uc3QgcHJvcCBpbiB0eXBlLnByb3RvdHlwZSkge1xuICAgICAgICBsZXQgbTogYW55ID0gbnVsbDtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICBtID0gdHlwZS5wcm90b3R5cGVbcHJvcF07XG4gICAgICAgIH0gY2F0Y2gge1xuICAgICAgICAgIC8vIEFzIHdlIGFyZSBjcmVhdGluZyBzcHlzIGZvciBhYnN0cmFjdCBjbGFzc2VzLFxuICAgICAgICAgIC8vIHRoZXNlIGNsYXNzZXMgbWlnaHQgaGF2ZSBnZXR0ZXJzIHRoYXQgdGhyb3cgd2hlbiB0aGV5IGFyZSBhY2Nlc3NlZC5cbiAgICAgICAgICAvLyBBcyB3ZSBhcmUgb25seSBhdXRvIGNyZWF0aW5nIHNweXMgZm9yIG1ldGhvZHMsIHRoaXNcbiAgICAgICAgICAvLyBzaG91bGQgbm90IG1hdHRlci5cbiAgICAgICAgfVxuICAgICAgICBpZiAodHlwZW9mIG0gPT09ICdmdW5jdGlvbicpIHtcbiAgICAgICAgICB0aGlzLnNweShwcm9wKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHNweShuYW1lOiBzdHJpbmcpIHtcbiAgICBpZiAoISh0aGlzIGFzIGFueSlbbmFtZV0pIHtcbiAgICAgICh0aGlzIGFzIGFueSlbbmFtZV0gPSBqYXNtaW5lLmNyZWF0ZVNweShuYW1lKTtcbiAgICB9XG4gICAgcmV0dXJuICh0aGlzIGFzIGFueSlbbmFtZV07XG4gIH1cblxuICBwcm9wKG5hbWU6IHN0cmluZywgdmFsdWU6IGFueSkgeyAodGhpcyBhcyBhbnkpW25hbWVdID0gdmFsdWU7IH1cblxuICBzdGF0aWMgc3R1YihvYmplY3Q6IGFueSA9IG51bGwsIGNvbmZpZzogYW55ID0gbnVsbCwgb3ZlcnJpZGVzOiBhbnkgPSBudWxsKSB7XG4gICAgaWYgKCEob2JqZWN0IGluc3RhbmNlb2YgU3B5T2JqZWN0KSkge1xuICAgICAgb3ZlcnJpZGVzID0gY29uZmlnO1xuICAgICAgY29uZmlnID0gb2JqZWN0O1xuICAgICAgb2JqZWN0ID0gbmV3IFNweU9iamVjdCgpO1xuICAgIH1cblxuICAgIGNvbnN0IG0gPSB7Li4uY29uZmlnLCAuLi5vdmVycmlkZXN9O1xuICAgIE9iamVjdC5rZXlzKG0pLmZvckVhY2goa2V5ID0+IHsgb2JqZWN0LnNweShrZXkpLmFuZC5yZXR1cm5WYWx1ZShtW2tleV0pOyB9KTtcbiAgICByZXR1cm4gb2JqZWN0O1xuICB9XG59XG4iXX0=
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
import { ɵɵdefineInjectable } from './interface/defs';
/**
 * Creates a token that can be used in a DI Provider.
 *
 * Use an `InjectionToken` whenever the type you are injecting is not reified (does not have a
 * runtime representation) such as when injecting an interface, callable type, array or
 * parameterized type.
 *
 * `InjectionToken` is parameterized on `T` which is the type of object which will be returned by
 * the `Injector`. This provides additional level of type safety.
 *
 * ```
 * interface MyInterface {...}
 * var myInterface = injector.get(new InjectionToken<MyInterface>('SomeToken'));
 * // myInterface is inferred to be MyInterface.
 * ```
 *
 * When creating an `InjectionToken`, you can optionally specify a factory function which returns
 * (possibly by creating) a default value of the parameterized type `T`. This sets up the
 * `InjectionToken` using this factory as a provider as if it was defined explicitly in the
 * application's root injector. If the factory function, which takes zero arguments, needs to inject
 * dependencies, it can do so using the `inject` function. See below for an example.
 *
 * Additionally, if a `factory` is specified you can also specify the `providedIn` option, which
 * overrides the above behavior and marks the token as belonging to a particular `\@NgModule`. As
 * mentioned above, `'root'` is the default value for `providedIn`.
 *
 * \@usageNotes
 * ### Basic Example
 *
 * ### Plain InjectionToken
 *
 * {\@example core/di/ts/injector_spec.ts region='InjectionToken'}
 *
 * ### Tree-shakable InjectionToken
 *
 * {\@example core/di/ts/injector_spec.ts region='ShakableInjectionToken'}
 *
 *
 * \@publicApi
 * @template T
 */
export class InjectionToken {
    /**
     * @param {?} _desc
     * @param {?=} options
     */
    constructor(_desc, options) {
        this._desc = _desc;
        /**
         * \@internal
         */
        this.ngMetadataName = 'InjectionToken';
        /** @nocollapse */ this.ngInjectableDef = undefined;
        if (typeof options == 'number') {
            // This is a special hack to assign __NG_ELEMENT_ID__ to this instance.
            // __NG_ELEMENT_ID__ is Used by Ivy to determine bloom filter id.
            // We are using it to assign `-1` which is used to identify `Injector`.
            ((/** @type {?} */ (this))).__NG_ELEMENT_ID__ = options;
        }
        else if (options !== undefined) {
            /** @nocollapse */ this.ngInjectableDef = ɵɵdefineInjectable({
                token: this,
                providedIn: options.providedIn || 'root',
                factory: options.factory,
            });
        }
    }
    /**
     * @return {?}
     */
    toString() { return `InjectionToken ${this._desc}`; }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    InjectionToken.prototype.ngMetadataName;
    /** @type {?} */
    InjectionToken.prototype.ngInjectableDef;
    /**
     * @type {?}
     * @protected
     */
    InjectionToken.prototype._desc;
}
/**
 * @record
 * @template T
 */
export function InjectableDefToken() { }
if (false) {
    /** @type {?} */
    InjectableDefToken.prototype.ngInjectableDef;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5qZWN0aW9uX3Rva2VuLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvZGkvaW5qZWN0aW9uX3Rva2VuLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBVUEsT0FBTyxFQUFDLGtCQUFrQixFQUFDLE1BQU0sa0JBQWtCLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQTBDcEQsTUFBTSxPQUFPLGNBQWM7Ozs7O0lBTXpCLFlBQXNCLEtBQWEsRUFBRSxPQUdwQztRQUhxQixVQUFLLEdBQUwsS0FBSyxDQUFROzs7O1FBSjFCLG1CQUFjLEdBQUcsZ0JBQWdCLENBQUM7UUFRekMsSUFBSSxDQUFDLGVBQWUsR0FBRyxTQUFTLENBQUM7UUFDakMsSUFBSSxPQUFPLE9BQU8sSUFBSSxRQUFRLEVBQUU7WUFDOUIsdUVBQXVFO1lBQ3ZFLGlFQUFpRTtZQUNqRSx1RUFBdUU7WUFDdkUsQ0FBQyxtQkFBQSxJQUFJLEVBQU8sQ0FBQyxDQUFDLGlCQUFpQixHQUFHLE9BQU8sQ0FBQztTQUMzQzthQUFNLElBQUksT0FBTyxLQUFLLFNBQVMsRUFBRTtZQUNoQyxJQUFJLENBQUMsZUFBZSxHQUFHLGtCQUFrQixDQUFDO2dCQUN4QyxLQUFLLEVBQUUsSUFBSTtnQkFDWCxVQUFVLEVBQUUsT0FBTyxDQUFDLFVBQVUsSUFBSSxNQUFNO2dCQUN4QyxPQUFPLEVBQUUsT0FBTyxDQUFDLE9BQU87YUFDekIsQ0FBQyxDQUFDO1NBQ0o7SUFDSCxDQUFDOzs7O0lBRUQsUUFBUSxLQUFhLE9BQU8sa0JBQWtCLElBQUksQ0FBQyxLQUFLLEVBQUUsQ0FBQyxDQUFDLENBQUM7Q0FDOUQ7Ozs7OztJQXhCQyx3Q0FBMkM7O0lBRTNDLHlDQUEwQzs7Ozs7SUFFOUIsK0JBQXVCOzs7Ozs7QUFzQnJDLHdDQUE0Rjs7O0lBQXpCLDZDQUF1QiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtUeXBlfSBmcm9tICcuLi9pbnRlcmZhY2UvdHlwZSc7XG5cbmltcG9ydCB7ybXJtWRlZmluZUluamVjdGFibGV9IGZyb20gJy4vaW50ZXJmYWNlL2RlZnMnO1xuXG4vKipcbiAqIENyZWF0ZXMgYSB0b2tlbiB0aGF0IGNhbiBiZSB1c2VkIGluIGEgREkgUHJvdmlkZXIuXG4gKlxuICogVXNlIGFuIGBJbmplY3Rpb25Ub2tlbmAgd2hlbmV2ZXIgdGhlIHR5cGUgeW91IGFyZSBpbmplY3RpbmcgaXMgbm90IHJlaWZpZWQgKGRvZXMgbm90IGhhdmUgYVxuICogcnVudGltZSByZXByZXNlbnRhdGlvbikgc3VjaCBhcyB3aGVuIGluamVjdGluZyBhbiBpbnRlcmZhY2UsIGNhbGxhYmxlIHR5cGUsIGFycmF5IG9yXG4gKiBwYXJhbWV0ZXJpemVkIHR5cGUuXG4gKlxuICogYEluamVjdGlvblRva2VuYCBpcyBwYXJhbWV0ZXJpemVkIG9uIGBUYCB3aGljaCBpcyB0aGUgdHlwZSBvZiBvYmplY3Qgd2hpY2ggd2lsbCBiZSByZXR1cm5lZCBieVxuICogdGhlIGBJbmplY3RvcmAuIFRoaXMgcHJvdmlkZXMgYWRkaXRpb25hbCBsZXZlbCBvZiB0eXBlIHNhZmV0eS5cbiAqXG4gKiBgYGBcbiAqIGludGVyZmFjZSBNeUludGVyZmFjZSB7Li4ufVxuICogdmFyIG15SW50ZXJmYWNlID0gaW5qZWN0b3IuZ2V0KG5ldyBJbmplY3Rpb25Ub2tlbjxNeUludGVyZmFjZT4oJ1NvbWVUb2tlbicpKTtcbiAqIC8vIG15SW50ZXJmYWNlIGlzIGluZmVycmVkIHRvIGJlIE15SW50ZXJmYWNlLlxuICogYGBgXG4gKlxuICogV2hlbiBjcmVhdGluZyBhbiBgSW5qZWN0aW9uVG9rZW5gLCB5b3UgY2FuIG9wdGlvbmFsbHkgc3BlY2lmeSBhIGZhY3RvcnkgZnVuY3Rpb24gd2hpY2ggcmV0dXJuc1xuICogKHBvc3NpYmx5IGJ5IGNyZWF0aW5nKSBhIGRlZmF1bHQgdmFsdWUgb2YgdGhlIHBhcmFtZXRlcml6ZWQgdHlwZSBgVGAuIFRoaXMgc2V0cyB1cCB0aGVcbiAqIGBJbmplY3Rpb25Ub2tlbmAgdXNpbmcgdGhpcyBmYWN0b3J5IGFzIGEgcHJvdmlkZXIgYXMgaWYgaXQgd2FzIGRlZmluZWQgZXhwbGljaXRseSBpbiB0aGVcbiAqIGFwcGxpY2F0aW9uJ3Mgcm9vdCBpbmplY3Rvci4gSWYgdGhlIGZhY3RvcnkgZnVuY3Rpb24sIHdoaWNoIHRha2VzIHplcm8gYXJndW1lbnRzLCBuZWVkcyB0byBpbmplY3RcbiAqIGRlcGVuZGVuY2llcywgaXQgY2FuIGRvIHNvIHVzaW5nIHRoZSBgaW5qZWN0YCBmdW5jdGlvbi4gU2VlIGJlbG93IGZvciBhbiBleGFtcGxlLlxuICpcbiAqIEFkZGl0aW9uYWxseSwgaWYgYSBgZmFjdG9yeWAgaXMgc3BlY2lmaWVkIHlvdSBjYW4gYWxzbyBzcGVjaWZ5IHRoZSBgcHJvdmlkZWRJbmAgb3B0aW9uLCB3aGljaFxuICogb3ZlcnJpZGVzIHRoZSBhYm92ZSBiZWhhdmlvciBhbmQgbWFya3MgdGhlIHRva2VuIGFzIGJlbG9uZ2luZyB0byBhIHBhcnRpY3VsYXIgYEBOZ01vZHVsZWAuIEFzXG4gKiBtZW50aW9uZWQgYWJvdmUsIGAncm9vdCdgIGlzIHRoZSBkZWZhdWx0IHZhbHVlIGZvciBgcHJvdmlkZWRJbmAuXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqICMjIyBCYXNpYyBFeGFtcGxlXG4gKlxuICogIyMjIFBsYWluIEluamVjdGlvblRva2VuXG4gKlxuICoge0BleGFtcGxlIGNvcmUvZGkvdHMvaW5qZWN0b3Jfc3BlYy50cyByZWdpb249J0luamVjdGlvblRva2VuJ31cbiAqXG4gKiAjIyMgVHJlZS1zaGFrYWJsZSBJbmplY3Rpb25Ub2tlblxuICpcbiAqIHtAZXhhbXBsZSBjb3JlL2RpL3RzL2luamVjdG9yX3NwZWMudHMgcmVnaW9uPSdTaGFrYWJsZUluamVjdGlvblRva2VuJ31cbiAqXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgSW5qZWN0aW9uVG9rZW48VD4ge1xuICAvKiogQGludGVybmFsICovXG4gIHJlYWRvbmx5IG5nTWV0YWRhdGFOYW1lID0gJ0luamVjdGlvblRva2VuJztcblxuICByZWFkb25seSBuZ0luamVjdGFibGVEZWY6IG5ldmVyfHVuZGVmaW5lZDtcblxuICBjb25zdHJ1Y3Rvcihwcm90ZWN0ZWQgX2Rlc2M6IHN0cmluZywgb3B0aW9ucz86IHtcbiAgICBwcm92aWRlZEluPzogVHlwZTxhbnk+fCAncm9vdCcgfCBudWxsLFxuICAgIGZhY3Rvcnk6ICgpID0+IFRcbiAgfSkge1xuICAgIHRoaXMubmdJbmplY3RhYmxlRGVmID0gdW5kZWZpbmVkO1xuICAgIGlmICh0eXBlb2Ygb3B0aW9ucyA9PSAnbnVtYmVyJykge1xuICAgICAgLy8gVGhpcyBpcyBhIHNwZWNpYWwgaGFjayB0byBhc3NpZ24gX19OR19FTEVNRU5UX0lEX18gdG8gdGhpcyBpbnN0YW5jZS5cbiAgICAgIC8vIF9fTkdfRUxFTUVOVF9JRF9fIGlzIFVzZWQgYnkgSXZ5IHRvIGRldGVybWluZSBibG9vbSBmaWx0ZXIgaWQuXG4gICAgICAvLyBXZSBhcmUgdXNpbmcgaXQgdG8gYXNzaWduIGAtMWAgd2hpY2ggaXMgdXNlZCB0byBpZGVudGlmeSBgSW5qZWN0b3JgLlxuICAgICAgKHRoaXMgYXMgYW55KS5fX05HX0VMRU1FTlRfSURfXyA9IG9wdGlvbnM7XG4gICAgfSBlbHNlIGlmIChvcHRpb25zICE9PSB1bmRlZmluZWQpIHtcbiAgICAgIHRoaXMubmdJbmplY3RhYmxlRGVmID0gybXJtWRlZmluZUluamVjdGFibGUoe1xuICAgICAgICB0b2tlbjogdGhpcyxcbiAgICAgICAgcHJvdmlkZWRJbjogb3B0aW9ucy5wcm92aWRlZEluIHx8ICdyb290JyxcbiAgICAgICAgZmFjdG9yeTogb3B0aW9ucy5mYWN0b3J5LFxuICAgICAgfSk7XG4gICAgfVxuICB9XG5cbiAgdG9TdHJpbmcoKTogc3RyaW5nIHsgcmV0dXJuIGBJbmplY3Rpb25Ub2tlbiAke3RoaXMuX2Rlc2N9YDsgfVxufVxuXG5leHBvcnQgaW50ZXJmYWNlIEluamVjdGFibGVEZWZUb2tlbjxUPiBleHRlbmRzIEluamVjdGlvblRva2VuPFQ+IHsgbmdJbmplY3RhYmxlRGVmOiBuZXZlcjsgfVxuIl19
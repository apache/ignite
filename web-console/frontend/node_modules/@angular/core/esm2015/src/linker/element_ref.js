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
import { injectElementRef as render3InjectElementRef } from '../render3/view_engine_compatibility';
import { noop } from '../util/noop';
/**
 * A wrapper around a native element inside of a View.
 *
 * An `ElementRef` is backed by a render-specific element. In the browser, this is usually a DOM
 * element.
 *
 * \@security Permitting direct access to the DOM can make your application more vulnerable to
 * XSS attacks. Carefully review any use of `ElementRef` in your code. For more detail, see the
 * [Security Guide](http://g.co/ng/security).
 *
 * \@publicApi
 * @template T
 */
// Note: We don't expose things like `Injector`, `ViewContainer`, ... here,
// i.e. users have to ask for what they need. With that, we can build better analysis tools
// and could do better codegen in the future.
export class ElementRef {
    /**
     * @param {?} nativeElement
     */
    constructor(nativeElement) { this.nativeElement = nativeElement; }
}
/**
 * \@internal
 * @nocollapse
 */
ElementRef.__NG_ELEMENT_ID__ = (/**
 * @return {?}
 */
() => SWITCH_ELEMENT_REF_FACTORY(ElementRef));
if (false) {
    /**
     * \@internal
     * @nocollapse
     * @type {?}
     */
    ElementRef.__NG_ELEMENT_ID__;
    /**
     * The underlying native element or `null` if direct access to native elements is not supported
     * (e.g. when the application runs in a web worker).
     *
     * <div class="callout is-critical">
     *   <header>Use with caution</header>
     *   <p>
     *    Use this API as the last resort when direct access to DOM is needed. Use templating and
     *    data-binding provided by Angular instead. Alternatively you can take a look at {\@link
     * Renderer2}
     *    which provides API that can safely be used even when direct access to native elements is not
     *    supported.
     *   </p>
     *   <p>
     *    Relying on direct DOM access creates tight coupling between your application and rendering
     *    layers which will make it impossible to separate the two and deploy your application into a
     *    web worker.
     *   </p>
     * </div>
     *
     * @type {?}
     */
    ElementRef.prototype.nativeElement;
}
/** @type {?} */
export const SWITCH_ELEMENT_REF_FACTORY__POST_R3__ = render3InjectElementRef;
/** @type {?} */
const SWITCH_ELEMENT_REF_FACTORY__PRE_R3__ = noop;
/** @type {?} */
const SWITCH_ELEMENT_REF_FACTORY = SWITCH_ELEMENT_REF_FACTORY__PRE_R3__;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZWxlbWVudF9yZWYuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9saW5rZXIvZWxlbWVudF9yZWYudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsZ0JBQWdCLElBQUksdUJBQXVCLEVBQUMsTUFBTSxzQ0FBc0MsQ0FBQztBQUNqRyxPQUFPLEVBQUMsSUFBSSxFQUFDLE1BQU0sY0FBYyxDQUFDOzs7Ozs7Ozs7Ozs7Ozs7OztBQWlCbEMsTUFBTSxPQUFPLFVBQVU7Ozs7SUF3QnJCLFlBQVksYUFBZ0IsSUFBSSxJQUFJLENBQUMsYUFBYSxHQUFHLGFBQWEsQ0FBQyxDQUFDLENBQUM7Ozs7OztBQU05RCw0QkFBaUI7OztBQUFxQixHQUFHLEVBQUUsQ0FBQywwQkFBMEIsQ0FBQyxVQUFVLENBQUMsRUFBQzs7Ozs7OztJQUExRiw2QkFBMEY7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0lBUjFGLG1DQUF3Qjs7O0FBVzFCLE1BQU0sT0FBTyxxQ0FBcUMsR0FBRyx1QkFBdUI7O01BQ3RFLG9DQUFvQyxHQUFHLElBQUk7O01BQzNDLDBCQUEwQixHQUM1QixvQ0FBb0MiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7aW5qZWN0RWxlbWVudFJlZiBhcyByZW5kZXIzSW5qZWN0RWxlbWVudFJlZn0gZnJvbSAnLi4vcmVuZGVyMy92aWV3X2VuZ2luZV9jb21wYXRpYmlsaXR5JztcbmltcG9ydCB7bm9vcH0gZnJvbSAnLi4vdXRpbC9ub29wJztcblxuLyoqXG4gKiBBIHdyYXBwZXIgYXJvdW5kIGEgbmF0aXZlIGVsZW1lbnQgaW5zaWRlIG9mIGEgVmlldy5cbiAqXG4gKiBBbiBgRWxlbWVudFJlZmAgaXMgYmFja2VkIGJ5IGEgcmVuZGVyLXNwZWNpZmljIGVsZW1lbnQuIEluIHRoZSBicm93c2VyLCB0aGlzIGlzIHVzdWFsbHkgYSBET01cbiAqIGVsZW1lbnQuXG4gKlxuICogQHNlY3VyaXR5IFBlcm1pdHRpbmcgZGlyZWN0IGFjY2VzcyB0byB0aGUgRE9NIGNhbiBtYWtlIHlvdXIgYXBwbGljYXRpb24gbW9yZSB2dWxuZXJhYmxlIHRvXG4gKiBYU1MgYXR0YWNrcy4gQ2FyZWZ1bGx5IHJldmlldyBhbnkgdXNlIG9mIGBFbGVtZW50UmVmYCBpbiB5b3VyIGNvZGUuIEZvciBtb3JlIGRldGFpbCwgc2VlIHRoZVxuICogW1NlY3VyaXR5IEd1aWRlXShodHRwOi8vZy5jby9uZy9zZWN1cml0eSkuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG4vLyBOb3RlOiBXZSBkb24ndCBleHBvc2UgdGhpbmdzIGxpa2UgYEluamVjdG9yYCwgYFZpZXdDb250YWluZXJgLCAuLi4gaGVyZSxcbi8vIGkuZS4gdXNlcnMgaGF2ZSB0byBhc2sgZm9yIHdoYXQgdGhleSBuZWVkLiBXaXRoIHRoYXQsIHdlIGNhbiBidWlsZCBiZXR0ZXIgYW5hbHlzaXMgdG9vbHNcbi8vIGFuZCBjb3VsZCBkbyBiZXR0ZXIgY29kZWdlbiBpbiB0aGUgZnV0dXJlLlxuZXhwb3J0IGNsYXNzIEVsZW1lbnRSZWY8VCBleHRlbmRzIGFueSA9IGFueT4ge1xuICAvKipcbiAgICogVGhlIHVuZGVybHlpbmcgbmF0aXZlIGVsZW1lbnQgb3IgYG51bGxgIGlmIGRpcmVjdCBhY2Nlc3MgdG8gbmF0aXZlIGVsZW1lbnRzIGlzIG5vdCBzdXBwb3J0ZWRcbiAgICogKGUuZy4gd2hlbiB0aGUgYXBwbGljYXRpb24gcnVucyBpbiBhIHdlYiB3b3JrZXIpLlxuICAgKlxuICAgKiA8ZGl2IGNsYXNzPVwiY2FsbG91dCBpcy1jcml0aWNhbFwiPlxuICAgKiAgIDxoZWFkZXI+VXNlIHdpdGggY2F1dGlvbjwvaGVhZGVyPlxuICAgKiAgIDxwPlxuICAgKiAgICBVc2UgdGhpcyBBUEkgYXMgdGhlIGxhc3QgcmVzb3J0IHdoZW4gZGlyZWN0IGFjY2VzcyB0byBET00gaXMgbmVlZGVkLiBVc2UgdGVtcGxhdGluZyBhbmRcbiAgICogICAgZGF0YS1iaW5kaW5nIHByb3ZpZGVkIGJ5IEFuZ3VsYXIgaW5zdGVhZC4gQWx0ZXJuYXRpdmVseSB5b3UgY2FuIHRha2UgYSBsb29rIGF0IHtAbGlua1xuICAgKiBSZW5kZXJlcjJ9XG4gICAqICAgIHdoaWNoIHByb3ZpZGVzIEFQSSB0aGF0IGNhbiBzYWZlbHkgYmUgdXNlZCBldmVuIHdoZW4gZGlyZWN0IGFjY2VzcyB0byBuYXRpdmUgZWxlbWVudHMgaXMgbm90XG4gICAqICAgIHN1cHBvcnRlZC5cbiAgICogICA8L3A+XG4gICAqICAgPHA+XG4gICAqICAgIFJlbHlpbmcgb24gZGlyZWN0IERPTSBhY2Nlc3MgY3JlYXRlcyB0aWdodCBjb3VwbGluZyBiZXR3ZWVuIHlvdXIgYXBwbGljYXRpb24gYW5kIHJlbmRlcmluZ1xuICAgKiAgICBsYXllcnMgd2hpY2ggd2lsbCBtYWtlIGl0IGltcG9zc2libGUgdG8gc2VwYXJhdGUgdGhlIHR3byBhbmQgZGVwbG95IHlvdXIgYXBwbGljYXRpb24gaW50byBhXG4gICAqICAgIHdlYiB3b3JrZXIuXG4gICAqICAgPC9wPlxuICAgKiA8L2Rpdj5cbiAgICpcbiAgICovXG4gIHB1YmxpYyBuYXRpdmVFbGVtZW50OiBUO1xuXG4gIGNvbnN0cnVjdG9yKG5hdGl2ZUVsZW1lbnQ6IFQpIHsgdGhpcy5uYXRpdmVFbGVtZW50ID0gbmF0aXZlRWxlbWVudDsgfVxuXG4gIC8qKlxuICAgKiBAaW50ZXJuYWxcbiAgICogQG5vY29sbGFwc2VcbiAgICovXG4gIHN0YXRpYyBfX05HX0VMRU1FTlRfSURfXzogKCkgPT4gRWxlbWVudFJlZiA9ICgpID0+IFNXSVRDSF9FTEVNRU5UX1JFRl9GQUNUT1JZKEVsZW1lbnRSZWYpO1xufVxuXG5leHBvcnQgY29uc3QgU1dJVENIX0VMRU1FTlRfUkVGX0ZBQ1RPUllfX1BPU1RfUjNfXyA9IHJlbmRlcjNJbmplY3RFbGVtZW50UmVmO1xuY29uc3QgU1dJVENIX0VMRU1FTlRfUkVGX0ZBQ1RPUllfX1BSRV9SM19fID0gbm9vcDtcbmNvbnN0IFNXSVRDSF9FTEVNRU5UX1JFRl9GQUNUT1JZOiB0eXBlb2YgcmVuZGVyM0luamVjdEVsZW1lbnRSZWYgPVxuICAgIFNXSVRDSF9FTEVNRU5UX1JFRl9GQUNUT1JZX19QUkVfUjNfXztcbiJdfQ==
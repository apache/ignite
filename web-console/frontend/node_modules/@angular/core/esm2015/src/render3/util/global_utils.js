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
import { assertDefined } from '../../util/assert';
import { global } from '../../util/global';
import { getComponent, getContext, getDirectives, getHostElement, getInjector, getListeners, getRootComponents, getViewComponent, markDirty } from '../global_utils_api';
/**
 * This value reflects the property on the window where the dev
 * tools are patched (window.ng).
 *
 * @type {?}
 */
export const GLOBAL_PUBLISH_EXPANDO_KEY = 'ng';
/** @type {?} */
let _published = false;
/**
 * Publishes a collection of default debug tools onto`window.ng`.
 *
 * These functions are available globally when Angular is in development
 * mode and are automatically stripped away from prod mode is on.
 * @return {?}
 */
export function publishDefaultGlobalUtils() {
    if (!_published) {
        _published = true;
        publishGlobalUtil('getComponent', getComponent);
        publishGlobalUtil('getContext', getContext);
        publishGlobalUtil('getListeners', getListeners);
        publishGlobalUtil('getViewComponent', getViewComponent);
        publishGlobalUtil('getHostElement', getHostElement);
        publishGlobalUtil('getInjector', getInjector);
        publishGlobalUtil('getRootComponents', getRootComponents);
        publishGlobalUtil('getDirectives', getDirectives);
        publishGlobalUtil('markDirty', markDirty);
    }
}
/**
 * Publishes the given function to `window.ng` so that it can be
 * used from the browser console when an application is not in production.
 * @param {?} name
 * @param {?} fn
 * @return {?}
 */
export function publishGlobalUtil(name, fn) {
    /** @type {?} */
    const w = (/** @type {?} */ ((/** @type {?} */ (global))));
    ngDevMode && assertDefined(fn, 'function not defined');
    if (w) {
        /** @type {?} */
        let container = w[GLOBAL_PUBLISH_EXPANDO_KEY];
        if (!container) {
            container = w[GLOBAL_PUBLISH_EXPANDO_KEY] = {};
        }
        container[name] = fn;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZ2xvYmFsX3V0aWxzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvcmVuZGVyMy91dGlsL2dsb2JhbF91dGlscy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQU9BLE9BQU8sRUFBQyxhQUFhLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUNoRCxPQUFPLEVBQUMsTUFBTSxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFFekMsT0FBTyxFQUFDLFlBQVksRUFBRSxVQUFVLEVBQUUsYUFBYSxFQUFFLGNBQWMsRUFBRSxXQUFXLEVBQUUsWUFBWSxFQUFFLGlCQUFpQixFQUFFLGdCQUFnQixFQUFFLFNBQVMsRUFBQyxNQUFNLHFCQUFxQixDQUFDOzs7Ozs7O0FBb0J2SyxNQUFNLE9BQU8sMEJBQTBCLEdBQUcsSUFBSTs7SUFFMUMsVUFBVSxHQUFHLEtBQUs7Ozs7Ozs7O0FBT3RCLE1BQU0sVUFBVSx5QkFBeUI7SUFDdkMsSUFBSSxDQUFDLFVBQVUsRUFBRTtRQUNmLFVBQVUsR0FBRyxJQUFJLENBQUM7UUFDbEIsaUJBQWlCLENBQUMsY0FBYyxFQUFFLFlBQVksQ0FBQyxDQUFDO1FBQ2hELGlCQUFpQixDQUFDLFlBQVksRUFBRSxVQUFVLENBQUMsQ0FBQztRQUM1QyxpQkFBaUIsQ0FBQyxjQUFjLEVBQUUsWUFBWSxDQUFDLENBQUM7UUFDaEQsaUJBQWlCLENBQUMsa0JBQWtCLEVBQUUsZ0JBQWdCLENBQUMsQ0FBQztRQUN4RCxpQkFBaUIsQ0FBQyxnQkFBZ0IsRUFBRSxjQUFjLENBQUMsQ0FBQztRQUNwRCxpQkFBaUIsQ0FBQyxhQUFhLEVBQUUsV0FBVyxDQUFDLENBQUM7UUFDOUMsaUJBQWlCLENBQUMsbUJBQW1CLEVBQUUsaUJBQWlCLENBQUMsQ0FBQztRQUMxRCxpQkFBaUIsQ0FBQyxlQUFlLEVBQUUsYUFBYSxDQUFDLENBQUM7UUFDbEQsaUJBQWlCLENBQUMsV0FBVyxFQUFFLFNBQVMsQ0FBQyxDQUFDO0tBQzNDO0FBQ0gsQ0FBQzs7Ozs7Ozs7QUFVRCxNQUFNLFVBQVUsaUJBQWlCLENBQUMsSUFBWSxFQUFFLEVBQVk7O1VBQ3BELENBQUMsR0FBRyxtQkFBQSxtQkFBQSxNQUFNLEVBQU8sRUFBMEI7SUFDakQsU0FBUyxJQUFJLGFBQWEsQ0FBQyxFQUFFLEVBQUUsc0JBQXNCLENBQUMsQ0FBQztJQUN2RCxJQUFJLENBQUMsRUFBRTs7WUFDRCxTQUFTLEdBQUcsQ0FBQyxDQUFDLDBCQUEwQixDQUFDO1FBQzdDLElBQUksQ0FBQyxTQUFTLEVBQUU7WUFDZCxTQUFTLEdBQUcsQ0FBQyxDQUFDLDBCQUEwQixDQUFDLEdBQUcsRUFBRSxDQUFDO1NBQ2hEO1FBQ0QsU0FBUyxDQUFDLElBQUksQ0FBQyxHQUFHLEVBQUUsQ0FBQztLQUN0QjtBQUNILENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5pbXBvcnQge2Fzc2VydERlZmluZWR9IGZyb20gJy4uLy4uL3V0aWwvYXNzZXJ0JztcbmltcG9ydCB7Z2xvYmFsfSBmcm9tICcuLi8uLi91dGlsL2dsb2JhbCc7XG5cbmltcG9ydCB7Z2V0Q29tcG9uZW50LCBnZXRDb250ZXh0LCBnZXREaXJlY3RpdmVzLCBnZXRIb3N0RWxlbWVudCwgZ2V0SW5qZWN0b3IsIGdldExpc3RlbmVycywgZ2V0Um9vdENvbXBvbmVudHMsIGdldFZpZXdDb21wb25lbnQsIG1hcmtEaXJ0eX0gZnJvbSAnLi4vZ2xvYmFsX3V0aWxzX2FwaSc7XG5cblxuXG4vKipcbiAqIFRoaXMgZmlsZSBpbnRyb2R1Y2VzIHNlcmllcyBvZiBnbG9iYWxseSBhY2Nlc3NpYmxlIGRlYnVnIHRvb2xzXG4gKiB0byBhbGxvdyBmb3IgdGhlIEFuZ3VsYXIgZGVidWdnaW5nIHN0b3J5IHRvIGZ1bmN0aW9uLlxuICpcbiAqIFRvIHNlZSB0aGlzIGluIGFjdGlvbiBydW4gdGhlIGZvbGxvd2luZyBjb21tYW5kOlxuICpcbiAqICAgYmF6ZWwgcnVuIC0tZGVmaW5lPWNvbXBpbGU9YW90XG4gKiAgIC8vcGFja2FnZXMvY29yZS90ZXN0L2J1bmRsaW5nL3RvZG86ZGV2c2VydmVyXG4gKlxuICogIFRoZW4gbG9hZCBgbG9jYWxob3N0OjU0MzJgIGFuZCBzdGFydCB1c2luZyB0aGUgY29uc29sZSB0b29scy5cbiAqL1xuXG4vKipcbiAqIFRoaXMgdmFsdWUgcmVmbGVjdHMgdGhlIHByb3BlcnR5IG9uIHRoZSB3aW5kb3cgd2hlcmUgdGhlIGRldlxuICogdG9vbHMgYXJlIHBhdGNoZWQgKHdpbmRvdy5uZykuXG4gKiAqL1xuZXhwb3J0IGNvbnN0IEdMT0JBTF9QVUJMSVNIX0VYUEFORE9fS0VZID0gJ25nJztcblxubGV0IF9wdWJsaXNoZWQgPSBmYWxzZTtcbi8qKlxuICogUHVibGlzaGVzIGEgY29sbGVjdGlvbiBvZiBkZWZhdWx0IGRlYnVnIHRvb2xzIG9udG9gd2luZG93Lm5nYC5cbiAqXG4gKiBUaGVzZSBmdW5jdGlvbnMgYXJlIGF2YWlsYWJsZSBnbG9iYWxseSB3aGVuIEFuZ3VsYXIgaXMgaW4gZGV2ZWxvcG1lbnRcbiAqIG1vZGUgYW5kIGFyZSBhdXRvbWF0aWNhbGx5IHN0cmlwcGVkIGF3YXkgZnJvbSBwcm9kIG1vZGUgaXMgb24uXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBwdWJsaXNoRGVmYXVsdEdsb2JhbFV0aWxzKCkge1xuICBpZiAoIV9wdWJsaXNoZWQpIHtcbiAgICBfcHVibGlzaGVkID0gdHJ1ZTtcbiAgICBwdWJsaXNoR2xvYmFsVXRpbCgnZ2V0Q29tcG9uZW50JywgZ2V0Q29tcG9uZW50KTtcbiAgICBwdWJsaXNoR2xvYmFsVXRpbCgnZ2V0Q29udGV4dCcsIGdldENvbnRleHQpO1xuICAgIHB1Ymxpc2hHbG9iYWxVdGlsKCdnZXRMaXN0ZW5lcnMnLCBnZXRMaXN0ZW5lcnMpO1xuICAgIHB1Ymxpc2hHbG9iYWxVdGlsKCdnZXRWaWV3Q29tcG9uZW50JywgZ2V0Vmlld0NvbXBvbmVudCk7XG4gICAgcHVibGlzaEdsb2JhbFV0aWwoJ2dldEhvc3RFbGVtZW50JywgZ2V0SG9zdEVsZW1lbnQpO1xuICAgIHB1Ymxpc2hHbG9iYWxVdGlsKCdnZXRJbmplY3RvcicsIGdldEluamVjdG9yKTtcbiAgICBwdWJsaXNoR2xvYmFsVXRpbCgnZ2V0Um9vdENvbXBvbmVudHMnLCBnZXRSb290Q29tcG9uZW50cyk7XG4gICAgcHVibGlzaEdsb2JhbFV0aWwoJ2dldERpcmVjdGl2ZXMnLCBnZXREaXJlY3RpdmVzKTtcbiAgICBwdWJsaXNoR2xvYmFsVXRpbCgnbWFya0RpcnR5JywgbWFya0RpcnR5KTtcbiAgfVxufVxuXG5leHBvcnQgZGVjbGFyZSB0eXBlIEdsb2JhbERldk1vZGVDb250YWluZXIgPSB7XG4gIFtHTE9CQUxfUFVCTElTSF9FWFBBTkRPX0tFWV06IHtbZm5OYW1lOiBzdHJpbmddOiBGdW5jdGlvbn07XG59O1xuXG4vKipcbiAqIFB1Ymxpc2hlcyB0aGUgZ2l2ZW4gZnVuY3Rpb24gdG8gYHdpbmRvdy5uZ2Agc28gdGhhdCBpdCBjYW4gYmVcbiAqIHVzZWQgZnJvbSB0aGUgYnJvd3NlciBjb25zb2xlIHdoZW4gYW4gYXBwbGljYXRpb24gaXMgbm90IGluIHByb2R1Y3Rpb24uXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBwdWJsaXNoR2xvYmFsVXRpbChuYW1lOiBzdHJpbmcsIGZuOiBGdW5jdGlvbik6IHZvaWQge1xuICBjb25zdCB3ID0gZ2xvYmFsIGFzIGFueSBhcyBHbG9iYWxEZXZNb2RlQ29udGFpbmVyO1xuICBuZ0Rldk1vZGUgJiYgYXNzZXJ0RGVmaW5lZChmbiwgJ2Z1bmN0aW9uIG5vdCBkZWZpbmVkJyk7XG4gIGlmICh3KSB7XG4gICAgbGV0IGNvbnRhaW5lciA9IHdbR0xPQkFMX1BVQkxJU0hfRVhQQU5ET19LRVldO1xuICAgIGlmICghY29udGFpbmVyKSB7XG4gICAgICBjb250YWluZXIgPSB3W0dMT0JBTF9QVUJMSVNIX0VYUEFORE9fS0VZXSA9IHt9O1xuICAgIH1cbiAgICBjb250YWluZXJbbmFtZV0gPSBmbjtcbiAgfVxufVxuIl19
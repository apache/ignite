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
import { Location } from '@angular/common';
import { APP_BOOTSTRAP_LISTENER } from '@angular/core';
import { Router } from '@angular/router';
import { UpgradeModule } from '@angular/upgrade/static';
const ɵ0 = (locationSyncBootstrapListener);
/**
 * Creates an initializer that sets up `ngRoute` integration
 * along with setting up the Angular router.
 *
 * \@usageNotes
 *
 * <code-example language="typescript">
 * \@NgModule({
 *  imports: [
 *   RouterModule.forRoot(SOME_ROUTES),
 *   UpgradeModule
 * ],
 * providers: [
 *   RouterUpgradeInitializer
 * ]
 * })
 * export class AppModule {
 *   ngDoBootstrap() {}
 * }
 * </code-example>
 *
 * \@publicApi
 * @type {?}
 */
export const RouterUpgradeInitializer = {
    provide: APP_BOOTSTRAP_LISTENER,
    multi: true,
    useFactory: (/** @type {?} */ (ɵ0)),
    deps: [UpgradeModule]
};
/**
 * \@internal
 * @param {?} ngUpgrade
 * @return {?}
 */
export function locationSyncBootstrapListener(ngUpgrade) {
    return (/**
     * @return {?}
     */
    () => { setUpLocationSync(ngUpgrade); });
}
/**
 * Sets up a location change listener to trigger `history.pushState`.
 * Works around the problem that `onPopState` does not trigger `history.pushState`.
 * Must be called *after* calling `UpgradeModule.bootstrap`.
 *
 * @see `HashLocationStrategy` / `PathLocationStrategy`
 *
 * \@publicApi
 * @param {?} ngUpgrade The upgrade NgModule.
 * @param {?=} urlType The location strategy.
 * @return {?}
 */
export function setUpLocationSync(ngUpgrade, urlType = 'path') {
    if (!ngUpgrade.$injector) {
        throw new Error(`
        RouterUpgradeInitializer can be used only after UpgradeModule.bootstrap has been called.
        Remove RouterUpgradeInitializer and call setUpLocationSync after UpgradeModule.bootstrap.
      `);
    }
    /** @type {?} */
    const router = ngUpgrade.injector.get(Router);
    /** @type {?} */
    const location = ngUpgrade.injector.get(Location);
    ngUpgrade.$injector.get('$rootScope')
        .$on('$locationChangeStart', (/**
     * @param {?} _
     * @param {?} next
     * @param {?} __
     * @return {?}
     */
    (_, next, __) => {
        /** @type {?} */
        let url;
        if (urlType === 'path') {
            url = resolveUrl(next);
        }
        else if (urlType === 'hash') {
            // Remove the first hash from the URL
            /** @type {?} */
            const hashIdx = next.indexOf('#');
            url = resolveUrl(next.substring(0, hashIdx) + next.substring(hashIdx + 1));
        }
        else {
            throw 'Invalid URLType passed to setUpLocationSync: ' + urlType;
        }
        /** @type {?} */
        const path = location.normalize(url.pathname);
        router.navigateByUrl(path + url.search + url.hash);
    }));
}
/**
 * Normalizes and parses a URL.
 *
 * - Normalizing means that a relative URL will be resolved into an absolute URL in the context of
 *   the application document.
 * - Parsing means that the anchor's `protocol`, `hostname`, `port`, `pathname` and related
 *   properties are all populated to reflect the normalized URL.
 *
 * While this approach has wide compatibility, it doesn't work as expected on IE. On IE, normalizing
 * happens similar to other browsers, but the parsed components will not be set. (E.g. if you assign
 * `a.href = 'foo'`, then `a.protocol`, `a.host`, etc. will not be correctly updated.)
 * We work around that by performing the parsing in a 2nd step by taking a previously normalized URL
 * and assigning it again. This correctly populates all properties.
 *
 * See
 * https://github.com/angular/angular.js/blob/2c7400e7d07b0f6cec1817dab40b9250ce8ebce6/src/ng/urlUtils.js#L26-L33
 * for more info.
 * @type {?}
 */
let anchor;
/**
 * @param {?} url
 * @return {?}
 */
function resolveUrl(url) {
    if (!anchor) {
        anchor = document.createElement('a');
    }
    anchor.setAttribute('href', url);
    anchor.setAttribute('href', anchor.href);
    return {
        // IE does not start `pathname` with `/` like other browsers.
        pathname: `/${anchor.pathname.replace(/^\//, '')}`,
        search: anchor.search,
        hash: anchor.hash
    };
}
export { ɵ0 };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXBncmFkZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3JvdXRlci91cGdyYWRlL3NyYy91cGdyYWRlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLFFBQVEsRUFBQyxNQUFNLGlCQUFpQixDQUFDO0FBQ3pDLE9BQU8sRUFBQyxzQkFBc0IsRUFBK0IsTUFBTSxlQUFlLENBQUM7QUFDbkYsT0FBTyxFQUFDLE1BQU0sRUFBQyxNQUFNLGlCQUFpQixDQUFDO0FBQ3ZDLE9BQU8sRUFBQyxhQUFhLEVBQUMsTUFBTSx5QkFBeUIsQ0FBQztXQTRCeEMsQ0FBQSw2QkFBNkIsQ0FBMkM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFIdEYsTUFBTSxPQUFPLHdCQUF3QixHQUFHO0lBQ3RDLE9BQU8sRUFBRSxzQkFBc0I7SUFDL0IsS0FBSyxFQUFFLElBQUk7SUFDWCxVQUFVLEVBQUUsdUJBQXdFO0lBQ3BGLElBQUksRUFBRSxDQUFDLGFBQWEsQ0FBQztDQUN0Qjs7Ozs7O0FBS0QsTUFBTSxVQUFVLDZCQUE2QixDQUFDLFNBQXdCO0lBQ3BFOzs7SUFBTyxHQUFHLEVBQUUsR0FBRyxpQkFBaUIsQ0FBQyxTQUFTLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBQztBQUNqRCxDQUFDOzs7Ozs7Ozs7Ozs7O0FBY0QsTUFBTSxVQUFVLGlCQUFpQixDQUFDLFNBQXdCLEVBQUUsVUFBMkIsTUFBTTtJQUMzRixJQUFJLENBQUMsU0FBUyxDQUFDLFNBQVMsRUFBRTtRQUN4QixNQUFNLElBQUksS0FBSyxDQUFDOzs7T0FHYixDQUFDLENBQUM7S0FDTjs7VUFFSyxNQUFNLEdBQVcsU0FBUyxDQUFDLFFBQVEsQ0FBQyxHQUFHLENBQUMsTUFBTSxDQUFDOztVQUMvQyxRQUFRLEdBQWEsU0FBUyxDQUFDLFFBQVEsQ0FBQyxHQUFHLENBQUMsUUFBUSxDQUFDO0lBRTNELFNBQVMsQ0FBQyxTQUFTLENBQUMsR0FBRyxDQUFDLFlBQVksQ0FBQztTQUNoQyxHQUFHLENBQUMsc0JBQXNCOzs7Ozs7SUFBRSxDQUFDLENBQU0sRUFBRSxJQUFZLEVBQUUsRUFBVSxFQUFFLEVBQUU7O1lBQzVELEdBQUc7UUFDUCxJQUFJLE9BQU8sS0FBSyxNQUFNLEVBQUU7WUFDdEIsR0FBRyxHQUFHLFVBQVUsQ0FBQyxJQUFJLENBQUMsQ0FBQztTQUN4QjthQUFNLElBQUksT0FBTyxLQUFLLE1BQU0sRUFBRTs7O2tCQUV2QixPQUFPLEdBQUcsSUFBSSxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUM7WUFDakMsR0FBRyxHQUFHLFVBQVUsQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLENBQUMsRUFBRSxPQUFPLENBQUMsR0FBRyxJQUFJLENBQUMsU0FBUyxDQUFDLE9BQU8sR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDO1NBQzVFO2FBQU07WUFDTCxNQUFNLCtDQUErQyxHQUFHLE9BQU8sQ0FBQztTQUNqRTs7Y0FDSyxJQUFJLEdBQUcsUUFBUSxDQUFDLFNBQVMsQ0FBQyxHQUFHLENBQUMsUUFBUSxDQUFDO1FBQzdDLE1BQU0sQ0FBQyxhQUFhLENBQUMsSUFBSSxHQUFHLEdBQUcsQ0FBQyxNQUFNLEdBQUcsR0FBRyxDQUFDLElBQUksQ0FBQyxDQUFDO0lBQ3JELENBQUMsRUFBQyxDQUFDO0FBQ1QsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7SUFvQkcsTUFBbUM7Ozs7O0FBQ3ZDLFNBQVMsVUFBVSxDQUFDLEdBQVc7SUFDN0IsSUFBSSxDQUFDLE1BQU0sRUFBRTtRQUNYLE1BQU0sR0FBRyxRQUFRLENBQUMsYUFBYSxDQUFDLEdBQUcsQ0FBQyxDQUFDO0tBQ3RDO0lBRUQsTUFBTSxDQUFDLFlBQVksQ0FBQyxNQUFNLEVBQUUsR0FBRyxDQUFDLENBQUM7SUFDakMsTUFBTSxDQUFDLFlBQVksQ0FBQyxNQUFNLEVBQUUsTUFBTSxDQUFDLElBQUksQ0FBQyxDQUFDO0lBRXpDLE9BQU87O1FBRUwsUUFBUSxFQUFFLElBQUksTUFBTSxDQUFDLFFBQVEsQ0FBQyxPQUFPLENBQUMsS0FBSyxFQUFFLEVBQUUsQ0FBQyxFQUFFO1FBQ2xELE1BQU0sRUFBRSxNQUFNLENBQUMsTUFBTTtRQUNyQixJQUFJLEVBQUUsTUFBTSxDQUFDLElBQUk7S0FDbEIsQ0FBQztBQUNKLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7TG9jYXRpb259IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbic7XG5pbXBvcnQge0FQUF9CT09UU1RSQVBfTElTVEVORVIsIENvbXBvbmVudFJlZiwgSW5qZWN0aW9uVG9rZW59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHtSb3V0ZXJ9IGZyb20gJ0Bhbmd1bGFyL3JvdXRlcic7XG5pbXBvcnQge1VwZ3JhZGVNb2R1bGV9IGZyb20gJ0Bhbmd1bGFyL3VwZ3JhZGUvc3RhdGljJztcblxuLyoqXG4gKiBDcmVhdGVzIGFuIGluaXRpYWxpemVyIHRoYXQgc2V0cyB1cCBgbmdSb3V0ZWAgaW50ZWdyYXRpb25cbiAqIGFsb25nIHdpdGggc2V0dGluZyB1cCB0aGUgQW5ndWxhciByb3V0ZXIuXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqXG4gKiA8Y29kZS1leGFtcGxlIGxhbmd1YWdlPVwidHlwZXNjcmlwdFwiPlxuICogQE5nTW9kdWxlKHtcbiAqICBpbXBvcnRzOiBbXG4gKiAgIFJvdXRlck1vZHVsZS5mb3JSb290KFNPTUVfUk9VVEVTKSxcbiAqICAgVXBncmFkZU1vZHVsZVxuICogXSxcbiAqIHByb3ZpZGVyczogW1xuICogICBSb3V0ZXJVcGdyYWRlSW5pdGlhbGl6ZXJcbiAqIF1cbiAqIH0pXG4gKiBleHBvcnQgY2xhc3MgQXBwTW9kdWxlIHtcbiAqICAgbmdEb0Jvb3RzdHJhcCgpIHt9XG4gKiB9XG4gKiA8L2NvZGUtZXhhbXBsZT5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjb25zdCBSb3V0ZXJVcGdyYWRlSW5pdGlhbGl6ZXIgPSB7XG4gIHByb3ZpZGU6IEFQUF9CT09UU1RSQVBfTElTVEVORVIsXG4gIG11bHRpOiB0cnVlLFxuICB1c2VGYWN0b3J5OiBsb2NhdGlvblN5bmNCb290c3RyYXBMaXN0ZW5lciBhcyhuZ1VwZ3JhZGU6IFVwZ3JhZGVNb2R1bGUpID0+ICgpID0+IHZvaWQsXG4gIGRlcHM6IFtVcGdyYWRlTW9kdWxlXVxufTtcblxuLyoqXG4gKiBAaW50ZXJuYWxcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGxvY2F0aW9uU3luY0Jvb3RzdHJhcExpc3RlbmVyKG5nVXBncmFkZTogVXBncmFkZU1vZHVsZSkge1xuICByZXR1cm4gKCkgPT4geyBzZXRVcExvY2F0aW9uU3luYyhuZ1VwZ3JhZGUpOyB9O1xufVxuXG4vKipcbiAqIFNldHMgdXAgYSBsb2NhdGlvbiBjaGFuZ2UgbGlzdGVuZXIgdG8gdHJpZ2dlciBgaGlzdG9yeS5wdXNoU3RhdGVgLlxuICogV29ya3MgYXJvdW5kIHRoZSBwcm9ibGVtIHRoYXQgYG9uUG9wU3RhdGVgIGRvZXMgbm90IHRyaWdnZXIgYGhpc3RvcnkucHVzaFN0YXRlYC5cbiAqIE11c3QgYmUgY2FsbGVkICphZnRlciogY2FsbGluZyBgVXBncmFkZU1vZHVsZS5ib290c3RyYXBgLlxuICpcbiAqIEBwYXJhbSBuZ1VwZ3JhZGUgVGhlIHVwZ3JhZGUgTmdNb2R1bGUuXG4gKiBAcGFyYW0gdXJsVHlwZSBUaGUgbG9jYXRpb24gc3RyYXRlZ3kuXG4gKiBAc2VlIGBIYXNoTG9jYXRpb25TdHJhdGVneWBcbiAqIEBzZWUgYFBhdGhMb2NhdGlvblN0cmF0ZWd5YFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHNldFVwTG9jYXRpb25TeW5jKG5nVXBncmFkZTogVXBncmFkZU1vZHVsZSwgdXJsVHlwZTogJ3BhdGgnIHwgJ2hhc2gnID0gJ3BhdGgnKSB7XG4gIGlmICghbmdVcGdyYWRlLiRpbmplY3Rvcikge1xuICAgIHRocm93IG5ldyBFcnJvcihgXG4gICAgICAgIFJvdXRlclVwZ3JhZGVJbml0aWFsaXplciBjYW4gYmUgdXNlZCBvbmx5IGFmdGVyIFVwZ3JhZGVNb2R1bGUuYm9vdHN0cmFwIGhhcyBiZWVuIGNhbGxlZC5cbiAgICAgICAgUmVtb3ZlIFJvdXRlclVwZ3JhZGVJbml0aWFsaXplciBhbmQgY2FsbCBzZXRVcExvY2F0aW9uU3luYyBhZnRlciBVcGdyYWRlTW9kdWxlLmJvb3RzdHJhcC5cbiAgICAgIGApO1xuICB9XG5cbiAgY29uc3Qgcm91dGVyOiBSb3V0ZXIgPSBuZ1VwZ3JhZGUuaW5qZWN0b3IuZ2V0KFJvdXRlcik7XG4gIGNvbnN0IGxvY2F0aW9uOiBMb2NhdGlvbiA9IG5nVXBncmFkZS5pbmplY3Rvci5nZXQoTG9jYXRpb24pO1xuXG4gIG5nVXBncmFkZS4kaW5qZWN0b3IuZ2V0KCckcm9vdFNjb3BlJylcbiAgICAgIC4kb24oJyRsb2NhdGlvbkNoYW5nZVN0YXJ0JywgKF86IGFueSwgbmV4dDogc3RyaW5nLCBfXzogc3RyaW5nKSA9PiB7XG4gICAgICAgIGxldCB1cmw7XG4gICAgICAgIGlmICh1cmxUeXBlID09PSAncGF0aCcpIHtcbiAgICAgICAgICB1cmwgPSByZXNvbHZlVXJsKG5leHQpO1xuICAgICAgICB9IGVsc2UgaWYgKHVybFR5cGUgPT09ICdoYXNoJykge1xuICAgICAgICAgIC8vIFJlbW92ZSB0aGUgZmlyc3QgaGFzaCBmcm9tIHRoZSBVUkxcbiAgICAgICAgICBjb25zdCBoYXNoSWR4ID0gbmV4dC5pbmRleE9mKCcjJyk7XG4gICAgICAgICAgdXJsID0gcmVzb2x2ZVVybChuZXh0LnN1YnN0cmluZygwLCBoYXNoSWR4KSArIG5leHQuc3Vic3RyaW5nKGhhc2hJZHggKyAxKSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgdGhyb3cgJ0ludmFsaWQgVVJMVHlwZSBwYXNzZWQgdG8gc2V0VXBMb2NhdGlvblN5bmM6ICcgKyB1cmxUeXBlO1xuICAgICAgICB9XG4gICAgICAgIGNvbnN0IHBhdGggPSBsb2NhdGlvbi5ub3JtYWxpemUodXJsLnBhdGhuYW1lKTtcbiAgICAgICAgcm91dGVyLm5hdmlnYXRlQnlVcmwocGF0aCArIHVybC5zZWFyY2ggKyB1cmwuaGFzaCk7XG4gICAgICB9KTtcbn1cblxuLyoqXG4gKiBOb3JtYWxpemVzIGFuZCBwYXJzZXMgYSBVUkwuXG4gKlxuICogLSBOb3JtYWxpemluZyBtZWFucyB0aGF0IGEgcmVsYXRpdmUgVVJMIHdpbGwgYmUgcmVzb2x2ZWQgaW50byBhbiBhYnNvbHV0ZSBVUkwgaW4gdGhlIGNvbnRleHQgb2ZcbiAqICAgdGhlIGFwcGxpY2F0aW9uIGRvY3VtZW50LlxuICogLSBQYXJzaW5nIG1lYW5zIHRoYXQgdGhlIGFuY2hvcidzIGBwcm90b2NvbGAsIGBob3N0bmFtZWAsIGBwb3J0YCwgYHBhdGhuYW1lYCBhbmQgcmVsYXRlZFxuICogICBwcm9wZXJ0aWVzIGFyZSBhbGwgcG9wdWxhdGVkIHRvIHJlZmxlY3QgdGhlIG5vcm1hbGl6ZWQgVVJMLlxuICpcbiAqIFdoaWxlIHRoaXMgYXBwcm9hY2ggaGFzIHdpZGUgY29tcGF0aWJpbGl0eSwgaXQgZG9lc24ndCB3b3JrIGFzIGV4cGVjdGVkIG9uIElFLiBPbiBJRSwgbm9ybWFsaXppbmdcbiAqIGhhcHBlbnMgc2ltaWxhciB0byBvdGhlciBicm93c2VycywgYnV0IHRoZSBwYXJzZWQgY29tcG9uZW50cyB3aWxsIG5vdCBiZSBzZXQuIChFLmcuIGlmIHlvdSBhc3NpZ25cbiAqIGBhLmhyZWYgPSAnZm9vJ2AsIHRoZW4gYGEucHJvdG9jb2xgLCBgYS5ob3N0YCwgZXRjLiB3aWxsIG5vdCBiZSBjb3JyZWN0bHkgdXBkYXRlZC4pXG4gKiBXZSB3b3JrIGFyb3VuZCB0aGF0IGJ5IHBlcmZvcm1pbmcgdGhlIHBhcnNpbmcgaW4gYSAybmQgc3RlcCBieSB0YWtpbmcgYSBwcmV2aW91c2x5IG5vcm1hbGl6ZWQgVVJMXG4gKiBhbmQgYXNzaWduaW5nIGl0IGFnYWluLiBUaGlzIGNvcnJlY3RseSBwb3B1bGF0ZXMgYWxsIHByb3BlcnRpZXMuXG4gKlxuICogU2VlXG4gKiBodHRwczovL2dpdGh1Yi5jb20vYW5ndWxhci9hbmd1bGFyLmpzL2Jsb2IvMmM3NDAwZTdkMDdiMGY2Y2VjMTgxN2RhYjQwYjkyNTBjZThlYmNlNi9zcmMvbmcvdXJsVXRpbHMuanMjTDI2LUwzM1xuICogZm9yIG1vcmUgaW5mby5cbiAqL1xubGV0IGFuY2hvcjogSFRNTEFuY2hvckVsZW1lbnR8dW5kZWZpbmVkO1xuZnVuY3Rpb24gcmVzb2x2ZVVybCh1cmw6IHN0cmluZyk6IHtwYXRobmFtZTogc3RyaW5nLCBzZWFyY2g6IHN0cmluZywgaGFzaDogc3RyaW5nfSB7XG4gIGlmICghYW5jaG9yKSB7XG4gICAgYW5jaG9yID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgnYScpO1xuICB9XG5cbiAgYW5jaG9yLnNldEF0dHJpYnV0ZSgnaHJlZicsIHVybCk7XG4gIGFuY2hvci5zZXRBdHRyaWJ1dGUoJ2hyZWYnLCBhbmNob3IuaHJlZik7XG5cbiAgcmV0dXJuIHtcbiAgICAvLyBJRSBkb2VzIG5vdCBzdGFydCBgcGF0aG5hbWVgIHdpdGggYC9gIGxpa2Ugb3RoZXIgYnJvd3NlcnMuXG4gICAgcGF0aG5hbWU6IGAvJHthbmNob3IucGF0aG5hbWUucmVwbGFjZSgvXlxcLy8sICcnKX1gLFxuICAgIHNlYXJjaDogYW5jaG9yLnNlYXJjaCxcbiAgICBoYXNoOiBhbmNob3IuaGFzaFxuICB9O1xufVxuIl19
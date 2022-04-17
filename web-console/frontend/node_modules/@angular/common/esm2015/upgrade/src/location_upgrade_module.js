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
import { APP_BASE_HREF, CommonModule, HashLocationStrategy, Location, LocationStrategy, PathLocationStrategy, PlatformLocation } from '@angular/common';
import { Inject, InjectionToken, NgModule, Optional } from '@angular/core';
import { UpgradeModule } from '@angular/upgrade/static';
import { $locationShim, $locationShimProvider } from './location_shim';
import { AngularJSUrlCodec, UrlCodec } from './params';
/**
 * Configuration options for LocationUpgrade.
 *
 * \@publicApi
 * @record
 */
export function LocationUpgradeConfig() { }
if (false) {
    /**
     * Configures whether the location upgrade module should use the `HashLocationStrategy`
     * or the `PathLocationStrategy`
     * @type {?|undefined}
     */
    LocationUpgradeConfig.prototype.useHash;
    /**
     * Configures the hash prefix used in the URL when using the `HashLocationStrategy`
     * @type {?|undefined}
     */
    LocationUpgradeConfig.prototype.hashPrefix;
    /**
     * Configures the URL codec for encoding and decoding URLs. Default is the `AngularJSCodec`
     * @type {?|undefined}
     */
    LocationUpgradeConfig.prototype.urlCodec;
    /**
     * Configures the base href when used in server-side rendered applications
     * @type {?|undefined}
     */
    LocationUpgradeConfig.prototype.serverBaseHref;
    /**
     * Configures the base href when used in client-side rendered applications
     * @type {?|undefined}
     */
    LocationUpgradeConfig.prototype.appBaseHref;
}
/**
 * A provider token used to configure the location upgrade module.
 *
 * \@publicApi
 * @type {?}
 */
export const LOCATION_UPGRADE_CONFIGURATION = new InjectionToken('LOCATION_UPGRADE_CONFIGURATION');
/** @type {?} */
const APP_BASE_HREF_RESOLVED = new InjectionToken('APP_BASE_HREF_RESOLVED');
/**
 * `NgModule` used for providing and configuring Angular's Unified Location Service for upgrading.
 *
 * @see [Using the Unified Angular Location Service](guide/upgrade#using-the-unified-angular-location-service)
 *
 * \@publicApi
 */
export class LocationUpgradeModule {
    /**
     * @param {?=} config
     * @return {?}
     */
    static config(config) {
        return {
            ngModule: LocationUpgradeModule,
            providers: [
                Location,
                {
                    provide: $locationShim,
                    useFactory: provide$location,
                    deps: [UpgradeModule, Location, PlatformLocation, UrlCodec, LocationStrategy]
                },
                { provide: LOCATION_UPGRADE_CONFIGURATION, useValue: config ? config : {} },
                { provide: UrlCodec, useFactory: provideUrlCodec, deps: [LOCATION_UPGRADE_CONFIGURATION] },
                {
                    provide: APP_BASE_HREF_RESOLVED,
                    useFactory: provideAppBaseHref,
                    deps: [LOCATION_UPGRADE_CONFIGURATION, [new Inject(APP_BASE_HREF), new Optional()]]
                },
                {
                    provide: LocationStrategy,
                    useFactory: provideLocationStrategy,
                    deps: [
                        PlatformLocation,
                        APP_BASE_HREF_RESOLVED,
                        LOCATION_UPGRADE_CONFIGURATION,
                    ]
                },
            ],
        };
    }
}
LocationUpgradeModule.decorators = [
    { type: NgModule, args: [{ imports: [CommonModule] },] }
];
/**
 * @param {?} config
 * @param {?=} appBaseHref
 * @return {?}
 */
export function provideAppBaseHref(config, appBaseHref) {
    if (config && config.appBaseHref != null) {
        return config.appBaseHref;
    }
    else if (appBaseHref != null) {
        return appBaseHref;
    }
    return '';
}
/**
 * @param {?} config
 * @return {?}
 */
export function provideUrlCodec(config) {
    /** @type {?} */
    const codec = config && config.urlCodec || AngularJSUrlCodec;
    return new ((/** @type {?} */ (codec)))();
}
/**
 * @param {?} platformLocation
 * @param {?} baseHref
 * @param {?=} options
 * @return {?}
 */
export function provideLocationStrategy(platformLocation, baseHref, options = {}) {
    return options.useHash ? new HashLocationStrategy(platformLocation, baseHref) :
        new PathLocationStrategy(platformLocation, baseHref);
}
/**
 * @param {?} ngUpgrade
 * @param {?} location
 * @param {?} platformLocation
 * @param {?} urlCodec
 * @param {?} locationStrategy
 * @return {?}
 */
export function provide$location(ngUpgrade, location, platformLocation, urlCodec, locationStrategy) {
    /** @type {?} */
    const $locationProvider = new $locationShimProvider(ngUpgrade, location, platformLocation, urlCodec, locationStrategy);
    return $locationProvider.$get();
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibG9jYXRpb25fdXBncmFkZV9tb2R1bGUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vdXBncmFkZS9zcmMvbG9jYXRpb25fdXBncmFkZV9tb2R1bGUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsYUFBYSxFQUFFLFlBQVksRUFBRSxvQkFBb0IsRUFBRSxRQUFRLEVBQUUsZ0JBQWdCLEVBQUUsb0JBQW9CLEVBQUUsZ0JBQWdCLEVBQUMsTUFBTSxpQkFBaUIsQ0FBQztBQUN0SixPQUFPLEVBQUMsTUFBTSxFQUFFLGNBQWMsRUFBdUIsUUFBUSxFQUFFLFFBQVEsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUM5RixPQUFPLEVBQUMsYUFBYSxFQUFDLE1BQU0seUJBQXlCLENBQUM7QUFFdEQsT0FBTyxFQUFDLGFBQWEsRUFBRSxxQkFBcUIsRUFBQyxNQUFNLGlCQUFpQixDQUFDO0FBQ3JFLE9BQU8sRUFBQyxpQkFBaUIsRUFBRSxRQUFRLEVBQUMsTUFBTSxVQUFVLENBQUM7Ozs7Ozs7QUFRckQsMkNBc0JDOzs7Ozs7O0lBakJDLHdDQUFrQjs7Ozs7SUFJbEIsMkNBQW9COzs7OztJQUlwQix5Q0FBMkI7Ozs7O0lBSTNCLCtDQUF3Qjs7Ozs7SUFJeEIsNENBQXFCOzs7Ozs7OztBQVF2QixNQUFNLE9BQU8sOEJBQThCLEdBQ3ZDLElBQUksY0FBYyxDQUF3QixnQ0FBZ0MsQ0FBQzs7TUFFekUsc0JBQXNCLEdBQUcsSUFBSSxjQUFjLENBQVMsd0JBQXdCLENBQUM7Ozs7Ozs7O0FBVW5GLE1BQU0sT0FBTyxxQkFBcUI7Ozs7O0lBQ2hDLE1BQU0sQ0FBQyxNQUFNLENBQUMsTUFBOEI7UUFDMUMsT0FBTztZQUNMLFFBQVEsRUFBRSxxQkFBcUI7WUFDL0IsU0FBUyxFQUFFO2dCQUNULFFBQVE7Z0JBQ1I7b0JBQ0UsT0FBTyxFQUFFLGFBQWE7b0JBQ3RCLFVBQVUsRUFBRSxnQkFBZ0I7b0JBQzVCLElBQUksRUFBRSxDQUFDLGFBQWEsRUFBRSxRQUFRLEVBQUUsZ0JBQWdCLEVBQUUsUUFBUSxFQUFFLGdCQUFnQixDQUFDO2lCQUM5RTtnQkFDRCxFQUFDLE9BQU8sRUFBRSw4QkFBOEIsRUFBRSxRQUFRLEVBQUUsTUFBTSxDQUFDLENBQUMsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLEVBQUUsRUFBQztnQkFDekUsRUFBQyxPQUFPLEVBQUUsUUFBUSxFQUFFLFVBQVUsRUFBRSxlQUFlLEVBQUUsSUFBSSxFQUFFLENBQUMsOEJBQThCLENBQUMsRUFBQztnQkFDeEY7b0JBQ0UsT0FBTyxFQUFFLHNCQUFzQjtvQkFDL0IsVUFBVSxFQUFFLGtCQUFrQjtvQkFDOUIsSUFBSSxFQUFFLENBQUMsOEJBQThCLEVBQUUsQ0FBQyxJQUFJLE1BQU0sQ0FBQyxhQUFhLENBQUMsRUFBRSxJQUFJLFFBQVEsRUFBRSxDQUFDLENBQUM7aUJBQ3BGO2dCQUNEO29CQUNFLE9BQU8sRUFBRSxnQkFBZ0I7b0JBQ3pCLFVBQVUsRUFBRSx1QkFBdUI7b0JBQ25DLElBQUksRUFBRTt3QkFDSixnQkFBZ0I7d0JBQ2hCLHNCQUFzQjt3QkFDdEIsOEJBQThCO3FCQUMvQjtpQkFDRjthQUNGO1NBQ0YsQ0FBQztJQUNKLENBQUM7OztZQTlCRixRQUFRLFNBQUMsRUFBQyxPQUFPLEVBQUUsQ0FBQyxZQUFZLENBQUMsRUFBQzs7Ozs7OztBQWlDbkMsTUFBTSxVQUFVLGtCQUFrQixDQUFDLE1BQTZCLEVBQUUsV0FBb0I7SUFDcEYsSUFBSSxNQUFNLElBQUksTUFBTSxDQUFDLFdBQVcsSUFBSSxJQUFJLEVBQUU7UUFDeEMsT0FBTyxNQUFNLENBQUMsV0FBVyxDQUFDO0tBQzNCO1NBQU0sSUFBSSxXQUFXLElBQUksSUFBSSxFQUFFO1FBQzlCLE9BQU8sV0FBVyxDQUFDO0tBQ3BCO0lBQ0QsT0FBTyxFQUFFLENBQUM7QUFDWixDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxlQUFlLENBQUMsTUFBNkI7O1VBQ3JELEtBQUssR0FBRyxNQUFNLElBQUksTUFBTSxDQUFDLFFBQVEsSUFBSSxpQkFBaUI7SUFDNUQsT0FBTyxJQUFJLENBQUMsbUJBQUEsS0FBSyxFQUFPLENBQUMsRUFBRSxDQUFDO0FBQzlCLENBQUM7Ozs7Ozs7QUFFRCxNQUFNLFVBQVUsdUJBQXVCLENBQ25DLGdCQUFrQyxFQUFFLFFBQWdCLEVBQUUsVUFBaUMsRUFBRTtJQUMzRixPQUFPLE9BQU8sQ0FBQyxPQUFPLENBQUMsQ0FBQyxDQUFDLElBQUksb0JBQW9CLENBQUMsZ0JBQWdCLEVBQUUsUUFBUSxDQUFDLENBQUMsQ0FBQztRQUN0RCxJQUFJLG9CQUFvQixDQUFDLGdCQUFnQixFQUFFLFFBQVEsQ0FBQyxDQUFDO0FBQ2hGLENBQUM7Ozs7Ozs7OztBQUVELE1BQU0sVUFBVSxnQkFBZ0IsQ0FDNUIsU0FBd0IsRUFBRSxRQUFrQixFQUFFLGdCQUFrQyxFQUNoRixRQUFrQixFQUFFLGdCQUFrQzs7VUFDbEQsaUJBQWlCLEdBQ25CLElBQUkscUJBQXFCLENBQUMsU0FBUyxFQUFFLFFBQVEsRUFBRSxnQkFBZ0IsRUFBRSxRQUFRLEVBQUUsZ0JBQWdCLENBQUM7SUFFaEcsT0FBTyxpQkFBaUIsQ0FBQyxJQUFJLEVBQUUsQ0FBQztBQUNsQyxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0FQUF9CQVNFX0hSRUYsIENvbW1vbk1vZHVsZSwgSGFzaExvY2F0aW9uU3RyYXRlZ3ksIExvY2F0aW9uLCBMb2NhdGlvblN0cmF0ZWd5LCBQYXRoTG9jYXRpb25TdHJhdGVneSwgUGxhdGZvcm1Mb2NhdGlvbn0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7SW5qZWN0LCBJbmplY3Rpb25Ub2tlbiwgTW9kdWxlV2l0aFByb3ZpZGVycywgTmdNb2R1bGUsIE9wdGlvbmFsfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7VXBncmFkZU1vZHVsZX0gZnJvbSAnQGFuZ3VsYXIvdXBncmFkZS9zdGF0aWMnO1xuXG5pbXBvcnQgeyRsb2NhdGlvblNoaW0sICRsb2NhdGlvblNoaW1Qcm92aWRlcn0gZnJvbSAnLi9sb2NhdGlvbl9zaGltJztcbmltcG9ydCB7QW5ndWxhckpTVXJsQ29kZWMsIFVybENvZGVjfSBmcm9tICcuL3BhcmFtcyc7XG5cblxuLyoqXG4gKiBDb25maWd1cmF0aW9uIG9wdGlvbnMgZm9yIExvY2F0aW9uVXBncmFkZS5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgTG9jYXRpb25VcGdyYWRlQ29uZmlnIHtcbiAgLyoqXG4gICAqIENvbmZpZ3VyZXMgd2hldGhlciB0aGUgbG9jYXRpb24gdXBncmFkZSBtb2R1bGUgc2hvdWxkIHVzZSB0aGUgYEhhc2hMb2NhdGlvblN0cmF0ZWd5YFxuICAgKiBvciB0aGUgYFBhdGhMb2NhdGlvblN0cmF0ZWd5YFxuICAgKi9cbiAgdXNlSGFzaD86IGJvb2xlYW47XG4gIC8qKlxuICAgKiBDb25maWd1cmVzIHRoZSBoYXNoIHByZWZpeCB1c2VkIGluIHRoZSBVUkwgd2hlbiB1c2luZyB0aGUgYEhhc2hMb2NhdGlvblN0cmF0ZWd5YFxuICAgKi9cbiAgaGFzaFByZWZpeD86IHN0cmluZztcbiAgLyoqXG4gICAqIENvbmZpZ3VyZXMgdGhlIFVSTCBjb2RlYyBmb3IgZW5jb2RpbmcgYW5kIGRlY29kaW5nIFVSTHMuIERlZmF1bHQgaXMgdGhlIGBBbmd1bGFySlNDb2RlY2BcbiAgICovXG4gIHVybENvZGVjPzogdHlwZW9mIFVybENvZGVjO1xuICAvKipcbiAgICogQ29uZmlndXJlcyB0aGUgYmFzZSBocmVmIHdoZW4gdXNlZCBpbiBzZXJ2ZXItc2lkZSByZW5kZXJlZCBhcHBsaWNhdGlvbnNcbiAgICovXG4gIHNlcnZlckJhc2VIcmVmPzogc3RyaW5nO1xuICAvKipcbiAgICogQ29uZmlndXJlcyB0aGUgYmFzZSBocmVmIHdoZW4gdXNlZCBpbiBjbGllbnQtc2lkZSByZW5kZXJlZCBhcHBsaWNhdGlvbnNcbiAgICovXG4gIGFwcEJhc2VIcmVmPzogc3RyaW5nO1xufVxuXG4vKipcbiAqIEEgcHJvdmlkZXIgdG9rZW4gdXNlZCB0byBjb25maWd1cmUgdGhlIGxvY2F0aW9uIHVwZ3JhZGUgbW9kdWxlLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNvbnN0IExPQ0FUSU9OX1VQR1JBREVfQ09ORklHVVJBVElPTiA9XG4gICAgbmV3IEluamVjdGlvblRva2VuPExvY2F0aW9uVXBncmFkZUNvbmZpZz4oJ0xPQ0FUSU9OX1VQR1JBREVfQ09ORklHVVJBVElPTicpO1xuXG5jb25zdCBBUFBfQkFTRV9IUkVGX1JFU09MVkVEID0gbmV3IEluamVjdGlvblRva2VuPHN0cmluZz4oJ0FQUF9CQVNFX0hSRUZfUkVTT0xWRUQnKTtcblxuLyoqXG4gKiBgTmdNb2R1bGVgIHVzZWQgZm9yIHByb3ZpZGluZyBhbmQgY29uZmlndXJpbmcgQW5ndWxhcidzIFVuaWZpZWQgTG9jYXRpb24gU2VydmljZSBmb3IgdXBncmFkaW5nLlxuICogXG4gKiBAc2VlIFtVc2luZyB0aGUgVW5pZmllZCBBbmd1bGFyIExvY2F0aW9uIFNlcnZpY2VdKGd1aWRlL3VwZ3JhZGUjdXNpbmctdGhlLXVuaWZpZWQtYW5ndWxhci1sb2NhdGlvbi1zZXJ2aWNlKVxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuQE5nTW9kdWxlKHtpbXBvcnRzOiBbQ29tbW9uTW9kdWxlXX0pXG5leHBvcnQgY2xhc3MgTG9jYXRpb25VcGdyYWRlTW9kdWxlIHtcbiAgc3RhdGljIGNvbmZpZyhjb25maWc/OiBMb2NhdGlvblVwZ3JhZGVDb25maWcpOiBNb2R1bGVXaXRoUHJvdmlkZXJzPExvY2F0aW9uVXBncmFkZU1vZHVsZT4ge1xuICAgIHJldHVybiB7XG4gICAgICBuZ01vZHVsZTogTG9jYXRpb25VcGdyYWRlTW9kdWxlLFxuICAgICAgcHJvdmlkZXJzOiBbXG4gICAgICAgIExvY2F0aW9uLFxuICAgICAgICB7XG4gICAgICAgICAgcHJvdmlkZTogJGxvY2F0aW9uU2hpbSxcbiAgICAgICAgICB1c2VGYWN0b3J5OiBwcm92aWRlJGxvY2F0aW9uLFxuICAgICAgICAgIGRlcHM6IFtVcGdyYWRlTW9kdWxlLCBMb2NhdGlvbiwgUGxhdGZvcm1Mb2NhdGlvbiwgVXJsQ29kZWMsIExvY2F0aW9uU3RyYXRlZ3ldXG4gICAgICAgIH0sXG4gICAgICAgIHtwcm92aWRlOiBMT0NBVElPTl9VUEdSQURFX0NPTkZJR1VSQVRJT04sIHVzZVZhbHVlOiBjb25maWcgPyBjb25maWcgOiB7fX0sXG4gICAgICAgIHtwcm92aWRlOiBVcmxDb2RlYywgdXNlRmFjdG9yeTogcHJvdmlkZVVybENvZGVjLCBkZXBzOiBbTE9DQVRJT05fVVBHUkFERV9DT05GSUdVUkFUSU9OXX0sXG4gICAgICAgIHtcbiAgICAgICAgICBwcm92aWRlOiBBUFBfQkFTRV9IUkVGX1JFU09MVkVELFxuICAgICAgICAgIHVzZUZhY3Rvcnk6IHByb3ZpZGVBcHBCYXNlSHJlZixcbiAgICAgICAgICBkZXBzOiBbTE9DQVRJT05fVVBHUkFERV9DT05GSUdVUkFUSU9OLCBbbmV3IEluamVjdChBUFBfQkFTRV9IUkVGKSwgbmV3IE9wdGlvbmFsKCldXVxuICAgICAgICB9LFxuICAgICAgICB7XG4gICAgICAgICAgcHJvdmlkZTogTG9jYXRpb25TdHJhdGVneSxcbiAgICAgICAgICB1c2VGYWN0b3J5OiBwcm92aWRlTG9jYXRpb25TdHJhdGVneSxcbiAgICAgICAgICBkZXBzOiBbXG4gICAgICAgICAgICBQbGF0Zm9ybUxvY2F0aW9uLFxuICAgICAgICAgICAgQVBQX0JBU0VfSFJFRl9SRVNPTFZFRCxcbiAgICAgICAgICAgIExPQ0FUSU9OX1VQR1JBREVfQ09ORklHVVJBVElPTixcbiAgICAgICAgICBdXG4gICAgICAgIH0sXG4gICAgICBdLFxuICAgIH07XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHByb3ZpZGVBcHBCYXNlSHJlZihjb25maWc6IExvY2F0aW9uVXBncmFkZUNvbmZpZywgYXBwQmFzZUhyZWY/OiBzdHJpbmcpIHtcbiAgaWYgKGNvbmZpZyAmJiBjb25maWcuYXBwQmFzZUhyZWYgIT0gbnVsbCkge1xuICAgIHJldHVybiBjb25maWcuYXBwQmFzZUhyZWY7XG4gIH0gZWxzZSBpZiAoYXBwQmFzZUhyZWYgIT0gbnVsbCkge1xuICAgIHJldHVybiBhcHBCYXNlSHJlZjtcbiAgfVxuICByZXR1cm4gJyc7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBwcm92aWRlVXJsQ29kZWMoY29uZmlnOiBMb2NhdGlvblVwZ3JhZGVDb25maWcpIHtcbiAgY29uc3QgY29kZWMgPSBjb25maWcgJiYgY29uZmlnLnVybENvZGVjIHx8IEFuZ3VsYXJKU1VybENvZGVjO1xuICByZXR1cm4gbmV3IChjb2RlYyBhcyBhbnkpKCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBwcm92aWRlTG9jYXRpb25TdHJhdGVneShcbiAgICBwbGF0Zm9ybUxvY2F0aW9uOiBQbGF0Zm9ybUxvY2F0aW9uLCBiYXNlSHJlZjogc3RyaW5nLCBvcHRpb25zOiBMb2NhdGlvblVwZ3JhZGVDb25maWcgPSB7fSkge1xuICByZXR1cm4gb3B0aW9ucy51c2VIYXNoID8gbmV3IEhhc2hMb2NhdGlvblN0cmF0ZWd5KHBsYXRmb3JtTG9jYXRpb24sIGJhc2VIcmVmKSA6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICBuZXcgUGF0aExvY2F0aW9uU3RyYXRlZ3kocGxhdGZvcm1Mb2NhdGlvbiwgYmFzZUhyZWYpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gcHJvdmlkZSRsb2NhdGlvbihcbiAgICBuZ1VwZ3JhZGU6IFVwZ3JhZGVNb2R1bGUsIGxvY2F0aW9uOiBMb2NhdGlvbiwgcGxhdGZvcm1Mb2NhdGlvbjogUGxhdGZvcm1Mb2NhdGlvbixcbiAgICB1cmxDb2RlYzogVXJsQ29kZWMsIGxvY2F0aW9uU3RyYXRlZ3k6IExvY2F0aW9uU3RyYXRlZ3kpIHtcbiAgY29uc3QgJGxvY2F0aW9uUHJvdmlkZXIgPVxuICAgICAgbmV3ICRsb2NhdGlvblNoaW1Qcm92aWRlcihuZ1VwZ3JhZGUsIGxvY2F0aW9uLCBwbGF0Zm9ybUxvY2F0aW9uLCB1cmxDb2RlYywgbG9jYXRpb25TdHJhdGVneSk7XG5cbiAgcmV0dXJuICRsb2NhdGlvblByb3ZpZGVyLiRnZXQoKTtcbn0iXX0=
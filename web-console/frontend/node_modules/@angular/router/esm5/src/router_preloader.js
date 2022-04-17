/**
*@license
*Copyright Google Inc. All Rights Reserved.
*
*Use of this source code is governed by an MIT-style license that can be
*found in the LICENSE file at https://angular.io/license
*/
import * as tslib_1 from "tslib";
import { Compiler, Injectable, Injector, NgModuleFactoryLoader, NgModuleRef } from '@angular/core';
import { from, of } from 'rxjs';
import { catchError, concatMap, filter, map, mergeAll, mergeMap } from 'rxjs/operators';
import { NavigationEnd, RouteConfigLoadEnd, RouteConfigLoadStart } from './events';
import { Router } from './router';
import { RouterConfigLoader } from './router_config_loader';
/**
 * @description
 *
 * Provides a preloading strategy.
 *
 * @publicApi
 */
var PreloadingStrategy = /** @class */ (function () {
    function PreloadingStrategy() {
    }
    return PreloadingStrategy;
}());
export { PreloadingStrategy };
/**
 * @description
 *
 * Provides a preloading strategy that preloads all modules as quickly as possible.
 *
 * ```
 * RouteModule.forRoot(ROUTES, {preloadingStrategy: PreloadAllModules})
 * ```
 *
 * @publicApi
 */
var PreloadAllModules = /** @class */ (function () {
    function PreloadAllModules() {
    }
    PreloadAllModules.prototype.preload = function (route, fn) {
        return fn().pipe(catchError(function () { return of(null); }));
    };
    return PreloadAllModules;
}());
export { PreloadAllModules };
/**
 * @description
 *
 * Provides a preloading strategy that does not preload any modules.
 *
 * This strategy is enabled by default.
 *
 * @publicApi
 */
var NoPreloading = /** @class */ (function () {
    function NoPreloading() {
    }
    NoPreloading.prototype.preload = function (route, fn) { return of(null); };
    return NoPreloading;
}());
export { NoPreloading };
/**
 * The preloader optimistically loads all router configurations to
 * make navigations into lazily-loaded sections of the application faster.
 *
 * The preloader runs in the background. When the router bootstraps, the preloader
 * starts listening to all navigation events. After every such event, the preloader
 * will check if any configurations can be loaded lazily.
 *
 * If a route is protected by `canLoad` guards, the preloaded will not load it.
 *
 * @publicApi
 */
var RouterPreloader = /** @class */ (function () {
    function RouterPreloader(router, moduleLoader, compiler, injector, preloadingStrategy) {
        this.router = router;
        this.injector = injector;
        this.preloadingStrategy = preloadingStrategy;
        var onStartLoad = function (r) { return router.triggerEvent(new RouteConfigLoadStart(r)); };
        var onEndLoad = function (r) { return router.triggerEvent(new RouteConfigLoadEnd(r)); };
        this.loader = new RouterConfigLoader(moduleLoader, compiler, onStartLoad, onEndLoad);
    }
    RouterPreloader.prototype.setUpPreloading = function () {
        var _this = this;
        this.subscription =
            this.router.events
                .pipe(filter(function (e) { return e instanceof NavigationEnd; }), concatMap(function () { return _this.preload(); }))
                .subscribe(function () { });
    };
    RouterPreloader.prototype.preload = function () {
        var ngModule = this.injector.get(NgModuleRef);
        return this.processRoutes(ngModule, this.router.config);
    };
    // TODO(jasonaden): This class relies on code external to the class to call setUpPreloading. If
    // this hasn't been done, ngOnDestroy will fail as this.subscription will be undefined. This
    // should be refactored.
    RouterPreloader.prototype.ngOnDestroy = function () { this.subscription.unsubscribe(); };
    RouterPreloader.prototype.processRoutes = function (ngModule, routes) {
        var e_1, _a;
        var res = [];
        try {
            for (var routes_1 = tslib_1.__values(routes), routes_1_1 = routes_1.next(); !routes_1_1.done; routes_1_1 = routes_1.next()) {
                var route = routes_1_1.value;
                // we already have the config loaded, just recurse
                if (route.loadChildren && !route.canLoad && route._loadedConfig) {
                    var childConfig = route._loadedConfig;
                    res.push(this.processRoutes(childConfig.module, childConfig.routes));
                    // no config loaded, fetch the config
                }
                else if (route.loadChildren && !route.canLoad) {
                    res.push(this.preloadConfig(ngModule, route));
                    // recurse into children
                }
                else if (route.children) {
                    res.push(this.processRoutes(ngModule, route.children));
                }
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (routes_1_1 && !routes_1_1.done && (_a = routes_1.return)) _a.call(routes_1);
            }
            finally { if (e_1) throw e_1.error; }
        }
        return from(res).pipe(mergeAll(), map(function (_) { return void 0; }));
    };
    RouterPreloader.prototype.preloadConfig = function (ngModule, route) {
        var _this = this;
        return this.preloadingStrategy.preload(route, function () {
            var loaded$ = _this.loader.load(ngModule.injector, route);
            return loaded$.pipe(mergeMap(function (config) {
                route._loadedConfig = config;
                return _this.processRoutes(config.module, config.routes);
            }));
        });
    };
    RouterPreloader = tslib_1.__decorate([
        Injectable(),
        tslib_1.__metadata("design:paramtypes", [Router, NgModuleFactoryLoader, Compiler,
            Injector, PreloadingStrategy])
    ], RouterPreloader);
    return RouterPreloader;
}());
export { RouterPreloader };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicm91dGVyX3ByZWxvYWRlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3JvdXRlci9zcmMvcm91dGVyX3ByZWxvYWRlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0VBTUU7O0FBRUYsT0FBTyxFQUFDLFFBQVEsRUFBRSxVQUFVLEVBQUUsUUFBUSxFQUFFLHFCQUFxQixFQUFFLFdBQVcsRUFBWSxNQUFNLGVBQWUsQ0FBQztBQUM1RyxPQUFPLEVBQTJCLElBQUksRUFBRSxFQUFFLEVBQUUsTUFBTSxNQUFNLENBQUM7QUFDekQsT0FBTyxFQUFDLFVBQVUsRUFBRSxTQUFTLEVBQUUsTUFBTSxFQUFFLEdBQUcsRUFBRSxRQUFRLEVBQUUsUUFBUSxFQUFDLE1BQU0sZ0JBQWdCLENBQUM7QUFHdEYsT0FBTyxFQUFRLGFBQWEsRUFBRSxrQkFBa0IsRUFBRSxvQkFBb0IsRUFBQyxNQUFNLFVBQVUsQ0FBQztBQUN4RixPQUFPLEVBQUMsTUFBTSxFQUFDLE1BQU0sVUFBVSxDQUFDO0FBQ2hDLE9BQU8sRUFBQyxrQkFBa0IsRUFBQyxNQUFNLHdCQUF3QixDQUFDO0FBRzFEOzs7Ozs7R0FNRztBQUNIO0lBQUE7SUFFQSxDQUFDO0lBQUQseUJBQUM7QUFBRCxDQUFDLEFBRkQsSUFFQzs7QUFFRDs7Ozs7Ozs7OztHQVVHO0FBQ0g7SUFBQTtJQUlBLENBQUM7SUFIQyxtQ0FBTyxHQUFQLFVBQVEsS0FBWSxFQUFFLEVBQXlCO1FBQzdDLE9BQU8sRUFBRSxFQUFFLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxjQUFNLE9BQUEsRUFBRSxDQUFFLElBQUksQ0FBQyxFQUFULENBQVMsQ0FBQyxDQUFDLENBQUM7SUFDaEQsQ0FBQztJQUNILHdCQUFDO0FBQUQsQ0FBQyxBQUpELElBSUM7O0FBRUQ7Ozs7Ozs7O0dBUUc7QUFDSDtJQUFBO0lBRUEsQ0FBQztJQURDLDhCQUFPLEdBQVAsVUFB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eEQ7YUFDRjs7Ozs7Ozs7O1FBQ0QsT0FBTyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsSUFBSSxDQUFDLFFBQVEsRUFBRSxFQUFFLEdBQUcsQ0FBQyxVQUFDLENBQUMsSUFBSyxPQUFBLEtBQUssQ0FBQyxFQUFOLENBQU0sQ0FBQyxDQUFDLENBQUM7SUFDeEQsQ0FBQztJQUVPLHVDQUFhLEdBQXJCLFVBQXNCLFFBQTBCLEVBQUUsS0FBWTtRQUE5RCxpQkFRQztRQVBDLE9BQU8sSUFBSSxDQUFDLGtCQUFrQixDQUFDLE9BQU8sQ0FBQyxLQUFLLEVBQUU7WUFDNUMsSUFBTSxPQUFPLEdBQUcsS0FBSSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLFFBQVEsRUFBRSxLQUFLLENBQUMsQ0FBQztZQUMzRCxPQUFPLE9BQU8sQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLFVBQUMsTUFBMEI7Z0JBQ3RELEtBQUssQ0FBQyxhQUFhLEdBQUcsTUFBTSxDQUFDO2dCQUM3QixPQUFPLEtBQUksQ0FBQyxhQUFhLENBQUMsTUFBTSxDQUFDLE1BQU0sRUFBRSxNQUFNLENBQUMsTUFBTSxDQUFDLENBQUM7WUFDMUQsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUNOLENBQUMsQ0FBQyxDQUFDO0lBQ0wsQ0FBQztJQTNEVSxlQUFlO1FBRDNCLFVBQVUsRUFBRTtpREFPUyxNQUFNLEVBQWdCLHFCQUFxQixFQUFZLFFBQVE7WUFDN0QsUUFBUSxFQUE4QixrQkFBa0I7T0FQbkUsZUFBZSxDQTREM0I7SUFBRCxzQkFBQztDQUFBLEFBNURELElBNERDO1NBNURZLGVBQWUiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbipAbGljZW5zZVxuKkNvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuKlxuKlVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4qZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuKi9cblxuaW1wb3J0IHtDb21waWxlciwgSW5qZWN0YWJsZSwgSW5qZWN0b3IsIE5nTW9kdWxlRmFjdG9yeUxvYWRlciwgTmdNb2R1bGVSZWYsIE9uRGVzdHJveX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQge09ic2VydmFibGUsIFN1YnNjcmlwdGlvbiwgZnJvbSwgb2YgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7Y2F0Y2hFcnJvciwgY29uY2F0TWFwLCBmaWx0ZXIsIG1hcCwgbWVyZ2VBbGwsIG1lcmdlTWFwfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5cbmltcG9ydCB7TG9hZGVkUm91dGVyQ29uZmlnLCBSb3V0ZSwgUm91dGVzfSBmcm9tICcuL2NvbmZpZyc7XG5pbXBvcnQge0V2ZW50LCBOYXZpZ2F0aW9uRW5kLCBSb3V0ZUNvbmZpZ0xvYWRFbmQsIFJvdXRlQ29uZmlnTG9hZFN0YXJ0fSBmcm9tICcuL2V2ZW50cyc7XG5pbXBvcnQge1JvdXRlcn0gZnJvbSAnLi9yb3V0ZXInO1xuaW1wb3J0IHtSb3V0ZXJDb25maWdMb2FkZXJ9IGZyb20gJy4vcm91dGVyX2NvbmZpZ19sb2FkZXInO1xuXG5cbi8qKlxuICogQGRlc2NyaXB0aW9uXG4gKlxuICogUHJvdmlkZXMgYSBwcmVsb2FkaW5nIHN0cmF0ZWd5LlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIFByZWxvYWRpbmdTdHJhdGVneSB7XG4gIGFic3RyYWN0IHByZWxvYWQocm91dGU6IFJvdXRlLCBmbjogKCkgPT4gT2JzZXJ2YWJsZTxhbnk+KTogT2JzZXJ2YWJsZTxhbnk+O1xufVxuXG4vKipcbiAqIEBkZXNjcmlwdGlvblxuICpcbiAqIFByb3ZpZGVzIGEgcHJlbG9hZGluZyBzdHJhdGVneSB0aGF0IHByZWxvYWRzIGFsbCBtb2R1bGVzIGFzIHF1aWNrbHkgYXMgcG9zc2libGUuXG4gKlxuICogYGBgXG4gKiBSb3V0ZU1vZHVsZS5mb3JSb290KFJPVVRFUywge3ByZWxvYWRpbmdTdHJhdGVneTogUHJlbG9hZEFsbE1vZHVsZXN9KVxuICogYGBgXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgUHJlbG9hZEFsbE1vZHVsZXMgaW1wbGVtZW50cyBQcmVsb2FkaW5nU3RyYXRlZ3kge1xuICBwcmVsb2FkKHJvdXRlOiBSb3V0ZSwgZm46ICgpID0+IE9ic2VydmFibGU8YW55Pik6IE9ic2VydmFibGU8YW55PiB7XG4gICAgcmV0dXJuIGZuKCkucGlwZShjYXRjaEVycm9yKCgpID0+IG9mIChudWxsKSkpO1xuICB9XG59XG5cbi8qKlxuICogQGRlc2NyaXB0aW9uXG4gKlxuICogUHJvdmlkZXMgYSBwcmVsb2FkaW5nIHN0cmF0ZWd5IHRoYXQgZG9lcyBub3QgcHJlbG9hZCBhbnkgbW9kdWxlcy5cbiAqXG4gKiBUaGlzIHN0cmF0ZWd5IGlzIGVuYWJsZWQgYnkgZGVmYXVsdC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBOb1ByZWxvYWRpbmcgaW1wbGVtZW50cyBQcmVsb2FkaW5nU3RyYXRlZ3kge1xuICBwcmVsb2FkKHJvdXRlOiBSb3V0ZSwgZm46ICgpID0+IE9ic2VydmFibGU8YW55Pik6IE9ic2VydmFibGU8YW55PiB7IHJldHVybiBvZiAobnVsbCk7IH1cbn1cblxuLyoqXG4gKiBUaGUgcHJlbG9hZGVyIG9wdGltaXN0aWNhbGx5IGxvYWRzIGFsbCByb3V0ZXIgY29uZmlndXJhdGlvbnMgdG9cbiAqIG1ha2UgbmF2aWdhdGlvbnMgaW50byBsYXppbHktbG9hZGVkIHNlY3Rpb25zIG9mIHRoZSBhcHBsaWNhdGlvbiBmYXN0ZXIuXG4gKlxuICogVGhlIHByZWxvYWRlciBydW5zIGluIHRoZSBiYWNrZ3JvdW5kLiBXaGVuIHRoZSByb3V0ZXIgYm9vdHN0cmFwcywgdGhlIHByZWxvYWRlclxuICogc3RhcnRzIGxpc3RlbmluZyB0byBhbGwgbmF2aWdhdGlvbiBldmVudHMuIEFmdGVyIGV2ZXJ5IHN1Y2ggZXZlbnQsIHRoZSBwcmVsb2FkZXJcbiAqIHdpbGwgY2hlY2sgaWYgYW55IGNvbmZpZ3VyYXRpb25zIGNhbiBiZSBsb2FkZWQgbGF6aWx5LlxuICpcbiAqIElmIGEgcm91dGUgaXMgcHJvdGVjdGVkIGJ5IGBjYW5Mb2FkYCBndWFyZHMsIHRoZSBwcmVsb2FkZWQgd2lsbCBub3QgbG9hZCBpdC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBSb3V0ZXJQcmVsb2FkZXIgaW1wbGVtZW50cyBPbkRlc3Ryb3kge1xuICBwcml2YXRlIGxvYWRlcjogUm91dGVyQ29uZmlnTG9hZGVyO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBzdWJzY3JpcHRpb24gITogU3Vic2NyaXB0aW9uO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHJpdmF0ZSByb3V0ZXI6IFJvdXRlciwgbW9kdWxlTG9hZGVyOiBOZ01vZHVsZUZhY3RvcnlMb2FkZXIsIGNvbXBpbGVyOiBDb21waWxlcixcbiAgICAgIHByaXZhdGUgaW5qZWN0b3I6IEluamVjdG9yLCBwcml2YXRlIHByZWxvYWRpbmdTdHJhdGVneTogUHJlbG9hZGluZ1N0cmF0ZWd5KSB7XG4gICAgY29uc3Qgb25TdGFydExvYWQgPSAocjogUm91dGUpID0+IHJvdXRlci50cmlnZ2VyRXZlbnQobmV3IFJvdXRlQ29uZmlnTG9hZFN0YXJ0KHIpKTtcbiAgICBjb25zdCBvbkVuZExvYWQgPSAocjogUm91dGUpID0+IHJvdXRlci50cmlnZ2VyRXZlbnQobmV3IFJvdXRlQ29uZmlnTG9hZEVuZChyKSk7XG5cbiAgICB0aGlzLmxvYWRlciA9IG5ldyBSb3V0ZXJDb25maWdMb2FkZXIobW9kdWxlTG9hZGVyLCBjb21waWxlciwgb25TdGFydExvYWQsIG9uRW5kTG9hZCk7XG4gIH1cblxuICBzZXRVcFByZWxvYWRpbmcoKTogdm9pZCB7XG4gICAgdGhpcy5zdWJzY3JpcHRpb24gPVxuICAgICAgICB0aGlzLnJvdXRlci5ldmVudHNcbiAgICAgICAgICAgIC5waXBlKGZpbHRlcigoZTogRXZlbnQpID0+IGUgaW5zdGFuY2VvZiBOYXZpZ2F0aW9uRW5kKSwgY29uY2F0TWFwKCgpID0+IHRoaXMucHJlbG9hZCgpKSlcbiAgICAgICAgICAgIC5zdWJzY3JpYmUoKCkgPT4ge30pO1xuICB9XG5cbiAgcHJlbG9hZCgpOiBPYnNlcnZhYmxlPGFueT4ge1xuICAgIGNvbnN0IG5nTW9kdWxlID0gdGhpcy5pbmplY3Rvci5nZXQoTmdNb2R1bGVSZWYpO1xuICAgIHJldHVybiB0aGlzLnByb2Nlc3NSb3V0ZXMobmdNb2R1bGUsIHRoaXMucm91dGVyLmNvbmZpZyk7XG4gIH1cblxuICAvLyBUT0RPKGphc29uYWRlbik6IFRoaXMgY2xhc3MgcmVsaWVzIG9uIGNvZGUgZXh0ZXJuYWwgdG8gdGhlIGNsYXNzIHRvIGNhbGwgc2V0VXBQcmVsb2FkaW5nLiBJZlxuICAvLyB0aGlzIGhhc24ndCBiZWVuIGRvbmUsIG5nT25EZXN0cm95IHdpbGwgZmFpbCBhcyB0aGlzLnN1YnNjcmlwdGlvbiB3aWxsIGJlIHVuZGVmaW5lZC4gVGhpc1xuICAvLyBzaG91bGQgYmUgcmVmYWN0b3JlZC5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7IHRoaXMuc3Vic2NyaXB0aW9uLnVuc3Vic2NyaWJlKCk7IH1cblxuICBwcml2YXRlIHByb2Nlc3NSb3V0ZXMobmdNb2R1bGU6IE5nTW9kdWxlUmVmPGFueT4sIHJvdXRlczogUm91dGVzKTogT2JzZXJ2YWJsZTx2b2lkPiB7XG4gICAgY29uc3QgcmVzOiBPYnNlcnZhYmxlPGFueT5bXSA9IFtdO1xuICAgIGZvciAoY29uc3Qgcm91dGUgb2Ygcm91dGVzKSB7XG4gICAgICAvLyB3ZSBhbHJlYWR5IGhhdmUgdGhlIGNvbmZpZyBsb2FkZWQsIGp1c3QgcmVjdXJzZVxuICAgICAgaWYgKHJvdXRlLmxvYWRDaGlsZHJlbiAmJiAhcm91dGUuY2FuTG9hZCAmJiByb3V0ZS5fbG9hZGVkQ29uZmlnKSB7XG4gICAgICAgIGNvbnN0IGNoaWxkQ29uZmlnID0gcm91dGUuX2xvYWRlZENvbmZpZztcbiAgICAgICAgcmVzLnB1c2godGhpcy5wcm9jZXNzUm91dGVzKGNoaWxkQ29uZmlnLm1vZHVsZSwgY2hpbGRDb25maWcucm91dGVzKSk7XG5cbiAgICAgICAgLy8gbm8gY29uZmlnIGxvYWRlZCwgZmV0Y2ggdGhlIGNvbmZpZ1xuICAgICAgfSBlbHNlIGlmIChyb3V0ZS5sb2FkQ2hpbGRyZW4gJiYgIXJvdXRlLmNhbkxvYWQpIHtcbiAgICAgICAgcmVzLnB1c2godGhpcy5wcmVsb2FkQ29uZmlnKG5nTW9kdWxlLCByb3V0ZSkpO1xuXG4gICAgICAgIC8vIHJlY3Vyc2UgaW50byBjaGlsZHJlblxuICAgICAgfSBlbHNlIGlmIChyb3V0ZS5jaGlsZHJlbikge1xuICAgICAgICByZXMucHVzaCh0aGlzLnByb2Nlc3NSb3V0ZXMobmdNb2R1bGUsIHJvdXRlLmNoaWxkcmVuKSk7XG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiBmcm9tKHJlcykucGlwZShtZXJnZUFsbCgpLCBtYXAoKF8pID0+IHZvaWQgMCkpO1xuICB9XG5cbiAgcHJpdmF0ZSBwcmVsb2FkQ29uZmlnKG5nTW9kdWxlOiBOZ01vZHVsZVJlZjxhbnk+LCByb3V0ZTogUm91dGUpOiBPYnNlcnZhYmxlPHZvaWQ+IHtcbiAgICByZXR1cm4gdGhpcy5wcmVsb2FkaW5nU3RyYXRlZ3kucHJlbG9hZChyb3V0ZSwgKCkgPT4ge1xuICAgICAgY29uc3QgbG9hZGVkJCA9IHRoaXMubG9hZGVyLmxvYWQobmdNb2R1bGUuaW5qZWN0b3IsIHJvdXRlKTtcbiAgICAgIHJldHVybiBsb2FkZWQkLnBpcGUobWVyZ2VNYXAoKGNvbmZpZzogTG9hZGVkUm91dGVyQ29uZmlnKSA9PiB7XG4gICAgICAgIHJvdXRlLl9sb2FkZWRDb25maWcgPSBjb25maWc7XG4gICAgICAgIHJldHVybiB0aGlzLnByb2Nlc3NSb3V0ZXMoY29uZmlnLm1vZHVsZSwgY29uZmlnLnJvdXRlcyk7XG4gICAgICB9KSk7XG4gICAgfSk7XG4gIH1cbn1cbiJdfQ==
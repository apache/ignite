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
import { InjectionToken, NgModuleFactory } from '@angular/core';
import { from, of } from 'rxjs';
import { map, mergeMap } from 'rxjs/operators';
import { LoadedRouterConfig, standardizeConfig } from './config';
import { flatten, wrapIntoObservable } from './utils/collection';
/**
 * The [DI token](guide/glossary/#di-token) for a router configuration.
 * @see `ROUTES`
 * \@publicApi
 * @type {?}
 */
export const ROUTES = new InjectionToken('ROUTES');
export class RouterConfigLoader {
    /**
     * @param {?} loader
     * @param {?} compiler
     * @param {?=} onLoadStartListener
     * @param {?=} onLoadEndListener
     */
    constructor(loader, compiler, onLoadStartListener, onLoadEndListener) {
        this.loader = loader;
        this.compiler = compiler;
        this.onLoadStartListener = onLoadStartListener;
        this.onLoadEndListener = onLoadEndListener;
    }
    /**
     * @param {?} parentInjector
     * @param {?} route
     * @return {?}
     */
    load(parentInjector, route) {
        if (this.onLoadStartListener) {
            this.onLoadStartListener(route);
        }
        /** @type {?} */
        const moduleFactory$ = this.loadModuleFactory((/** @type {?} */ (route.loadChildren)));
        return moduleFactory$.pipe(map((/**
         * @param {?} factory
         * @return {?}
         */
        (factory) => {
            if (this.onLoadEndListener) {
                this.onLoadEndListener(route);
            }
            /** @type {?} */
            const module = factory.create(parentInjector);
            return new LoadedRouterConfig(flatten(module.injector.get(ROUTES)).map(standardizeConfig), module);
        })));
    }
    /**
     * @private
     * @param {?} loadChildren
     * @return {?}
     */
    loadModuleFactory(loadChildren) {
        if (typeof loadChildren === 'string') {
            return from(this.loader.load(loadChildren));
        }
        else {
            return wrapIntoObservable(loadChildren()).pipe(mergeMap((/**
             * @param {?} t
             * @return {?}
             */
            (t) => {
                if (t instanceof NgModuleFactory) {
                    return of(t);
                }
                else {
                    return from(this.compiler.compileModuleAsync(t));
                }
            })));
        }
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    RouterConfigLoader.prototype.loader;
    /**
     * @type {?}
     * @private
     */
    RouterConfigLoader.prototype.compiler;
    /**
     * @type {?}
     * @private
     */
    RouterConfigLoader.prototype.onLoadStartListener;
    /**
     * @type {?}
     * @private
     */
    RouterConfigLoader.prototype.onLoadEndListener;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicm91dGVyX2NvbmZpZ19sb2FkZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9yb3V0ZXIvc3JjL3JvdXRlcl9jb25maWdfbG9hZGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFXLGNBQWMsRUFBWSxlQUFlLEVBQXdCLE1BQU0sZUFBZSxDQUFDO0FBQ3pHLE9BQU8sRUFBYSxJQUFJLEVBQUUsRUFBRSxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQzNDLE9BQU8sRUFBQyxHQUFHLEVBQUUsUUFBUSxFQUFDLE1BQU0sZ0JBQWdCLENBQUM7QUFDN0MsT0FBTyxFQUFlLGtCQUFrQixFQUFTLGlCQUFpQixFQUFDLE1BQU0sVUFBVSxDQUFDO0FBQ3BGLE9BQU8sRUFBQyxPQUFPLEVBQUUsa0JBQWtCLEVBQUMsTUFBTSxvQkFBb0IsQ0FBQzs7Ozs7OztBQU8vRCxNQUFNLE9BQU8sTUFBTSxHQUFHLElBQUksY0FBYyxDQUFZLFFBQVEsQ0FBQztBQUU3RCxNQUFNLE9BQU8sa0JBQWtCOzs7Ozs7O0lBQzdCLFlBQ1ksTUFBNkIsRUFBVSxRQUFrQixFQUN6RCxtQkFBd0MsRUFDeEMsaUJBQXNDO1FBRnRDLFdBQU0sR0FBTixNQUFNLENBQXVCO1FBQVUsYUFBUSxHQUFSLFFBQVEsQ0FBVTtRQUN6RCx3QkFBbUIsR0FBbkIsbUJBQW1CLENBQXFCO1FBQ3hDLHNCQUFpQixHQUFqQixpQkFBaUIsQ0FBcUI7SUFBRyxDQUFDOzs7Ozs7SUFFdEQsSUFBSSxDQUFDLGNBQXdCLEVBQUUsS0FBWTtRQUN6QyxJQUFJLElBQUksQ0FBQyxtQkFBbUIsRUFBRTtZQUM1QixJQUFJLENBQUMsbUJBQW1CLENBQUMsS0FBSyxDQUFDLENBQUM7U0FDakM7O2NBRUssY0FBYyxHQUFHLElBQUksQ0FBQyxpQkFBaUIsQ0FBQyxtQkFBQSxLQUFLLENBQUMsWUFBWSxFQUFFLENBQUM7UUFFbkUsT0FBTyxjQUFjLENBQUMsSUFBSSxDQUFDLEdBQUc7Ozs7UUFBQyxDQUFDLE9BQTZCLEVBQUUsRUFBRTtZQUMvRCxJQUFJLElBQUksQ0FBQyxpQkFBaUIsRUFBRTtnQkFDMUIsSUFBSSxDQUFDLGlCQUFpQixDQUFDLEtBQUssQ0FBQyxDQUFDO2FBQy9COztrQkFFSyxNQUFNLEdBQUcsT0FBTyxDQUFDLE1BQU0sQ0FBQyxjQUFjLENBQUM7WUFFN0MsT0FBTyxJQUFJLGtCQUFrQixDQUN6QixPQUFPLENBQUMsTUFBTSxDQUFDLFFBQVEsQ0FBQyxHQUFHLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxHQUFHLENBQUMsaUJBQWlCLENBQUMsRUFBRSxNQUFNLENBQUMsQ0FBQztRQUMzRSxDQUFDLEVBQUMsQ0FBQyxDQUFDO0lBQ04sQ0FBQzs7Ozs7O0lBRU8saUJBQWlCLENBQUMsWUFBMEI7UUFDbEQsSUFBSSxPQUFPLFlBQVksS0FBSyxRQUFRLEVBQUU7WUFDcEMsT0FBTyxJQUFJLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQztTQUM3QzthQUFNO1lBQ0wsT0FBTyxrQkFBa0IsQ0FBQyxZQUFZLEVBQUUsQ0FBQyxDQUFDLElBQUksQ0FBQyxRQUFROzs7O1lBQUMsQ0FBQyxDQUFNLEVBQUUsRUFBRTtnQkFDakUsSUFBSSxDQUFDLFlBQVksZUFBZSxFQUFFO29CQUNoQyxPQUFPLEVBQUUsQ0FBRSxDQUFDLENBQUMsQ0FBQztpQkFDZjtxQkFBTTtvQkFDTCxPQUFPLElBQUksQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLGtCQUFrQixDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7aUJBQ2xEO1lBQ0gsQ0FBQyxFQUFDLENBQUMsQ0FBQztTQUNMO0lBQ0gsQ0FBQztDQUNGOzs7Ozs7SUFwQ0ssb0NBQXFDOzs7OztJQUFFLHNDQUEwQjs7Ozs7SUFDakUsaURBQWdEOzs7OztJQUNoRCwrQ0FBOEMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Q29tcGlsZXIsIEluamVjdGlvblRva2VuLCBJbmplY3RvciwgTmdNb2R1bGVGYWN0b3J5LCBOZ01vZHVsZUZhY3RvcnlMb2FkZXJ9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHtPYnNlcnZhYmxlLCBmcm9tLCBvZiB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHttYXAsIG1lcmdlTWFwfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5pbXBvcnQge0xvYWRDaGlsZHJlbiwgTG9hZGVkUm91dGVyQ29uZmlnLCBSb3V0ZSwgc3RhbmRhcmRpemVDb25maWd9IGZyb20gJy4vY29uZmlnJztcbmltcG9ydCB7ZmxhdHRlbiwgd3JhcEludG9PYnNlcnZhYmxlfSBmcm9tICcuL3V0aWxzL2NvbGxlY3Rpb24nO1xuXG4vKipcbiAqIFRoZSBbREkgdG9rZW5dKGd1aWRlL2dsb3NzYXJ5LyNkaS10b2tlbikgZm9yIGEgcm91dGVyIGNvbmZpZ3VyYXRpb24uXG4gKiBAc2VlIGBST1VURVNgXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjb25zdCBST1VURVMgPSBuZXcgSW5qZWN0aW9uVG9rZW48Um91dGVbXVtdPignUk9VVEVTJyk7XG5cbmV4cG9ydCBjbGFzcyBSb3V0ZXJDb25maWdMb2FkZXIge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIHByaXZhdGUgbG9hZGVyOiBOZ01vZHVsZUZhY3RvcnlMb2FkZXIsIHByaXZhdGUgY29tcGlsZXI6IENvbXBpbGVyLFxuICAgICAgcHJpdmF0ZSBvbkxvYWRTdGFydExpc3RlbmVyPzogKHI6IFJvdXRlKSA9PiB2b2lkLFxuICAgICAgcHJpdmF0ZSBvbkxvYWRFbmRMaXN0ZW5lcj86IChyOiBSb3V0ZSkgPT4gdm9pZCkge31cblxuICBsb2FkKHBhcmVudEluamVjdG9yOiBJbmplY3Rvciwgcm91dGU6IFJvdXRlKTogT2JzZXJ2YWJsZTxMb2FkZWRSb3V0ZXJDb25maWc+IHtcbiAgICBpZiAodGhpcy5vbkxvYWRTdGFydExpc3RlbmVyKSB7XG4gICAgICB0aGlzLm9uTG9hZFN0YXJ0TGlzdGVuZXIocm91dGUpO1xuICAgIH1cblxuICAgIGNvbnN0IG1vZHVsZUZhY3RvcnkkID0gdGhpcy5sb2FkTW9kdWxlRmFjdG9yeShyb3V0ZS5sb2FkQ2hpbGRyZW4gISk7XG5cbiAgICByZXR1cm4gbW9kdWxlRmFjdG9yeSQucGlwZShtYXAoKGZhY3Rvcnk6IE5nTW9kdWxlRmFjdG9yeTxhbnk+KSA9PiB7XG4gICAgICBpZiAodGhpcy5vbkxvYWRFbmRMaXN0ZW5lcikge1xuICAgICAgICB0aGlzLm9uTG9hZEVuZExpc3RlbmVyKHJvdXRlKTtcbiAgICAgIH1cblxuICAgICAgY29uc3QgbW9kdWxlID0gZmFjdG9yeS5jcmVhdGUocGFyZW50SW5qZWN0b3IpO1xuXG4gICAgICByZXR1cm4gbmV3IExvYWRlZFJvdXRlckNvbmZpZyhcbiAgICAgICAgICBmbGF0dGVuKG1vZHVsZS5pbmplY3Rvci5nZXQoUk9VVEVTKSkubWFwKHN0YW5kYXJkaXplQ29uZmlnKSwgbW9kdWxlKTtcbiAgICB9KSk7XG4gIH1cblxuICBwcml2YXRlIGxvYWRNb2R1bGVGYWN0b3J5KGxvYWRDaGlsZHJlbjogTG9hZENoaWxkcmVuKTogT2JzZXJ2YWJsZTxOZ01vZHVsZUZhY3Rvcnk8YW55Pj4ge1xuICAgIGlmICh0eXBlb2YgbG9hZENoaWxkcmVuID09PSAnc3RyaW5nJykge1xuICAgICAgcmV0dXJuIGZyb20odGhpcy5sb2FkZXIubG9hZChsb2FkQ2hpbGRyZW4pKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHdyYXBJbnRvT2JzZXJ2YWJsZShsb2FkQ2hpbGRyZW4oKSkucGlwZShtZXJnZU1hcCgodDogYW55KSA9PiB7XG4gICAgICAgIGlmICh0IGluc3RhbmNlb2YgTmdNb2R1bGVGYWN0b3J5KSB7XG4gICAgICAgICAgcmV0dXJuIG9mICh0KTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gZnJvbSh0aGlzLmNvbXBpbGVyLmNvbXBpbGVNb2R1bGVBc3luYyh0KSk7XG4gICAgICAgIH1cbiAgICAgIH0pKTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==
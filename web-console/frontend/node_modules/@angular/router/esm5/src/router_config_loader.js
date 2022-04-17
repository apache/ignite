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
 * @publicApi
 */
export var ROUTES = new InjectionToken('ROUTES');
var RouterConfigLoader = /** @class */ (function () {
    function RouterConfigLoader(loader, compiler, onLoadStartListener, onLoadEndListener) {
        this.loader = loader;
        this.compiler = compiler;
        this.onLoadStartListener = onLoadStartListener;
        this.onLoadEndListener = onLoadEndListener;
    }
    RouterConfigLoader.prototype.load = function (parentInjector, route) {
        var _this = this;
        if (this.onLoadStartListener) {
            this.onLoadStartListener(route);
        }
        var moduleFactory$ = this.loadModuleFactory(route.loadChildren);
        return moduleFactory$.pipe(map(function (factory) {
            if (_this.onLoadEndListener) {
                _this.onLoadEndListener(route);
            }
            var module = factory.create(parentInjector);
            return new LoadedRouterConfig(flatten(module.injector.get(ROUTES)).map(standardizeConfig), module);
        }));
    };
    RouterConfigLoader.prototype.loadModuleFactory = function (loadChildren) {
        var _this = this;
        if (typeof loadChildren === 'string') {
            return from(this.loader.load(loadChildren));
        }
        else {
            return wrapIntoObservable(loadChildren()).pipe(mergeMap(function (t) {
                if (t instanceof NgModuleFactory) {
                    return of(t);
                }
                else {
                    return from(_this.compiler.compileModuleAsync(t));
                }
            }));
        }
    };
    return RouterConfigLoader;
}());
export { RouterConfigLoader };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicm91dGVyX2NvbmZpZ19sb2FkZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9yb3V0ZXIvc3JjL3JvdXRlcl9jb25maWdfbG9hZGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUVILE9BQU8sRUFBVyxjQUFjLEVBQVksZUFBZSxFQUF3QixNQUFNLGVBQWUsQ0FBQztBQUN6RyxPQUFPLEVBQWEsSUFBSSxFQUFFLEVBQUUsRUFBRSxNQUFNLE1BQU0sQ0FBQztBQUMzQyxPQUFPLEVBQUMsR0FBRyxFQUFFLFFBQVEsRUFBQyxNQUFNLGdCQUFnQixDQUFDO0FBQzdDLE9BQU8sRUFBZSxrQkFBa0IsRUFBUyxpQkFBaUIsRUFBQyxNQUFNLFVBQVUsQ0FBQztBQUNwRixPQUFPLEVBQUMsT0FBTyxFQUFFLGtCQUFrQixFQUFDLE1BQU0sb0JBQW9CLENBQUM7QUFFL0Q7Ozs7R0FJRztBQUNILE1BQU0sQ0FBQyxJQUFNLE1BQU0sR0FBRyxJQUFJLGNBQWMsQ0FBWSxRQUFRLENBQUMsQ0FBQztBQUU5RDtJQUNFLDRCQUNZLE1BQTZCLEVBQVUsUUFBa0IsRUFDekQsbUJBQXdDLEVBQ3hDLGlCQUFzQztRQUZ0QyxXQUFNLEdBQU4sTUFBTSxDQUF1QjtRQUFVLGFBQVEsR0FBUixRQUFRLENBQVU7UUFDekQsd0JBQW1CLEdBQW5CLG1CQUFtQixDQUFxQjtRQUN4QyxzQkFBaUIsR0FBakIsaUJBQWlCLENBQXFCO0lBQUcsQ0FBQztJQUV0RCxpQ0FBSSxHQUFKLFVBQUssY0FBd0IsRUFBRSxLQUFZO1FBQTNDLGlCQWlCQztRQWhCQyxJQUFJLElBQUksQ0FBQyxtQkFBbUIsRUFBRTtZQUM1QixJQUFJLENBQUMsbUJBQW1CLENBQUMsS0FBSyxDQUFDLENBQUM7U0FDakM7UUFFRCxJQUFNLGNBQWMsR0FBRyxJQUFJLENBQUMsaUJBQWlCLENBQUMsS0FBSyxDQUFDLFlBQWMsQ0FBQyxDQUFDO1FBRXBFLE9BQU8sY0FBYyxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsVUFBQyxPQUE2QjtZQUMzRCxJQUFJLEtBQUksQ0FBQyxpQkFBaUIsRUFBRTtnQkFDMUIsS0FBSSxDQUFDLGlCQUFpQixDQUFDLEtBQUssQ0FBQyxDQUFDO2FBQy9CO1lBRUQsSUFBTSxNQUFNLEdBQUcsT0FBTyxDQUFDLE1BQU0sQ0FBQyxjQUFjLENBQUMsQ0FBQztZQUU5QyxPQUFPLElBQUksa0JBQWtCLENBQ3pCLE9BQU8sQ0FBQyxNQUFNLENBQUMsUUFBUSxDQUFDLEdBQUcsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLEdBQUcsQ0FBQyxpQkFBaUIsQ0FBQyxFQUFFLE1BQU0sQ0FBQyxDQUFDO1FBQzNFLENBQUMsQ0FBQyxDQUFDLENBQUM7SUFDTixDQUFDO0lBRU8sOENBQWlCLEdBQXpCLFVBQTBCLFlBQTBCO1FBQXBELGlCQVlDO1FBWEMsSUFBSSxPQUFPLFlBQVksS0FBSyxRQUFRLEVBQUU7WUFDcEMsT0FBTyxJQUFJLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQztTQUM3QzthQUFNO1lBQ0wsT0FBTyxrQkFBa0IsQ0FBQyxZQUFZLEVBQUUsQ0FBQyxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsVUFBQyxDQUFNO2dCQUM3RCxJQUFJLENBQUMsWUFBWSxlQUFlLEVBQUU7b0JBQ2hDLE9BQU8sRUFBRSxDQUFFLENBQUMsQ0FBQyxDQUFDO2lCQUNmO3FCQUFNO29CQUNMLE9BQU8sSUFBSSxDQUFDLEtBQUksQ0FBQyxRQUFRLENBQUMsa0JBQWtCLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztpQkFDbEQ7WUFDSCxDQUFDLENBQUMsQ0FBQyxDQUFDO1NBQ0w7SUFDSCxDQUFDO0lBQ0gseUJBQUM7QUFBRCxDQUFDLEFBdENELElBc0NDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBpbGVyLCBJbmplY3Rpb25Ub2tlbiwgSW5qZWN0b3IsIE5nTW9kdWxlRmFjdG9yeSwgTmdNb2R1bGVGYWN0b3J5TG9hZGVyfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7T2JzZXJ2YWJsZSwgZnJvbSwgb2YgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7bWFwLCBtZXJnZU1hcH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHtMb2FkQ2hpbGRyZW4sIExvYWRlZFJvdXRlckNvbmZpZywgUm91dGUsIHN0YW5kYXJkaXplQ29uZmlnfSBmcm9tICcuL2NvbmZpZyc7XG5pbXBvcnQge2ZsYXR0ZW4sIHdyYXBJbnRvT2JzZXJ2YWJsZX0gZnJvbSAnLi91dGlscy9jb2xsZWN0aW9uJztcblxuLyoqXG4gKiBUaGUgW0RJIHRva2VuXShndWlkZS9nbG9zc2FyeS8jZGktdG9rZW4pIGZvciBhIHJvdXRlciBjb25maWd1cmF0aW9uLlxuICogQHNlZSBgUk9VVEVTYFxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY29uc3QgUk9VVEVTID0gbmV3IEluamVjdGlvblRva2VuPFJvdXRlW11bXT4oJ1JPVVRFUycpO1xuXG5leHBvcnQgY2xhc3MgUm91dGVyQ29uZmlnTG9hZGVyIHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIGxvYWRlcjogTmdNb2R1bGVGYWN0b3J5TG9hZGVyLCBwcml2YXRlIGNvbXBpbGVyOiBDb21waWxlcixcbiAgICAgIHByaXZhdGUgb25Mb2FkU3RhcnRMaXN0ZW5lcj86IChyOiBSb3V0ZSkgPT4gdm9pZCxcbiAgICAgIHByaXZhdGUgb25Mb2FkRW5kTGlzdGVuZXI/OiAocjogUm91dGUpID0+IHZvaWQpIHt9XG5cbiAgbG9hZChwYXJlbnRJbmplY3RvcjogSW5qZWN0b3IsIHJvdXRlOiBSb3V0ZSk6IE9ic2VydmFibGU8TG9hZGVkUm91dGVyQ29uZmlnPiB7XG4gICAgaWYgKHRoaXMub25Mb2FkU3RhcnRMaXN0ZW5lcikge1xuICAgICAgdGhpcy5vbkxvYWRTdGFydExpc3RlbmVyKHJvdXRlKTtcbiAgICB9XG5cbiAgICBjb25zdCBtb2R1bGVGYWN0b3J5JCA9IHRoaXMubG9hZE1vZHVsZUZhY3Rvcnkocm91dGUubG9hZENoaWxkcmVuICEpO1xuXG4gICAgcmV0dXJuIG1vZHVsZUZhY3RvcnkkLnBpcGUobWFwKChmYWN0b3J5OiBOZ01vZHVsZUZhY3Rvcnk8YW55PikgPT4ge1xuICAgICAgaWYgKHRoaXMub25Mb2FkRW5kTGlzdGVuZXIpIHtcbiAgICAgICAgdGhpcy5vbkxvYWRFbmRMaXN0ZW5lcihyb3V0ZSk7XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IG1vZHVsZSA9IGZhY3RvcnkuY3JlYXRlKHBhcmVudEluamVjdG9yKTtcblxuICAgICAgcmV0dXJuIG5ldyBMb2FkZWRSb3V0ZXJDb25maWcoXG4gICAgICAgICAgZmxhdHRlbihtb2R1bGUuaW5qZWN0b3IuZ2V0KFJPVVRFUykpLm1hcChzdGFuZGFyZGl6ZUNvbmZpZyksIG1vZHVsZSk7XG4gICAgfSkpO1xuICB9XG5cbiAgcHJpdmF0ZSBsb2FkTW9kdWxlRmFjdG9yeShsb2FkQ2hpbGRyZW46IExvYWRDaGlsZHJlbik6IE9ic2VydmFibGU8TmdNb2R1bGVGYWN0b3J5PGFueT4+IHtcbiAgICBpZiAodHlwZW9mIGxvYWRDaGlsZHJlbiA9PT0gJ3N0cmluZycpIHtcbiAgICAgIHJldHVybiBmcm9tKHRoaXMubG9hZGVyLmxvYWQobG9hZENoaWxkcmVuKSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiB3cmFwSW50b09ic2VydmFibGUobG9hZENoaWxkcmVuKCkpLnBpcGUobWVyZ2VNYXAoKHQ6IGFueSkgPT4ge1xuICAgICAgICBpZiAodCBpbnN0YW5jZW9mIE5nTW9kdWxlRmFjdG9yeSkge1xuICAgICAgICAgIHJldHVybiBvZiAodCk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuIGZyb20odGhpcy5jb21waWxlci5jb21waWxlTW9kdWxlQXN5bmModCkpO1xuICAgICAgICB9XG4gICAgICB9KSk7XG4gICAgfVxuICB9XG59XG4iXX0=
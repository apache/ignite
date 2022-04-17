/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { tokenReference } from '../compile_metadata';
export function listLazyRoutes(moduleMeta, reflector) {
    var e_1, _a, e_2, _b;
    var allLazyRoutes = [];
    try {
        for (var _c = tslib_1.__values(moduleMeta.transitiveModule.providers), _d = _c.next(); !_d.done; _d = _c.next()) {
            var _e = _d.value, provider = _e.provider, module = _e.module;
            if (tokenReference(provider.token) === reflector.ROUTES) {
                var loadChildren = _collectLoadChildren(provider.useValue);
                try {
                    for (var loadChildren_1 = (e_2 = void 0, tslib_1.__values(loadChildren)), loadChildren_1_1 = loadChildren_1.next(); !loadChildren_1_1.done; loadChildren_1_1 = loadChildren_1.next()) {
                        var route = loadChildren_1_1.value;
                        allLazyRoutes.push(parseLazyRoute(route, reflector, module.reference));
                    }
                }
                catch (e_2_1) { e_2 = { error: e_2_1 }; }
                finally {
                    try {
                        if (loadChildren_1_1 && !loadChildren_1_1.done && (_b = loadChildren_1.return)) _b.call(loadChildren_1);
                    }
                    finally { if (e_2) throw e_2.error; }
                }
            }
        }
    }
    catch (e_1_1) { e_1 = { error: e_1_1 }; }
    finally {
        try {
            if (_d && !_d.done && (_a = _c.return)) _a.call(_c);
        }
        finally { if (e_1) throw e_1.error; }
    }
    return allLazyRoutes;
}
function _collectLoadChildren(routes, target) {
    var e_3, _a;
    if (target === void 0) { target = []; }
    if (typeof routes === 'string') {
        target.push(routes);
    }
    else if (Array.isArray(routes)) {
        try {
            for (var routes_1 = tslib_1.__values(routes), routes_1_1 = routes_1.next(); !routes_1_1.done; routes_1_1 = routes_1.next()) {
                var route = routes_1_1.value;
                _collectLoadChildren(route, target);
            }
        }
        catch (e_3_1) { e_3 = { error: e_3_1 }; }
        finally {
            try {
                if (routes_1_1 && !routes_1_1.done && (_a = routes_1.return)) _a.call(routes_1);
            }
            finally { if (e_3) throw e_3.error; }
        }
    }
    else if (routes.loadChildren) {
        _collectLoadChildren(routes.loadChildren, target);
    }
    else if (routes.children) {
        _collectLoadChildren(routes.children, target);
    }
    return target;
}
export function parseLazyRoute(route, reflector, module) {
    var _a = tslib_1.__read(route.split('#'), 2), routePath = _a[0], routeName = _a[1];
    var referencedModule = reflector.resolveExternalReference({
        moduleName: routePath,
        name: routeName,
    }, module ? module.filePath : undefined);
    return { route: route, module: module || referencedModule, referencedModule: referencedModule };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibGF6eV9yb3V0ZXMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvYW90L2xhenlfcm91dGVzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFFSCxPQUFPLEVBQTBCLGNBQWMsRUFBQyxNQUFNLHFCQUFxQixDQUFDO0FBYzVFLE1BQU0sVUFBVSxjQUFjLENBQzFCLFVBQW1DLEVBQUUsU0FBMEI7O0lBQ2pFLElBQU0sYUFBYSxHQUFnQixFQUFFLENBQUM7O1FBQ3RDLEtBQWlDLElBQUEsS0FBQSxpQkFBQSxVQUFVLENBQUMsZ0JBQWdCLENBQUMsU0FBUyxDQUFBLGdCQUFBLDRCQUFFO1lBQTdELElBQUEsYUFBa0IsRUFBakIsc0JBQVEsRUFBRSxrQkFBTTtZQUMxQixJQUFJLGNBQWMsQ0FBQyxRQUFRLENBQUMsS0FBSyxDQUFDLEtBQUssU0FBUyxDQUFDLE1BQU0sRUFBRTtnQkFDdkQsSUFBTSxZQUFZLEdBQUcsb0JBQW9CLENBQUMsUUFBUSxDQUFDLFFBQVEsQ0FBQyxDQUFDOztvQkFDN0QsS0FBb0IsSUFBQSxnQ0FBQSxpQkFBQSxZQUFZLENBQUEsQ0FBQSwwQ0FBQSxvRUFBRTt3QkFBN0IsSUFBTSxLQUFLLHlCQUFBO3dCQUNkLGFBQWEsQ0FBQyxJQUFJLENBQUMsY0FBYyxDQUFDLEtBQUssRUFBRSxTQUFTLEVBQUUsTUFBTSxDQUFDLFNBQVMsQ0FBQyxDQUFDLENBQUM7cUJBQ3hFOzs7Ozs7Ozs7YUFDRjtTQUNGOzs7Ozs7Ozs7SUFDRCxPQUFPLGFBQWEsQ0FBQztBQUN2QixDQUFDO0FBRUQsU0FBUyxvQkFBb0IsQ0FBQyxNQUFnQyxFQUFFLE1BQXFCOztJQUFyQix1QkFBQSxFQUFBLFdBQXFCO0lBQ25GLElBQUksT0FBTyxNQUFNLEtBQUssUUFBUSxFQUFFO1FBQzlCLE1BQU0sQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLENBQUM7S0FDckI7U0FBTSxJQUFJLEtBQUssQ0FBQyxPQUFPLENBQUMsTUFBTSxDQUFDLEVBQUU7O1lBQ2hDLEtBQW9CLElBQUEsV0FBQSxpQkFBQSxNQUFNLENBQUEsOEJBQUEsa0RBQUU7Z0JBQXZCLElBQU0sS0FBSyxtQkFBQTtnQkFDZCxvQkFBb0IsQ0FBQyxLQUFLLEVBQUUsTUFBTSxDQUFDLENBQUM7YUFDckM7Ozs7Ozs7OztLQUNGO1NBQU0sSUFBSSxNQUFNLENBQUMsWUFBWSxFQUFFO1FBQzlCLG9CQUFvQixDQUFDLE1BQU0sQ0FBQyxZQUFZLEVBQUUsTUFBTSxDQUFDLENBQUM7S0FDbkQ7U0FBTSxJQUFJLE1BQU0sQ0FBQyxRQUFRLEVBQUU7UUFDMUIsb0JBQW9CLENBQUMsTUFBTSxDQUFDLFFBQVEsRUFBRSxNQUFNLENBQUMsQ0FBQztLQUMvQztJQUNELE9BQU8sTUFBTSxDQUFDO0FBQ2hCLENBQUM7QUFFRCxNQUFNLFVBQVUsY0FBYyxDQUMxQixLQUFhLEVBQUUsU0FBMEIsRUFBRSxNQUFxQjtJQUM1RCxJQUFBLHdDQUF5QyxFQUF4QyxpQkFBUyxFQUFFLGlCQUE2QixDQUFDO0lBQ2hELElBQU0sZ0JBQWdCLEdBQUcsU0FBUyxDQUFDLHdCQUF3QixDQUN2RDtRQUNFLFVBQVUsRUFBRSxTQUFTO1FBQ3JCLElBQUksRUFBRSxTQUFTO0tBQ2hCLEVBQ0QsTUFBTSxDQUFDLENBQUMsQ0FBQyxNQUFNLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQyxTQUFTLENBQUMsQ0FBQztJQUMxQyxPQUFPLEVBQUMsS0FBSyxFQUFFLEtBQUssRUFBRSxNQUFNLEVBQUUsTUFBTSxJQUFJLGdCQUFnQixFQUFFLGdCQUFnQixrQkFBQSxFQUFDLENBQUM7QUFDOUUsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDb21waWxlTmdNb2R1bGVNZXRhZGF0YSwgdG9rZW5SZWZlcmVuY2V9IGZyb20gJy4uL2NvbXBpbGVfbWV0YWRhdGEnO1xuaW1wb3J0IHtSb3V0ZX0gZnJvbSAnLi4vY29yZSc7XG5pbXBvcnQge0NvbXBpbGVNZXRhZGF0YVJlc29sdmVyfSBmcm9tICcuLi9tZXRhZGF0YV9yZXNvbHZlcic7XG5cbmltcG9ydCB7QW90Q29tcGlsZXJIb3N0fSBmcm9tICcuL2NvbXBpbGVyX2hvc3QnO1xuaW1wb3J0IHtTdGF0aWNSZWZsZWN0b3J9IGZyb20gJy4vc3RhdGljX3JlZmxlY3Rvcic7XG5pbXBvcnQge1N0YXRpY1N5bWJvbH0gZnJvbSAnLi9zdGF0aWNfc3ltYm9sJztcblxuZXhwb3J0IGludGVyZmFjZSBMYXp5Um91dGUge1xuICBtb2R1bGU6IFN0YXRpY1N5bWJvbDtcbiAgcm91dGU6IHN0cmluZztcbiAgcmVmZXJlbmNlZE1vZHVsZTogU3RhdGljU3ltYm9sO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gbGlzdExhenlSb3V0ZXMoXG4gICAgbW9kdWxlTWV0YTogQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEsIHJlZmxlY3RvcjogU3RhdGljUmVmbGVjdG9yKTogTGF6eVJvdXRlW10ge1xuICBjb25zdCBhbGxMYXp5Um91dGVzOiBMYXp5Um91dGVbXSA9IFtdO1xuICBmb3IgKGNvbnN0IHtwcm92aWRlciwgbW9kdWxlfSBvZiBtb2R1bGVNZXRhLnRyYW5zaXRpdmVNb2R1bGUucHJvdmlkZXJzKSB7XG4gICAgaWYgKHRva2VuUmVmZXJlbmNlKHByb3ZpZGVyLnRva2VuKSA9PT0gcmVmbGVjdG9yLlJPVVRFUykge1xuICAgICAgY29uc3QgbG9hZENoaWxkcmVuID0gX2NvbGxlY3RMb2FkQ2hpbGRyZW4ocHJvdmlkZXIudXNlVmFsdWUpO1xuICAgICAgZm9yIChjb25zdCByb3V0ZSBvZiBsb2FkQ2hpbGRyZW4pIHtcbiAgICAgICAgYWxsTGF6eVJvdXRlcy5wdXNoKHBhcnNlTGF6eVJvdXRlKHJvdXRlLCByZWZsZWN0b3IsIG1vZHVsZS5yZWZlcmVuY2UpKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cbiAgcmV0dXJuIGFsbExhenlSb3V0ZXM7XG59XG5cbmZ1bmN0aW9uIF9jb2xsZWN0TG9hZENoaWxkcmVuKHJvdXRlczogc3RyaW5nIHwgUm91dGUgfCBSb3V0ZVtdLCB0YXJnZXQ6IHN0cmluZ1tdID0gW10pOiBzdHJpbmdbXSB7XG4gIGlmICh0eXBlb2Ygcm91dGVzID09PSAnc3RyaW5nJykge1xuICAgIHRhcmdldC5wdXNoKHJvdXRlcyk7XG4gIH0gZWxzZSBpZiAoQXJyYXkuaXNBcnJheShyb3V0ZXMpKSB7XG4gICAgZm9yIChjb25zdCByb3V0ZSBvZiByb3V0ZXMpIHtcbiAgICAgIF9jb2xsZWN0TG9hZENoaWxkcmVuKHJvdXRlLCB0YXJnZXQpO1xuICAgIH1cbiAgfSBlbHNlIGlmIChyb3V0ZXMubG9hZENoaWxkcmVuKSB7XG4gICAgX2NvbGxlY3RMb2FkQ2hpbGRyZW4ocm91dGVzLmxvYWRDaGlsZHJlbiwgdGFyZ2V0KTtcbiAgfSBlbHNlIGlmIChyb3V0ZXMuY2hpbGRyZW4pIHtcbiAgICBfY29sbGVjdExvYWRDaGlsZHJlbihyb3V0ZXMuY2hpbGRyZW4sIHRhcmdldCk7XG4gIH1cbiAgcmV0dXJuIHRhcmdldDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlTGF6eVJvdXRlKFxuICAgIHJvdXRlOiBzdHJpbmcsIHJlZmxlY3RvcjogU3RhdGljUmVmbGVjdG9yLCBtb2R1bGU/OiBTdGF0aWNTeW1ib2wpOiBMYXp5Um91dGUge1xuICBjb25zdCBbcm91dGVQYXRoLCByb3V0ZU5hbWVdID0gcm91dGUuc3BsaXQoJyMnKTtcbiAgY29uc3QgcmVmZXJlbmNlZE1vZHVsZSA9IHJlZmxlY3Rvci5yZXNvbHZlRXh0ZXJuYWxSZWZlcmVuY2UoXG4gICAgICB7XG4gICAgICAgIG1vZHVsZU5hbWU6IHJvdXRlUGF0aCxcbiAgICAgICAgbmFtZTogcm91dGVOYW1lLFxuICAgICAgfSxcbiAgICAgIG1vZHVsZSA/IG1vZHVsZS5maWxlUGF0aCA6IHVuZGVmaW5lZCk7XG4gIHJldHVybiB7cm91dGU6IHJvdXRlLCBtb2R1bGU6IG1vZHVsZSB8fCByZWZlcmVuY2VkTW9kdWxlLCByZWZlcmVuY2VkTW9kdWxlfTtcbn1cbiJdfQ==
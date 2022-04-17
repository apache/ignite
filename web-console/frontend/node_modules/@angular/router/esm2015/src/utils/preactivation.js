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
import { equalParamsAndUrlSegments } from '../router_state';
import { equalPath } from '../url_tree';
import { forEach, shallowEqual } from '../utils/collection';
import { nodeChildrenAsMap } from '../utils/tree';
export class CanActivate {
    /**
     * @param {?} path
     */
    constructor(path) {
        this.path = path;
        this.route = this.path[this.path.length - 1];
    }
}
if (false) {
    /** @type {?} */
    CanActivate.prototype.route;
    /** @type {?} */
    CanActivate.prototype.path;
}
export class CanDeactivate {
    /**
     * @param {?} component
     * @param {?} route
     */
    constructor(component, route) {
        this.component = component;
        this.route = route;
    }
}
if (false) {
    /** @type {?} */
    CanDeactivate.prototype.component;
    /** @type {?} */
    CanDeactivate.prototype.route;
}
/**
 * @param {?} future
 * @param {?} curr
 * @param {?} parentContexts
 * @return {?}
 */
export function getAllRouteGuards(future, curr, parentContexts) {
    /** @type {?} */
    const futureRoot = future._root;
    /** @type {?} */
    const currRoot = curr ? curr._root : null;
    return getChildRouteGuards(futureRoot, currRoot, parentContexts, [futureRoot.value]);
}
/**
 * @param {?} p
 * @return {?}
 */
export function getCanActivateChild(p) {
    /** @type {?} */
    const canActivateChild = p.routeConfig ? p.routeConfig.canActivateChild : null;
    if (!canActivateChild || canActivateChild.length === 0)
        return null;
    return { node: p, guards: canActivateChild };
}
/**
 * @param {?} token
 * @param {?} snapshot
 * @param {?} moduleInjector
 * @return {?}
 */
export function getToken(token, snapshot, moduleInjector) {
    /** @type {?} */
    const config = getClosestLoadedConfig(snapshot);
    /** @type {?} */
    const injector = config ? config.module.injector : moduleInjector;
    return injector.get(token);
}
/**
 * @param {?} snapshot
 * @return {?}
 */
function getClosestLoadedConfig(snapshot) {
    if (!snapshot)
        return null;
    for (let s = snapshot.parent; s; s = s.parent) {
        /** @type {?} */
        const route = s.routeConfig;
        if (route && route._loadedConfig)
            return route._loadedConfig;
    }
    return null;
}
/**
 * @param {?} futureNode
 * @param {?} currNode
 * @param {?} contexts
 * @param {?} futurePath
 * @param {?=} checks
 * @return {?}
 */
function getChildRouteGuards(futureNode, currNode, contexts, futurePath, checks = {
    canDeactivateChecks: [],
    canActivateChecks: []
}) {
    /** @type {?} */
    const prevChildren = nodeChildrenAsMap(currNode);
    // Process the children of the future route
    futureNode.children.forEach((/**
     * @param {?} c
     * @return {?}
     */
    c => {
        getRouteGuards(c, prevChildren[c.value.outlet], contexts, futurePath.concat([c.value]), checks);
        delete prevChildren[c.value.outlet];
    }));
    // Process any children left from the current route (not active for the future route)
    forEach(prevChildren, (/**
     * @param {?} v
     * @param {?} k
     * @return {?}
     */
    (v, k) => deactivateRouteAndItsChildren(v, (/** @type {?} */ (contexts)).getContext(k), checks)));
    return checks;
}
/**
 * @param {?} futureNode
 * @param {?} currNode
 * @param {?} parentContexts
 * @param {?} futurePath
 * @param {?=} checks
 * @return {?}
 */
function getRouteGuards(futureNode, currNode, parentContexts, futurePath, checks = {
    canDeactivateChecks: [],
    canActivateChecks: []
}) {
    /** @type {?} */
    const future = futureNode.value;
    /** @type {?} */
    const curr = currNode ? currNode.value : null;
    /** @type {?} */
    const context = parentContexts ? parentContexts.getContext(futureNode.value.outlet) : null;
    // reusing the node
    if (curr && future.routeConfig === curr.routeConfig) {
        /** @type {?} */
        const shouldRun = shouldRunGuardsAndResolvers(curr, future, (/** @type {?} */ (future.routeConfig)).runGuardsAndResolvers);
        if (shouldRun) {
            checks.canActivateChecks.push(new CanActivate(futurePath));
        }
        else {
            // we need to set the data
            future.data = curr.data;
            future._resolvedData = curr._resolvedData;
        }
        // If we have a component, we need to go through an outlet.
        if (future.component) {
            getChildRouteGuards(futureNode, currNode, context ? context.children : null, futurePath, checks);
            // if we have a componentless route, we recurse but keep the same outlet map.
        }
        else {
            getChildRouteGuards(futureNode, currNode, parentContexts, futurePath, checks);
        }
        if (shouldRun) {
            /** @type {?} */
            const component = context && context.outlet && context.outlet.component || null;
            checks.canDeactivateChecks.push(new CanDeactivate(component, curr));
        }
    }
    else {
        if (curr) {
            deactivateRouteAndItsChildren(currNode, context, checks);
        }
        checks.canActivateChecks.push(new CanActivate(futurePath));
        // If we have a component, we need to go through an outlet.
        if (future.component) {
            getChildRouteGuards(futureNode, null, context ? context.children : null, futurePath, checks);
            // if we have a componentless route, we recurse but keep the same outlet map.
        }
        else {
            getChildRouteGuards(futureNode, null, parentContexts, futurePath, checks);
        }
    }
    return checks;
}
/**
 * @param {?} curr
 * @param {?} future
 * @param {?} mode
 * @return {?}
 */
function shouldRunGuardsAndResolvers(curr, future, mode) {
    if (typeof mode === 'function') {
        return mode(curr, future);
    }
    switch (mode) {
        case 'pathParamsChange':
            return !equalPath(curr.url, future.url);
        case 'pathParamsOrQueryParamsChange':
            return !equalPath(curr.url, future.url) ||
                !shallowEqual(curr.queryParams, future.queryParams);
        case 'always':
            return true;
        case 'paramsOrQueryParamsChange':
            return !equalParamsAndUrlSegments(curr, future) ||
                !shallowEqual(curr.queryParams, future.queryParams);
        case 'paramsChange':
        default:
            return !equalParamsAndUrlSegments(curr, future);
    }
}
/**
 * @param {?} route
 * @param {?} context
 * @param {?} checks
 * @return {?}
 */
function deactivateRouteAndItsChildren(route, context, checks) {
    /** @type {?} */
    const children = nodeChildrenAsMap(route);
    /** @type {?} */
    const r = route.value;
    forEach(children, (/**
     * @param {?} node
     * @param {?} childName
     * @return {?}
     */
    (node, childName) => {
        if (!r.component) {
            deactivateRouteAndItsChildren(node, context, checks);
        }
        else if (context) {
            deactivateRouteAndItsChildren(node, context.children.getContext(childName), checks);
        }
        else {
            deactivateRouteAndItsChildren(node, null, checks);
        }
    }));
    if (!r.component) {
        checks.canDeactivateChecks.push(new CanDeactivate(null, r));
    }
    else if (context && context.outlet && context.outlet.isActivated) {
        checks.canDeactivateChecks.push(new CanDeactivate(context.outlet.component, r));
    }
    else {
        checks.canDeactivateChecks.push(new CanDeactivate(null, r));
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicHJlYWN0aXZhdGlvbi5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3JvdXRlci9zcmMvdXRpbHMvcHJlYWN0aXZhdGlvbi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVlBLE9BQU8sRUFBOEMseUJBQXlCLEVBQUMsTUFBTSxpQkFBaUIsQ0FBQztBQUN2RyxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sYUFBYSxDQUFDO0FBQ3RDLE9BQU8sRUFBQyxPQUFPLEVBQUUsWUFBWSxFQUFDLE1BQU0scUJBQXFCLENBQUM7QUFDMUQsT0FBTyxFQUFXLGlCQUFpQixFQUFDLE1BQU0sZUFBZSxDQUFDO0FBRTFELE1BQU0sT0FBTyxXQUFXOzs7O0lBRXRCLFlBQW1CLElBQThCO1FBQTlCLFNBQUksR0FBSixJQUFJLENBQTBCO1FBQy9DLElBQUksQ0FBQyxLQUFLLEdBQUcsSUFBSSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLE1BQU0sR0FBRyxDQUFDLENBQUMsQ0FBQztJQUMvQyxDQUFDO0NBQ0Y7OztJQUpDLDRCQUF1Qzs7SUFDM0IsMkJBQXFDOztBQUtuRCxNQUFNLE9BQU8sYUFBYTs7Ozs7SUFDeEIsWUFBbUIsU0FBc0IsRUFBUyxLQUE2QjtRQUE1RCxjQUFTLEdBQVQsU0FBUyxDQUFhO1FBQVMsVUFBSyxHQUFMLEtBQUssQ0FBd0I7SUFBRyxDQUFDO0NBQ3BGOzs7SUFEYSxrQ0FBNkI7O0lBQUUsOEJBQW9DOzs7Ozs7OztBQVFqRixNQUFNLFVBQVUsaUJBQWlCLENBQzdCLE1BQTJCLEVBQUUsSUFBeUIsRUFDdEQsY0FBc0M7O1VBQ2xDLFVBQVUsR0FBRyxNQUFNLENBQUMsS0FBSzs7VUFDekIsUUFBUSxHQUFHLElBQUksQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsSUFBSTtJQUV6QyxPQUFPLG1CQUFtQixDQUFDLFVBQVUs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QU0sSUFBSSxPQUFPLENBQUMsTUFBTSxDQUFDLFdBQVcsRUFBRTtRQUNsRSxNQUFNLENBQUMsbUJBQW1CLENBQUMsSUFBSSxDQUFDLElBQUksYUFBYSxDQUFDLE9BQU8sQ0FBQyxNQUFNLENBQUMsU0FBUyxFQUFFLENBQUMsQ0FBQyxDQUFDLENBQUM7S0FDakY7U0FBTTtRQUNMLE1BQU0sQ0FBQyxtQkFBbUIsQ0FBQyxJQUFJLENBQUMsSUFBSSxhQUFhLENBQUMsSUFBSSxFQUFFLENBQUMsQ0FBQyxDQUFDLENBQUM7S0FDN0Q7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0luamVjdG9yfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHtMb2FkZWRSb3V0ZXJDb25maWcsIFJ1bkd1YXJkc0FuZFJlc29sdmVyc30gZnJvbSAnLi4vY29uZmlnJztcbmltcG9ydCB7Q2hpbGRyZW5PdXRsZXRDb250ZXh0cywgT3V0bGV0Q29udGV4dH0gZnJvbSAnLi4vcm91dGVyX291dGxldF9jb250ZXh0JztcbmltcG9ydCB7QWN0aXZhdGVkUm91dGVTbmFwc2hvdCwgUm91dGVyU3RhdGVTbmFwc2hvdCwgZXF1YWxQYXJhbXNBbmRVcmxTZWdtZW50c30gZnJvbSAnLi4vcm91dGVyX3N0YXRlJztcbmltcG9ydCB7ZXF1YWxQYXRofSBmcm9tICcuLi91cmxfdHJlZSc7XG5pbXBvcnQge2ZvckVhY2gsIHNoYWxsb3dFcXVhbH0gZnJvbSAnLi4vdXRpbHMvY29sbGVjdGlvbic7XG5pbXBvcnQge1RyZWVOb2RlLCBub2RlQ2hpbGRyZW5Bc01hcH0gZnJvbSAnLi4vdXRpbHMvdHJlZSc7XG5cbmV4cG9ydCBjbGFzcyBDYW5BY3RpdmF0ZSB7XG4gIHJlYWRvbmx5IHJvdXRlOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90O1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgcGF0aDogQWN0aXZhdGVkUm91dGVTbmFwc2hvdFtdKSB7XG4gICAgdGhpcy5yb3V0ZSA9IHRoaXMucGF0aFt0aGlzLnBhdGgubGVuZ3RoIC0gMV07XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENhbkRlYWN0aXZhdGUge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgY29tcG9uZW50OiBPYmplY3R8bnVsbCwgcHVibGljIHJvdXRlOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90KSB7fVxufVxuXG5leHBvcnQgZGVjbGFyZSB0eXBlIENoZWNrcyA9IHtcbiAgY2FuRGVhY3RpdmF0ZUNoZWNrczogQ2FuRGVhY3RpdmF0ZVtdLFxuICBjYW5BY3RpdmF0ZUNoZWNrczogQ2FuQWN0aXZhdGVbXSxcbn07XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRBbGxSb3V0ZUd1YXJkcyhcbiAgICBmdXR1cmU6IFJvdXRlclN0YXRlU25hcHNob3QsIGN1cnI6IFJvdXRlclN0YXRlU25hcHNob3QsXG4gICAgcGFyZW50Q29udGV4dHM6IENoaWxkcmVuT3V0bGV0Q29udGV4dHMpIHtcbiAgY29uc3QgZnV0dXJlUm9vdCA9IGZ1dHVyZS5fcm9vdDtcbiAgY29uc3QgY3VyclJvb3QgPSBjdXJyID8gY3Vyci5fcm9vdCA6IG51bGw7XG5cbiAgcmV0dXJuIGdldENoaWxkUm91dGVHdWFyZHMoZnV0dXJlUm9vdCwgY3VyclJvb3QsIHBhcmVudENvbnRleHRzLCBbZnV0dXJlUm9vdC52YWx1ZV0pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0Q2FuQWN0aXZhdGVDaGlsZChwOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90KTpcbiAgICB7bm9kZTogQWN0aXZhdGVkUm91dGVTbmFwc2hvdCwgZ3VhcmRzOiBhbnlbXX18bnVsbCB7XG4gIGNvbnN0IGNhbkFjdGl2YXRlQ2hpbGQgPSBwLnJvdXRlQ29uZmlnID8gcC5yb3V0ZUNvbmZpZy5jYW5BY3RpdmF0ZUNoaWxkIDogbnVsbDtcbiAgaWYgKCFjYW5BY3RpdmF0ZUNoaWxkIHx8IGNhbkFjdGl2YXRlQ2hpbGQubGVuZ3RoID09PSAwKSByZXR1cm4gbnVsbDtcbiAgcmV0dXJuIHtub2RlOiBwLCBndWFyZHM6IGNhbkFjdGl2YXRlQ2hpbGR9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0VG9rZW4oXG4gICAgdG9rZW46IGFueSwgc25hcHNob3Q6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QsIG1vZHVsZUluamVjdG9yOiBJbmplY3Rvcik6IGFueSB7XG4gIGNvbnN0IGNvbmZpZyA9IGdldENsb3Nlc3RMb2FkZWRDb25maWcoc25hcHNob3QpO1xuICBjb25zdCBpbmplY3RvciA9IGNvbmZpZyA/IGNvbmZpZy5tb2R1bGUuaW5qZWN0b3IgOiBtb2R1bGVJbmplY3RvcjtcbiAgcmV0dXJuIGluamVjdG9yLmdldCh0b2tlbik7XG59XG5cbmZ1bmN0aW9uIGdldENsb3Nlc3RMb2FkZWRDb25maWcoc25hcHNob3Q6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QpOiBMb2FkZWRSb3V0ZXJDb25maWd8bnVsbCB7XG4gIGlmICghc25hcHNob3QpIHJldHVybiBudWxsO1xuXG4gIGZvciAobGV0IHMgPSBzbmFwc2hvdC5wYXJlbnQ7IHM7IHMgPSBzLnBhcmVudCkge1xuICAgIGNvbnN0IHJvdXRlID0gcy5yb3V0ZUNvbmZpZztcbiAgICBpZiAocm91dGUgJiYgcm91dGUuX2xvYWRlZENvbmZpZykgcmV0dXJuIHJvdXRlLl9sb2FkZWRDb25maWc7XG4gIH1cblxuICByZXR1cm4gbnVsbDtcbn1cblxuZnVuY3Rpb24gZ2V0Q2hpbGRSb3V0ZUd1YXJkcyhcbiAgICBmdXR1cmVOb2RlOiBUcmVlTm9kZTxBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90PiwgY3Vyck5vZGU6IFRyZWVOb2RlPEFjdGl2YXRlZFJvdXRlU25hcHNob3Q+fCBudWxsLFxuICAgIGNvbnRleHRzOiBDaGlsZHJlbk91dGxldENvbnRleHRzIHwgbnVsbCwgZnV0dXJlUGF0aDogQWN0aXZhdGVkUm91dGVTbmFwc2hvdFtdLFxuICAgIGNoZWNrczogQ2hlY2tzID0ge1xuICAgICAgY2FuRGVhY3RpdmF0ZUNoZWNrczogW10sXG4gICAgICBjYW5BY3RpdmF0ZUNoZWNrczogW11cbiAgICB9KTogQ2hlY2tzIHtcbiAgY29uc3QgcHJldkNoaWxkcmVuID0gbm9kZUNoaWxkcmVuQXNNYXAoY3Vyck5vZGUpO1xuXG4gIC8vIFByb2Nlc3MgdGhlIGNoaWxkcmVuIG9mIHRoZSBmdXR1cmUgcm91dGVcbiAgZnV0dXJlTm9kZS5jaGlsZHJlbi5mb3JFYWNoKGMgPT4ge1xuICAgIGdldFJvdXRlR3VhcmRzKGMsIHByZXZDaGlsZHJlbltjLnZhbHVlLm91dGxldF0sIGNvbnRleHRzLCBmdXR1cmVQYXRoLmNvbmNhdChbYy52YWx1ZV0pLCBjaGVja3MpO1xuICAgIGRlbGV0ZSBwcmV2Q2hpbGRyZW5bYy52YWx1ZS5vdXRsZXRdO1xuICB9KTtcblxuICAvLyBQcm9jZXNzIGFueSBjaGlsZHJlbiBsZWZ0IGZyb20gdGhlIGN1cnJlbnQgcm91dGUgKG5vdCBhY3RpdmUgZm9yIHRoZSBmdXR1cmUgcm91dGUpXG4gIGZvckVhY2goXG4gICAgICBwcmV2Q2hpbGRyZW4sICh2OiBUcmVlTm9kZTxBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90Piwgazogc3RyaW5nKSA9PlxuICAgICAgICAgICAgICAgICAgICAgICAgZGVhY3RpdmF0ZVJvdXRlQW5kSXRzQ2hpbGRyZW4odiwgY29udGV4dHMgIS5nZXRDb250ZXh0KGspLCBjaGVja3MpKTtcblxuICByZXR1cm4gY2hlY2tzO1xufVxuXG5mdW5jdGlvbiBnZXRSb3V0ZUd1YXJkcyhcbiAgICBmdXR1cmVOb2RlOiBUcmVlTm9kZTxBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90PiwgY3Vyck5vZGU6IFRyZWVOb2RlPEFjdGl2YXRlZFJvdXRlU25hcHNob3Q+LFxuICAgIHBhcmVudENvbnRleHRzOiBDaGlsZHJlbk91dGxldENvbnRleHRzIHwgbnVsbCwgZnV0dXJlUGF0aDogQWN0aXZhdGVkUm91dGVTbmFwc2hvdFtdLFxuICAgIGNoZWNrczogQ2hlY2tzID0ge1xuICAgICAgY2FuRGVhY3RpdmF0ZUNoZWNrczogW10sXG4gICAgICBjYW5BY3RpdmF0ZUNoZWNrczogW11cbiAgICB9KTogQ2hlY2tzIHtcbiAgY29uc3QgZnV0dXJlID0gZnV0dXJlTm9kZS52YWx1ZTtcbiAgY29uc3QgY3VyciA9IGN1cnJOb2RlID8gY3Vyck5vZGUudmFsdWUgOiBudWxsO1xuICBjb25zdCBjb250ZXh0ID0gcGFyZW50Q29udGV4dHMgPyBwYXJlbnRDb250ZXh0cy5nZXRDb250ZXh0KGZ1dHVyZU5vZGUudmFsdWUub3V0bGV0KSA6IG51bGw7XG5cbiAgLy8gcmV1c2luZyB0aGUgbm9kZVxuICBpZiAoY3VyciAmJiBmdXR1cmUucm91dGVDb25maWcgPT09IGN1cnIucm91dGVDb25maWcpIHtcbiAgICBjb25zdCBzaG91bGRSdW4gPVxuICAgICAgICBzaG91bGRSdW5HdWFyZHNBbmRSZXNvbHZlcnMoY3VyciwgZnV0dXJlLCBmdXR1cmUucm91dGVDb25maWcgIS5ydW5HdWFyZHNBbmRSZXNvbHZlcnMpO1xuICAgIGlmIChzaG91bGRSdW4pIHtcbiAgICAgIGNoZWNrcy5jYW5BY3RpdmF0ZUNoZWNrcy5wdXNoKG5ldyBDYW5BY3RpdmF0ZShmdXR1cmVQYXRoKSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIC8vIHdlIG5lZWQgdG8gc2V0IHRoZSBkYXRhXG4gICAgICBmdXR1cmUuZGF0YSA9IGN1cnIuZGF0YTtcbiAgICAgIGZ1dHVyZS5fcmVzb2x2ZWREYXRhID0gY3Vyci5fcmVzb2x2ZWREYXRhO1xuICAgIH1cblxuICAgIC8vIElmIHdlIGhhdmUgYSBjb21wb25lbnQsIHdlIG5lZWQgdG8gZ28gdGhyb3VnaCBhbiBvdXRsZXQuXG4gICAgaWYgKGZ1dHVyZS5jb21wb25lbnQpIHtcbiAgICAgIGdldENoaWxkUm91dGVHdWFyZHMoXG4gICAgICAgICAgZnV0dXJlTm9kZSwgY3Vyck5vZGUsIGNvbnRleHQgPyBjb250ZXh0LmNoaWxkcmVuIDogbnVsbCwgZnV0dXJlUGF0aCwgY2hlY2tzKTtcblxuICAgICAgLy8gaWYgd2UgaGF2ZSBhIGNvbXBvbmVudGxlc3Mgcm91dGUsIHdlIHJlY3Vyc2UgYnV0IGtlZXAgdGhlIHNhbWUgb3V0bGV0IG1hcC5cbiAgICB9IGVsc2Uge1xuICAgICAgZ2V0Q2hpbGRSb3V0ZUd1YXJkcyhmdXR1cmVOb2RlLCBjdXJyTm9kZSwgcGFyZW50Q29udGV4dHMsIGZ1dHVyZVBhdGgsIGNoZWNrcyk7XG4gICAgfVxuXG4gICAgaWYgKHNob3VsZFJ1bikge1xuICAgICAgY29uc3QgY29tcG9uZW50ID0gY29udGV4dCAmJiBjb250ZXh0Lm91dGxldCAmJiBjb250ZXh0Lm91dGxldC5jb21wb25lbnQgfHwgbnVsbDtcbiAgICAgIGNoZWNrcy5jYW5EZWFjdGl2YXRlQ2hlY2tzLnB1c2gobmV3IENhbkRlYWN0aXZhdGUoY29tcG9uZW50LCBjdXJyKSk7XG4gICAgfVxuICB9IGVsc2Uge1xuICAgIGlmIChjdXJyKSB7XG4gICAgICBkZWFjdGl2YXRlUm91dGVBbmRJdHNDaGlsZHJlbihjdXJyTm9kZSwgY29udGV4dCwgY2hlY2tzKTtcbiAgICB9XG5cbiAgICBjaGVja3MuY2FuQWN0aXZhdGVDaGVja3MucHVzaChuZXcgQ2FuQWN0aXZhdGUoZnV0dXJlUGF0aCkpO1xuICAgIC8vIElmIHdlIGhhdmUgYSBjb21wb25lbnQsIHdlIG5lZWQgdG8gZ28gdGhyb3VnaCBhbiBvdXRsZXQuXG4gICAgaWYgKGZ1dHVyZS5jb21wb25lbnQpIHtcbiAgICAgIGdldENoaWxkUm91dGVHdWFyZHMoZnV0dXJlTm9kZSwgbnVsbCwgY29udGV4dCA/IGNvbnRleHQuY2hpbGRyZW4gOiBudWxsLCBmdXR1cmVQYXRoLCBjaGVja3MpO1xuXG4gICAgICAvLyBpZiB3ZSBoYXZlIGEgY29tcG9uZW50bGVzcyByb3V0ZSwgd2UgcmVjdXJzZSBidXQga2VlcCB0aGUgc2FtZSBvdXRsZXQgbWFwLlxuICAgIH0gZWxzZSB7XG4gICAgICBnZXRDaGlsZFJvdXRlR3VhcmRzKGZ1dHVyZU5vZGUsIG51bGwsIHBhcmVudENvbnRleHRzLCBmdXR1cmVQYXRoLCBjaGVja3MpO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBjaGVja3M7XG59XG5cbmZ1bmN0aW9uIHNob3VsZFJ1bkd1YXJkc0FuZFJlc29sdmVycyhcbiAgICBjdXJyOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90LCBmdXR1cmU6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QsXG4gICAgbW9kZTogUnVuR3VhcmRzQW5kUmVzb2x2ZXJzIHwgdW5kZWZpbmVkKTogYm9vbGVhbiB7XG4gIGlmICh0eXBlb2YgbW9kZSA9PT0gJ2Z1bmN0aW9uJykge1xuICAgIHJldHVybiBtb2RlKGN1cnIsIGZ1dHVyZSk7XG4gIH1cbiAgc3dpdGNoIChtb2RlKSB7XG4gICAgY2FzZSAncGF0aFBhcmFtc0NoYW5nZSc6XG4gICAgICByZXR1cm4gIWVxdWFsUGF0aChjdXJyLnVybCwgZnV0dXJlLnVybCk7XG5cbiAgICBjYXNlICdwYXRoUGFyYW1zT3JRdWVyeVBhcmFtc0NoYW5nZSc6XG4gICAgICByZXR1cm4gIWVxdWFsUGF0aChjdXJyLnVybCwgZnV0dXJlLnVybCkgfHxcbiAgICAgICAgICAhc2hhbGxvd0VxdWFsKGN1cnIucXVlcnlQYXJhbXMsIGZ1dHVyZS5xdWVyeVBhcmFtcyk7XG5cbiAgICBjYXNlICdhbHdheXMnOlxuICAgICAgcmV0dXJuIHRydWU7XG5cbiAgICBjYXNlICdwYXJhbXNPclF1ZXJ5UGFyYW1zQ2hhbmdlJzpcbiAgICAgIHJldHVybiAhZXF1YWxQYXJhbXNBbmRVcmxTZWdtZW50cyhjdXJyLCBmdXR1cmUpIHx8XG4gICAgICAgICAgIXNoYWxsb3dFcXVhbChjdXJyLnF1ZXJ5UGFyYW1zLCBmdXR1cmUucXVlcnlQYXJhbXMpO1xuXG4gICAgY2FzZSAncGFyYW1zQ2hhbmdlJzpcbiAgICBkZWZhdWx0OlxuICAgICAgcmV0dXJuICFlcXVhbFBhcmFtc0FuZFVybFNlZ21lbnRzKGN1cnIsIGZ1dHVyZSk7XG4gIH1cbn1cblxuZnVuY3Rpb24gZGVhY3RpdmF0ZVJvdXRlQW5kSXRzQ2hpbGRyZW4oXG4gICAgcm91dGU6IFRyZWVOb2RlPEFjdGl2YXRlZFJvdXRlU25hcHNob3Q+LCBjb250ZXh0OiBPdXRsZXRDb250ZXh0IHwgbnVsbCwgY2hlY2tzOiBDaGVja3MpOiB2b2lkIHtcbiAgY29uc3QgY2hpbGRyZW4gPSBub2RlQ2hpbGRyZW5Bc01hcChyb3V0ZSk7XG4gIGNvbnN0IHIgPSByb3V0ZS52YWx1ZTtcblxuICBmb3JFYWNoKGNoaWxkcmVuLCAobm9kZTogVHJlZU5vZGU8QWN0aXZhdGVkUm91dGVTbmFwc2hvdD4sIGNoaWxkTmFtZTogc3RyaW5nKSA9PiB7XG4gICAgaWYgKCFyLmNvbXBvbmVudCkge1xuICAgICAgZGVhY3RpdmF0ZVJvdXRlQW5kSXRzQ2hpbGRyZW4obm9kZSwgY29udGV4dCwgY2hlY2tzKTtcbiAgICB9IGVsc2UgaWYgKGNvbnRleHQpIHtcbiAgICAgIGRlYWN0aXZhdGVSb3V0ZUFuZEl0c0NoaWxkcmVuKG5vZGUsIGNvbnRleHQuY2hpbGRyZW4uZ2V0Q29udGV4dChjaGlsZE5hbWUpLCBjaGVja3MpO1xuICAgIH0gZWxzZSB7XG4gICAgICBkZWFjdGl2YXRlUm91dGVBbmRJdHNDaGlsZHJlbihub2RlLCBudWxsLCBjaGVja3MpO1xuICAgIH1cbiAgfSk7XG5cbiAgaWYgKCFyLmNvbXBvbmVudCkge1xuICAgIGNoZWNrcy5jYW5EZWFjdGl2YXRlQ2hlY2tzLnB1c2gobmV3IENhbkRlYWN0aXZhdGUobnVsbCwgcikpO1xuICB9IGVsc2UgaWYgKGNvbnRleHQgJiYgY29udGV4dC5vdXRsZXQgJiYgY29udGV4dC5vdXRsZXQuaXNBY3RpdmF0ZWQpIHtcbiAgICBjaGVja3MuY2FuRGVhY3RpdmF0ZUNoZWNrcy5wdXNoKG5ldyBDYW5EZWFjdGl2YXRlKGNvbnRleHQub3V0bGV0LmNvbXBvbmVudCwgcikpO1xuICB9IGVsc2Uge1xuICAgIGNoZWNrcy5jYW5EZWFjdGl2YXRlQ2hlY2tzLnB1c2gobmV3IENhbkRlYWN0aXZhdGUobnVsbCwgcikpO1xuICB9XG59XG4iXX0=
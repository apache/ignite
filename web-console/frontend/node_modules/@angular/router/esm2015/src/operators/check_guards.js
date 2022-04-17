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
import { defer, from, of } from 'rxjs';
import { concatAll, concatMap, first, map, mergeMap } from 'rxjs/operators';
import { ActivationStart, ChildActivationStart } from '../events';
import { wrapIntoObservable } from '../utils/collection';
import { getCanActivateChild, getToken } from '../utils/preactivation';
import { isBoolean, isCanActivate, isCanActivateChild, isCanDeactivate, isFunction } from '../utils/type_guards';
import { prioritizedGuardValue } from './prioritized_guard_value';
/**
 * @param {?} moduleInjector
 * @param {?=} forwardEvent
 * @return {?}
 */
export function checkGuards(moduleInjector, forwardEvent) {
    return (/**
     * @param {?} source
     * @return {?}
     */
    function (source) {
        return source.pipe(mergeMap((/**
         * @param {?} t
         * @return {?}
         */
        t => {
            const { targetSnapshot, currentSnapshot, guards: { canActivateChecks, canDeactivateChecks } } = t;
            if (canDeactivateChecks.length === 0 && canActivateChecks.length === 0) {
                return of(Object.assign({}, t, { guardsResult: true }));
            }
            return runCanDeactivateChecks(canDeactivateChecks, (/** @type {?} */ (targetSnapshot)), currentSnapshot, moduleInjector)
                .pipe(mergeMap((/**
             * @param {?} canDeactivate
             * @return {?}
             */
            canDeactivate => {
                return canDeactivate && isBoolean(canDeactivate) ?
                    runCanActivateChecks((/** @type {?} */ (targetSnapshot)), canActivateChecks, moduleInjector, forwardEvent) :
                    of(canDeactivate);
            })), map((/**
             * @param {?} guardsResult
             * @return {?}
             */
            guardsResult => (Object.assign({}, t, { guardsResult })))));
        })));
    });
}
/**
 * @param {?} checks
 * @param {?} futureRSS
 * @param {?} currRSS
 * @param {?} moduleInjector
 * @return {?}
 */
function runCanDeactivateChecks(checks, futureRSS, currRSS, moduleInjector) {
    return from(checks).pipe(mergeMap((/**
     * @param {?} check
     * @return {?}
     */
    check => runCanDeactivate(check.component, check.route, currRSS, futureRSS, moduleInjector))), first((/**
     * @param {?} result
     * @return {?}
     */
    result => { return result !== true; }), (/** @type {?} */ (true))));
}
/**
 * @param {?} futureSnapshot
 * @param {?} checks
 * @param {?} moduleInjector
 * @param {?=} forwardEvent
 * @return {?}
 */
function runCanActivateChecks(futureSnapshot, checks, moduleInjector, forwardEvent) {
    return from(checks).pipe(concatMap((/**
     * @param {?} check
     * @return {?}
     */
    (check) => {
        return from([
            fireChildActivationStart(check.route.parent, forwardEvent),
            fireActivationStart(check.route, forwardEvent),
            runCanActivateChild(futureSnapshot, check.path, moduleInjector),
            runCanActivate(futureSnapshot, check.route, moduleInjector)
        ])
            .pipe(concatAll(), first((/**
         * @param {?} result
         * @return {?}
         */
        result => {
            return result !== true;
        }), (/** @type {?} */ (true))));
    })), first((/**
     * @param {?} result
     * @return {?}
     */
    result => { return result !== true; }), (/** @type {?} */ (true))));
}
/**
 * This should fire off `ActivationStart` events for each route being activated at this
 * level.
 * In other words, if you're activating `a` and `b` below, `path` will contain the
 * `ActivatedRouteSnapshot`s for both and we will fire `ActivationStart` for both. Always
 * return
 * `true` so checks continue to run.
 * @param {?} snapshot
 * @param {?=} forwardEvent
 * @return {?}
 */
function fireActivationStart(snapshot, forwardEvent) {
    if (snapshot !== null && forwardEvent) {
        forwardEvent(new ActivationStart(snapshot));
    }
    return of(true);
}
/**
 * This should fire off `ChildActivationStart` events for each route being activated at this
 * level.
 * In other words, if you're activating `a` and `b` below, `path` will contain the
 * `ActivatedRouteSnapshot`s for both and we will fire `ChildActivationStart` for both. Always
 * return
 * `true` so checks continue to run.
 * @param {?} snapshot
 * @param {?=} forwardEvent
 * @return {?}
 */
function fireChildActivationStart(snapshot, forwardEvent) {
    if (snapshot !== null && forwardEvent) {
        forwardEvent(new ChildActivationStart(snapshot));
    }
    return of(true);
}
/**
 * @param {?} futureRSS
 * @param {?} futureARS
 * @param {?} moduleInjector
 * @return {?}
 */
function runCanActivate(futureRSS, futureARS, moduleInjector) {
    /** @type {?} */
    const canActivate = futureARS.routeConfig ? futureARS.routeConfig.canActivate : null;
    if (!canActivate || canActivate.length === 0)
        return of(true);
    /** @type {?} */
    const canActivateObservables = canActivate.map((/**
     * @param {?} c
     * @return {?}
     */
    (c) => {
        return defer((/**
         * @return {?}
         */
        () => {
            /** @type {?} */
            const guard = getToken(c, futureARS, moduleInjector);
            /** @type {?} */
            let observable;
            if (isCanActivate(guard)) {
                observable = wrapIntoObservable(guard.canActivate(futureARS, futureRSS));
            }
            else if (isFunction(guard)) {
                observable = wrapIntoObservable(guard(futureARS, futureRSS));
            }
            else {
                throw new Error('Invalid CanActivate guard');
            }
            return observable.pipe(first());
        }));
    }));
    return of(canActivateObservables).pipe(prioritizedGuardValue());
}
/**
 * @param {?} futureRSS
 * @param {?} path
 * @param {?} moduleInjector
 * @return {?}
 */
function runCanActivateChild(futureRSS, path, moduleInjector) {
    /** @type {?} */
    const futureARS = path[path.length - 1];
    /** @type {?} */
    const canActivateChildGuards = path.slice(0, path.length - 1)
        .reverse()
        .map((/**
     * @param {?} p
     * @return {?}
     */
    p => getCanActivateChild(p)))
        .filter((/**
     * @param {?} _
     * @return {?}
     */
    _ => _ !== null));
    /** @type {?} */
    const canActivateChildGuardsMapped = canActivateChildGuards.map((/**
     * @param {?} d
     * @return {?}
     */
    (d) => {
        return defer((/**
         * @return {?}
         */
        () => {
            /** @type {?} */
            const guardsMapped = d.guards.map((/**
             * @param {?} c
             * @return {?}
             */
            (c) => {
                /** @type {?} */
                const guard = getToken(c, d.node, moduleInjector);
                /** @type {?} */
                let observable;
                if (isCanActivateChild(guard)) {
                    observable = wrapIntoObservable(guard.canActivateChild(futureARS, futureRSS));
                }
                else if (isFunction(guard)) {
                    observable = wrapIntoObservable(guard(futureARS, futureRSS));
                }
                else {
                    throw new Error('Invalid CanActivateChild guard');
                }
                return observable.pipe(first());
            }));
            return of(guardsMapped).pipe(prioritizedGuardValue());
        }));
    }));
    return of(canActivateChildGuardsMapped).pipe(prioritizedGuardValue());
}
/**
 * @param {?} component
 * @param {?} currARS
 * @param {?} currRSS
 * @param {?} futureRSS
 * @param {?} moduleInjector
 * @return {?}
 */
function runCanDeactivate(component, currARS, currRSS, futureRSS, moduleInjector) {
    /** @type {?} */
    const canDeactivate = currARS && currARS.routeConfig ? currARS.routeConfig.canDeactivate : null;
    if (!canDeactivate || canDeactivate.length === 0)
        return of(true);
    /** @type {?} */
    const canDeactivateObservables = canDeactivate.map((/**
     * @param {?} c
     * @return {?}
     */
    (c) => {
        /** @type {?} */
        const guard = getToken(c, currARS, moduleInjector);
        /** @type {?} */
        let observable;
        if (isCanDeactivate(guard)) {
            observable =
                wrapIntoObservable(guard.canDeactivate((/** @type {?} */ (component)), currARS, currRSS, futureRSS));
        }
        else if (isFunction(guard)) {
            observable = wrapIntoObservable(guard(component, currARS, currRSS, futureRSS));
        }
        else {
            throw new Error('Invalid CanDeactivate guard');
        }
        return observable.pipe(first());
    }));
    return of(canDeactivateObservables).pipe(prioritizedGuardValue());
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY2hlY2tfZ3VhcmRzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcm91dGVyL3NyYy9vcGVyYXRvcnMvY2hlY2tfZ3VhcmRzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBU0EsT0FBTyxFQUF1QyxLQUFLLEVBQUUsSUFBSSxFQUFFLEVBQUUsRUFBRSxNQUFNLE1BQU0sQ0FBQztBQUM1RSxPQUFPLEVBQUMsU0FBUyxFQUFFLFNBQVMsRUFBRSxLQUFLLEVBQUUsR0FBRyxFQUFFLFFBQVEsRUFBQyxNQUFNLGdCQUFnQixDQUFDO0FBRTFFLE9BQU8sRUFBQyxlQUFlLEVBQUUsb0JBQW9CLEVBQVEsTUFBTSxXQUFXLENBQUM7QUFLdkUsT0FBTyxFQUFDLGtCQUFrQixFQUFDLE1BQU0scUJBQXFCLENBQUM7QUFDdkQsT0FBTyxFQUE2QixtQkFBbUIsRUFBRSxRQUFRLEVBQUMsTUFBTSx3QkFBd0IsQ0FBQztBQUNqRyxPQUFPLEVBQUMsU0FBUyxFQUFFLGFBQWEsRUFBRSxrQkFBa0IsRUFBRSxlQUFlLEVBQUUsVUFBVSxFQUFDLE1BQU0sc0JBQXNCLENBQUM7QUFFL0csT0FBTyxFQUFDLHFCQUFxQixFQUFDLE1BQU0sMkJBQTJCLENBQUM7Ozs7OztBQUVoRSxNQUFNLFVBQVUsV0FBVyxDQUFDLGNBQXdCLEVBQUUsWUFBbUM7SUFFdkY7Ozs7SUFBTyxVQUFTLE1BQXdDO1FBRXRELE9BQU8sTUFBTSxDQUFDLElBQUksQ0FBQyxRQUFROzs7O1FBQUMsQ0FBQyxDQUFDLEVBQUU7a0JBQ3hCLEVBQUMsY0FBYyxFQUFFLGVBQWUsRUFBRSxNQUFNLEVBQUUsRUFBQyxpQkFBaUIsRUFBRSxtQkFBbUIsRUFBQyxFQUFDLEdBQUcsQ0FBQztZQUM3RixJQUFJLG1CQUFtQixDQUFDLE1BQU0sS0FBSyxDQUFDLElBQUksaUJBQWlCLENBQUMsTUFBTSxLQUFLLENBQUMsRUFBRTtnQkFDdEUsT0FB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a0JBQWtCLENBQUMsS0FBSyxDQUFDLFNBQVMsRUFBRSxPQUFPLEVBQUUsT0FBTyxFQUFFLFNBQVMsQ0FBQyxDQUFDLENBQUM7U0FDaEY7YUFBTTtZQUNMLE1BQU0sSUFBSSxLQUFLLENBQUMsNkJBQTZCLENBQUMsQ0FBQztTQUNoRDtRQUNELE9BQU8sVUFBVSxDQUFDLElBQUksQ0FBQyxLQUFLLEVBQUUsQ0FBQyxDQUFDO0lBQ2xDLENBQUMsRUFBQztJQUNGLE9BQU8sRUFBRSxDQUFFLHdCQUF3QixDQUFDLENBQUMsSUFBSSxDQUFDLHFCQUFxQixFQUFFLENBQUMsQ0FBQztBQUNyRSxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0luamVjdG9yfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7TW9ub1R5cGVPcGVyYXRvckZ1bmN0aW9uLCBPYnNlcnZhYmxlLCBkZWZlciwgZnJvbSwgb2YgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7Y29uY2F0QWxsLCBjb25jYXRNYXAsIGZpcnN0LCBtYXAsIG1lcmdlTWFwfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5cbmltcG9ydCB7QWN0aXZhdGlvblN0YXJ0LCBDaGlsZEFjdGl2YXRpb25TdGFydCwgRXZlbnR9IGZyb20gJy4uL2V2ZW50cyc7XG5pbXBvcnQge0NhbkFjdGl2YXRlQ2hpbGRGbiwgQ2FuQWN0aXZhdGVGbiwgQ2FuRGVhY3RpdmF0ZUZufSBmcm9tICcuLi9pbnRlcmZhY2VzJztcbmltcG9ydCB7TmF2aWdhdGlvblRyYW5zaXRpb259IGZyb20gJy4uL3JvdXRlcic7XG5pbXBvcnQge0FjdGl2YXRlZFJvdXRlU25hcHNob3QsIFJvdXRlclN0YXRlU25hcHNob3R9IGZyb20gJy4uL3JvdXRlcl9zdGF0ZSc7XG5pbXBvcnQge1VybFRyZWV9IGZyb20gJy4uL3VybF90cmVlJztcbmltcG9ydCB7d3JhcEludG9PYnNlcnZhYmxlfSBmcm9tICcuLi91dGlscy9jb2xsZWN0aW9uJztcbmltcG9ydCB7Q2FuQWN0aXZhdGUsIENhbkRlYWN0aXZhdGUsIGdldENhbkFjdGl2YXRlQ2hpbGQsIGdldFRva2VufSBmcm9tICcuLi91dGlscy9wcmVhY3RpdmF0aW9uJztcbmltcG9ydCB7aXNCb29sZWFuLCBpc0NhbkFjdGl2YXRlLCBpc0NhbkFjdGl2YXRlQ2hpbGQsIGlzQ2FuRGVhY3RpdmF0ZSwgaXNGdW5jdGlvbn0gZnJvbSAnLi4vdXRpbHMvdHlwZV9ndWFyZHMnO1xuXG5pbXBvcnQge3ByaW9yaXRpemVkR3VhcmRWYWx1ZX0gZnJvbSAnLi9wcmlvcml0aXplZF9ndWFyZF92YWx1ZSc7XG5cbmV4cG9ydCBmdW5jdGlvbiBjaGVja0d1YXJkcyhtb2R1bGVJbmplY3RvcjogSW5qZWN0b3IsIGZvcndhcmRFdmVudD86IChldnQ6IEV2ZW50KSA9PiB2b2lkKTpcbiAgICBNb25vVHlwZU9wZXJhdG9yRnVuY3Rpb248TmF2aWdhdGlvblRyYW5zaXRpb24+IHtcbiAgcmV0dXJuIGZ1bmN0aW9uKHNvdXJjZTogT2JzZXJ2YWJsZTxOYXZpZ2F0aW9uVHJhbnNpdGlvbj4pIHtcblxuICAgIHJldHVybiBzb3VyY2UucGlwZShtZXJnZU1hcCh0ID0+IHtcbiAgICAgIGNvbnN0IHt0YXJnZXRTbmFwc2hvdCwgY3VycmVudFNuYXBzaG90LCBndWFyZHM6IHtjYW5BY3RpdmF0ZUNoZWNrcywgY2FuRGVhY3RpdmF0ZUNoZWNrc319ID0gdDtcbiAgICAgIGlmIChjYW5EZWFjdGl2YXRlQ2hlY2tzLmxlbmd0aCA9PT0gMCAmJiBjYW5BY3RpdmF0ZUNoZWNrcy5sZW5ndGggPT09IDApIHtcbiAgICAgICAgcmV0dXJuIG9mICh7Li4udCwgZ3VhcmRzUmVzdWx0OiB0cnVlfSk7XG4gICAgICB9XG5cbiAgICAgIHJldHVybiBydW5DYW5EZWFjdGl2YXRlQ2hlY2tzKFxuICAgICAgICAgICAgICAgICBjYW5EZWFjdGl2YXRlQ2hlY2tzLCB0YXJnZXRTbmFwc2hvdCAhLCBjdXJyZW50U25hcHNob3QsIG1vZHVsZUluamVjdG9yKVxuICAgICAgICAgIC5waXBlKFxuICAgICAgICAgICAgICBtZXJnZU1hcChjYW5EZWFjdGl2YXRlID0+IHtcbiAgICAgICAgICAgICAgICByZXR1cm4gY2FuRGVhY3RpdmF0ZSAmJiBpc0Jvb2xlYW4oY2FuRGVhY3RpdmF0ZSkgP1xuICAgICAgICAgICAgICAgICAgICBydW5DYW5BY3RpdmF0ZUNoZWNrcyhcbiAgICAgICAgICAgICAgICAgICAgICAgIHRhcmdldFNuYXBzaG90ICEsIGNhbkFjdGl2YXRlQ2hlY2tzLCBtb2R1bGVJbmplY3RvciwgZm9yd2FyZEV2ZW50KSA6XG4gICAgICAgICAgICAgICAgICAgIG9mIChjYW5EZWFjdGl2YXRlKTtcbiAgICAgICAgICAgICAgfSksXG4gICAgICAgICAgICAgIG1hcChndWFyZHNSZXN1bHQgPT4gKHsuLi50LCBndWFyZHNSZXN1bHR9KSkpO1xuICAgIH0pKTtcbiAgfTtcbn1cblxuZnVuY3Rpb24gcnVuQ2FuRGVhY3RpdmF0ZUNoZWNrcyhcbiAgICBjaGVja3M6IENhbkRlYWN0aXZhdGVbXSwgZnV0dXJlUlNTOiBSb3V0ZXJTdGF0ZVNuYXBzaG90LCBjdXJyUlNTOiBSb3V0ZXJTdGF0ZVNuYXBzaG90LFxuICAgIG1vZHVsZUluamVjdG9yOiBJbmplY3Rvcikge1xuICByZXR1cm4gZnJvbShjaGVja3MpLnBpcGUoXG4gICAgICBtZXJnZU1hcChcbiAgICAgICAgICBjaGVjayA9PlxuICAgICAgICAgICAgICBydW5DYW5EZWFjdGl2YXRlKGNoZWNrLmNvbXBvbmVudCwgY2hlY2sucm91dGUsIGN1cnJSU1MsIGZ1dHVyZVJTUywgbW9kdWxlSW5qZWN0b3IpKSxcbiAgICAgIGZpcnN0KHJlc3VsdCA9PiB7IHJldHVybiByZXN1bHQgIT09IHRydWU7IH0sIHRydWUgYXMgYm9vbGVhbiB8IFVybFRyZWUpKTtcbn1cblxuZnVuY3Rpb24gcnVuQ2FuQWN0aXZhdGVDaGVja3MoXG4gICAgZnV0dXJlU25hcHNob3Q6IFJvdXRlclN0YXRlU25hcHNob3QsIGNoZWNrczogQ2FuQWN0aXZhdGVbXSwgbW9kdWxlSW5qZWN0b3I6IEluamVjdG9yLFxuICAgIGZvcndhcmRFdmVudD86IChldnQ6IEV2ZW50KSA9PiB2b2lkKSB7XG4gIHJldHVybiBmcm9tKGNoZWNrcykucGlwZShcbiAgICAgIGNvbmNhdE1hcCgoY2hlY2s6IENhbkFjdGl2YXRlKSA9PiB7XG4gICAgICAgIHJldHVybiBmcm9tKFtcbiAgICAgICAgICAgICAgICAgZmlyZUNoaWxkQWN0aXZhdGlvblN0YXJ0KGNoZWNrLnJvdXRlLnBhcmVudCwgZm9yd2FyZEV2ZW50KSxcbiAgICAgICAgICAgICAgICAgZmlyZUFjdGl2YXRpb25TdGFydChjaGVjay5yb3V0ZSwgZm9yd2FyZEV2ZW50KSxcbiAgICAgICAgICAgICAgICAgcnVuQ2FuQWN0aXZhdGVDaGlsZChmdXR1cmVTbmFwc2hvdCwgY2hlY2sucGF0aCwgbW9kdWxlSW5qZWN0b3IpLFxuICAgICAgICAgICAgICAgICBydW5DYW5BY3RpdmF0ZShmdXR1cmVTbmFwc2hvdCwgY2hlY2sucm91dGUsIG1vZHVsZUluamVjdG9yKVxuICAgICAgICAgICAgICAgXSlcbiAgICAgICAgICAgIC5waXBlKGNvbmNhdEFsbCgpLCBmaXJzdChyZXN1bHQgPT4ge1xuICAgICAgICAgICAgICAgICAgICByZXR1cm4gcmVzdWx0ICE9PSB0cnVlO1xuICAgICAgICAgICAgICAgICAgfSwgdHJ1ZSBhcyBib29sZWFuIHwgVXJsVHJlZSkpO1xuICAgICAgfSksXG4gICAgICBmaXJzdChyZXN1bHQgPT4geyByZXR1cm4gcmVzdWx0ICE9PSB0cnVlOyB9LCB0cnVlIGFzIGJvb2xlYW4gfCBVcmxUcmVlKSk7XG59XG5cbi8qKlxuICAgKiBUaGlzIHNob3VsZCBmaXJlIG9mZiBgQWN0aXZhdGlvblN0YXJ0YCBldmVudHMgZm9yIGVhY2ggcm91dGUgYmVpbmcgYWN0aXZhdGVkIGF0IHRoaXNcbiAgICogbGV2ZWwuXG4gICAqIEluIG90aGVyIHdvcmRzLCBpZiB5b3UncmUgYWN0aXZhdGluZyBgYWAgYW5kIGBiYCBiZWxvdywgYHBhdGhgIHdpbGwgY29udGFpbiB0aGVcbiAgICogYEFjdGl2YXRlZFJvdXRlU25hcHNob3RgcyBmb3IgYm90aCBhbmQgd2Ugd2lsbCBmaXJlIGBBY3RpdmF0aW9uU3RhcnRgIGZvciBib3RoLiBBbHdheXNcbiAgICogcmV0dXJuXG4gICAqIGB0cnVlYCBzbyBjaGVja3MgY29udGludWUgdG8gcnVuLlxuICAgKi9cbmZ1bmN0aW9uIGZpcmVBY3RpdmF0aW9uU3RhcnQoXG4gICAgc25hcHNob3Q6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QgfCBudWxsLFxuICAgIGZvcndhcmRFdmVudD86IChldnQ6IEV2ZW50KSA9PiB2b2lkKTogT2JzZXJ2YWJsZTxib29sZWFuPiB7XG4gIGlmIChzbmFwc2hvdCAhPT0gbnVsbCAmJiBmb3J3YXJkRXZlbnQpIHtcbiAgICBmb3J3YXJkRXZlbnQobmV3IEFjdGl2YXRpb25TdGFydChzbmFwc2hvdCkpO1xuICB9XG4gIHJldHVybiBvZiAodHJ1ZSk7XG59XG5cbi8qKlxuICAgKiBUaGlzIHNob3VsZCBmaXJlIG9mZiBgQ2hpbGRBY3RpdmF0aW9uU3RhcnRgIGV2ZW50cyBmb3IgZWFjaCByb3V0ZSBiZWluZyBhY3RpdmF0ZWQgYXQgdGhpc1xuICAgKiBsZXZlbC5cbiAgICogSW4gb3RoZXIgd29yZHMsIGlmIHlvdSdyZSBhY3RpdmF0aW5nIGBhYCBhbmQgYGJgIGJlbG93LCBgcGF0aGAgd2lsbCBjb250YWluIHRoZVxuICAgKiBgQWN0aXZhdGVkUm91dGVTbmFwc2hvdGBzIGZvciBib3RoIGFuZCB3ZSB3aWxsIGZpcmUgYENoaWxkQWN0aXZhdGlvblN0YXJ0YCBmb3IgYm90aC4gQWx3YXlzXG4gICAqIHJldHVyblxuICAgKiBgdHJ1ZWAgc28gY2hlY2tzIGNvbnRpbnVlIHRvIHJ1bi5cbiAgICovXG5mdW5jdGlvbiBmaXJlQ2hpbGRBY3RpdmF0aW9uU3RhcnQoXG4gICAgc25hcHNob3Q6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QgfCBudWxsLFxuICAgIGZvcndhcmRFdmVudD86IChldnQ6IEV2ZW50KSA9PiB2b2lkKTogT2JzZXJ2YWJsZTxib29sZWFuPiB7XG4gIGlmIChzbmFwc2hvdCAhPT0gbnVsbCAmJiBmb3J3YXJkRXZlbnQpIHtcbiAgICBmb3J3YXJkRXZlbnQobmV3IENoaWxkQWN0aXZhdGlvblN0YXJ0KHNuYXBzaG90KSk7XG4gIH1cbiAgcmV0dXJuIG9mICh0cnVlKTtcbn1cblxuZnVuY3Rpb24gcnVuQ2FuQWN0aXZhdGUoXG4gICAgZnV0dXJlUlNTOiBSb3V0ZXJTdGF0ZVNuYXBzaG90LCBmdXR1cmVBUlM6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QsXG4gICAgbW9kdWxlSW5qZWN0b3I6IEluamVjdG9yKTogT2JzZXJ2YWJsZTxib29sZWFufFVybFRyZWU+IHtcbiAgY29uc3QgY2FuQWN0aXZhdGUgPSBmdXR1cmVBUlMucm91dGVDb25maWcgPyBmdXR1cmVBUlMucm91dGVDb25maWcuY2FuQWN0aXZhdGUgOiBudWxsO1xuICBpZiAoIWNhbkFjdGl2YXRlIHx8IGNhbkFjdGl2YXRlLmxlbmd0aCA9PT0gMCkgcmV0dXJuIG9mICh0cnVlKTtcblxuICBjb25zdCBjYW5BY3RpdmF0ZU9ic2VydmFibGVzID0gY2FuQWN0aXZhdGUubWFwKChjOiBhbnkpID0+IHtcbiAgICByZXR1cm4gZGVmZXIoKCkgPT4ge1xuICAgICAgY29uc3QgZ3VhcmQgPSBnZXRUb2tlbihjLCBmdXR1cmVBUlMsIG1vZHVsZUluamVjdG9yKTtcbiAgICAgIGxldCBvYnNlcnZhYmxlO1xuICAgICAgaWYgKGlzQ2FuQWN0aXZhdGUoZ3VhcmQpKSB7XG4gICAgICAgIG9ic2VydmFibGUgPSB3cmFwSW50b09ic2VydmFibGUoZ3VhcmQuY2FuQWN0aXZhdGUoZnV0dXJlQVJTLCBmdXR1cmVSU1MpKTtcbiAgICAgIH0gZWxzZSBpZiAoaXNGdW5jdGlvbjxDYW5BY3RpdmF0ZUZuPihndWFyZCkpIHtcbiAgICAgICAgb2JzZXJ2YWJsZSA9IHdyYXBJbnRvT2JzZXJ2YWJsZShndWFyZChmdXR1cmVBUlMsIGZ1dHVyZVJTUykpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdJbnZhbGlkIENhbkFjdGl2YXRlIGd1YXJkJyk7XG4gICAgICB9XG4gICAgICByZXR1cm4gb2JzZXJ2YWJsZS5waXBlKGZpcnN0KCkpO1xuICAgIH0pO1xuICB9KTtcbiAgcmV0dXJuIG9mIChjYW5BY3RpdmF0ZU9ic2VydmFibGVzKS5waXBlKHByaW9yaXRpemVkR3VhcmRWYWx1ZSgpKTtcbn1cblxuZnVuY3Rpb24gcnVuQ2FuQWN0aXZhdGVDaGlsZChcbiAgICBmdXR1cmVSU1M6IFJvdXRlclN0YXRlU25hcHNob3QsIHBhdGg6IEFjdGl2YXRlZFJvdXRlU25hcHNob3RbXSxcbiAgICBtb2R1bGVJbmplY3RvcjogSW5qZWN0b3IpOiBPYnNlcnZhYmxlPGJvb2xlYW58VXJsVHJlZT4ge1xuICBjb25zdCBmdXR1cmVBUlMgPSBwYXRoW3BhdGgubGVuZ3RoIC0gMV07XG5cbiAgY29uc3QgY2FuQWN0aXZhdGVDaGlsZEd1YXJkcyA9IHBhdGguc2xpY2UoMCwgcGF0aC5sZW5ndGggLSAxKVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC5yZXZlcnNlKClcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAubWFwKHAgPT4gZ2V0Q2FuQWN0aXZhdGVDaGlsZChwKSlcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAuZmlsdGVyKF8gPT4gXyAhPT0gbnVsbCk7XG5cbiAgY29uc3QgY2FuQWN0aXZhdGVDaGlsZEd1YXJkc01hcHBlZCA9IGNhbkFjdGl2YXRlQ2hpbGRHdWFyZHMubWFwKChkOiBhbnkpID0+IHtcbiAgICByZXR1cm4gZGVmZXIoKCkgPT4ge1xuICAgICAgY29uc3QgZ3VhcmRzTWFwcGVkID0gZC5ndWFyZHMubWFwKChjOiBhbnkpID0+IHtcbiAgICAgICAgY29uc3QgZ3VhcmQgPSBnZXRUb2tlbihjLCBkLm5vZGUsIG1vZHVsZUluamVjdG9yKTtcbiAgICAgICAgbGV0IG9ic2VydmFibGU7XG4gICAgICAgIGlmIChpc0NhbkFjdGl2YXRlQ2hpbGQoZ3VhcmQpKSB7XG4gICAgICAgICAgb2JzZXJ2YWJsZSA9IHdyYXBJbnRvT2JzZXJ2YWJsZShndWFyZC5jYW5BY3RpdmF0ZUNoaWxkKGZ1dHVyZUFSUywgZnV0dXJlUlNTKSk7XG4gICAgICAgIH0gZWxzZSBpZiAoaXNGdW5jdGlvbjxDYW5BY3RpdmF0ZUNoaWxkRm4+KGd1YXJkKSkge1xuICAgICAgICAgIG9ic2VydmFibGUgPSB3cmFwSW50b09ic2VydmFibGUoZ3VhcmQoZnV0dXJlQVJTLCBmdXR1cmVSU1MpKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoJ0ludmFsaWQgQ2FuQWN0aXZhdGVDaGlsZCBndWFyZCcpO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiBvYnNlcnZhYmxlLnBpcGUoZmlyc3QoKSk7XG4gICAgICB9KTtcbiAgICAgIHJldHVybiBvZiAoZ3VhcmRzTWFwcGVkKS5waXBlKHByaW9yaXRpemVkR3VhcmRWYWx1ZSgpKTtcbiAgICB9KTtcbiAgfSk7XG4gIHJldHVybiBvZiAoY2FuQWN0aXZhdGVDaGlsZEd1YXJkc01hcHBlZCkucGlwZShwcmlvcml0aXplZEd1YXJkVmFsdWUoKSk7XG59XG5cbmZ1bmN0aW9uIHJ1bkNhbkRlYWN0aXZhdGUoXG4gICAgY29tcG9uZW50OiBPYmplY3QgfCBudWxsLCBjdXJyQVJTOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90LCBjdXJyUlNTOiBSb3V0ZXJTdGF0ZVNuYXBzaG90LFxuICAgIGZ1dHVyZVJTUzogUm91dGVyU3RhdGVTbmFwc2hvdCwgbW9kdWxlSW5qZWN0b3I6IEluamVjdG9yKTogT2JzZXJ2YWJsZTxib29sZWFufFVybFRyZWU+IHtcbiAgY29uc3QgY2FuRGVhY3RpdmF0ZSA9IGN1cnJBUlMgJiYgY3VyckFSUy5yb3V0ZUNvbmZpZyA/IGN1cnJBUlMucm91dGVDb25maWcuY2FuRGVhY3RpdmF0ZSA6IG51bGw7XG4gIGlmICghY2FuRGVhY3RpdmF0ZSB8fCBjYW5EZWFjdGl2YXRlLmxlbmd0aCA9PT0gMCkgcmV0dXJuIG9mICh0cnVlKTtcbiAgY29uc3QgY2FuRGVhY3RpdmF0ZU9ic2VydmFibGVzID0gY2FuRGVhY3RpdmF0ZS5tYXAoKGM6IGFueSkgPT4ge1xuICAgIGNvbnN0IGd1YXJkID0gZ2V0VG9rZW4oYywgY3VyckFSUywgbW9kdWxlSW5qZWN0b3IpO1xuICAgIGxldCBvYnNlcnZhYmxlO1xuICAgIGlmIChpc0NhbkRlYWN0aXZhdGUoZ3VhcmQpKSB7XG4gICAgICBvYnNlcnZhYmxlID1cbiAgICAgICAgICB3cmFwSW50b09ic2VydmFibGUoZ3VhcmQuY2FuRGVhY3RpdmF0ZShjb21wb25lbnQgISwgY3VyckFSUywgY3VyclJTUywgZnV0dXJlUlNTKSk7XG4gICAgfSBlbHNlIGlmIChpc0Z1bmN0aW9uPENhbkRlYWN0aXZhdGVGbjxhbnk+PihndWFyZCkpIHtcbiAgICAgIG9ic2VydmFibGUgPSB3cmFwSW50b09ic2VydmFibGUoZ3VhcmQoY29tcG9uZW50LCBjdXJyQVJTLCBjdXJyUlNTLCBmdXR1cmVSU1MpKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdJbnZhbGlkIENhbkRlYWN0aXZhdGUgZ3VhcmQnKTtcbiAgICB9XG4gICAgcmV0dXJuIG9ic2VydmFibGUucGlwZShmaXJzdCgpKTtcbiAgfSk7XG4gIHJldHVybiBvZiAoY2FuRGVhY3RpdmF0ZU9ic2VydmFibGVzKS5waXBlKHByaW9yaXRpemVkR3VhcmRWYWx1ZSgpKTtcbn1cbiJdfQ==
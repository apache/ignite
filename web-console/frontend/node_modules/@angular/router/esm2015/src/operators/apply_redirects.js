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
import { map, switchMap } from 'rxjs/operators';
import { applyRedirects as applyRedirectsFn } from '../apply_redirects';
/**
 * @param {?} moduleInjector
 * @param {?} configLoader
 * @param {?} urlSerializer
 * @param {?} config
 * @return {?}
 */
export function applyRedirects(moduleInjector, configLoader, urlSerializer, config) {
    return (/**
     * @param {?} source
     * @return {?}
     */
    function (source) {
        return source.pipe(switchMap((/**
         * @param {?} t
         * @return {?}
         */
        t => applyRedirectsFn(moduleInjector, configLoader, urlSerializer, t.extractedUrl, config)
            .pipe(map((/**
         * @param {?} urlAfterRedirects
         * @return {?}
         */
        urlAfterRedirects => (Object.assign({}, t, { urlAfterRedirects }))))))));
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYXBwbHlfcmVkaXJlY3RzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcm91dGVyL3NyYy9vcGVyYXRvcnMvYXBwbHlfcmVkaXJlY3RzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBVUEsT0FBTyxFQUFDLEdBQUcsRUFBRSxTQUFTLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUU5QyxPQUFPLEVBQUMsY0FBYyxJQUFJLGdCQUFnQixFQUFDLE1BQU0sb0JBQW9CLENBQUM7Ozs7Ozs7O0FBTXRFLE1BQU0sVUFBVSxjQUFjLENBQzFCLGNBQXdCLEVBQUUsWUFBZ0MsRUFBRSxhQUE0QixFQUN4RixNQUFjO0lBQ2hCOzs7O0lBQU8sVUFBUyxNQUF3QztRQUN0RCxPQUFPLE1BQU0sQ0FBQyxJQUFJLENBQUMsU0FBUzs7OztRQUN4QixDQUFDLENBQUMsRUFBRSxDQUFDLGdCQUFnQixDQUFDLGNBQWMsRUFBRSxZQUFZLEVBQUUsYUFBYSxFQUFFLENBQUMsQ0FBQyxZQUFZLEVBQUUsTUFBTSxDQUFDO2FBQ2hGLElBQUksQ0FBQyxHQUFHOzs7O1FBQUMsaUJBQWlCLENBQUMsRUFBRSxDQUFDLG1CQUFLLENBQUMsSUFBRSxpQkFBaUIsSUFBRSxFQUFDLENBQUMsRUFBQyxDQUFDLENBQUM7SUFDOUUsQ0FBQyxFQUFDO0FBQ0osQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtJbmplY3Rvcn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQge01vbm9UeXBlT3BlcmF0b3JGdW5jdGlvbiwgT2JzZXJ2YWJsZX0gZnJvbSAncnhqcyc7XG5pbXBvcnQge21hcCwgc3dpdGNoTWFwfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5cbmltcG9ydCB7YXBwbHlSZWRpcmVjdHMgYXMgYXBwbHlSZWRpcmVjdHNGbn0gZnJvbSAnLi4vYXBwbHlfcmVkaXJlY3RzJztcbmltcG9ydCB7Um91dGVzfSBmcm9tICcuLi9jb25maWcnO1xuaW1wb3J0IHtOYXZpZ2F0aW9uVHJhbnNpdGlvbn0gZnJvbSAnLi4vcm91dGVyJztcbmltcG9ydCB7Um91dGVyQ29uZmlnTG9hZGVyfSBmcm9tICcuLi9yb3V0ZXJfY29uZmlnX2xvYWRlcic7XG5pbXBvcnQge1VybFNlcmlhbGl6ZXJ9IGZyb20gJy4uL3VybF90cmVlJztcblxuZXhwb3J0IGZ1bmN0aW9uIGFwcGx5UmVkaXJlY3RzKFxuICAgIG1vZHVsZUluamVjdG9yOiBJbmplY3RvciwgY29uZmlnTG9hZGVyOiBSb3V0ZXJDb25maWdMb2FkZXIsIHVybFNlcmlhbGl6ZXI6IFVybFNlcmlhbGl6ZXIsXG4gICAgY29uZmlnOiBSb3V0ZXMpOiBNb25vVHlwZU9wZXJhdG9yRnVuY3Rpb248TmF2aWdhdGlvblRyYW5zaXRpb24+IHtcbiAgcmV0dXJuIGZ1bmN0aW9uKHNvdXJjZTogT2JzZXJ2YWJsZTxOYXZpZ2F0aW9uVHJhbnNpdGlvbj4pIHtcbiAgICByZXR1cm4gc291cmNlLnBpcGUoc3dpdGNoTWFwKFxuICAgICAgICB0ID0+IGFwcGx5UmVkaXJlY3RzRm4obW9kdWxlSW5qZWN0b3IsIGNvbmZpZ0xvYWRlciwgdXJsU2VyaWFsaXplciwgdC5leHRyYWN0ZWRVcmwsIGNvbmZpZylcbiAgICAgICAgICAgICAgICAgLnBpcGUobWFwKHVybEFmdGVyUmVkaXJlY3RzID0+ICh7Li4udCwgdXJsQWZ0ZXJSZWRpcmVjdHN9KSkpKSk7XG4gIH07XG59XG4iXX0=
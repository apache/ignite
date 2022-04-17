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
import { map, mergeMap } from 'rxjs/operators';
import { recognize as recognizeFn } from '../recognize';
/**
 * @param {?} rootComponentType
 * @param {?} config
 * @param {?} serializer
 * @param {?} paramsInheritanceStrategy
 * @param {?} relativeLinkResolution
 * @return {?}
 */
export function recognize(rootComponentType, config, serializer, paramsInheritanceStrategy, relativeLinkResolution) {
    return (/**
     * @param {?} source
     * @return {?}
     */
    function (source) {
        return source.pipe(mergeMap((/**
         * @param {?} t
         * @return {?}
         */
        t => recognizeFn(rootComponentType, config, t.urlAfterRedirects, serializer(t.urlAfterRedirects), paramsInheritanceStrategy, relativeLinkResolution)
            .pipe(map((/**
         * @param {?} targetSnapshot
         * @return {?}
         */
        targetSnapshot => (Object.assign({}, t, { targetSnapshot }))))))));
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVjb2duaXplLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcm91dGVyL3NyYy9vcGVyYXRvcnMvcmVjb2duaXplLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBVUEsT0FBTyxFQUFDLEdBQUcsRUFBRSxRQUFRLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUc3QyxPQUFPLEVBQUMsU0FBUyxJQUFJLFdBQVcsRUFBQyxNQUFNLGNBQWMsQ0FBQzs7Ozs7Ozs7O0FBSXRELE1BQU0sVUFBVSxTQUFTLENBQ3JCLGlCQUFrQyxFQUFFLE1BQWUsRUFBRSxVQUFvQyxFQUN6Rix5QkFBaUQsRUFBRSxzQkFDcEM7SUFDakI7Ozs7SUFBTyxVQUFTLE1BQXdDO1FBQ3RELE9BQU8sTUFBTSxDQUFDLElBQUksQ0FBQyxRQUFROzs7O1FBQ3ZCLENBQUMsQ0FBQyxFQUFFLENBQUMsV0FBVyxDQUNQLGlCQUFpQixFQUFFLE1BQU0sRUFBRSxDQUFDLENBQUMsaUJBQWlCLEVBQUUsVUFBVSxDQUFDLENBQUMsQ0FBQyxpQkFBaUIsQ0FBQyxFQUMvRSx5QkFBeUIsRUFBRSxzQkFBc0IsQ0FBQzthQUNqRCxJQUFJLENBQUMsR0FBRzs7OztRQUFDLGNBQWMsQ0FBQyxFQUFFLENBQUMsbUJBQUssQ0FBQyxJQUFFLGNBQWMsSUFBRSxFQUFDLENBQUMsRUFBQyxDQUFDLENBQUM7SUFDeEUsQ0FBQyxFQUFDO0FBQ0osQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtUeXBlfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7TW9ub1R5cGVPcGVyYXRvckZ1bmN0aW9uLCBPYnNlcnZhYmxlfSBmcm9tICdyeGpzJztcbmltcG9ydCB7bWFwLCBtZXJnZU1hcH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuXG5pbXBvcnQge1JvdXRlfSBmcm9tICcuLi9jb25maWcnO1xuaW1wb3J0IHtyZWNvZ25pemUgYXMgcmVjb2duaXplRm59IGZyb20gJy4uL3JlY29nbml6ZSc7XG5pbXBvcnQge05hdmlnYXRpb25UcmFuc2l0aW9ufSBmcm9tICcuLi9yb3V0ZXInO1xuaW1wb3J0IHtVcmxUcmVlfSBmcm9tICcuLi91cmxfdHJlZSc7XG5cbmV4cG9ydCBmdW5jdGlvbiByZWNvZ25pemUoXG4gICAgcm9vdENvbXBvbmVudFR5cGU6IFR5cGU8YW55PnwgbnVsbCwgY29uZmlnOiBSb3V0ZVtdLCBzZXJpYWxpemVyOiAodXJsOiBVcmxUcmVlKSA9PiBzdHJpbmcsXG4gICAgcGFyYW1zSW5oZXJpdGFuY2VTdHJhdGVneTogJ2VtcHR5T25seScgfCAnYWx3YXlzJywgcmVsYXRpdmVMaW5rUmVzb2x1dGlvbjogJ2xlZ2FjeScgfFxuICAgICAgICAnY29ycmVjdGVkJyk6IE1vbm9UeXBlT3BlcmF0b3JGdW5jdGlvbjxOYXZpZ2F0aW9uVHJhbnNpdGlvbj4ge1xuICByZXR1cm4gZnVuY3Rpb24oc291cmNlOiBPYnNlcnZhYmxlPE5hdmlnYXRpb25UcmFuc2l0aW9uPikge1xuICAgIHJldHVybiBzb3VyY2UucGlwZShtZXJnZU1hcChcbiAgICAgICAgdCA9PiByZWNvZ25pemVGbihcbiAgICAgICAgICAgICAgICAgcm9vdENvbXBvbmVudFR5cGUsIGNvbmZpZywgdC51cmxBZnRlclJlZGlyZWN0cywgc2VyaWFsaXplcih0LnVybEFmdGVyUmVkaXJlY3RzKSxcbiAgICAgICAgICAgICAgICAgcGFyYW1zSW5oZXJpdGFuY2VTdHJhdGVneSwgcmVsYXRpdmVMaW5rUmVzb2x1dGlvbilcbiAgICAgICAgICAgICAgICAgLnBpcGUobWFwKHRhcmdldFNuYXBzaG90ID0+ICh7Li4udCwgdGFyZ2V0U25hcHNob3R9KSkpKSk7XG4gIH07XG59XG4iXX0=
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
import { ChangeDetectorRef } from '../../change_detection/change_detector_ref';
import { getCompilerFacade } from '../../compiler/compiler_facade';
import { ReflectionCapabilities } from '../../reflection/reflection_capabilities';
import { Attribute, Host, Inject, Optional, Self, SkipSelf } from '../metadata';
/** @type {?} */
let _reflect = null;
/**
 * @return {?}
 */
export function getReflect() {
    return (_reflect = _reflect || new ReflectionCapabilities());
}
/**
 * @param {?} type
 * @return {?}
 */
export function reflectDependencies(type) {
    return convertDependencies(getReflect().parameters(type));
}
/**
 * @param {?} deps
 * @return {?}
 */
export function convertDependencies(deps) {
    /** @type {?} */
    const compiler = getCompilerFacade();
    return deps.map((/**
     * @param {?} dep
     * @return {?}
     */
    dep => reflectDependency(compiler, dep)));
}
/**
 * @param {?} compiler
 * @param {?} dep
 * @return {?}
 */
function reflectDependency(compiler, dep) {
    /** @type {?} */
    const meta = {
        token: null,
        host: false,
        optional: false,
        resolved: compiler.R3ResolvedDependencyType.Token,
        self: false,
        skipSelf: false,
    };
    /**
     * @param {?} token
     * @return {?}
     */
    function setTokenAndResolvedType(token) {
        meta.resolved = compiler.R3ResolvedDependencyType.Token;
        meta.token = token;
    }
    if (Array.isArray(dep)) {
        if (dep.length === 0) {
            throw new Error('Dependency array must have arguments.');
        }
        for (let j = 0; j < dep.length; j++) {
            /** @type {?} */
            const param = dep[j];
            if (param === undefined) {
                // param may be undefined if type of dep is not set by ngtsc
                continue;
            }
            else if (param instanceof Optional || param.__proto__.ngMetadataName === 'Optional') {
                meta.optional = true;
            }
            else if (param instanceof SkipSelf || param.__proto__.ngMetadataName === 'SkipSelf') {
                meta.skipSelf = true;
            }
            else if (param instanceof Self || param.__proto__.ngMetadataName === 'Self') {
                meta.self = true;
            }
            else if (param instanceof Host || param.__proto__.ngMetadataName === 'Host') {
                meta.host = true;
            }
            else if (param instanceof Inject) {
                meta.token = param.token;
            }
            else if (param instanceof Attribute) {
                if (param.attributeName === undefined) {
                    throw new Error(`Attribute name must be defined.`);
                }
                meta.token = param.attributeName;
                meta.resolved = compiler.R3ResolvedDependencyType.Attribute;
            }
            else if (param === ChangeDetectorRef) {
                meta.token = param;
                meta.resolved = compiler.R3ResolvedDependencyType.ChangeDetectorRef;
            }
            else {
                setTokenAndResolvedType(param);
            }
        }
    }
    else {
        setTokenAndResolvedType(dep);
    }
    return meta;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL2RpL2ppdC91dGlsLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLGlCQUFpQixFQUFDLE1BQU0sNENBQTRDLENBQUM7QUFDN0UsT0FBTyxFQUE2QyxpQkFBaUIsRUFBQyxNQUFNLGdDQUFnQyxDQUFDO0FBRTdHLE9BQU8sRUFBQyxzQkFBc0IsRUFBQyxNQUFNLDBDQUEwQyxDQUFDO0FBQ2hGLE9BQU8sRUFBQyxTQUFTLEVBQUUsSUFBSSxFQUFFLE1BQU0sRUFBRSxRQUFRLEVBQUUsSUFBSSxFQUFFLFFBQVEsRUFBQyxNQUFNLGFBQWEsQ0FBQzs7SUFFMUUsUUFBUSxHQUFnQyxJQUFJOzs7O0FBRWhELE1BQU0sVUFBVSxVQUFVO0lBQ3hCLE9BQU8sQ0FBQyxRQUFRLEdBQUcsUUFBUSxJQUFJLElBQUksc0JBQXNCLEVBQUUsQ0FBQyxDQUFDO0FBQy9ELENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLG1CQUFtQixDQUFDLElBQWU7SUFDakQsT0FBTyxtQkFBbUIsQ0FBQyxVQUFVLEVBQUUsQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQztBQUM1RCxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxtQkFBbUIsQ0FBQyxJQUFXOztVQUN2QyxRQUFRLEdBQUcsaUJBQWlCLEVBQUU7SUFDcEMsT0FBTyxJQUFJLENBQUMsR0FBRzs7OztJQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsaUJBQWlCLENBQUMsUUFBUSxFQUFFLEdBQUcsQ0FBQyxFQUFDLENBQUM7QUFDM0QsQ0FBQzs7Ozs7O0FBRUQsU0FBUyxpQkFBaUIsQ0FBQyxRQUF3QixFQUFFLEdBQWdCOztVQUM3RCxJQUFJLEdBQStCO1FBQ3ZDLEtBQUssRUFBRSxJQUFJO1FBQ1gsSUFBSSxFQUFFLEtBQUs7UUFDWCxRQUFRLEVBQUUsS0FBSztRQUNmLFFBQVEsRUFBRSxRQUFRLENBQUMsd0JBQXdCLENBQUMsS0FBSztRQUNqRCxJQUFJLEVBQUUsS0FBSztRQUNYLFFBQVEsRUFBRSxLQUFLO0tBQ2hCOzs7OztJQUVELFNBQVMsdUJBQXVCLENBQUMsS0FBVTtRQUN6QyxJQUFJLENBQUMsUUFBUSxHQUFHLFFBQVEsQ0FBQyx3QkFBd0IsQ0FBQyxLQUFLLENBQUM7UUFDeEQsSUFBSSxDQUFDLEtBQUssR0FBRyxLQUFLLENBQUM7SUFDckIsQ0FBQztJQUVELElBQUksS0FBSyxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsRUFBRTtRQUN0QixJQUFJLEdBQUcsQ0FBQyxNQUFNLEtBQUssQ0FBQyxFQUFFO1lBQ3BCLE1BQU0sSUFBSSxLQUFLLENBQUMsdUNBQXVDLENBQUMsQ0FBQztTQUMxRDtRQUNELEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxHQUFHLENBQUMsTUFBTSxFQUFFLENBQUMsRUFBRSxFQUFFOztrQkFDN0IsS0FBSyxHQUFHLEdBQUcsQ0FBQyxDQUFDLENBQUM7WUFDcEIsSUFBSSxLQUFLLEtBQUssU0FBUyxFQUFFO2dCQUN2Qiw0REFBNEQ7Z0JBQzVELFNBQVM7YUFDVjtpQkFBTSxJQUFJLEtBQUssWUFBWSxRQUFRLElBQUksS0FBSyxDQUFDLFNBQVMsQ0FBQyxjQUFjLEtBQUssVUFBVSxFQUFFO2dCQUNyRixJQUFJLENBQUMsUUFBUSxHQUFHLElBQUksQ0FBQzthQUN0QjtpQkFBTSxJQUFJLEtBQUssWUFBWSxRQUFRLElBQUksS0FBSyxDQUFDLFNBQVMsQ0FBQyxjQUFjLEtBQUssVUFBVSxFQUFFO2dCQUNyRixJQUFJLENBQUMsUUFBUSxHQUFHLElBQUksQ0FBQzthQUN0QjtpQkFBTSxJQUFJLEtBQUssWUFBWSxJQUFJLElBQUksS0FBSyxDQUFDLFNBQVMsQ0FBQyxjQUFjLEtBQUssTUFBTSxFQUFFO2dCQUM3RSxJQUFJLENBQUMsSUFBSSxHQUFHLElBQUksQ0FBQzthQUNsQjtpQkFBTSxJQUFJLEtBQUssWUFBWSxJQUFJLElBQUksS0FBSyxDQUFDLFNBQVMsQ0FBQyxjQUFjLEtBQUssTUFBTSxFQUFFO2dCQUM3RSxJQUFJLENBQUMsSUFBSSxHQUFHLElBQUksQ0FBQzthQUNsQjtpQkFBTSxJQUFJLEtBQUssWUFBWSxNQUFNLEVBQUU7Z0JBQ2xDLElBQUksQ0FBQyxLQUFLLEdBQUcsS0FBSyxDQUFDLEtBQUssQ0FBQzthQUMxQjtpQkFBTSxJQUFJLEtBQUssWUFBWSxTQUFTLEVBQUU7Z0JBQ3JDLElBQUksS0FBSyxDQUFDLGFBQWEsS0FBSyxTQUFTLEVBQUU7b0JBQ3JDLE1BQU0sSUFBSSxLQUFLLENBQUMsaUNBQWlDLENBQUMsQ0FBQztpQkFDcEQ7Z0JBQ0QsSUFBSSxDQUFDLEtBQUssR0FBRyxLQUFLLENBQUMsYUFBYSxDQUFDO2dCQUNqQyxJQUFJLENBQUMsUUFBUSxHQUFHLFFBQVEsQ0FBQyx3QkFBd0IsQ0FBQyxTQUFTLENBQUM7YUFDN0Q7aUJBQU0sSUFBSSxLQUFLLEtBQUssaUJBQWlCLEVBQUU7Z0JBQ3RDLElBQUksQ0FBQyxLQUFLLEdBQUcsS0FBSyxDQUFDO2dCQUNuQixJQUFJLENBQUMsUUFBUSxHQUFHLFFBQVEsQ0FBQyx3QkFBd0IsQ0FBQyxpQkFBaUIsQ0FBQzthQUNyRTtpQkFBTTtnQkFDTCx1QkFBdUIsQ0FBQyxLQUFLLENBQUMsQ0FBQzthQUNoQztTQUNGO0tBQ0Y7U0FBTTtRQUNMLHVCQUF1QixDQUFDLEdBQUcsQ0FBQyxDQUFDO0tBQzlCO0lBQ0QsT0FBTyxJQUFJLENBQUM7QUFDZCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NoYW5nZURldGVjdG9yUmVmfSBmcm9tICcuLi8uLi9jaGFuZ2VfZGV0ZWN0aW9uL2NoYW5nZV9kZXRlY3Rvcl9yZWYnO1xuaW1wb3J0IHtDb21waWxlckZhY2FkZSwgUjNEZXBlbmRlbmN5TWV0YWRhdGFGYWNhZGUsIGdldENvbXBpbGVyRmFjYWRlfSBmcm9tICcuLi8uLi9jb21waWxlci9jb21waWxlcl9mYWNhZGUnO1xuaW1wb3J0IHtUeXBlfSBmcm9tICcuLi8uLi9pbnRlcmZhY2UvdHlwZSc7XG5pbXBvcnQge1JlZmxlY3Rpb25DYXBhYmlsaXRpZXN9IGZyb20gJy4uLy4uL3JlZmxlY3Rpb24vcmVmbGVjdGlvbl9jYXBhYmlsaXRpZXMnO1xuaW1wb3J0IHtBdHRyaWJ1dGUsIEhvc3QsIEluamVjdCwgT3B0aW9uYWwsIFNlbGYsIFNraXBTZWxmfSBmcm9tICcuLi9tZXRhZGF0YSc7XG5cbmxldCBfcmVmbGVjdDogUmVmbGVjdGlvbkNhcGFiaWxpdGllc3xudWxsID0gbnVsbDtcblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlZmxlY3QoKTogUmVmbGVjdGlvbkNhcGFiaWxpdGllcyB7XG4gIHJldHVybiAoX3JlZmxlY3QgPSBfcmVmbGVjdCB8fCBuZXcgUmVmbGVjdGlvbkNhcGFiaWxpdGllcygpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlZmxlY3REZXBlbmRlbmNpZXModHlwZTogVHlwZTxhbnk+KTogUjNEZXBlbmRlbmN5TWV0YWRhdGFGYWNhZGVbXSB7XG4gIHJldHVybiBjb252ZXJ0RGVwZW5kZW5jaWVzKGdldFJlZmxlY3QoKS5wYXJhbWV0ZXJzKHR5cGUpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNvbnZlcnREZXBlbmRlbmNpZXMoZGVwczogYW55W10pOiBSM0RlcGVuZGVuY3lNZXRhZGF0YUZhY2FkZVtdIHtcbiAgY29uc3QgY29tcGlsZXIgPSBnZXRDb21waWxlckZhY2FkZSgpO1xuICByZXR1cm4gZGVwcy5tYXAoZGVwID0+IHJlZmxlY3REZXBlbmRlbmN5KGNvbXBpbGVyLCBkZXApKTtcbn1cblxuZnVuY3Rpb24gcmVmbGVjdERlcGVuZGVuY3koY29tcGlsZXI6IENvbXBpbGVyRmFjYWRlLCBkZXA6IGFueSB8IGFueVtdKTogUjNEZXBlbmRlbmN5TWV0YWRhdGFGYWNhZGUge1xuICBjb25zdCBtZXRhOiBSM0RlcGVuZGVuY3lNZXRhZGF0YUZhY2FkZSA9IHtcbiAgICB0b2tlbjogbnVsbCxcbiAgICBob3N0OiBmYWxzZSxcbiAgICBvcHRpb25hbDogZmFsc2UsXG4gICAgcmVzb2x2ZWQ6IGNvbXBpbGVyLlIzUmVzb2x2ZWREZXBlbmRlbmN5VHlwZS5Ub2tlbixcbiAgICBzZWxmOiBmYWxzZSxcbiAgICBza2lwU2VsZjogZmFsc2UsXG4gIH07XG5cbiAgZnVuY3Rpb24gc2V0VG9rZW5BbmRSZXNvbHZlZFR5cGUodG9rZW46IGFueSk6IHZvaWQge1xuICAgIG1ldGEucmVzb2x2ZWQgPSBjb21waWxlci5SM1Jlc29sdmVkRGVwZW5kZW5jeVR5cGUuVG9rZW47XG4gICAgbWV0YS50b2tlbiA9IHRva2VuO1xuICB9XG5cbiAgaWYgKEFycmF5LmlzQXJyYXkoZGVwKSkge1xuICAgIGlmIChkZXAubGVuZ3RoID09PSAwKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ0RlcGVuZGVuY3kgYXJyYXkgbXVzdCBoYXZlIGFyZ3VtZW50cy4nKTtcbiAgICB9XG4gICAgZm9yIChsZXQgaiA9IDA7IGogPCBkZXAubGVuZ3RoOyBqKyspIHtcbiAgICAgIGNvbnN0IHBhcmFtID0gZGVwW2pdO1xuICAgICAgaWYgKHBhcmFtID09PSB1bmRlZmluZWQpIHtcbiAgICAgICAgLy8gcGFyYW0gbWF5IGJlIHVuZGVmaW5lZCBpZiB0eXBlIG9mIGRlcCBpcyBub3Qgc2V0IGJ5IG5ndHNjXG4gICAgICAgIGNvbnRpbnVlO1xuICAgICAgfSBlbHNlIGlmIChwYXJhbSBpbnN0YW5jZW9mIE9wdGlvbmFsIHx8IHBhcmFtLl9fcHJvdG9fXy5uZ01ldGFkYXRhTmFtZSA9PT0gJ09wdGlvbmFsJykge1xuICAgICAgICBtZXRhLm9wdGlvbmFsID0gdHJ1ZTtcbiAgICAgIH0gZWxzZSBpZiAocGFyYW0gaW5zdGFuY2VvZiBTa2lwU2VsZiB8fCBwYXJhbS5fX3Byb3RvX18ubmdNZXRhZGF0YU5hbWUgPT09ICdTa2lwU2VsZicpIHtcbiAgICAgICAgbWV0YS5za2lwU2VsZiA9IHRydWU7XG4gICAgICB9IGVsc2UgaWYgKHBhcmFtIGluc3RhbmNlb2YgU2VsZiB8fCBwYXJhbS5fX3Byb3RvX18ubmdNZXRhZGF0YU5hbWUgPT09ICdTZWxmJykge1xuICAgICAgICBtZXRhLnNlbGYgPSB0cnVlO1xuICAgICAgfSBlbHNlIGlmIChwYXJhbSBpbnN0YW5jZW9mIEhvc3QgfHwgcGFyYW0uX19wcm90b19fLm5nTWV0YWRhdGFOYW1lID09PSAnSG9zdCcpIHtcbiAgICAgICAgbWV0YS5ob3N0ID0gdHJ1ZTtcbiAgICAgIH0gZWxzZSBpZiAocGFyYW0gaW5zdGFuY2VvZiBJbmplY3QpIHtcbiAgICAgICAgbWV0YS50b2tlbiA9IHBhcmFtLnRva2VuO1xuICAgICAgfSBlbHNlIGlmIChwYXJhbSBpbnN0YW5jZW9mIEF0dHJpYnV0ZSkge1xuICAgICAgICBpZiAocGFyYW0uYXR0cmlidXRlTmFtZSA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKGBBdHRyaWJ1dGUgbmFtZSBtdXN0IGJlIGRlZmluZWQuYCk7XG4gICAgICAgIH1cbiAgICAgICAgbWV0YS50b2tlbiA9IHBhcmFtLmF0dHJpYnV0ZU5hbWU7XG4gICAgICAgIG1ldGEucmVzb2x2ZWQgPSBjb21waWxlci5SM1Jlc29sdmVkRGVwZW5kZW5jeVR5cGUuQXR0cmlidXRlO1xuICAgICAgfSBlbHNlIGlmIChwYXJhbSA9PT0gQ2hhbmdlRGV0ZWN0b3JSZWYpIHtcbiAgICAgICAgbWV0YS50b2tlbiA9IHBhcmFtO1xuICAgICAgICBtZXRhLnJlc29sdmVkID0gY29tcGlsZXIuUjNSZXNvbHZlZERlcGVuZGVuY3lUeXBlLkNoYW5nZURldGVjdG9yUmVmO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgc2V0VG9rZW5BbmRSZXNvbHZlZFR5cGUocGFyYW0pO1xuICAgICAgfVxuICAgIH1cbiAgfSBlbHNlIHtcbiAgICBzZXRUb2tlbkFuZFJlc29sdmVkVHlwZShkZXApO1xuICB9XG4gIHJldHVybiBtZXRhO1xufVxuIl19
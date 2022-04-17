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
import { getCompilerFacade } from '../../compiler/compiler_facade';
import { reflectDependencies } from '../../di/jit/util';
import { NG_PIPE_DEF } from '../fields';
import { angularCoreEnv } from './environment';
/**
 * @param {?} type
 * @param {?} meta
 * @return {?}
 */
export function compilePipe(type, meta) {
    /** @type {?} */
    /** @nocollapse */ let ngPipeDef = null;
    Object.defineProperty(type, NG_PIPE_DEF, {
        get: (/**
         * @return {?}
         */
        () => {
            if (ngPipeDef === null) {
                /** @type {?} */
                const typeName = type.name;
                ngPipeDef =
                    getCompilerFacade().compilePipe(angularCoreEnv, `ng:///${typeName}/ngPipeDef.js`, {
                        type: type,
                        typeArgumentCount: 0,
                        name: typeName,
                        deps: reflectDependencies(type),
                        pipeName: meta.name,
                        pure: meta.pure !== undefined ? meta.pure : true
                    });
            }
            return ngPipeDef;
        }),
        // Make the property configurable in dev mode to allow overriding in tests
        configurable: !!ngDevMode,
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGlwZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3JlbmRlcjMvaml0L3BpcGUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsaUJBQWlCLEVBQUMsTUFBTSxnQ0FBZ0MsQ0FBQztBQUNqRSxPQUFPLEVBQUMsbUJBQW1CLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUd0RCxPQUFPLEVBQUMsV0FBVyxFQUFDLE1BQU0sV0FBVyxDQUFDO0FBRXRDLE9BQU8sRUFBQyxjQUFjLEVBQUMsTUFBTSxlQUFlLENBQUM7Ozs7OztBQUU3QyxNQUFNLFVBQVUsV0FBVyxDQUFDLElBQWUsRUFBRSxJQUFVOztRQUNqRCxTQUFTLEdBQVEsSUFBSTtJQUN6QixNQUFNLENBQUMsY0FBYyxDQUFDLElBQUksRUFBRSxXQUFXLEVBQUU7UUFDdkMsR0FBRzs7O1FBQUUsR0FBRyxFQUFFO1lBQ1IsSUFBSSxTQUFTLEtBQUssSUFBSSxFQUFFOztzQkFDaEIsUUFBUSxHQUFHLElBQUksQ0FBQyxJQUFJO2dCQUMxQixTQUFTO29CQUNMLGlCQUFpQixFQUFFLENBQUMsV0FBVyxDQUFDLGNBQWMsRUFBRSxTQUFTLFFBQVEsZUFBZSxFQUFFO3dCQUNoRixJQUFJLEVBQUUsSUFBSTt3QkFDVixpQkFBaUIsRUFBRSxDQUFDO3dCQUNwQixJQUFJLEVBQUUsUUFBUTt3QkFDZCxJQUFJLEVBQUUsbUJBQW1CLENBQUMsSUFBSSxDQUFDO3dCQUMvQixRQUFRLEVBQUUsSUFBSSxDQUFDLElBQUk7d0JBQ25CLElBQUksRUFBRSxJQUFJLENBQUMsSUFBSSxLQUFLLFNBQVMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsSUFBSTtxQkFDakQsQ0FBQyxDQUFDO2FBQ1I7WUFDRCxPQUFPLFNBQVMsQ0FBQztRQUNuQixDQUFDLENBQUE7O1FBRUQsWUFBWSxFQUFFLENBQUMsQ0FBQyxTQUFTO0tBQzFCLENBQUMsQ0FBQztBQUNMLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Z2V0Q29tcGlsZXJGYWNhZGV9IGZyb20gJy4uLy4uL2NvbXBpbGVyL2NvbXBpbGVyX2ZhY2FkZSc7XG5pbXBvcnQge3JlZmxlY3REZXBlbmRlbmNpZXN9IGZyb20gJy4uLy4uL2RpL2ppdC91dGlsJztcbmltcG9ydCB7VHlwZX0gZnJvbSAnLi4vLi4vaW50ZXJmYWNlL3R5cGUnO1xuaW1wb3J0IHtQaXBlfSBmcm9tICcuLi8uLi9tZXRhZGF0YS9kaXJlY3RpdmVzJztcbmltcG9ydCB7TkdfUElQRV9ERUZ9IGZyb20gJy4uL2ZpZWxkcyc7XG5cbmltcG9ydCB7YW5ndWxhckNvcmVFbnZ9IGZyb20gJy4vZW52aXJvbm1lbnQnO1xuXG5leHBvcnQgZnVuY3Rpb24gY29tcGlsZVBpcGUodHlwZTogVHlwZTxhbnk+LCBtZXRhOiBQaXBlKTogdm9pZCB7XG4gIGxldCBuZ1BpcGVEZWY6IGFueSA9IG51bGw7XG4gIE9iamVjdC5kZWZpbmVQcm9wZXJ0eSh0eXBlLCBOR19QSVBFX0RFRiwge1xuICAgIGdldDogKCkgPT4ge1xuICAgICAgaWYgKG5nUGlwZURlZiA9PT0gbnVsbCkge1xuICAgICAgICBjb25zdCB0eXBlTmFtZSA9IHR5cGUubmFtZTtcbiAgICAgICAgbmdQaXBlRGVmID1cbiAgICAgICAgICAgIGdldENvbXBpbGVyRmFjYWRlKCkuY29tcGlsZVBpcGUoYW5ndWxhckNvcmVFbnYsIGBuZzovLy8ke3R5cGVOYW1lfS9uZ1BpcGVEZWYuanNgLCB7XG4gICAgICAgICAgICAgIHR5cGU6IHR5cGUsXG4gICAgICAgICAgICAgIHR5cGVBcmd1bWVudENvdW50OiAwLFxuICAgICAgICAgICAgICBuYW1lOiB0eXBlTmFtZSxcbiAgICAgICAgICAgICAgZGVwczogcmVmbGVjdERlcGVuZGVuY2llcyh0eXBlKSxcbiAgICAgICAgICAgICAgcGlwZU5hbWU6IG1ldGEubmFtZSxcbiAgICAgICAgICAgICAgcHVyZTogbWV0YS5wdXJlICE9PSB1bmRlZmluZWQgPyBtZXRhLnB1cmUgOiB0cnVlXG4gICAgICAgICAgICB9KTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBuZ1BpcGVEZWY7XG4gICAgfSxcbiAgICAvLyBNYWtlIHRoZSBwcm9wZXJ0eSBjb25maWd1cmFibGUgaW4gZGV2IG1vZGUgdG8gYWxsb3cgb3ZlcnJpZGluZyBpbiB0ZXN0c1xuICAgIGNvbmZpZ3VyYWJsZTogISFuZ0Rldk1vZGUsXG4gIH0pO1xufVxuIl19
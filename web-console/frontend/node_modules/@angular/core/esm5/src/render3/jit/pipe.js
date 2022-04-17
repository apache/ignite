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
export function compilePipe(type, meta) {
    var ngPipeDef = null;
    Object.defineProperty(type, NG_PIPE_DEF, {
        get: function () {
            if (ngPipeDef === null) {
                var typeName = type.name;
                ngPipeDef =
                    getCompilerFacade().compilePipe(angularCoreEnv, "ng:///" + typeName + "/ngPipeDef.js", {
                        type: type,
                        typeArgumentCount: 0,
                        name: typeName,
                        deps: reflectDependencies(type),
                        pipeName: meta.name,
                        pure: meta.pure !== undefined ? meta.pure : true
                    });
            }
            return ngPipeDef;
        },
        // Make the property configurable in dev mode to allow overriding in tests
        configurable: !!ngDevMode,
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGlwZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3JlbmRlcjMvaml0L3BpcGUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HO0FBRUgsT0FBTyxFQUFDLGlCQUFpQixFQUFDLE1BQU0sZ0NBQWdDLENBQUM7QUFDakUsT0FBTyxFQUFDLG1CQUFtQixFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFHdEQsT0FBTyxFQUFDLFdBQVcsRUFBQyxNQUFNLFdBQVcsQ0FBQztBQUV0QyxPQUFPLEVBQUMsY0FBYyxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBRTdDLE1BQU0sVUFBVSxXQUFXLENBQUMsSUFBZSxFQUFFLElBQVU7SUFDckQsSUFBSSxTQUFTLEdBQVEsSUFBSSxDQUFDO0lBQzFCLE1BQU0sQ0FBQyxjQUFjLENBQUMsSUFBSSxFQUFFLFdBQVcsRUFBRTtRQUN2QyxHQUFHLEVBQUU7WUFDSCxJQUFJLFNBQVMsS0FBSyxJQUFJLEVBQUU7Z0JBQ3RCLElBQU0sUUFBUSxHQUFHLElBQUksQ0FBQyxJQUFJLENBQUM7Z0JBQzNCLFNBQVM7b0JBQ0wsaUJBQWlCLEVBQUUsQ0FBQyxXQUFXLENBQUMsY0FBYyxFQUFFLFdBQVMsUUFBUSxrQkFBZSxFQUFFO3dCQUNoRixJQUFJLEVBQUUsSUFBSTt3QkFDVixpQkFBaUIsRUFBRSxDQUFDO3dCQUNwQixJQUFJLEVBQUUsUUFBUTt3QkFDZCxJQUFJLEVBQUUsbUJBQW1CLENBQUMsSUFBSSxDQUFDO3dCQUMvQixRQUFRLEVBQUUsSUFBSSxDQUFDLElBQUk7d0JBQ25CLElBQUksRUFBRSxJQUFJLENBQUMsSUFBSSxLQUFLLFNBQVMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsSUFBSTtxQkFDakQsQ0FBQyxDQUFDO2FBQ1I7WUFDRCxPQUFPLFNBQVMsQ0FBQztRQUNuQixDQUFDO1FBQ0QsMEVBQTBFO1FBQzFFLFlBQVksRUFBRSxDQUFDLENBQUMsU0FBUztLQUMxQixDQUFDLENBQUM7QUFDTCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge2dldENvbXBpbGVyRmFjYWRlfSBmcm9tICcuLi8uLi9jb21waWxlci9jb21waWxlcl9mYWNhZGUnO1xuaW1wb3J0IHtyZWZsZWN0RGVwZW5kZW5jaWVzfSBmcm9tICcuLi8uLi9kaS9qaXQvdXRpbCc7XG5pbXBvcnQge1R5cGV9IGZyb20gJy4uLy4uL2ludGVyZmFjZS90eXBlJztcbmltcG9ydCB7UGlwZX0gZnJvbSAnLi4vLi4vbWV0YWRhdGEvZGlyZWN0aXZlcyc7XG5pbXBvcnQge05HX1BJUEVfREVGfSBmcm9tICcuLi9maWVsZHMnO1xuXG5pbXBvcnQge2FuZ3VsYXJDb3JlRW52fSBmcm9tICcuL2Vudmlyb25tZW50JztcblxuZXhwb3J0IGZ1bmN0aW9uIGNvbXBpbGVQaXBlKHR5cGU6IFR5cGU8YW55PiwgbWV0YTogUGlwZSk6IHZvaWQge1xuICBsZXQgbmdQaXBlRGVmOiBhbnkgPSBudWxsO1xuICBPYmplY3QuZGVmaW5lUHJvcGVydHkodHlwZSwgTkdfUElQRV9ERUYsIHtcbiAgICBnZXQ6ICgpID0+IHtcbiAgICAgIGlmIChuZ1BpcGVEZWYgPT09IG51bGwpIHtcbiAgICAgICAgY29uc3QgdHlwZU5hbWUgPSB0eXBlLm5hbWU7XG4gICAgICAgIG5nUGlwZURlZiA9XG4gICAgICAgICAgICBnZXRDb21waWxlckZhY2FkZSgpLmNvbXBpbGVQaXBlKGFuZ3VsYXJDb3JlRW52LCBgbmc6Ly8vJHt0eXBlTmFtZX0vbmdQaXBlRGVmLmpzYCwge1xuICAgICAgICAgICAgICB0eXBlOiB0eXBlLFxuICAgICAgICAgICAgICB0eXBlQXJndW1lbnRDb3VudDogMCxcbiAgICAgICAgICAgICAgbmFtZTogdHlwZU5hbWUsXG4gICAgICAgICAgICAgIGRlcHM6IHJlZmxlY3REZXBlbmRlbmNpZXModHlwZSksXG4gICAgICAgICAgICAgIHBpcGVOYW1lOiBtZXRhLm5hbWUsXG4gICAgICAgICAgICAgIHB1cmU6IG1ldGEucHVyZSAhPT0gdW5kZWZpbmVkID8gbWV0YS5wdXJlIDogdHJ1ZVxuICAgICAgICAgICAgfSk7XG4gICAgICB9XG4gICAgICByZXR1cm4gbmdQaXBlRGVmO1xuICAgIH0sXG4gICAgLy8gTWFrZSB0aGUgcHJvcGVydHkgY29uZmlndXJhYmxlIGluIGRldiBtb2RlIHRvIGFsbG93IG92ZXJyaWRpbmcgaW4gdGVzdHNcbiAgICBjb25maWd1cmFibGU6ICEhbmdEZXZNb2RlLFxuICB9KTtcbn1cbiJdfQ==
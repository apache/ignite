/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { createNgModule } from './core';
import { findLast } from './directive_resolver';
import { stringify } from './util';
/**
 * Resolves types to {@link NgModule}.
 */
var NgModuleResolver = /** @class */ (function () {
    function NgModuleResolver(_reflector) {
        this._reflector = _reflector;
    }
    NgModuleResolver.prototype.isNgModule = function (type) { return this._reflector.annotations(type).some(createNgModule.isTypeOf); };
    NgModuleResolver.prototype.resolve = function (type, throwIfNotFound) {
        if (throwIfNotFound === void 0) { throwIfNotFound = true; }
        var ngModuleMeta = findLast(this._reflector.annotations(type), createNgModule.isTypeOf);
        if (ngModuleMeta) {
            return ngModuleMeta;
        }
        else {
            if (throwIfNotFound) {
                throw new Error("No NgModule metadata found for '" + stringify(type) + "'.");
            }
            return null;
        }
    };
    return NgModuleResolver;
}());
export { NgModuleResolver };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfbW9kdWxlX3Jlc29sdmVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL25nX21vZHVsZV9yZXNvbHZlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFHSCxPQUFPLEVBQWlCLGNBQWMsRUFBQyxNQUFNLFFBQVEsQ0FBQztBQUN0RCxPQUFPLEVBQUMsUUFBUSxFQUFDLE1BQU0sc0JBQXNCLENBQUM7QUFDOUMsT0FBTyxFQUFDLFNBQVMsRUFBQyxNQUFNLFFBQVEsQ0FBQztBQUdqQzs7R0FFRztBQUNIO0lBQ0UsMEJBQW9CLFVBQTRCO1FBQTVCLGVBQVUsR0FBVixVQUFVLENBQWtCO0lBQUcsQ0FBQztJQUVwRCxxQ0FBVSxHQUFWLFVBQVcsSUFBUyxJQUFJLE9BQU8sSUFBSSxDQUFDLFVBQVUsQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLENBQUMsSUFBSSxDQUFDLGNBQWMsQ0FBQyxRQUFRLENBQUMsQ0FBQyxDQUFDLENBQUM7SUFFakcsa0NBQU8sR0FBUCxVQUFRLElBQVUsRUFBRSxlQUFzQjtRQUF0QixnQ0FBQSxFQUFBLHNCQUFzQjtRQUN4QyxJQUFNLFlBQVksR0FDZCxRQUFRLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLEVBQUUsY0FBYyxDQUFDLFFBQVEsQ0FBQyxDQUFDO1FBRXpFLElBQUksWUFBWSxFQUFFO1lBQ2hCLE9BQU8sWUFBWSxDQUFDO1NBQ3JCO2FBQU07WUFDTCxJQUFJLGVBQWUsRUFBRTtnQkFDbkIsTUFBTSxJQUFJLEtBQUssQ0FBQyxxQ0FBbUMsU0FBUyxDQUFDLElBQUksQ0FBQyxPQUFJLENBQUMsQ0FBQzthQUN6RTtZQUNELE9BQU8sSUFBSSxDQUFDO1NBQ2I7SUFDSCxDQUFDO0lBQ0gsdUJBQUM7QUFBRCxDQUFDLEFBbEJELElBa0JDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBpbGVSZWZsZWN0b3J9IGZyb20gJy4vY29tcGlsZV9yZWZsZWN0b3InO1xuaW1wb3J0IHtOZ01vZHVsZSwgVHlwZSwgY3JlYXRlTmdNb2R1bGV9IGZyb20gJy4vY29yZSc7XG5pbXBvcnQge2ZpbmRMYXN0fSBmcm9tICcuL2RpcmVjdGl2ZV9yZXNvbHZlcic7XG5pbXBvcnQge3N0cmluZ2lmeX0gZnJvbSAnLi91dGlsJztcblxuXG4vKipcbiAqIFJlc29sdmVzIHR5cGVzIHRvIHtAbGluayBOZ01vZHVsZX0uXG4gKi9cbmV4cG9ydCBjbGFzcyBOZ01vZHVsZVJlc29sdmVyIHtcbiAgY29uc3RydWN0b3IocHJpdmF0ZSBfcmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yKSB7fVxuXG4gIGlzTmdNb2R1bGUodHlwZTogYW55KSB7IHJldHVybiB0aGlzLl9yZWZsZWN0b3IuYW5ub3RhdGlvbnModHlwZSkuc29tZShjcmVhdGVOZ01vZHVsZS5pc1R5cGVPZik7IH1cblxuICByZXNvbHZlKHR5cGU6IFR5cGUsIHRocm93SWZOb3RGb3VuZCA9IHRydWUpOiBOZ01vZHVsZXxudWxsIHtcbiAgICBjb25zdCBuZ01vZHVsZU1ldGE6IE5nTW9kdWxlID1cbiAgICAgICAgZmluZExhc3QodGhpcy5fcmVmbGVjdG9yLmFubm90YXRpb25zKHR5cGUpLCBjcmVhdGVOZ01vZHVsZS5pc1R5cGVPZik7XG5cbiAgICBpZiAobmdNb2R1bGVNZXRhKSB7XG4gICAgICByZXR1cm4gbmdNb2R1bGVNZXRhO1xuICAgIH0gZWxzZSB7XG4gICAgICBpZiAodGhyb3dJZk5vdEZvdW5kKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihgTm8gTmdNb2R1bGUgbWV0YWRhdGEgZm91bmQgZm9yICcke3N0cmluZ2lmeSh0eXBlKX0nLmApO1xuICAgICAgfVxuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuICB9XG59XG4iXX0=
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Injectable, Optional } from '../di';
import { ivyEnabled } from '../ivy_switch';
import { Compiler } from './compiler';
var _SEPARATOR = '#';
var FACTORY_CLASS_SUFFIX = 'NgFactory';
/**
 * Configuration for SystemJsNgModuleLoader.
 * token.
 *
 * @publicApi
 * @deprecated the `string` form of `loadChildren` is deprecated, and `SystemJsNgModuleLoaderConfig`
 * is part of its implementation. See `LoadChildren` for more details.
 */
var SystemJsNgModuleLoaderConfig = /** @class */ (function () {
    function SystemJsNgModuleLoaderConfig() {
    }
    return SystemJsNgModuleLoaderConfig;
}());
export { SystemJsNgModuleLoaderConfig };
var DEFAULT_CONFIG = {
    factoryPathPrefix: '',
    factoryPathSuffix: '.ngfactory',
};
/**
 * NgModuleFactoryLoader that uses SystemJS to load NgModuleFactory
 * @publicApi
 * @deprecated the `string` form of `loadChildren` is deprecated, and `SystemJsNgModuleLoader` is
 * part of its implementation. See `LoadChildren` for more details.
 */
var SystemJsNgModuleLoader = /** @class */ (function () {
    function SystemJsNgModuleLoader(_compiler, config) {
        this._compiler = _compiler;
        this._config = config || DEFAULT_CONFIG;
    }
    SystemJsNgModuleLoader.prototype.load = function (path) {
        var legacyOfflineMode = !ivyEnabled && this._compiler instanceof Compiler;
        return legacyOfflineMode ? this.loadFactory(path) : this.loadAndCompile(path);
    };
    SystemJsNgModuleLoader.prototype.loadAndCompile = function (path) {
        var _this = this;
        var _a = tslib_1.__read(path.split(_SEPARATOR), 2), module = _a[0], exportName = _a[1];
        if (exportName === undefined) {
            exportName = 'default';
        }
        return System.import(module)
            .then(function (module) { return module[exportName]; })
            .then(function (type) { return checkNotEmpty(type, module, exportName); })
            .then(function (type) { return _this._compiler.compileModuleAsync(type); });
    };
    SystemJsNgModuleLoader.prototype.loadFactory = function (path) {
        var _a = tslib_1.__read(path.split(_SEPARATOR), 2), module = _a[0], exportName = _a[1];
        var factoryClassSuffix = FACTORY_CLASS_SUFFIX;
        if (exportName === undefined) {
            exportName = 'default';
            factoryClassSuffix = '';
        }
        return System.import(this._config.factoryPathPrefix + module + this._config.factoryPathSuffix)
            .then(function (module) { return module[exportName + factoryClassSuffix]; })
            .then(function (factory) { return checkNotEmpty(factory, module, exportName); });
    };
    SystemJsNgModuleLoader = tslib_1.__decorate([
        Injectable(),
        tslib_1.__param(1, Optional()),
        tslib_1.__metadata("design:paramtypes", [Compiler, SystemJsNgModuleLoaderConfig])
    ], SystemJsNgModuleLoader);
    return SystemJsNgModuleLoader;
}());
export { SystemJsNgModuleLoader };
function checkNotEmpty(value, modulePath, exportName) {
    if (!value) {
        throw new Error("Cannot find '" + exportName + "' in '" + modulePath + "'");
    }
    return value;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3lzdGVtX2pzX25nX21vZHVsZV9mYWN0b3J5X2xvYWRlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL2xpbmtlci9zeXN0ZW1fanNfbmdfbW9kdWxlX2ZhY3RvcnlfbG9hZGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFHSCxPQUFPLEVBQUMsVUFBVSxFQUFFLFFBQVEsRUFBQyxNQUFNLE9BQU8sQ0FBQztBQUMzQyxPQUFPLEVBQUMsVUFBVSxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBRXpDLE9BQU8sRUFBQyxRQUFRLEVBQUMsTUFBTSxZQUFZLENBQUM7QUFJcEMsSUFBTSxVQUFVLEdBQUcsR0FBRyxDQUFDO0FBRXZCLElBQU0sb0JBQW9CLEdBQUcsV0FBVyxDQUFDO0FBR3pDOzs7Ozs7O0dBT0c7QUFDSDtJQUFBO0lBWUEsQ0FBQztJQUFELG1DQUFDO0FBQUQsQ0FBQyxBQVpELElBWUM7O0FBRUQsSUFBTSxjQUFjLEdBQWlDO0lBQ25ELGlCQUFpQixFQUFFLEVBQUU7SUFDckIsaUJBQWlCLEVBQUUsWUFBWTtDQUNoQyxDQUFDO0FBRUY7Ozs7O0dBS0c7QUFFSDtJQUdFLGdDQUFvQixTQUFtQixFQUFjLE1BQXFDO1FBQXRFLGNBQVMsR0FBVCxTQUFTLENBQVU7UUFDckMsSUFBSSxDQUFDLE9BQU8sR0FBRyxNQUFNLElBQUksY0FBYyxDQUFDO0lBQzFDLENBQUM7SUFFRCxxQ0FBSSxHQUFKLFVBQUssSUFBWTtRQUNmLElBQU0saUJBQWlCLEdBQUcsQ0FBQyxVQUFVLElBQUksSUFBSSxDQUFDLFNBQVMsWUFBWSxRQUFRLENBQUM7UUFDNUUsT0FBTyxpQkFBaUIsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLFdBQVcsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLGNBQWMsQ0FBQyxJQUFJLENBQUMsQ0FBQztJQUNoRixDQUFDO0lBRU8sK0NBQWMsR0FBdEIsVUFBdUIsSUFBWTtRQUFuQyxpQkFVQztRQVRLLElBQUEsOENBQTZDLEVBQTVDLGNBQU0sRUFBRSxrQkFBb0MsQ0FBQztRQUNsRCxJQUFJLFVBQVUsS0FBSyxTQUFTLEVBQUU7WUFDNUIsVUFBVSxHQUFHLFNBQVMsQ0FBQztTQUN4QjtRQUVELE9BQU8sTUFBTSxDQUFDLE1BQU0sQ0FBQyxNQUFNLENBQUM7YUFDdkIsSUFBSSxDQUFDLFVBQUMsTUFBVyxJQUFLLE9BQUEsTUFBTSxDQUFDLFVBQVUsQ0FBQyxFQUFsQixDQUFrQixDQUFDO2FBQ3pDLElBQUksQ0FBQyxVQUFDLElBQVMsSUFBSyxPQUFBLGFBQWEsQ0FBQyxJQUFJLEVBQUUsTUFBTSxFQUFFLFVBQVUsQ0FBQyxFQUF2QyxDQUF1QyxDQUFDO2FBQzVELElBQUksQ0FBQyxVQUFDLElBQVMsSUFBSyxPQUFBLEtBQUksQ0FBQyxTQUFTLENBQUMsa0JBQWtCLENBQUMsSUFBSSxDQUFDLEVBQXZDLENBQXVDLENBQUMsQ0FBQztJQUNwRSxDQUFDO0lBRU8sNENBQVcsR0FBbkIsVUFBb0IsSUFBWTtRQUMxQixJQUFBLDhDQUE2QyxFQUE1QyxjQUFNLEVBQUUsa0JBQW9DLENBQUM7UUFDbEQsSUFBSSxrQkFBa0IsR0FBRyxvQkFBb0IsQ0FBQztRQUM5QyxJQUFJLFVBQVUsS0FBSyxTQUFTLEVBQUU7WUFDNUIsVUFBVSxHQUFHLFNBQVMsQ0FBQztZQUN2QixrQkFBa0IsR0FBRyxFQUFFLENBQUM7U0FDekI7UUFFRCxPQUFPLE1BQU0sQ0FBQyxNQUFNLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxpQkFBaUIsR0FBRyxNQUFNLEdBQUcsSUFBSSxDQUFDLE9BQU8sQ0FBQyxpQkFBaUIsQ0FBQzthQUN6RixJQUFJLENBQUMsVUFBQyxNQUFXLElBQUssT0FBQSxNQUFNLENBQUMsVUFBVSxHQUFHLGtCQUFrQixDQUFDLEVBQXZDLENBQXVDLENBQUM7YUFDOUQsSUFBSSxDQUFDLFVBQUMsT0FBWSxJQUFLLE9BQUEsYUFBYSxDQUFDLE9BQU8sRUFBRSxNQUFNLEVBQUUsVUFBVSxDQUFDLEVBQTFDLENBQTBDLENBQUMsQ0FBQztJQUMxRSxDQUFDO0lBbkNVLHNCQUFzQjtRQURsQyxVQUFVLEVBQUU7UUFJK0IsbUJBQUEsUUFBUSxFQUFFLENBQUE7aURBQXJCLFFBQVEsRUFBdUIsNEJBQTRCO09BSC9FLHNCQUFzQixDQW9DbEM7SUFBRCw2QkFBQztDQUFBLEFBcENELElBb0NDO1NBcENZLHNCQUFzQjtBQXNDbkMsU0FBUyxhQUFhLENBQUMsS0FBVSxFQUFFLFVBQWtCLEVBQUUsVUFBa0I7SUFDdkUsSUFBSSxDQUFDLEtBQUssRUFBRTtRQUNWLE1BQU0sSUFBSSxLQUFLLENBQUMsa0JBQWdCLFVBQVUsY0FBUyxVQUFVLE1BQUcsQ0FBQyxDQUFDO0tBQ25FO0lBQ0QsT0FBTyxLQUFLLENBQUM7QUFDZixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5cbmltcG9ydCB7SW5qZWN0YWJsZSwgT3B0aW9uYWx9IGZyb20gJy4uL2RpJztcbmltcG9ydCB7aXZ5RW5hYmxlZH0gZnJvbSAnLi4vaXZ5X3N3aXRjaCc7XG5cbmltcG9ydCB7Q29tcGlsZXJ9IGZyb20gJy4vY29tcGlsZXInO1xuaW1wb3J0IHtOZ01vZHVsZUZhY3Rvcnl9IGZyb20gJy4vbmdfbW9kdWxlX2ZhY3RvcnknO1xuaW1wb3J0IHtOZ01vZHVsZUZhY3RvcnlMb2FkZXJ9IGZyb20gJy4vbmdfbW9kdWxlX2ZhY3RvcnlfbG9hZGVyJztcblxuY29uc3QgX1NFUEFSQVRPUiA9ICcjJztcblxuY29uc3QgRkFDVE9SWV9DTEFTU19TVUZGSVggPSAnTmdGYWN0b3J5JztcbmRlY2xhcmUgdmFyIFN5c3RlbTogYW55O1xuXG4vKipcbiAqIENvbmZpZ3VyYXRpb24gZm9yIFN5c3RlbUpzTmdNb2R1bGVMb2FkZXIuXG4gKiB0b2tlbi5cbiAqXG4gKiBAcHVibGljQXBpXG4gKiBAZGVwcmVjYXRlZCB0aGUgYHN0cmluZ2AgZm9ybSBvZiBgbG9hZENoaWxkcmVuYCBpcyBkZXByZWNhdGVkLCBhbmQgYFN5c3RlbUpzTmdNb2R1bGVMb2FkZXJDb25maWdgXG4gKiBpcyBwYXJ0IG9mIGl0cyBpbXBsZW1lbnRhdGlvbi4gU2VlIGBMb2FkQ2hpbGRyZW5gIGZvciBtb3JlIGRldGFpbHMuXG4gKi9cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBTeXN0ZW1Kc05nTW9kdWxlTG9hZGVyQ29uZmlnIHtcbiAgLyoqXG4gICAqIFByZWZpeCB0byBhZGQgd2hlbiBjb21wdXRpbmcgdGhlIG5hbWUgb2YgdGhlIGZhY3RvcnkgbW9kdWxlIGZvciBhIGdpdmVuIG1vZHVsZSBuYW1lLlxuICAgKi9cbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIGZhY3RvcnlQYXRoUHJlZml4ICE6IHN0cmluZztcblxuICAvKipcbiAgICogU3VmZml4IHRvIGFkZCB3aGVuIGNvbXB1dGluZyB0aGUgbmFtZSBvZiB0aGUgZmFjdG9yeSBtb2R1bGUgZm9yIGEgZ2l2ZW4gbW9kdWxlIG5hbWUuXG4gICAqL1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgZmFjdG9yeVBhdGhTdWZmaXggITogc3RyaW5nO1xufVxuXG5jb25zdCBERUZBVUxUX0NPTkZJRzogU3lzdGVtSnNOZ01vZHVsZUxvYWRlckNvbmZpZyA9IHtcbiAgZmFjdG9yeVBhdGhQcmVmaXg6ICcnLFxuICBmYWN0b3J5UGF0aFN1ZmZpeDogJy5uZ2ZhY3RvcnknLFxufTtcblxuLyoqXG4gKiBOZ01vZHVsZUZhY3RvcnlMb2FkZXIgdGhhdCB1c2VzIFN5c3RlbUpTIHRvIGxvYWQgTmdNb2R1bGVGYWN0b3J5XG4gKiBAcHVibGljQXBpXG4gKiBAZGVwcmVjYXRlZCB0aGUgYHN0cmluZ2AgZm9ybSBvZiBgbG9hZENoaWxkcmVuYCBpcyBkZXByZWNhdGVkLCBhbmQgYFN5c3RlbUpzTmdNb2R1bGVMb2FkZXJgIGlzXG4gKiBwYXJ0IG9mIGl0cyBpbXBsZW1lbnRhdGlvbi4gU2VlIGBMb2FkQ2hpbGRyZW5gIGZvciBtb3JlIGRldGFpbHMuXG4gKi9cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBTeXN0ZW1Kc05nTW9kdWxlTG9hZGVyIGltcGxlbWVudHMgTmdNb2R1bGVGYWN0b3J5TG9hZGVyIHtcbiAgcHJpdmF0ZSBfY29uZmlnOiBTeXN0ZW1Kc05nTW9kdWxlTG9hZGVyQ29uZmlnO1xuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgX2NvbXBpbGVyOiBDb21waWxlciwgQE9wdGlvbmFsKCkgY29uZmlnPzogU3lzdGVtSnNOZ01vZHVsZUxvYWRlckNvbmZpZykge1xuICAgIHRoaXMuX2NvbmZpZyA9IGNvbmZpZyB8fCBERUZBVUxUX0NPTkZJRztcbiAgfVxuXG4gIGxvYWQocGF0aDogc3RyaW5nKTogUHJvbWlzZTxOZ01vZHVsZUZhY3Rvcnk8YW55Pj4ge1xuICAgIGNvbnN0IGxlZ2FjeU9mZmxpbmVNb2RlID0gIWl2eUVuYWJsZWQgJiYgdGhpcy5fY29tcGlsZXIgaW5zdGFuY2VvZiBDb21waWxlcjtcbiAgICByZXR1cm4gbGVnYWN5T2ZmbGluZU1vZGUgPyB0aGlzLmxvYWRGYWN0b3J5KHBhdGgpIDogdGhpcy5sb2FkQW5kQ29tcGlsZShwYXRoKTtcbiAgfVxuXG4gIHByaXZhdGUgbG9hZEFuZENvbXBpbGUocGF0aDogc3RyaW5nKTogUHJvbWlzZTxOZ01vZHVsZUZhY3Rvcnk8YW55Pj4ge1xuICAgIGxldCBbbW9kdWxlLCBleHBvcnROYW1lXSA9IHBhdGguc3BsaXQoX1NFUEFSQVRPUik7XG4gICAgaWYgKGV4cG9ydE5hbWUgPT09IHVuZGVmaW5lZCkge1xuICAgICAgZXhwb3J0TmFtZSA9ICdkZWZhdWx0JztcbiAgICB9XG5cbiAgICByZXR1cm4gU3lzdGVtLmltcG9ydChtb2R1bGUpXG4gICAgICAgIC50aGVuKChtb2R1bGU6IGFueSkgPT4gbW9kdWxlW2V4cG9ydE5hbWVdKVxuICAgICAgICAudGhlbigodHlwZTogYW55KSA9PiBjaGVja05vdEVtcHR5KHR5cGUsIG1vZHVsZSwgZXhwb3J0TmFtZSkpXG4gICAgICAgIC50aGVuKCh0eXBlOiBhbnkpID0+IHRoaXMuX2NvbXBpbGVyLmNvbXBpbGVNb2R1bGVBc3luYyh0eXBlKSk7XG4gIH1cblxuICBwcml2YXRlIGxvYWRGYWN0b3J5KHBhdGg6IHN0cmluZyk6IFByb21pc2U8TmdNb2R1bGVGYWN0b3J5PGFueT4+IHtcbiAgICBsZXQgW21vZHVsZSwgZXhwb3J0TmFtZV0gPSBwYXRoLnNwbGl0KF9TRVBBUkFUT1IpO1xuICAgIGxldCBmYWN0b3J5Q2xhc3NTdWZmaXggPSBGQUNUT1JZX0NMQVNTX1NVRkZJWDtcbiAgICBpZiAoZXhwb3J0TmFtZSA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICBleHBvcnROYW1lID0gJ2RlZmF1bHQnO1xuICAgICAgZmFjdG9yeUNsYXNzU3VmZml4ID0gJyc7XG4gICAgfVxuXG4gICAgcmV0dXJuIFN5c3RlbS5pbXBvcnQodGhpcy5fY29uZmlnLmZhY3RvcnlQYXRoUHJlZml4ICsgbW9kdWxlICsgdGhpcy5fY29uZmlnLmZhY3RvcnlQYXRoU3VmZml4KVxuICAgICAgICAudGhlbigobW9kdWxlOiBhbnkpID0+IG1vZHVsZVtleHBvcnROYW1lICsgZmFjdG9yeUNsYXNzU3VmZml4XSlcbiAgICAgICAgLnRoZW4oKGZhY3Rvcnk6IGFueSkgPT4gY2hlY2tOb3RFbXB0eShmYWN0b3J5LCBtb2R1bGUsIGV4cG9ydE5hbWUpKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBjaGVja05vdEVtcHR5KHZhbHVlOiBhbnksIG1vZHVsZVBhdGg6IHN0cmluZywgZXhwb3J0TmFtZTogc3RyaW5nKTogYW55IHtcbiAgaWYgKCF2YWx1ZSkge1xuICAgIHRocm93IG5ldyBFcnJvcihgQ2Fubm90IGZpbmQgJyR7ZXhwb3J0TmFtZX0nIGluICcke21vZHVsZVBhdGh9J2ApO1xuICB9XG4gIHJldHVybiB2YWx1ZTtcbn1cbiJdfQ==
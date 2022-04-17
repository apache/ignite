/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Component, Directive, NgModule, Pipe, ɵReflectionCapabilities as ReflectionCapabilities } from '@angular/core';
import { MetadataOverrider } from './metadata_overrider';
var reflection = new ReflectionCapabilities();
/**
 * Allows to override ivy metadata for tests (via the `TestBed`).
 */
var OverrideResolver = /** @class */ (function () {
    function OverrideResolver() {
        this.overrides = new Map();
        this.resolved = new Map();
    }
    OverrideResolver.prototype.addOverride = function (type, override) {
        var overrides = this.overrides.get(type) || [];
        overrides.push(override);
        this.overrides.set(type, overrides);
        this.resolved.delete(type);
    };
    OverrideResolver.prototype.setOverrides = function (overrides) {
        var _this = this;
        this.overrides.clear();
        overrides.forEach(function (_a) {
            var _b = tslib_1.__read(_a, 2), type = _b[0], override = _b[1];
            _this.addOverride(type, override);
        });
    };
    OverrideResolver.prototype.getAnnotation = function (type) {
        var annotations = reflection.annotations(type);
        // Try to find the nearest known Type annotation and make sure that this annotation is an
        // instance of the type we are looking for, so we can use it for resolution. Note: there might
        // be multiple known annotations found due to the fact that Components can extend Directives (so
        // both Directive and Component annotations would be present), so we always check if the known
        // annotation has the right type.
        for (var i = annotations.length - 1; i >= 0; i--) {
            var annotation = annotations[i];
            var isKnownType = annotation instanceof Directive || annotation instanceof Component ||
                annotation instanceof Pipe || annotation instanceof NgModule;
            if (isKnownType) {
                return annotation instanceof this.type ? annotation : null;
            }
        }
        return null;
    };
    OverrideResolver.prototype.resolve = function (type) {
        var _this = this;
        var resolved = this.resolved.get(type) || null;
        if (!resolved) {
            resolved = this.getAnnotation(type);
            if (resolved) {
                var overrides = this.overrides.get(type);
                if (overrides) {
                    var overrider_1 = new MetadataOverrider();
                    overrides.forEach(function (override) {
                        resolved = overrider_1.overrideMetadata(_this.type, resolved, override);
                    });
                }
            }
            this.resolved.set(type, resolved);
        }
        return resolved;
    };
    return OverrideResolver;
}());
var DirectiveResolver = /** @class */ (function (_super) {
    tslib_1.__extends(DirectiveResolver, _super);
    function DirectiveResolver() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    Object.defineProperty(DirectiveResolver.prototype, "type", {
        get: function () { return Directive; },
        enumerable: true,
        configurable: true
    });
    return DirectiveResolver;
}(OverrideResolver));
export { DirectiveResolver };
var ComponentResolver = /** @class */ (function (_super) {
    tslib_1.__extends(ComponentResolver, _super);
    function ComponentResolver() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    Object.defineProperty(ComponentResolver.prototype, "type", {
        get: function () { return Component; },
        enumerable: true,
        configurable: true
    });
    return ComponentResolver;
}(OverrideResolver));
export { ComponentResolver };
var PipeResolver = /** @class */ (function (_super) {
    tslib_1.__extends(PipeResolver, _super);
    function PipeResolver() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    Object.defineProperty(PipeResolver.prototype, "type", {
        get: function () { return Pipe; },
        enumerable: true,
        configurable: true
    });
    return PipeResolver;
}(OverrideResolver));
export { PipeResolver };
var NgModuleResolver = /** @class */ (function (_super) {
    tslib_1.__extends(NgModuleResolver, _super);
    function NgModuleResolver() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    Object.defineProperty(NgModuleResolver.prototype, "type", {
        get: function () { return NgModule; },
        enumerable: true,
        configurable: true
    });
    return NgModuleResolver;
}(OverrideResolver));
export { NgModuleResolver };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVzb2x2ZXJzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS90ZXN0aW5nL3NyYy9yZXNvbHZlcnMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sRUFBQyxTQUFTLEVBQUUsU0FBUyxFQUFFLFFBQVEsRUFBRSxJQUFJLEVBQVEsdUJBQXVCLElBQUksc0JBQXNCLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFHNUgsT0FBTyxFQUFDLGlCQUFpQixFQUFDLE1BQU0sc0JBQXNCLENBQUM7QUFFdkQsSUFBTSxVQUFVLEdBQUcsSUFBSSxzQkFBc0IsRUFBRSxDQUFDO0FBV2hEOztHQUVHO0FBQ0g7SUFBQTtRQUNVLGNBQVMsR0FBRyxJQUFJLEdBQUcsRUFBb0MsQ0FBQztRQUN4RCxhQUFRLEdBQUcsSUFBSSxHQUFHLEVBQXFCLENBQUM7SUFxRGxELENBQUM7SUFqREMsc0NBQVcsR0FBWCxVQUFZLElBQWUsRUFBRSxRQUE2QjtRQUN4RCxJQUFNLFNBQVMsR0FBRyxJQUFJLENBQUMsU0FBUyxDQUFDLEdBQUcsQ0FBQyxJQUFJLENBQUMsSUFBSSxFQUFFLENBQUM7UUFDakQsU0FBUyxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsQ0FBQztRQUN6QixJQUFJLENBQUMsU0FBUyxDQUFDLEdBQUcsQ0FBQyxJQUFJLEVBQUUsU0FBUyxDQUFDLENBQUM7UUFDcEMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxNQUFNLENBQUMsSUFBSSxDQUFDLENBQUM7SUFDN0IsQ0FBQztJQUVELHVDQUFZLEdBQVosVUFBYSxTQUFrRDtRQUEvRCxpQkFHQztRQUZDLElBQUksQ0FBQyxTQUFTLENBQUMsS0FBSyxFQUFFLENBQUM7UUFDdkIsU0FBUyxDQUFDLE9BQU8sQ0FBQyxVQUFDLEVBQWdCO2dCQUFoQiwwQkFBZ0IsRUFBZixZQUFJLEVBQUUsZ0JBQVE7WUFBUSxL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cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBvbmVudCwgRGlyZWN0aXZlLCBOZ01vZHVsZSwgUGlwZSwgVHlwZSwgybVSZWZsZWN0aW9uQ2FwYWJpbGl0aWVzIGFzIFJlZmxlY3Rpb25DYXBhYmlsaXRpZXN9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQge01ldGFkYXRhT3ZlcnJpZGV9IGZyb20gJy4vbWV0YWRhdGFfb3ZlcnJpZGUnO1xuaW1wb3J0IHtNZXRhZGF0YU92ZXJyaWRlcn0gZnJvbSAnLi9tZXRhZGF0YV9vdmVycmlkZXInO1xuXG5jb25zdCByZWZsZWN0aW9uID0gbmV3IFJlZmxlY3Rpb25DYXBhYmlsaXRpZXMoKTtcblxuLyoqXG4gKiBCYXNlIGludGVyZmFjZSB0byByZXNvbHZlIGBAQ29tcG9uZW50YCwgYEBEaXJlY3RpdmVgLCBgQFBpcGVgIGFuZCBgQE5nTW9kdWxlYC5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBSZXNvbHZlcjxUPiB7XG4gIGFkZE92ZXJyaWRlKHR5cGU6IFR5cGU8YW55Piwgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8VD4pOiB2b2lkO1xuICBzZXRPdmVycmlkZXMob3ZlcnJpZGVzOiBBcnJheTxbVHlwZTxhbnk+LCBNZXRhZGF0YU92ZXJyaWRlPFQ+XT4pOiB2b2lkO1xuICByZXNvbHZlKHR5cGU6IFR5cGU8YW55Pik6IFR8bnVsbDtcbn1cblxuLyoqXG4gKiBBbGxvd3MgdG8gb3ZlcnJpZGUgaXZ5IG1ldGFkYXRhIGZvciB0ZXN0cyAodmlhIHRoZSBgVGVzdEJlZGApLlxuICovXG5hYnN0cmFjdCBjbGFzcyBPdmVycmlkZVJlc29sdmVyPFQ+IGltcGxlbWVudHMgUmVzb2x2ZXI8VD4ge1xuICBwcml2YXRlIG92ZXJyaWRlcyA9IG5ldyBNYXA8VHlwZTxhbnk+LCBNZXRhZGF0YU92ZXJyaWRlPFQ+W10+KCk7XG4gIHByaXZhdGUgcmVzb2x2ZWQgPSBuZXcgTWFwPFR5cGU8YW55PiwgVHxudWxsPigpO1xuXG4gIGFic3RyYWN0IGdldCB0eXBlKCk6IGFueTtcblxuICBhZGRPdmVycmlkZSh0eXBlOiBUeXBlPGFueT4sIG92ZXJyaWRlOiBNZXRhZGF0YU92ZXJyaWRlPFQ+KSB7XG4gICAgY29uc3Qgb3ZlcnJpZGVzID0gdGhpcy5vdmVycmlkZXMuZ2V0KHR5cGUpIHx8IFtdO1xuICAgIG92ZXJyaWRlcy5wdXNoKG92ZXJyaWRlKTtcbiAgICB0aGlzLm92ZXJyaWRlcy5zZXQodHlwZSwgb3ZlcnJpZGVzKTtcbiAgICB0aGlzLnJlc29sdmVkLmRlbGV0ZSh0eXBlKTtcbiAgfVxuXG4gIHNldE92ZXJyaWRlcyhvdmVycmlkZXM6IEFycmF5PFtUeXBlPGFueT4sIE1ldGFkYXRhT3ZlcnJpZGU8VD5dPikge1xuICAgIHRoaXMub3ZlcnJpZGVzLmNsZWFyKCk7XG4gICAgb3ZlcnJpZGVzLmZvckVhY2goKFt0eXBlLCBvdmVycmlkZV0pID0+IHsgdGhpcy5hZGRPdmVycmlkZSh0eXBlLCBvdmVycmlkZSk7IH0pO1xuICB9XG5cbiAgZ2V0QW5ub3RhdGlvbih0eXBlOiBUeXBlPGFueT4pOiBUfG51bGwge1xuICAgIGNvbnN0IGFubm90YXRpb25zID0gcmVmbGVjdGlvbi5hbm5vdGF0aW9ucyh0eXBlKTtcbiAgICAvLyBUcnkgdG8gZmluZCB0aGUgbmVhcmVzdCBrbm93biBUeXBlIGFubm90YXRpb24gYW5kIG1ha2Ugc3VyZSB0aGF0IHRoaXMgYW5ub3RhdGlvbiBpcyBhblxuICAgIC8vIGluc3RhbmNlIG9mIHRoZSB0eXBlIHdlIGFyZSBsb29raW5nIGZvciwgc28gd2UgY2FuIHVzZSBpdCBmb3IgcmVzb2x1dGlvbi4gTm90ZTogdGhlcmUgbWlnaHRcbiAgICAvLyBiZSBtdWx0aXBsZSBrbm93biBhbm5vdGF0aW9ucyBmb3VuZCBkdWUgdG8gdGhlIGZhY3QgdGhhdCBDb21wb25lbnRzIGNhbiBleHRlbmQgRGlyZWN0aXZlcyAoc29cbiAgICAvLyBib3RoIERpcmVjdGl2ZSBhbmQgQ29tcG9uZW50IGFubm90YXRpb25zIHdvdWxkIGJlIHByZXNlbnQpLCBzbyB3ZSBhbHdheXMgY2hlY2sgaWYgdGhlIGtub3duXG4gICAgLy8gYW5ub3RhdGlvbiBoYXMgdGhlIHJpZ2h0IHR5cGUuXG4gICAgZm9yIChsZXQgaSA9IGFubm90YXRpb25zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgICBjb25zdCBhbm5vdGF0aW9uID0gYW5ub3RhdGlvbnNbaV07XG4gICAgICBjb25zdCBpc0tub3duVHlwZSA9IGFubm90YXRpb24gaW5zdGFuY2VvZiBEaXJlY3RpdmUgfHwgYW5ub3RhdGlvbiBpbnN0YW5jZW9mIENvbXBvbmVudCB8fFxuICAgICAgICAgIGFubm90YXRpb24gaW5zdGFuY2VvZiBQaXBlIHx8IGFubm90YXRpb24gaW5zdGFuY2VvZiBOZ01vZHVsZTtcbiAgICAgIGlmIChpc0tub3duVHlwZSkge1xuICAgICAgICByZXR1cm4gYW5ub3RhdGlvbiBpbnN0YW5jZW9mIHRoaXMudHlwZSA/IGFubm90YXRpb24gOiBudWxsO1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHJlc29sdmUodHlwZTogVHlwZTxhbnk+KTogVHxudWxsIHtcbiAgICBsZXQgcmVzb2x2ZWQgPSB0aGlzLnJlc29sdmVkLmdldCh0eXBlKSB8fCBudWxsO1xuXG4gICAgaWYgKCFyZXNvbHZlZCkge1xuICAgICAgcmVzb2x2ZWQgPSB0aGlzLmdldEFubm90YXRpb24odHlwZSk7XG4gICAgICBpZiAocmVzb2x2ZWQpIHtcbiAgICAgICAgY29uc3Qgb3ZlcnJpZGVzID0gdGhpcy5vdmVycmlkZXMuZ2V0KHR5cGUpO1xuICAgICAgICBpZiAob3ZlcnJpZGVzKSB7XG4gICAgICAgICAgY29uc3Qgb3ZlcnJpZGVyID0gbmV3IE1ldGFkYXRhT3ZlcnJpZGVyKCk7XG4gICAgICAgICAgb3ZlcnJpZGVzLmZvckVhY2gob3ZlcnJpZGUgPT4ge1xuICAgICAgICAgICAgcmVzb2x2ZWQgPSBvdmVycmlkZXIub3ZlcnJpZGVNZXRhZGF0YSh0aGlzLnR5cGUsIHJlc29sdmVkICEsIG92ZXJyaWRlKTtcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgdGhpcy5yZXNvbHZlZC5zZXQodHlwZSwgcmVzb2x2ZWQpO1xuICAgIH1cblxuICAgIHJldHVybiByZXNvbHZlZDtcbiAgfVxufVxuXG5cbmV4cG9ydCBjbGFzcyBEaXJlY3RpdmVSZXNvbHZlciBleHRlbmRzIE92ZXJyaWRlUmVzb2x2ZXI8RGlyZWN0aXZlPiB7XG4gIGdldCB0eXBlKCkgeyByZXR1cm4gRGlyZWN0aXZlOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb21wb25lbnRSZXNvbHZlciBleHRlbmRzIE92ZXJyaWRlUmVzb2x2ZXI8Q29tcG9uZW50PiB7XG4gIGdldCB0eXBlKCkgeyByZXR1cm4gQ29tcG9uZW50OyB9XG59XG5cbmV4cG9ydCBjbGFzcyBQaXBlUmVzb2x2ZXIgZXh0ZW5kcyBPdmVycmlkZVJlc29sdmVyPFBpcGU+IHtcbiAgZ2V0IHR5cGUoKSB7IHJldHVybiBQaXBlOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBOZ01vZHVsZVJlc29sdmVyIGV4dGVuZHMgT3ZlcnJpZGVSZXNvbHZlcjxOZ01vZHVsZT4ge1xuICBnZXQgdHlwZSgpIHsgcmV0dXJuIE5nTW9kdWxlOyB9XG59XG4iXX0=
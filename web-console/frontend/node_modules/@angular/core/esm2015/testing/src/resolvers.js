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
import { Component, Directive, NgModule, Pipe, ɵReflectionCapabilities as ReflectionCapabilities } from '@angular/core';
import { MetadataOverrider } from './metadata_overrider';
/** @type {?} */
const reflection = new ReflectionCapabilities();
/**
 * Base interface to resolve `\@Component`, `\@Directive`, `\@Pipe` and `\@NgModule`.
 * @record
 * @template T
 */
export function Resolver() { }
if (false) {
    /**
     * @param {?} type
     * @param {?} override
     * @return {?}
     */
    Resolver.prototype.addOverride = function (type, override) { };
    /**
     * @param {?} overrides
     * @return {?}
     */
    Resolver.prototype.setOverrides = function (overrides) { };
    /**
     * @param {?} type
     * @return {?}
     */
    Resolver.prototype.resolve = function (type) { };
}
/**
 * Allows to override ivy metadata for tests (via the `TestBed`).
 * @abstract
 * @template T
 */
class OverrideResolver {
    constructor() {
        this.overrides = new Map();
        this.resolved = new Map();
    }
    /**
     * @param {?} type
     * @param {?} override
     * @return {?}
     */
    addOverride(type, override) {
        /** @type {?} */
        const overrides = this.overrides.get(type) || [];
        overrides.push(override);
        this.overrides.set(type, overrides);
        this.resolved.delete(type);
    }
    /**
     * @param {?} overrides
     * @return {?}
     */
    setOverrides(overrides) {
        this.overrides.clear();
        overrides.forEach((/**
         * @param {?} __0
         * @return {?}
         */
        ([type, override]) => { this.addOverride(type, override); }));
    }
    /**
     * @param {?} type
     * @return {?}
     */
    getAnnotation(type) {
        /** @type {?} */
        const annotations = reflection.annotations(type);
        // Try to find the nearest known Type annotation and make sure that this annotation is an
        // instance of the type we are looking for, so we can use it for resolution. Note: there might
        // be multiple known annotations found due to the fact that Components can extend Directives (so
        // both Directive and Component annotations would be present), so we always check if the known
        // annotation has the right type.
        for (let i = annotations.length - 1; i >= 0; i--) {
            /** @type {?} */
            const annotation = annotations[i];
            /** @type {?} */
            const isKnownType = annotation instanceof Directive || annotation instanceof Component ||
                annotation instanceof Pipe || annotation instanceof NgModule;
            if (isKnownType) {
                return annotation instanceof this.type ? annotation : null;
            }
        }
        return null;
    }
    /**
     * @param {?} type
     * @return {?}
     */
    resolve(type) {
        /** @type {?} */
        let resolved = this.resolved.get(type) || null;
        if (!resolved) {
            resolved = this.getAnnotation(type);
            if (resolved) {
                /** @type {?} */
                const overrides = this.overrides.get(type);
                if (overrides) {
                    /** @type {?} */
                    const overrider = new MetadataOverrider();
                    overrides.forEach((/**
                     * @param {?} override
                     * @return {?}
                     */
                    override => {
                        resolved = overrider.overrideMetadata(this.type, (/** @type {?} */ (resolved)), override);
                    }));
                }
            }
            this.resolved.set(type, resolved);
        }
        return resolved;
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    OverrideResolver.prototype.overrides;
    /**
     * @type {?}
     * @private
     */
    OverrideResolver.prototype.resolved;
    /**
     * @abstract
     * @return {?}
     */
    OverrideResolver.prototype.type = function () { };
}
export class DirectiveResolver extends OverrideResolver {
    /**
     * @return {?}
     */
    get type() { return Directive; }
}
export class ComponentResolver extends OverrideResolver {
    /**
     * @return {?}
     */
    get type() { return Component; }
}
export class PipeResolver extends OverrideResolver {
    /**
     * @return {?}
     */
    get type() { return Pipe; }
}
export class NgModuleResolver extends OverrideResolver {
    /**
     * @return {?}
     */
    get type() { return NgModule; }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVzb2x2ZXJzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS90ZXN0aW5nL3NyYy9yZXNvbHZlcnMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsU0FBUyxFQUFFLFNBQVMsRUFBRSxRQUFRLEVBQUUsSUFBSSxFQUFRLHVCQUF1QixJQUFJLHNCQUFzQixFQUFDLE1BQU0sZUFBZSxDQUFDO0FBRzVILE9BQU8sRUFBQyxpQkFBaUIsRUFBQyxNQUFNLHNCQUFzQixDQUFDOztNQUVqRCxVQUFVLEdBQUcsSUFBSSxzQkFBc0IsRUFBRTs7Ozs7O0FBSy9DLDhCQUlDOzs7Ozs7O0lBSEMsK0RBQWtFOzs7OztJQUNsRSwyREFBdUU7Ozs7O0lBQ3ZFLGlEQUFpQzs7Ozs7OztBQU1uQyxNQUFlLGdCQUFnQjtJQUEvQjtRQUNVLGNBQVMsR0FBRyxJQUFJLEdBQUcsRUFBb0MsQ0FBQztRQUN4RCxhQUFRLEdBQUcsSUFBSSxHQUFHLEVBQXFCLENBQUM7SUFxRGxELENBQUM7Ozs7OztJQWpEQyxXQUFXLENBQUMsSUFBZSxFQUFFLFFBQTZCOztjQUNsRCxTQUFTLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLElBQUksRUFBRTtRQUNoRCxTQUFTLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxDQUFDO1FBQ3pCLElBQUksQ0FBQyxTQUFTLENBQUMsR0FBRyxDQUFDLElBQUksRUFBRSxTQUFTLENBQUMsQ0FBQztRQUNwQyxJQUFJLENBQUMsUUFBUSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsQ0FBQztJQUM3QixDQUFDOzs7OztJQUVELFlBQVksQ0FBQyxTQUFrRDtRQUM3RCxJQUFJLENBQUMsU0FBUyxDQUFDLEtBQUssRUFBRSxDQUFDO1FBQ3ZCLFNBQVMsQ0FBQyxPQUFPOzs7O1FBQUMsQ0FBQyxDQUFDLElBQUksRUFBRSxRQUFRLENBQUMsRUFBRSxFQUFFLEdBQUcsSUFBSSxDQUFDLFdBQVcsQ0FBQyxJQUFJLEVBQUUsUUFBUSxDQUFDLENBQUMsQ0FBQyxDQUFDLEVBQUMsQ0FBQztJQUNqRixDQUFDOzs7OztJQUVELGFBQWEsQ0FBQyxJQUFlOztjQUNyQixXQUFXLEdBQUcsVUFBVSxDQUFDLFdBQVcsQ0FBQyxJQUFJLENBQUM7UUFDaEQseUZBQXlGO1FBQ3pGLDhGQUE4RjtRQUM5RixnR0FBZ0c7UUFDaEcsOEZBQThGO1FBQzlGLGlDQUFpQztRQUNqQyxLQUFLLElBQUksQ0FBQyxHQUFHLFdBQVcsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxFQUFFLENBQUMsSUFBSSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUU7O2tCQUMxQyxVQUFVLEdBQUcsV0FBVyxDQUFDLENBQUMsQ0FBQzs7a0JBQzNCLFdBQVcsR0FBRyxVQUFVLFlBQVksU0FBUyxJQUFJLFVBQVUsWUFBWSxTQUFTO2dCQUNsRixVQUFVLFlBQVksSUFBSSxJQUFJLFVBQVUsWUFBWSxRQUFRO1lBQ2hFLElBQUksV0FBVyxFQUFFO2dCQUNmLE9BQU8sVUFBVSxZQUFZLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDO2FBQzVEO1NBQ0Y7UUFDRCxPQUFPLElBQUksQ0FBQztJQUNkLENBQUM7Ozs7O0lBRUQsT0FBTyxDQUFDLElBQWU7O1lBQ2pCLFFBQVEsR0FBRyxJQUFJLENBQUMsUUFBUSxDQUFDLEdBQUcsQ0FBQyxJQUFJLENBQUMsSUFBSSxJQUFJO1FBRTlDLElBQUksQ0FBQyxRQUFRLEVBQUU7WUFDYixRQUFRLEdBQUcsSUFBSSxDQUFDLGFBQWEsQ0FBQyxJQUFJLENBQUMsQ0FBQztZQUNwQyxJQUFJLFFBQVEsRUFBRTs7c0JBQ04sU0FBUyxHQUFHLElBQUksQ0FBQyxTQUFTLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQztnQkFDMUMsSUFBSSxTQUFTLEVBQUU7OzBCQUNQLFNBQVMsR0FBRyxJQUFJLGlCQUFpQixFQUFFO29CQUN6QyxTQUFTLENBQUMsT0FBTzs7OztvQkFBQyxRQUFRLENBQUMsRUFBRTt3QkFDM0IsUUFBUSxHQUFHLFNBQVMsQ0FBQyxnQkFBZ0IsQ0FBQyxJQUFJLENBQUMsSUFBSSxFQUFFLG1CQUFBLFFBQVEsRUFBRSxFQUFFLFFBQVEsQ0FBQyxDQUFDO29CQUN6RSxDQUFDLEVBQUMsQ0FBQztpQkFDSjthQUNGO1lBQ0QsSUFBSSxDQUFDLFFBQVEsQ0FBQyxHQUFHLENBQUMsSUFBSSxFQUFFLFFBQVEsQ0FBQyxDQUFDO1NBQ25DO1FBRUQsT0FBTyxRQUFRLENBQUM7SUFDbEIsQ0FBQztDQUNGOzs7Ozs7SUF0REMscUNBQWdFOzs7OztJQUNoRSxvQ0FBZ0Q7Ozs7O0lBRWhELGtEQUF5Qjs7QUFzRDNCLE1BQU0sT0FBTyxpQkFBa0IsU0FBUSxnQkFBMkI7Ozs7SUFDaEUsSUFBSSxJQUFJLEtBQUssT0FBTyxTQUFTLENBQUMsQ0FBQyxDQUFDO0NBQ2pDO0FBRUQsTUFBTSxPQUFPLGlCQUFrQixTQUFRLGdCQUEyQjs7OztJQUNoRSxJQUFJLElBQUksS0FBSyxPQUFPLFNBQVMsQ0FBQyxDQUFDLENBQUM7Q0FDakM7QUFFRCxNQUFNLE9BQU8sWUFBYSxTQUFRLGdCQUFzQjs7OztJQUN0RCxJQUFJLElBQUksS0FBSyxPQUFPLElBQUksQ0FBQyxDQUFDLENBQUM7Q0FDNUI7QUFFRCxNQUFNLE9BQU8sZ0JBQWlCLFNBQVEsZ0JBQTBCOzs7O0lBQzlELElBQUksSUFBSSxLQUFLLE9BQU8sUUFBUSxDQUFDLENBQUMsQ0FBQztDQUNoQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDb21wb25lbnQsIERpcmVjdGl2ZSwgTmdNb2R1bGUsIFBpcGUsIFR5cGUsIMm1UmVmbGVjdGlvbkNhcGFiaWxpdGllcyBhcyBSZWZsZWN0aW9uQ2FwYWJpbGl0aWVzfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHtNZXRhZGF0YU92ZXJyaWRlfSBmcm9tICcuL21ldGFkYXRhX292ZXJyaWRlJztcbmltcG9ydCB7TWV0YWRhdGFPdmVycmlkZXJ9IGZyb20gJy4vbWV0YWRhdGFfb3ZlcnJpZGVyJztcblxuY29uc3QgcmVmbGVjdGlvbiA9IG5ldyBSZWZsZWN0aW9uQ2FwYWJpbGl0aWVzKCk7XG5cbi8qKlxuICogQmFzZSBpbnRlcmZhY2UgdG8gcmVzb2x2ZSBgQENvbXBvbmVudGAsIGBARGlyZWN0aXZlYCwgYEBQaXBlYCBhbmQgYEBOZ01vZHVsZWAuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgUmVzb2x2ZXI8VD4ge1xuICBhZGRPdmVycmlkZSh0eXBlOiBUeXBlPGFueT4sIG92ZXJyaWRlOiBNZXRhZGF0YU92ZXJyaWRlPFQ+KTogdm9pZDtcbiAgc2V0T3ZlcnJpZGVzKG92ZXJyaWRlczogQXJyYXk8W1R5cGU8YW55PiwgTWV0YWRhdGFPdmVycmlkZTxUPl0+KTogdm9pZDtcbiAgcmVzb2x2ZSh0eXBlOiBUeXBlPGFueT4pOiBUfG51bGw7XG59XG5cbi8qKlxuICogQWxsb3dzIHRvIG92ZXJyaWRlIGl2eSBtZXRhZGF0YSBmb3IgdGVzdHMgKHZpYSB0aGUgYFRlc3RCZWRgKS5cbiAqL1xuYWJzdHJhY3QgY2xhc3MgT3ZlcnJpZGVSZXNvbHZlcjxUPiBpbXBsZW1lbnRzIFJlc29sdmVyPFQ+IHtcbiAgcHJpdmF0ZSBvdmVycmlkZXMgPSBuZXcgTWFwPFR5cGU8YW55PiwgTWV0YWRhdGFPdmVycmlkZTxUPltdPigpO1xuICBwcml2YXRlIHJlc29sdmVkID0gbmV3IE1hcDxUeXBlPGFueT4sIFR8bnVsbD4oKTtcblxuICBhYnN0cmFjdCBnZXQgdHlwZSgpOiBhbnk7XG5cbiAgYWRkT3ZlcnJpZGUodHlwZTogVHlwZTxhbnk+LCBvdmVycmlkZTogTWV0YWRhdGFPdmVycmlkZTxUPikge1xuICAgIGNvbnN0IG92ZXJyaWRlcyA9IHRoaXMub3ZlcnJpZGVzLmdldCh0eXBlKSB8fCBbXTtcbiAgICBvdmVycmlkZXMucHVzaChvdmVycmlkZSk7XG4gICAgdGhpcy5vdmVycmlkZXMuc2V0KHR5cGUsIG92ZXJyaWRlcyk7XG4gICAgdGhpcy5yZXNvbHZlZC5kZWxldGUodHlwZSk7XG4gIH1cblxuICBzZXRPdmVycmlkZXMob3ZlcnJpZGVzOiBBcnJheTxbVHlwZTxhbnk+LCBNZXRhZGF0YU92ZXJyaWRlPFQ+XT4pIHtcbiAgICB0aGlzLm92ZXJyaWRlcy5jbGVhcigpO1xuICAgIG92ZXJyaWRlcy5mb3JFYWNoKChbdHlwZSwgb3ZlcnJpZGVdKSA9PiB7IHRoaXMuYWRkT3ZlcnJpZGUodHlwZSwgb3ZlcnJpZGUpOyB9KTtcbiAgfVxuXG4gIGdldEFubm90YXRpb24odHlwZTogVHlwZTxhbnk+KTogVHxudWxsIHtcbiAgICBjb25zdCBhbm5vdGF0aW9ucyA9IHJlZmxlY3Rpb24uYW5ub3RhdGlvbnModHlwZSk7XG4gICAgLy8gVHJ5IHRvIGZpbmQgdGhlIG5lYXJlc3Qga25vd24gVHlwZSBhbm5vdGF0aW9uIGFuZCBtYWtlIHN1cmUgdGhhdCB0aGlzIGFubm90YXRpb24gaXMgYW5cbiAgICAvLyBpbnN0YW5jZSBvZiB0aGUgdHlwZSB3ZSBhcmUgbG9va2luZyBmb3IsIHNvIHdlIGNhbiB1c2UgaXQgZm9yIHJlc29sdXRpb24uIE5vdGU6IHRoZXJlIG1pZ2h0XG4gICAgLy8gYmUgbXVsdGlwbGUga25vd24gYW5ub3RhdGlvbnMgZm91bmQgZHVlIHRvIHRoZSBmYWN0IHRoYXQgQ29tcG9uZW50cyBjYW4gZXh0ZW5kIERpcmVjdGl2ZXMgKHNvXG4gICAgLy8gYm90aCBEaXJlY3RpdmUgYW5kIENvbXBvbmVudCBhbm5vdGF0aW9ucyB3b3VsZCBiZSBwcmVzZW50KSwgc28gd2UgYWx3YXlzIGNoZWNrIGlmIHRoZSBrbm93blxuICAgIC8vIGFubm90YXRpb24gaGFzIHRoZSByaWdodCB0eXBlLlxuICAgIGZvciAobGV0IGkgPSBhbm5vdGF0aW9ucy5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgICAgY29uc3QgYW5ub3RhdGlvbiA9IGFubm90YXRpb25zW2ldO1xuICAgICAgY29uc3QgaXNLbm93blR5cGUgPSBhbm5vdGF0aW9uIGluc3RhbmNlb2YgRGlyZWN0aXZlIHx8IGFubm90YXRpb24gaW5zdGFuY2VvZiBDb21wb25lbnQgfHxcbiAgICAgICAgICBhbm5vdGF0aW9uIGluc3RhbmNlb2YgUGlwZSB8fCBhbm5vdGF0aW9uIGluc3RhbmNlb2YgTmdNb2R1bGU7XG4gICAgICBpZiAoaXNLbm93blR5cGUpIHtcbiAgICAgICAgcmV0dXJuIGFubm90YXRpb24gaW5zdGFuY2VvZiB0aGlzLnR5cGUgPyBhbm5vdGF0aW9uIDogbnVsbDtcbiAgICAgIH1cbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICByZXNvbHZlKHR5cGU6IFR5cGU8YW55Pik6IFR8bnVsbCB7XG4gICAgbGV0IHJlc29sdmVkID0gdGhpcy5yZXNvbHZlZC5nZXQodHlwZSkgfHwgbnVsbDtcblxuICAgIGlmICghcmVzb2x2ZWQpIHtcbiAgICAgIHJlc29sdmVkID0gdGhpcy5nZXRBbm5vdGF0aW9uKHR5cGUpO1xuICAgICAgaWYgKHJlc29sdmVkKSB7XG4gICAgICAgIGNvbnN0IG92ZXJyaWRlcyA9IHRoaXMub3ZlcnJpZGVzLmdldCh0eXBlKTtcbiAgICAgICAgaWYgKG92ZXJyaWRlcykge1xuICAgICAgICAgIGNvbnN0IG92ZXJyaWRlciA9IG5ldyBNZXRhZGF0YU92ZXJyaWRlcigpO1xuICAgICAgICAgIG92ZXJyaWRlcy5mb3JFYWNoKG92ZXJyaWRlID0+IHtcbiAgICAgICAgICAgIHJlc29sdmVkID0gb3ZlcnJpZGVyLm92ZXJyaWRlTWV0YWRhdGEodGhpcy50eXBlLCByZXNvbHZlZCAhLCBvdmVycmlkZSk7XG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIHRoaXMucmVzb2x2ZWQuc2V0KHR5cGUsIHJlc29sdmVkKTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVzb2x2ZWQ7XG4gIH1cbn1cblxuXG5leHBvcnQgY2xhc3MgRGlyZWN0aXZlUmVzb2x2ZXIgZXh0ZW5kcyBPdmVycmlkZVJlc29sdmVyPERpcmVjdGl2ZT4ge1xuICBnZXQgdHlwZSgpIHsgcmV0dXJuIERpcmVjdGl2ZTsgfVxufVxuXG5leHBvcnQgY2xhc3MgQ29tcG9uZW50UmVzb2x2ZXIgZXh0ZW5kcyBPdmVycmlkZVJlc29sdmVyPENvbXBvbmVudD4ge1xuICBnZXQgdHlwZSgpIHsgcmV0dXJuIENvbXBvbmVudDsgfVxufVxuXG5leHBvcnQgY2xhc3MgUGlwZVJlc29sdmVyIGV4dGVuZHMgT3ZlcnJpZGVSZXNvbHZlcjxQaXBlPiB7XG4gIGdldCB0eXBlKCkgeyByZXR1cm4gUGlwZTsgfVxufVxuXG5leHBvcnQgY2xhc3MgTmdNb2R1bGVSZXNvbHZlciBleHRlbmRzIE92ZXJyaWRlUmVzb2x2ZXI8TmdNb2R1bGU+IHtcbiAgZ2V0IHR5cGUoKSB7IHJldHVybiBOZ01vZHVsZTsgfVxufVxuIl19
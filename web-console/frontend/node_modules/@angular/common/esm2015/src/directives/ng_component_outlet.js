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
import { ComponentFactoryResolver, Directive, Injector, Input, NgModuleFactory, NgModuleRef, Type, ViewContainerRef } from '@angular/core';
/**
 * Instantiates a single {\@link Component} type and inserts its Host View into current View.
 * `NgComponentOutlet` provides a declarative approach for dynamic component creation.
 *
 * `NgComponentOutlet` requires a component type, if a falsy value is set the view will clear and
 * any existing component will get destroyed.
 *
 * \@usageNotes
 *
 * ### Fine tune control
 *
 * You can control the component creation process by using the following optional attributes:
 *
 * * `ngComponentOutletInjector`: Optional custom {\@link Injector} that will be used as parent for
 * the Component. Defaults to the injector of the current view container.
 *
 * * `ngComponentOutletContent`: Optional list of projectable nodes to insert into the content
 * section of the component, if exists.
 *
 * * `ngComponentOutletNgModuleFactory`: Optional module factory to allow dynamically loading other
 * module, then load a component from that module.
 *
 * ### Syntax
 *
 * Simple
 * ```
 * <ng-container *ngComponentOutlet="componentTypeExpression"></ng-container>
 * ```
 *
 * Customized injector/content
 * ```
 * <ng-container *ngComponentOutlet="componentTypeExpression;
 *                                   injector: injectorExpression;
 *                                   content: contentNodesExpression;">
 * </ng-container>
 * ```
 *
 * Customized ngModuleFactory
 * ```
 * <ng-container *ngComponentOutlet="componentTypeExpression;
 *                                   ngModuleFactory: moduleFactory;">
 * </ng-container>
 * ```
 *
 * ### A simple example
 *
 * {\@example common/ngComponentOutlet/ts/module.ts region='SimpleExample'}
 *
 * A more complete example with additional options:
 *
 * {\@example common/ngComponentOutlet/ts/module.ts region='CompleteExample'}
 *
 * \@publicApi
 * \@ngModule CommonModule
 */
export class NgComponentOutlet {
    /**
     * @param {?} _viewContainerRef
     */
    constructor(_viewContainerRef) {
        this._viewContainerRef = _viewContainerRef;
        this._componentRef = null;
        this._moduleRef = null;
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        this._viewContainerRef.clear();
        this._componentRef = null;
        if (this.ngComponentOutlet) {
            /** @type {?} */
            const elInjector = this.ngComponentOutletInjector || this._viewContainerRef.parentInjector;
            if (changes['ngComponentOutletNgModuleFactory']) {
                if (this._moduleRef)
                    this._moduleRef.destroy();
                if (this.ngComponentOutletNgModuleFactory) {
                    /** @type {?} */
                    const parentModule = elInjector.get(NgModuleRef);
                    this._moduleRef = this.ngComponentOutletNgModuleFactory.create(parentModule.injector);
                }
                else {
                    this._moduleRef = null;
                }
            }
            /** @type {?} */
            const componentFactoryResolver = this._moduleRef ? this._moduleRef.componentFactoryResolver :
                elInjector.get(ComponentFactoryResolver);
            /** @type {?} */
            const componentFactory = componentFactoryResolver.resolveComponentFactory(this.ngComponentOutlet);
            this._componentRef = this._viewContainerRef.createComponent(componentFactory, this._viewContainerRef.length, elInjector, this.ngComponentOutletContent);
        }
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        if (this._moduleRef)
            this._moduleRef.destroy();
    }
}
NgComponentOutlet.decorators = [
    { type: Directive, args: [{ selector: '[ngComponentOutlet]' },] }
];
/** @nocollapse */
NgComponentOutlet.ctorParameters = () => [
    { type: ViewContainerRef }
];
NgComponentOutlet.propDecorators = {
    ngComponentOutlet: [{ type: Input }],
    ngComponentOutletInjector: [{ type: Input }],
    ngComponentOutletContent: [{ type: Input }],
    ngComponentOutletNgModuleFactory: [{ type: Input }]
};
if (false) {
    /** @type {?} */
    NgComponentOutlet.prototype.ngComponentOutlet;
    /** @type {?} */
    NgComponentOutlet.prototype.ngComponentOutletInjector;
    /** @type {?} */
    NgComponentOutlet.prototype.ngComponentOutletContent;
    /** @type {?} */
    NgComponentOutlet.prototype.ngComponentOutletNgModuleFactory;
    /**
     * @type {?}
     * @private
     */
    NgComponentOutlet.prototype._componentRef;
    /**
     * @type {?}
     * @private
     */
    NgComponentOutlet.prototype._moduleRef;
    /**
     * @type {?}
     * @private
     */
    NgComponentOutlet.prototype._viewContainerRef;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfY29tcG9uZW50X291dGxldC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi9zcmMvZGlyZWN0aXZlcy9uZ19jb21wb25lbnRfb3V0bGV0LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLHdCQUF3QixFQUFnQixTQUFTLEVBQUUsUUFBUSxFQUFFLEtBQUssRUFBRSxlQUFlLEVBQUUsV0FBVyxFQUF1RCxJQUFJLEVBQUUsZ0JBQWdCLEVBQUMsTUFBTSxlQUFlLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBMkQ1TSxNQUFNLE9BQU8saUJBQWlCOzs7O0lBYTVCLFlBQW9CLGlCQUFtQztRQUFuQyxzQkFBaUIsR0FBakIsaUJBQWlCLENBQWtCO1FBSC9DLGtCQUFhLEdBQTJCLElBQUksQ0FBQztRQUM3QyxlQUFVLEdBQTBCLElBQUksQ0FBQztJQUVTLENBQUM7Ozs7O0lBRTNELFdBQVcsQ0FBQyxPQUFzQjtRQUNoQyxJQUFJLENBQUMsaUJBQWlCLENBQUMsS0FBSyxFQUFFLENBQUM7UUFDL0IsSUFBSSxDQUFDLGFBQWEsR0FBRyxJQUFJLENBQUM7UUFFMUIsSUFBSSxJQUFJLENBQUMsaUJBQWlCLEVBQUU7O2tCQUNwQixVQUFVLEdBQUcsSUFBSSxDQUFDLHlCQUF5QixJQUFJLElBQUksQ0FBQyxpQkFBaUIsQ0FBQyxjQUFjO1lBRTFGLElBQUksT0FBTyxDQUFDLGtDQUFrQyxDQUFDLEVBQUU7Z0JBQy9DLElBQUksSUFBSSxDQUFDLFVBQVU7b0JBQUUsSUFBSSxDQUFDLFVBQVUsQ0FBQyxPQUFPLEVBQUUsQ0FBQztnQkFFL0MsSUFBSSxJQUFJLENBQUMsZ0NBQWdDLEVBQUU7OzBCQUNuQyxZQUFZLEdBQUcsVUFBVSxDQUFDLEdBQUcsQ0FBQyxXQUFXLENBQUM7b0JBQ2hELElBQUksQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDLGdDQUFnQyxDQUFDLE1BQU0sQ0FBQyxZQUFZLENBQUMsUUFBUSxDQUFDLENBQUM7aUJBQ3ZGO3FCQUFNO29CQUNMLElBQUksQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDO2lCQUN4QjthQUNGOztrQkFFSyx3QkFBd0IsR0FBRyxJQUFJLENBQUMsVUFBVSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLHdCQUF3QixDQUFDLENBQUM7Z0JBQzFDLFVBQVUsQ0FBQyxHQUFHLENBQUMsd0JBQXdCLENBQUM7O2tCQUVyRixnQkFBZ0IsR0FDbEIsd0JBQXdCLENBQUMsdUJBQXVCLENBQUMsSUFBSSxDQUFDLGlCQUFpQixDQUFDO1lBRTVFLElBQUksQ0FBQyxhQUFhLEdBQUcsSUFBSSxDQUFDLGlCQUFpQixDQUFDLGVBQWUsQ0FDdkQsZ0JBQWdCLEVBQUUsSUFBSSxDQUFDLGlCQUFpQixDQUFDLE1BQU0sRUFBRSxVQUFVLEVBQzNELElBQUksQ0FBQyx3QkFBd0IsQ0FBQyxDQUFDO1NBQ3BDO0lBQ0gsQ0FBQzs7OztJQUVELFdBQVc7UUFDVCxJQUFJLElBQUksQ0FBQyxVQUFVO1lBQUUsSUFBSSxDQUFDLFVBQVUsQ0FBQyxPQUFPLEVBQUUsQ0FBQztJQUNqRCxDQUFDOzs7WUFoREYsU0FBUyxTQUFDLEVBQUMsUUFBUSxFQUFFLHFCQUFxQixFQUFDOzs7O1lBMUR5SCxnQkFBZ0I7OztnQ0E2RGxMLEtBQUs7d0NBRUwsS0FBSzt1Q0FFTCxLQUFLOytDQUVMLEtBQUs7Ozs7SUFOTiw4Q0FBd0M7O0lBRXhDLHNEQUErQzs7SUFFL0MscURBQTZDOztJQUU3Qyw2REFBa0U7Ozs7O0lBRWxFLDBDQUFxRDs7Ozs7SUFDckQsdUNBQWlEOzs7OztJQUVyQyw4Q0FBMkMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Q29tcG9uZW50RmFjdG9yeVJlc29sdmVyLCBDb21wb25lbnRSZWYsIERpcmVjdGl2ZSwgSW5qZWN0b3IsIElucHV0LCBOZ01vZHVsZUZhY3RvcnksIE5nTW9kdWxlUmVmLCBPbkNoYW5nZXMsIE9uRGVzdHJveSwgU2ltcGxlQ2hhbmdlcywgU3RhdGljUHJvdmlkZXIsIFR5cGUsIFZpZXdDb250YWluZXJSZWZ9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5cbi8qKlxuICogSW5zdGFudGlhdGVzIGEgc2luZ2xlIHtAbGluayBDb21wb25lbnR9IHR5cGUgYW5kIGluc2VydHMgaXRzIEhvc3QgVmlldyBpbnRvIGN1cnJlbnQgVmlldy5cbiAqIGBOZ0NvbXBvbmVudE91dGxldGAgcHJvdmlkZXMgYSBkZWNsYXJhdGl2ZSBhcHByb2FjaCBmb3IgZHluYW1pYyBjb21wb25lbnQgY3JlYXRpb24uXG4gKlxuICogYE5nQ29tcG9uZW50T3V0bGV0YCByZXF1aXJlcyBhIGNvbXBvbmVudCB0eXBlLCBpZiBhIGZhbHN5IHZhbHVlIGlzIHNldCB0aGUgdmlldyB3aWxsIGNsZWFyIGFuZFxuICogYW55IGV4aXN0aW5nIGNvbXBvbmVudCB3aWxsIGdldCBkZXN0cm95ZWQuXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqXG4gKiAjIyMgRmluZSB0dW5lIGNvbnRyb2xcbiAqXG4gKiBZb3UgY2FuIGNvbnRyb2wgdGhlIGNvbXBvbmVudCBjcmVhdGlvbiBwcm9jZXNzIGJ5IHVzaW5nIHRoZSBmb2xsb3dpbmcgb3B0aW9uYWwgYXR0cmlidXRlczpcbiAqXG4gKiAqIGBuZ0NvbXBvbmVudE91dGxldEluamVjdG9yYDogT3B0aW9uYWwgY3VzdG9tIHtAbGluayBJbmplY3Rvcn0gdGhhdCB3aWxsIGJlIHVzZWQgYXMgcGFyZW50IGZvclxuICogdGhlIENvbXBvbmVudC4gRGVmYXVsdHMgdG8gdGhlIGluamVjdG9yIG9mIHRoZSBjdXJyZW50IHZpZXcgY29udGFpbmVyLlxuICpcbiAqICogYG5nQ29tcG9uZW50T3V0bGV0Q29udGVudGA6IE9wdGlvbmFsIGxpc3Qgb2YgcHJvamVjdGFibGUgbm9kZXMgdG8gaW5zZXJ0IGludG8gdGhlIGNvbnRlbnRcbiAqIHNlY3Rpb24gb2YgdGhlIGNvbXBvbmVudCwgaWYgZXhpc3RzLlxuICpcbiAqICogYG5nQ29tcG9uZW50T3V0bGV0TmdNb2R1bGVGYWN0b3J5YDogT3B0aW9uYWwgbW9kdWxlIGZhY3RvcnkgdG8gYWxsb3cgZHluYW1pY2FsbHkgbG9hZGluZyBvdGhlclxuICogbW9kdWxlLCB0aGVuIGxvYWQgYSBjb21wb25lbnQgZnJvbSB0aGF0IG1vZHVsZS5cbiAqXG4gKiAjIyMgU3ludGF4XG4gKlxuICogU2ltcGxlXG4gKiBgYGBcbiAqIDxuZy1jb250YWluZXIgKm5nQ29tcG9uZW50T3V0bGV0PVwiY29tcG9uZW50VHlwZUV4cHJlc3Npb25cIj48L25nLWNvbnRhaW5lcj5cbiAqIGBgYFxuICpcbiAqIEN1c3RvbWl6ZWQgaW5qZWN0b3IvY29udGVudFxuICogYGBgXG4gKiA8bmctY29udGFpbmVyICpuZ0NvbXBvbmVudE91dGxldD1cImNvbXBvbmVudFR5cGVFeHByZXNzaW9uO1xuICogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGluamVjdG9yOiBpbmplY3RvckV4cHJlc3Npb247XG4gKiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgY29udGVudDogY29udGVudE5vZGVzRXhwcmVzc2lvbjtcIj5cbiAqIDwvbmctY29udGFpbmVyPlxuICogYGBgXG4gKlxuICogQ3VzdG9taXplZCBuZ01vZHVsZUZhY3RvcnlcbiAqIGBgYFxuICogPG5nLWNvbnRhaW5lciAqbmdDb21wb25lbnRPdXRsZXQ9XCJjb21wb25lbnRUeXBlRXhwcmVzc2lvbjtcbiAqICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBuZ01vZHVsZUZhY3Rvcnk6IG1vZHVsZUZhY3Rvcnk7XCI+XG4gKiA8L25nLWNvbnRhaW5lcj5cbiAqIGBgYFxuICpcbiAqICMjIyBBIHNpbXBsZSBleGFtcGxlXG4gKlxuICoge0BleGFtcGxlIGNvbW1vbi9uZ0NvbXBvbmVudE91dGxldC90cy9tb2R1bGUudHMgcmVnaW9uPSdTaW1wbGVFeGFtcGxlJ31cbiAqXG4gKiBBIG1vcmUgY29tcGxldGUgZXhhbXBsZSB3aXRoIGFkZGl0aW9uYWwgb3B0aW9uczpcbiAqXG4gKiB7QGV4YW1wbGUgY29tbW9uL25nQ29tcG9uZW50T3V0bGV0L3RzL21vZHVsZS50cyByZWdpb249J0NvbXBsZXRlRXhhbXBsZSd9XG4gKlxuICogQHB1YmxpY0FwaVxuICogQG5nTW9kdWxlIENvbW1vbk1vZHVsZVxuICovXG5ARGlyZWN0aXZlKHtzZWxlY3RvcjogJ1tuZ0NvbXBvbmVudE91dGxldF0nfSlcbmV4cG9ydCBjbGFzcyBOZ0NvbXBvbmVudE91dGxldCBpbXBsZW1lbnRzIE9uQ2hhbmdlcywgT25EZXN0cm95IHtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIEBJbnB1dCgpIG5nQ29tcG9uZW50T3V0bGV0ICE6IFR5cGU8YW55PjtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIEBJbnB1dCgpIG5nQ29tcG9uZW50T3V0bGV0SW5qZWN0b3IgITogSW5qZWN0b3I7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBASW5wdXQoKSBuZ0NvbXBvbmVudE91dGxldENvbnRlbnQgITogYW55W11bXTtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIEBJbnB1dCgpIG5nQ29tcG9uZW50T3V0bGV0TmdNb2R1bGVGYWN0b3J5ICE6IE5nTW9kdWxlRmFjdG9yeTxhbnk+O1xuXG4gIHByaXZhdGUgX2NvbXBvbmVudFJlZjogQ29tcG9uZW50UmVmPGFueT58bnVsbCA9IG51bGw7XG4gIHByaXZhdGUgX21vZHVsZVJlZjogTmdNb2R1bGVSZWY8YW55PnxudWxsID0gbnVsbDtcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIF92aWV3Q29udGFpbmVyUmVmOiBWaWV3Q29udGFpbmVyUmVmKSB7fVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpIHtcbiAgICB0aGlzLl92aWV3Q29udGFpbmVyUmVmLmNsZWFyKCk7XG4gICAgdGhpcy5fY29tcG9uZW50UmVmID0gbnVsbDtcblxuICAgIGlmICh0aGlzLm5nQ29tcG9uZW50T3V0bGV0KSB7XG4gICAgICBjb25zdCBlbEluamVjdG9yID0gdGhpcy5uZ0NvbXBvbmVudE91dGxldEluamVjdG9yIHx8IHRoaXMuX3ZpZXdDb250YWluZXJSZWYucGFyZW50SW5qZWN0b3I7XG5cbiAgICAgIGlmIChjaGFuZ2VzWyduZ0NvbXBvbmVudE91dGxldE5nTW9kdWxlRmFjdG9yeSddKSB7XG4gICAgICAgIGlmICh0aGlzLl9tb2R1bGVSZWYpIHRoaXMuX21vZHVsZVJlZi5kZXN0cm95KCk7XG5cbiAgICAgICAgaWYgKHRoaXMubmdDb21wb25lbnRPdXRsZXROZ01vZHVsZUZhY3RvcnkpIHtcbiAgICAgICAgICBjb25zdCBwYXJlbnRNb2R1bGUgPSBlbEluamVjdG9yLmdldChOZ01vZHVsZVJlZik7XG4gICAgICAgICAgdGhpcy5fbW9kdWxlUmVmID0gdGhpcy5uZ0NvbXBvbmVudE91dGxldE5nTW9kdWxlRmFjdG9yeS5jcmVhdGUocGFyZW50TW9kdWxlLmluamVjdG9yKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB0aGlzLl9tb2R1bGVSZWYgPSBudWxsO1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IGNvbXBvbmVudEZhY3RvcnlSZXNvbHZlciA9IHRoaXMuX21vZHVsZVJlZiA/IHRoaXMuX21vZHVsZVJlZi5jb21wb25lbnRGYWN0b3J5UmVzb2x2ZXIgOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgZWxJbmplY3Rvci5nZXQoQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyKTtcblxuICAgICAgY29uc3QgY29tcG9uZW50RmFjdG9yeSA9XG4gICAgICAgICAgY29tcG9uZW50RmFjdG9yeVJlc29sdmVyLnJlc29sdmVDb21wb25lbnRGYWN0b3J5KHRoaXMubmdDb21wb25lbnRPdXRsZXQpO1xuXG4gICAgICB0aGlzLl9jb21wb25lbnRSZWYgPSB0aGlzLl92aWV3Q29udGFpbmVyUmVmLmNyZWF0ZUNvbXBvbmVudChcbiAgICAgICAgICBjb21wb25lbnRGYWN0b3J5LCB0aGlzLl92aWV3Q29udGFpbmVyUmVmLmxlbmd0aCwgZWxJbmplY3RvcixcbiAgICAgICAgICB0aGlzLm5nQ29tcG9uZW50T3V0bGV0Q29udGVudCk7XG4gICAgfVxuICB9XG5cbiAgbmdPbkRlc3Ryb3koKSB7XG4gICAgaWYgKHRoaXMuX21vZHVsZVJlZikgdGhpcy5fbW9kdWxlUmVmLmRlc3Ryb3koKTtcbiAgfVxufVxuIl19
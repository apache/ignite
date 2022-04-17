/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { ComponentFactoryResolver, Directive, Injector, Input, NgModuleFactory, NgModuleRef, Type, ViewContainerRef } from '@angular/core';
/**
 * Instantiates a single {@link Component} type and inserts its Host View into current View.
 * `NgComponentOutlet` provides a declarative approach for dynamic component creation.
 *
 * `NgComponentOutlet` requires a component type, if a falsy value is set the view will clear and
 * any existing component will get destroyed.
 *
 * @usageNotes
 *
 * ### Fine tune control
 *
 * You can control the component creation process by using the following optional attributes:
 *
 * * `ngComponentOutletInjector`: Optional custom {@link Injector} that will be used as parent for
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
 * {@example common/ngComponentOutlet/ts/module.ts region='SimpleExample'}
 *
 * A more complete example with additional options:
 *
 * {@example common/ngComponentOutlet/ts/module.ts region='CompleteExample'}
 *
 * @publicApi
 * @ngModule CommonModule
 */
var NgComponentOutlet = /** @class */ (function () {
    function NgComponentOutlet(_viewContainerRef) {
        this._viewContainerRef = _viewContainerRef;
        this._componentRef = null;
        this._moduleRef = null;
    }
    NgComponentOutlet.prototype.ngOnChanges = function (changes) {
        this._viewContainerRef.clear();
        this._componentRef = null;
        if (this.ngComponentOutlet) {
            var elInjector = this.ngComponentOutletInjector || this._viewContainerRef.parentInjector;
            if (changes['ngComponentOutletNgModuleFactory']) {
                if (this._moduleRef)
                    this._moduleRef.destroy();
                if (this.ngComponentOutletNgModuleFactory) {
                    var parentModule = elInjector.get(NgModuleRef);
                    this._moduleRef = this.ngComponentOutletNgModuleFactory.create(parentModule.injector);
                }
                else {
                    this._moduleRef = null;
                }
            }
            var componentFactoryResolver = this._moduleRef ? this._moduleRef.componentFactoryResolver :
                elInjector.get(ComponentFactoryResolver);
            var componentFactory = componentFactoryResolver.resolveComponentFactory(this.ngComponentOutlet);
            this._componentRef = this._viewContainerRef.createComponent(componentFactory, this._viewContainerRef.length, elInjector, this.ngComponentOutletContent);
        }
    };
    NgComponentOutlet.prototype.ngOnDestroy = function () {
        if (this._moduleRef)
            this._moduleRef.destroy();
    };
    tslib_1.__decorate([
        Input(),
        tslib_1.__metadata("design:type", Type)
    ], NgComponentOutlet.prototype, "ngComponentOutlet", void 0);
    tslib_1.__decorate([
        Input(),
        tslib_1.__metadata("design:type", Injector)
    ], NgComponentOutlet.prototype, "ngComponentOutletInjector", void 0);
    tslib_1.__decorate([
        Input(),
        tslib_1.__metadata("design:type", Array)
    ], NgComponentOutlet.prototype, "ngComponentOutletContent", void 0);
    tslib_1.__decorate([
        Input(),
        tslib_1.__metadata("design:type", NgModuleFactory)
    ], NgComponentOutlet.prototype, "ngComponentOutletNgModuleFactory", void 0);
    NgComponentOutlet = tslib_1.__decorate([
        Directive({ selector: '[ngComponentOutlet]' }),
        tslib_1.__metadata("design:paramtypes", [ViewContainerRef])
    ], NgComponentOutlet);
    return NgComponentOutlet;
}());
export { NgComponentOutlet };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfY29tcG9uZW50X291dGxldC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi9zcmMvZGlyZWN0aXZlcy9uZ19jb21wb25lbnRfb3V0bGV0LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFFSCxPQUFPLEVBQUMsd0JBQXdCLEVBQWdCLFNBQVMsRUFBRSxRQUFRLEVBQUUsS0FBSyxFQUFFLGVBQWUsRUFBRSxXQUFXLEVBQXVELElBQUksRUFBRSxnQkFBZ0IsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUc1TTs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0dBc0RHO0FBRUg7SUFhRSwyQkFBb0IsaUJBQW1DO1FBQW5DLHNCQUFpQixHQUFqQixpQkFBaUIsQ0FBa0I7UUFIL0Msa0JBQWEsR0FBMkIsSUFBSSxDQUFDO1FBQzdDLGVBQVUsR0FBMEIsSUFBSSxDQUFDO0lBRVMsQ0FBQztJQUUzRCx1Q0FBVyxHQUFYLFVBQVksT0FBc0I7UUFDaEMsSUFBSSxDQUFDLGlCQUFpQixDQUFDLEtBQUssRUFBRSxDQUFDO1FBQy9CLElBQUksQ0FBQyxhQUFhLEdBQUcsSUFBSSxDQUFDO1FBRTFCLElBQUksSUFBSSxDQUFDLGlCQUFpQixFQUFFO1lBQzFCLElBQU0sVUFBVSxHQUFHLElBQUksQ0FBQyx5QkFBeUIsSUFBSSxJQUFJLENBQUMsaUJBQWlCLENBQUMsY0FBYyxDQUFDO1lBRTNGLElBQUksT0FBTyxDQUFDLGtDQUFrQyxDQUFDLEVBQUU7Z0JBQy9DLElBQUksSUFBSSxDQUFDLFVBQVU7b0JBQUUsSUFBSSxDQUFDLFVBQVUsQ0FBQyxPQUFPLEVBQUUsQ0FBQztnQkFFL0MsSUFBSSxJQUFJLENBQUMsZ0NBQWdDLEVBQUU7b0JBQ3pDLElBQU0sWUFBWSxHQUFHLFVBQVUsQ0FBQyxHQUFHLENBQUMsV0FBVyxDQUFDLENBQUM7b0JBQ2pELElBQUksQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDLGdDQUFnQyxDQUFDLE1BQU0sQ0FBQyxZQUFZLENBQUMsUUFBUSxDQUFDLENBQUM7aUJBQ3ZGO3FCQUFNO29CQUNMLElBQUksQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDO2lCQUN4QjthQUNGO1lBRUQsSUFBTSx3QkFBd0IsR0FBRyxJQUFJLENBQUMsVUFBVSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLHdCQUF3QixDQUFDLENBQUM7Z0JBQzFDLFVBQVUsQ0FBQyxHQUFHLENBQUMsd0JBQXdCLENBQUMsQ0FBQztZQUU1RixJQUFNLGdCQUFnQixHQUNsQix3QkFBd0IsQ0FBQyx1QkFBdUIsQ0FBQyxJQUFJLENBQUMsaUJBQWlCLENBQUMsQ0FBQztZQUU3RSxJQUFJLENBQUMsYUFBYSxHQUFHLElBQUksQ0FBQyxpQkFBaUIsQ0FBQyxlQUFlLENBQ3ZELGdCQUFnQixFQUFFLElBQUksQ0FBQyxpQkFBaUIsQ0FBQyxNQUFNLEVBQUUsVUFBVSxFQUMzRCxJQUFJLENBQUMsd0JBQXdCLENBQUMsQ0FBQztTQUNwQztJQUNILENBQUM7SUFFRCx1Q0FBVyxHQUFYO1FBQ0UsSUFBSSxJQUFJLENBQUMsVUFBVTtZQUFFLElBQUksQ0FBQyxVQUFVLENBQUMsT0FBTyxFQUFFLENBQUM7SUFDakQsQ0FBQztJQTdDUTtRQUFSLEtBQUssRUFBRTswQ0FBc0IsSUFBSTtnRUFBTTtJQUUvQjtRQUFSLEtBQUssRUFBRTswQ0FBOEIsUUFBUTt3RUFBQztJQUV0QztRQUFSLEtBQUssRUFBRTs7dUVBQXFDO0lBRXBDO1FBQVIsS0FBSyxFQUFFOzBDQUFxQyxlQUFlOytFQUFNO0lBUnZELGlCQUFpQjtRQUQ3QixTQUFTLENBQUMsRUFBQyxRQUFRLEVBQUUscUJBQXFCLEVBQUMsQ0FBQztpREFjSixnQkFBZ0I7T0FiNUMsaUJBQWlCLENBZ0Q3QjtJQUFELHdCQUFDO0NBQUEsQUFoREQsSUFnREM7U0FoRFksaUJBQWlCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBvbmVudEZhY3RvcnlSZXNvbHZlciwgQ29tcG9uZW50UmVmLCBEaXJlY3RpdmUsIEluamVjdG9yLCBJbnB1dCwgTmdNb2R1bGVGYWN0b3J5LCBOZ01vZHVsZVJlZiwgT25DaGFuZ2VzLCBPbkRlc3Ryb3ksIFNpbXBsZUNoYW5nZXMsIFN0YXRpY1Byb3ZpZGVyLCBUeXBlLCBWaWV3Q29udGFpbmVyUmVmfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuXG4vKipcbiAqIEluc3RhbnRpYXRlcyBhIHNpbmdsZSB7QGxpbmsgQ29tcG9uZW50fSB0eXBlIGFuZCBpbnNlcnRzIGl0cyBIb3N0IFZpZXcgaW50byBjdXJyZW50IFZpZXcuXG4gKiBgTmdDb21wb25lbnRPdXRsZXRgIHByb3ZpZGVzIGEgZGVjbGFyYXRpdmUgYXBwcm9hY2ggZm9yIGR5bmFtaWMgY29tcG9uZW50IGNyZWF0aW9uLlxuICpcbiAqIGBOZ0NvbXBvbmVudE91dGxldGAgcmVxdWlyZXMgYSBjb21wb25lbnQgdHlwZSwgaWYgYSBmYWxzeSB2YWx1ZSBpcyBzZXQgdGhlIHZpZXcgd2lsbCBjbGVhciBhbmRcbiAqIGFueSBleGlzdGluZyBjb21wb25lbnQgd2lsbCBnZXQgZGVzdHJveWVkLlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKlxuICogIyMjIEZpbmUgdHVuZSBjb250cm9sXG4gKlxuICogWW91IGNhbiBjb250cm9sIHRoZSBjb21wb25lbnQgY3JlYXRpb24gcHJvY2VzcyBieSB1c2luZyB0aGUgZm9sbG93aW5nIG9wdGlvbmFsIGF0dHJpYnV0ZXM6XG4gKlxuICogKiBgbmdDb21wb25lbnRPdXRsZXRJbmplY3RvcmA6IE9wdGlvbmFsIGN1c3RvbSB7QGxpbmsgSW5qZWN0b3J9IHRoYXQgd2lsbCBiZSB1c2VkIGFzIHBhcmVudCBmb3JcbiAqIHRoZSBDb21wb25lbnQuIERlZmF1bHRzIHRvIHRoZSBpbmplY3RvciBvZiB0aGUgY3VycmVudCB2aWV3IGNvbnRhaW5lci5cbiAqXG4gKiAqIGBuZ0NvbXBvbmVudE91dGxldENvbnRlbnRgOiBPcHRpb25hbCBsaXN0IG9mIHByb2plY3RhYmxlIG5vZGVzIHRvIGluc2VydCBpbnRvIHRoZSBjb250ZW50XG4gKiBzZWN0aW9uIG9mIHRoZSBjb21wb25lbnQsIGlmIGV4aXN0cy5cbiAqXG4gKiAqIGBuZ0NvbXBvbmVudE91dGxldE5nTW9kdWxlRmFjdG9yeWA6IE9wdGlvbmFsIG1vZHVsZSBmYWN0b3J5IHRvIGFsbG93IGR5bmFtaWNhbGx5IGxvYWRpbmcgb3RoZXJcbiAqIG1vZHVsZSwgdGhlbiBsb2FkIGEgY29tcG9uZW50IGZyb20gdGhhdCBtb2R1bGUuXG4gKlxuICogIyMjIFN5bnRheFxuICpcbiAqIFNpbXBsZVxuICogYGBgXG4gKiA8bmctY29udGFpbmVyICpuZ0NvbXBvbmVudE91dGxldD1cImNvbXBvbmVudFR5cGVFeHByZXNzaW9uXCI+PC9uZy1jb250YWluZXI+XG4gKiBgYGBcbiAqXG4gKiBDdXN0b21pemVkIGluamVjdG9yL2NvbnRlbnRcbiAqIGBgYFxuICogPG5nLWNvbnRhaW5lciAqbmdDb21wb25lbnRPdXRsZXQ9XCJjb21wb25lbnRUeXBlRXhwcmVzc2lvbjtcbiAqICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBpbmplY3RvcjogaW5qZWN0b3JFeHByZXNzaW9uO1xuICogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNvbnRlbnQ6IGNvbnRlbnROb2Rlc0V4cHJlc3Npb247XCI+XG4gKiA8L25nLWNvbnRhaW5lcj5cbiAqIGBgYFxuICpcbiAqIEN1c3RvbWl6ZWQgbmdNb2R1bGVGYWN0b3J5XG4gKiBgYGBcbiAqIDxuZy1jb250YWluZXIgKm5nQ29tcG9uZW50T3V0bGV0PVwiY29tcG9uZW50VHlwZUV4cHJlc3Npb247XG4gKiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgbmdNb2R1bGVGYWN0b3J5OiBtb2R1bGVGYWN0b3J5O1wiPlxuICogPC9uZy1jb250YWluZXI+XG4gKiBgYGBcbiAqXG4gKiAjIyMgQSBzaW1wbGUgZXhhbXBsZVxuICpcbiAqIHtAZXhhbXBsZSBjb21tb24vbmdDb21wb25lbnRPdXRsZXQvdHMvbW9kdWxlLnRzIHJlZ2lvbj0nU2ltcGxlRXhhbXBsZSd9XG4gKlxuICogQSBtb3JlIGNvbXBsZXRlIGV4YW1wbGUgd2l0aCBhZGRpdGlvbmFsIG9wdGlvbnM6XG4gKlxuICoge0BleGFtcGxlIGNvbW1vbi9uZ0NvbXBvbmVudE91dGxldC90cy9tb2R1bGUudHMgcmVnaW9uPSdDb21wbGV0ZUV4YW1wbGUnfVxuICpcbiAqIEBwdWJsaWNBcGlcbiAqIEBuZ01vZHVsZSBDb21tb25Nb2R1bGVcbiAqL1xuQERpcmVjdGl2ZSh7c2VsZWN0b3I6ICdbbmdDb21wb25lbnRPdXRsZXRdJ30pXG5leHBvcnQgY2xhc3MgTmdDb21wb25lbnRPdXRsZXQgaW1wbGVtZW50cyBPbkNoYW5nZXMsIE9uRGVzdHJveSB7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBASW5wdXQoKSBuZ0NvbXBvbmVudE91dGxldCAhOiBUeXBlPGFueT47XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBASW5wdXQoKSBuZ0NvbXBvbmVudE91dGxldEluamVjdG9yICE6IEluamVjdG9yO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgQElucHV0KCkgbmdDb21wb25lbnRPdXRsZXRDb250ZW50ICE6IGFueVtdW107XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBASW5wdXQoKSBuZ0NvbXBvbmVudE91dGxldE5nTW9kdWxlRmFjdG9yeSAhOiBOZ01vZHVsZUZhY3Rvcnk8YW55PjtcblxuICBwcml2YXRlIF9jb21wb25lbnRSZWY6IENvbXBvbmVudFJlZjxhbnk+fG51bGwgPSBudWxsO1xuICBwcml2YXRlIF9tb2R1bGVSZWY6IE5nTW9kdWxlUmVmPGFueT58bnVsbCA9IG51bGw7XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBfdmlld0NvbnRhaW5lclJlZjogVmlld0NvbnRhaW5lclJlZikge31cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKSB7XG4gICAgdGhpcy5fdmlld0NvbnRhaW5lclJlZi5jbGVhcigpO1xuICAgIHRoaXMuX2NvbXBvbmVudFJlZiA9IG51bGw7XG5cbiAgICBpZiAodGhpcy5uZ0NvbXBvbmVudE91dGxldCkge1xuICAgICAgY29uc3QgZWxJbmplY3RvciA9IHRoaXMubmdDb21wb25lbnRPdXRsZXRJbmplY3RvciB8fCB0aGlzLl92aWV3Q29udGFpbmVyUmVmLnBhcmVudEluamVjdG9yO1xuXG4gICAgICBpZiAoY2hhbmdlc1snbmdDb21wb25lbnRPdXRsZXROZ01vZHVsZUZhY3RvcnknXSkge1xuICAgICAgICBpZiAodGhpcy5fbW9kdWxlUmVmKSB0aGlzLl9tb2R1bGVSZWYuZGVzdHJveSgpO1xuXG4gICAgICAgIGlmICh0aGlzLm5nQ29tcG9uZW50T3V0bGV0TmdNb2R1bGVGYWN0b3J5KSB7XG4gICAgICAgICAgY29uc3QgcGFyZW50TW9kdWxlID0gZWxJbmplY3Rvci5nZXQoTmdNb2R1bGVSZWYpO1xuICAgICAgICAgIHRoaXMuX21vZHVsZVJlZiA9IHRoaXMubmdDb21wb25lbnRPdXRsZXROZ01vZHVsZUZhY3RvcnkuY3JlYXRlKHBhcmVudE1vZHVsZS5pbmplY3Rvcik7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgdGhpcy5fbW9kdWxlUmVmID0gbnVsbDtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBjb25zdCBjb21wb25lbnRGYWN0b3J5UmVzb2x2ZXIgPSB0aGlzLl9tb2R1bGVSZWYgPyB0aGlzLl9tb2R1bGVSZWYuY29tcG9uZW50RmFjdG9yeVJlc29sdmVyIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGVsSW5qZWN0b3IuZ2V0KENvbXBvbmVudEZhY3RvcnlSZXNvbHZlcik7XG5cbiAgICAgIGNvbnN0IGNvbXBvbmVudEZhY3RvcnkgPVxuICAgICAgICAgIGNvbXBvbmVudEZhY3RvcnlSZXNvbHZlci5yZXNvbHZlQ29tcG9uZW50RmFjdG9yeSh0aGlzLm5nQ29tcG9uZW50T3V0bGV0KTtcblxuICAgICAgdGhpcy5fY29tcG9uZW50UmVmID0gdGhpcy5fdmlld0NvbnRhaW5lclJlZi5jcmVhdGVDb21wb25lbnQoXG4gICAgICAgICAgY29tcG9uZW50RmFjdG9yeSwgdGhpcy5fdmlld0NvbnRhaW5lclJlZi5sZW5ndGgsIGVsSW5qZWN0b3IsXG4gICAgICAgICAgdGhpcy5uZ0NvbXBvbmVudE91dGxldENvbnRlbnQpO1xuICAgIH1cbiAgfVxuXG4gIG5nT25EZXN0cm95KCkge1xuICAgIGlmICh0aGlzLl9tb2R1bGVSZWYpIHRoaXMuX21vZHVsZVJlZi5kZXN0cm95KCk7XG4gIH1cbn1cbiJdfQ==
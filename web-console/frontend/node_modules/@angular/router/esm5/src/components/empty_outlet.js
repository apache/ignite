/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Component } from '@angular/core';
/**
 * This component is used internally within the router to be a placeholder when an empty
 * router-outlet is needed. For example, with a config such as:
 *
 * `{path: 'parent', outlet: 'nav', children: [...]}`
 *
 * In order to render, there needs to be a component on this config, which will default
 * to this `EmptyOutletComponent`.
 */
var ɵEmptyOutletComponent = /** @class */ (function () {
    function ɵEmptyOutletComponent() {
    }
    ɵEmptyOutletComponent = tslib_1.__decorate([
        Component({ template: "<router-outlet></router-outlet>" })
    ], ɵEmptyOutletComponent);
    return ɵEmptyOutletComponent;
}());
export { ɵEmptyOutletComponent };
export { ɵEmptyOutletComponent as EmptyOutletComponent };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZW1wdHlfb3V0bGV0LmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcm91dGVyL3NyYy9jb21wb25lbnRzL2VtcHR5X291dGxldC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBRUgsT0FBTyxFQUFDLFNBQVMsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUV4Qzs7Ozs7Ozs7R0FRRztBQUVIO0lBQUE7SUFDQSxDQUFDO0lBRFkscUJBQXFCO1FBRGpDLFNBQVMsQ0FBQyxFQUFDLFFBQVEsRUFBRSxpQ0FBaUMsRUFBQyxDQUFDO09BQzVDLHFCQUFxQixDQUNqQztJQUFELDRCQUFDO0NBQUEsQUFERCxJQUNDO1NBRFkscUJBQXFCO0FBR2xDLE9BQU8sRUFBQyxxQkFBcUIsSUFBSSxvQkFBb0IsRUFBQyxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBvbmVudH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbi8qKlxuICogVGhpcyBjb21wb25lbnQgaXMgdXNlZCBpbnRlcm5hbGx5IHdpdGhpbiB0aGUgcm91dGVyIHRvIGJlIGEgcGxhY2Vob2xkZXIgd2hlbiBhbiBlbXB0eVxuICogcm91dGVyLW91dGxldCBpcyBuZWVkZWQuIEZvciBleGFtcGxlLCB3aXRoIGEgY29uZmlnIHN1Y2ggYXM6XG4gKlxuICogYHtwYXRoOiAncGFyZW50Jywgb3V0bGV0OiAnbmF2JywgY2hpbGRyZW46IFsuLi5dfWBcbiAqXG4gKiBJbiBvcmRlciB0byByZW5kZXIsIHRoZXJlIG5lZWRzIHRvIGJlIGEgY29tcG9uZW50IG9uIHRoaXMgY29uZmlnLCB3aGljaCB3aWxsIGRlZmF1bHRcbiAqIHRvIHRoaXMgYEVtcHR5T3V0bGV0Q29tcG9uZW50YC5cbiAqL1xuQENvbXBvbmVudCh7dGVtcGxhdGU6IGA8cm91dGVyLW91dGxldD48L3JvdXRlci1vdXRsZXQ+YH0pXG5leHBvcnQgY2xhc3MgybVFbXB0eU91dGxldENvbXBvbmVudCB7XG59XG5cbmV4cG9ydCB7ybVFbXB0eU91dGxldENvbXBvbmVudCBhcyBFbXB0eU91dGxldENvbXBvbmVudH07XG4iXX0=
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
import { DirectiveResolver } from '@angular/compiler';
/**
 * An implementation of {\@link DirectiveResolver} that allows overriding
 * various properties of directives.
 */
export class MockDirectiveResolver extends DirectiveResolver {
    /**
     * @param {?} reflector
     */
    constructor(reflector) {
        super(reflector);
        this._directives = new Map();
    }
    /**
     * @param {?} type
     * @param {?=} throwIfNotFound
     * @return {?}
     */
    resolve(type, throwIfNotFound = true) {
        return this._directives.get(type) || super.resolve(type, throwIfNotFound);
    }
    /**
     * Overrides the {\@link core.Directive} for a directive.
     * @param {?} type
     * @param {?} metadata
     * @return {?}
     */
    setDirective(type, metadata) {
        this._directives.set(type, metadata);
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    MockDirectiveResolver.prototype._directives;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGlyZWN0aXZlX3Jlc29sdmVyX21vY2suanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci90ZXN0aW5nL3NyYy9kaXJlY3RpdmVfcmVzb2x2ZXJfbW9jay50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQU9BLE9BQU8sRUFBbUIsaUJBQWlCLEVBQU8sTUFBTSxtQkFBbUIsQ0FBQzs7Ozs7QUFNNUUsTUFBTSxPQUFPLHFCQUFzQixTQUFRLGlCQUFpQjs7OztJQUcxRCxZQUFZLFNBQTJCO1FBQUksS0FBSyxDQUFDLFNBQVMsQ0FBQyxDQUFDO1FBRnBELGdCQUFXLEdBQUcsSUFBSSxHQUFHLEVBQTZCLENBQUM7SUFFRSxDQUFDOzs7Ozs7SUFLOUQsT0FBTyxDQUFDLElBQWUsRUFBRSxlQUFlLEdBQUcsSUFBSTtRQUM3QyxPQUFPLElBQUksQ0FBQyxXQUFXLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQyxJQUFJLEtBQUssQ0FBQyxPQUFPLENBQUMsSUFBSSxFQUFFLGVBQWUsQ0FBQyxDQUFDO0lBQzVFLENBQUM7Ozs7Ozs7SUFLRCxZQUFZLENBQUMsSUFBZSxFQUFFLFFBQXdCO1FBQ3BELElBQUksQ0FBQyxXQUFXLENBQUMsR0FBRyxDQUFDLElBQUksRUFBRSxRQUFRLENBQUMsQ0FBQztJQUN2QyxDQUFDO0NBQ0Y7Ozs7OztJQWpCQyw0Q0FBMkQiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5pbXBvcnQge0NvbXBpbGVSZWZsZWN0b3IsIERpcmVjdGl2ZVJlc29sdmVyLCBjb3JlfSBmcm9tICdAYW5ndWxhci9jb21waWxlcic7XG5cbi8qKlxuICogQW4gaW1wbGVtZW50YXRpb24gb2Yge0BsaW5rIERpcmVjdGl2ZVJlc29sdmVyfSB0aGF0IGFsbG93cyBvdmVycmlkaW5nXG4gKiB2YXJpb3VzIHByb3BlcnRpZXMgb2YgZGlyZWN0aXZlcy5cbiAqL1xuZXhwb3J0IGNsYXNzIE1vY2tEaXJlY3RpdmVSZXNvbHZlciBleHRlbmRzIERpcmVjdGl2ZVJlc29sdmVyIHtcbiAgcHJpdmF0ZSBfZGlyZWN0aXZlcyA9IG5ldyBNYXA8Y29yZS5UeXBlLCBjb3JlLkRpcmVjdGl2ZT4oKTtcblxuICBjb25zdHJ1Y3RvcihyZWZsZWN0b3I6IENvbXBpbGVSZWZsZWN0b3IpIHsgc3VwZXIocmVmbGVjdG9yKTsgfVxuXG4gIHJlc29sdmUodHlwZTogY29yZS5UeXBlKTogY29yZS5EaXJlY3RpdmU7XG4gIHJlc29sdmUodHlwZTogY29yZS5UeXBlLCB0aHJvd0lmTm90Rm91bmQ6IHRydWUpOiBjb3JlLkRpcmVjdGl2ZTtcbiAgcmVzb2x2ZSh0eXBlOiBjb3JlLlR5cGUsIHRocm93SWZOb3RGb3VuZDogYm9vbGVhbik6IGNvcmUuRGlyZWN0aXZlfG51bGw7XG4gIHJlc29sdmUodHlwZTogY29yZS5UeXBlLCB0aHJvd0lmTm90Rm91bmQgPSB0cnVlKTogY29yZS5EaXJlY3RpdmV8bnVsbCB7XG4gICAgcmV0dXJuIHRoaXMuX2RpcmVjdGl2ZXMuZ2V0KHR5cGUpIHx8IHN1cGVyLnJlc29sdmUodHlwZSwgdGhyb3dJZk5vdEZvdW5kKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBPdmVycmlkZXMgdGhlIHtAbGluayBjb3JlLkRpcmVjdGl2ZX0gZm9yIGEgZGlyZWN0aXZlLlxuICAgKi9cbiAgc2V0RGlyZWN0aXZlKHR5cGU6IGNvcmUuVHlwZSwgbWV0YWRhdGE6IGNvcmUuRGlyZWN0aXZlKTogdm9pZCB7XG4gICAgdGhpcy5fZGlyZWN0aXZlcy5zZXQodHlwZSwgbWV0YWRhdGEpO1xuICB9XG59XG4iXX0=
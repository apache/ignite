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
import { AbstractControlDirective } from './abstract_control_directive';
/**
 * @return {?}
 */
function unimplemented() {
    throw new Error('unimplemented');
}
/**
 * \@description
 * A base class that all control `FormControl`-based directives extend. It binds a `FormControl`
 * object to a DOM element.
 *
 * \@publicApi
 * @abstract
 */
export class NgControl extends AbstractControlDirective {
    constructor() {
        super(...arguments);
        /**
         * \@description
         * The parent form for the control.
         *
         * \@internal
         */
        this._parent = null;
        /**
         * \@description
         * The name for the control
         */
        this.name = null;
        /**
         * \@description
         * The value accessor for the control
         */
        this.valueAccessor = null;
        /**
         * \@description
         * The uncomposed array of synchronous validators for the control
         *
         * \@internal
         */
        this._rawValidators = [];
        /**
         * \@description
         * The uncomposed array of async validators for the control
         *
         * \@internal
         */
        this._rawAsyncValidators = [];
    }
    /**
     * \@description
     * The registered synchronous validator function for the control
     *
     * @throws An exception that this method is not implemented
     * @return {?}
     */
    get validator() { return (/** @type {?} */ (unimplemented())); }
    /**
     * \@description
     * The registered async validator function for the control
     *
     * @throws An exception that this method is not implemented
     * @return {?}
     */
    get asyncValidator() { return (/** @type {?} */ (unimplemented())); }
}
if (false) {
    /**
     * \@description
     * The parent form for the control.
     *
     * \@internal
     * @type {?}
     */
    NgControl.prototype._parent;
    /**
     * \@description
     * The name for the control
     * @type {?}
     */
    NgControl.prototype.name;
    /**
     * \@description
     * The value accessor for the control
     * @type {?}
     */
    NgControl.prototype.valueAccessor;
    /**
     * \@description
     * The uncomposed array of synchronous validators for the control
     *
     * \@internal
     * @type {?}
     */
    NgControl.prototype._rawValidators;
    /**
     * \@description
     * The uncomposed array of async validators for the control
     *
     * \@internal
     * @type {?}
     */
    NgControl.prototype._rawAsyncValidators;
    /**
     * \@description
     * The callback method to update the model from the view when requested
     *
     * @abstract
     * @param {?} newValue The new value for the view
     * @return {?}
     */
    NgControl.prototype.viewToModelUpdate = function (newValue) { };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfY29udHJvbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2Zvcm1zL3NyYy9kaXJlY3RpdmVzL25nX2NvbnRyb2wudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFTQSxPQUFPLEVBQUMsd0JBQXdCLEVBQUMsTUFBTSw4QkFBOEIsQ0FBQzs7OztBQUt0RSxTQUFTLGFBQWE7SUFDcEIsTUFBTSxJQUFJLEtBQUssQ0FBQyxlQUFlLENBQUMsQ0FBQztBQUNuQyxDQUFDOzs7Ozs7Ozs7QUFTRCxNQUFNLE9BQWdCLFNBQVUsU0FBUSx3QkFBd0I7SUFBaEU7Ozs7Ozs7O1FBT0UsWUFBTyxHQUEwQixJQUFJLENBQUM7Ozs7O1FBTXRDLFNBQUksR0FBZ0IsSUFBSSxDQUFDOzs7OztRQU16QixrQkFBYSxHQUE4QixJQUFJLENBQUM7Ozs7Ozs7UUFRaEQsbUJBQWMsR0FBaUMsRUFBRSxDQUFDOzs7Ozs7O1FBUWxELHdCQUFtQixHQUEyQyxFQUFFLENBQUM7SUF5Qm5FLENBQUM7Ozs7Ozs7O0lBakJDLElBQUksU0FBUyxLQUF1QixPQUFPLG1CQUFhLGFBQWEsRUFBRSxFQUFBLENBQUMsQ0FBQyxDQUFDOzs7Ozs7OztJQVExRSxJQUFJLGNBQWMsS0FBNEIsT0FBTyxtQkFBa0IsYUFBYSxFQUFFLEVBQUEsQ0FBQyxDQUFDLENBQUM7Q0FTMUY7Ozs7Ozs7OztJQXJEQyw0QkFBc0M7Ozs7OztJQU10Qyx5QkFBeUI7Ozs7OztJQU16QixrQ0FBZ0Q7Ozs7Ozs7O0lBUWhELG1DQUFrRDs7Ozs7Ozs7SUFRbEQsd0NBQWlFOzs7Ozs7Ozs7SUF3QmpFLGdFQUFnRCIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuXG5pbXBvcnQge0Fic3RyYWN0Q29udHJvbERpcmVjdGl2ZX0gZnJvbSAnLi9hYnN0cmFjdF9jb250cm9sX2RpcmVjdGl2ZSc7XG5pbXBvcnQge0NvbnRyb2xDb250YWluZXJ9IGZyb20gJy4vY29udHJvbF9jb250YWluZXInO1xuaW1wb3J0IHtDb250cm9sVmFsdWVBY2Nlc3Nvcn0gZnJvbSAnLi9jb250cm9sX3ZhbHVlX2FjY2Vzc29yJztcbmltcG9ydCB7QXN5bmNWYWxpZGF0b3IsIEFzeW5jVmFsaWRhdG9yRm4sIFZhbGlkYXRvciwgVmFsaWRhdG9yRm59IGZyb20gJy4vdmFsaWRhdG9ycyc7XG5cbmZ1bmN0aW9uIHVuaW1wbGVtZW50ZWQoKTogYW55IHtcbiAgdGhyb3cgbmV3IEVycm9yKCd1bmltcGxlbWVudGVkJyk7XG59XG5cbi8qKlxuICogQGRlc2NyaXB0aW9uXG4gKiBBIGJhc2UgY2xhc3MgdGhhdCBhbGwgY29udHJvbCBgRm9ybUNvbnRyb2xgLWJhc2VkIGRpcmVjdGl2ZXMgZXh0ZW5kLiBJdCBiaW5kcyBhIGBGb3JtQ29udHJvbGBcbiAqIG9iamVjdCB0byBhIERPTSBlbGVtZW50LlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIE5nQ29udHJvbCBleHRlbmRzIEFic3RyYWN0Q29udHJvbERpcmVjdGl2ZSB7XG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogVGhlIHBhcmVudCBmb3JtIGZvciB0aGUgY29udHJvbC5cbiAgICpcbiAgICogQGludGVybmFsXG4gICAqL1xuICBfcGFyZW50OiBDb250cm9sQ29udGFpbmVyfG51bGwgPSBudWxsO1xuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogVGhlIG5hbWUgZm9yIHRoZSBjb250cm9sXG4gICAqL1xuICBuYW1lOiBzdHJpbmd8bnVsbCA9IG51bGw7XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBUaGUgdmFsdWUgYWNjZXNzb3IgZm9yIHRoZSBjb250cm9sXG4gICAqL1xuICB2YWx1ZUFjY2Vzc29yOiBDb250cm9sVmFsdWVBY2Nlc3NvcnxudWxsID0gbnVsbDtcblxuICAvKipcbiAgICogQGRlc2NyaXB0aW9uXG4gICAqIFRoZSB1bmNvbXBvc2VkIGFycmF5IG9mIHN5bmNocm9ub3VzIHZhbGlkYXRvcnMgZm9yIHRoZSBjb250cm9sXG4gICAqXG4gICAqIEBpbnRlcm5hbFxuICAgKi9cbiAgX3Jhd1ZhbGlkYXRvcnM6IEFycmF5PFZhbGlkYXRvcnxWYWxpZGF0b3JGbj4gPSBbXTtcblxuICAvKipcbiAgICogQGRlc2NyaXB0aW9uXG4gICAqIFRoZSB1bmNvbXBvc2VkIGFycmF5IG9mIGFzeW5jIHZhbGlkYXRvcnMgZm9yIHRoZSBjb250cm9sXG4gICAqXG4gICAqIEBpbnRlcm5hbFxuICAgKi9cbiAgX3Jhd0FzeW5jVmFsaWRhdG9yczogQXJyYXk8QXN5bmNWYWxpZGF0b3J8QXN5bmNWYWxpZGF0b3JGbj4gPSBbXTtcblxuICAvKipcbiAgICogQGRlc2NyaXB0aW9uXG4gICAqIFRoZSByZWdpc3RlcmVkIHN5bmNocm9ub3VzIHZhbGlkYXRvciBmdW5jdGlvbiBmb3IgdGhlIGNvbnRyb2xcbiAgICpcbiAgICogQHRocm93cyBBbiBleGNlcHRpb24gdGhhdCB0aGlzIG1ldGhvZCBpcyBub3QgaW1wbGVtZW50ZWRcbiAgICovXG4gIGdldCB2YWxpZGF0b3IoKTogVmFsaWRhdG9yRm58bnVsbCB7IHJldHVybiA8VmFsaWRhdG9yRm4+dW5pbXBsZW1lbnRlZCgpOyB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBUaGUgcmVnaXN0ZXJlZCBhc3luYyB2YWxpZGF0b3IgZnVuY3Rpb24gZm9yIHRoZSBjb250cm9sXG4gICAqXG4gICAqIEB0aHJvd3MgQW4gZXhjZXB0aW9uIHRoYXQgdGhpcyBtZXRob2QgaXMgbm90IGltcGxlbWVudGVkXG4gICAqL1xuICBnZXQgYXN5bmNWYWxpZGF0b3IoKTogQXN5bmNWYWxpZGF0b3JGbnxudWxsIHsgcmV0dXJuIDxBc3luY1ZhbGlkYXRvckZuPnVuaW1wbGVtZW50ZWQoKTsgfVxuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogVGhlIGNhbGxiYWNrIG1ldGhvZCB0byB1cGRhdGUgdGhlIG1vZGVsIGZyb20gdGhlIHZpZXcgd2hlbiByZXF1ZXN0ZWRcbiAgICpcbiAgICogQHBhcmFtIG5ld1ZhbHVlIFRoZSBuZXcgdmFsdWUgZm9yIHRoZSB2aWV3XG4gICAqL1xuICBhYnN0cmFjdCB2aWV3VG9Nb2RlbFVwZGF0ZShuZXdWYWx1ZTogYW55KTogdm9pZDtcbn1cbiJdfQ==
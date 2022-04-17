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
import { InjectionToken } from '@angular/core';
/**
 * \@description
 * Defines an interface that acts as a bridge between the Angular forms API and a
 * native element in the DOM.
 *
 * Implement this interface to create a custom form control directive
 * that integrates with Angular forms.
 *
 * @see DefaultValueAccessor
 *
 * \@publicApi
 * @record
 */
export function ControlValueAccessor() { }
if (false) {
    /**
     * \@description
     * Writes a new value to the element.
     *
     * This method is called by the forms API to write to the view when programmatic
     * changes from model to view are requested.
     *
     * \@usageNotes
     * ### Write a value to the element
     *
     * The following example writes a value to the native DOM element.
     *
     * ```ts
     * writeValue(value: any): void {
     *   this._renderer.setProperty(this._elementRef.nativeElement, 'value', value);
     * }
     * ```
     *
     * @param {?} obj The new value for the element
     * @return {?}
     */
    ControlValueAccessor.prototype.writeValue = function (obj) { };
    /**
     * \@description
     * Registers a callback function that is called when the control's value
     * changes in the UI.
     *
     * This method is called by the forms API on initialization to update the form
     * model when values propagate from the view to the model.
     *
     * When implementing the `registerOnChange` method in your own value accessor,
     * save the given function so your class calls it at the appropriate time.
     *
     * \@usageNotes
     * ### Store the change function
     *
     * The following example stores the provided function as an internal method.
     *
     * ```ts
     * registerOnChange(fn: (_: any) => void): void {
     *   this._onChange = fn;
     * }
     * ```
     *
     * When the value changes in the UI, call the registered
     * function to allow the forms API to update itself:
     *
     * ```ts
     * host: {
     *    '(change)': '_onChange($event.target.value)'
     * }
     * ```
     *
     * @param {?} fn The callback function to register
     * @return {?}
     */
    ControlValueAccessor.prototype.registerOnChange = function (fn) { };
    /**
     * \@description
     * Registers a callback function is called by the forms API on initialization
     * to update the form model on blur.
     *
     * When implementing `registerOnTouched` in your own value accessor, save the given
     * function so your class calls it when the control should be considered
     * blurred or "touched".
     *
     * \@usageNotes
     * ### Store the callback function
     *
     * The following example stores the provided function as an internal method.
     *
     * ```ts
     * registerOnTouched(fn: any): void {
     *   this._onTouched = fn;
     * }
     * ```
     *
     * On blur (or equivalent), your class should call the registered function to allow
     * the forms API to update itself:
     *
     * ```ts
     * host: {
     *    '(blur)': '_onTouched()'
     * }
     * ```
     *
     * @param {?} fn The callback function to register
     * @return {?}
     */
    ControlValueAccessor.prototype.registerOnTouched = function (fn) { };
    /**
     * \@description
     * Function that is called by the forms API when the control status changes to
     * or from 'DISABLED'. Depending on the status, it enables or disables the
     * appropriate DOM element.
     *
     * \@usageNotes
     * The following is an example of writing the disabled property to a native DOM element:
     *
     * ```ts
     * setDisabledState(isDisabled: boolean): void {
     *   this._renderer.setProperty(this._elementRef.nativeElement, 'disabled', isDisabled);
     * }
     * ```
     *
     * @param {?} isDisabled The disabled status to set on the element
     * @return {?}
     */
    ControlValueAccessor.prototype.setDisabledState = function (isDisabled) { };
}
/**
 * Used to provide a `ControlValueAccessor` for form controls.
 *
 * See `DefaultValueAccessor` for how to implement one.
 *
 * \@publicApi
 * @type {?}
 */
export const NG_VALUE_ACCESSOR = new InjectionToken('NgValueAccessor');
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29udHJvbF92YWx1ZV9hY2Nlc3Nvci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2Zvcm1zL3NyYy9kaXJlY3RpdmVzL2NvbnRyb2xfdmFsdWVfYWNjZXNzb3IudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsY0FBYyxFQUFDLE1BQU0sZUFBZSxDQUFDOzs7Ozs7Ozs7Ozs7OztBQWM3QywwQ0E2R0M7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0lBeEZDLCtEQUEyQjs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7SUFtQzNCLG9FQUFnQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0lBaUNoQyxxRUFBaUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7SUFtQmpDLDRFQUE2Qzs7Ozs7Ozs7OztBQVUvQyxNQUFNLE9BQU8saUJBQWlCLEdBQUcsSUFBSSxjQUFjLENBQXVCLGlCQUFpQixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0luamVjdGlvblRva2VufSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuLyoqXG4gKiBAZGVzY3JpcHRpb25cbiAqIERlZmluZXMgYW4gaW50ZXJmYWNlIHRoYXQgYWN0cyBhcyBhIGJyaWRnZSBiZXR3ZWVuIHRoZSBBbmd1bGFyIGZvcm1zIEFQSSBhbmQgYVxuICogbmF0aXZlIGVsZW1lbnQgaW4gdGhlIERPTS5cbiAqXG4gKiBJbXBsZW1lbnQgdGhpcyBpbnRlcmZhY2UgdG8gY3JlYXRlIGEgY3VzdG9tIGZvcm0gY29udHJvbCBkaXJlY3RpdmVcbiAqIHRoYXQgaW50ZWdyYXRlcyB3aXRoIEFuZ3VsYXIgZm9ybXMuXG4gKlxuICogQHNlZSBEZWZhdWx0VmFsdWVBY2Nlc3NvclxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBDb250cm9sVmFsdWVBY2Nlc3NvciB7XG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogV3JpdGVzIGEgbmV3IHZhbHVlIHRvIHRoZSBlbGVtZW50LlxuICAgKlxuICAgKiBUaGlzIG1ldGhvZCBpcyBjYWxsZWQgYnkgdGhlIGZvcm1zIEFQSSB0byB3cml0ZSB0byB0aGUgdmlldyB3aGVuIHByb2dyYW1tYXRpY1xuICAgKiBjaGFuZ2VzIGZyb20gbW9kZWwgdG8gdmlldyBhcmUgcmVxdWVzdGVkLlxuICAgKlxuICAgKiBAdXNhZ2VOb3Rlc1xuICAgKiAjIyMgV3JpdGUgYSB2YWx1ZSB0byB0aGUgZWxlbWVudFxuICAgKlxuICAgKiBUaGUgZm9sbG93aW5nIGV4YW1wbGUgd3JpdGVzIGEgdmFsdWUgdG8gdGhlIG5hdGl2ZSBET00gZWxlbWVudC5cbiAgICpcbiAgICogYGBgdHNcbiAgICogd3JpdGVWYWx1ZSh2YWx1ZTogYW55KTogdm9pZCB7XG4gICAqICAgdGhpcy5fcmVuZGVyZXIuc2V0UHJvcGVydHkodGhpcy5fZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCAndmFsdWUnLCB2YWx1ZSk7XG4gICAqIH1cbiAgICogYGBgXG4gICAqXG4gICAqIEBwYXJhbSBvYmogVGhlIG5ldyB2YWx1ZSBmb3IgdGhlIGVsZW1lbnRcbiAgICovXG4gIHdyaXRlVmFsdWUob2JqOiBhbnkpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogUmVnaXN0ZXJzIGEgY2FsbGJhY2sgZnVuY3Rpb24gdGhhdCBpcyBjYWxsZWQgd2hlbiB0aGUgY29udHJvbCdzIHZhbHVlXG4gICAqIGNoYW5nZXMgaW4gdGhlIFVJLlxuICAgKlxuICAgKiBUaGlzIG1ldGhvZCBpcyBjYWxsZWQgYnkgdGhlIGZvcm1zIEFQSSBvbiBpbml0aWFsaXphdGlvbiB0byB1cGRhdGUgdGhlIGZvcm1cbiAgICogbW9kZWwgd2hlbiB2YWx1ZXMgcHJvcGFnYXRlIGZyb20gdGhlIHZpZXcgdG8gdGhlIG1vZGVsLlxuICAgKlxuICAgKiBXaGVuIGltcGxlbWVudGluZyB0aGUgYHJlZ2lzdGVyT25DaGFuZ2VgIG1ldGhvZCBpbiB5b3VyIG93biB2YWx1ZSBhY2Nlc3NvcixcbiAgICogc2F2ZSB0aGUgZ2l2ZW4gZnVuY3Rpb24gc28geW91ciBjbGFzcyBjYWxscyBpdCBhdCB0aGUgYXBwcm9wcmlhdGUgdGltZS5cbiAgICpcbiAgICogQHVzYWdlTm90ZXNcbiAgICogIyMjIFN0b3JlIHRoZSBjaGFuZ2UgZnVuY3Rpb25cbiAgICpcbiAgICogVGhlIGZvbGxvd2luZyBleGFtcGxlIHN0b3JlcyB0aGUgcHJvdmlkZWQgZnVuY3Rpb24gYXMgYW4gaW50ZXJuYWwgbWV0aG9kLlxuICAgKlxuICAgKiBgYGB0c1xuICAgKiByZWdpc3Rlck9uQ2hhbmdlKGZuOiAoXzogYW55KSA9PiB2b2lkKTogdm9pZCB7XG4gICAqICAgdGhpcy5fb25DaGFuZ2UgPSBmbjtcbiAgICogfVxuICAgKiBgYGBcbiAgICpcbiAgICogV2hlbiB0aGUgdmFsdWUgY2hhbmdlcyBpbiB0aGUgVUksIGNhbGwgdGhlIHJlZ2lzdGVyZWRcbiAgICogZnVuY3Rpb24gdG8gYWxsb3cgdGhlIGZvcm1zIEFQSSB0byB1cGRhdGUgaXRzZWxmOlxuICAgKlxuICAgKiBgYGB0c1xuICAgKiBob3N0OiB7XG4gICAqICAgICcoY2hhbmdlKSc6ICdfb25DaGFuZ2UoJGV2ZW50LnRhcmdldC52YWx1ZSknXG4gICAqIH1cbiAgICogYGBgXG4gICAqXG4gICAqIEBwYXJhbSBmbiBUaGUgY2FsbGJhY2sgZnVuY3Rpb24gdG8gcmVnaXN0ZXJcbiAgICovXG4gIHJlZ2lzdGVyT25DaGFuZ2UoZm46IGFueSk6IHZvaWQ7XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBSZWdpc3RlcnMgYSBjYWxsYmFjayBmdW5jdGlvbiBpcyBjYWxsZWQgYnkgdGhlIGZvcm1zIEFQSSBvbiBpbml0aWFsaXphdGlvblxuICAgKiB0byB1cGRhdGUgdGhlIGZvcm0gbW9kZWwgb24gYmx1ci5cbiAgICpcbiAgICogV2hlbiBpbXBsZW1lbnRpbmcgYHJlZ2lzdGVyT25Ub3VjaGVkYCBpbiB5b3VyIG93biB2YWx1ZSBhY2Nlc3Nvciwgc2F2ZSB0aGUgZ2l2ZW5cbiAgICogZnVuY3Rpb24gc28geW91ciBjbGFzcyBjYWxscyBpdCB3aGVuIHRoZSBjb250cm9sIHNob3VsZCBiZSBjb25zaWRlcmVkXG4gICAqIGJsdXJyZWQgb3IgXCJ0b3VjaGVkXCIuXG4gICAqXG4gICAqIEB1c2FnZU5vdGVzXG4gICAqICMjIyBTdG9yZSB0aGUgY2FsbGJhY2sgZnVuY3Rpb25cbiAgICpcbiAgICogVGhlIGZvbGxvd2luZyBleGFtcGxlIHN0b3JlcyB0aGUgcHJvdmlkZWQgZnVuY3Rpb24gYXMgYW4gaW50ZXJuYWwgbWV0aG9kLlxuICAgKlxuICAgKiBgYGB0c1xuICAgKiByZWdpc3Rlck9uVG91Y2hlZChmbjogYW55KTogdm9pZCB7XG4gICAqICAgdGhpcy5fb25Ub3VjaGVkID0gZm47XG4gICAqIH1cbiAgICogYGBgXG4gICAqXG4gICAqIE9uIGJsdXIgKG9yIGVxdWl2YWxlbnQpLCB5b3VyIGNsYXNzIHNob3VsZCBjYWxsIHRoZSByZWdpc3RlcmVkIGZ1bmN0aW9uIHRvIGFsbG93XG4gICAqIHRoZSBmb3JtcyBBUEkgdG8gdXBkYXRlIGl0c2VsZjpcbiAgICpcbiAgICogYGBgdHNcbiAgICogaG9zdDoge1xuICAgKiAgICAnKGJsdXIpJzogJ19vblRvdWNoZWQoKSdcbiAgICogfVxuICAgKiBgYGBcbiAgICpcbiAgICogQHBhcmFtIGZuIFRoZSBjYWxsYmFjayBmdW5jdGlvbiB0byByZWdpc3RlclxuICAgKi9cbiAgcmVnaXN0ZXJPblRvdWNoZWQoZm46IGFueSk6IHZvaWQ7XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBGdW5jdGlvbiB0aGF0IGlzIGNhbGxlZCBieSB0aGUgZm9ybXMgQVBJIHdoZW4gdGhlIGNvbnRyb2wgc3RhdHVzIGNoYW5nZXMgdG9cbiAgICogb3IgZnJvbSAnRElTQUJMRUQnLiBEZXBlbmRpbmcgb24gdGhlIHN0YXR1cywgaXQgZW5hYmxlcyBvciBkaXNhYmxlcyB0aGVcbiAgICogYXBwcm9wcmlhdGUgRE9NIGVsZW1lbnQuXG4gICAqXG4gICAqIEB1c2FnZU5vdGVzXG4gICAqIFRoZSBmb2xsb3dpbmcgaXMgYW4gZXhhbXBsZSBvZiB3cml0aW5nIHRoZSBkaXNhYmxlZCBwcm9wZXJ0eSB0byBhIG5hdGl2ZSBET00gZWxlbWVudDpcbiAgICpcbiAgICogYGBgdHNcbiAgICogc2V0RGlzYWJsZWRTdGF0ZShpc0Rpc2FibGVkOiBib29sZWFuKTogdm9pZCB7XG4gICAqICAgdGhpcy5fcmVuZGVyZXIuc2V0UHJvcGVydHkodGhpcy5fZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCAnZGlzYWJsZWQnLCBpc0Rpc2FibGVkKTtcbiAgICogfVxuICAgKiBgYGBcbiAgICpcbiAgICogQHBhcmFtIGlzRGlzYWJsZWQgVGhlIGRpc2FibGVkIHN0YXR1cyB0byBzZXQgb24gdGhlIGVsZW1lbnRcbiAgICovXG4gIHNldERpc2FibGVkU3RhdGU/KGlzRGlzYWJsZWQ6IGJvb2xlYW4pOiB2b2lkO1xufVxuXG4vKipcbiAqIFVzZWQgdG8gcHJvdmlkZSBhIGBDb250cm9sVmFsdWVBY2Nlc3NvcmAgZm9yIGZvcm0gY29udHJvbHMuXG4gKlxuICogU2VlIGBEZWZhdWx0VmFsdWVBY2Nlc3NvcmAgZm9yIGhvdyB0byBpbXBsZW1lbnQgb25lLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNvbnN0IE5HX1ZBTFVFX0FDQ0VTU09SID0gbmV3IEluamVjdGlvblRva2VuPENvbnRyb2xWYWx1ZUFjY2Vzc29yPignTmdWYWx1ZUFjY2Vzc29yJyk7XG4iXX0=
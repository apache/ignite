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
import { Directive, EventEmitter, Host, Inject, Input, Optional, Output, Self, SkipSelf, forwardRef } from '@angular/core';
import { NG_ASYNC_VALIDATORS, NG_VALIDATORS } from '../../validators';
import { AbstractFormGroupDirective } from '../abstract_form_group_directive';
import { ControlContainer } from '../control_container';
import { NG_VALUE_ACCESSOR } from '../control_value_accessor';
import { NgControl } from '../ng_control';
import { ReactiveErrors } from '../reactive_errors';
import { _ngModelWarning, composeAsyncValidators, composeValidators, controlPath, isPropertyUpdated, selectValueAccessor } from '../shared';
import { NG_MODEL_WITH_FORM_CONTROL_WARNING } from './form_control_directive';
import { FormGroupDirective } from './form_group_directive';
import { FormArrayName, FormGroupName } from './form_group_name';
/** @type {?} */
export const controlNameBinding = {
    provide: NgControl,
    useExisting: forwardRef((/**
     * @return {?}
     */
    () => FormControlName))
};
/**
 * \@description
 * Syncs a `FormControl` in an existing `FormGroup` to a form control
 * element by name.
 *
 * @see [Reactive Forms Guide](guide/reactive-forms)
 * @see `FormControl`
 * @see `AbstractControl`
 *
 * \@usageNotes
 *
 * ### Register `FormControl` within a group
 *
 * The following example shows how to register multiple form controls within a form group
 * and set their value.
 *
 * {\@example forms/ts/simpleFormGroup/simple_form_group_example.ts region='Component'}
 *
 * To see `formControlName` examples with different form control types, see:
 *
 * * Radio buttons: `RadioControlValueAccessor`
 * * Selects: `SelectControlValueAccessor`
 *
 * ### Use with ngModel
 *
 * Support for using the `ngModel` input property and `ngModelChange` event with reactive
 * form directives has been deprecated in Angular v6 and will be removed in Angular v7.
 *
 * Now deprecated:
 *
 * ```html
 * <form [formGroup]="form">
 *   <input formControlName="first" [(ngModel)]="value">
 * </form>
 * ```
 *
 * ```ts
 * this.value = 'some value';
 * ```
 *
 * This has been deprecated for a few reasons. First, developers have found this pattern
 * confusing. It seems like the actual `ngModel` directive is being used, but in fact it's
 * an input/output property named `ngModel` on the reactive form directive that simply
 * approximates (some of) its behavior. Specifically, it allows getting/setting the value
 * and intercepting value events. However, some of `ngModel`'s other features - like
 * delaying updates with`ngModelOptions` or exporting the directive - simply don't work,
 * which has understandably caused some confusion.
 *
 * In addition, this pattern mixes template-driven and reactive forms strategies, which
 * we generally don't recommend because it doesn't take advantage of the full benefits of
 * either strategy. Setting the value in the template violates the template-agnostic
 * principles behind reactive forms, whereas adding a `FormControl`/`FormGroup` layer in
 * the class removes the convenience of defining forms in the template.
 *
 * To update your code before v7, you'll want to decide whether to stick with reactive form
 * directives (and get/set values using reactive forms patterns) or switch over to
 * template-driven directives.
 *
 * After (choice 1 - use reactive forms):
 *
 * ```html
 * <form [formGroup]="form">
 *   <input formControlName="first">
 * </form>
 * ```
 *
 * ```ts
 * this.form.get('first').setValue('some value');
 * ```
 *
 * After (choice 2 - use template-driven forms):
 *
 * ```html
 * <input [(ngModel)]="value">
 * ```
 *
 * ```ts
 * this.value = 'some value';
 * ```
 *
 * By default, when you use this pattern, you will see a deprecation warning once in dev
 * mode. You can choose to silence this warning by providing a config for
 * `ReactiveFormsModule` at import time:
 *
 * ```ts
 * imports: [
 *   ReactiveFormsModule.withConfig({warnOnNgModelWithFormControl: 'never'})
 * ]
 * ```
 *
 * Alternatively, you can choose to surface a separate warning for each instance of this
 * pattern with a config value of `"always"`. This may help to track down where in the code
 * the pattern is being used as the code is being updated.
 *
 * \@ngModule ReactiveFormsModule
 * \@publicApi
 */
export class FormControlName extends NgControl {
    /**
     * @param {?} parent
     * @param {?} validators
     * @param {?} asyncValidators
     * @param {?} valueAccessors
     * @param {?} _ngModelWarningConfig
     */
    constructor(parent, validators, asyncValidators, valueAccessors, _ngModelWarningConfig) {
        super();
        this._ngModelWarningConfig = _ngModelWarningConfig;
        this._added = false;
        /**
         * @deprecated as of v6
         */
        this.update = new EventEmitter();
        /**
         * \@description
         * Instance property used to track whether an ngModel warning has been sent out for this
         * particular FormControlName instance. Used to support warning config of "always".
         *
         * \@internal
         */
        this._ngModelWarningSent = false;
        this._parent = parent;
        this._rawValidators = validators || [];
        this._rawAsyncValidators = asyncValidators || [];
        this.valueAccessor = selectValueAccessor(this, valueAccessors);
    }
    /**
     * \@description
     * Triggers a warning that this input should not be used with reactive forms.
     * @param {?} isDisabled
     * @return {?}
     */
    set isDisabled(isDisabled) { ReactiveErrors.disabledAttrWarning(); }
    /**
     * \@description
     * A lifecycle method called when the directive's inputs change. For internal use only.
     *
     * @param {?} changes A object of key/value pairs for the set of changed inputs.
     * @return {?}
     */
    ngOnChanges(changes) {
        if (!this._added)
            this._setUpControl();
        if (isPropertyUpdated(changes, this.viewModel)) {
            _ngModelWarning('formControlName', FormControlName, this, this._ngModelWarningConfig);
            this.viewModel = this.model;
            this.formDirective.updateModel(this, this.model);
        }
    }
    /**
     * \@description
     * Lifecycle method called before the directive's instance is destroyed. For internal use only.
     * @return {?}
     */
    ngOnDestroy() {
        if (this.formDirective) {
            this.formDirective.removeControl(this);
        }
    }
    /**
     * \@description
     * Sets the new value for the view model and emits an `ngModelChange` event.
     *
     * @param {?} newValue The new value for the view model.
     * @return {?}
     */
    viewToModelUpdate(newValue) {
        this.viewModel = newValue;
        this.update.emit(newValue);
    }
    /**
     * \@description
     * Returns an array that represents the path from the top-level form to this control.
     * Each index is the string name of the control on that level.
     * @return {?}
     */
    get path() { return controlPath(this.name, (/** @type {?} */ (this._parent))); }
    /**
     * \@description
     * The top-level directive for this group if present, otherwise null.
     * @return {?}
     */
    get formDirective() { return this._parent ? this._parent.formDirective : null; }
    /**
     * \@description
     * Synchronous validator function composed of all the synchronous validators
     * registered with this directive.
     * @return {?}
     */
    get validator() { return composeValidators(this._rawValidators); }
    /**
     * \@description
     * Async validator function composed of all the async validators registered with this
     * directive.
     * @return {?}
     */
    get asyncValidator() {
        return (/** @type {?} */ (composeAsyncValidators(this._rawAsyncValidators)));
    }
    /**
     * @private
     * @return {?}
     */
    _checkParentType() {
        if (!(this._parent instanceof FormGroupName) &&
            this._parent instanceof AbstractFormGroupDirective) {
            ReactiveErrors.ngModelGroupException();
        }
        else if (!(this._parent instanceof FormGroupName) && !(this._parent instanceof FormGroupDirective) &&
            !(this._parent instanceof FormArrayName)) {
            ReactiveErrors.controlParentException();
        }
    }
    /**
     * @private
     * @return {?}
     */
    _setUpControl() {
        this._checkParentType();
        ((/** @type {?} */ (this))).control = this.formDirective.addControl(this);
        if (this.control.disabled && (/** @type {?} */ (this.valueAccessor)).setDisabledState) {
            (/** @type {?} */ ((/** @type {?} */ (this.valueAccessor)).setDisabledState))(true);
        }
        this._added = true;
    }
}
/**
 * \@description
 * Static property used to track whether any ngModel warnings have been sent across
 * all instances of FormControlName. Used to support warning config of "once".
 *
 * \@internal
 */
FormControlName._ngModelWarningSentOnce = false;
FormControlName.decorators = [
    { type: Directive, args: [{ selector: '[formControlName]', providers: [controlNameBinding] },] }
];
/** @nocollapse */
FormControlName.ctorParameters = () => [
    { type: ControlContainer, decorators: [{ type: Optional }, { type: Host }, { type: SkipSelf }] },
    { type: Array, decorators: [{ type: Optional }, { type: Self }, { type: Inject, args: [NG_VALIDATORS,] }] },
    { type: Array, decorators: [{ type: Optional }, { type: Self }, { type: Inject, args: [NG_ASYNC_VALIDATORS,] }] },
    { type: Array, decorators: [{ type: Optional }, { type: Self }, { type: Inject, args: [NG_VALUE_ACCESSOR,] }] },
    { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [NG_MODEL_WITH_FORM_CONTROL_WARNING,] }] }
];
FormControlName.propDecorators = {
    name: [{ type: Input, args: ['formControlName',] }],
    isDisabled: [{ type: Input, args: ['disabled',] }],
    model: [{ type: Input, args: ['ngModel',] }],
    update: [{ type: Output, args: ['ngModelChange',] }]
};
if (false) {
    /**
     * \@description
     * Static property used to track whether any ngModel warnings have been sent across
     * all instances of FormControlName. Used to support warning config of "once".
     *
     * \@internal
     * @type {?}
     */
    FormControlName._ngModelWarningSentOnce;
    /**
     * @type {?}
     * @private
     */
    FormControlName.prototype._added;
    /**
     * \@description
     * Internal reference to the view model value.
     * \@internal
     * @type {?}
     */
    FormControlName.prototype.viewModel;
    /**
     * \@description
     * Tracks the `FormControl` instance bound to the directive.
     * @type {?}
     */
    FormControlName.prototype.control;
    /**
     * \@description
     * Tracks the name of the `FormControl` bound to the directive. The name corresponds
     * to a key in the parent `FormGroup` or `FormArray`.
     * @type {?}
     */
    FormControlName.prototype.name;
    /**
     * @deprecated as of v6
     * @type {?}
     */
    FormControlName.prototype.model;
    /**
     * @deprecated as of v6
     * @type {?}
     */
    FormControlName.prototype.update;
    /**
     * \@description
     * Instance property used to track whether an ngModel warning has been sent out for this
     * particular FormControlName instance. Used to support warning config of "always".
     *
     * \@internal
     * @type {?}
     */
    FormControlName.prototype._ngModelWarningSent;
    /**
     * @type {?}
     * @private
     */
    FormControlName.prototype._ngModelWarningConfig;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZm9ybV9jb250cm9sX25hbWUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9mb3Jtcy9zcmMvZGlyZWN0aXZlcy9yZWFjdGl2ZV9kaXJlY3RpdmVzL2Zvcm1fY29udHJvbF9uYW1lLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLFNBQVMsRUFBRSxZQUFZLEVBQUUsSUFBSSxFQUFFLE1BQU0sRUFBRSxLQUFLLEVBQXdCLFFBQVEsRUFBRSxNQUFNLEVBQUUsSUFBSSxFQUFpQixRQUFRLEVBQUUsVUFBVSxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBRzlKLE9BQU8sRUFBQyxtQkFBbUIsRUFBRSxhQUFhLEVBQUMsTUFBTSxrQkFBa0IsQ0FBQztBQUNwRSxPQUFPLEVBQUMsMEJBQTBCLEVBQUMsTUFBTSxrQ0FBa0MsQ0FBQztBQUM1RSxPQUFPLEVBQUMsZ0JBQWdCLEVBQUMsTUFBTSxzQkFBc0IsQ0FBQztBQUN0RCxPQUFPLEVBQXVCLGlCQUFpQixFQUFDLE1BQU0sMkJBQTJCLENBQUM7QUFDbEYsT0FBTyxFQUFDLFNBQVMsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUN4QyxPQUFPLEVBQUMsY0FBYyxFQUFDLE1BQU0sb0JBQW9CLENBQUM7QUFDbEQsT0FBTyxFQUFDLGVBQWUsRUFBRSxzQkFBc0IsRUFBRSxpQkFBaUIsRUFBRSxXQUFXLEVBQUUsaUJBQWlCLEVBQUUsbUJBQW1CLEVBQUMsTUFBTSxXQUFXLENBQUM7QUFHMUksT0FBTyxFQUFDLGtDQUFrQyxFQUFDLE1BQU0sMEJBQTBCLENBQUM7QUFDNUUsT0FBTyxFQUFDLGtCQUFrQixFQUFDLE1BQU0sd0JBQXdCLENBQUM7QUFDMUQsT0FBTyxFQUFDLGFBQWEsRUFBRSxhQUFhLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQzs7QUFFL0QsTUFBTSxPQUFPLGtCQUFrQixHQUFRO0lBQ3JDLE9BQU8sRUFBRSxTQUFT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QzVDLFFBQVEsWUFBSSxNQUFNLFNBQUMsa0NBQWtDOzs7bUJBekN6RCxLQUFLLFNBQUMsaUJBQWlCO3lCQU12QixLQUFLLFNBQUMsVUFBVTtvQkFNaEIsS0FBSyxTQUFDLFNBQVM7cUJBR2YsTUFBTSxTQUFDLGVBQWU7Ozs7Ozs7Ozs7O0lBU3ZCLHdDQUF1Qzs7Ozs7SUE3Q3ZDLGlDQUF1Qjs7Ozs7OztJQU12QixvQ0FBZTs7Ozs7O0lBT2Ysa0NBQWdDOzs7Ozs7O0lBUWhDLCtCQUF5Qzs7Ozs7SUFZekMsZ0NBQTZCOzs7OztJQUc3QixpQ0FBcUQ7Ozs7Ozs7OztJQWtCckQsOENBQTRCOzs7OztJQVF4QixnREFDSSIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtEaXJlY3RpdmUsIEV2ZW50RW1pdHRlciwgSG9zdCwgSW5qZWN0LCBJbnB1dCwgT25DaGFuZ2VzLCBPbkRlc3Ryb3ksIE9wdGlvbmFsLCBPdXRwdXQsIFNlbGYsIFNpbXBsZUNoYW5nZXMsIFNraXBTZWxmLCBmb3J3YXJkUmVmfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHtGb3JtQ29udHJvbH0gZnJvbSAnLi4vLi4vbW9kZWwnO1xuaW1wb3J0IHtOR19BU1lOQ19WQUxJREFUT1JTLCBOR19WQUxJREFUT1JTfSBmcm9tICcuLi8uLi92YWxpZGF0b3JzJztcbmltcG9ydCB7QWJzdHJhY3RGb3JtR3JvdXBEaXJlY3RpdmV9IGZyb20gJy4uL2Fic3RyYWN0X2Zvcm1fZ3JvdXBfZGlyZWN0aXZlJztcbmltcG9ydCB7Q29udHJvbENvbnRhaW5lcn0gZnJvbSAnLi4vY29udHJvbF9jb250YWluZXInO1xuaW1wb3J0IHtDb250cm9sVmFsdWVBY2Nlc3NvciwgTkdfVkFMVUVfQUNDRVNTT1J9IGZyb20gJy4uL2NvbnRyb2xfdmFsdWVfYWNjZXNzb3InO1xuaW1wb3J0IHtOZ0NvbnRyb2x9IGZyb20gJy4uL25nX2NvbnRyb2wnO1xuaW1wb3J0IHtSZWFjdGl2ZUVycm9yc30gZnJvbSAnLi4vcmVhY3RpdmVfZXJyb3JzJztcbmltcG9ydCB7X25nTW9kZWxXYXJuaW5nLCBjb21wb3NlQXN5bmNWYWxpZGF0b3JzLCBjb21wb3NlVmFsaWRhdG9ycywgY29udHJvbFBhdGgsIGlzUHJvcGVydHlVcGRhdGVkLCBzZWxlY3RWYWx1ZUFjY2Vzc29yfSBmcm9tICcuLi9zaGFyZWQnO1xuaW1wb3J0IHtBc3luY1ZhbGlkYXRvciwgQXN5bmNWYWxpZGF0b3JGbiwgVmFsaWRhdG9yLCBWYWxpZGF0b3JGbn0gZnJvbSAnLi4vdmFsaWRhdG9ycyc7XG5cbmltcG9ydCB7TkdfTU9ERUxfV0lUSF9GT1JNX0NPTlRST0xfV0FSTklOR30gZnJvbSAnLi9mb3JtX2NvbnRyb2xfZGlyZWN0aXZlJztcbmltcG9ydCB7Rm9ybUdyb3VwRGlyZWN0aXZlfSBmcm9tICcuL2Zvcm1fZ3JvdXBfZGlyZWN0aXZlJztcbmltcG9ydCB7Rm9ybUFycmF5TmFtZSwgRm9ybUdyb3VwTmFtZX0gZnJvbSAnLi9mb3JtX2dyb3VwX25hbWUnO1xuXG5leHBvcnQgY29uc3QgY29udHJvbE5hbWVCaW5kaW5nOiBhbnkgPSB7XG4gIHByb3ZpZGU6IE5nQ29udHJvbCxcbiAgdXNlRXhpc3Rpbmc6IGZvcndhcmRSZWYoKCkgPT4gRm9ybUNvbnRyb2xOYW1lKVxufTtcblxuLyoqXG4gKiBAZGVzY3JpcHRpb25cbiAqIFN5bmNzIGEgYEZvcm1Db250cm9sYCBpbiBhbiBleGlzdGluZyBgRm9ybUdyb3VwYCB0byBhIGZvcm0gY29udHJvbFxuICogZWxlbWVudCBieSBuYW1lLlxuICogXG4gKiBAc2VlIFtSZWFjdGl2ZSBGb3JtcyBHdWlkZV0oZ3VpZGUvcmVhY3RpdmUtZm9ybXMpXG4gKiBAc2VlIGBGb3JtQ29udHJvbGBcbiAqIEBzZWUgYEFic3RyYWN0Q29udHJvbGBcbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICogXG4gKiAjIyMgUmVnaXN0ZXIgYEZvcm1Db250cm9sYCB3aXRoaW4gYSBncm91cFxuICpcbiAqIFRoZSBmb2xsb3dpbmcgZXhhbXBsZSBzaG93cyBob3cgdG8gcmVnaXN0ZXIgbXVsdGlwbGUgZm9ybSBjb250cm9scyB3aXRoaW4gYSBmb3JtIGdyb3VwXG4gKiBhbmQgc2V0IHRoZWlyIHZhbHVlLlxuICpcbiAqIHtAZXhhbXBsZSBmb3Jtcy90cy9zaW1wbGVGb3JtR3JvdXAvc2ltcGxlX2Zvcm1fZ3JvdXBfZXhhbXBsZS50cyByZWdpb249J0NvbXBvbmVudCd9XG4gKlxuICogVG8gc2VlIGBmb3JtQ29udHJvbE5hbWVgIGV4YW1wbGVzIHdpdGggZGlmZmVyZW50IGZvcm0gY29udHJvbCB0eXBlcywgc2VlOlxuICpcbiAqICogUmFkaW8gYnV0dG9uczogYFJhZGlvQ29udHJvbFZhbHVlQWNjZXNzb3JgXG4gKiAqIFNlbGVjdHM6IGBTZWxlY3RDb250cm9sVmFsdWVBY2Nlc3NvcmBcbiAqXG4gKiAjIyMgVXNlIHdpdGggbmdNb2RlbFxuICpcbiAqIFN1cHBvcnQgZm9yIHVzaW5nIHRoZSBgbmdNb2RlbGAgaW5wdXQgcHJvcGVydHkgYW5kIGBuZ01vZGVsQ2hhbmdlYCBldmVudCB3aXRoIHJlYWN0aXZlXG4gKiBmb3JtIGRpcmVjdGl2ZXMgaGFzIGJlZW4gZGVwcmVjYXRlZCBpbiBBbmd1bGFyIHY2IGFuZCB3aWxsIGJlIHJlbW92ZWQgaW4gQW5ndWxhciB2Ny5cbiAqXG4gKiBOb3cgZGVwcmVjYXRlZDpcbiAqXG4gKiBgYGBodG1sXG4gKiA8Zm9ybSBbZm9ybUdyb3VwXT1cImZvcm1cIj5cbiAqICAgPGlucHV0IGZvcm1Db250cm9sTmFtZT1cImZpcnN0XCIgWyhuZ01vZGVsKV09XCJ2YWx1ZVwiPlxuICogPC9mb3JtPlxuICogYGBgXG4gKlxuICogYGBgdHNcbiAqIHRoaXMudmFsdWUgPSAnc29tZSB2YWx1ZSc7XG4gKiBgYGBcbiAqXG4gKiBUaGlzIGhhcyBiZWVuIGRlcHJlY2F0ZWQgZm9yIGEgZmV3IHJlYXNvbnMuIEZpcnN0LCBkZXZlbG9wZXJzIGhhdmUgZm91bmQgdGhpcyBwYXR0ZXJuXG4gKiBjb25mdXNpbmcuIEl0IHNlZW1zIGxpa2UgdGhlIGFjdHVhbCBgbmdNb2RlbGAgZGlyZWN0aXZlIGlzIGJlaW5nIHVzZWQsIGJ1dCBpbiBmYWN0IGl0J3NcbiAqIGFuIGlucHV0L291dHB1dCBwcm9wZXJ0eSBuYW1lZCBgbmdNb2RlbGAgb24gdGhlIHJlYWN0aXZlIGZvcm0gZGlyZWN0aXZlIHRoYXQgc2ltcGx5XG4gKiBhcHByb3hpbWF0ZXMgKHNvbWUgb2YpIGl0cyBiZWhhdmlvci4gU3BlY2lmaWNhbGx5LCBpdCBhbGxvd3MgZ2V0dGluZy9zZXR0aW5nIHRoZSB2YWx1ZVxuICogYW5kIGludGVyY2VwdGluZyB2YWx1ZSBldmVudHMuIEhvd2V2ZXIsIHNvbWUgb2YgYG5nTW9kZWxgJ3Mgb3RoZXIgZmVhdHVyZXMgLSBsaWtlXG4gKiBkZWxheWluZyB1cGRhdGVzIHdpdGhgbmdNb2RlbE9wdGlvbnNgIG9yIGV4cG9ydGluZyB0aGUgZGlyZWN0aXZlIC0gc2ltcGx5IGRvbid0IHdvcmssXG4gKiB3aGljaCBoYXMgdW5kZXJzdGFuZGFibHkgY2F1c2VkIHNvbWUgY29uZnVzaW9uLlxuICpcbiAqIEluIGFkZGl0aW9uLCB0aGlzIHBhdHRlcm4gbWl4ZXMgdGVtcGxhdGUtZHJpdmVuIGFuZCByZWFjdGl2ZSBmb3JtcyBzdHJhdGVnaWVzLCB3aGljaFxuICogd2UgZ2VuZXJhbGx5IGRvbid0IHJlY29tbWVuZCBiZWNhdXNlIGl0IGRvZXNuJ3QgdGFrZSBhZHZhbnRhZ2Ugb2YgdGhlIGZ1bGwgYmVuZWZpdHMgb2ZcbiAqIGVpdGhlciBzdHJhdGVneS4gU2V0dGluZyB0aGUgdmFsdWUgaW4gdGhlIHRlbXBsYXRlIHZpb2xhdGVzIHRoZSB0ZW1wbGF0ZS1hZ25vc3RpY1xuICogcHJpbmNpcGxlcyBiZWhpbmQgcmVhY3RpdmUgZm9ybXMsIHdoZXJlYXMgYWRkaW5nIGEgYEZvcm1Db250cm9sYC9gRm9ybUdyb3VwYCBsYXllciBpblxuICogdGhlIGNsYXNzIHJlbW92ZXMgdGhlIGNvbnZlbmllbmNlIG9mIGRlZmluaW5nIGZvcm1zIGluIHRoZSB0ZW1wbGF0ZS5cbiAqXG4gKiBUbyB1cGRhdGUgeW91ciBjb2RlIGJlZm9yZSB2NywgeW91J2xsIHdhbnQgdG8gZGVjaWRlIHdoZXRoZXIgdG8gc3RpY2sgd2l0aCByZWFjdGl2ZSBmb3JtXG4gKiBkaXJlY3RpdmVzIChhbmQgZ2V0L3NldCB2YWx1ZXMgdXNpbmcgcmVhY3RpdmUgZm9ybXMgcGF0dGVybnMpIG9yIHN3aXRjaCBvdmVyIHRvXG4gKiB0ZW1wbGF0ZS1kcml2ZW4gZGlyZWN0aXZlcy5cbiAqXG4gKiBBZnRlciAoY2hvaWNlIDEgLSB1c2UgcmVhY3RpdmUgZm9ybXMpOlxuICpcbiAqIGBgYGh0bWxcbiAqIDxmb3JtIFtmb3JtR3JvdXBdPVwiZm9ybVwiPlxuICogICA8aW5wdXQgZm9ybUNvbnRyb2xOYW1lPVwiZmlyc3RcIj5cbiAqIDwvZm9ybT5cbiAqIGBgYFxuICpcbiAqIGBgYHRzXG4gKiB0aGlzLmZvcm0uZ2V0KCdmaXJzdCcpLnNldFZhbHVlKCdzb21lIHZhbHVlJyk7XG4gKiBgYGBcbiAqXG4gKiBBZnRlciAoY2hvaWNlIDIgLSB1c2UgdGVtcGxhdGUtZHJpdmVuIGZvcm1zKTpcbiAqXG4gKiBgYGBodG1sXG4gKiA8aW5wdXQgWyhuZ01vZGVsKV09XCJ2YWx1ZVwiPlxuICogYGBgXG4gKlxuICogYGBgdHNcbiAqIHRoaXMudmFsdWUgPSAnc29tZSB2YWx1ZSc7XG4gKiBgYGBcbiAqXG4gKiBCeSBkZWZhdWx0LCB3aGVuIHlvdSB1c2UgdGhpcyBwYXR0ZXJuLCB5b3Ugd2lsbCBzZWUgYSBkZXByZWNhdGlvbiB3YXJuaW5nIG9uY2UgaW4gZGV2XG4gKiBtb2RlLiBZb3UgY2FuIGNob29zZSB0byBzaWxlbmNlIHRoaXMgd2FybmluZyBieSBwcm92aWRpbmcgYSBjb25maWcgZm9yXG4gKiBgUmVhY3RpdmVGb3Jtc01vZHVsZWAgYXQgaW1wb3J0IHRpbWU6XG4gKlxuICogYGBgdHNcbiAqIGltcG9ydHM6IFtcbiAqICAgUmVhY3RpdmVGb3Jtc01vZHVsZS53aXRoQ29uZmlnKHt3YXJuT25OZ01vZGVsV2l0aEZvcm1Db250cm9sOiAnbmV2ZXInfSlcbiAqIF1cbiAqIGBgYFxuICpcbiAqIEFsdGVybmF0aXZlbHksIHlvdSBjYW4gY2hvb3NlIHRvIHN1cmZhY2UgYSBzZXBhcmF0ZSB3YXJuaW5nIGZvciBlYWNoIGluc3RhbmNlIG9mIHRoaXNcbiAqIHBhdHRlcm4gd2l0aCBhIGNvbmZpZyB2YWx1ZSBvZiBgXCJhbHdheXNcImAuIFRoaXMgbWF5IGhlbHAgdG8gdHJhY2sgZG93biB3aGVyZSBpbiB0aGUgY29kZVxuICogdGhlIHBhdHRlcm4gaXMgYmVpbmcgdXNlZCBhcyB0aGUgY29kZSBpcyBiZWluZyB1cGRhdGVkLlxuICpcbiAqIEBuZ01vZHVsZSBSZWFjdGl2ZUZvcm1zTW9kdWxlXG4gKiBAcHVibGljQXBpXG4gKi9cbkBEaXJlY3RpdmUoe3NlbGVjdG9yOiAnW2Zvcm1Db250cm9sTmFtZV0nLCBwcm92aWRlcnM6IFtjb250cm9sTmFtZUJpbmRpbmddfSlcbmV4cG9ydCBjbGFzcyBGb3JtQ29udHJvbE5hbWUgZXh0ZW5kcyBOZ0NvbnRyb2wgaW1wbGVtZW50cyBPbkNoYW5nZXMsIE9uRGVzdHJveSB7XG4gIHByaXZhdGUgX2FkZGVkID0gZmFsc2U7XG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogSW50ZXJuYWwgcmVmZXJlbmNlIHRvIHRoZSB2aWV3IG1vZGVsIHZhbHVlLlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIHZpZXdNb2RlbDogYW55O1xuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogVHJhY2tzIHRoZSBgRm9ybUNvbnRyb2xgIGluc3RhbmNlIGJvdW5kIHRvIHRoZSBkaXJlY3RpdmUuXG4gICAqL1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcmVhZG9ubHkgY29udHJvbCAhOiBGb3JtQ29udHJvbDtcblxuICAvKipcbiAgICogQGRlc2NyaXB0aW9uXG4gICAqIFRyYWNrcyB0aGUgbmFtZSBvZiB0aGUgYEZvcm1Db250cm9sYCBib3VuZCB0byB0aGUgZGlyZWN0aXZlLiBUaGUgbmFtZSBjb3JyZXNwb25kc1xuICAgKiB0byBhIGtleSBpbiB0aGUgcGFyZW50IGBGb3JtR3JvdXBgIG9yIGBGb3JtQXJyYXlgLlxuICAgKi9cbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIEBJbnB1dCgnZm9ybUNvbnRyb2xOYW1lJykgbmFtZSAhOiBzdHJpbmc7XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBUcmlnZ2VycyBhIHdhcm5pbmcgdGhhdCB0aGlzIGlucHV0IHNob3VsZCBub3QgYmUgdXNlZCB3aXRoIHJlYWN0aXZlIGZvcm1zLlxuICAgKi9cbiAgQElucHV0KCdkaXNhYmxlZCcpXG4gIHNldCBpc0Rpc2FibGVkKGlzRGlzYWJsZWQ6IGJvb2xlYW4pIHsgUmVhY3RpdmVFcnJvcnMuZGlzYWJsZWRBdHRyV2FybmluZygpOyB9XG5cbiAgLy8gVE9ETyhrYXJhKTogcmVtb3ZlIG5leHQgNCBwcm9wZXJ0aWVzIG9uY2UgZGVwcmVjYXRpb24gcGVyaW9kIGlzIG92ZXJcblxuICAvKiogQGRlcHJlY2F0ZWQgYXMgb2YgdjYgKi9cbiAgQElucHV0KCduZ01vZGVsJykgbW9kZWw6IGFueTtcblxuICAvKiogQGRlcHJlY2F0ZWQgYXMgb2YgdjYgKi9cbiAgQE91dHB1dCgnbmdNb2RlbENoYW5nZScpIHVwZGF0ZSA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuICAvKipcbiAgICogQGRlc2NyaXB0aW9uXG4gICAqIFN0YXRpYyBwcm9wZXJ0eSB1c2VkIHRvIHRyYWNrIHdoZXRoZXIgYW55IG5nTW9kZWwgd2FybmluZ3MgaGF2ZSBiZWVuIHNlbnQgYWNyb3NzXG4gICAqIGFsbCBpbnN0YW5jZXMgb2YgRm9ybUNvbnRyb2xOYW1lLiBVc2VkIHRvIHN1cHBvcnQgd2FybmluZyBjb25maWcgb2YgXCJvbmNlXCIuXG4gICAqXG4gICAqIEBpbnRlcm5hbFxuICAgKi9cbiAgc3RhdGljIF9uZ01vZGVsV2FybmluZ1NlbnRPbmNlID0gZmFsc2U7XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBJbnN0YW5jZSBwcm9wZXJ0eSB1c2VkIHRvIHRyYWNrIHdoZXRoZXIgYW4gbmdNb2RlbCB3YXJuaW5nIGhhcyBiZWVuIHNlbnQgb3V0IGZvciB0aGlzXG4gICAqIHBhcnRpY3VsYXIgRm9ybUNvbnRyb2xOYW1lIGluc3RhbmNlLiBVc2VkIHRvIHN1cHBvcnQgd2FybmluZyBjb25maWcgb2YgXCJhbHdheXNcIi5cbiAgICpcbiAgICogQGludGVybmFsXG4gICAqL1xuICBfbmdNb2RlbFdhcm5pbmdTZW50ID0gZmFsc2U7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBAT3B0aW9uYWwoKSBASG9zdCgpIEBTa2lwU2VsZigpIHBhcmVudDogQ29udHJvbENvbnRhaW5lcixcbiAgICAgIEBPcHRpb25hbCgpIEBTZWxmKCkgQEluamVjdChOR19WQUxJREFUT1JTKSB2YWxpZGF0b3JzOiBBcnJheTxWYWxpZGF0b3J8VmFsaWRhdG9yRm4+LFxuICAgICAgQE9wdGlvbmFsKCkgQFNlbGYoKSBASW5qZWN0KE5HX0FTWU5DX1ZBTElEQVRPUlMpIGFzeW5jVmFsaWRhdG9yczpcbiAgICAgICAgICBBcnJheTxBc3luY1ZhbGlkYXRvcnxBc3luY1ZhbGlkYXRvckZuPixcbiAgICAgIEBPcHRpb25hbCgpIEBTZWxmKCkgQEluamVjdChOR19WQUxVRV9BQ0NFU1NPUikgdmFsdWVBY2Nlc3NvcnM6IENvbnRyb2xWYWx1ZUFjY2Vzc29yW10sXG4gICAgICBAT3B0aW9uYWwoKSBASW5qZWN0KE5HX01PREVMX1dJVEhfRk9STV9DT05UUk9MX1dBUk5JTkcpIHByaXZhdGUgX25nTW9kZWxXYXJuaW5nQ29uZmlnOiBzdHJpbmd8XG4gICAgICBudWxsKSB7XG4gICAgc3VwZXIoKTtcbiAgICB0aGlzLl9wYXJlbnQgPSBwYXJlbnQ7XG4gICAgdGhpcy5fcmF3VmFsaWRhdG9ycyA9IHZhbGlkYXRvcnMgfHwgW107XG4gICAgdGhpcy5fcmF3QXN5bmNWYWxpZGF0b3JzID0gYXN5bmNWYWxpZGF0b3JzIHx8IFtdO1xuICAgIHRoaXMudmFsdWVBY2Nlc3NvciA9IHNlbGVjdFZhbHVlQWNjZXNzb3IodGhpcywgdmFsdWVBY2Nlc3NvcnMpO1xuICB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBBIGxpZmVjeWNsZSBtZXRob2QgY2FsbGVkIHdoZW4gdGhlIGRpcmVjdGl2ZSdzIGlucHV0cyBjaGFuZ2UuIEZvciBpbnRlcm5hbCB1c2Ugb25seS5cbiAgICpcbiAgICogQHBhcmFtIGNoYW5nZXMgQSBvYmplY3Qgb2Yga2V5L3ZhbHVlIHBhaXJzIGZvciB0aGUgc2V0IG9mIGNoYW5nZWQgaW5wdXRzLlxuICAgKi9cbiAgbmdPbkNoYW5nZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcykge1xuICAgIGlmICghdGhpcy5fYWRkZWQpIHRoaXMuX3NldFVwQ29udHJvbCgpO1xuICAgIGlmIChpc1Byb3BlcnR5VXBkYXRlZChjaGFuZ2VzLCB0aGlzLnZpZXdNb2RlbCkpIHtcbiAgICAgIF9uZ01vZGVsV2FybmluZygnZm9ybUNvbnRyb2xOYW1lJywgRm9ybUNvbnRyb2xOYW1lLCB0aGlzLCB0aGlzLl9uZ01vZGVsV2FybmluZ0NvbmZpZyk7XG4gICAgICB0aGlzLnZpZXdNb2RlbCA9IHRoaXMubW9kZWw7XG4gICAgICB0aGlzLmZvcm1EaXJlY3RpdmUudXBkYXRlTW9kZWwodGhpcywgdGhpcy5tb2RlbCk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBMaWZlY3ljbGUgbWV0aG9kIGNhbGxlZCBiZWZvcmUgdGhlIGRpcmVjdGl2ZSdzIGluc3RhbmNlIGlzIGRlc3Ryb3llZC4gRm9yIGludGVybmFsIHVzZSBvbmx5LlxuICAgKi9cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuZm9ybURpcmVjdGl2ZSkge1xuICAgICAgdGhpcy5mb3JtRGlyZWN0aXZlLnJlbW92ZUNvbnRyb2wodGhpcyk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBTZXRzIHRoZSBuZXcgdmFsdWUgZm9yIHRoZSB2aWV3IG1vZGVsIGFuZCBlbWl0cyBhbiBgbmdNb2RlbENoYW5nZWAgZXZlbnQuXG4gICAqXG4gICAqIEBwYXJhbSBuZXdWYWx1ZSBUaGUgbmV3IHZhbHVlIGZvciB0aGUgdmlldyBtb2RlbC5cbiAgICovXG4gIHZpZXdUb01vZGVsVXBkYXRlKG5ld1ZhbHVlOiBhbnkpOiB2b2lkIHtcbiAgICB0aGlzLnZpZXdNb2RlbCA9IG5ld1ZhbHVlO1xuICAgIHRoaXMudXBkYXRlLmVtaXQobmV3VmFsdWUpO1xuICB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBSZXR1cm5zIGFuIGFycmF5IHRoYXQgcmVwcmVzZW50cyB0aGUgcGF0aCBmcm9tIHRoZSB0b3AtbGV2ZWwgZm9ybSB0byB0aGlzIGNvbnRyb2wuXG4gICAqIEVhY2ggaW5kZXggaXMgdGhlIHN0cmluZyBuYW1lIG9mIHRoZSBjb250cm9sIG9uIHRoYXQgbGV2ZWwuXG4gICAqL1xuICBnZXQgcGF0aCgpOiBzdHJpbmdbXSB7IHJldHVybiBjb250cm9sUGF0aCh0aGlzLm5hbWUsIHRoaXMuX3BhcmVudCAhKTsgfVxuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogVGhlIHRvcC1sZXZlbCBkaXJlY3RpdmUgZm9yIHRoaXMgZ3JvdXAgaWYgcHJlc2VudCwgb3RoZXJ3aXNlIG51bGwuXG4gICAqL1xuICBnZXQgZm9ybURpcmVjdGl2ZSgpOiBhbnkgeyByZXR1cm4gdGhpcy5fcGFyZW50ID8gdGhpcy5fcGFyZW50LmZvcm1EaXJlY3RpdmUgOiBudWxsOyB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBTeW5jaHJvbm91cyB2YWxpZGF0b3IgZnVuY3Rpb24gY29tcG9zZWQgb2YgYWxsIHRoZSBzeW5jaHJvbm91cyB2YWxpZGF0b3JzXG4gICAqIHJlZ2lzdGVyZWQgd2l0aCB0aGlzIGRpcmVjdGl2ZS5cbiAgICovXG4gIGdldCB2YWxpZGF0b3IoKTogVmFsaWRhdG9yRm58bnVsbCB7IHJldHVybiBjb21wb3NlVmFsaWRhdG9ycyh0aGlzLl9yYXdWYWxpZGF0b3JzKTsgfVxuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogQXN5bmMgdmFsaWRhdG9yIGZ1bmN0aW9uIGNvbXBvc2VkIG9mIGFsbCB0aGUgYXN5bmMgdmFsaWRhdG9ycyByZWdpc3RlcmVkIHdpdGggdGhpc1xuICAgKiBkaXJlY3RpdmUuXG4gICAqL1xuICBnZXQgYXN5bmNWYWxpZGF0b3IoKTogQXN5bmNWYWxpZGF0b3JGbiB7XG4gICAgcmV0dXJuIGNvbXBvc2VBc3luY1ZhbGlkYXRvcnModGhpcy5fcmF3QXN5bmNWYWxpZGF0b3JzKSAhO1xuICB9XG5cbiAgcHJpdmF0ZSBfY2hlY2tQYXJlbnRUeXBlKCk6IHZvaWQge1xuICAgIGlmICghKHRoaXMuX3BhcmVudCBpbnN0YW5jZW9mIEZvcm1Hcm91cE5hbWUpICYmXG4gICAgICAgIHRoaXMuX3BhcmVudCBpbnN0YW5jZW9mIEFic3RyYWN0Rm9ybUdyb3VwRGlyZWN0aXZlKSB7XG4gICAgICBSZWFjdGl2ZUVycm9ycy5uZ01vZGVsR3JvdXBFeGNlcHRpb24oKTtcbiAgICB9IGVsc2UgaWYgKFxuICAgICAgICAhKHRoaXMuX3BhcmVudCBpbnN0YW5jZW9mIEZvcm1Hcm91cE5hbWUpICYmICEodGhpcy5fcGFyZW50IGluc3RhbmNlb2YgRm9ybUdyb3VwRGlyZWN0aXZlKSAmJlxuICAgICAgICAhKHRoaXMuX3BhcmVudCBpbnN0YW5jZW9mIEZvcm1BcnJheU5hbWUpKSB7XG4gICAgICBSZWFjdGl2ZUVycm9ycy5jb250cm9sUGFyZW50RXhjZXB0aW9uKCk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBfc2V0VXBDb250cm9sKCkge1xuICAgIHRoaXMuX2NoZWNrUGFyZW50VHlwZSgpO1xuICAgICh0aGlzIGFze2NvbnRyb2w6IEZvcm1Db250cm9sfSkuY29udHJvbCA9IHRoaXMuZm9ybURpcmVjdGl2ZS5hZGRDb250cm9sKHRoaXMpO1xuICAgIGlmICh0aGlzLmNvbnRyb2wuZGlzYWJsZWQgJiYgdGhpcy52YWx1ZUFjY2Vzc29yICEuc2V0RGlzYWJsZWRTdGF0ZSkge1xuICAgICAgdGhpcy52YWx1ZUFjY2Vzc29yICEuc2V0RGlzYWJsZWRTdGF0ZSAhKHRydWUpO1xuICAgIH1cbiAgICB0aGlzLl9hZGRlZCA9IHRydWU7XG4gIH1cbn1cbiJdfQ==
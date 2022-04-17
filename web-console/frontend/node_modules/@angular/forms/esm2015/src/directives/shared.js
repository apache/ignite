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
import { isDevMode, ɵlooseIdentical as looseIdentical } from '@angular/core';
import { Validators } from '../validators';
import { CheckboxControlValueAccessor } from './checkbox_value_accessor';
import { DefaultValueAccessor } from './default_value_accessor';
import { normalizeAsyncValidator, normalizeValidator } from './normalize_validator';
import { NumberValueAccessor } from './number_value_accessor';
import { RadioControlValueAccessor } from './radio_control_value_accessor';
import { RangeValueAccessor } from './range_value_accessor';
import { ReactiveErrors } from './reactive_errors';
import { SelectControlValueAccessor } from './select_control_value_accessor';
import { SelectMultipleControlValueAccessor } from './select_multiple_control_value_accessor';
/**
 * @param {?} name
 * @param {?} parent
 * @return {?}
 */
export function controlPath(name, parent) {
    return [...(/** @type {?} */ (parent.path)), name];
}
/**
 * @param {?} control
 * @param {?} dir
 * @return {?}
 */
export function setUpControl(control, dir) {
    if (!control)
        _throwError(dir, 'Cannot find control with');
    if (!dir.valueAccessor)
        _throwError(dir, 'No value accessor for form control with');
    control.validator = Validators.compose([(/** @type {?} */ (control.validator)), dir.validator]);
    control.asyncValidator = Validators.composeAsync([(/** @type {?} */ (control.asyncValidator)), dir.asyncValidator]);
    (/** @type {?} */ (dir.valueAccessor)).writeValue(control.value);
    setUpViewChangePipeline(control, dir);
    setUpModelChangePipeline(control, dir);
    setUpBlurPipeline(control, dir);
    if ((/** @type {?} */ (dir.valueAccessor)).setDisabledState) {
        control.registerOnDisabledChange((/**
         * @param {?} isDisabled
         * @return {?}
         */
        (isDisabled) => { (/** @type {?} */ ((/** @type {?} */ (dir.valueAccessor)).setDisabledState))(isDisabled); }));
    }
    // re-run validation when validator binding changes, e.g. minlength=3 -> minlength=4
    dir._rawValidators.forEach((/**
     * @param {?} validator
     * @return {?}
     */
    (validator) => {
        if (((/** @type {?} */ (validator))).registerOnValidatorChange)
            (/** @type {?} */ (((/** @type {?} */ (validator))).registerOnValidatorChange))((/**
             * @return {?}
             */
            () => control.updateValueAndValidity()));
    }));
    dir._rawAsyncValidators.forEach((/**
     * @param {?} validator
     * @return {?}
     */
    (validator) => {
        if (((/** @type {?} */ (validator))).registerOnValidatorChange)
            (/** @type {?} */ (((/** @type {?} */ (validator))).registerOnValidatorChange))((/**
             * @return {?}
             */
            () => control.updateValueAndValidity()));
    }));
}
/**
 * @param {?} control
 * @param {?} dir
 * @return {?}
 */
export function cleanUpControl(control, dir) {
    (/** @type {?} */ (dir.valueAccessor)).registerOnChange((/**
     * @return {?}
     */
    () => _noControlError(dir)));
    (/** @type {?} */ (dir.valueAccessor)).registerOnTouched((/**
     * @return {?}
     */
    () => _noControlError(dir)));
    dir._rawValidators.forEach((/**
     * @param {?} validator
     * @return {?}
     */
    (validator) => {
        if (validator.registerOnValidatorChange) {
            validator.registerOnValidatorChange(null);
        }
    }));
    dir._rawAsyncValidators.forEach((/**
     * @param {?} validator
     * @return {?}
     */
    (validator) => {
        if (validator.registerOnValidatorChange) {
            validator.registerOnValidatorChange(null);
        }
    }));
    if (control)
        control._clearChangeFns();
}
/**
 * @param {?} control
 * @param {?} dir
 * @return {?}
 */
function setUpViewChangePipeline(control, dir) {
    (/** @type {?} */ (dir.valueAccessor)).registerOnChange((/**
     * @param {?} newValue
     * @return {?}
     */
    (newValue) => {
        control._pendingValue = newValue;
        control._pendingChange = true;
        control._pendingDirty = true;
        if (control.updateOn === 'change')
            updateControl(control, dir);
    }));
}
/**
 * @param {?} control
 * @param {?} dir
 * @return {?}
 */
function setUpBlurPipeline(control, dir) {
    (/** @type {?} */ (dir.valueAccessor)).registerOnTouched((/**
     * @return {?}
     */
    () => {
        control._pendingTouched = true;
        if (control.updateOn === 'blur' && control._pendingChange)
            updateControl(control, dir);
        if (control.updateOn !== 'submit')
            control.markAsTouched();
    }));
}
/**
 * @param {?} control
 * @param {?} dir
 * @return {?}
 */
function updateControl(control, dir) {
    if (control._pendingDirty)
        control.markAsDirty();
    control.setValue(control._pendingValue, { emitModelToViewChange: false });
    dir.viewToModelUpdate(control._pendingValue);
    control._pendingChange = false;
}
/**
 * @param {?} control
 * @param {?} dir
 * @return {?}
 */
function setUpModelChangePipeline(control, dir) {
    control.registerOnChange((/**
     * @param {?} newValue
     * @param {?} emitModelEvent
     * @return {?}
     */
    (newValue, emitModelEvent) => {
        // control -> view
        (/** @type {?} */ (dir.valueAccessor)).writeValue(newValue);
        // control -> ngModel
        if (emitModelEvent)
            dir.viewToModelUpdate(newValue);
    }));
}
/**
 * @param {?} control
 * @param {?} dir
 * @return {?}
 */
export function setUpFormContainer(control, dir) {
    if (control == null)
        _throwError(dir, 'Cannot find control with');
    control.validator = Validators.compose([control.validator, dir.validator]);
    control.asyncValidator = Validators.composeAsync([control.asyncValidator, dir.asyncValidator]);
}
/**
 * @param {?} dir
 * @return {?}
 */
function _noControlError(dir) {
    return _throwError(dir, 'There is no FormControl instance attached to form control element with');
}
/**
 * @param {?} dir
 * @param {?} message
 * @return {?}
 */
function _throwError(dir, message) {
    /** @type {?} */
    let messageEnd;
    if ((/** @type {?} */ (dir.path)).length > 1) {
        messageEnd = `path: '${(/** @type {?} */ (dir.path)).join(' -> ')}'`;
    }
    else if ((/** @type {?} */ (dir.path))[0]) {
        messageEnd = `name: '${dir.path}'`;
    }
    else {
        messageEnd = 'unspecified name attribute';
    }
    throw new Error(`${message} ${messageEnd}`);
}
/**
 * @param {?} validators
 * @return {?}
 */
export function composeValidators(validators) {
    return validators != null ? Validators.compose(validators.map(normalizeValidator)) : null;
}
/**
 * @param {?} validators
 * @return {?}
 */
export function composeAsyncValidators(validators) {
    return validators != null ? Validators.composeAsync(validators.map(normalizeAsyncValidator)) :
        null;
}
/**
 * @param {?} changes
 * @param {?} viewModel
 * @return {?}
 */
export function isPropertyUpdated(changes, viewModel) {
    if (!changes.hasOwnProperty('model'))
        return false;
    /** @type {?} */
    const change = changes['model'];
    if (change.isFirstChange())
        return true;
    return !looseIdentical(viewModel, change.currentValue);
}
/** @type {?} */
const BUILTIN_ACCESSORS = [
    CheckboxControlValueAccessor,
    RangeValueAccessor,
    NumberValueAccessor,
    SelectControlValueAccessor,
    SelectMultipleControlValueAccessor,
    RadioControlValueAccessor,
];
/**
 * @param {?} valueAccessor
 * @return {?}
 */
export function isBuiltInAccessor(valueAccessor) {
    return BUILTIN_ACCESSORS.some((/**
     * @param {?} a
     * @return {?}
     */
    a => valueAccessor.constructor === a));
}
/**
 * @param {?} form
 * @param {?} directives
 * @return {?}
 */
export function syncPendingControls(form, directives) {
    form._syncPendingControls();
    directives.forEach((/**
     * @param {?} dir
     * @return {?}
     */
    dir => {
        /** @type {?} */
        const control = (/** @type {?} */ (dir.control));
        if (control.updateOn === 'submit' && control._pendingChange) {
            dir.viewToModelUpdate(control._pendingValue);
            control._pendingChange = false;
        }
    }));
}
// TODO: vsavkin remove it once https://github.com/angular/angular/issues/3011 is implemented
/**
 * @param {?} dir
 * @param {?} valueAccessors
 * @return {?}
 */
export function selectValueAccessor(dir, valueAccessors) {
    if (!valueAccessors)
        return null;
    if (!Array.isArray(valueAccessors))
        _throwError(dir, 'Value accessor was not provided as an array for form control with');
    /** @type {?} */
    let defaultAccessor = undefined;
    /** @type {?} */
    let builtinAccessor = undefined;
    /** @type {?} */
    let customAccessor = undefined;
    valueAccessors.forEach((/**
     * @param {?} v
     * @return {?}
     */
    (v) => {
        if (v.constructor === DefaultValueAccessor) {
            defaultAccessor = v;
        }
        else if (isBuiltInAccessor(v)) {
            if (builtinAccessor)
                _throwError(dir, 'More than one built-in value accessor matches form control with');
            builtinAccessor = v;
        }
        else {
            if (customAccessor)
                _throwError(dir, 'More than one custom value accessor matches form control with');
            customAccessor = v;
        }
    }));
    if (customAccessor)
        return customAccessor;
    if (builtinAccessor)
        return builtinAccessor;
    if (defaultAccessor)
        return defaultAccessor;
    _throwError(dir, 'No valid value accessor for form control with');
    return null;
}
/**
 * @template T
 * @param {?} list
 * @param {?} el
 * @return {?}
 */
export function removeDir(list, el) {
    /** @type {?} */
    const index = list.indexOf(el);
    if (index > -1)
        list.splice(index, 1);
}
// TODO(kara): remove after deprecation period
/**
 * @param {?} name
 * @param {?} type
 * @param {?} instance
 * @param {?} warningConfig
 * @return {?}
 */
export function _ngModelWarning(name, type, instance, warningConfig) {
    if (!isDevMode() || warningConfig === 'never')
        return;
    if (((warningConfig === null || warningConfig === 'once') && !type._ngModelWarningSentOnce) ||
        (warningConfig === 'always' && !instance._ngModelWarningSent)) {
        ReactiveErrors.ngModelWarning(name);
        type._ngModelWarningSentOnce = true;
        instance._ngModelWarningSent = true;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2hhcmVkLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvZm9ybXMvc3JjL2RpcmVjdGl2ZXMvc2hhcmVkLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLFNBQVMsRUFBRSxlQUFlLElBQUksY0FBYyxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBRzNFLE9BQU8sRUFBQyxVQUFVLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFHekMsT0FBTyxFQUFDLDRCQUE0QixFQUFDLE1BQU0sMkJBQTJCLENBQUM7QUFHdkUsT0FBTyxFQUFDLG9CQUFvQixFQUFDLE1BQU0sMEJBQTBCLENBQUM7QUFFOUQsT0FBTyxFQUFDLHVCQUF1QixFQUFFLGtCQUFrQixFQUFDLE1BQU0sdUJBQXVCLENBQUM7QUFDbEYsT0FBTyxFQUFDLG1CQUFtQixFQUFDLE1BQU0seUJBQXlCLENBQUM7QUFDNUQsT0FBTyxFQUFDLHlCQUF5QixFQUFDLE1BQU0sZ0NBQWdDLENBQUM7QUFDekUsT0FBTyxFQUFDLGtCQUFrQixFQUFDLE1BQU0sd0JBQXdCLENBQUM7QUFFMUQsT0FBTyxFQUFDLGNBQWMsRUFBQyxNQUFNLG1CQUFtQixDQUFDO0FBQ2pELE9BQU8sRUFBQywwQkFBMEIsRUFBQyxNQUFNLGlDQUFpQyxDQUFDO0FBQzNFLE9BQU8sRUFBQyxrQ0FBa0MsRUFBQyxNQUFNLDBDQUEwQyxDQUFDOzs7Ozs7QUFJNUYsTUFBTSxVQUFVLFdBQVcsQ0FBQyxJQUFZLEVBQUUsTUFBd0I7SUFDaEUsT0FBTyxDQUFDLEdBQUcsbUJBQUEsTUFBTSxDQUFDLElBQUksRUFBRSxFQUFFLElBQUksQ0FBQyxDQUFDO0FBQ2xDLENBQUM7Ozs7OztBQUVELE1BQU0sVUFBVSxZQUFZLENBQUMsT0FBb0IsRUFBRSxHQUFjO0lBQy9ELElBQUksQ0FBQyxPQUFPO1FBQUUsV0FBVyxDQUFDLEdBQUcsRUFBRSwwQkFBMEIsQ0FB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QUMsdUJBQXVCLENBQUM7UUFDdkYsQ0FBQyxhQUFhLEtBQUssUUFBUSxJQUFJLENBQUMsUUFBUSxDQUFDLG1CQUFtQixDQUFDLEVBQUU7UUFDakUsY0FBYyxDQUFDLGNBQWMsQ0FBQyxJQUFJLENBQUMsQ0FBQztRQUNwQyxJQUFJLENBQUMsdUJBQXVCLEdBQUcsSUFBSSxDQUFDO1FBQ3BDLFFBQVEsQ0FBQyxtQkFBbUIsR0FBRyxJQUFJLENBQUM7S0FDckM7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge2lzRGV2TW9kZSwgybVsb29zZUlkZW50aWNhbCBhcyBsb29zZUlkZW50aWNhbH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7Rm9ybUFycmF5LCBGb3JtQ29udHJvbCwgRm9ybUdyb3VwfSBmcm9tICcuLi9tb2RlbCc7XG5pbXBvcnQge1ZhbGlkYXRvcnN9IGZyb20gJy4uL3ZhbGlkYXRvcnMnO1xuaW1wb3J0IHtBYnN0cmFjdENvbnRyb2xEaXJlY3RpdmV9IGZyb20gJy4vYWJzdHJhY3RfY29udHJvbF9kaXJlY3RpdmUnO1xuaW1wb3J0IHtBYnN0cmFjdEZvcm1Hcm91cERpcmVjdGl2ZX0gZnJvbSAnLi9hYnN0cmFjdF9mb3JtX2dyb3VwX2RpcmVjdGl2ZSc7XG5pbXBvcnQge0NoZWNrYm94Q29udHJvbFZhbHVlQWNjZXNzb3J9IGZyb20gJy4vY2hlY2tib3hfdmFsdWVfYWNjZXNzb3InO1xuaW1wb3J0IHtDb250cm9sQ29udGFpbmVyfSBmcm9tICcuL2NvbnRyb2xfY29udGFpbmVyJztcbmltcG9ydCB7Q29udHJvbFZhbHVlQWNjZXNzb3J9IGZyb20gJy4vY29udHJvbF92YWx1ZV9hY2Nlc3Nvcic7XG5pbXBvcnQge0RlZmF1bHRWYWx1ZUFjY2Vzc29yfSBmcm9tICcuL2RlZmF1bHRfdmFsdWVfYWNjZXNzb3InO1xuaW1wb3J0IHtOZ0NvbnRyb2x9IGZyb20gJy4vbmdfY29udHJvbCc7XG5pbXBvcnQge25vcm1hbGl6ZUFzeW5jVmFsaWRhdG9yLCBub3JtYWxpemVWYWxpZGF0b3J9IGZyb20gJy4vbm9ybWFsaXplX3ZhbGlkYXRvcic7XG5pbXBvcnQge051bWJlclZhbHVlQWNjZXNzb3J9IGZyb20gJy4vbnVtYmVyX3ZhbHVlX2FjY2Vzc29yJztcbmltcG9ydCB7UmFkaW9Db250cm9sVmFsdWVBY2Nlc3Nvcn0gZnJvbSAnLi9yYWRpb19jb250cm9sX3ZhbHVlX2FjY2Vzc29yJztcbmltcG9ydCB7UmFuZ2VWYWx1ZUFjY2Vzc29yfSBmcm9tICcuL3JhbmdlX3ZhbHVlX2FjY2Vzc29yJztcbmltcG9ydCB7Rm9ybUFycmF5TmFtZX0gZnJvbSAnLi9yZWFjdGl2ZV9kaXJlY3RpdmVzL2Zvcm1fZ3JvdXBfbmFtZSc7XG5pbXBvcnQge1JlYWN0aXZlRXJyb3JzfSBmcm9tICcuL3JlYWN0aXZlX2Vycm9ycyc7XG5pbXBvcnQge1NlbGVjdENvbnRyb2xWYWx1ZUFjY2Vzc29yfSBmcm9tICcuL3NlbGVjdF9jb250cm9sX3ZhbHVlX2FjY2Vzc29yJztcbmltcG9ydCB7U2VsZWN0TXVsdGlwbGVDb250cm9sVmFsdWVBY2Nlc3Nvcn0gZnJvbSAnLi9zZWxlY3RfbXVsdGlwbGVfY29udHJvbF92YWx1ZV9hY2Nlc3Nvcic7XG5pbXBvcnQge0FzeW5jVmFsaWRhdG9yLCBBc3luY1ZhbGlkYXRvckZuLCBWYWxpZGF0b3IsIFZhbGlkYXRvckZufSBmcm9tICcuL3ZhbGlkYXRvcnMnO1xuXG5cbmV4cG9ydCBmdW5jdGlvbiBjb250cm9sUGF0aChuYW1lOiBzdHJpbmcsIHBhcmVudDogQ29udHJvbENvbnRhaW5lcik6IHN0cmluZ1tdIHtcbiAgcmV0dXJuIFsuLi5wYXJlbnQucGF0aCAhLCBuYW1lXTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNldFVwQ29udHJvbChjb250cm9sOiBGb3JtQ29udHJvbCwgZGlyOiBOZ0NvbnRyb2wpOiB2b2lkIHtcbiAgaWYgKCFjb250cm9sKSBfdGhyb3dFcnJvcihkaXIsICdDYW5ub3QgZmluZCBjb250cm9sIHdpdGgnKTtcbiAgaWYgKCFkaXIudmFsdWVBY2Nlc3NvcikgX3Rocm93RXJyb3IoZGlyLCAnTm8gdmFsdWUgYWNjZXNzb3IgZm9yIGZvcm0gY29udHJvbCB3aXRoJyk7XG5cbiAgY29udHJvbC52YWxpZGF0b3IgPSBWYWxpZGF0b3JzLmNvbXBvc2UoW2NvbnRyb2wudmFsaWRhdG9yICEsIGRpci52YWxpZGF0b3JdKTtcbiAgY29udHJvbC5hc3luY1ZhbGlkYXRvciA9IFZhbGlkYXRvcnMuY29tcG9zZUFzeW5jKFtjb250cm9sLmFzeW5jVmFsaWRhdG9yICEsIGRpci5hc3luY1ZhbGlkYXRvcl0pO1xuICBkaXIudmFsdWVBY2Nlc3NvciAhLndyaXRlVmFsdWUoY29udHJvbC52YWx1ZSk7XG5cbiAgc2V0VXBWaWV3Q2hhbmdlUGlwZWxpbmUoY29udHJvbCwgZGlyKTtcbiAgc2V0VXBNb2RlbENoYW5nZVBpcGVsaW5lKGNvbnRyb2wsIGRpcik7XG5cbiAgc2V0VXBCbHVyUGlwZWxpbmUoY29udHJvbCwgZGlyKTtcblxuICBpZiAoZGlyLnZhbHVlQWNjZXNzb3IgIS5zZXREaXNhYmxlZFN0YXRlKSB7XG4gICAgY29udHJvbC5yZWdpc3Rlck9uRGlzYWJsZWRDaGFuZ2UoXG4gICAgICAgIChpc0Rpc2FibGVkOiBib29sZWFuKSA9PiB7IGRpci52YWx1ZUFjY2Vzc29yICEuc2V0RGlzYWJsZWRTdGF0ZSAhKGlzRGlzYWJsZWQpOyB9KTtcbiAgfVxuXG4gIC8vIHJlLXJ1biB2YWxpZGF0aW9uIHdoZW4gdmFsaWRhdG9yIGJpbmRpbmcgY2hhbmdlcywgZS5nLiBtaW5sZW5ndGg9MyAtPiBtaW5sZW5ndGg9NFxuICBkaXIuX3Jhd1ZhbGlkYXRvcnMuZm9yRWFjaCgodmFsaWRhdG9yOiBWYWxpZGF0b3IgfCBWYWxpZGF0b3JGbikgPT4ge1xuICAgIGlmICgoPFZhbGlkYXRvcj52YWxpZGF0b3IpLnJlZ2lzdGVyT25WYWxpZGF0b3JDaGFuZ2UpXG4gICAgICAoPFZhbGlkYXRvcj52YWxpZGF0b3IpLnJlZ2lzdGVyT25WYWxpZGF0b3JDaGFuZ2UgISgoKSA9PiBjb250cm9sLnVwZGF0ZVZhbHVlQW5kVmFsaWRpdHkoKSk7XG4gIH0pO1xuXG4gIGRpci5fcmF3QXN5bmNWYWxpZGF0b3JzLmZvckVhY2goKHZhbGlkYXRvcjogQXN5bmNWYWxpZGF0b3IgfCBBc3luY1ZhbGlkYXRvckZuKSA9PiB7XG4gICAgaWYgKCg8VmFsaWRhdG9yPnZhbGlkYXRvcikucmVnaXN0ZXJPblZhbGlkYXRvckNoYW5nZSlcbiAgICAgICg8VmFsaWRhdG9yPnZhbGlkYXRvcikucmVnaXN0ZXJPblZhbGlkYXRvckNoYW5nZSAhKCgpID0+IGNvbnRyb2wudXBkYXRlVmFsdWVBbmRWYWxpZGl0eSgpKTtcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhblVwQ29udHJvbChjb250cm9sOiBGb3JtQ29udHJvbCwgZGlyOiBOZ0NvbnRyb2wpIHtcbiAgZGlyLnZhbHVlQWNjZXNzb3IgIS5yZWdpc3Rlck9uQ2hhbmdlKCgpID0+IF9ub0NvbnRyb2xFcnJvcihkaXIpKTtcbiAgZGlyLnZhbHVlQWNjZXNzb3IgIS5yZWdpc3Rlck9uVG91Y2hlZCgoKSA9PiBfbm9Db250cm9sRXJyb3IoZGlyKSk7XG5cbiAgZGlyLl9yYXdWYWxpZGF0b3JzLmZvckVhY2goKHZhbGlkYXRvcjogYW55KSA9PiB7XG4gICAgaWYgKHZhbGlkYXRvci5yZWdpc3Rlck9uVmFsaWRhdG9yQ2hhbmdlKSB7XG4gICAgICB2YWxpZGF0b3IucmVnaXN0ZXJPblZhbGlkYXRvckNoYW5nZShudWxsKTtcbiAgICB9XG4gIH0pO1xuXG4gIGRpci5fcmF3QXN5bmNWYWxpZGF0b3JzLmZvckVhY2goKHZhbGlkYXRvcjogYW55KSA9PiB7XG4gICAgaWYgKHZhbGlkYXRvci5yZWdpc3Rlck9uVmFsaWRhdG9yQ2hhbmdlKSB7XG4gICAgICB2YWxpZGF0b3IucmVnaXN0ZXJPblZhbGlkYXRvckNoYW5nZShudWxsKTtcbiAgICB9XG4gIH0pO1xuXG4gIGlmIChjb250cm9sKSBjb250cm9sLl9jbGVhckNoYW5nZUZucygpO1xufVxuXG5mdW5jdGlvbiBzZXRVcFZpZXdDaGFuZ2VQaXBlbGluZShjb250cm9sOiBGb3JtQ29udHJvbCwgZGlyOiBOZ0NvbnRyb2wpOiB2b2lkIHtcbiAgZGlyLnZhbHVlQWNjZXNzb3IgIS5yZWdpc3Rlck9uQ2hhbmdlKChuZXdWYWx1ZTogYW55KSA9PiB7XG4gICAgY29udHJvbC5fcGVuZGluZ1ZhbHVlID0gbmV3VmFsdWU7XG4gICAgY29udHJvbC5fcGVuZGluZ0NoYW5nZSA9IHRydWU7XG4gICAgY29udHJvbC5fcGVuZGluZ0RpcnR5ID0gdHJ1ZTtcblxuICAgIGlmIChjb250cm9sLnVwZGF0ZU9uID09PSAnY2hhbmdlJykgdXBkYXRlQ29udHJvbChjb250cm9sLCBkaXIpO1xuICB9KTtcbn1cblxuZnVuY3Rpb24gc2V0VXBCbHVyUGlwZWxpbmUoY29udHJvbDogRm9ybUNvbnRyb2wsIGRpcjogTmdDb250cm9sKTogdm9pZCB7XG4gIGRpci52YWx1ZUFjY2Vzc29yICEucmVnaXN0ZXJPblRvdWNoZWQoKCkgPT4ge1xuICAgIGNvbnRyb2wuX3BlbmRpbmdUb3VjaGVkID0gdHJ1ZTtcblxuICAgIGlmIChjb250cm9sLnVwZGF0ZU9uID09PSAnYmx1cicgJiYgY29udHJvbC5fcGVuZGluZ0NoYW5nZSkgdXBkYXRlQ29udHJvbChjb250cm9sLCBkaXIpO1xuICAgIGlmIChjb250cm9sLnVwZGF0ZU9uICE9PSAnc3VibWl0JykgY29udHJvbC5tYXJrQXNUb3VjaGVkKCk7XG4gIH0pO1xufVxuXG5mdW5jdGlvbiB1cGRhdGVDb250cm9sKGNvbnRyb2w6IEZvcm1Db250cm9sLCBkaXI6IE5nQ29udHJvbCk6IHZvaWQge1xuICBpZiAoY29udHJvbC5fcGVuZGluZ0RpcnR5KSBjb250cm9sLm1hcmtBc0RpcnR5KCk7XG4gIGNvbnRyb2wuc2V0VmFsdWUoY29udHJvbC5fcGVuZGluZ1ZhbHVlLCB7ZW1pdE1vZGVsVG9WaWV3Q2hhbmdlOiBmYWxzZX0pO1xuICBkaXIudmlld1RvTW9kZWxVcGRhdGUoY29udHJvbC5fcGVuZGluZ1ZhbHVlKTtcbiAgY29udHJvbC5fcGVuZGluZ0NoYW5nZSA9IGZhbHNlO1xufVxuXG5mdW5jdGlvbiBzZXRVcE1vZGVsQ2hhbmdlUGlwZWxpbmUoY29udHJvbDogRm9ybUNvbnRyb2wsIGRpcjogTmdDb250cm9sKTogdm9pZCB7XG4gIGNvbnRyb2wucmVnaXN0ZXJPbkNoYW5nZSgobmV3VmFsdWU6IGFueSwgZW1pdE1vZGVsRXZlbnQ6IGJvb2xlYW4pID0+IHtcbiAgICAvLyBjb250cm9sIC0+IHZpZXdcbiAgICBkaXIudmFsdWVBY2Nlc3NvciAhLndyaXRlVmFsdWUobmV3VmFsdWUpO1xuXG4gICAgLy8gY29udHJvbCAtPiBuZ01vZGVsXG4gICAgaWYgKGVtaXRNb2RlbEV2ZW50KSBkaXIudmlld1RvTW9kZWxVcGRhdGUobmV3VmFsdWUpO1xuICB9KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNldFVwRm9ybUNvbnRhaW5lcihcbiAgICBjb250cm9sOiBGb3JtR3JvdXAgfCBGb3JtQXJyYXksIGRpcjogQWJzdHJhY3RGb3JtR3JvdXBEaXJlY3RpdmUgfCBGb3JtQXJyYXlOYW1lKSB7XG4gIGlmIChjb250cm9sID09IG51bGwpIF90aHJvd0Vycm9yKGRpciwgJ0Nhbm5vdCBmaW5kIGNvbnRyb2wgd2l0aCcpO1xuICBjb250cm9sLnZhbGlkYXRvciA9IFZhbGlkYXRvcnMuY29tcG9zZShbY29udHJvbC52YWxpZGF0b3IsIGRpci52YWxpZGF0b3JdKTtcbiAgY29udHJvbC5hc3luY1ZhbGlkYXRvciA9IFZhbGlkYXRvcnMuY29tcG9zZUFzeW5jKFtjb250cm9sLmFzeW5jVmFsaWRhdG9yLCBkaXIuYXN5bmNWYWxpZGF0b3JdKTtcbn1cblxuZnVuY3Rpb24gX25vQ29udHJvbEVycm9yKGRpcjogTmdDb250cm9sKSB7XG4gIHJldHVybiBfdGhyb3dFcnJvcihkaXIsICdUaGVyZSBpcyBubyBGb3JtQ29udHJvbCBpbnN0YW5jZSBhdHRhY2hlZCB0byBmb3JtIGNvbnRyb2wgZWxlbWVudCB3aXRoJyk7XG59XG5cbmZ1bmN0aW9uIF90aHJvd0Vycm9yKGRpcjogQWJzdHJhY3RDb250cm9sRGlyZWN0aXZlLCBtZXNzYWdlOiBzdHJpbmcpOiB2b2lkIHtcbiAgbGV0IG1lc3NhZ2VFbmQ6IHN0cmluZztcbiAgaWYgKGRpci5wYXRoICEubGVuZ3RoID4gMSkge1xuICAgIG1lc3NhZ2VFbmQgPSBgcGF0aDogJyR7ZGlyLnBhdGghLmpvaW4oJyAtPiAnKX0nYDtcbiAgfSBlbHNlIGlmIChkaXIucGF0aCAhWzBdKSB7XG4gICAgbWVzc2FnZUVuZCA9IGBuYW1lOiAnJHtkaXIucGF0aH0nYDtcbiAgfSBlbHNlIHtcbiAgICBtZXNzYWdlRW5kID0gJ3Vuc3BlY2lmaWVkIG5hbWUgYXR0cmlidXRlJztcbiAgfVxuICB0aHJvdyBuZXcgRXJyb3IoYCR7bWVzc2FnZX0gJHttZXNzYWdlRW5kfWApO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY29tcG9zZVZhbGlkYXRvcnModmFsaWRhdG9yczogQXJyYXk8VmFsaWRhdG9yfFZhbGlkYXRvckZuPik6IFZhbGlkYXRvckZufG51bGwge1xuICByZXR1cm4gdmFsaWRhdG9ycyAhPSBudWxsID8gVmFsaWRhdG9ycy5jb21wb3NlKHZhbGlkYXRvcnMubWFwKG5vcm1hbGl6ZVZhbGlkYXRvcikpIDogbnVsbDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNvbXBvc2VBc3luY1ZhbGlkYXRvcnModmFsaWRhdG9yczogQXJyYXk8QXN5bmNWYWxpZGF0b3J8QXN5bmNWYWxpZGF0b3JGbj4pOlxuICAgIEFzeW5jVmFsaWRhdG9yRm58bnVsbCB7XG4gIHJldHVybiB2YWxpZGF0b3JzICE9IG51bGwgPyBWYWxpZGF0b3JzLmNvbXBvc2VBc3luYyh2YWxpZGF0b3JzLm1hcChub3JtYWxpemVBc3luY1ZhbGlkYXRvcikpIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1Byb3BlcnR5VXBkYXRlZChjaGFuZ2VzOiB7W2tleTogc3RyaW5nXTogYW55fSwgdmlld01vZGVsOiBhbnkpOiBib29sZWFuIHtcbiAgaWYgKCFjaGFuZ2VzLmhhc093blByb3BlcnR5KCdtb2RlbCcpKSByZXR1cm4gZmFsc2U7XG4gIGNvbnN0IGNoYW5nZSA9IGNoYW5nZXNbJ21vZGVsJ107XG5cbiAgaWYgKGNoYW5nZS5pc0ZpcnN0Q2hhbmdlKCkpIHJldHVybiB0cnVlO1xuICByZXR1cm4gIWxvb3NlSWRlbnRpY2FsKHZpZXdNb2RlbCwgY2hhbmdlLmN1cnJlbnRWYWx1ZSk7XG59XG5cbmNvbnN0IEJVSUxUSU5fQUNDRVNTT1JTID0gW1xuICBDaGVja2JveENvbnRyb2xWYWx1ZUFjY2Vzc29yLFxuICBSYW5nZVZhbHVlQWNjZXNzb3IsXG4gIE51bWJlclZhbHVlQWNjZXNzb3IsXG4gIFNlbGVjdENvbnRyb2xWYWx1ZUFjY2Vzc29yLFxuICBTZWxlY3RNdWx0aXBsZUNvbnRyb2xWYWx1ZUFjY2Vzc29yLFxuICBSYWRpb0NvbnRyb2xWYWx1ZUFjY2Vzc29yLFxuXTtcblxuZXhwb3J0IGZ1bmN0aW9uIGlzQnVpbHRJbkFjY2Vzc29yKHZhbHVlQWNjZXNzb3I6IENvbnRyb2xWYWx1ZUFjY2Vzc29yKTogYm9vbGVhbiB7XG4gIHJldHVybiBCVUlMVElOX0FDQ0VTU09SUy5zb21lKGEgPT4gdmFsdWVBY2Nlc3Nvci5jb25zdHJ1Y3RvciA9PT0gYSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzeW5jUGVuZGluZ0NvbnRyb2xzKGZvcm06IEZvcm1Hcm91cCwgZGlyZWN0aXZlczogTmdDb250cm9sW10pOiB2b2lkIHtcbiAgZm9ybS5fc3luY1BlbmRpbmdDb250cm9scygpO1xuICBkaXJlY3RpdmVzLmZvckVhY2goZGlyID0+IHtcbiAgICBjb25zdCBjb250cm9sID0gZGlyLmNvbnRyb2wgYXMgRm9ybUNvbnRyb2w7XG4gICAgaWYgKGNvbnRyb2wudXBkYXRlT24gPT09ICdzdWJtaXQnICYmIGNvbnRyb2wuX3BlbmRpbmdDaGFuZ2UpIHtcbiAgICAgIGRpci52aWV3VG9Nb2RlbFVwZGF0ZShjb250cm9sLl9wZW5kaW5nVmFsdWUpO1xuICAgICAgY29udHJvbC5fcGVuZGluZ0NoYW5nZSA9IGZhbHNlO1xuICAgIH1cbiAgfSk7XG59XG5cbi8vIFRPRE86IHZzYXZraW4gcmVtb3ZlIGl0IG9uY2UgaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvYW5ndWxhci9pc3N1ZXMvMzAxMSBpcyBpbXBsZW1lbnRlZFxuZXhwb3J0IGZ1bmN0aW9uIHNlbGVjdFZhbHVlQWNjZXNzb3IoXG4gICAgZGlyOiBOZ0NvbnRyb2wsIHZhbHVlQWNjZXNzb3JzOiBDb250cm9sVmFsdWVBY2Nlc3NvcltdKTogQ29udHJvbFZhbHVlQWNjZXNzb3J8bnVsbCB7XG4gIGlmICghdmFsdWVBY2Nlc3NvcnMpIHJldHVybiBudWxsO1xuXG4gIGlmICghQXJyYXkuaXNBcnJheSh2YWx1ZUFjY2Vzc29ycykpXG4gICAgX3Rocm93RXJyb3IoZGlyLCAnVmFsdWUgYWNjZXNzb3Igd2FzIG5vdCBwcm92aWRlZCBhcyBhbiBhcnJheSBmb3IgZm9ybSBjb250cm9sIHdpdGgnKTtcblxuICBsZXQgZGVmYXVsdEFjY2Vzc29yOiBDb250cm9sVmFsdWVBY2Nlc3Nvcnx1bmRlZmluZWQgPSB1bmRlZmluZWQ7XG4gIGxldCBidWlsdGluQWNjZXNzb3I6IENvbnRyb2xWYWx1ZUFjY2Vzc29yfHVuZGVmaW5lZCA9IHVuZGVmaW5lZDtcbiAgbGV0IGN1c3RvbUFjY2Vzc29yOiBDb250cm9sVmFsdWVBY2Nlc3Nvcnx1bmRlZmluZWQgPSB1bmRlZmluZWQ7XG5cbiAgdmFsdWVBY2Nlc3NvcnMuZm9yRWFjaCgodjogQ29udHJvbFZhbHVlQWNjZXNzb3IpID0+IHtcbiAgICBpZiAodi5jb25zdHJ1Y3RvciA9PT0gRGVmYXVsdFZhbHVlQWNjZXNzb3IpIHtcbiAgICAgIGRlZmF1bHRBY2Nlc3NvciA9IHY7XG5cbiAgICB9IGVsc2UgaWYgKGlzQnVpbHRJbkFjY2Vzc29yKHYpKSB7XG4gICAgICBpZiAoYnVpbHRpbkFjY2Vzc29yKVxuICAgICAgICBfdGhyb3dFcnJvcihkaXIsICdNb3JlIHRoYW4gb25lIGJ1aWx0LWluIHZhbHVlIGFjY2Vzc29yIG1hdGNoZXMgZm9ybSBjb250cm9sIHdpdGgnKTtcbiAgICAgIGJ1aWx0aW5BY2Nlc3NvciA9IHY7XG5cbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKGN1c3RvbUFjY2Vzc29yKVxuICAgICAgICBfdGhyb3dFcnJvcihkaXIsICdNb3JlIHRoYW4gb25lIGN1c3RvbSB2YWx1ZSBhY2Nlc3NvciBtYXRjaGVzIGZvcm0gY29udHJvbCB3aXRoJyk7XG4gICAgICBjdXN0b21BY2Nlc3NvciA9IHY7XG4gICAgfVxuICB9KTtcblxuICBpZiAoY3VzdG9tQWNjZXNzb3IpIHJldHVybiBjdXN0b21BY2Nlc3NvcjtcbiAgaWYgKGJ1aWx0aW5BY2Nlc3NvcikgcmV0dXJuIGJ1aWx0aW5BY2Nlc3NvcjtcbiAgaWYgKGRlZmF1bHRBY2Nlc3NvcikgcmV0dXJuIGRlZmF1bHRBY2Nlc3NvcjtcblxuICBfdGhyb3dFcnJvcihkaXIsICdObyB2YWxpZCB2YWx1ZSBhY2Nlc3NvciBmb3IgZm9ybSBjb250cm9sIHdpdGgnKTtcbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiByZW1vdmVEaXI8VD4obGlzdDogVFtdLCBlbDogVCk6IHZvaWQge1xuICBjb25zdCBpbmRleCA9IGxpc3QuaW5kZXhPZihlbCk7XG4gIGlmIChpbmRleCA+IC0xKSBsaXN0LnNwbGljZShpbmRleCwgMSk7XG59XG5cbi8vIFRPRE8oa2FyYSk6IHJlbW92ZSBhZnRlciBkZXByZWNhdGlvbiBwZXJpb2RcbmV4cG9ydCBmdW5jdGlvbiBfbmdNb2RlbFdhcm5pbmcoXG4gICAgbmFtZTogc3RyaW5nLCB0eXBlOiB7X25nTW9kZWxXYXJuaW5nU2VudE9uY2U6IGJvb2xlYW59LFxuICAgIGluc3RhbmNlOiB7X25nTW9kZWxXYXJuaW5nU2VudDogYm9vbGVhbn0sIHdhcm5pbmdDb25maWc6IHN0cmluZyB8IG51bGwpIHtcbiAgaWYgKCFpc0Rldk1vZGUoKSB8fCB3YXJuaW5nQ29uZmlnID09PSAnbmV2ZXInKSByZXR1cm47XG5cbiAgaWYgKCgod2FybmluZ0NvbmZpZyA9PT0gbnVsbCB8fCB3YXJuaW5nQ29uZmlnID09PSAnb25jZScpICYmICF0eXBlLl9uZ01vZGVsV2FybmluZ1NlbnRPbmNlKSB8fFxuICAgICAgKHdhcm5pbmdDb25maWcgPT09ICdhbHdheXMnICYmICFpbnN0YW5jZS5fbmdNb2RlbFdhcm5pbmdTZW50KSkge1xuICAgIFJlYWN0aXZlRXJyb3JzLm5nTW9kZWxXYXJuaW5nKG5hbWUpO1xuICAgIHR5cGUuX25nTW9kZWxXYXJuaW5nU2VudE9uY2UgPSB0cnVlO1xuICAgIGluc3RhbmNlLl9uZ01vZGVsV2FybmluZ1NlbnQgPSB0cnVlO1xuICB9XG59XG4iXX0=
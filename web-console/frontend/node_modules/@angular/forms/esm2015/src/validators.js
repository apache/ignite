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
import { InjectionToken, ɵisObservable as isObservable, ɵisPromise as isPromise } from '@angular/core';
import { forkJoin, from } from 'rxjs';
import { map } from 'rxjs/operators';
/**
 * @param {?} value
 * @return {?}
 */
function isEmptyInputValue(value) {
    // we don't check for string here so it also works with arrays
    return value == null || value.length === 0;
}
/**
 * \@description
 * An `InjectionToken` for registering additional synchronous validators used with `AbstractControl`s.
 *
 * @see `NG_ASYNC_VALIDATORS`
 *
 * \@usageNotes
 *
 * ### Providing a custom validator
 *
 * The following example registers a custom validator directive. Adding the validator to the
 * existing collection of validators requires the `multi: true` option.
 *
 * ```typescript
 * \@Directive({
 *   selector: '[customValidator]',
 *   providers: [{provide: NG_VALIDATORS, useExisting: CustomValidatorDirective, multi: true}]
 * })
 * class CustomValidatorDirective implements Validator {
 *   validate(control: AbstractControl): ValidationErrors | null {
 *     return { 'custom': true };
 *   }
 * }
 * ```
 *
 * \@publicApi
 * @type {?}
 */
export const NG_VALIDATORS = new InjectionToken('NgValidators');
/**
 * \@description
 * An `InjectionToken` for registering additional asynchronous validators used with `AbstractControl`s.
 *
 * @see `NG_VALIDATORS`
 *
 * \@publicApi
 * @type {?}
 */
export const NG_ASYNC_VALIDATORS = new InjectionToken('NgAsyncValidators');
/**
 * A regular expression that matches valid e-mail addresses.
 *
 * At a high level, this regexp matches e-mail addresses of the format `local-part\@tld`, where:
 * - `local-part` consists of one or more of the allowed characters (alphanumeric and some
 *   punctuation symbols).
 * - `local-part` cannot begin or end with a period (`.`).
 * - `local-part` cannot be longer than 64 characters.
 * - `tld` consists of one or more `labels` separated by periods (`.`). For example `localhost` or
 *   `foo.com`.
 * - A `label` consists of one or more of the allowed characters (alphanumeric, dashes (`-`) and
 *   periods (`.`)).
 * - A `label` cannot begin or end with a dash (`-`) or a period (`.`).
 * - A `label` cannot be longer than 63 characters.
 * - The whole address cannot be longer than 254 characters.
 *
 * ## Implementation background
 *
 * This regexp was ported over from AngularJS (see there for git history):
 * https://github.com/angular/angular.js/blob/c133ef836/src/ng/directive/input.js#L27
 * It is based on the
 * [WHATWG version](https://html.spec.whatwg.org/multipage/input.html#valid-e-mail-address) with
 * some enhancements to incorporate more RFC rules (such as rules related to domain names and the
 * lengths of different parts of the address). The main differences from the WHATWG version are:
 *   - Disallow `local-part` to begin or end with a period (`.`).
 *   - Disallow `local-part` length to exceed 64 characters.
 *   - Disallow total address length to exceed 254 characters.
 *
 * See [this commit](https://github.com/angular/angular.js/commit/f3f5cf72e) for more details.
 * @type {?}
 */
const EMAIL_REGEXP = /^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/;
/**
 * \@description
 * Provides a set of built-in validators that can be used by form controls.
 *
 * A validator is a function that processes a `FormControl` or collection of
 * controls and returns an error map or null. A null map means that validation has passed.
 *
 * @see [Form Validation](/guide/form-validation)
 *
 * \@publicApi
 */
export class Validators {
    /**
     * \@description
     * Validator that requires the control's value to be greater than or equal to the provided number.
     * The validator exists only as a function and not as a directive.
     *
     * \@usageNotes
     *
     * ### Validate against a minimum of 3
     *
     * ```typescript
     * const control = new FormControl(2, Validators.min(3));
     *
     * console.log(control.errors); // {min: {min: 3, actual: 2}}
     * ```
     *
     * @see `updateValueAndValidity()`
     *
     * @param {?} min
     * @return {?} A validator function that returns an error map with the
     * `min` property if the validation check fails, otherwise `null`.
     *
     */
    static min(min) {
        return (/**
         * @param {?} control
         * @return {?}
         */
        (control) => {
            if (isEmptyInputValue(control.value) || isEmptyInputValue(min)) {
                return null; // don't validate empty values to allow optional controls
            }
            /** @type {?} */
            const value = parseFloat(control.value);
            // Controls with NaN values after parsing should be treated as not having a
            // minimum, per the HTML forms spec: https://www.w3.org/TR/html5/forms.html#attr-input-min
            return !isNaN(value) && value < min ? { 'min': { 'min': min, 'actual': control.value } } : null;
        });
    }
    /**
     * \@description
     * Validator that requires the control's value to be less than or equal to the provided number.
     * The validator exists only as a function and not as a directive.
     *
     * \@usageNotes
     *
     * ### Validate against a maximum of 15
     *
     * ```typescript
     * const control = new FormControl(16, Validators.max(15));
     *
     * console.log(control.errors); // {max: {max: 15, actual: 16}}
     * ```
     *
     * @see `updateValueAndValidity()`
     *
     * @param {?} max
     * @return {?} A validator function that returns an error map with the
     * `max` property if the validation check fails, otherwise `null`.
     *
     */
    static max(max) {
        return (/**
         * @param {?} control
         * @return {?}
         */
        (control) => {
            if (isEmptyInputValue(control.value) || isEmptyInputValue(max)) {
                return null; // don't validate empty values to allow optional controls
            }
            /** @type {?} */
            const value = parseFloat(control.value);
            // Controls with NaN values after parsing should be treated as not having a
            // maximum, per the HTML forms spec: https://www.w3.org/TR/html5/forms.html#attr-input-max
            return !isNaN(value) && value > max ? { 'max': { 'max': max, 'actual': control.value } } : null;
        });
    }
    /**
     * \@description
     * Validator that requires the control have a non-empty value.
     *
     * \@usageNotes
     *
     * ### Validate that the field is non-empty
     *
     * ```typescript
     * const control = new FormControl('', Validators.required);
     *
     * console.log(control.errors); // {required: true}
     * ```
     *
     * @see `updateValueAndValidity()`
     *
     * @param {?} control
     * @return {?} An error map with the `required` property
     * if the validation check fails, otherwise `null`.
     *
     */
    static required(control) {
        return isEmptyInputValue(control.value) ? { 'required': true } : null;
    }
    /**
     * \@description
     * Validator that requires the control's value be true. This validator is commonly
     * used for required checkboxes.
     *
     * \@usageNotes
     *
     * ### Validate that the field value is true
     *
     * ```typescript
     * const control = new FormControl('', Validators.requiredTrue);
     *
     * console.log(control.errors); // {required: true}
     * ```
     *
     * @see `updateValueAndValidity()`
     *
     * @param {?} control
     * @return {?} An error map that contains the `required` property
     * set to `true` if the validation check fails, otherwise `null`.
     *
     */
    static requiredTrue(control) {
        return control.value === true ? null : { 'required': true };
    }
    /**
     * \@description
     * Validator that requires the control's value pass an email validation test.
     *
     * Tests the value using a [regular expression](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Regular_Expressions)
     * pattern suitable for common usecases. The pattern is based on the definition of a valid email
     * address in the [WHATWG HTML specification](https://html.spec.whatwg.org/multipage/input.html#valid-e-mail-address)
     * with some enhancements to incorporate more RFC rules (such as rules related to domain names and
     * the lengths of different parts of the address).
     *
     * The differences from the WHATWG version include:
     * - Disallow `local-part` (the part before the `\@` symbol) to begin or end with a period (`.`).
     * - Disallow `local-part` to be longer than 64 characters.
     * - Disallow the whole address to be longer than 254 characters.
     *
     * If this pattern does not satisfy your business needs, you can use `Validators.pattern()` to
     * validate the value against a different pattern.
     *
     * \@usageNotes
     *
     * ### Validate that the field matches a valid email pattern
     *
     * ```typescript
     * const control = new FormControl('bad\@', Validators.email);
     *
     * console.log(control.errors); // {email: true}
     * ```
     *
     * @see `updateValueAndValidity()`
     *
     * @param {?} control
     * @return {?} An error map with the `email` property
     * if the validation check fails, otherwise `null`.
     *
     */
    static email(control) {
        if (isEmptyInputValue(control.value)) {
            return null; // don't validate empty values to allow optional controls
        }
        return EMAIL_REGEXP.test(control.value) ? null : { 'email': true };
    }
    /**
     * \@description
     * Validator that requires the length of the control's value to be greater than or equal
     * to the provided minimum length. This validator is also provided by default if you use the
     * the HTML5 `minlength` attribute.
     *
     * \@usageNotes
     *
     * ### Validate that the field has a minimum of 3 characters
     *
     * ```typescript
     * const control = new FormControl('ng', Validators.minLength(3));
     *
     * console.log(control.errors); // {minlength: {requiredLength: 3, actualLength: 2}}
     * ```
     *
     * ```html
     * <input minlength="5">
     * ```
     *
     * @see `updateValueAndValidity()`
     *
     * @param {?} minLength
     * @return {?} A validator function that returns an error map with the
     * `minlength` if the validation check fails, otherwise `null`.
     *
     */
    static minLength(minLength) {
        return (/**
         * @param {?} control
         * @return {?}
         */
        (control) => {
            if (isEmptyInputValue(control.value)) {
                return null; // don't validate empty values to allow optional controls
            }
            /** @type {?} */
            const length = control.value ? control.value.length : 0;
            return length < minLength ?
                { 'minlength': { 'requiredLength': minLength, 'actualLength': length } } :
                null;
        });
    }
    /**
     * \@description
     * Validator that requires the length of the control's value to be less than or equal
     * to the provided maximum length. This validator is also provided by default if you use the
     * the HTML5 `maxlength` attribute.
     *
     * \@usageNotes
     *
     * ### Validate that the field has maximum of 5 characters
     *
     * ```typescript
     * const control = new FormControl('Angular', Validators.maxLength(5));
     *
     * console.log(control.errors); // {maxlength: {requiredLength: 5, actualLength: 7}}
     * ```
     *
     * ```html
     * <input maxlength="5">
     * ```
     *
     * @see `updateValueAndValidity()`
     *
     * @param {?} maxLength
     * @return {?} A validator function that returns an error map with the
     * `maxlength` property if the validation check fails, otherwise `null`.
     *
     */
    static maxLength(maxLength) {
        return (/**
         * @param {?} control
         * @return {?}
         */
        (control) => {
            /** @type {?} */
            const length = control.value ? control.value.length : 0;
            return length > maxLength ?
                { 'maxlength': { 'requiredLength': maxLength, 'actualLength': length } } :
                null;
        });
    }
    /**
     * \@description
     * Validator that requires the control's value to match a regex pattern. This validator is also
     * provided by default if you use the HTML5 `pattern` attribute.
     *
     * \@usageNotes
     *
     * ### Validate that the field only contains letters or spaces
     *
     * ```typescript
     * const control = new FormControl('1', Validators.pattern('[a-zA-Z ]*'));
     *
     * console.log(control.errors); // {pattern: {requiredPattern: '^[a-zA-Z ]*$', actualValue: '1'}}
     * ```
     *
     * ```html
     * <input pattern="[a-zA-Z ]*">
     * ```
     *
     * @see `updateValueAndValidity()`
     *
     * @param {?} pattern A regular expression to be used as is to test the values, or a string.
     * If a string is passed, the `^` character is prepended and the `$` character is
     * appended to the provided string (if not already present), and the resulting regular
     * expression is used to test the values.
     *
     * @return {?} A validator function that returns an error map with the
     * `pattern` property if the validation check fails, otherwise `null`.
     *
     */
    static pattern(pattern) {
        if (!pattern)
            return Validators.nullValidator;
        /** @type {?} */
        let regex;
        /** @type {?} */
        let regexStr;
        if (typeof pattern === 'string') {
            regexStr = '';
            if (pattern.charAt(0) !== '^')
                regexStr += '^';
            regexStr += pattern;
            if (pattern.charAt(pattern.length - 1) !== '$')
                regexStr += '$';
            regex = new RegExp(regexStr);
        }
        else {
            regexStr = pattern.toString();
            regex = pattern;
        }
        return (/**
         * @param {?} control
         * @return {?}
         */
        (control) => {
            if (isEmptyInputValue(control.value)) {
                return null; // don't validate empty values to allow optional controls
            }
            /** @type {?} */
            const value = control.value;
            return regex.test(value) ? null :
                { 'pattern': { 'requiredPattern': regexStr, 'actualValue': value } };
        });
    }
    /**
     * \@description
     * Validator that performs no operation.
     *
     * @see `updateValueAndValidity()`
     *
     * @param {?} control
     * @return {?}
     */
    static nullValidator(control) { return null; }
    /**
     * @param {?} validators
     * @return {?}
     */
    static compose(validators) {
        if (!validators)
            return null;
        /** @type {?} */
        const presentValidators = (/** @type {?} */ (validators.filter(isPresent)));
        if (presentValidators.length == 0)
            return null;
        return (/**
         * @param {?} control
         * @return {?}
         */
        function (control) {
            return _mergeErrors(_executeValidators(control, presentValidators));
        });
    }
    /**
     * \@description
     * Compose multiple async validators into a single function that returns the union
     * of the individual error objects for the provided control.
     *
     * @see `updateValueAndValidity()`
     *
     * @param {?} validators
     * @return {?} A validator function that returns an error map with the
     * merged error objects of the async validators if the validation check fails, otherwise `null`.
     *
     */
    static composeAsync(validators) {
        if (!validators)
            return null;
        /** @type {?} */
        const presentValidators = (/** @type {?} */ (validators.filter(isPresent)));
        if (presentValidators.length == 0)
            return null;
        return (/**
         * @param {?} control
         * @return {?}
         */
        function (control) {
            /** @type {?} */
            const observables = _executeAsyncValidators(control, presentValidators).map(toObservable);
            return forkJoin(observables).pipe(map(_mergeErrors));
        });
    }
}
/**
 * @param {?} o
 * @return {?}
 */
function isPresent(o) {
    return o != null;
}
/**
 * @param {?} r
 * @return {?}
 */
export function toObservable(r) {
    /** @type {?} */
    const obs = isPromise(r) ? from(r) : r;
    if (!(isObservable(obs))) {
        throw new Error(`Expected validator to return Promise or Observable.`);
    }
    return obs;
}
/**
 * @param {?} control
 * @param {?} validators
 * @return {?}
 */
function _executeValidators(control, validators) {
    return validators.map((/**
     * @param {?} v
     * @return {?}
     */
    v => v(control)));
}
/**
 * @param {?} control
 * @param {?} validators
 * @return {?}
 */
function _executeAsyncValidators(control, validators) {
    return validators.map((/**
     * @param {?} v
     * @return {?}
     */
    v => v(control)));
}
/**
 * @param {?} arrayOfErrors
 * @return {?}
 */
function _mergeErrors(arrayOfErrors) {
    /** @type {?} */
    const res = arrayOfErrors.reduce((/**
     * @param {?} res
     * @param {?} errors
     * @return {?}
     */
    (res, errors) => {
        return errors != null ? Object.assign({}, (/** @type {?} */ (res)), errors) : (/** @type {?} */ (res));
    }), {});
    return Object.keys(res).length === 0 ? null : res;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidmFsaWRhdG9ycy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2Zvcm1zL3NyYy92YWxpZGF0b3JzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLGNBQWMsRUFBRSxhQUFhLElBQUksWUFBWSxFQUFFLFVBQVUsSUFBSSxTQUFTLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFDckcsT0FBTyxFQUFhLFFBQVEsRUFBRSxJQUFJLEVBQUMsTUFBTSxNQUFNLENBQUM7QUFDaEQsT0FBTyxFQUFDLEdBQUcsRUFBQyxNQUFNLGdCQUFnQixDQUFDOzs7OztBQUluQyxTQUFTLGlCQUFpQixDQUFDLEtBQVU7SUFDbkMsOERBQThEO0lBQzlELE9BQU8sS0FBSyxJQUFJLElBQUksSUFBSSxLQUFLLENBQUMsTUFBTSxLQUFLLENBQUMsQ0FBQztBQUM3QyxDQUFDOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQTZCRCxNQUFNLE9BQU8sYUFBYSxHQUFHLElBQUksY0FBYyxDQUE0QixjQUFjLENBQUM7Ozs7Ozs7Ozs7QUFVMUYsTUFBTSxPQUFPLG1CQUFtQixHQUM1QixJQUFJLGNBQWMsQ0FBNEIsbUJBQW1CLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O01BZ0NoRSxZQUFZLEdBQ2Qsb01BQW9NOzs7Ozs7Ozs7Ozs7QUFheE0sTUFBTSxPQUFPLFVBQVU7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0lBc0JyQixNQUFNLENBQUMsR0FBRyxDQUFDLEdBQVc7UUFDcEI7Ozs7UUFBTyxDQUFDLE9BQXdCLEVBQTJCLEVBQUU7WUFDM0QsSUFBSSxpQkFBaUIsQ0FBQyxPQUFPLENBQUMsS0FBSyxDQUFDLElBQUksaUJBQWlCLENBQUMsR0FBRyxDQUFDLEVBQUU7Z0JBQzlELE9BQU8sSUFBSSxDQUFDLENBQUUseURBQXlEO2FBQ3hFOztrQkFDSyxLQUFLLEdBQUcsVUFB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TUFBTSxJQUFJLElBQUksQ0FBQyxDQUFDLG1CQUFLLG1CQUFBLEdBQUcsRUFBRSxFQUFLLE1BQU0sRUFBRSxDQUFDLENBQUMsbUJBQUEsR0FBRyxFQUFFLENBQUM7SUFDeEQsQ0FBQyxHQUFFLEVBQUUsQ0FBQztJQUNWLE9BQU8sTUFBTSxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQyxNQUFNLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLEdBQUcsQ0FBQztBQUNwRCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0luamVjdGlvblRva2VuLCDJtWlzT2JzZXJ2YWJsZSBhcyBpc09ic2VydmFibGUsIMm1aXNQcm9taXNlIGFzIGlzUHJvbWlzZX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQge09ic2VydmFibGUsIGZvcmtKb2luLCBmcm9tfSBmcm9tICdyeGpzJztcbmltcG9ydCB7bWFwfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5pbXBvcnQge0FzeW5jVmFsaWRhdG9yRm4sIFZhbGlkYXRpb25FcnJvcnMsIFZhbGlkYXRvciwgVmFsaWRhdG9yRm59IGZyb20gJy4vZGlyZWN0aXZlcy92YWxpZGF0b3JzJztcbmltcG9ydCB7QWJzdHJhY3RDb250cm9sLCBGb3JtQ29udHJvbH0gZnJvbSAnLi9tb2RlbCc7XG5cbmZ1bmN0aW9uIGlzRW1wdHlJbnB1dFZhbHVlKHZhbHVlOiBhbnkpOiBib29sZWFuIHtcbiAgLy8gd2UgZG9uJ3QgY2hlY2sgZm9yIHN0cmluZyBoZXJlIHNvIGl0IGFsc28gd29ya3Mgd2l0aCBhcnJheXNcbiAgcmV0dXJuIHZhbHVlID09IG51bGwgfHwgdmFsdWUubGVuZ3RoID09PSAwO1xufVxuXG4vKipcbiAqIEBkZXNjcmlwdGlvblxuICogQW4gYEluamVjdGlvblRva2VuYCBmb3IgcmVnaXN0ZXJpbmcgYWRkaXRpb25hbCBzeW5jaHJvbm91cyB2YWxpZGF0b3JzIHVzZWQgd2l0aCBgQWJzdHJhY3RDb250cm9sYHMuXG4gKlxuICogQHNlZSBgTkdfQVNZTkNfVkFMSURBVE9SU2BcbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICpcbiAqICMjIyBQcm92aWRpbmcgYSBjdXN0b20gdmFsaWRhdG9yXG4gKlxuICogVGhlIGZvbGxvd2luZyBleGFtcGxlIHJlZ2lzdGVycyBhIGN1c3RvbSB2YWxpZGF0b3IgZGlyZWN0aXZlLiBBZGRpbmcgdGhlIHZhbGlkYXRvciB0byB0aGVcbiAqIGV4aXN0aW5nIGNvbGxlY3Rpb24gb2YgdmFsaWRhdG9ycyByZXF1aXJlcyB0aGUgYG11bHRpOiB0cnVlYCBvcHRpb24uXG4gKlxuICogYGBgdHlwZXNjcmlwdFxuICogQERpcmVjdGl2ZSh7XG4gKiAgIHNlbGVjdG9yOiAnW2N1c3RvbVZhbGlkYXRvcl0nLFxuICogICBwcm92aWRlcnM6IFt7cHJvdmlkZTogTkdfVkFMSURBVE9SUywgdXNlRXhpc3Rpbmc6IEN1c3RvbVZhbGlkYXRvckRpcmVjdGl2ZSwgbXVsdGk6IHRydWV9XVxuICogfSlcbiAqIGNsYXNzIEN1c3RvbVZhbGlkYXRvckRpcmVjdGl2ZSBpbXBsZW1lbnRzIFZhbGlkYXRvciB7XG4gKiAgIHZhbGlkYXRlKGNvbnRyb2w6IEFic3RyYWN0Q29udHJvbCk6IFZhbGlkYXRpb25FcnJvcnMgfCBudWxsIHtcbiAqICAgICByZXR1cm4geyAnY3VzdG9tJzogdHJ1ZSB9O1xuICogICB9XG4gKiB9XG4gKiBgYGBcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjb25zdCBOR19WQUxJREFUT1JTID0gbmV3IEluamVjdGlvblRva2VuPEFycmF5PFZhbGlkYXRvcnxGdW5jdGlvbj4+KCdOZ1ZhbGlkYXRvcnMnKTtcblxuLyoqXG4gKiBAZGVzY3JpcHRpb25cbiAqIEFuIGBJbmplY3Rpb25Ub2tlbmAgZm9yIHJlZ2lzdGVyaW5nIGFkZGl0aW9uYWwgYXN5bmNocm9ub3VzIHZhbGlkYXRvcnMgdXNlZCB3aXRoIGBBYnN0cmFjdENvbnRyb2xgcy5cbiAqXG4gKiBAc2VlIGBOR19WQUxJREFUT1JTYFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNvbnN0IE5HX0FTWU5DX1ZBTElEQVRPUlMgPVxuICAgIG5ldyBJbmplY3Rpb25Ub2tlbjxBcnJheTxWYWxpZGF0b3J8RnVuY3Rpb24+PignTmdBc3luY1ZhbGlkYXRvcnMnKTtcblxuLyoqXG4gKiBBIHJlZ3VsYXIgZXhwcmVzc2lvbiB0aGF0IG1hdGNoZXMgdmFsaWQgZS1tYWlsIGFkZHJlc3Nlcy5cbiAqXG4gKiBBdCBhIGhpZ2ggbGV2ZWwsIHRoaXMgcmVnZXhwIG1hdGNoZXMgZS1tYWlsIGFkZHJlc3NlcyBvZiB0aGUgZm9ybWF0IGBsb2NhbC1wYXJ0QHRsZGAsIHdoZXJlOlxuICogLSBgbG9jYWwtcGFydGAgY29uc2lzdHMgb2Ygb25lIG9yIG1vcmUgb2YgdGhlIGFsbG93ZWQgY2hhcmFjdGVycyAoYWxwaGFudW1lcmljIGFuZCBzb21lXG4gKiAgIHB1bmN0dWF0aW9uIHN5bWJvbHMpLlxuICogLSBgbG9jYWwtcGFydGAgY2Fubm90IGJlZ2luIG9yIGVuZCB3aXRoIGEgcGVyaW9kIChgLmApLlxuICogLSBgbG9jYWwtcGFydGAgY2Fubm90IGJlIGxvbmdlciB0aGFuIDY0IGNoYXJhY3RlcnMuXG4gKiAtIGB0bGRgIGNvbnNpc3RzIG9mIG9uZSBvciBtb3JlIGBsYWJlbHNgIHNlcGFyYXRlZCBieSBwZXJpb2RzIChgLmApLiBGb3IgZXhhbXBsZSBgbG9jYWxob3N0YCBvclxuICogICBgZm9vLmNvbWAuXG4gKiAtIEEgYGxhYmVsYCBjb25zaXN0cyBvZiBvbmUgb3IgbW9yZSBvZiB0aGUgYWxsb3dlZCBjaGFyYWN0ZXJzIChhbHBoYW51bWVyaWMsIGRhc2hlcyAoYC1gKSBhbmRcbiAqICAgcGVyaW9kcyAoYC5gKSkuXG4gKiAtIEEgYGxhYmVsYCBjYW5ub3QgYmVnaW4gb3IgZW5kIHdpdGggYSBkYXNoIChgLWApIG9yIGEgcGVyaW9kIChgLmApLlxuICogLSBBIGBsYWJlbGAgY2Fubm90IGJlIGxvbmdlciB0aGFuIDYzIGNoYXJhY3RlcnMuXG4gKiAtIFRoZSB3aG9sZSBhZGRyZXNzIGNhbm5vdCBiZSBsb25nZXIgdGhhbiAyNTQgY2hhcmFjdGVycy5cbiAqXG4gKiAjIyBJbXBsZW1lbnRhdGlvbiBiYWNrZ3JvdW5kXG4gKlxuICogVGhpcyByZWdleHAgd2FzIHBvcnRlZCBvdmVyIGZyb20gQW5ndWxhckpTIChzZWUgdGhlcmUgZm9yIGdpdCBoaXN0b3J5KTpcbiAqIGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL2FuZ3VsYXIuanMvYmxvYi9jMTMzZWY4MzYvc3JjL25nL2RpcmVjdGl2ZS9pbnB1dC5qcyNMMjdcbiAqIEl0IGlzIGJhc2VkIG9uIHRoZVxuICogW1dIQVRXRyB2ZXJzaW9uXShodHRwczovL2h0bWwuc3BlYy53aGF0d2cub3JnL211bHRpcGFnZS9pbnB1dC5odG1sI3ZhbGlkLWUtbWFpbC1hZGRyZXNzKSB3aXRoXG4gKiBzb21lIGVuaGFuY2VtZW50cyB0byBpbmNvcnBvcmF0ZSBtb3JlIFJGQyBydWxlcyAoc3VjaCBhcyBydWxlcyByZWxhdGVkIHRvIGRvbWFpbiBuYW1lcyBhbmQgdGhlXG4gKiBsZW5ndGhzIG9mIGRpZmZlcmVudCBwYXJ0cyBvZiB0aGUgYWRkcmVzcykuIFRoZSBtYWluIGRpZmZlcmVuY2VzIGZyb20gdGhlIFdIQVRXRyB2ZXJzaW9uIGFyZTpcbiAqICAgLSBEaXNhbGxvdyBgbG9jYWwtcGFydGAgdG8gYmVnaW4gb3IgZW5kIHdpdGggYSBwZXJpb2QgKGAuYCkuXG4gKiAgIC0gRGlzYWxsb3cgYGxvY2FsLXBhcnRgIGxlbmd0aCB0byBleGNlZWQgNjQgY2hhcmFjdGVycy5cbiAqICAgLSBEaXNhbGxvdyB0b3RhbCBhZGRyZXNzIGxlbmd0aCB0byBleGNlZWQgMjU0IGNoYXJhY3RlcnMuXG4gKlxuICogU2VlIFt0aGlzIGNvbW1pdF0oaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvYW5ndWxhci5qcy9jb21taXQvZjNmNWNmNzJlKSBmb3IgbW9yZSBkZXRhaWxzLlxuICovXG5jb25zdCBFTUFJTF9SRUdFWFAgPVxuICAgIC9eKD89LnsxLDI1NH0kKSg/PS57MSw2NH1AKVthLXpBLVowLTkhIyQlJicqKy89P15fYHt8fX4tXSsoPzpcXC5bYS16QS1aMC05ISMkJSYnKisvPT9eX2B7fH1+LV0rKSpAW2EtekEtWjAtOV0oPzpbYS16QS1aMC05LV17MCw2MX1bYS16QS1aMC05XSk/KD86XFwuW2EtekEtWjAtOV0oPzpbYS16QS1aMC05LV17MCw2MX1bYS16QS1aMC05XSk/KSokLztcblxuLyoqXG4gKiBAZGVzY3JpcHRpb25cbiAqIFByb3ZpZGVzIGEgc2V0IG9mIGJ1aWx0LWluIHZhbGlkYXRvcnMgdGhhdCBjYW4gYmUgdXNlZCBieSBmb3JtIGNvbnRyb2xzLlxuICpcbiAqIEEgdmFsaWRhdG9yIGlzIGEgZnVuY3Rpb24gdGhhdCBwcm9jZXNzZXMgYSBgRm9ybUNvbnRyb2xgIG9yIGNvbGxlY3Rpb24gb2ZcbiAqIGNvbnRyb2xzIGFuZCByZXR1cm5zIGFuIGVycm9yIG1hcCBvciBudWxsLiBBIG51bGwgbWFwIG1lYW5zIHRoYXQgdmFsaWRhdGlvbiBoYXMgcGFzc2VkLlxuICpcbiAqIEBzZWUgW0Zvcm0gVmFsaWRhdGlvbl0oL2d1aWRlL2Zvcm0tdmFsaWRhdGlvbilcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBWYWxpZGF0b3JzIHtcbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBWYWxpZGF0b3IgdGhhdCByZXF1aXJlcyB0aGUgY29udHJvbCdzIHZhbHVlIHRvIGJlIGdyZWF0ZXIgdGhhbiBvciBlcXVhbCB0byB0aGUgcHJvdmlkZWQgbnVtYmVyLlxuICAgKiBUaGUgdmFsaWRhdG9yIGV4aXN0cyBvbmx5IGFzIGEgZnVuY3Rpb24gYW5kIG5vdCBhcyBhIGRpcmVjdGl2ZS5cbiAgICpcbiAgICogQHVzYWdlTm90ZXNcbiAgICpcbiAgICogIyMjIFZhbGlkYXRlIGFnYWluc3QgYSBtaW5pbXVtIG9mIDNcbiAgICpcbiAgICogYGBgdHlwZXNjcmlwdFxuICAgKiBjb25zdCBjb250cm9sID0gbmV3IEZvcm1Db250cm9sKDIsIFZhbGlkYXRvcnMubWluKDMpKTtcbiAgICpcbiAgICogY29uc29sZS5sb2coY29udHJvbC5lcnJvcnMpOyAvLyB7bWluOiB7bWluOiAzLCBhY3R1YWw6IDJ9fVxuICAgKiBgYGBcbiAgICpcbiAgICogQHJldHVybnMgQSB2YWxpZGF0b3IgZnVuY3Rpb24gdGhhdCByZXR1cm5zIGFuIGVycm9yIG1hcCB3aXRoIHRoZVxuICAgKiBgbWluYCBwcm9wZXJ0eSBpZiB0aGUgdmFsaWRhdGlvbiBjaGVjayBmYWlscywgb3RoZXJ3aXNlIGBudWxsYC5cbiAgICpcbiAgICogQHNlZSBgdXBkYXRlVmFsdWVBbmRWYWxpZGl0eSgpYFxuICAgKlxuICAgKi9cbiAgc3RhdGljIG1pbihtaW46IG51bWJlcik6IFZhbGlkYXRvckZuIHtcbiAgICByZXR1cm4gKGNvbnRyb2w6IEFic3RyYWN0Q29udHJvbCk6IFZhbGlkYXRpb25FcnJvcnMgfCBudWxsID0+IHtcbiAgICAgIGlmIChpc0VtcHR5SW5wdXRWYWx1ZShjb250cm9sLnZhbHVlKSB8fCBpc0VtcHR5SW5wdXRWYWx1ZShtaW4pKSB7XG4gICAgICAgIHJldHVybiBudWxsOyAgLy8gZG9uJ3QgdmFsaWRhdGUgZW1wdHkgdmFsdWVzIHRvIGFsbG93IG9wdGlvbmFsIGNvbnRyb2xzXG4gICAgICB9XG4gICAgICBjb25zdCB2YWx1ZSA9IHBhcnNlRmxvYXQoY29udHJvbC52YWx1ZSk7XG4gICAgICAvLyBDb250cm9scyB3aXRoIE5hTiB2YWx1ZXMgYWZ0ZXIgcGFyc2luZyBzaG91bGQgYmUgdHJlYXRlZCBhcyBub3QgaGF2aW5nIGFcbiAgICAgIC8vIG1pbmltdW0sIHBlciB0aGUgSFRNTCBmb3JtcyBzcGVjOiBodHRwczovL3d3dy53My5vcmcvVFIvaHRtbDUvZm9ybXMuaHRtbCNhdHRyLWlucHV0LW1pblxuICAgICAgcmV0dXJuICFpc05hTih2YWx1ZSkgJiYgdmFsdWUgPCBtaW4gPyB7J21pbic6IHsnbWluJzogbWluLCAnYWN0dWFsJzogY29udHJvbC52YWx1ZX19IDogbnVsbDtcbiAgICB9O1xuICB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBWYWxpZGF0b3IgdGhhdCByZXF1aXJlcyB0aGUgY29udHJvbCdzIHZhbHVlIHRvIGJlIGxlc3MgdGhhbiBvciBlcXVhbCB0byB0aGUgcHJvdmlkZWQgbnVtYmVyLlxuICAgKiBUaGUgdmFsaWRhdG9yIGV4aXN0cyBvbmx5IGFzIGEgZnVuY3Rpb24gYW5kIG5vdCBhcyBhIGRpcmVjdGl2ZS5cbiAgICpcbiAgICogQHVzYWdlTm90ZXNcbiAgICpcbiAgICogIyMjIFZhbGlkYXRlIGFnYWluc3QgYSBtYXhpbXVtIG9mIDE1XG4gICAqXG4gICAqIGBgYHR5cGVzY3JpcHRcbiAgICogY29uc3QgY29udHJvbCA9IG5ldyBGb3JtQ29udHJvbCgxNiwgVmFsaWRhdG9ycy5tYXgoMTUpKTtcbiAgICpcbiAgICogY29uc29sZS5sb2coY29udHJvbC5lcnJvcnMpOyAvLyB7bWF4OiB7bWF4OiAxNSwgYWN0dWFsOiAxNn19XG4gICAqIGBgYFxuICAgKlxuICAgKiBAcmV0dXJucyBBIHZhbGlkYXRvciBmdW5jdGlvbiB0aGF0IHJldHVybnMgYW4gZXJyb3IgbWFwIHdpdGggdGhlXG4gICAqIGBtYXhgIHByb3BlcnR5IGlmIHRoZSB2YWxpZGF0aW9uIGNoZWNrIGZhaWxzLCBvdGhlcndpc2UgYG51bGxgLlxuICAgKlxuICAgKiBAc2VlIGB1cGRhdGVWYWx1ZUFuZFZhbGlkaXR5KClgXG4gICAqXG4gICAqL1xuICBzdGF0aWMgbWF4KG1heDogbnVtYmVyKTogVmFsaWRhdG9yRm4ge1xuICAgIHJldHVybiAoY29udHJvbDogQWJzdHJhY3RDb250cm9sKTogVmFsaWRhdGlvbkVycm9ycyB8IG51bGwgPT4ge1xuICAgICAgaWYgKGlzRW1wdHlJbnB1dFZhbHVlKGNvbnRyb2wudmFsdWUpIHx8IGlzRW1wdHlJbnB1dFZhbHVlKG1heCkpIHtcbiAgICAgICAgcmV0dXJuIG51bGw7ICAvLyBkb24ndCB2YWxpZGF0ZSBlbXB0eSB2YWx1ZXMgdG8gYWxsb3cgb3B0aW9uYWwgY29udHJvbHNcbiAgICAgIH1cbiAgICAgIGNvbnN0IHZhbHVlID0gcGFyc2VGbG9hdChjb250cm9sLnZhbHVlKTtcbiAgICAgIC8vIENvbnRyb2xzIHdpdGggTmFOIHZhbHVlcyBhZnRlciBwYXJzaW5nIHNob3VsZCBiZSB0cmVhdGVkIGFzIG5vdCBoYXZpbmcgYVxuICAgICAgLy8gbWF4aW11bSwgcGVyIHRoZSBIVE1MIGZvcm1zIHNwZWM6IGh0dHBzOi8vd3d3LnczLm9yZy9UUi9odG1sNS9mb3Jtcy5odG1sI2F0dHItaW5wdXQtbWF4XG4gICAgICByZXR1cm4gIWlzTmFOKHZhbHVlKSAmJiB2YWx1ZSA+IG1heCA/IHsnbWF4JzogeydtYXgnOiBtYXgsICdhY3R1YWwnOiBjb250cm9sLnZhbHVlfX0gOiBudWxsO1xuICAgIH07XG4gIH1cblxuICAvKipcbiAgICogQGRlc2NyaXB0aW9uXG4gICAqIFZhbGlkYXRvciB0aGF0IHJlcXVpcmVzIHRoZSBjb250cm9sIGhhdmUgYSBub24tZW1wdHkgdmFsdWUuXG4gICAqXG4gICAqIEB1c2FnZU5vdGVzXG4gICAqXG4gICAqICMjIyBWYWxpZGF0ZSB0aGF0IHRoZSBmaWVsZCBpcyBub24tZW1wdHlcbiAgICpcbiAgICogYGBgdHlwZXNjcmlwdFxuICAgKiBjb25zdCBjb250cm9sID0gbmV3IEZvcm1Db250cm9sKCcnLCBWYWxpZGF0b3JzLnJlcXVpcmVkKTtcbiAgICpcbiAgICogY29uc29sZS5sb2coY29udHJvbC5lcnJvcnMpOyAvLyB7cmVxdWlyZWQ6IHRydWV9XG4gICAqIGBgYFxuICAgKlxuICAgKiBAcmV0dXJucyBBbiBlcnJvciBtYXAgd2l0aCB0aGUgYHJlcXVpcmVkYCBwcm9wZXJ0eVxuICAgKiBpZiB0aGUgdmFsaWRhdGlvbiBjaGVjayBmYWlscywgb3RoZXJ3aXNlIGBudWxsYC5cbiAgICpcbiAgICogQHNlZSBgdXBkYXRlVmFsdWVBbmRWYWxpZGl0eSgpYFxuICAgKlxuICAgKi9cbiAgc3RhdGljIHJlcXVpcmVkKGNvbnRyb2w6IEFic3RyYWN0Q29udHJvbCk6IFZhbGlkYXRpb25FcnJvcnN8bnVsbCB7XG4gICAgcmV0dXJuIGlzRW1wdHlJbnB1dFZhbHVlKGNvbnRyb2wudmFsdWUpID8geydyZXF1aXJlZCc6IHRydWV9IDogbnVsbDtcbiAgfVxuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogVmFsaWRhdG9yIHRoYXQgcmVxdWlyZXMgdGhlIGNvbnRyb2wncyB2YWx1ZSBiZSB0cnVlLiBUaGlzIHZhbGlkYXRvciBpcyBjb21tb25seVxuICAgKiB1c2VkIGZvciByZXF1aXJlZCBjaGVja2JveGVzLlxuICAgKlxuICAgKiBAdXNhZ2VOb3Rlc1xuICAgKlxuICAgKiAjIyMgVmFsaWRhdGUgdGhhdCB0aGUgZmllbGQgdmFsdWUgaXMgdHJ1ZVxuICAgKlxuICAgKiBgYGB0eXBlc2NyaXB0XG4gICAqIGNvbnN0IGNvbnRyb2wgPSBuZXcgRm9ybUNvbnRyb2woJycsIFZhbGlkYXRvcnMucmVxdWlyZWRUcnVlKTtcbiAgICpcbiAgICogY29uc29sZS5sb2coY29udHJvbC5lcnJvcnMpOyAvLyB7cmVxdWlyZWQ6IHRydWV9XG4gICAqIGBgYFxuICAgKlxuICAgKiBAcmV0dXJucyBBbiBlcnJvciBtYXAgdGhhdCBjb250YWlucyB0aGUgYHJlcXVpcmVkYCBwcm9wZXJ0eVxuICAgKiBzZXQgdG8gYHRydWVgIGlmIHRoZSB2YWxpZGF0aW9uIGNoZWNrIGZhaWxzLCBvdGhlcndpc2UgYG51bGxgLlxuICAgKlxuICAgKiBAc2VlIGB1cGRhdGVWYWx1ZUFuZFZhbGlkaXR5KClgXG4gICAqXG4gICAqL1xuICBzdGF0aWMgcmVxdWlyZWRUcnVlKGNvbnRyb2w6IEFic3RyYWN0Q29udHJvbCk6IFZhbGlkYXRpb25FcnJvcnN8bnVsbCB7XG4gICAgcmV0dXJuIGNvbnRyb2wudmFsdWUgPT09IHRydWUgPyBudWxsIDogeydyZXF1aXJlZCc6IHRydWV9O1xuICB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBWYWxpZGF0b3IgdGhhdCByZXF1aXJlcyB0aGUgY29udHJvbCdzIHZhbHVlIHBhc3MgYW4gZW1haWwgdmFsaWRhdGlvbiB0ZXN0LlxuICAgKlxuICAgKiBUZXN0cyB0aGUgdmFsdWUgdXNpbmcgYSBbcmVndWxhciBleHByZXNzaW9uXShodHRwczovL2RldmVsb3Blci5tb3ppbGxhLm9yZy9lbi1VUy9kb2NzL1dlYi9KYXZhU2NyaXB0L0d1aWRlL1JlZ3VsYXJfRXhwcmVzc2lvbnMpXG4gICAqIHBhdHRlcm4gc3VpdGFibGUgZm9yIGNvbW1vbiB1c2VjYXNlcy4gVGhlIHBhdHRlcm4gaXMgYmFzZWQgb24gdGhlIGRlZmluaXRpb24gb2YgYSB2YWxpZCBlbWFpbFxuICAgKiBhZGRyZXNzIGluIHRoZSBbV0hBVFdHIEhUTUwgc3BlY2lmaWNhdGlvbl0oaHR0cHM6Ly9odG1sLnNwZWMud2hhdHdnLm9yZy9tdWx0aXBhZ2UvaW5wdXQuaHRtbCN2YWxpZC1lLW1haWwtYWRkcmVzcylcbiAgICogd2l0aCBzb21lIGVuaGFuY2VtZW50cyB0byBpbmNvcnBvcmF0ZSBtb3JlIFJGQyBydWxlcyAoc3VjaCBhcyBydWxlcyByZWxhdGVkIHRvIGRvbWFpbiBuYW1lcyBhbmRcbiAgICogdGhlIGxlbmd0aHMgb2YgZGlmZmVyZW50IHBhcnRzIG9mIHRoZSBhZGRyZXNzKS5cbiAgICpcbiAgICogVGhlIGRpZmZlcmVuY2VzIGZyb20gdGhlIFdIQVRXRyB2ZXJzaW9uIGluY2x1ZGU6XG4gICAqIC0gRGlzYWxsb3cgYGxvY2FsLXBhcnRgICh0aGUgcGFydCBiZWZvcmUgdGhlIGBAYCBzeW1ib2wpIHRvIGJlZ2luIG9yIGVuZCB3aXRoIGEgcGVyaW9kIChgLmApLlxuICAgKiAtIERpc2FsbG93IGBsb2NhbC1wYXJ0YCB0byBiZSBsb25nZXIgdGhhbiA2NCBjaGFyYWN0ZXJzLlxuICAgKiAtIERpc2FsbG93IHRoZSB3aG9sZSBhZGRyZXNzIHRvIGJlIGxvbmdlciB0aGFuIDI1NCBjaGFyYWN0ZXJzLlxuICAgKlxuICAgKiBJZiB0aGlzIHBhdHRlcm4gZG9lcyBub3Qgc2F0aXNmeSB5b3VyIGJ1c2luZXNzIG5lZWRzLCB5b3UgY2FuIHVzZSBgVmFsaWRhdG9ycy5wYXR0ZXJuKClgIHRvXG4gICAqIHZhbGlkYXRlIHRoZSB2YWx1ZSBhZ2FpbnN0IGEgZGlmZmVyZW50IHBhdHRlcm4uXG4gICAqXG4gICAqIEB1c2FnZU5vdGVzXG4gICAqXG4gICAqICMjIyBWYWxpZGF0ZSB0aGF0IHRoZSBmaWVsZCBtYXRjaGVzIGEgdmFsaWQgZW1haWwgcGF0dGVyblxuICAgKlxuICAgKiBgYGB0eXBlc2NyaXB0XG4gICAqIGNvbnN0IGNvbnRyb2wgPSBuZXcgRm9ybUNvbnRyb2woJ2JhZEAnLCBWYWxpZGF0b3JzLmVtYWlsKTtcbiAgICpcbiAgICogY29uc29sZS5sb2coY29udHJvbC5lcnJvcnMpOyAvLyB7ZW1haWw6IHRydWV9XG4gICAqIGBgYFxuICAgKlxuICAgKiBAcmV0dXJucyBBbiBlcnJvciBtYXAgd2l0aCB0aGUgYGVtYWlsYCBwcm9wZXJ0eVxuICAgKiBpZiB0aGUgdmFsaWRhdGlvbiBjaGVjayBmYWlscywgb3RoZXJ3aXNlIGBudWxsYC5cbiAgICpcbiAgICogQHNlZSBgdXBkYXRlVmFsdWVBbmRWYWxpZGl0eSgpYFxuICAgKlxuICAgKi9cbiAgc3RhdGljIGVtYWlsKGNvbnRyb2w6IEFic3RyYWN0Q29udHJvbCk6IFZhbGlkYXRpb25FcnJvcnN8bnVsbCB7XG4gICAgaWYgKGlzRW1wdHlJbnB1dFZhbHVlKGNvbnRyb2wudmFsdWUpKSB7XG4gICAgICByZXR1cm4gbnVsbDsgIC8vIGRvbid0IHZhbGlkYXRlIGVtcHR5IHZhbHVlcyB0byBhbGxvdyBvcHRpb25hbCBjb250cm9sc1xuICAgIH1cbiAgICByZXR1cm4gRU1BSUxfUkVHRVhQLnRlc3QoY29udHJvbC52YWx1ZSkgPyBudWxsIDogeydlbWFpbCc6IHRydWV9O1xuICB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBWYWxpZGF0b3IgdGhhdCByZXF1aXJlcyB0aGUgbGVuZ3RoIG9mIHRoZSBjb250cm9sJ3MgdmFsdWUgdG8gYmUgZ3JlYXRlciB0aGFuIG9yIGVxdWFsXG4gICAqIHRvIHRoZSBwcm92aWRlZCBtaW5pbXVtIGxlbmd0aC4gVGhpcyB2YWxpZGF0b3IgaXMgYWxzbyBwcm92aWRlZCBieSBkZWZhdWx0IGlmIHlvdSB1c2UgdGhlXG4gICAqIHRoZSBIVE1MNSBgbWlubGVuZ3RoYCBhdHRyaWJ1dGUuXG4gICAqXG4gICAqIEB1c2FnZU5vdGVzXG4gICAqXG4gICAqICMjIyBWYWxpZGF0ZSB0aGF0IHRoZSBmaWVsZCBoYXMgYSBtaW5pbXVtIG9mIDMgY2hhcmFjdGVyc1xuICAgKlxuICAgKiBgYGB0eXBlc2NyaXB0XG4gICAqIGNvbnN0IGNvbnRyb2wgPSBuZXcgRm9ybUNvbnRyb2woJ25nJywgVmFsaWRhdG9ycy5taW5MZW5ndGgoMykpO1xuICAgKlxuICAgKiBjb25zb2xlLmxvZyhjb250cm9sLmVycm9ycyk7IC8vIHttaW5sZW5ndGg6IHtyZXF1aXJlZExlbmd0aDogMywgYWN0dWFsTGVuZ3RoOiAyfX1cbiAgICogYGBgXG4gICAqXG4gICAqIGBgYGh0bWxcbiAgICogPGlucHV0IG1pbmxlbmd0aD1cIjVcIj5cbiAgICogYGBgXG4gICAqXG4gICAqIEByZXR1cm5zIEEgdmFsaWRhdG9yIGZ1bmN0aW9uIHRoYXQgcmV0dXJucyBhbiBlcnJvciBtYXAgd2l0aCB0aGVcbiAgICogYG1pbmxlbmd0aGAgaWYgdGhlIHZhbGlkYXRpb24gY2hlY2sgZmFpbHMsIG90aGVyd2lzZSBgbnVsbGAuXG4gICAqXG4gICAqIEBzZWUgYHVwZGF0ZVZhbHVlQW5kVmFsaWRpdHkoKWBcbiAgICpcbiAgICovXG4gIHN0YXRpYyBtaW5MZW5ndGgobWluTGVuZ3RoOiBudW1iZXIpOiBWYWxpZGF0b3JGbiB7XG4gICAgcmV0dXJuIChjb250cm9sOiBBYnN0cmFjdENvbnRyb2wpOiBWYWxpZGF0aW9uRXJyb3JzIHwgbnVsbCA9PiB7XG4gICAgICBpZiAoaXNFbXB0eUlucHV0VmFsdWUoY29udHJvbC52YWx1ZSkpIHtcbiAgICAgICAgcmV0dXJuIG51bGw7ICAvLyBkb24ndCB2YWxpZGF0ZSBlbXB0eSB2YWx1ZXMgdG8gYWxsb3cgb3B0aW9uYWwgY29udHJvbHNcbiAgICAgIH1cbiAgICAgIGNvbnN0IGxlbmd0aDogbnVtYmVyID0gY29udHJvbC52YWx1ZSA/IGNvbnRyb2wudmFsdWUubGVuZ3RoIDogMDtcbiAgICAgIHJldHVybiBsZW5ndGggPCBtaW5MZW5ndGggP1xuICAgICAgICAgIHsnbWlubGVuZ3RoJzogeydyZXF1aXJlZExlbmd0aCc6IG1pbkxlbmd0aCwgJ2FjdHVhbExlbmd0aCc6IGxlbmd0aH19IDpcbiAgICAgICAgICBudWxsO1xuICAgIH07XG4gIH1cblxuICAvKipcbiAgICogQGRlc2NyaXB0aW9uXG4gICAqIFZhbGlkYXRvciB0aGF0IHJlcXVpcmVzIHRoZSBsZW5ndGggb2YgdGhlIGNvbnRyb2wncyB2YWx1ZSB0byBiZSBsZXNzIHRoYW4gb3IgZXF1YWxcbiAgICogdG8gdGhlIHByb3ZpZGVkIG1heGltdW0gbGVuZ3RoLiBUaGlzIHZhbGlkYXRvciBpcyBhbHNvIHByb3ZpZGVkIGJ5IGRlZmF1bHQgaWYgeW91IHVzZSB0aGVcbiAgICogdGhlIEhUTUw1IGBtYXhsZW5ndGhgIGF0dHJpYnV0ZS5cbiAgICpcbiAgICogQHVzYWdlTm90ZXNcbiAgICpcbiAgICogIyMjIFZhbGlkYXRlIHRoYXQgdGhlIGZpZWxkIGhhcyBtYXhpbXVtIG9mIDUgY2hhcmFjdGVyc1xuICAgKlxuICAgKiBgYGB0eXBlc2NyaXB0XG4gICAqIGNvbnN0IGNvbnRyb2wgPSBuZXcgRm9ybUNvbnRyb2woJ0FuZ3VsYXInLCBWYWxpZGF0b3JzLm1heExlbmd0aCg1KSk7XG4gICAqXG4gICAqIGNvbnNvbGUubG9nKGNvbnRyb2wuZXJyb3JzKTsgLy8ge21heGxlbmd0aDoge3JlcXVpcmVkTGVuZ3RoOiA1LCBhY3R1YWxMZW5ndGg6IDd9fVxuICAgKiBgYGBcbiAgICpcbiAgICogYGBgaHRtbFxuICAgKiA8aW5wdXQgbWF4bGVuZ3RoPVwiNVwiPlxuICAgKiBgYGBcbiAgICpcbiAgICogQHJldHVybnMgQSB2YWxpZGF0b3IgZnVuY3Rpb24gdGhhdCByZXR1cm5zIGFuIGVycm9yIG1hcCB3aXRoIHRoZVxuICAgKiBgbWF4bGVuZ3RoYCBwcm9wZXJ0eSBpZiB0aGUgdmFsaWRhdGlvbiBjaGVjayBmYWlscywgb3RoZXJ3aXNlIGBudWxsYC5cbiAgICpcbiAgICogQHNlZSBgdXBkYXRlVmFsdWVBbmRWYWxpZGl0eSgpYFxuICAgKlxuICAgKi9cbiAgc3RhdGljIG1heExlbmd0aChtYXhMZW5ndGg6IG51bWJlcik6IFZhbGlkYXRvckZuIHtcbiAgICByZXR1cm4gKGNvbnRyb2w6IEFic3RyYWN0Q29udHJvbCk6IFZhbGlkYXRpb25FcnJvcnMgfCBudWxsID0+IHtcbiAgICAgIGNvbnN0IGxlbmd0aDogbnVtYmVyID0gY29udHJvbC52YWx1ZSA/IGNvbnRyb2wudmFsdWUubGVuZ3RoIDogMDtcbiAgICAgIHJldHVybiBsZW5ndGggPiBtYXhMZW5ndGggP1xuICAgICAgICAgIHsnbWF4bGVuZ3RoJzogeydyZXF1aXJlZExlbmd0aCc6IG1heExlbmd0aCwgJ2FjdHVhbExlbmd0aCc6IGxlbmd0aH19IDpcbiAgICAgICAgICBudWxsO1xuICAgIH07XG4gIH1cblxuICAvKipcbiAgICogQGRlc2NyaXB0aW9uXG4gICAqIFZhbGlkYXRvciB0aGF0IHJlcXVpcmVzIHRoZSBjb250cm9sJ3MgdmFsdWUgdG8gbWF0Y2ggYSByZWdleCBwYXR0ZXJuLiBUaGlzIHZhbGlkYXRvciBpcyBhbHNvXG4gICAqIHByb3ZpZGVkIGJ5IGRlZmF1bHQgaWYgeW91IHVzZSB0aGUgSFRNTDUgYHBhdHRlcm5gIGF0dHJpYnV0ZS5cbiAgICpcbiAgICogQHVzYWdlTm90ZXNcbiAgICpcbiAgICogIyMjIFZhbGlkYXRlIHRoYXQgdGhlIGZpZWxkIG9ubHkgY29udGFpbnMgbGV0dGVycyBvciBzcGFjZXNcbiAgICpcbiAgICogYGBgdHlwZXNjcmlwdFxuICAgKiBjb25zdCBjb250cm9sID0gbmV3IEZvcm1Db250cm9sKCcxJywgVmFsaWRhdG9ycy5wYXR0ZXJuKCdbYS16QS1aIF0qJykpO1xuICAgKlxuICAgKiBjb25zb2xlLmxvZyhjb250cm9sLmVycm9ycyk7IC8vIHtwYXR0ZXJuOiB7cmVxdWlyZWRQYXR0ZXJuOiAnXlthLXpBLVogXSokJywgYWN0dWFsVmFsdWU6ICcxJ319XG4gICAqIGBgYFxuICAgKlxuICAgKiBgYGBodG1sXG4gICAqIDxpbnB1dCBwYXR0ZXJuPVwiW2EtekEtWiBdKlwiPlxuICAgKiBgYGBcbiAgICpcbiAgICogQHBhcmFtIHBhdHRlcm4gQSByZWd1bGFyIGV4cHJlc3Npb24gdG8gYmUgdXNlZCBhcyBpcyB0byB0ZXN0IHRoZSB2YWx1ZXMsIG9yIGEgc3RyaW5nLlxuICAgKiBJZiBhIHN0cmluZyBpcyBwYXNzZWQsIHRoZSBgXmAgY2hhcmFjdGVyIGlzIHByZXBlbmRlZCBhbmQgdGhlIGAkYCBjaGFyYWN0ZXIgaXNcbiAgICogYXBwZW5kZWQgdG8gdGhlIHByb3ZpZGVkIHN0cmluZyAoaWYgbm90IGFscmVhZHkgcHJlc2VudCksIGFuZCB0aGUgcmVzdWx0aW5nIHJlZ3VsYXJcbiAgICogZXhwcmVzc2lvbiBpcyB1c2VkIHRvIHRlc3QgdGhlIHZhbHVlcy5cbiAgICpcbiAgICogQHJldHVybnMgQSB2YWxpZGF0b3IgZnVuY3Rpb24gdGhhdCByZXR1cm5zIGFuIGVycm9yIG1hcCB3aXRoIHRoZVxuICAgKiBgcGF0dGVybmAgcHJvcGVydHkgaWYgdGhlIHZhbGlkYXRpb24gY2hlY2sgZmFpbHMsIG90aGVyd2lzZSBgbnVsbGAuXG4gICAqXG4gICAqIEBzZWUgYHVwZGF0ZVZhbHVlQW5kVmFsaWRpdHkoKWBcbiAgICpcbiAgICovXG4gIHN0YXRpYyBwYXR0ZXJuKHBhdHRlcm46IHN0cmluZ3xSZWdFeHApOiBWYWxpZGF0b3JGbiB7XG4gICAgaWYgKCFwYXR0ZXJuKSByZXR1cm4gVmFsaWRhdG9ycy5udWxsVmFsaWRhdG9yO1xuICAgIGxldCByZWdleDogUmVnRXhwO1xuICAgIGxldCByZWdleFN0cjogc3RyaW5nO1xuICAgIGlmICh0eXBlb2YgcGF0dGVybiA9PT0gJ3N0cmluZycpIHtcbiAgICAgIHJlZ2V4U3RyID0gJyc7XG5cbiAgICAgIGlmIChwYXR0ZXJuLmNoYXJBdCgwKSAhPT0gJ14nKSByZWdleFN0ciArPSAnXic7XG5cbiAgICAgIHJlZ2V4U3RyICs9IHBhdHRlcm47XG5cbiAgICAgIGlmIChwYXR0ZXJuLmNoYXJBdChwYXR0ZXJuLmxlbmd0aCAtIDEpICE9PSAnJCcpIHJlZ2V4U3RyICs9ICckJztcblxuICAgICAgcmVnZXggPSBuZXcgUmVnRXhwKHJlZ2V4U3RyKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmVnZXhTdHIgPSBwYXR0ZXJuLnRvU3RyaW5nKCk7XG4gICAgICByZWdleCA9IHBhdHRlcm47XG4gICAgfVxuICAgIHJldHVybiAoY29udHJvbDogQWJzdHJhY3RDb250cm9sKTogVmFsaWRhdGlvbkVycm9ycyB8IG51bGwgPT4ge1xuICAgICAgaWYgKGlzRW1wdHlJbnB1dFZhbHVlKGNvbnRyb2wudmFsdWUpKSB7XG4gICAgICAgIHJldHVybiBudWxsOyAgLy8gZG9uJ3QgdmFsaWRhdGUgZW1wdHkgdmFsdWVzIHRvIGFsbG93IG9wdGlvbmFsIGNvbnRyb2xzXG4gICAgICB9XG4gICAgICBjb25zdCB2YWx1ZTogc3RyaW5nID0gY29udHJvbC52YWx1ZTtcbiAgICAgIHJldHVybiByZWdleC50ZXN0KHZhbHVlKSA/IG51bGwgOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgeydwYXR0ZXJuJzogeydyZXF1aXJlZFBhdHRlcm4nOiByZWdleFN0ciwgJ2FjdHVhbFZhbHVlJzogdmFsdWV9fTtcbiAgICB9O1xuICB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBWYWxpZGF0b3IgdGhhdCBwZXJmb3JtcyBubyBvcGVyYXRpb24uXG4gICAqXG4gICAqIEBzZWUgYHVwZGF0ZVZhbHVlQW5kVmFsaWRpdHkoKWBcbiAgICpcbiAgICovXG4gIHN0YXRpYyBudWxsVmFsaWRhdG9yKGNvbnRyb2w6IEFic3RyYWN0Q29udHJvbCk6IFZhbGlkYXRpb25FcnJvcnN8bnVsbCB7IHJldHVybiBudWxsOyB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBDb21wb3NlIG11bHRpcGxlIHZhbGlkYXRvcnMgaW50byBhIHNpbmdsZSBmdW5jdGlvbiB0aGF0IHJldHVybnMgdGhlIHVuaW9uXG4gICAqIG9mIHRoZSBpbmRpdmlkdWFsIGVycm9yIG1hcHMgZm9yIHRoZSBwcm92aWRlZCBjb250cm9sLlxuICAgKlxuICAgKiBAcmV0dXJucyBBIHZhbGlkYXRvciBmdW5jdGlvbiB0aGF0IHJldHVybnMgYW4gZXJyb3IgbWFwIHdpdGggdGhlXG4gICAqIG1lcmdlZCBlcnJvciBtYXBzIG9mIHRoZSB2YWxpZGF0b3JzIGlmIHRoZSB2YWxpZGF0aW9uIGNoZWNrIGZhaWxzLCBvdGhlcndpc2UgYG51bGxgLlxuICAgKlxuICAgKiBAc2VlIGB1cGRhdGVWYWx1ZUFuZFZhbGlkaXR5KClgXG4gICAqXG4gICAqL1xuICBzdGF0aWMgY29tcG9zZSh2YWxpZGF0b3JzOiBudWxsKTogbnVsbDtcbiAgc3RhdGljIGNvbXBvc2UodmFsaWRhdG9yczogKFZhbGlkYXRvckZufG51bGx8dW5kZWZpbmVkKVtdKTogVmFsaWRhdG9yRm58bnVsbDtcbiAgc3RhdGljIGNvbXBvc2UodmFsaWRhdG9yczogKFZhbGlkYXRvckZufG51bGx8dW5kZWZpbmVkKVtdfG51bGwpOiBWYWxpZGF0b3JGbnxudWxsIHtcbiAgICBpZiAoIXZhbGlkYXRvcnMpIHJldHVybiBudWxsO1xuICAgIGNvbnN0IHByZXNlbnRWYWxpZGF0b3JzOiBWYWxpZGF0b3JGbltdID0gdmFsaWRhdG9ycy5maWx0ZXIoaXNQcmVzZW50KSBhcyBhbnk7XG4gICAgaWYgKHByZXNlbnRWYWxpZGF0b3JzLmxlbmd0aCA9PSAwKSByZXR1cm4gbnVsbDtcblxuICAgIHJldHVybiBmdW5jdGlvbihjb250cm9sOiBBYnN0cmFjdENvbnRyb2wpIHtcbiAgICAgIHJldHVybiBfbWVyZ2VFcnJvcnMoX2V4ZWN1dGVWYWxpZGF0b3JzKGNvbnRyb2wsIHByZXNlbnRWYWxpZGF0b3JzKSk7XG4gICAgfTtcbiAgfVxuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogQ29tcG9zZSBtdWx0aXBsZSBhc3luYyB2YWxpZGF0b3JzIGludG8gYSBzaW5nbGUgZnVuY3Rpb24gdGhhdCByZXR1cm5zIHRoZSB1bmlvblxuICAgKiBvZiB0aGUgaW5kaXZpZHVhbCBlcnJvciBvYmplY3RzIGZvciB0aGUgcHJvdmlkZWQgY29udHJvbC5cbiAgICpcbiAgICogQHJldHVybnMgQSB2YWxpZGF0b3IgZnVuY3Rpb24gdGhhdCByZXR1cm5zIGFuIGVycm9yIG1hcCB3aXRoIHRoZVxuICAgKiBtZXJnZWQgZXJyb3Igb2JqZWN0cyBvZiB0aGUgYXN5bmMgdmFsaWRhdG9ycyBpZiB0aGUgdmFsaWRhdGlvbiBjaGVjayBmYWlscywgb3RoZXJ3aXNlIGBudWxsYC5cbiAgICpcbiAgICogQHNlZSBgdXBkYXRlVmFsdWVBbmRWYWxpZGl0eSgpYFxuICAgKlxuICAgKi9cbiAgc3RhdGljIGNvbXBvc2VBc3luYyh2YWxpZGF0b3JzOiAoQXN5bmNWYWxpZGF0b3JGbnxudWxsKVtdKTogQXN5bmNWYWxpZGF0b3JGbnxudWxsIHtcbiAgICBpZiAoIXZhbGlkYXRvcnMpIHJldHVybiBudWxsO1xuICAgIGNvbnN0IHByZXNlbnRWYWxpZGF0b3JzOiBBc3luY1ZhbGlkYXRvckZuW10gPSB2YWxpZGF0b3JzLmZpbHRlcihpc1ByZXNlbnQpIGFzIGFueTtcbiAgICBpZiAocHJlc2VudFZhbGlkYXRvcnMubGVuZ3RoID09IDApIHJldHVybiBudWxsO1xuXG4gICAgcmV0dXJuIGZ1bmN0aW9uKGNvbnRyb2w6IEFic3RyYWN0Q29udHJvbCkge1xuICAgICAgY29uc3Qgb2JzZXJ2YWJsZXMgPSBfZXhlY3V0ZUFzeW5jVmFsaWRhdG9ycyhjb250cm9sLCBwcmVzZW50VmFsaWRhdG9ycykubWFwKHRvT2JzZXJ2YWJsZSk7XG4gICAgICByZXR1cm4gZm9ya0pvaW4ob2JzZXJ2YWJsZXMpLnBpcGUobWFwKF9tZXJnZUVycm9ycykpO1xuICAgIH07XG4gIH1cbn1cblxuZnVuY3Rpb24gaXNQcmVzZW50KG86IGFueSk6IGJvb2xlYW4ge1xuICByZXR1cm4gbyAhPSBudWxsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdG9PYnNlcnZhYmxlKHI6IGFueSk6IE9ic2VydmFibGU8YW55PiB7XG4gIGNvbnN0IG9icyA9IGlzUHJvbWlzZShyKSA/IGZyb20ocikgOiByO1xuICBpZiAoIShpc09ic2VydmFibGUob2JzKSkpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoYEV4cGVjdGVkIHZhbGlkYXRvciB0byByZXR1cm4gUHJvbWlzZSBvciBPYnNlcnZhYmxlLmApO1xuICB9XG4gIHJldHVybiBvYnM7XG59XG5cbmZ1bmN0aW9uIF9leGVjdXRlVmFsaWRhdG9ycyhjb250cm9sOiBBYnN0cmFjdENvbnRyb2wsIHZhbGlkYXRvcnM6IFZhbGlkYXRvckZuW10pOiBhbnlbXSB7XG4gIHJldHVybiB2YWxpZGF0b3JzLm1hcCh2ID0+IHYoY29udHJvbCkpO1xufVxuXG5mdW5jdGlvbiBfZXhlY3V0ZUFzeW5jVmFsaWRhdG9ycyhjb250cm9sOiBBYnN0cmFjdENvbnRyb2wsIHZhbGlkYXRvcnM6IEFzeW5jVmFsaWRhdG9yRm5bXSk6IGFueVtdIHtcbiAgcmV0dXJuIHZhbGlkYXRvcnMubWFwKHYgPT4gdihjb250cm9sKSk7XG59XG5cbmZ1bmN0aW9uIF9tZXJnZUVycm9ycyhhcnJheU9mRXJyb3JzOiBWYWxpZGF0aW9uRXJyb3JzW10pOiBWYWxpZGF0aW9uRXJyb3JzfG51bGwge1xuICBjb25zdCByZXM6IHtba2V5OiBzdHJpbmddOiBhbnl9ID1cbiAgICAgIGFycmF5T2ZFcnJvcnMucmVkdWNlKChyZXM6IFZhbGlkYXRpb25FcnJvcnMgfCBudWxsLCBlcnJvcnM6IFZhbGlkYXRpb25FcnJvcnMgfCBudWxsKSA9PiB7XG4gICAgICAgIHJldHVybiBlcnJvcnMgIT0gbnVsbCA/IHsuLi5yZXMgISwgLi4uZXJyb3JzfSA6IHJlcyAhO1xuICAgICAgfSwge30pO1xuICByZXR1cm4gT2JqZWN0LmtleXMocmVzKS5sZW5ndGggPT09IDAgPyBudWxsIDogcmVzO1xufVxuIl19
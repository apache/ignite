/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Directive, EventEmitter, Inject, Input, Optional, Self, forwardRef } from '@angular/core';
import { FormGroup } from '../model';
import { NG_ASYNC_VALIDATORS, NG_VALIDATORS } from '../validators';
import { ControlContainer } from './control_container';
import { composeAsyncValidators, composeValidators, removeDir, setUpControl, setUpFormContainer, syncPendingControls } from './shared';
export var formDirectiveProvider = {
    provide: ControlContainer,
    useExisting: forwardRef(function () { return NgForm; })
};
var ɵ0 = function () { return Promise.resolve(null); };
var resolvedPromise = (ɵ0)();
/**
 * @description
 * Creates a top-level `FormGroup` instance and binds it to a form
 * to track aggregate form value and validation status.
 *
 * As soon as you import the `FormsModule`, this directive becomes active by default on
 * all `<form>` tags.  You don't need to add a special selector.
 *
 * You optionally export the directive into a local template variable using `ngForm` as the key
 * (ex: `#myForm="ngForm"`). This is optional, but useful.  Many properties from the underlying
 * `FormGroup` instance are duplicated on the directive itself, so a reference to it
 * gives you access to the aggregate value and validity status of the form, as well as
 * user interaction properties like `dirty` and `touched`.
 *
 * To register child controls with the form, use `NgModel` with a `name`
 * attribute. You may use `NgModelGroup` to create sub-groups within the form.
 *
 * If necessary, listen to the directive's `ngSubmit` event to be notified when the user has
 * triggered a form submission. The `ngSubmit` event emits the original form
 * submission event.
 *
 * In template driven forms, all `<form>` tags are automatically tagged as `NgForm`.
 * To import the `FormsModule` but skip its usage in some forms,
 * for example, to use native HTML5 validation, add the `ngNoForm` and the `<form>`
 * tags won't create an `NgForm` directive. In reactive forms, using `ngNoForm` is
 * unnecessary because the `<form>` tags are inert. In that case, you would
 * refrain from using the `formGroup` directive.
 *
 * @usageNotes
 *
 * ### Migrating from deprecated ngForm selector
 *
 * Support for using `ngForm` element selector has been deprecated in Angular v6 and will be removed
 * in Angular v9.
 *
 * This has been deprecated to keep selectors consistent with other core Angular selectors,
 * as element selectors are typically written in kebab-case.
 *
 * Now deprecated:
 * ```html
 * <ngForm #myForm="ngForm">
 * ```
 *
 * After:
 * ```html
 * <ng-form #myForm="ngForm">
 * ```
 *
 * ### Listening for form submission
 *
 * The following example shows how to capture the form values from the "ngSubmit" event.
 *
 * {@example forms/ts/simpleForm/simple_form_example.ts region='Component'}
 *
 * ### Setting the update options
 *
 * The following example shows you how to change the "updateOn" option from its default using
 * ngFormOptions.
 *
 * ```html
 * <form [ngFormOptions]="{updateOn: 'blur'}">
 *    <input name="one" ngModel>  <!-- this ngModel will update on blur -->
 * </form>
 * ```
 *
 * @ngModule FormsModule
 * @publicApi
 */
var NgForm = /** @class */ (function (_super) {
    tslib_1.__extends(NgForm, _super);
    function NgForm(validators, asyncValidators) {
        var _this = _super.call(this) || this;
        /**
         * @description
         * Returns whether the form submission has been triggered.
         */
        _this.submitted = false;
        _this._directives = [];
        /**
         * @description
         * Event emitter for the "ngSubmit" event
         */
        _this.ngSubmit = new EventEmitter();
        _this.form =
            new FormGroup({}, composeValidators(validators), composeAsyncValidators(asyncValidators));
        return _this;
    }
    /**
     * @description
     * Lifecycle method called after the view is initialized. For internal use only.
     */
    NgForm.prototype.ngAfterViewInit = function () { this._setUpdateStrategy(); };
    Object.defineProperty(NgForm.prototype, "formDirective", {
        /**
         * @description
         * The directive instance.
         */
        get: function () { return this; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NgForm.prototype, "control", {
        /**
         * @description
         * The internal `FormGroup` instance.
         */
        get: function () { return this.form; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NgForm.prototype, "path", {
        /**
         * @description
         * Returns an array representing the path to this group. Because this directive
         * always lives at the top level of a form, it is always an empty array.
         */
        get: function () { return []; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NgForm.prototype, "controls", {
        /**
         * @description
         * Returns a map of the controls in this group.
         */
        get: function () { return this.form.controls; },
        enumerable: true,
        configurable: true
    });
    /**
     * @description
     * Method that sets up the control directive in this group, re-calculates its value
     * and validity, and adds the instance to the internal list of directives.
     *
     * @param dir The `NgModel` directive instance.
     */
    NgForm.prototype.addControl = function (dir) {
        var _this = this;
        resolvedPromise.then(function () {
            var container = _this._findContainer(dir.path);
            dir.control =
                container.registerControl(dir.name, dir.control);
            setUpControl(dir.control, dir);
            dir.control.updateValueAndValidity({ emitEvent: false });
            _this._directives.push(dir);
        });
    };
    /**
     * @description
     * Retrieves the `FormControl` instance from the provided `NgModel` directive.
     *
     * @param dir The `NgModel` directive instance.
     */
    NgForm.prototype.getControl = function (dir) { return this.form.get(dir.path); };
    /**
     * @description
     * Removes the `NgModel` instance from the internal list of directives
     *
     * @param dir The `NgModel` directive instance.
     */
    NgForm.prototype.removeControl = function (dir) {
        var _this = this;
        resolvedPromise.then(function () {
            var container = _this._findContainer(dir.path);
            if (container) {
                container.removeControl(dir.name);
            }
            removeDir(_this._directives, dir);
        });
    };
    /**
     * @description
     * Adds a new `NgModelGroup` directive instance to the form.
     *
     * @param dir The `NgModelGroup` directive instance.
     */
    NgForm.prototype.addFormGroup = function (dir) {
        var _this = this;
        resolvedPromise.then(function () {
            var container = _this._findContainer(dir.path);
            var group = new FormGroup({});
            setUpFormContainer(group, dir);
            container.registerControl(dir.name, group);
            group.updateValueAndValidity({ emitEvent: false });
        });
    };
    /**
     * @description
     * Removes the `NgModelGroup` directive instance from the form.
     *
     * @param dir The `NgModelGroup` directive instance.
     */
    NgForm.prototype.removeFormGroup = function (dir) {
        var _this = this;
        resolvedPromise.then(function () {
            var container = _this._findContainer(dir.path);
            if (container) {
                container.removeControl(dir.name);
            }
        });
    };
    /**
     * @description
     * Retrieves the `FormGroup` for a provided `NgModelGroup` directive instance
     *
     * @param dir The `NgModelGroup` directive instance.
     */
    NgForm.prototype.getFormGroup = function (dir) { return this.form.get(dir.path); };
    /**
     * Sets the new value for the provided `NgControl` directive.
     *
     * @param dir The `NgControl` directive instance.
     * @param value The new value for the directive's control.
     */
    NgForm.prototype.updateModel = function (dir, value) {
        var _this = this;
        resolvedPromise.then(function () {
            var ctrl = _this.form.get(dir.path);
            ctrl.setValue(value);
        });
    };
    /**
     * @description
     * Sets the value for this `FormGroup`.
     *
     * @param value The new value
     */
    NgForm.prototype.setValue = function (value) { this.control.setValue(value); };
    /**
     * @description
     * Method called when the "submit" event is triggered on the form.
     * Triggers the `ngSubmit` emitter to emit the "submit" event as its payload.
     *
     * @param $event The "submit" event object
     */
    NgForm.prototype.onSubmit = function ($event) {
        this.submitted = true;
        syncPendingControls(this.form, this._directives);
        this.ngSubmit.emit($event);
        return false;
    };
    /**
     * @description
     * Method called when the "reset" event is triggered on the form.
     */
    NgForm.prototype.onReset = function () { this.resetForm(); };
    /**
     * @description
     * Resets the form to an initial value and resets its submitted status.
     *
     * @param value The new value for the form.
     */
    NgForm.prototype.resetForm = function (value) {
        if (value === void 0) { value = undefined; }
        this.form.reset(value);
        this.submitted = false;
    };
    NgForm.prototype._setUpdateStrategy = function () {
        if (this.options && this.options.updateOn != null) {
            this.form._updateOn = this.options.updateOn;
        }
    };
    /** @internal */
    NgForm.prototype._findContainer = function (path) {
        path.pop();
        return path.length ? this.form.get(path) : this.form;
    };
    tslib_1.__decorate([
        Input('ngFormOptions'),
        tslib_1.__metadata("design:type", Object)
    ], NgForm.prototype, "options", void 0);
    NgForm = tslib_1.__decorate([
        Directive({
            selector: 'form:not([ngNoForm]):not([formGroup]),ngForm,ng-form,[ngForm]',
            providers: [formDirectiveProvider],
            host: { '(submit)': 'onSubmit($event)', '(reset)': 'onReset()' },
            outputs: ['ngSubmit'],
            exportAs: 'ngForm'
        }),
        tslib_1.__param(0, Optional()), tslib_1.__param(0, Self()), tslib_1.__param(0, Inject(NG_VALIDATORS)),
        tslib_1.__param(1, Optional()), tslib_1.__param(1, Self()), tslib_1.__param(1, Inject(NG_ASYNC_VALIDATORS)),
        tslib_1.__metadata("design:paramtypes", [Array, Array])
    ], NgForm);
    return NgForm;
}(ControlContainer));
export { NgForm };
export { ɵ0 };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfZm9ybS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2Zvcm1zL3NyYy9kaXJlY3RpdmVzL25nX2Zvcm0udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sRUFBZ0IsU0FBUyxFQUFFLFlBQVksRUFBRSxNQUFNLEVBQUUsS0FBSyxFQUFFLFFBQVEsRUFBRSxJQUFJLEVBQUUsVUFBVSxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBRWhILE9BQU8sRUFBK0IsU0FBUyxFQUFZLE1BQU0sVUFBVSxDQUFDO0FBQzVFLE9BQU8sRUFBQyxtQkFBbUIsRUFBRSxhQUFhLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFFakUsT0FBTyxFQUFDLGdCQUFnQixFQUFDLE1BQU0scUJBQXFCLENBQUM7QUFLckQsT0FBTyxFQUFDLHNCQUFzQixFQUFFLGlCQUFpQixFQUFFLFNBQVMsRUFBRSxZQUFZLEVBQUUsa0JBQWtCLEVBQUUsbUJBQW1CLEVBQUMsTUFBTSxVQUFVLENBQUM7QUFFckksTUFBTSxDQUFDLElBQU0scUJBQXFCLEdBQVE7SUFDeEMsT0FBTyxFQUFFLGdCQUFnQjtJQUN6QixXQUFXLEVBQUUsVUFBVSxDQUFDLGNBQU0sT0FBQSxNQUFNLEVBQU4sQ0FBTSxDQUFDO0NBQ3RDLENBQUM7U0FFdUIsY0FBTSxPQUFBLE9BQU8sQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLEVBQXJCLENBQXFCO0FBQXBELElBQU0sZUFBZSxHQUFHLElBQTZCLEVBQUUsQ0FBQztBQUV4RDs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztHQW1FRztBQVFIO0lBQTRCLGtDQUFnQjtJQWtDMUMsZ0JBQytDLFVBQWlCLEVBQ1gsZUFBc0I7UUFGM0UsWUFHRSxpQkFBTyxTQUdSO1FBdENEOzs7V0FHRztRQUNhLGVB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QUFFLENBQUMsVUFBVSxDQUFDO1lBQ3JCLFFBQVEsRUFBRSxRQUFRO1NBQ25CLENBQUM7UUFvQ0ssbUJBQUEsUUFBUSxFQUFFLENBQUEsRUFBRSxtQkFBQSxJQUFJLEVBQUUsQ0FBQSxFQUFFLG1CQUFBLE1BQU0sQ0FBQyxhQUFhLENBQUMsQ0FBQTtRQUN6QyxtQkFBQSxRQUFRLEVBQUUsQ0FBQSxFQUFFLG1CQUFBLElBQUksRUFBRSxDQUFBLEVBQUUsbUJBQUEsTUFBTSxDQUFDLG1CQUFtQixDQUFDLENBQUE7O09BcEN6QyxNQUFNLENBeU5sQjtJQUFELGFBQUM7Q0FBQSxBQXpORCxDQUE0QixnQkFBZ0IsR0F5TjNDO1NBek5ZLE1BQU0iLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7QWZ0ZXJWaWV3SW5pdCwgRGlyZWN0aXZlLCBFdmVudEVtaXR0ZXIsIEluamVjdCwgSW5wdXQsIE9wdGlvbmFsLCBTZWxmLCBmb3J3YXJkUmVmfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHtBYnN0cmFjdENvbnRyb2wsIEZvcm1Db250cm9sLCBGb3JtR3JvdXAsIEZvcm1Ib29rc30gZnJvbSAnLi4vbW9kZWwnO1xuaW1wb3J0IHtOR19BU1lOQ19WQUxJREFUT1JTLCBOR19WQUxJREFUT1JTfSBmcm9tICcuLi92YWxpZGF0b3JzJztcblxuaW1wb3J0IHtDb250cm9sQ29udGFpbmVyfSBmcm9tICcuL2NvbnRyb2xfY29udGFpbmVyJztcbmltcG9ydCB7Rm9ybX0gZnJvbSAnLi9mb3JtX2ludGVyZmFjZSc7XG5pbXBvcnQge05nQ29udHJvbH0gZnJvbSAnLi9uZ19jb250cm9sJztcbmltcG9ydCB7TmdNb2RlbH0gZnJvbSAnLi9uZ19tb2RlbCc7XG5pbXBvcnQge05nTW9kZWxHcm91cH0gZnJvbSAnLi9uZ19tb2RlbF9ncm91cCc7XG5pbXBvcnQge2NvbXBvc2VBc3luY1ZhbGlkYXRvcnMsIGNvbXBvc2VWYWxpZGF0b3JzLCByZW1vdmVEaXIsIHNldFVwQ29udHJvbCwgc2V0VXBGb3JtQ29udGFpbmVyLCBzeW5jUGVuZGluZ0NvbnRyb2xzfSBmcm9tICcuL3NoYXJlZCc7XG5cbmV4cG9ydCBjb25zdCBmb3JtRGlyZWN0aXZlUHJvdmlkZXI6IGFueSA9IHtcbiAgcHJvdmlkZTogQ29udHJvbENvbnRhaW5lcixcbiAgdXNlRXhpc3Rpbmc6IGZvcndhcmRSZWYoKCkgPT4gTmdGb3JtKVxufTtcblxuY29uc3QgcmVzb2x2ZWRQcm9taXNlID0gKCgpID0+IFByb21pc2UucmVzb2x2ZShudWxsKSkoKTtcblxuLyoqXG4gKiBAZGVzY3JpcHRpb25cbiAqIENyZWF0ZXMgYSB0b3AtbGV2ZWwgYEZvcm1Hcm91cGAgaW5zdGFuY2UgYW5kIGJpbmRzIGl0IHRvIGEgZm9ybVxuICogdG8gdHJhY2sgYWdncmVnYXRlIGZvcm0gdmFsdWUgYW5kIHZhbGlkYXRpb24gc3RhdHVzLlxuICpcbiAqIEFzIHNvb24gYXMgeW91IGltcG9ydCB0aGUgYEZvcm1zTW9kdWxlYCwgdGhpcyBkaXJlY3RpdmUgYmVjb21lcyBhY3RpdmUgYnkgZGVmYXVsdCBvblxuICogYWxsIGA8Zm9ybT5gIHRhZ3MuICBZb3UgZG9uJ3QgbmVlZCB0byBhZGQgYSBzcGVjaWFsIHNlbGVjdG9yLlxuICpcbiAqIFlvdSBvcHRpb25hbGx5IGV4cG9ydCB0aGUgZGlyZWN0aXZlIGludG8gYSBsb2NhbCB0ZW1wbGF0ZSB2YXJpYWJsZSB1c2luZyBgbmdGb3JtYCBhcyB0aGUga2V5XG4gKiAoZXg6IGAjbXlGb3JtPVwibmdGb3JtXCJgKS4gVGhpcyBpcyBvcHRpb25hbCwgYnV0IHVzZWZ1bC4gIE1hbnkgcHJvcGVydGllcyBmcm9tIHRoZSB1bmRlcmx5aW5nXG4gKiBgRm9ybUdyb3VwYCBpbnN0YW5jZSBhcmUgZHVwbGljYXRlZCBvbiB0aGUgZGlyZWN0aXZlIGl0c2VsZiwgc28gYSByZWZlcmVuY2UgdG8gaXRcbiAqIGdpdmVzIHlvdSBhY2Nlc3MgdG8gdGhlIGFnZ3JlZ2F0ZSB2YWx1ZSBhbmQgdmFsaWRpdHkgc3RhdHVzIG9mIHRoZSBmb3JtLCBhcyB3ZWxsIGFzXG4gKiB1c2VyIGludGVyYWN0aW9uIHByb3BlcnRpZXMgbGlrZSBgZGlydHlgIGFuZCBgdG91Y2hlZGAuXG4gKlxuICogVG8gcmVnaXN0ZXIgY2hpbGQgY29udHJvbHMgd2l0aCB0aGUgZm9ybSwgdXNlIGBOZ01vZGVsYCB3aXRoIGEgYG5hbWVgXG4gKiBhdHRyaWJ1dGUuIFlvdSBtYXkgdXNlIGBOZ01vZGVsR3JvdXBgIHRvIGNyZWF0ZSBzdWItZ3JvdXBzIHdpdGhpbiB0aGUgZm9ybS5cbiAqXG4gKiBJZiBuZWNlc3NhcnksIGxpc3RlbiB0byB0aGUgZGlyZWN0aXZlJ3MgYG5nU3VibWl0YCBldmVudCB0byBiZSBub3RpZmllZCB3aGVuIHRoZSB1c2VyIGhhc1xuICogdHJpZ2dlcmVkIGEgZm9ybSBzdWJtaXNzaW9uLiBUaGUgYG5nU3VibWl0YCBldmVudCBlbWl0cyB0aGUgb3JpZ2luYWwgZm9ybVxuICogc3VibWlzc2lvbiBldmVudC5cbiAqXG4gKiBJbiB0ZW1wbGF0ZSBkcml2ZW4gZm9ybXMsIGFsbCBgPGZvcm0+YCB0YWdzIGFyZSBhdXRvbWF0aWNhbGx5IHRhZ2dlZCBhcyBgTmdGb3JtYC5cbiAqIFRvIGltcG9ydCB0aGUgYEZvcm1zTW9kdWxlYCBidXQgc2tpcCBpdHMgdXNhZ2UgaW4gc29tZSBmb3JtcyxcbiAqIGZvciBleGFtcGxlLCB0byB1c2UgbmF0aXZlIEhUTUw1IHZhbGlkYXRpb24sIGFkZCB0aGUgYG5nTm9Gb3JtYCBhbmQgdGhlIGA8Zm9ybT5gXG4gKiB0YWdzIHdvbid0IGNyZWF0ZSBhbiBgTmdGb3JtYCBkaXJlY3RpdmUuIEluIHJlYWN0aXZlIGZvcm1zLCB1c2luZyBgbmdOb0Zvcm1gIGlzXG4gKiB1bm5lY2Vzc2FyeSBiZWNhdXNlIHRoZSBgPGZvcm0+YCB0YWdzIGFyZSBpbmVydC4gSW4gdGhhdCBjYXNlLCB5b3Ugd291bGRcbiAqIHJlZnJhaW4gZnJvbSB1c2luZyB0aGUgYGZvcm1Hcm91cGAgZGlyZWN0aXZlLlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKlxuICogIyMjIE1pZ3JhdGluZyBmcm9tIGRlcHJlY2F0ZWQgbmdGb3JtIHNlbGVjdG9yXG4gKlxuICogU3VwcG9ydCBmb3IgdXNpbmcgYG5nRm9ybWAgZWxlbWVudCBzZWxlY3RvciBoYXMgYmVlbiBkZXByZWNhdGVkIGluIEFuZ3VsYXIgdjYgYW5kIHdpbGwgYmUgcmVtb3ZlZFxuICogaW4gQW5ndWxhciB2OS5cbiAqXG4gKiBUaGlzIGhhcyBiZWVuIGRlcHJlY2F0ZWQgdG8ga2VlcCBzZWxlY3RvcnMgY29uc2lzdGVudCB3aXRoIG90aGVyIGNvcmUgQW5ndWxhciBzZWxlY3RvcnMsXG4gKiBhcyBlbGVtZW50IHNlbGVjdG9ycyBhcmUgdHlwaWNhbGx5IHdyaXR0ZW4gaW4ga2ViYWItY2FzZS5cbiAqXG4gKiBOb3cgZGVwcmVjYXRlZDpcbiAqIGBgYGh0bWxcbiAqIDxuZ0Zvcm0gI215Rm9ybT1cIm5nRm9ybVwiPlxuICogYGBgXG4gKlxuICogQWZ0ZXI6XG4gKiBgYGBodG1sXG4gKiA8bmctZm9ybSAjbXlGb3JtPVwibmdGb3JtXCI+XG4gKiBgYGBcbiAqXG4gKiAjIyMgTGlzdGVuaW5nIGZvciBmb3JtIHN1Ym1pc3Npb25cbiAqXG4gKiBUaGUgZm9sbG93aW5nIGV4YW1wbGUgc2hvd3MgaG93IHRvIGNhcHR1cmUgdGhlIGZvcm0gdmFsdWVzIGZyb20gdGhlIFwibmdTdWJtaXRcIiBldmVudC5cbiAqXG4gKiB7QGV4YW1wbGUgZm9ybXMvdHMvc2ltcGxlRm9ybS9zaW1wbGVfZm9ybV9leGFtcGxlLnRzIHJlZ2lvbj0nQ29tcG9uZW50J31cbiAqXG4gKiAjIyMgU2V0dGluZyB0aGUgdXBkYXRlIG9wdGlvbnNcbiAqXG4gKiBUaGUgZm9sbG93aW5nIGV4YW1wbGUgc2hvd3MgeW91IGhvdyB0byBjaGFuZ2UgdGhlIFwidXBkYXRlT25cIiBvcHRpb24gZnJvbSBpdHMgZGVmYXVsdCB1c2luZ1xuICogbmdGb3JtT3B0aW9ucy5cbiAqXG4gKiBgYGBodG1sXG4gKiA8Zm9ybSBbbmdGb3JtT3B0aW9uc109XCJ7dXBkYXRlT246ICdibHVyJ31cIj5cbiAqICAgIDxpbnB1dCBuYW1lPVwib25lXCIgbmdNb2RlbD4gIDwhLS0gdGhpcyBuZ01vZGVsIHdpbGwgdXBkYXRlIG9uIGJsdXIgLS0+XG4gKiA8L2Zvcm0+XG4gKiBgYGBcbiAqXG4gKiBAbmdNb2R1bGUgRm9ybXNNb2R1bGVcbiAqIEBwdWJsaWNBcGlcbiAqL1xuQERpcmVjdGl2ZSh7XG4gIHNlbGVjdG9yOiAnZm9ybTpub3QoW25nTm9Gb3JtXSk6bm90KFtmb3JtR3JvdXBdKSxuZ0Zvcm0sbmctZm9ybSxbbmdGb3JtXScsXG4gIHByb3ZpZGVyczogW2Zvcm1EaXJlY3RpdmVQcm92aWRlcl0sXG4gIGhvc3Q6IHsnKHN1Ym1pdCknOiAnb25TdWJtaXQoJGV2ZW50KScsICcocmVzZXQpJzogJ29uUmVzZXQoKSd9LFxuICBvdXRwdXRzOiBbJ25nU3VibWl0J10sXG4gIGV4cG9ydEFzOiAnbmdGb3JtJ1xufSlcbmV4cG9ydCBjbGFzcyBOZ0Zvcm0gZXh0ZW5kcyBDb250cm9sQ29udGFpbmVyIGltcGxlbWVudHMgRm9ybSxcbiAgICBBZnRlclZpZXdJbml0IHtcbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBSZXR1cm5zIHdoZXRoZXIgdGhlIGZvcm0gc3VibWlzc2lvbiBoYXMgYmVlbiB0cmlnZ2VyZWQuXG4gICAqL1xuICBwdWJsaWMgcmVhZG9ubHkgc3VibWl0dGVkOiBib29sZWFuID0gZmFsc2U7XG5cbiAgcHJpdmF0ZSBfZGlyZWN0aXZlczogTmdNb2RlbFtdID0gW107XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBUaGUgYEZvcm1Hcm91cGAgaW5zdGFuY2UgY3JlYXRlZCBmb3IgdGhpcyBmb3JtLlxuICAgKi9cbiAgZm9ybTogRm9ybUdyb3VwO1xuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogRXZlbnQgZW1pdHRlciBmb3IgdGhlIFwibmdTdWJtaXRcIiBldmVudFxuICAgKi9cbiAgbmdTdWJtaXQgPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBUcmFja3Mgb3B0aW9ucyBmb3IgdGhlIGBOZ0Zvcm1gIGluc3RhbmNlLlxuICAgKlxuICAgKiAqKnVwZGF0ZU9uKio6IFNldHMgdGhlIGRlZmF1bHQgYHVwZGF0ZU9uYCB2YWx1ZSBmb3IgYWxsIGNoaWxkIGBOZ01vZGVsc2AgYmVsb3cgaXRcbiAgICogdW5sZXNzIGV4cGxpY2l0bHkgc2V0IGJ5IGEgY2hpbGQgYE5nTW9kZWxgIHVzaW5nIGBuZ01vZGVsT3B0aW9uc2ApLiBEZWZhdWx0cyB0byAnY2hhbmdlJy5cbiAgICogUG9zc2libGUgdmFsdWVzOiBgJ2NoYW5nZSdgIHwgYCdibHVyJ2AgfCBgJ3N1Ym1pdCdgLlxuICAgKlxuICAgKi9cbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIEBJbnB1dCgnbmdGb3JtT3B0aW9ucycpIG9wdGlvbnMgIToge3VwZGF0ZU9uPzogRm9ybUhvb2tzfTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIEBPcHRpb25hbCgpIEBTZWxmKCkgQEluamVjdChOR19WQUxJREFUT1JTKSB2YWxpZGF0b3JzOiBhbnlbXSxcbiAgICAgIEBPcHRpb25hbCgpIEBTZWxmKCkgQEluamVjdChOR19BU1lOQ19WQUxJREFUT1JTKSBhc3luY1ZhbGlkYXRvcnM6IGFueVtdKSB7XG4gICAgc3VwZXIoKTtcbiAgICB0aGlzLmZvcm0gPVxuICAgICAgICBuZXcgRm9ybUdyb3VwKHt9LCBjb21wb3NlVmFsaWRhdG9ycyh2YWxpZGF0b3JzKSwgY29tcG9zZUFzeW5jVmFsaWRhdG9ycyhhc3luY1ZhbGlkYXRvcnMpKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogTGlmZWN5Y2xlIG1ldGhvZCBjYWxsZWQgYWZ0ZXIgdGhlIHZpZXcgaXMgaW5pdGlhbGl6ZWQuIEZvciBpbnRlcm5hbCB1c2Ugb25seS5cbiAgICovXG4gIG5nQWZ0ZXJWaWV3SW5pdCgpIHsgdGhpcy5fc2V0VXBkYXRlU3RyYXRlZ3koKTsgfVxuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogVGhlIGRpcmVjdGl2ZSBpbnN0YW5jZS5cbiAgICovXG4gIGdldCBmb3JtRGlyZWN0aXZlKCk6IEZvcm0geyByZXR1cm4gdGhpczsgfVxuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogVGhlIGludGVybmFsIGBGb3JtR3JvdXBgIGluc3RhbmNlLlxuICAgKi9cbiAgZ2V0IGNvbnRyb2woKTogRm9ybUdyb3VwIHsgcmV0dXJuIHRoaXMuZm9ybTsgfVxuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogUmV0dXJucyBhbiBhcnJheSByZXByZXNlbnRpbmcgdGhlIHBhdGggdG8gdGhpcyBncm91cC4gQmVjYXVzZSB0aGlzIGRpcmVjdGl2ZVxuICAgKiBhbHdheXMgbGl2ZXMgYXQgdGhlIHRvcCBsZXZlbCBvZiBhIGZvcm0sIGl0IGlzIGFsd2F5cyBhbiBlbXB0eSBhcnJheS5cbiAgICovXG4gIGdldCBwYXRoKCk6IHN0cmluZ1tdIHsgcmV0dXJuIFtdOyB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBSZXR1cm5zIGEgbWFwIG9mIHRoZSBjb250cm9scyBpbiB0aGlzIGdyb3VwLlxuICAgKi9cbiAgZ2V0IGNvbnRyb2xzKCk6IHtba2V5OiBzdHJpbmddOiBBYnN0cmFjdENvbnRyb2x9IHsgcmV0dXJuIHRoaXMuZm9ybS5jb250cm9sczsgfVxuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogTWV0aG9kIHRoYXQgc2V0cyB1cCB0aGUgY29udHJvbCBkaXJlY3RpdmUgaW4gdGhpcyBncm91cCwgcmUtY2FsY3VsYXRlcyBpdHMgdmFsdWVcbiAgICogYW5kIHZhbGlkaXR5LCBhbmQgYWRkcyB0aGUgaW5zdGFuY2UgdG8gdGhlIGludGVybmFsIGxpc3Qgb2YgZGlyZWN0aXZlcy5cbiAgICpcbiAgICogQHBhcmFtIGRpciBUaGUgYE5nTW9kZWxgIGRpcmVjdGl2ZSBpbnN0YW5jZS5cbiAgICovXG4gIGFkZENvbnRyb2woZGlyOiBOZ01vZGVsKTogdm9pZCB7XG4gICAgcmVzb2x2ZWRQcm9taXNlLnRoZW4oKCkgPT4ge1xuICAgICAgY29uc3QgY29udGFpbmVyID0gdGhpcy5fZmluZENvbnRhaW5lcihkaXIucGF0aCk7XG4gICAgICAoZGlyIGFze2NvbnRyb2w6IEZvcm1Db250cm9sfSkuY29udHJvbCA9XG4gICAgICAgICAgPEZvcm1Db250cm9sPmNvbnRhaW5lci5yZWdpc3RlckNvbnRyb2woZGlyLm5hbWUsIGRpci5jb250cm9sKTtcbiAgICAgIHNldFVwQ29udHJvbChkaXIuY29udHJvbCwgZGlyKTtcbiAgICAgIGRpci5jb250cm9sLnVwZGF0ZVZhbHVlQW5kVmFsaWRpdHkoe2VtaXRFdmVudDogZmFsc2V9KTtcbiAgICAgIHRoaXMuX2RpcmVjdGl2ZXMucHVzaChkaXIpO1xuICAgIH0pO1xuICB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBSZXRyaWV2ZXMgdGhlIGBGb3JtQ29udHJvbGAgaW5zdGFuY2UgZnJvbSB0aGUgcHJvdmlkZWQgYE5nTW9kZWxgIGRpcmVjdGl2ZS5cbiAgICpcbiAgICogQHBhcmFtIGRpciBUaGUgYE5nTW9kZWxgIGRpcmVjdGl2ZSBpbnN0YW5jZS5cbiAgICovXG4gIGdldENvbnRyb2woZGlyOiBOZ01vZGVsKTogRm9ybUNvbnRyb2wgeyByZXR1cm4gPEZvcm1Db250cm9sPnRoaXMuZm9ybS5nZXQoZGlyLnBhdGgpOyB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBSZW1vdmVzIHRoZSBgTmdNb2RlbGAgaW5zdGFuY2UgZnJvbSB0aGUgaW50ZXJuYWwgbGlzdCBvZiBkaXJlY3RpdmVzXG4gICAqXG4gICAqIEBwYXJhbSBkaXIgVGhlIGBOZ01vZGVsYCBkaXJlY3RpdmUgaW5zdGFuY2UuXG4gICAqL1xuICByZW1vdmVDb250cm9sKGRpcjogTmdNb2RlbCk6IHZvaWQge1xuICAgIHJlc29sdmVkUHJvbWlzZS50aGVuKCgpID0+IHtcbiAgICAgIGNvbnN0IGNvbnRhaW5lciA9IHRoaXMuX2ZpbmRDb250YWluZXIoZGlyLnBhdGgpO1xuICAgICAgaWYgKGNvbnRhaW5lcikge1xuICAgICAgICBjb250YWluZXIucmVtb3ZlQ29udHJvbChkaXIubmFtZSk7XG4gICAgICB9XG4gICAgICByZW1vdmVEaXI8TmdNb2RlbD4odGhpcy5fZGlyZWN0aXZlcywgZGlyKTtcbiAgICB9KTtcbiAgfVxuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogQWRkcyBhIG5ldyBgTmdNb2RlbEdyb3VwYCBkaXJlY3RpdmUgaW5zdGFuY2UgdG8gdGhlIGZvcm0uXG4gICAqXG4gICAqIEBwYXJhbSBkaXIgVGhlIGBOZ01vZGVsR3JvdXBgIGRpcmVjdGl2ZSBpbnN0YW5jZS5cbiAgICovXG4gIGFkZEZvcm1Hcm91cChkaXI6IE5nTW9kZWxHcm91cCk6IHZvaWQge1xuICAgIHJlc29sdmVkUHJvbWlzZS50aGVuKCgpID0+IHtcbiAgICAgIGNvbnN0IGNvbnRhaW5lciA9IHRoaXMuX2ZpbmRDb250YWluZXIoZGlyLnBhdGgpO1xuICAgICAgY29uc3QgZ3JvdXAgPSBuZXcgRm9ybUdyb3VwKHt9KTtcbiAgICAgIHNldFVwRm9ybUNvbnRhaW5lcihncm91cCwgZGlyKTtcbiAgICAgIGNvbnRhaW5lci5yZWdpc3RlckNvbnRyb2woZGlyLm5hbWUsIGdyb3VwKTtcbiAgICAgIGdyb3VwLnVwZGF0ZVZhbHVlQW5kVmFsaWRpdHkoe2VtaXRFdmVudDogZmFsc2V9KTtcbiAgICB9KTtcbiAgfVxuXG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogUmVtb3ZlcyB0aGUgYE5nTW9kZWxHcm91cGAgZGlyZWN0aXZlIGluc3RhbmNlIGZyb20gdGhlIGZvcm0uXG4gICAqXG4gICAqIEBwYXJhbSBkaXIgVGhlIGBOZ01vZGVsR3JvdXBgIGRpcmVjdGl2ZSBpbnN0YW5jZS5cbiAgICovXG4gIHJlbW92ZUZvcm1Hcm91cChkaXI6IE5nTW9kZWxHcm91cCk6IHZvaWQge1xuICAgIHJlc29sdmVkUHJvbWlzZS50aGVuKCgpID0+IHtcbiAgICAgIGNvbnN0IGNvbnRhaW5lciA9IHRoaXMuX2ZpbmRDb250YWluZXIoZGlyLnBhdGgpO1xuICAgICAgaWYgKGNvbnRhaW5lcikge1xuICAgICAgICBjb250YWluZXIucmVtb3ZlQ29udHJvbChkaXIubmFtZSk7XG4gICAgICB9XG4gICAgfSk7XG4gIH1cblxuICAvKipcbiAgICogQGRlc2NyaXB0aW9uXG4gICAqIFJldHJpZXZlcyB0aGUgYEZvcm1Hcm91cGAgZm9yIGEgcHJvdmlkZWQgYE5nTW9kZWxHcm91cGAgZGlyZWN0aXZlIGluc3RhbmNlXG4gICAqXG4gICAqIEBwYXJhbSBkaXIgVGhlIGBOZ01vZGVsR3JvdXBgIGRpcmVjdGl2ZSBpbnN0YW5jZS5cbiAgICovXG4gIGdldEZvcm1Hcm91cChkaXI6IE5nTW9kZWxHcm91cCk6IEZvcm1Hcm91cCB7IHJldHVybiA8Rm9ybUdyb3VwPnRoaXMuZm9ybS5nZXQoZGlyLnBhdGgpOyB9XG5cbiAgLyoqXG4gICAqIFNldHMgdGhlIG5ldyB2YWx1ZSBmb3IgdGhlIHByb3ZpZGVkIGBOZ0NvbnRyb2xgIGRpcmVjdGl2ZS5cbiAgICpcbiAgICogQHBhcmFtIGRpciBUaGUgYE5nQ29udHJvbGAgZGlyZWN0aXZlIGluc3RhbmNlLlxuICAgKiBAcGFyYW0gdmFsdWUgVGhlIG5ldyB2YWx1ZSBmb3IgdGhlIGRpcmVjdGl2ZSdzIGNvbnRyb2wuXG4gICAqL1xuICB1cGRhdGVNb2RlbChkaXI6IE5nQ29udHJvbCwgdmFsdWU6IGFueSk6IHZvaWQge1xuICAgIHJlc29sdmVkUHJvbWlzZS50aGVuKCgpID0+IHtcbiAgICAgIGNvbnN0IGN0cmwgPSA8Rm9ybUNvbnRyb2w+dGhpcy5mb3JtLmdldChkaXIucGF0aCAhKTtcbiAgICAgIGN0cmwuc2V0VmFsdWUodmFsdWUpO1xuICAgIH0pO1xuICB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBTZXRzIHRoZSB2YWx1ZSBmb3IgdGhpcyBgRm9ybUdyb3VwYC5cbiAgICpcbiAgICogQHBhcmFtIHZhbHVlIFRoZSBuZXcgdmFsdWVcbiAgICovXG4gIHNldFZhbHVlKHZhbHVlOiB7W2tleTogc3RyaW5nXTogYW55fSk6IHZvaWQgeyB0aGlzLmNvbnRyb2wuc2V0VmFsdWUodmFsdWUpOyB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBNZXRob2QgY2FsbGVkIHdoZW4gdGhlIFwic3VibWl0XCIgZXZlbnQgaXMgdHJpZ2dlcmVkIG9uIHRoZSBmb3JtLlxuICAgKiBUcmlnZ2VycyB0aGUgYG5nU3VibWl0YCBlbWl0dGVyIHRvIGVtaXQgdGhlIFwic3VibWl0XCIgZXZlbnQgYXMgaXRzIHBheWxvYWQuXG4gICAqXG4gICAqIEBwYXJhbSAkZXZlbnQgVGhlIFwic3VibWl0XCIgZXZlbnQgb2JqZWN0XG4gICAqL1xuICBvblN1Ym1pdCgkZXZlbnQ6IEV2ZW50KTogYm9vbGVhbiB7XG4gICAgKHRoaXMgYXN7c3VibWl0dGVkOiBib29sZWFufSkuc3VibWl0dGVkID0gdHJ1ZTtcbiAgICBzeW5jUGVuZGluZ0NvbnRyb2xzKHRoaXMuZm9ybSwgdGhpcy5fZGlyZWN0aXZlcyk7XG4gICAgdGhpcy5uZ1N1Ym1pdC5lbWl0KCRldmVudCk7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBNZXRob2QgY2FsbGVkIHdoZW4gdGhlIFwicmVzZXRcIiBldmVudCBpcyB0cmlnZ2VyZWQgb24gdGhlIGZvcm0uXG4gICAqL1xuICBvblJlc2V0KCk6IHZvaWQgeyB0aGlzLnJlc2V0Rm9ybSgpOyB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBSZXNldHMgdGhlIGZvcm0gdG8gYW4gaW5pdGlhbCB2YWx1ZSBhbmQgcmVzZXRzIGl0cyBzdWJtaXR0ZWQgc3RhdHVzLlxuICAgKlxuICAgKiBAcGFyYW0gdmFsdWUgVGhlIG5ldyB2YWx1ZSBmb3IgdGhlIGZvcm0uXG4gICAqL1xuICByZXNldEZvcm0odmFsdWU6IGFueSA9IHVuZGVmaW5lZCk6IHZvaWQge1xuICAgIHRoaXMuZm9ybS5yZXNldCh2YWx1ZSk7XG4gICAgKHRoaXMgYXN7c3VibWl0dGVkOiBib29sZWFufSkuc3VibWl0dGVkID0gZmFsc2U7XG4gIH1cblxuICBwcml2YXRlIF9zZXRVcGRhdGVTdHJhdGVneSgpIHtcbiAgICBpZiAodGhpcy5vcHRpb25zICYmIHRoaXMub3B0aW9ucy51cGRhdGVPbiAhPSBudWxsKSB7XG4gICAgICB0aGlzLmZvcm0uX3VwZGF0ZU9uID0gdGhpcy5vcHRpb25zLnVwZGF0ZU9uO1xuICAgIH1cbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX2ZpbmRDb250YWluZXIocGF0aDogc3RyaW5nW10pOiBGb3JtR3JvdXAge1xuICAgIHBhdGgucG9wKCk7XG4gICAgcmV0dXJuIHBhdGgubGVuZ3RoID8gPEZvcm1Hcm91cD50aGlzLmZvcm0uZ2V0KHBhdGgpIDogdGhpcy5mb3JtO1xuICB9XG59XG4iXX0=
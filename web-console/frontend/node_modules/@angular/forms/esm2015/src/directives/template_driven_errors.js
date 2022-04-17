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
import { FormErrorExamples as Examples } from './error_examples';
export class TemplateDrivenErrors {
    /**
     * @return {?}
     */
    static modelParentException() {
        throw new Error(`
      ngModel cannot be used to register form controls with a parent formGroup directive.  Try using
      formGroup's partner directive "formControlName" instead.  Example:

      ${Examples.formControlName}

      Or, if you'd like to avoid registering this form control, indicate that it's standalone in ngModelOptions:

      Example:

      ${Examples.ngModelWithFormGroup}`);
    }
    /**
     * @return {?}
     */
    static formGroupNameException() {
        throw new Error(`
      ngModel cannot be used to register form controls with a parent formGroupName or formArrayName directive.

      Option 1: Use formControlName instead of ngModel (reactive strategy):

      ${Examples.formGroupName}

      Option 2:  Update ngModel's parent be ngModelGroup (template-driven strategy):

      ${Examples.ngModelGroup}`);
    }
    /**
     * @return {?}
     */
    static missingNameException() {
        throw new Error(`If ngModel is used within a form tag, either the name attribute must be set or the form
      control must be defined as 'standalone' in ngModelOptions.

      Example 1: <input [(ngModel)]="person.firstName" name="first">
      Example 2: <input [(ngModel)]="person.firstName" [ngModelOptions]="{standalone: true}">`);
    }
    /**
     * @return {?}
     */
    static modelGroupParentException() {
        throw new Error(`
      ngModelGroup cannot be used with a parent formGroup directive.

      Option 1: Use formGroupName instead of ngModelGroup (reactive strategy):

      ${Examples.formGroupName}

      Option 2:  Use a regular form tag instead of the formGroup directive (template-driven strategy):

      ${Examples.ngModelGroup}`);
    }
    /**
     * @return {?}
     */
    static ngFormWarning() {
        console.warn(`
    It looks like you're using 'ngForm'.

    Support for using the 'ngForm' element selector has been deprecated in Angular v6 and will be removed
    in Angular v9.

    Use 'ng-form' instead.

    Before:
    <ngForm #myForm="ngForm">

    After:
    <ng-form #myForm="ngForm">
    `);
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGVtcGxhdGVfZHJpdmVuX2Vycm9ycy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2Zvcm1zL3NyYy9kaXJlY3RpdmVzL3RlbXBsYXRlX2RyaXZlbl9lcnJvcnMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsaUJBQWlCLElBQUksUUFBUSxFQUFDLE1BQU0sa0JBQWtCLENBQUM7QUFFL0QsTUFBTSxPQUFPLG9CQUFvQjs7OztJQUMvQixNQUFNLENBQUMsb0JBQW9CO1FBQ3pCLE1BQU0sSUFBSSxLQUFLLENBQUM7Ozs7UUFJWixRQUFRLENBQUMsZUFBZTs7Ozs7O1FBTXhCLFFBQVEsQ0FBQyxvQkFBb0IsRUFBRSxDQUFDLENBQUM7SUFDdkMsQ0FBQzs7OztJQUVELE1BQU0sQ0FBQyxzQkFBc0I7UUFDM0IsTUFBTSxJQUFJLEtBQUssQ0FBQzs7Ozs7UUFLWixRQUFRLENBQUMsYUFBYTs7OztRQUl0QixRQUFRLENBQUMsWUFBWSxFQUFFLENBQUMsQ0FBQztJQUMvQixDQUFDOzs7O0lBRUQsTUFBTSxDQUFDLG9CQUFvQjtRQUN6QixNQUFNLElBQUksS0FBSyxDQUNYOzs7OzhGQUlzRixDQUFDLENBQUM7SUFDOUYsQ0FBQzs7OztJQUVELE1BQU0sQ0FBQyx5QkFBeUI7UUFDOUIsTUFBTSxJQUFJLEtBQUssQ0FBQzs7Ozs7UUFLWixRQUFRLENBQUMsYUFBYTs7OztRQUl0QixRQUFRLENBQUMsWUFBWSxFQUFFLENBQUMsQ0FBQztJQUMvQixDQUFDOzs7O0lBRUQsTUFBTSxDQUFDLGFBQWE7UUFDbEIsT0FBTyxDQUFDLElBQUksQ0FBQzs7Ozs7Ozs7Ozs7OztLQWFaLENBQUMsQ0FBQztJQUNMLENBQUM7Q0FDRiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtGb3JtRXJyb3JFeGFtcGxlcyBhcyBFeGFtcGxlc30gZnJvbSAnLi9lcnJvcl9leGFtcGxlcyc7XG5cbmV4cG9ydCBjbGFzcyBUZW1wbGF0ZURyaXZlbkVycm9ycyB7XG4gIHN0YXRpYyBtb2RlbFBhcmVudEV4Y2VwdGlvbigpOiB2b2lkIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoYFxuICAgICAgbmdNb2RlbCBjYW5ub3QgYmUgdXNlZCB0byByZWdpc3RlciBmb3JtIGNvbnRyb2xzIHdpdGggYSBwYXJlbnQgZm9ybUdyb3VwIGRpcmVjdGl2ZS4gIFRyeSB1c2luZ1xuICAgICAgZm9ybUdyb3VwJ3MgcGFydG5lciBkaXJlY3RpdmUgXCJmb3JtQ29udHJvbE5hbWVcIiBpbnN0ZWFkLiAgRXhhbXBsZTpcblxuICAgICAgJHtFeGFtcGxlcy5mb3JtQ29udHJvbE5hbWV9XG5cbiAgICAgIE9yLCBpZiB5b3UnZCBsaWtlIHRvIGF2b2lkIHJlZ2lzdGVyaW5nIHRoaXMgZm9ybSBjb250cm9sLCBpbmRpY2F0ZSB0aGF0IGl0J3Mgc3RhbmRhbG9uZSBpbiBuZ01vZGVsT3B0aW9uczpcblxuICAgICAgRXhhbXBsZTpcblxuICAgICAgJHtFeGFtcGxlcy5uZ01vZGVsV2l0aEZvcm1Hcm91cH1gKTtcbiAgfVxuXG4gIHN0YXRpYyBmb3JtR3JvdXBOYW1lRXhjZXB0aW9uKCk6IHZvaWQge1xuICAgIHRocm93IG5ldyBFcnJvcihgXG4gICAgICBuZ01vZGVsIGNhbm5vdCBiZSB1c2VkIHRvIHJlZ2lzdGVyIGZvcm0gY29udHJvbHMgd2l0aCBhIHBhcmVudCBmb3JtR3JvdXBOYW1lIG9yIGZvcm1BcnJheU5hbWUgZGlyZWN0aXZlLlxuXG4gICAgICBPcHRpb24gMTogVXNlIGZvcm1Db250cm9sTmFtZSBpbnN0ZWFkIG9mIG5nTW9kZWwgKHJlYWN0aXZlIHN0cmF0ZWd5KTpcblxuICAgICAgJHtFeGFtcGxlcy5mb3JtR3JvdXBOYW1lfVxuXG4gICAgICBPcHRpb24gMjogIFVwZGF0ZSBuZ01vZGVsJ3MgcGFyZW50IGJlIG5nTW9kZWxHcm91cCAodGVtcGxhdGUtZHJpdmVuIHN0cmF0ZWd5KTpcblxuICAgICAgJHtFeGFtcGxlcy5uZ01vZGVsR3JvdXB9YCk7XG4gIH1cblxuICBzdGF0aWMgbWlzc2luZ05hbWVFeGNlcHRpb24oKSB7XG4gICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICBgSWYgbmdNb2RlbCBpcyB1c2VkIHdpdGhpbiBhIGZvcm0gdGFnLCBlaXRoZXIgdGhlIG5hbWUgYXR0cmlidXRlIG11c3QgYmUgc2V0IG9yIHRoZSBmb3JtXG4gICAgICBjb250cm9sIG11c3QgYmUgZGVmaW5lZCBhcyAnc3RhbmRhbG9uZScgaW4gbmdNb2RlbE9wdGlvbnMuXG5cbiAgICAgIEV4YW1wbGUgMTogPGlucHV0IFsobmdNb2RlbCldPVwicGVyc29uLmZpcnN0TmFtZVwiIG5hbWU9XCJmaXJzdFwiPlxuICAgICAgRXhhbXBsZSAyOiA8aW5wdXQgWyhuZ01vZGVsKV09XCJwZXJzb24uZmlyc3ROYW1lXCIgW25nTW9kZWxPcHRpb25zXT1cIntzdGFuZGFsb25lOiB0cnVlfVwiPmApO1xuICB9XG5cbiAgc3RhdGljIG1vZGVsR3JvdXBQYXJlbnRFeGNlcHRpb24oKSB7XG4gICAgdGhyb3cgbmV3IEVycm9yKGBcbiAgICAgIG5nTW9kZWxHcm91cCBjYW5ub3QgYmUgdXNlZCB3aXRoIGEgcGFyZW50IGZvcm1Hcm91cCBkaXJlY3RpdmUuXG5cbiAgICAgIE9wdGlvbiAxOiBVc2UgZm9ybUdyb3VwTmFtZSBpbnN0ZWFkIG9mIG5nTW9kZWxHcm91cCAocmVhY3RpdmUgc3RyYXRlZ3kpOlxuXG4gICAgICAke0V4YW1wbGVzLmZvcm1Hcm91cE5hbWV9XG5cbiAgICAgIE9wdGlvbiAyOiAgVXNlIGEgcmVndWxhciBmb3JtIHRhZyBpbnN0ZWFkIG9mIHRoZSBmb3JtR3JvdXAgZGlyZWN0aXZlICh0ZW1wbGF0ZS1kcml2ZW4gc3RyYXRlZ3kpOlxuXG4gICAgICAke0V4YW1wbGVzLm5nTW9kZWxHcm91cH1gKTtcbiAgfVxuXG4gIHN0YXRpYyBuZ0Zvcm1XYXJuaW5nKCkge1xuICAgIGNvbnNvbGUud2FybihgXG4gICAgSXQgbG9va3MgbGlrZSB5b3UncmUgdXNpbmcgJ25nRm9ybScuXG5cbiAgICBTdXBwb3J0IGZvciB1c2luZyB0aGUgJ25nRm9ybScgZWxlbWVudCBzZWxlY3RvciBoYXMgYmVlbiBkZXByZWNhdGVkIGluIEFuZ3VsYXIgdjYgYW5kIHdpbGwgYmUgcmVtb3ZlZFxuICAgIGluIEFuZ3VsYXIgdjkuXG5cbiAgICBVc2UgJ25nLWZvcm0nIGluc3RlYWQuXG5cbiAgICBCZWZvcmU6XG4gICAgPG5nRm9ybSAjbXlGb3JtPVwibmdGb3JtXCI+XG5cbiAgICBBZnRlcjpcbiAgICA8bmctZm9ybSAjbXlGb3JtPVwibmdGb3JtXCI+XG4gICAgYCk7XG4gIH1cbn1cbiJdfQ==
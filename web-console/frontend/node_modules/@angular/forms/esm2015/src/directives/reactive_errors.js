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
export class ReactiveErrors {
    /**
     * @return {?}
     */
    static controlParentException() {
        throw new Error(`formControlName must be used with a parent formGroup directive.  You'll want to add a formGroup
       directive and pass it an existing FormGroup instance (you can create one in your class).

      Example:

      ${Examples.formControlName}`);
    }
    /**
     * @return {?}
     */
    static ngModelGroupException() {
        throw new Error(`formControlName cannot be used with an ngModelGroup parent. It is only compatible with parents
       that also have a "form" prefix: formGroupName, formArrayName, or formGroup.

       Option 1:  Update the parent to be formGroupName (reactive form strategy)

        ${Examples.formGroupName}

        Option 2: Use ngModel instead of formControlName (template-driven strategy)

        ${Examples.ngModelGroup}`);
    }
    /**
     * @return {?}
     */
    static missingFormException() {
        throw new Error(`formGroup expects a FormGroup instance. Please pass one in.

       Example:

       ${Examples.formControlName}`);
    }
    /**
     * @return {?}
     */
    static groupParentException() {
        throw new Error(`formGroupName must be used with a parent formGroup directive.  You'll want to add a formGroup
      directive and pass it an existing FormGroup instance (you can create one in your class).

      Example:

      ${Examples.formGroupName}`);
    }
    /**
     * @return {?}
     */
    static arrayParentException() {
        throw new Error(`formArrayName must be used with a parent formGroup directive.  You'll want to add a formGroup
       directive and pass it an existing FormGroup instance (you can create one in your class).

        Example:

        ${Examples.formArrayName}`);
    }
    /**
     * @return {?}
     */
    static disabledAttrWarning() {
        console.warn(`
      It looks like you're using the disabled attribute with a reactive form directive. If you set disabled to true
      when you set up this control in your component class, the disabled attribute will actually be set in the DOM for
      you. We recommend using this approach to avoid 'changed after checked' errors.
       
      Example: 
      form = new FormGroup({
        first: new FormControl({value: 'Nancy', disabled: true}, Validators.required),
        last: new FormControl('Drew', Validators.required)
      });
    `);
    }
    /**
     * @param {?} directiveName
     * @return {?}
     */
    static ngModelWarning(directiveName) {
        console.warn(`
    It looks like you're using ngModel on the same form field as ${directiveName}. 
    Support for using the ngModel input property and ngModelChange event with 
    reactive form directives has been deprecated in Angular v6 and will be removed 
    in Angular v7.
    
    For more information on this, see our API docs here:
    https://angular.io/api/forms/${directiveName === 'formControl' ? 'FormControlDirective'
            : 'FormControlName'}#use-with-ngmodel
    `);
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVhY3RpdmVfZXJyb3JzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvZm9ybXMvc3JjL2RpcmVjdGl2ZXMvcmVhY3RpdmVfZXJyb3JzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBU0EsT0FBTyxFQUFDLGlCQUFpQixJQUFJLFFBQVEsRUFBQyxNQUFNLGtCQUFrQixDQUFDO0FBRS9ELE1BQU0sT0FBTyxjQUFjOzs7O0lBQ3pCLE1BQU0sQ0FBQyxzQkFBc0I7UUFDM0IsTUFBTSxJQUFJLEtBQUssQ0FDWDs7Ozs7UUFLQSxRQUFRLENBQUMsZUFBZSxFQUFFLENBQUMsQ0FBQztJQUNsQyxDQUFDOzs7O0lBRUQsTUFBTSxDQUFDLHFCQUFxQjtRQUMxQixNQUFNLElBQUksS0FBSyxDQUNYOzs7OztVQUtFLFFBQVEsQ0FBQyxhQUFhOzs7O1VBSXRCLFFBQVEsQ0FBQyxZQUFZLEVBQUUsQ0FBQyxDQUFDO0lBQ2pDLENBQUM7Ozs7SUFDRCxNQUFNLENBQUMsb0JBQW9CO1FBQ3pCLE1BQU0sSUFBSSxLQUFLLENBQUM7Ozs7U0FJWCxRQUFRLENBQUMsZUFBZSxFQUFFLENBQUMsQ0FBQztJQUNuQyxDQUFDOzs7O0lBRUQsTUFBTSxDQUFDLG9CQUFvQjtRQUN6QixNQUFNLElBQUksS0FBSyxDQUNYOzs7OztRQUtBLFFBQVEsQ0FBQyxhQUFhLEVBQUUsQ0FBQyxDQUFDO0lBQ2hDLENBQUM7Ozs7SUFFRCxNQUFNLENBQUMsb0JBQW9CO1FBQ3pCLE1BQU0sSUFBSSxLQUFLLENBQ1g7Ozs7O1VBS0UsUUFBUSxDQUFDLGFBQWEsRUFBRSxDQUFDLENBQUM7SUFDbEMsQ0FBQzs7OztJQUVELE1BQU0sQ0FBQyxtQkFBbUI7UUFDeEIsT0FBTyxDQUFDLElBQUksQ0FBQzs7Ozs7Ozs7OztLQVVaLENBQUMsQ0FBQztJQUNMLENBQUM7Ozs7O0lBRUQsTUFBTSxDQUFDLGNBQWMsQ0FBQyxhQUFxQjtRQUN6QyxPQUFPLENBQUMsSUFBSSxDQUFDO21FQUNrRCxhQUFhOzs7Ozs7bUNBTTdDLGFBQWEsS0FBSyxhQUFhLENBQUMsQ0FBQyxDQUFDLHNCQUFzQjtZQUNyRixDQUFDLENBQUMsaUJBQWlCO0tBQ3BCLENBQUMsQ0FBQztJQUNMLENBQUM7Q0FDRiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuXG5pbXBvcnQge0Zvcm1FcnJvckV4YW1wbGVzIGFzIEV4YW1wbGVzfSBmcm9tICcuL2Vycm9yX2V4YW1wbGVzJztcblxuZXhwb3J0IGNsYXNzIFJlYWN0aXZlRXJyb3JzIHtcbiAgc3RhdGljIGNvbnRyb2xQYXJlbnRFeGNlcHRpb24oKTogdm9pZCB7XG4gICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICBgZm9ybUNvbnRyb2xOYW1lIG11c3QgYmUgdXNlZCB3aXRoIGEgcGFyZW50IGZvcm1Hcm91cCBkaXJlY3RpdmUuICBZb3UnbGwgd2FudCB0byBhZGQgYSBmb3JtR3JvdXBcbiAgICAgICBkaXJlY3RpdmUgYW5kIHBhc3MgaXQgYW4gZXhpc3RpbmcgRm9ybUdyb3VwIGluc3RhbmNlICh5b3UgY2FuIGNyZWF0ZSBvbmUgaW4geW91ciBjbGFzcykuXG5cbiAgICAgIEV4YW1wbGU6XG5cbiAgICAgICR7RXhhbXBsZXMuZm9ybUNvbnRyb2xOYW1lfWApO1xuICB9XG5cbiAgc3RhdGljIG5nTW9kZWxHcm91cEV4Y2VwdGlvbigpOiB2b2lkIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIGBmb3JtQ29udHJvbE5hbWUgY2Fubm90IGJlIHVzZWQgd2l0aCBhbiBuZ01vZGVsR3JvdXAgcGFyZW50LiBJdCBpcyBvbmx5IGNvbXBhdGlibGUgd2l0aCBwYXJlbnRzXG4gICAgICAgdGhhdCBhbHNvIGhhdmUgYSBcImZvcm1cIiBwcmVmaXg6IGZvcm1Hcm91cE5hbWUsIGZvcm1BcnJheU5hbWUsIG9yIGZvcm1Hcm91cC5cblxuICAgICAgIE9wdGlvbiAxOiAgVXBkYXRlIHRoZSBwYXJlbnQgdG8gYmUgZm9ybUdyb3VwTmFtZSAocmVhY3RpdmUgZm9ybSBzdHJhdGVneSlcblxuICAgICAgICAke0V4YW1wbGVzLmZvcm1Hcm91cE5hbWV9XG5cbiAgICAgICAgT3B0aW9uIDI6IFVzZSBuZ01vZGVsIGluc3RlYWQgb2YgZm9ybUNvbnRyb2xOYW1lICh0ZW1wbGF0ZS1kcml2ZW4gc3RyYXRlZ3kpXG5cbiAgICAgICAgJHtFeGFtcGxlcy5uZ01vZGVsR3JvdXB9YCk7XG4gIH1cbiAgc3RhdGljIG1pc3NpbmdGb3JtRXhjZXB0aW9uKCk6IHZvaWQge1xuICAgIHRocm93IG5ldyBFcnJvcihgZm9ybUdyb3VwIGV4cGVjdHMgYSBGb3JtR3JvdXAgaW5zdGFuY2UuIFBsZWFzZSBwYXNzIG9uZSBpbi5cblxuICAgICAgIEV4YW1wbGU6XG5cbiAgICAgICAke0V4YW1wbGVzLmZvcm1Db250cm9sTmFtZX1gKTtcbiAgfVxuXG4gIHN0YXRpYyBncm91cFBhcmVudEV4Y2VwdGlvbigpOiB2b2lkIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIGBmb3JtR3JvdXBOYW1lIG11c3QgYmUgdXNlZCB3aXRoIGEgcGFyZW50IGZvcm1Hcm91cCBkaXJlY3RpdmUuICBZb3UnbGwgd2FudCB0byBhZGQgYSBmb3JtR3JvdXBcbiAgICAgIGRpcmVjdGl2ZSBhbmQgcGFzcyBpdCBhbiBleGlzdGluZyBGb3JtR3JvdXAgaW5zdGFuY2UgKHlvdSBjYW4gY3JlYXRlIG9uZSBpbiB5b3VyIGNsYXNzKS5cblxuICAgICAgRXhhbXBsZTpcblxuICAgICAgJHtFeGFtcGxlcy5mb3JtR3JvdXBOYW1lfWApO1xuICB9XG5cbiAgc3RhdGljIGFycmF5UGFyZW50RXhjZXB0aW9uKCk6IHZvaWQge1xuICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgYGZvcm1BcnJheU5hbWUgbXVzdCBiZSB1c2VkIHdpdGggYSBwYXJlbnQgZm9ybUdyb3VwIGRpcmVjdGl2ZS4gIFlvdSdsbCB3YW50IHRvIGFkZCBhIGZvcm1Hcm91cFxuICAgICAgIGRpcmVjdGl2ZSBhbmQgcGFzcyBpdCBhbiBleGlzdGluZyBGb3JtR3JvdXAgaW5zdGFuY2UgKHlvdSBjYW4gY3JlYXRlIG9uZSBpbiB5b3VyIGNsYXNzKS5cblxuICAgICAgICBFeGFtcGxlOlxuXG4gICAgICAgICR7RXhhbXBsZXMuZm9ybUFycmF5TmFtZX1gKTtcbiAgfVxuXG4gIHN0YXRpYyBkaXNhYmxlZEF0dHJXYXJuaW5nKCk6IHZvaWQge1xuICAgIGNvbnNvbGUud2FybihgXG4gICAgICBJdCBsb29rcyBsaWtlIHlvdSdyZSB1c2luZyB0aGUgZGlzYWJsZWQgYXR0cmlidXRlIHdpdGggYSByZWFjdGl2ZSBmb3JtIGRpcmVjdGl2ZS4gSWYgeW91IHNldCBkaXNhYmxlZCB0byB0cnVlXG4gICAgICB3aGVuIHlvdSBzZXQgdXAgdGhpcyBjb250cm9sIGluIHlvdXIgY29tcG9uZW50IGNsYXNzLCB0aGUgZGlzYWJsZWQgYXR0cmlidXRlIHdpbGwgYWN0dWFsbHkgYmUgc2V0IGluIHRoZSBET00gZm9yXG4gICAgICB5b3UuIFdlIHJlY29tbWVuZCB1c2luZyB0aGlzIGFwcHJvYWNoIHRvIGF2b2lkICdjaGFuZ2VkIGFmdGVyIGNoZWNrZWQnIGVycm9ycy5cbiAgICAgICBcbiAgICAgIEV4YW1wbGU6IFxuICAgICAgZm9ybSA9IG5ldyBGb3JtR3JvdXAoe1xuICAgICAgICBmaXJzdDogbmV3IEZvcm1Db250cm9sKHt2YWx1ZTogJ05hbmN5JywgZGlzYWJsZWQ6IHRydWV9LCBWYWxpZGF0b3JzLnJlcXVpcmVkKSxcbiAgICAgICAgbGFzdDogbmV3IEZvcm1Db250cm9sKCdEcmV3JywgVmFsaWRhdG9ycy5yZXF1aXJlZClcbiAgICAgIH0pO1xuICAgIGApO1xuICB9XG5cbiAgc3RhdGljIG5nTW9kZWxXYXJuaW5nKGRpcmVjdGl2ZU5hbWU6IHN0cmluZyk6IHZvaWQge1xuICAgIGNvbnNvbGUud2FybihgXG4gICAgSXQgbG9va3MgbGlrZSB5b3UncmUgdXNpbmcgbmdNb2RlbCBvbiB0aGUgc2FtZSBmb3JtIGZpZWxkIGFzICR7ZGlyZWN0aXZlTmFtZX0uIFxuICAgIFN1cHBvcnQgZm9yIHVzaW5nIHRoZSBuZ01vZGVsIGlucHV0IHByb3BlcnR5IGFuZCBuZ01vZGVsQ2hhbmdlIGV2ZW50IHdpdGggXG4gICAgcmVhY3RpdmUgZm9ybSBkaXJlY3RpdmVzIGhhcyBiZWVuIGRlcHJlY2F0ZWQgaW4gQW5ndWxhciB2NiBhbmQgd2lsbCBiZSByZW1vdmVkIFxuICAgIGluIEFuZ3VsYXIgdjcuXG4gICAgXG4gICAgRm9yIG1vcmUgaW5mb3JtYXRpb24gb24gdGhpcywgc2VlIG91ciBBUEkgZG9jcyBoZXJlOlxuICAgIGh0dHBzOi8vYW5ndWxhci5pby9hcGkvZm9ybXMvJHtkaXJlY3RpdmVOYW1lID09PSAnZm9ybUNvbnRyb2wnID8gJ0Zvcm1Db250cm9sRGlyZWN0aXZlJyBcbiAgICAgIDogJ0Zvcm1Db250cm9sTmFtZSd9I3VzZS13aXRoLW5nbW9kZWxcbiAgICBgKTtcbiAgfVxufVxuIl19
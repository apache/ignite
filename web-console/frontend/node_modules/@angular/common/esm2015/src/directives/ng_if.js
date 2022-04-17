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
import { Directive, Input, TemplateRef, ViewContainerRef, ɵstringify as stringify } from '@angular/core';
/**
 * A structural directive that conditionally includes a template based on the value of
 * an expression coerced to Boolean.
 * When the expression evaluates to true, Angular renders the template
 * provided in a `then` clause, and when  false or null,
 * Angular renders the template provided in an optional `else` clause. The default
 * template for the `else` clause is blank.
 *
 * A [shorthand form](guide/structural-directives#the-asterisk--prefix) of the directive,
 * `*ngIf="condition"`, is generally used, provided
 * as an attribute of the anchor element for the inserted template.
 * Angular expands this into a more explicit version, in which the anchor element
 * is contained in an `<ng-template>` element.
 *
 * Simple form with shorthand syntax:
 *
 * ```
 * <div *ngIf="condition">Content to render when condition is true.</div>
 * ```
 *
 * Simple form with expanded syntax:
 *
 * ```
 * <ng-template [ngIf]="condition"><div>Content to render when condition is
 * true.</div></ng-template>
 * ```
 *
 * Form with an "else" block:
 *
 * ```
 * <div *ngIf="condition; else elseBlock">Content to render when condition is true.</div>
 * <ng-template #elseBlock>Content to render when condition is false.</ng-template>
 * ```
 *
 * Shorthand form with "then" and "else" blocks:
 *
 * ```
 * <div *ngIf="condition; then thenBlock else elseBlock"></div>
 * <ng-template #thenBlock>Content to render when condition is true.</ng-template>
 * <ng-template #elseBlock>Content to render when condition is false.</ng-template>
 * ```
 *
 * Form with storing the value locally:
 *
 * ```
 * <div *ngIf="condition as value; else elseBlock">{{value}}</div>
 * <ng-template #elseBlock>Content to render when value is null.</ng-template>
 * ```
 *
 * \@usageNotes
 *
 * The `*ngIf` directive is most commonly used to conditionally show an inline template,
 * as seen in the following  example.
 * The default `else` template is blank.
 *
 * {\@example common/ngIf/ts/module.ts region='NgIfSimple'}
 *
 * ### Showing an alternative template using `else`
 *
 * To display a template when `expression` evaluates to false, use an `else` template
 * binding as shown in the following example.
 * The `else` binding points to an `<ng-template>`  element labeled `#elseBlock`.
 * The template can be defined anywhere in the component view, but is typically placed right after
 * `ngIf` for readability.
 *
 * {\@example common/ngIf/ts/module.ts region='NgIfElse'}
 *
 * ### Using an external `then` template
 *
 * In the previous example, the then-clause template is specified inline, as the content of the
 * tag that contains the `ngIf` directive. You can also specify a template that is defined
 * externally, by referencing a labeled `<ng-template>` element. When you do this, you can
 * change which template to use at runtime, as shown in the following example.
 *
 * {\@example common/ngIf/ts/module.ts region='NgIfThenElse'}
 *
 * ### Storing a conditional result in a variable
 *
 * You might want to show a set of properties from the same object. If you are waiting
 * for asynchronous data, the object can be undefined.
 * In this case, you can use `ngIf` and store the result of the condition in a local
 * variable as shown in the the following example.
 *
 * {\@example common/ngIf/ts/module.ts region='NgIfAs'}
 *
 * This code uses only one `AsyncPipe`, so only one subscription is created.
 * The conditional statement stores the result of `userStream|async` in the local variable `user`.
 * You can then bind the local `user` repeatedly.
 *
 * The conditional displays the data only if `userStream` returns a value,
 * so you don't need to use the
 * [safe-navigation-operator](guide/template-syntax#safe-navigation-operator) (`?.`)
 * to guard against null values when accessing properties.
 * You can display an alternative template while waiting for the data.
 *
 * ### Shorthand syntax
 *
 * The shorthand syntax `*ngIf` expands into two separate template specifications
 * for the "then" and "else" clauses. For example, consider the following shorthand statement,
 * that is meant to show a loading page while waiting for data to be loaded.
 *
 * ```
 * <div class="hero-list" *ngIf="heroes else loading">
 *  ...
 * </div>
 *
 * <ng-template #loading>
 *  <div>Loading...</div>
 * </ng-template>
 * ```
 *
 * You can see that the "else" clause references the `<ng-template>`
 * with the `#loading` label, and the template for the "then" clause
 * is provided as the content of the anchor element.
 *
 * However, when Angular expands the shorthand syntax, it creates
 * another `<ng-template>` tag, with `ngIf` and `ngIfElse` directives.
 * The anchor element containing the template for the "then" clause becomes
 * the content of this unlabeled `<ng-template>` tag.
 *
 * ```
 * <ng-template [ngIf]="hero-list" [ngIfElse]="loading">
 *  <div class="hero-list">
 *   ...
 *  </div>
 * </ng-template>
 *
 * <ng-template #loading>
 *  <div>Loading...</div>
 * </ng-template>
 * ```
 *
 * The presence of the implicit template object has implications for the nesting of
 * structural directives. For more on this subject, see
 * [Structural Directives](https://angular.io/guide/structural-directives#one-per-element).
 *
 * \@ngModule CommonModule
 * \@publicApi
 */
export class NgIf {
    /**
     * @param {?} _viewContainer
     * @param {?} templateRef
     */
    constructor(_viewContainer, templateRef) {
        this._viewContainer = _viewContainer;
        this._context = new NgIfContext();
        this._thenTemplateRef = null;
        this._elseTemplateRef = null;
        this._thenViewRef = null;
        this._elseViewRef = null;
        this._thenTemplateRef = templateRef;
    }
    /**
     * The Boolean expression to evaluate as the condition for showing a template.
     * @param {?} condition
     * @return {?}
     */
    set ngIf(condition) {
        this._context.$implicit = this._context.ngIf = condition;
        this._updateView();
    }
    /**
     * A template to show if the condition expression evaluates to true.
     * @param {?} templateRef
     * @return {?}
     */
    set ngIfThen(templateRef) {
        assertTemplate('ngIfThen', templateRef);
        this._thenTemplateRef = templateRef;
        this._thenViewRef = null; // clear previous view if any.
        this._updateView();
    }
    /**
     * A template to show if the condition expression evaluates to false.
     * @param {?} templateRef
     * @return {?}
     */
    set ngIfElse(templateRef) {
        assertTemplate('ngIfElse', templateRef);
        this._elseTemplateRef = templateRef;
        this._elseViewRef = null; // clear previous view if any.
        this._updateView();
    }
    /**
     * @private
     * @return {?}
     */
    _updateView() {
        if (this._context.$implicit) {
            if (!this._thenViewRef) {
                this._viewContainer.clear();
                this._elseViewRef = null;
                if (this._thenTemplateRef) {
                    this._thenViewRef =
                        this._viewContainer.createEmbeddedView(this._thenTemplateRef, this._context);
                }
            }
        }
        else {
            if (!this._elseViewRef) {
                this._viewContainer.clear();
                this._thenViewRef = null;
                if (this._elseTemplateRef) {
                    this._elseViewRef =
                        this._viewContainer.createEmbeddedView(this._elseTemplateRef, this._context);
                }
            }
        }
    }
}
NgIf.decorators = [
    { type: Directive, args: [{ selector: '[ngIf]' },] }
];
/** @nocollapse */
NgIf.ctorParameters = () => [
    { type: ViewContainerRef },
    { type: TemplateRef }
];
NgIf.propDecorators = {
    ngIf: [{ type: Input }],
    ngIfThen: [{ type: Input }],
    ngIfElse: [{ type: Input }]
};
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    NgIf.ngIfUseIfTypeGuard;
    /**
     * Assert the correct type of the expression bound to the `ngIf` input within the template.
     *
     * The presence of this static field is a signal to the Ivy template type check compiler that
     * when the `NgIf` structural directive renders its template, the type of the expression bound
     * to `ngIf` should be narrowed in some way. For `NgIf`, the binding expression itself is used to
     * narrow its type, which allows the strictNullChecks feature of TypeScript to work with `NgIf`.
     * @type {?}
     */
    NgIf.ngTemplateGuard_ngIf;
    /**
     * @type {?}
     * @private
     */
    NgIf.prototype._context;
    /**
     * @type {?}
     * @private
     */
    NgIf.prototype._thenTemplateRef;
    /**
     * @type {?}
     * @private
     */
    NgIf.prototype._elseTemplateRef;
    /**
     * @type {?}
     * @private
     */
    NgIf.prototype._thenViewRef;
    /**
     * @type {?}
     * @private
     */
    NgIf.prototype._elseViewRef;
    /**
     * @type {?}
     * @private
     */
    NgIf.prototype._viewContainer;
}
/**
 * \@publicApi
 */
export class NgIfContext {
    constructor() {
        this.$implicit = null;
        this.ngIf = null;
    }
}
if (false) {
    /** @type {?} */
    NgIfContext.prototype.$implicit;
    /** @type {?} */
    NgIfContext.prototype.ngIf;
}
/**
 * @param {?} property
 * @param {?} templateRef
 * @return {?}
 */
function assertTemplate(property, templateRef) {
    /** @type {?} */
    const isTemplateRefOrNull = !!(!templateRef || templateRef.createEmbeddedView);
    if (!isTemplateRefOrNull) {
        throw new Error(`${property} must be a TemplateRef, but received '${stringify(templateRef)}'.`);
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfaWYuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vc3JjL2RpcmVjdGl2ZXMvbmdfaWYudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsU0FBUyxFQUFtQixLQUFLLEVBQUUsV0FBVyxFQUFFLGdCQUFnQixFQUFFLFVBQVUsSUFBSSxTQUFTLEVBQUMsTUFBTSxlQUFlLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBK0l4SCxNQUFNLE9BQU8sSUFBSTs7Ozs7SUFPZixZQUFvQixjQUFnQyxFQUFFLFdBQXFDO1FBQXZFLG1CQUFjLEdBQWQsY0FBYyxDQUFrQjtRQU41QyxhQUFRLEdBQWdCLElBQUksV0FBVyxFQUFFLENBQUM7UUFDMUMscUJBQWdCLEdBQWtDLElBQUksQ0FBQztRQUN2RCxxQkFBZ0IsR0FBa0MsSUFBSSxDQUFDO1FBQ3ZELGlCQUFZLEdBQXNDLElBQUksQ0FBQztRQUN2RCxpQkFBWSxHQUFzQyxJQUFJLENBQUM7UUFHN0QsSUFBSSxDQUFDLGdCQUFnQixHQUFHLFdBQVcsQ0FBQztJQUN0QyxDQUFDOzs7Ozs7SUFLRCxJQUNJLElBQUksQ0FBQyxTQUFjO1FBQ3JCLElBQUksQ0FBQyxRQUFRLENBQUMsU0FBUyxHQUFHLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxHQUFHLFNBQVMsQ0FBQztRQUN6RCxJQUFJLENBQUMsV0FBVyxFQUFFLENBQUM7SUFDckIsQ0FBQzs7Ozs7O0lBS0QsSUFDSSxRQUFRLENBQUMsV0FBMEM7UUFDckQsY0FBYyxDQUFDLFVBQVUsRUFBRSxXQUFXLENBQUMsQ0FBQztRQUN4QyxJQUFJLENBQUMsZ0JBQWdCLEdBQUcsV0FBVyxDQUFDO1FBQ3BDLElBQUksQ0FBQyxZQUFZLEdBQUcsSUFBSSxDQUFDLENBQUUsOEJBQThCO1FBQ3pELElBQUksQ0FBQyxXQUFXLEVBQUUsQ0FBQztJQUNyQixDQUFDOzs7Ozs7SUFLRCxJQUNJLFFBQVEsQ0FBQyxXQUEwQztRQUNyRCxjQUFjLENBQUMsVUFBVSxFQUFFLFdBQVcsQ0FBQyxDQUFDO1FBQ3hDLElBQUksQ0FBQyxnQkFBZ0IsR0FBRyxXQUFXLENBQUM7UUFDcEMsSUFBSSxDQUFDLFlBQVksR0FBRyxJQUFJLENBQUMsQ0FBRSw4QkFBOEI7UUFDekQsSUFBSSxDQUFDLFdBQVcsRUFBRSxDQUFDO0lBQ3JCLENBQUM7Ozs7O0lBRU8sV0FBVztRQUNqQixJQUFJLElBQUksQ0FBQyxRQUFRLENBQUMsU0FBUyxFQUFFO1lBQzNCLElBQUksQ0FBQyxJQUFJLENBQUMsWUFBWSxFQUFFO2dCQUN0QixJQUFJLENBQUMsY0FBYyxDQUFDLEtBQUssRUFBRSxDQUFDO2dCQUM1QixJQUFJLENBQUMsWUFBWSxHQUFHLElBQUksQ0FBQztnQkFDekIsSUFBSSxJQUFJLENBQUMsZ0JBQWdCLEVBQUU7b0JBQ3pCLElBQUksQ0FBQyxZQUFZO3dCQUNiLElBQUksQ0FBQyxjQUFjLENBQUMsa0JBQWtCLENBQUMsSUFBSSxDQUFDLGdCQUFnQixFQUFFLElBQUksQ0FBQyxRQUFRLENBQUMsQ0FBQztpQkFDbEY7YUFDRjtTQUNGO2FBQU07WUFDTCxJQUFJLENBQUMsSUFBSSxDQUFDLFlBQVksRUFBRTtnQkFDdEIsSUFBSSxDQUFDLGNBQWMsQ0FBQyxLQUFLLEVBQUUsQ0FBQztnQkFDNUIsSUFBSSxDQUFDLFlBQVksR0FBRyxJQUFJLENBQUM7Z0JBQ3pCLElBQUksSUFBSSxDQUFDLGdCQUFnQixFQUFFO29CQUN6QixJQUFJLENBQUMsWUFBWTt3QkFDYixJQUFJLENBQUMsY0FBYyxDQUFDLGtCQUFrQixDQUFDLElBQUksQ0FBQyxnQkFBZ0IsRUFBRSxJQUFJLENBQUMsUUFBUSxDQUFDLENBQUM7aUJBQ2xGO2FBQ0Y7U0FDRjtJQUNILENBQUM7OztZQS9ERixTQUFTLFNBQUMsRUFBQyxRQUFRLEVBQUUsUUFBUSxFQUFDOzs7O1lBOUl5QixnQkFBZ0I7WUFBN0IsV0FBVzs7O21CQTZKbkQsS0FBSzt1QkFTTCxLQUFLO3VCQVdMLEtBQUs7Ozs7Ozs7SUErQk4sd0JBQXVDOzs7Ozs7Ozs7O0lBVXZDLDBCQUF1Qzs7Ozs7SUExRXZDLHdCQUFrRDs7Ozs7SUFDbEQsZ0NBQStEOzs7OztJQUMvRCxnQ0FBK0Q7Ozs7O0lBQy9ELDRCQUErRDs7Ozs7SUFDL0QsNEJBQStEOzs7OztJQUVuRCw4QkFBd0M7Ozs7O0FBMEV0RCxNQUFNLE9BQU8sV0FBVztJQUF4QjtRQUNTLGNBQVMsR0FBUSxJQUFJLENBQUM7UUFDdEIsU0FBSSxHQUFRLElBQUksQ0FBQztJQUMxQixDQUFDO0NBQUE7OztJQUZDLGdDQUE2Qjs7SUFDN0IsMkJBQXdCOzs7Ozs7O0FBRzFCLFNBQVMsY0FBYyxDQUFDLFFBQWdCLEVBQUUsV0FBbUM7O1VBQ3JFLG1CQUFtQixHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsV0FBVyxJQUFJLFdBQVcsQ0FBQyxrQkFBa0IsQ0FBQztJQUM5RSxJQUFJLENBQUMsbUJBQW1CLEVBQUU7UUFDeEIsTUFBTSxJQUFJLEtBQUssQ0FBQyxHQUFHLFFBQVEseUNBQXlDLFNBQVMsQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLENBQUM7S0FDakc7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0RpcmVjdGl2ZSwgRW1iZWRkZWRWaWV3UmVmLCBJbnB1dCwgVGVtcGxhdGVSZWYsIFZpZXdDb250YWluZXJSZWYsIMm1c3RyaW5naWZ5IGFzIHN0cmluZ2lmeX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cblxuLyoqXG4gKiBBIHN0cnVjdHVyYWwgZGlyZWN0aXZlIHRoYXQgY29uZGl0aW9uYWxseSBpbmNsdWRlcyBhIHRlbXBsYXRlIGJhc2VkIG9uIHRoZSB2YWx1ZSBvZlxuICogYW4gZXhwcmVzc2lvbiBjb2VyY2VkIHRvIEJvb2xlYW4uXG4gKiBXaGVuIHRoZSBleHByZXNzaW9uIGV2YWx1YXRlcyB0byB0cnVlLCBBbmd1bGFyIHJlbmRlcnMgdGhlIHRlbXBsYXRlXG4gKiBwcm92aWRlZCBpbiBhIGB0aGVuYCBjbGF1c2UsIGFuZCB3aGVuICBmYWxzZSBvciBudWxsLFxuICogQW5ndWxhciByZW5kZXJzIHRoZSB0ZW1wbGF0ZSBwcm92aWRlZCBpbiBhbiBvcHRpb25hbCBgZWxzZWAgY2xhdXNlLiBUaGUgZGVmYXVsdFxuICogdGVtcGxhdGUgZm9yIHRoZSBgZWxzZWAgY2xhdXNlIGlzIGJsYW5rLlxuICpcbiAqIEEgW3Nob3J0aGFuZCBmb3JtXShndWlkZS9zdHJ1Y3R1cmFsLWRpcmVjdGl2ZXMjdGhlLWFzdGVyaXNrLS1wcmVmaXgpIG9mIHRoZSBkaXJlY3RpdmUsXG4gKiBgKm5nSWY9XCJjb25kaXRpb25cImAsIGlzIGdlbmVyYWxseSB1c2VkLCBwcm92aWRlZFxuICogYXMgYW4gYXR0cmlidXRlIG9mIHRoZSBhbmNob3IgZWxlbWVudCBmb3IgdGhlIGluc2VydGVkIHRlbXBsYXRlLlxuICogQW5ndWxhciBleHBhbmRzIHRoaXMgaW50byBhIG1vcmUgZXhwbGljaXQgdmVyc2lvbiwgaW4gd2hpY2ggdGhlIGFuY2hvciBlbGVtZW50XG4gKiBpcyBjb250YWluZWQgaW4gYW4gYDxuZy10ZW1wbGF0ZT5gIGVsZW1lbnQuXG4gKlxuICogU2ltcGxlIGZvcm0gd2l0aCBzaG9ydGhhbmQgc3ludGF4OlxuICpcbiAqIGBgYFxuICogPGRpdiAqbmdJZj1cImNvbmRpdGlvblwiPkNvbnRlbnQgdG8gcmVuZGVyIHdoZW4gY29uZGl0aW9uIGlzIHRydWUuPC9kaXY+XG4gKiBgYGBcbiAqXG4gKiBTaW1wbGUgZm9ybSB3aXRoIGV4cGFuZGVkIHN5bnRheDpcbiAqXG4gKiBgYGBcbiAqIDxuZy10ZW1wbGF0ZSBbbmdJZl09XCJjb25kaXRpb25cIj48ZGl2PkNvbnRlbnQgdG8gcmVuZGVyIHdoZW4gY29uZGl0aW9uIGlzXG4gKiB0cnVlLjwvZGl2PjwvbmctdGVtcGxhdGU+XG4gKiBgYGBcbiAqXG4gKiBGb3JtIHdpdGggYW4gXCJlbHNlXCIgYmxvY2s6XG4gKlxuICogYGBgXG4gKiA8ZGl2ICpuZ0lmPVwiY29uZGl0aW9uOyBlbHNlIGVsc2VCbG9ja1wiPkNvbnRlbnQgdG8gcmVuZGVyIHdoZW4gY29uZGl0aW9uIGlzIHRydWUuPC9kaXY+XG4gKiA8bmctdGVtcGxhdGUgI2Vsc2VCbG9jaz5Db250ZW50IHRvIHJlbmRlciB3aGVuIGNvbmRpdGlvbiBpcyBmYWxzZS48L25nLXRlbXBsYXRlPlxuICogYGBgXG4gKlxuICogU2hvcnRoYW5kIGZvcm0gd2l0aCBcInRoZW5cIiBhbmQgXCJlbHNlXCIgYmxvY2tzOlxuICpcbiAqIGBgYFxuICogPGRpdiAqbmdJZj1cImNvbmRpdGlvbjsgdGhlbiB0aGVuQmxvY2sgZWxzZSBlbHNlQmxvY2tcIj48L2Rpdj5cbiAqIDxuZy10ZW1wbGF0ZSAjdGhlbkJsb2NrPkNvbnRlbnQgdG8gcmVuZGVyIHdoZW4gY29uZGl0aW9uIGlzIHRydWUuPC9uZy10ZW1wbGF0ZT5cbiAqIDxuZy10ZW1wbGF0ZSAjZWxzZUJsb2NrPkNvbnRlbnQgdG8gcmVuZGVyIHdoZW4gY29uZGl0aW9uIGlzIGZhbHNlLjwvbmctdGVtcGxhdGU+XG4gKiBgYGBcbiAqXG4gKiBGb3JtIHdpdGggc3RvcmluZyB0aGUgdmFsdWUgbG9jYWxseTpcbiAqXG4gKiBgYGBcbiAqIDxkaXYgKm5nSWY9XCJjb25kaXRpb24gYXMgdmFsdWU7IGVsc2UgZWxzZUJsb2NrXCI+e3t2YWx1ZX19PC9kaXY+XG4gKiA8bmctdGVtcGxhdGUgI2Vsc2VCbG9jaz5Db250ZW50IHRvIHJlbmRlciB3aGVuIHZhbHVlIGlzIG51bGwuPC9uZy10ZW1wbGF0ZT5cbiAqIGBgYFxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKlxuICogVGhlIGAqbmdJZmAgZGlyZWN0aXZlIGlzIG1vc3QgY29tbW9ubHkgdXNlZCB0byBjb25kaXRpb25hbGx5IHNob3cgYW4gaW5saW5lIHRlbXBsYXRlLFxuICogYXMgc2VlbiBpbiB0aGUgZm9sbG93aW5nICBleGFtcGxlLlxuICogVGhlIGRlZmF1bHQgYGVsc2VgIHRlbXBsYXRlIGlzIGJsYW5rLlxuICpcbiAqIHtAZXhhbXBsZSBjb21tb24vbmdJZi90cy9tb2R1bGUudHMgcmVnaW9uPSdOZ0lmU2ltcGxlJ31cbiAqXG4gKiAjIyMgU2hvd2luZyBhbiBhbHRlcm5hdGl2ZSB0ZW1wbGF0ZSB1c2luZyBgZWxzZWBcbiAqXG4gKiBUbyBkaXNwbGF5IGEgdGVtcGxhdGUgd2hlbiBgZXhwcmVzc2lvbmAgZXZhbHVhdGVzIHRvIGZhbHNlLCB1c2UgYW4gYGVsc2VgIHRlbXBsYXRlXG4gKiBiaW5kaW5nIGFzIHNob3duIGluIHRoZSBmb2xsb3dpbmcgZXhhbXBsZS5cbiAqIFRoZSBgZWxzZWAgYmluZGluZyBwb2ludHMgdG8gYW4gYDxuZy10ZW1wbGF0ZT5gICBlbGVtZW50IGxhYmVsZWQgYCNlbHNlQmxvY2tgLlxuICogVGhlIHRlbXBsYXRlIGNhbiBiZSBkZWZpbmVkIGFueXdoZXJlIGluIHRoZSBjb21wb25lbnQgdmlldywgYnV0IGlzIHR5cGljYWxseSBwbGFjZWQgcmlnaHQgYWZ0ZXJcbiAqIGBuZ0lmYCBmb3IgcmVhZGFiaWxpdHkuXG4gKlxuICoge0BleGFtcGxlIGNvbW1vbi9uZ0lmL3RzL21vZHVsZS50cyByZWdpb249J05nSWZFbHNlJ31cbiAqXG4gKiAjIyMgVXNpbmcgYW4gZXh0ZXJuYWwgYHRoZW5gIHRlbXBsYXRlXG4gKlxuICogSW4gdGhlIHByZXZpb3VzIGV4YW1wbGUsIHRoZSB0aGVuLWNsYXVzZSB0ZW1wbGF0ZSBpcyBzcGVjaWZpZWQgaW5saW5lLCBhcyB0aGUgY29udGVudCBvZiB0aGVcbiAqIHRhZyB0aGF0IGNvbnRhaW5zIHRoZSBgbmdJZmAgZGlyZWN0aXZlLiBZb3UgY2FuIGFsc28gc3BlY2lmeSBhIHRlbXBsYXRlIHRoYXQgaXMgZGVmaW5lZFxuICogZXh0ZXJuYWxseSwgYnkgcmVmZXJlbmNpbmcgYSBsYWJlbGVkIGA8bmctdGVtcGxhdGU+YCBlbGVtZW50LiBXaGVuIHlvdSBkbyB0aGlzLCB5b3UgY2FuXG4gKiBjaGFuZ2Ugd2hpY2ggdGVtcGxhdGUgdG8gdXNlIGF0IHJ1bnRpbWUsIGFzIHNob3duIGluIHRoZSBmb2xsb3dpbmcgZXhhbXBsZS5cbiAqXG4gKiB7QGV4YW1wbGUgY29tbW9uL25nSWYvdHMvbW9kdWxlLnRzIHJlZ2lvbj0nTmdJZlRoZW5FbHNlJ31cbiAqXG4gKiAjIyMgU3RvcmluZyBhIGNvbmRpdGlvbmFsIHJlc3VsdCBpbiBhIHZhcmlhYmxlXG4gKlxuICogWW91IG1pZ2h0IHdhbnQgdG8gc2hvdyBhIHNldCBvZiBwcm9wZXJ0aWVzIGZyb20gdGhlIHNhbWUgb2JqZWN0LiBJZiB5b3UgYXJlIHdhaXRpbmdcbiAqIGZvciBhc3luY2hyb25vdXMgZGF0YSwgdGhlIG9iamVjdCBjYW4gYmUgdW5kZWZpbmVkLlxuICogSW4gdGhpcyBjYXNlLCB5b3UgY2FuIHVzZSBgbmdJZmAgYW5kIHN0b3JlIHRoZSByZXN1bHQgb2YgdGhlIGNvbmRpdGlvbiBpbiBhIGxvY2FsXG4gKiB2YXJpYWJsZSBhcyBzaG93biBpbiB0aGUgdGhlIGZvbGxvd2luZyBleGFtcGxlLlxuICpcbiAqIHtAZXhhbXBsZSBjb21tb24vbmdJZi90cy9tb2R1bGUudHMgcmVnaW9uPSdOZ0lmQXMnfVxuICpcbiAqIFRoaXMgY29kZSB1c2VzIG9ubHkgb25lIGBBc3luY1BpcGVgLCBzbyBvbmx5IG9uZSBzdWJzY3JpcHRpb24gaXMgY3JlYXRlZC5cbiAqIFRoZSBjb25kaXRpb25hbCBzdGF0ZW1lbnQgc3RvcmVzIHRoZSByZXN1bHQgb2YgYHVzZXJTdHJlYW18YXN5bmNgIGluIHRoZSBsb2NhbCB2YXJpYWJsZSBgdXNlcmAuXG4gKiBZb3UgY2FuIHRoZW4gYmluZCB0aGUgbG9jYWwgYHVzZXJgIHJlcGVhdGVkbHkuXG4gKlxuICogVGhlIGNvbmRpdGlvbmFsIGRpc3BsYXlzIHRoZSBkYXRhIG9ubHkgaWYgYHVzZXJTdHJlYW1gIHJldHVybnMgYSB2YWx1ZSxcbiAqIHNvIHlvdSBkb24ndCBuZWVkIHRvIHVzZSB0aGVcbiAqIFtzYWZlLW5hdmlnYXRpb24tb3BlcmF0b3JdKGd1aWRlL3RlbXBsYXRlLXN5bnRheCNzYWZlLW5hdmlnYXRpb24tb3BlcmF0b3IpIChgPy5gKVxuICogdG8gZ3VhcmQgYWdhaW5zdCBudWxsIHZhbHVlcyB3aGVuIGFjY2Vzc2luZyBwcm9wZXJ0aWVzLlxuICogWW91IGNhbiBkaXNwbGF5IGFuIGFsdGVybmF0aXZlIHRlbXBsYXRlIHdoaWxlIHdhaXRpbmcgZm9yIHRoZSBkYXRhLlxuICpcbiAqICMjIyBTaG9ydGhhbmQgc3ludGF4XG4gKlxuICogVGhlIHNob3J0aGFuZCBzeW50YXggYCpuZ0lmYCBleHBhbmRzIGludG8gdHdvIHNlcGFyYXRlIHRlbXBsYXRlIHNwZWNpZmljYXRpb25zXG4gKiBmb3IgdGhlIFwidGhlblwiIGFuZCBcImVsc2VcIiBjbGF1c2VzLiBGb3IgZXhhbXBsZSwgY29uc2lkZXIgdGhlIGZvbGxvd2luZyBzaG9ydGhhbmQgc3RhdGVtZW50LFxuICogdGhhdCBpcyBtZWFudCB0byBzaG93IGEgbG9hZGluZyBwYWdlIHdoaWxlIHdhaXRpbmcgZm9yIGRhdGEgdG8gYmUgbG9hZGVkLlxuICpcbiAqIGBgYFxuICogPGRpdiBjbGFzcz1cImhlcm8tbGlzdFwiICpuZ0lmPVwiaGVyb2VzIGVsc2UgbG9hZGluZ1wiPlxuICogIC4uLlxuICogPC9kaXY+XG4gKlxuICogPG5nLXRlbXBsYXRlICNsb2FkaW5nPlxuICogIDxkaXY+TG9hZGluZy4uLjwvZGl2PlxuICogPC9uZy10ZW1wbGF0ZT5cbiAqIGBgYFxuICpcbiAqIFlvdSBjYW4gc2VlIHRoYXQgdGhlIFwiZWxzZVwiIGNsYXVzZSByZWZlcmVuY2VzIHRoZSBgPG5nLXRlbXBsYXRlPmBcbiAqIHdpdGggdGhlIGAjbG9hZGluZ2AgbGFiZWwsIGFuZCB0aGUgdGVtcGxhdGUgZm9yIHRoZSBcInRoZW5cIiBjbGF1c2VcbiAqIGlzIHByb3ZpZGVkIGFzIHRoZSBjb250ZW50IG9mIHRoZSBhbmNob3IgZWxlbWVudC5cbiAqXG4gKiBIb3dldmVyLCB3aGVuIEFuZ3VsYXIgZXhwYW5kcyB0aGUgc2hvcnRoYW5kIHN5bnRheCwgaXQgY3JlYXRlc1xuICogYW5vdGhlciBgPG5nLXRlbXBsYXRlPmAgdGFnLCB3aXRoIGBuZ0lmYCBhbmQgYG5nSWZFbHNlYCBkaXJlY3RpdmVzLlxuICogVGhlIGFuY2hvciBlbGVtZW50IGNvbnRhaW5pbmcgdGhlIHRlbXBsYXRlIGZvciB0aGUgXCJ0aGVuXCIgY2xhdXNlIGJlY29tZXNcbiAqIHRoZSBjb250ZW50IG9mIHRoaXMgdW5sYWJlbGVkIGA8bmctdGVtcGxhdGU+YCB0YWcuXG4gKlxuICogYGBgXG4gKiA8bmctdGVtcGxhdGUgW25nSWZdPVwiaGVyby1saXN0XCIgW25nSWZFbHNlXT1cImxvYWRpbmdcIj5cbiAqICA8ZGl2IGNsYXNzPVwiaGVyby1saXN0XCI+XG4gKiAgIC4uLlxuICogIDwvZGl2PlxuICogPC9uZy10ZW1wbGF0ZT5cbiAqXG4gKiA8bmctdGVtcGxhdGUgI2xvYWRpbmc+XG4gKiAgPGRpdj5Mb2FkaW5nLi4uPC9kaXY+XG4gKiA8L25nLXRlbXBsYXRlPlxuICogYGBgXG4gKlxuICogVGhlIHByZXNlbmNlIG9mIHRoZSBpbXBsaWNpdCB0ZW1wbGF0ZSBvYmplY3QgaGFzIGltcGxpY2F0aW9ucyBmb3IgdGhlIG5lc3Rpbmcgb2ZcbiAqIHN0cnVjdHVyYWwgZGlyZWN0aXZlcy4gRm9yIG1vcmUgb24gdGhpcyBzdWJqZWN0LCBzZWVcbiAqIFtTdHJ1Y3R1cmFsIERpcmVjdGl2ZXNdKGh0dHBzOi8vYW5ndWxhci5pby9ndWlkZS9zdHJ1Y3R1cmFsLWRpcmVjdGl2ZXMjb25lLXBlci1lbGVtZW50KS5cbiAqXG4gKiBAbmdNb2R1bGUgQ29tbW9uTW9kdWxlXG4gKiBAcHVibGljQXBpXG4gKi9cbkBEaXJlY3RpdmUoe3NlbGVjdG9yOiAnW25nSWZdJ30pXG5leHBvcnQgY2xhc3MgTmdJZiB7XG4gIHByaXZhdGUgX2NvbnRleHQ6IE5nSWZDb250ZXh0ID0gbmV3IE5nSWZDb250ZXh0KCk7XG4gIHByaXZhdGUgX3RoZW5UZW1wbGF0ZVJlZjogVGVtcGxhdGVSZWY8TmdJZkNvbnRleHQ+fG51bGwgPSBudWxsO1xuICBwcml2YXRlIF9lbHNlVGVtcGxhdGVSZWY6IFRlbXBsYXRlUmVmPE5nSWZDb250ZXh0PnxudWxsID0gbnVsbDtcbiAgcHJpdmF0ZSBfdGhlblZpZXdSZWY6IEVtYmVkZGVkVmlld1JlZjxOZ0lmQ29udGV4dD58bnVsbCA9IG51bGw7XG4gIHByaXZhdGUgX2Vsc2VWaWV3UmVmOiBFbWJlZGRlZFZpZXdSZWY8TmdJZkNvbnRleHQ+fG51bGwgPSBudWxsO1xuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgX3ZpZXdDb250YWluZXI6IFZpZXdDb250YWluZXJSZWYsIHRlbXBsYXRlUmVmOiBUZW1wbGF0ZVJlZjxOZ0lmQ29udGV4dD4pIHtcbiAgICB0aGlzLl90aGVuVGVtcGxhdGVSZWYgPSB0ZW1wbGF0ZVJlZjtcbiAgfVxuXG4gIC8qKlxuICAgKiBUaGUgQm9vbGVhbiBleHByZXNzaW9uIHRvIGV2YWx1YXRlIGFzIHRoZSBjb25kaXRpb24gZm9yIHNob3dpbmcgYSB0ZW1wbGF0ZS5cbiAgICovXG4gIEBJbnB1dCgpXG4gIHNldCBuZ0lmKGNvbmRpdGlvbjogYW55KSB7XG4gICAgdGhpcy5fY29udGV4dC4kaW1wbGljaXQgPSB0aGlzLl9jb250ZXh0Lm5nSWYgPSBjb25kaXRpb247XG4gICAgdGhpcy5fdXBkYXRlVmlldygpO1xuICB9XG5cbiAgLyoqXG4gICAqIEEgdGVtcGxhdGUgdG8gc2hvdyBpZiB0aGUgY29uZGl0aW9uIGV4cHJlc3Npb24gZXZhbHVhdGVzIHRvIHRydWUuXG4gICAqL1xuICBASW5wdXQoKVxuICBzZXQgbmdJZlRoZW4odGVtcGxhdGVSZWY6IFRlbXBsYXRlUmVmPE5nSWZDb250ZXh0PnxudWxsKSB7XG4gICAgYXNzZXJ0VGVtcGxhdGUoJ25nSWZUaGVuJywgdGVtcGxhdGVSZWYpO1xuICAgIHRoaXMuX3RoZW5UZW1wbGF0ZVJlZiA9IHRlbXBsYXRlUmVmO1xuICAgIHRoaXMuX3RoZW5WaWV3UmVmID0gbnVsbDsgIC8vIGNsZWFyIHByZXZpb3VzIHZpZXcgaWYgYW55LlxuICAgIHRoaXMuX3VwZGF0ZVZpZXcoKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBBIHRlbXBsYXRlIHRvIHNob3cgaWYgdGhlIGNvbmRpdGlvbiBleHByZXNzaW9uIGV2YWx1YXRlcyB0byBmYWxzZS5cbiAgICovXG4gIEBJbnB1dCgpXG4gIHNldCBuZ0lmRWxzZSh0ZW1wbGF0ZVJlZjogVGVtcGxhdGVSZWY8TmdJZkNvbnRleHQ+fG51bGwpIHtcbiAgICBhc3NlcnRUZW1wbGF0ZSgnbmdJZkVsc2UnLCB0ZW1wbGF0ZVJlZik7XG4gICAgdGhpcy5fZWxzZVRlbXBsYXRlUmVmID0gdGVtcGxhdGVSZWY7XG4gICAgdGhpcy5fZWxzZVZpZXdSZWYgPSBudWxsOyAgLy8gY2xlYXIgcHJldmlvdXMgdmlldyBpZiBhbnkuXG4gICAgdGhpcy5fdXBkYXRlVmlldygpO1xuICB9XG5cbiAgcHJpdmF0ZSBfdXBkYXRlVmlldygpIHtcbiAgICBpZiAodGhpcy5fY29udGV4dC4kaW1wbGljaXQpIHtcbiAgICAgIGlmICghdGhpcy5fdGhlblZpZXdSZWYpIHtcbiAgICAgICAgdGhpcy5fdmlld0NvbnRhaW5lci5jbGVhcigpO1xuICAgICAgICB0aGlzLl9lbHNlVmlld1JlZiA9IG51bGw7XG4gICAgICAgIGlmICh0aGlzLl90aGVuVGVtcGxhdGVSZWYpIHtcbiAgICAgICAgICB0aGlzLl90aGVuVmlld1JlZiA9XG4gICAgICAgICAgICAgIHRoaXMuX3ZpZXdDb250YWluZXIuY3JlYXRlRW1iZWRkZWRWaWV3KHRoaXMuX3RoZW5UZW1wbGF0ZVJlZiwgdGhpcy5fY29udGV4dCk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKCF0aGlzLl9lbHNlVmlld1JlZikge1xuICAgICAgICB0aGlzLl92aWV3Q29udGFpbmVyLmNsZWFyKCk7XG4gICAgICAgIHRoaXMuX3RoZW5WaWV3UmVmID0gbnVsbDtcbiAgICAgICAgaWYgKHRoaXMuX2Vsc2VUZW1wbGF0ZVJlZikge1xuICAgICAgICAgIHRoaXMuX2Vsc2VWaWV3UmVmID1cbiAgICAgICAgICAgICAgdGhpcy5fdmlld0NvbnRhaW5lci5jcmVhdGVFbWJlZGRlZFZpZXcodGhpcy5fZWxzZVRlbXBsYXRlUmVmLCB0aGlzLl9jb250ZXh0KTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgcHVibGljIHN0YXRpYyBuZ0lmVXNlSWZUeXBlR3VhcmQ6IHZvaWQ7XG5cbiAgLyoqXG4gICAqIEFzc2VydCB0aGUgY29ycmVjdCB0eXBlIG9mIHRoZSBleHByZXNzaW9uIGJvdW5kIHRvIHRoZSBgbmdJZmAgaW5wdXQgd2l0aGluIHRoZSB0ZW1wbGF0ZS5cbiAgICpcbiAgICogVGhlIHByZXNlbmNlIG9mIHRoaXMgc3RhdGljIGZpZWxkIGlzIGEgc2lnbmFsIHRvIHRoZSBJdnkgdGVtcGxhdGUgdHlwZSBjaGVjayBjb21waWxlciB0aGF0XG4gICAqIHdoZW4gdGhlIGBOZ0lmYCBzdHJ1Y3R1cmFsIGRpcmVjdGl2ZSByZW5kZXJzIGl0cyB0ZW1wbGF0ZSwgdGhlIHR5cGUgb2YgdGhlIGV4cHJlc3Npb24gYm91bmRcbiAgICogdG8gYG5nSWZgIHNob3VsZCBiZSBuYXJyb3dlZCBpbiBzb21lIHdheS4gRm9yIGBOZ0lmYCwgdGhlIGJpbmRpbmcgZXhwcmVzc2lvbiBpdHNlbGYgaXMgdXNlZCB0b1xuICAgKiBuYXJyb3cgaXRzIHR5cGUsIHdoaWNoIGFsbG93cyB0aGUgc3RyaWN0TnVsbENoZWNrcyBmZWF0dXJlIG9mIFR5cGVTY3JpcHQgdG8gd29yayB3aXRoIGBOZ0lmYC5cbiAgICovXG4gIHN0YXRpYyBuZ1RlbXBsYXRlR3VhcmRfbmdJZjogJ2JpbmRpbmcnO1xufVxuXG4vKipcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIE5nSWZDb250ZXh0IHtcbiAgcHVibGljICRpbXBsaWNpdDogYW55ID0gbnVsbDtcbiAgcHVibGljIG5nSWY6IGFueSA9IG51bGw7XG59XG5cbmZ1bmN0aW9uIGFzc2VydFRlbXBsYXRlKHByb3BlcnR5OiBzdHJpbmcsIHRlbXBsYXRlUmVmOiBUZW1wbGF0ZVJlZjxhbnk+fCBudWxsKTogdm9pZCB7XG4gIGNvbnN0IGlzVGVtcGxhdGVSZWZPck51bGwgPSAhISghdGVtcGxhdGVSZWYgfHwgdGVtcGxhdGVSZWYuY3JlYXRlRW1iZWRkZWRWaWV3KTtcbiAgaWYgKCFpc1RlbXBsYXRlUmVmT3JOdWxsKSB7XG4gICAgdGhyb3cgbmV3IEVycm9yKGAke3Byb3BlcnR5fSBtdXN0IGJlIGEgVGVtcGxhdGVSZWYsIGJ1dCByZWNlaXZlZCAnJHtzdHJpbmdpZnkodGVtcGxhdGVSZWYpfScuYCk7XG4gIH1cbn1cbiJdfQ==
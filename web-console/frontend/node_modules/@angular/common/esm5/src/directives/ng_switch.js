/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Directive, Host, Input, TemplateRef, ViewContainerRef } from '@angular/core';
var SwitchView = /** @class */ (function () {
    function SwitchView(_viewContainerRef, _templateRef) {
        this._viewContainerRef = _viewContainerRef;
        this._templateRef = _templateRef;
        this._created = false;
    }
    SwitchView.prototype.create = function () {
        this._created = true;
        this._viewContainerRef.createEmbeddedView(this._templateRef);
    };
    SwitchView.prototype.destroy = function () {
        this._created = false;
        this._viewContainerRef.clear();
    };
    SwitchView.prototype.enforceState = function (created) {
        if (created && !this._created) {
            this.create();
        }
        else if (!created && this._created) {
            this.destroy();
        }
    };
    return SwitchView;
}());
export { SwitchView };
/**
 * @ngModule CommonModule
 *
 * @description A structural directive that adds or removes templates (displaying or hiding views)
 * when the next match expression matches the switch expression.
 *
 * The `[ngSwitch]` directive on a container specifies an expression to match against.
 * The expressions to match are provided by `ngSwitchCase` directives on views within the container.
 * - Every view that matches is rendered.
 * - If there are no matches, a view with the `ngSwitchDefault` directive is rendered.
 * - Elements within the `[NgSwitch]` statement but outside of any `NgSwitchCase`
 * or `ngSwitchDefault` directive are preserved at the location.
 *
 * @usageNotes
 * Define a container element for the directive, and specify the switch expression
 * to match against as an attribute:
 *
 * ```
 * <container-element [ngSwitch]="switch_expression">
 * ```
 *
 * Within the container, `*ngSwitchCase` statements specify the match expressions
 * as attributes. Include `*ngSwitchDefault` as the final case.
 *
 * ```
 * <container-element [ngSwitch]="switch_expression">
 *    <some-element *ngSwitchCase="match_expression_1">...</some-element>
 * ...
 *    <some-element *ngSwitchDefault>...</some-element>
 * </container-element>
 * ```
 *
 * ### Usage Examples
 *
 * The following example shows how to use more than one case to display the same view:
 *
 * ```
 * <container-element [ngSwitch]="switch_expression">
 *   <!-- the same view can be shown in more than one case -->
 *   <some-element *ngSwitchCase="match_expression_1">...</some-element>
 *   <some-element *ngSwitchCase="match_expression_2">...</some-element>
 *   <some-other-element *ngSwitchCase="match_expression_3">...</some-other-element>
 *   <!--default case when there are no matches -->
 *   <some-element *ngSwitchDefault>...</some-element>
 * </container-element>
 * ```
 *
 * The following example shows how cases can be nested:
 * ```
 * <container-element [ngSwitch]="switch_expression">
 *       <some-element *ngSwitchCase="match_expression_1">...</some-element>
 *       <some-element *ngSwitchCase="match_expression_2">...</some-element>
 *       <some-other-element *ngSwitchCase="match_expression_3">...</some-other-element>
 *       <ng-container *ngSwitchCase="match_expression_3">
 *         <!-- use a ng-container to group multiple root nodes -->
 *         <inner-element></inner-element>
 *         <inner-other-element></inner-other-element>
 *       </ng-container>
 *       <some-element *ngSwitchDefault>...</some-element>
 *     </container-element>
 * ```
 *
 * @publicApi
 * @see `NgSwitchCase`
 * @see `NgSwitchDefault`
 * @see [Structural Directives](guide/structural-directives)
 *
 */
var NgSwitch = /** @class */ (function () {
    function NgSwitch() {
        this._defaultUsed = false;
        this._caseCount = 0;
        this._lastCaseCheckIndex = 0;
        this._lastCasesMatched = false;
    }
    Object.defineProperty(NgSwitch.prototype, "ngSwitch", {
        set: function (newValue) {
            this._ngSwitch = newValue;
            if (this._caseCount === 0) {
                this._updateDefaultCases(true);
            }
        },
        enumerable: true,
        configurable: true
    });
    /** @internal */
    NgSwitch.prototype._addCase = function () { return this._caseCount++; };
    /** @internal */
    NgSwitch.prototype._addDefault = function (view) {
        if (!this._defaultViews) {
            this._defaultViews = [];
        }
        this._defaultViews.push(view);
    };
    /** @internal */
    NgSwitch.prototype._matchCase = function (value) {
        var matched = value == this._ngSwitch;
        this._lastCasesMatched = this._lastCasesMatched || matched;
        this._lastCaseCheckIndex++;
        if (this._lastCaseCheckIndex === this._caseCount) {
            this._updateDefaultCases(!this._lastCasesMatched);
            this._lastCaseCheckIndex = 0;
            this._lastCasesMatched = false;
        }
        return matched;
    };
    NgSwitch.prototype._updateDefaultCases = function (useDefault) {
        if (this._defaultViews && useDefault !== this._defaultUsed) {
            this._defaultUsed = useDefault;
            for (var i = 0; i < this._defaultViews.length; i++) {
                var defaultView = this._defaultViews[i];
                defaultView.enforceState(useDefault);
            }
        }
    };
    tslib_1.__decorate([
        Input(),
        tslib_1.__metadata("design:type", Object),
        tslib_1.__metadata("design:paramtypes", [Object])
    ], NgSwitch.prototype, "ngSwitch", null);
    NgSwitch = tslib_1.__decorate([
        Directive({ selector: '[ngSwitch]' })
    ], NgSwitch);
    return NgSwitch;
}());
export { NgSwitch };
/**
 * @ngModule CommonModule
 *
 * @description
 * Provides a switch case expression to match against an enclosing `ngSwitch` expression.
 * When the expressions match, the given `NgSwitchCase` template is rendered.
 * If multiple match expressions match the switch expression value, all of them are displayed.
 *
 * @usageNotes
 *
 * Within a switch container, `*ngSwitchCase` statements specify the match expressions
 * as attributes. Include `*ngSwitchDefault` as the final case.
 *
 * ```
 * <container-element [ngSwitch]="switch_expression">
 *   <some-element *ngSwitchCase="match_expression_1">...</some-element>
 *   ...
 *   <some-element *ngSwitchDefault>...</some-element>
 * </container-element>
 * ```
 *
 * Each switch-case statement contains an in-line HTML template or template reference
 * that defines the subtree to be selected if the value of the match expression
 * matches the value of the switch expression.
 *
 * Unlike JavaScript, which uses strict equality, Angular uses loose equality.
 * This means that the empty string, `""` matches 0.
 *
 * @publicApi
 * @see `NgSwitch`
 * @see `NgSwitchDefault`
 *
 */
var NgSwitchCase = /** @class */ (function () {
    function NgSwitchCase(viewContainer, templateRef, ngSwitch) {
        this.ngSwitch = ngSwitch;
        ngSwitch._addCase();
        this._view = new SwitchView(viewContainer, templateRef);
    }
    /**
     * Performs case matching. For internal use only.
     */
    NgSwitchCase.prototype.ngDoCheck = function () { this._view.enforceState(this.ngSwitch._matchCase(this.ngSwitchCase)); };
    tslib_1.__decorate([
        Input(),
        tslib_1.__metadata("design:type", Object)
    ], NgSwitchCase.prototype, "ngSwitchCase", void 0);
    NgSwitchCase = tslib_1.__decorate([
        Directive({ selector: '[ngSwitchCase]' }),
        tslib_1.__param(2, Host()),
        tslib_1.__metadata("design:paramtypes", [ViewContainerRef, TemplateRef,
            NgSwitch])
    ], NgSwitchCase);
    return NgSwitchCase;
}());
export { NgSwitchCase };
/**
 * @ngModule CommonModule
 *
 * @description
 *
 * Creates a view that is rendered when no `NgSwitchCase` expressions
 * match the `NgSwitch` expression.
 * This statement should be the final case in an `NgSwitch`.
 *
 * @publicApi
 * @see `NgSwitch`
 * @see `NgSwitchCase`
 *
 */
var NgSwitchDefault = /** @class */ (function () {
    function NgSwitchDefault(viewContainer, templateRef, ngSwitch) {
        ngSwitch._addDefault(new SwitchView(viewContainer, templateRef));
    }
    NgSwitchDefault = tslib_1.__decorate([
        Directive({ selector: '[ngSwitchDefault]' }),
        tslib_1.__param(2, Host()),
        tslib_1.__metadata("design:paramtypes", [ViewContainerRef, TemplateRef,
            NgSwitch])
    ], NgSwitchDefault);
    return NgSwitchDefault;
}());
export { NgSwitchDefault };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfc3dpdGNoLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL3NyYy9kaXJlY3RpdmVzL25nX3N3aXRjaC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBRUgsT0FBTyxFQUFDLFNBQVMsRUFBVyxJQUFJLEVBQUUsS0FBSyxFQUFFLFdBQVcsRUFBRSxnQkFBZ0IsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUU3RjtJQUdFLG9CQUNZLGlCQUFtQyxFQUFVLFlBQWlDO1FBQTlFLHNCQUFpQixHQUFqQixpQkFBaUIsQ0FBa0I7UUFBVSxpQkFBWSxHQUFaLFlBQVksQ0FBcUI7UUFIbEYsYUFBUSxHQUFHLEtBQUssQ0FBQztJQUdvRSxDQUFDO0lBRTlGLDJCQUFNLEdBQU47UUFDRSxJQUFJLENBQUMsUUFBUSxHQUFHLElBQUksQ0FBQztRQUNyQixJQUFJLENBQUMsaUJBQWlCLENBQUMsa0JBQWtCLENBQUMsSUFBSSxDQUFDLFlBQVksQ0FBQyxDQUFDO0lBQy9ELENBQUM7SUFFRCw0QkFBTyxHQUFQO1FBQ0UsSUFBSSxDQUFDLFFBQVEsR0FBRyxLQUFLLENBQUM7UUFDdEIsSUFBSSxDQUFDLGlCQUFpQixDQUFDLEtBQUssRUFBRSxDQUFDO0lBQ2pDLENBQUM7SUFFRCxpQ0FBWSxHQUFaLFVBQWEsT0FBZ0I7UUFDM0IsSUFBSSxPQUFPLElBQUksQ0FBQyxJQUFJLENBQUMsUUFBUSxFQUFFO1lBQzdCLElBQUksQ0FBQyxNQUFNLEVBQUUsQ0FBQztTQUNmO2FBQU0sSUFBSSxDQUFDLE9BQU8sSUFBSSxJQUFJLENBQUMsUUFBUSxFQUFFO1lBQ3BDLElBQUksQ0FBQyxPQUFPLEVBQUUsQ0FBQztTQUNoQjtJQUNILENBQUM7SUFDSCxpQkFBQztBQUFELENBQUMsQUF2QkQsSUF1QkM7O0FBRUQ7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7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SUFBSSxDQUFDLFlBQVksQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO0lBWnJGO1FBREMsS0FBSyxFQUFFOztzREFDVTtJQU5QLFlBQVk7UUFEeEIsU0FBUyxDQUFDLEVBQUMsUUFBUSxFQUFFLGdCQUFnQixFQUFDLENBQUM7UUFXakMsbUJBQUEsSUFBSSxFQUFFLENBQUE7aURBRFEsZ0JBQWdCLEVBQWUsV0FBVztZQUMvQixRQUFRO09BVjNCLFlBQVksQ0FtQnhCO0lBQUQsbUJBQUM7Q0FBQSxBQW5CRCxJQW1CQztTQW5CWSxZQUFZO0FBcUJ6Qjs7Ozs7Ozs7Ozs7OztHQWFHO0FBRUg7SUFDRSx5QkFDSSxhQUErQixFQUFFLFdBQWdDLEVBQ3pELFFBQWtCO1FBQzVCLFFBQVEsQ0FBQyxXQUFXLENBQUMsSUFBSSxVQUFVLENBQUMsYUFBYSxFQUFFLFdBQVcsQ0FBQyxDQUFDLENBQUM7SUFDbkUsQ0FBQztJQUxVLGVBQWU7UUFEM0IsU0FBUyxDQUFDLEVBQUMsUUFBUSxFQUFFLG1CQUFtQixFQUFDLENBQUM7UUFJcEMsbUJBQUEsSUFBSSxFQUFFLENBQUE7aURBRFEsZ0JBQWdCLEVBQWUsV0FBVztZQUN2QyxRQUFRO09BSG5CLGVBQWUsQ0FNM0I7SUFBRCxzQkFBQztDQUFBLEFBTkQsSUFNQztTQU5ZLGVBQWUiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7RGlyZWN0aXZlLCBEb0NoZWNrLCBIb3N0LCBJbnB1dCwgVGVtcGxhdGVSZWYsIFZpZXdDb250YWluZXJSZWZ9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5leHBvcnQgY2xhc3MgU3dpdGNoVmlldyB7XG4gIHByaXZhdGUgX2NyZWF0ZWQgPSBmYWxzZTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHByaXZhdGUgX3ZpZXdDb250YWluZXJSZWY6IFZpZXdDb250YWluZXJSZWYsIHByaXZhdGUgX3RlbXBsYXRlUmVmOiBUZW1wbGF0ZVJlZjxPYmplY3Q+KSB7fVxuXG4gIGNyZWF0ZSgpOiB2b2lkIHtcbiAgICB0aGlzLl9jcmVhdGVkID0gdHJ1ZTtcbiAgICB0aGlzLl92aWV3Q29udGFpbmVyUmVmLmNyZWF0ZUVtYmVkZGVkVmlldyh0aGlzLl90ZW1wbGF0ZVJlZik7XG4gIH1cblxuICBkZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuX2NyZWF0ZWQgPSBmYWxzZTtcbiAgICB0aGlzLl92aWV3Q29udGFpbmVyUmVmLmNsZWFyKCk7XG4gIH1cblxuICBlbmZvcmNlU3RhdGUoY3JlYXRlZDogYm9vbGVhbikge1xuICAgIGlmIChjcmVhdGVkICYmICF0aGlzLl9jcmVhdGVkKSB7XG4gICAgICB0aGlzLmNyZWF0ZSgpO1xuICAgIH0gZWxzZSBpZiAoIWNyZWF0ZWQgJiYgdGhpcy5fY3JlYXRlZCkge1xuICAgICAgdGhpcy5kZXN0cm95KCk7XG4gICAgfVxuICB9XG59XG5cbi8qKlxuICogQG5nTW9kdWxlIENvbW1vbk1vZHVsZVxuICpcbiAqIEBkZXNjcmlwdGlvbiBBIHN0cnVjdHVyYWwgZGlyZWN0aXZlIHRoYXQgYWRkcyBvciByZW1vdmVzIHRlbXBsYXRlcyAoZGlzcGxheWluZyBvciBoaWRpbmcgdmlld3MpXG4gKiB3aGVuIHRoZSBuZXh0IG1hdGNoIGV4cHJlc3Npb24gbWF0Y2hlcyB0aGUgc3dpdGNoIGV4cHJlc3Npb24uXG4gKlxuICogVGhlIGBbbmdTd2l0Y2hdYCBkaXJlY3RpdmUgb24gYSBjb250YWluZXIgc3BlY2lmaWVzIGFuIGV4cHJlc3Npb24gdG8gbWF0Y2ggYWdhaW5zdC5cbiAqIFRoZSBleHByZXNzaW9ucyB0byBtYXRjaCBhcmUgcHJvdmlkZWQgYnkgYG5nU3dpdGNoQ2FzZWAgZGlyZWN0aXZlcyBvbiB2aWV3cyB3aXRoaW4gdGhlIGNvbnRhaW5lci5cbiAqIC0gRXZlcnkgdmlldyB0aGF0IG1hdGNoZXMgaXMgcmVuZGVyZWQuXG4gKiAtIElmIHRoZXJlIGFyZSBubyBtYXRjaGVzLCBhIHZpZXcgd2l0aCB0aGUgYG5nU3dpdGNoRGVmYXVsdGAgZGlyZWN0aXZlIGlzIHJlbmRlcmVkLlxuICogLSBFbGVtZW50cyB3aXRoaW4gdGhlIGBbTmdTd2l0Y2hdYCBzdGF0ZW1lbnQgYnV0IG91dHNpZGUgb2YgYW55IGBOZ1N3aXRjaENhc2VgXG4gKiBvciBgbmdTd2l0Y2hEZWZhdWx0YCBkaXJlY3RpdmUgYXJlIHByZXNlcnZlZCBhdCB0aGUgbG9jYXRpb24uXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqIERlZmluZSBhIGNvbnRhaW5lciBlbGVtZW50IGZvciB0aGUgZGlyZWN0aXZlLCBhbmQgc3BlY2lmeSB0aGUgc3dpdGNoIGV4cHJlc3Npb25cbiAqIHRvIG1hdGNoIGFnYWluc3QgYXMgYW4gYXR0cmlidXRlOlxuICpcbiAqIGBgYFxuICogPGNvbnRhaW5lci1lbGVtZW50IFtuZ1N3aXRjaF09XCJzd2l0Y2hfZXhwcmVzc2lvblwiPlxuICogYGBgXG4gKlxuICogV2l0aGluIHRoZSBjb250YWluZXIsIGAqbmdTd2l0Y2hDYXNlYCBzdGF0ZW1lbnRzIHNwZWNpZnkgdGhlIG1hdGNoIGV4cHJlc3Npb25zXG4gKiBhcyBhdHRyaWJ1dGVzLiBJbmNsdWRlIGAqbmdTd2l0Y2hEZWZhdWx0YCBhcyB0aGUgZmluYWwgY2FzZS5cbiAqXG4gKiBgYGBcbiAqIDxjb250YWluZXItZWxlbWVudCBbbmdTd2l0Y2hdPVwic3dpdGNoX2V4cHJlc3Npb25cIj5cbiAqICAgIDxzb21lLWVsZW1lbnQgKm5nU3dpdGNoQ2FzZT1cIm1hdGNoX2V4cHJlc3Npb25fMVwiPi4uLjwvc29tZS1lbGVtZW50PlxuICogLi4uXG4gKiAgICA8c29tZS1lbGVtZW50ICpuZ1N3aXRjaERlZmF1bHQ+Li4uPC9zb21lLWVsZW1lbnQ+XG4gKiA8L2NvbnRhaW5lci1lbGVtZW50PlxuICogYGBgXG4gKlxuICogIyMjIFVzYWdlIEV4YW1wbGVzXG4gKlxuICogVGhlIGZvbGxvd2luZyBleGFtcGxlIHNob3dzIGhvdyB0byB1c2UgbW9yZSB0aGFuIG9uZSBjYXNlIHRvIGRpc3BsYXkgdGhlIHNhbWUgdmlldzpcbiAqXG4gKiBgYGBcbiAqIDxjb250YWluZXItZWxlbWVudCBbbmdTd2l0Y2hdPVwic3dpdGNoX2V4cHJlc3Npb25cIj5cbiAqICAgPCEtLSB0aGUgc2FtZSB2aWV3IGNhbiBiZSBzaG93biBpbiBtb3JlIHRoYW4gb25lIGNhc2UgLS0+XG4gKiAgIDxzb21lLWVsZW1lbnQgKm5nU3dpdGNoQ2FzZT1cIm1hdGNoX2V4cHJlc3Npb25fMVwiPi4uLjwvc29tZS1lbGVtZW50PlxuICogICA8c29tZS1lbGVtZW50ICpuZ1N3aXRjaENhc2U9XCJtYXRjaF9leHByZXNzaW9uXzJcIj4uLi48L3NvbWUtZWxlbWVudD5cbiAqICAgPHNvbWUtb3RoZXItZWxlbWVudCAqbmdTd2l0Y2hDYXNlPVwibWF0Y2hfZXhwcmVzc2lvbl8zXCI+Li4uPC9zb21lLW90aGVyLWVsZW1lbnQ+XG4gKiAgIDwhLS1kZWZhdWx0IGNhc2Ugd2hlbiB0aGVyZSBhcmUgbm8gbWF0Y2hlcyAtLT5cbiAqICAgPHNvbWUtZWxlbWVudCAqbmdTd2l0Y2hEZWZhdWx0Pi4uLjwvc29tZS1lbGVtZW50PlxuICogPC9jb250YWluZXItZWxlbWVudD5cbiAqIGBgYFxuICpcbiAqIFRoZSBmb2xsb3dpbmcgZXhhbXBsZSBzaG93cyBob3cgY2FzZXMgY2FuIGJlIG5lc3RlZDpcbiAqIGBgYFxuICogPGNvbnRhaW5lci1lbGVtZW50IFtuZ1N3aXRjaF09XCJzd2l0Y2hfZXhwcmVzc2lvblwiPlxuICogICAgICAgPHNvbWUtZWxlbWVudCAqbmdTd2l0Y2hDYXNlPVwibWF0Y2hfZXhwcmVzc2lvbl8xXCI+Li4uPC9zb21lLWVsZW1lbnQ+XG4gKiAgICAgICA8c29tZS1lbGVtZW50ICpuZ1N3aXRjaENhc2U9XCJtYXRjaF9leHByZXNzaW9uXzJcIj4uLi48L3NvbWUtZWxlbWVudD5cbiAqICAgICAgIDxzb21lLW90aGVyLWVsZW1lbnQgKm5nU3dpdGNoQ2FzZT1cIm1hdGNoX2V4cHJlc3Npb25fM1wiPi4uLjwvc29tZS1vdGhlci1lbGVtZW50PlxuICogICAgICAgPG5nLWNvbnRhaW5lciAqbmdTd2l0Y2hDYXNlPVwibWF0Y2hfZXhwcmVzc2lvbl8zXCI+XG4gKiAgICAgICAgIDwhLS0gdXNlIGEgbmctY29udGFpbmVyIHRvIGdyb3VwIG11bHRpcGxlIHJvb3Qgbm9kZXMgLS0+XG4gKiAgICAgICAgIDxpbm5lci1lbGVtZW50PjwvaW5uZXItZWxlbWVudD5cbiAqICAgICAgICAgPGlubmVyLW90aGVyLWVsZW1lbnQ+PC9pbm5lci1vdGhlci1lbGVtZW50PlxuICogICAgICAgPC9uZy1jb250YWluZXI+XG4gKiAgICAgICA8c29tZS1lbGVtZW50ICpuZ1N3aXRjaERlZmF1bHQ+Li4uPC9zb21lLWVsZW1lbnQ+XG4gKiAgICAgPC9jb250YWluZXItZWxlbWVudD5cbiAqIGBgYFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqIEBzZWUgYE5nU3dpdGNoQ2FzZWBcbiAqIEBzZWUgYE5nU3dpdGNoRGVmYXVsdGBcbiAqIEBzZWUgW1N0cnVjdHVyYWwgRGlyZWN0aXZlc10oZ3VpZGUvc3RydWN0dXJhbC1kaXJlY3RpdmVzKVxuICpcbiAqL1xuQERpcmVjdGl2ZSh7c2VsZWN0b3I6ICdbbmdTd2l0Y2hdJ30pXG5leHBvcnQgY2xhc3MgTmdTd2l0Y2gge1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfZGVmYXVsdFZpZXdzICE6IFN3aXRjaFZpZXdbXTtcbiAgcHJpdmF0ZSBfZGVmYXVsdFVzZWQgPSBmYWxzZTtcbiAgcHJpdmF0ZSBfY2FzZUNvdW50ID0gMDtcbiAgcHJpdmF0ZSBfbGFzdENhc2VDaGVja0luZGV4ID0gMDtcbiAgcHJpdmF0ZSBfbGFzdENhc2VzTWF0Y2hlZCA9IGZhbHNlO1xuICBwcml2YXRlIF9uZ1N3aXRjaDogYW55O1xuXG4gIEBJbnB1dCgpXG4gIHNldCBuZ1N3aXRjaChuZXdWYWx1ZTogYW55KSB7XG4gICAgdGhpcy5fbmdTd2l0Y2ggPSBuZXdWYWx1ZTtcbiAgICBpZiAodGhpcy5fY2FzZUNvdW50ID09PSAwKSB7XG4gICAgICB0aGlzLl91cGRhdGVEZWZhdWx0Q2FzZXModHJ1ZSk7XG4gICAgfVxuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfYWRkQ2FzZSgpOiBudW1iZXIgeyByZXR1cm4gdGhpcy5fY2FzZUNvdW50Kys7IH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9hZGREZWZhdWx0KHZpZXc6IFN3aXRjaFZpZXcpIHtcbiAgICBpZiAoIXRoaXMuX2RlZmF1bHRWaWV3cykge1xuICAgICAgdGhpcy5fZGVmYXVsdFZpZXdzID0gW107XG4gICAgfVxuICAgIHRoaXMuX2RlZmF1bHRWaWV3cy5wdXNoKHZpZXcpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfbWF0Y2hDYXNlKHZhbHVlOiBhbnkpOiBib29sZWFuIHtcbiAgICBjb25zdCBtYXRjaGVkID0gdmFsdWUgPT0gdGhpcy5fbmdTd2l0Y2g7XG4gICAgdGhpcy5fbGFzdENhc2VzTWF0Y2hlZCA9IHRoaXMuX2xhc3RDYXNlc01hdGNoZWQgfHwgbWF0Y2hlZDtcbiAgICB0aGlzLl9sYXN0Q2FzZUNoZWNrSW5kZXgrKztcbiAgICBpZiAodGhpcy5fbGFzdENhc2VDaGVja0luZGV4ID09PSB0aGlzLl9jYXNlQ291bnQpIHtcbiAgICAgIHRoaXMuX3VwZGF0ZURlZmF1bHRDYXNlcyghdGhpcy5fbGFzdENhc2VzTWF0Y2hlZCk7XG4gICAgICB0aGlzLl9sYXN0Q2FzZUNoZWNrSW5kZXggPSAwO1xuICAgICAgdGhpcy5fbGFzdENhc2VzTWF0Y2hlZCA9IGZhbHNlO1xuICAgIH1cbiAgICByZXR1cm4gbWF0Y2hlZDtcbiAgfVxuXG4gIHByaXZhdGUgX3VwZGF0ZURlZmF1bHRDYXNlcyh1c2VEZWZhdWx0OiBib29sZWFuKSB7XG4gICAgaWYgKHRoaXMuX2RlZmF1bHRWaWV3cyAmJiB1c2VEZWZhdWx0ICE9PSB0aGlzLl9kZWZhdWx0VXNlZCkge1xuICAgICAgdGhpcy5fZGVmYXVsdFVzZWQgPSB1c2VEZWZhdWx0O1xuICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCB0aGlzLl9kZWZhdWx0Vmlld3MubGVuZ3RoOyBpKyspIHtcbiAgICAgICAgY29uc3QgZGVmYXVsdFZpZXcgPSB0aGlzLl9kZWZhdWx0Vmlld3NbaV07XG4gICAgICAgIGRlZmF1bHRWaWV3LmVuZm9yY2VTdGF0ZSh1c2VEZWZhdWx0KTtcbiAgICAgIH1cbiAgICB9XG4gIH1cbn1cblxuLyoqXG4gKiBAbmdNb2R1bGUgQ29tbW9uTW9kdWxlXG4gKlxuICogQGRlc2NyaXB0aW9uXG4gKiBQcm92aWRlcyBhIHN3aXRjaCBjYXNlIGV4cHJlc3Npb24gdG8gbWF0Y2ggYWdhaW5zdCBhbiBlbmNsb3NpbmcgYG5nU3dpdGNoYCBleHByZXNzaW9uLlxuICogV2hlbiB0aGUgZXhwcmVzc2lvbnMgbWF0Y2gsIHRoZSBnaXZlbiBgTmdTd2l0Y2hDYXNlYCB0ZW1wbGF0ZSBpcyByZW5kZXJlZC5cbiAqIElmIG11bHRpcGxlIG1hdGNoIGV4cHJlc3Npb25zIG1hdGNoIHRoZSBzd2l0Y2ggZXhwcmVzc2lvbiB2YWx1ZSwgYWxsIG9mIHRoZW0gYXJlIGRpc3BsYXllZC5cbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICpcbiAqIFdpdGhpbiBhIHN3aXRjaCBjb250YWluZXIsIGAqbmdTd2l0Y2hDYXNlYCBzdGF0ZW1lbnRzIHNwZWNpZnkgdGhlIG1hdGNoIGV4cHJlc3Npb25zXG4gKiBhcyBhdHRyaWJ1dGVzLiBJbmNsdWRlIGAqbmdTd2l0Y2hEZWZhdWx0YCBhcyB0aGUgZmluYWwgY2FzZS5cbiAqXG4gKiBgYGBcbiAqIDxjb250YWluZXItZWxlbWVudCBbbmdTd2l0Y2hdPVwic3dpdGNoX2V4cHJlc3Npb25cIj5cbiAqICAgPHNvbWUtZWxlbWVudCAqbmdTd2l0Y2hDYXNlPVwibWF0Y2hfZXhwcmVzc2lvbl8xXCI+Li4uPC9zb21lLWVsZW1lbnQ+XG4gKiAgIC4uLlxuICogICA8c29tZS1lbGVtZW50ICpuZ1N3aXRjaERlZmF1bHQ+Li4uPC9zb21lLWVsZW1lbnQ+XG4gKiA8L2NvbnRhaW5lci1lbGVtZW50PlxuICogYGBgXG4gKlxuICogRWFjaCBzd2l0Y2gtY2FzZSBzdGF0ZW1lbnQgY29udGFpbnMgYW4gaW4tbGluZSBIVE1MIHRlbXBsYXRlIG9yIHRlbXBsYXRlIHJlZmVyZW5jZVxuICogdGhhdCBkZWZpbmVzIHRoZSBzdWJ0cmVlIHRvIGJlIHNlbGVjdGVkIGlmIHRoZSB2YWx1ZSBvZiB0aGUgbWF0Y2ggZXhwcmVzc2lvblxuICogbWF0Y2hlcyB0aGUgdmFsdWUgb2YgdGhlIHN3aXRjaCBleHByZXNzaW9uLlxuICpcbiAqIFVubGlrZSBKYXZhU2NyaXB0LCB3aGljaCB1c2VzIHN0cmljdCBlcXVhbGl0eSwgQW5ndWxhciB1c2VzIGxvb3NlIGVxdWFsaXR5LlxuICogVGhpcyBtZWFucyB0aGF0IHRoZSBlbXB0eSBzdHJpbmcsIGBcIlwiYCBtYXRjaGVzIDAuXG4gKlxuICogQHB1YmxpY0FwaVxuICogQHNlZSBgTmdTd2l0Y2hgXG4gKiBAc2VlIGBOZ1N3aXRjaERlZmF1bHRgXG4gKlxuICovXG5ARGlyZWN0aXZlKHtzZWxlY3RvcjogJ1tuZ1N3aXRjaENhc2VdJ30pXG5leHBvcnQgY2xhc3MgTmdTd2l0Y2hDYXNlIGltcGxlbWVudHMgRG9DaGVjayB7XG4gIHByaXZhdGUgX3ZpZXc6IFN3aXRjaFZpZXc7XG4gIC8qKlxuICAgKiBTdG9yZXMgdGhlIEhUTUwgdGVtcGxhdGUgdG8gYmUgc2VsZWN0ZWQgb24gbWF0Y2guXG4gICAqL1xuICBASW5wdXQoKVxuICBuZ1N3aXRjaENhc2U6IGFueTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHZpZXdDb250YWluZXI6IFZpZXdDb250YWluZXJSZWYsIHRlbXBsYXRlUmVmOiBUZW1wbGF0ZVJlZjxPYmplY3Q+LFxuICAgICAgQEhvc3QoKSBwcml2YXRlIG5nU3dpdGNoOiBOZ1N3aXRjaCkge1xuICAgIG5nU3dpdGNoLl9hZGRDYXNlKCk7XG4gICAgdGhpcy5fdmlldyA9IG5ldyBTd2l0Y2hWaWV3KHZpZXdDb250YWluZXIsIHRlbXBsYXRlUmVmKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBQZXJmb3JtcyBjYXNlIG1hdGNoaW5nLiBGb3IgaW50ZXJuYWwgdXNlIG9ubHkuXG4gICAqL1xuICBuZ0RvQ2hlY2soKSB7IHRoaXMuX3ZpZXcuZW5mb3JjZVN0YXRlKHRoaXMubmdTd2l0Y2guX21hdGNoQ2FzZSh0aGlzLm5nU3dpdGNoQ2FzZSkpOyB9XG59XG5cbi8qKlxuICogQG5nTW9kdWxlIENvbW1vbk1vZHVsZVxuICpcbiAqIEBkZXNjcmlwdGlvblxuICpcbiAqIENyZWF0ZXMgYSB2aWV3IHRoYXQgaXMgcmVuZGVyZWQgd2hlbiBubyBgTmdTd2l0Y2hDYXNlYCBleHByZXNzaW9uc1xuICogbWF0Y2ggdGhlIGBOZ1N3aXRjaGAgZXhwcmVzc2lvbi5cbiAqIFRoaXMgc3RhdGVtZW50IHNob3VsZCBiZSB0aGUgZmluYWwgY2FzZSBpbiBhbiBgTmdTd2l0Y2hgLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqIEBzZWUgYE5nU3dpdGNoYFxuICogQHNlZSBgTmdTd2l0Y2hDYXNlYFxuICpcbiAqL1xuQERpcmVjdGl2ZSh7c2VsZWN0b3I6ICdbbmdTd2l0Y2hEZWZhdWx0XSd9KVxuZXhwb3J0IGNsYXNzIE5nU3dpdGNoRGVmYXVsdCB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgdmlld0NvbnRhaW5lcjogVmlld0NvbnRhaW5lclJlZiwgdGVtcGxhdGVSZWY6IFRlbXBsYXRlUmVmPE9iamVjdD4sXG4gICAgICBASG9zdCgpIG5nU3dpdGNoOiBOZ1N3aXRjaCkge1xuICAgIG5nU3dpdGNoLl9hZGREZWZhdWx0KG5ldyBTd2l0Y2hWaWV3KHZpZXdDb250YWluZXIsIHRlbXBsYXRlUmVmKSk7XG4gIH1cbn1cbiJdfQ==
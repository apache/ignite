/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Directive, Input, IterableDiffers, TemplateRef, ViewContainerRef, isDevMode } from '@angular/core';
/**
 * @publicApi
 */
var NgForOfContext = /** @class */ (function () {
    function NgForOfContext($implicit, ngForOf, index, count) {
        this.$implicit = $implicit;
        this.ngForOf = ngForOf;
        this.index = index;
        this.count = count;
    }
    Object.defineProperty(NgForOfContext.prototype, "first", {
        get: function () { return this.index === 0; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NgForOfContext.prototype, "last", {
        get: function () { return this.index === this.count - 1; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NgForOfContext.prototype, "even", {
        get: function () { return this.index % 2 === 0; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NgForOfContext.prototype, "odd", {
        get: function () { return !this.even; },
        enumerable: true,
        configurable: true
    });
    return NgForOfContext;
}());
export { NgForOfContext };
/**
 * A [structural directive](guide/structural-directives) that renders
 * a template for each item in a collection.
 * The directive is placed on an element, which becomes the parent
 * of the cloned templates.
 *
 * The `ngForOf` directive is generally used in the
 * [shorthand form](guide/structural-directives#the-asterisk--prefix) `*ngFor`.
 * In this form, the template to be rendered for each iteration is the content
 * of an anchor element containing the directive.
 *
 * The following example shows the shorthand syntax with some options,
 * contained in an `<li>` element.
 *
 * ```
 * <li *ngFor="let item of items; index as i; trackBy: trackByFn">...</li>
 * ```
 *
 * The shorthand form expands into a long form that uses the `ngForOf` selector
 * on an `<ng-template>` element.
 * The content of the `<ng-template>` element is the `<li>` element that held the
 * short-form directive.
 *
 * Here is the expanded version of the short-form example.
 *
 * ```
 * <ng-template ngFor let-item [ngForOf]="items" let-i="index" [ngForTrackBy]="trackByFn">
 *   <li>...</li>
 * </ng-template>
 * ```
 *
 * Angular automatically expands the shorthand syntax as it compiles the template.
 * The context for each embedded view is logically merged to the current component
 * context according to its lexical position.
 *
 * When using the shorthand syntax, Angular allows only [one structural directive
 * on an element](guide/structural-directives#one-structural-directive-per-host-element).
 * If you want to iterate conditionally, for example,
 * put the `*ngIf` on a container element that wraps the `*ngFor` element.
 * For futher discussion, see
 * [Structural Directives](guide/structural-directives#one-per-element).
 *
 * @usageNotes
 *
 * ### Local variables
 *
 * `NgForOf` provides exported values that can be aliased to local variables.
 * For example:
 *
 *  ```
 * <li *ngFor="let user of userObservable | async as users; index as i; first as isFirst">
 *    {{i}}/{{users.length}}. {{user}} <span *ngIf="isFirst">default</span>
 * </li>
 * ```
 *
 * The following exported values can be aliased to local variables:
 *
 * - `$implicit: T`: The value of the individual items in the iterable (`ngForOf`).
 * - `ngForOf: NgIterable<T>`: The value of the iterable expression. Useful when the expression is
 * more complex then a property access, for example when using the async pipe (`userStreams |
 * async`).
 * - `index: number`: The index of the current item in the iterable.
 * - `first: boolean`: True when the item is the first item in the iterable.
 * - `last: boolean`: True when the item is the last item in the iterable.
 * - `even: boolean`: True when the item has an even index in the iterable.
 * - `odd: boolean`: True when the item has an odd index in the iterable.
 *
 * ### Change propagation
 *
 * When the contents of the iterator changes, `NgForOf` makes the corresponding changes to the DOM:
 *
 * * When an item is added, a new instance of the template is added to the DOM.
 * * When an item is removed, its template instance is removed from the DOM.
 * * When items are reordered, their respective templates are reordered in the DOM.
 *
 * Angular uses object identity to track insertions and deletions within the iterator and reproduce
 * those changes in the DOM. This has important implications for animations and any stateful
 * controls that are present, such as `<input>` elements that accept user input. Inserted rows can
 * be animated in, deleted rows can be animated out, and unchanged rows retain any unsaved state
 * such as user input.
 * For more on animations, see [Transitions and Triggers](guide/transition-and-triggers).
 *
 * The identities of elements in the iterator can change while the data does not.
 * This can happen, for example, if the iterator is produced from an RPC to the server, and that
 * RPC is re-run. Even if the data hasn't changed, the second response produces objects with
 * different identities, and Angular must tear down the entire DOM and rebuild it (as if all old
 * elements were deleted and all new elements inserted).
 *
 * To avoid this expensive operation, you can customize the default tracking algorithm.
 * by supplying the `trackBy` option to `NgForOf`.
 * `trackBy` takes a function that has two arguments: `index` and `item`.
 * If `trackBy` is given, Angular tracks changes by the return value of the function.
 *
 * @see [Structural Directives](guide/structural-directives)
 * @ngModule CommonModule
 * @publicApi
 */
var NgForOf = /** @class */ (function () {
    function NgForOf(_viewContainer, _template, _differs) {
        this._viewContainer = _viewContainer;
        this._template = _template;
        this._differs = _differs;
        this._ngForOfDirty = true;
        this._differ = null;
    }
    Object.defineProperty(NgForOf.prototype, "ngForOf", {
        /**
         * The value of the iterable expression, which can be used as a
         * [template input variable](guide/structural-directives#template-input-variable).
         */
        set: function (ngForOf) {
            this._ngForOf = ngForOf;
            this._ngForOfDirty = true;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NgForOf.prototype, "ngForTrackBy", {
        get: function () { return this._trackByFn; },
        /**
         * A function that defines how to track changes for items in the iterable.
         *
         * When items are added, moved, or removed in the iterable,
         * the directive must re-render the appropriate DOM nodes.
         * To minimize churn in the DOM, only nodes that have changed
         * are re-rendered.
         *
         * By default, the change detector assumes that
         * the object instance identifies the node in the iterable.
         * When this function is supplied, the directive uses
         * the result of calling this function to identify the item node,
         * rather than the identity of the object itself.
         *
         * The function receives two inputs,
         * the iteration index and the node object ID.
         */
        set: function (fn) {
            if (isDevMode() && fn != null && typeof fn !== 'function') {
                // TODO(vicb): use a log service once there is a public one available
                if (console && console.warn) {
                    console.warn("trackBy must be a function, but received " + JSON.stringify(fn) + ". " +
                        "See https://angular.io/docs/ts/latest/api/common/index/NgFor-directive.html#!#change-propagation for more information.");
                }
            }
            this._trackByFn = fn;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NgForOf.prototype, "ngForTemplate", {
        /**
         * A reference to the template that is stamped out for each item in the iterable.
         * @see [template reference variable](guide/template-syntax#template-reference-variables--var-)
         */
        set: function (value) {
            // TODO(TS2.1): make TemplateRef<Partial<NgForRowOf<T>>> once we move to TS v2.1
            // The current type is too restrictive; a template that just uses index, for example,
            // should be acceptable.
            if (value) {
                this._template = value;
            }
        },
        enumerable: true,
        configurable: true
    });
    /**
     * Applies the changes when needed.
     */
    NgForOf.prototype.ngDoCheck = function () {
        if (this._ngForOfDirty) {
            this._ngForOfDirty = false;
            // React on ngForOf changes only once all inputs have been initialized
            var value = this._ngForOf;
            if (!this._differ && value) {
                try {
                    this._differ = this._differs.find(value).create(this.ngForTrackBy);
                }
                catch (_a) {
                    throw new Error("Cannot find a differ supporting object '" + value + "' of type '" + getTypeName(value) + "'. NgFor only supports binding to Iterables such as Arrays.");
                }
            }
        }
        if (this._differ) {
            var changes = this._differ.diff(this._ngForOf);
            if (changes)
                this._applyChanges(changes);
        }
    };
    NgForOf.prototype._applyChanges = function (changes) {
        var _this = this;
        var insertTuples = [];
        changes.forEachOperation(function (item, adjustedPreviousIndex, currentIndex) {
            if (item.previousIndex == null) {
                var view = _this._viewContainer.createEmbeddedView(_this._template, new NgForOfContext(null, _this._ngForOf, -1, -1), currentIndex === null ? undefined : currentIndex);
                var tuple = new RecordViewTuple(item, view);
                insertTuples.push(tuple);
            }
            else if (currentIndex == null) {
                _this._viewContainer.remove(adjustedPreviousIndex === null ? undefined : adjustedPreviousIndex);
            }
            else if (adjustedPreviousIndex !== null) {
                var view = _this._viewContainer.get(adjustedPreviousIndex);
                _this._viewContainer.move(view, currentIndex);
                var tuple = new RecordViewTuple(item, view);
                insertTuples.push(tuple);
            }
        });
        for (var i = 0; i < insertTuples.length; i++) {
            this._perViewChange(insertTuples[i].view, insertTuples[i].record);
        }
        for (var i = 0, ilen = this._viewContainer.length; i < ilen; i++) {
            var viewRef = this._viewContainer.get(i);
            viewRef.context.index = i;
            viewRef.context.count = ilen;
            viewRef.context.ngForOf = this._ngForOf;
        }
        changes.forEachIdentityChange(function (record) {
            var viewRef = _this._viewContainer.get(record.currentIndex);
            viewRef.context.$implicit = record.item;
        });
    };
    NgForOf.prototype._perViewChange = function (view, record) {
        view.context.$implicit = record.item;
    };
    /**
     * Asserts the correct type of the context for the template that `NgForOf` will render.
     *
     * The presence of this method is a signal to the Ivy template type-check compiler that the
     * `NgForOf` structural directive renders its template with a specific context type.
     */
    NgForOf.ngTemplateContextGuard = function (dir, ctx) {
        return true;
    };
    tslib_1.__decorate([
        Input(),
        tslib_1.__metadata("design:type", Object),
        tslib_1.__metadata("design:paramtypes", [Object])
    ], NgForOf.prototype, "ngForOf", null);
    tslib_1.__decorate([
        Input(),
        tslib_1.__metadata("design:type", Function),
        tslib_1.__metadata("design:paramtypes", [Function])
    ], NgForOf.prototype, "ngForTrackBy", null);
    tslib_1.__decorate([
        Input(),
        tslib_1.__metadata("design:type", TemplateRef),
        tslib_1.__metadata("design:paramtypes", [TemplateRef])
    ], NgForOf.prototype, "ngForTemplate", null);
    NgForOf = tslib_1.__decorate([
        Directive({ selector: '[ngFor][ngForOf]' }),
        tslib_1.__metadata("design:paramtypes", [ViewContainerRef, TemplateRef,
            IterableDiffers])
    ], NgForOf);
    return NgForOf;
}());
export { NgForOf };
var RecordViewTuple = /** @class */ (function () {
    function RecordViewTuple(record, view) {
        this.record = record;
        this.view = view;
    }
    return RecordViewTuple;
}());
function getTypeName(type) {
    return type['name'] || typeof type;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfZm9yX29mLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL3NyYy9kaXJlY3RpdmVzL25nX2Zvcl9vZi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBRUgsT0FBTyxFQUFDLFNBQVMsRUFBNEIsS0FBSyxFQUF5RCxlQUFlLEVBQWMsV0FBVyxFQUFtQixnQkFBZ0IsRUFBYyxTQUFTLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFFcE87O0dBRUc7QUFDSDtJQUNFLHdCQUNXLFNBQVksRUFBUyxPQUFzQixFQUFTLEtBQWEsRUFDakUsS0FBYTtRQURiLGNBQVMsR0FBVCxTQUFTLENBQUc7UUFBUyxZQUFPLEdBQVAsT0FBTyxDQUFlO1FBQVMsVUFBSyxHQUFMLEtBQUssQ0FBUTtRQUNqRSxVQUFLLEdBQUwsS0FBSyxDQUFRO0lBQUcsQ0FBQztJQUU1QixzQkFBSSxpQ0FBSzthQUFULGNBQXVCLE9BQU8sSUFBSSxDQUFDLEtBQUssS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7T0FBQTtJQUVqRCxzQkFBSSxnQ0FBSTthQUFSLGNBQXNCLE9BQU8sSUFBSSxDQUFDLEtBQUssS0FBSyxJQUFJLENBQUMsS0FBSyxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUM7OztPQUFBO0lBRTdELHNCQUFJLGdDQUFJO2FBQVIsY0FBc0IsT0FBTyxJQUFJLENBQUMsS0FBSyxHQUFHLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7T0FBQTtJQUVwRCxzQkFBSSwrQkFBRzthQUFQLGNBQXFCLE9BQU8sQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQzs7O09BQUE7SUFDM0MscUJBQUM7QUFBRCxDQUFDLEFBWkQsSUFZQzs7QUFFRDs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7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QixPQUFPLENBZ0puQjtJQUFELGNBQUM7Q0FBQSxBQWhKRCxJQWdKQztTQWhKWSxPQUFPO0FBa0pwQjtJQUNFLHlCQUFtQixNQUFXLEVBQVMsSUFBd0M7UUFBNUQsV0FBTSxHQUFOLE1BQU0sQ0FBSztRQUFTLFNBQUksR0FBSixJQUFJLENBQW9DO0lBQUcsQ0FBQztJQUNyRixzQkFBQztBQUFELENBQUMsQUFGRCxJQUVDO0FBRUQsU0FBUyxXQUFXLENBQUMsSUFBUztJQUM1QixPQUFPLElBQUksQ0FBQyxNQUFNLENBQUMsSUFBSSxPQUFPLElBQUksQ0FBQztBQUNyQyxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0RpcmVjdGl2ZSwgRG9DaGVjaywgRW1iZWRkZWRWaWV3UmVmLCBJbnB1dCwgSXRlcmFibGVDaGFuZ2VSZWNvcmQsIEl0ZXJhYmxlQ2hhbmdlcywgSXRlcmFibGVEaWZmZXIsIEl0ZXJhYmxlRGlmZmVycywgTmdJdGVyYWJsZSwgVGVtcGxhdGVSZWYsIFRyYWNrQnlGdW5jdGlvbiwgVmlld0NvbnRhaW5lclJlZiwgZm9yd2FyZFJlZiwgaXNEZXZNb2RlfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuLyoqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBOZ0Zvck9mQ29udGV4dDxUPiB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljICRpbXBsaWNpdDogVCwgcHVibGljIG5nRm9yT2Y6IE5nSXRlcmFibGU8VD4sIHB1YmxpYyBpbmRleDogbnVtYmVyLFxuICAgICAgcHVibGljIGNvdW50OiBudW1iZXIpIHt9XG5cbiAgZ2V0IGZpcnN0KCk6IGJvb2xlYW4geyByZXR1cm4gdGhpcy5pbmRleCA9PT0gMDsgfVxuXG4gIGdldCBsYXN0KCk6IGJvb2xlYW4geyByZXR1cm4gdGhpcy5pbmRleCA9PT0gdGhpcy5jb3VudCAtIDE7IH1cblxuICBnZXQgZXZlbigpOiBib29sZWFuIHsgcmV0dXJuIHRoaXMuaW5kZXggJSAyID09PSAwOyB9XG5cbiAgZ2V0IG9kZCgpOiBib29sZWFuIHsgcmV0dXJuICF0aGlzLmV2ZW47IH1cbn1cblxuLyoqXG4gKiBBIFtzdHJ1Y3R1cmFsIGRpcmVjdGl2ZV0oZ3VpZGUvc3RydWN0dXJhbC1kaXJlY3RpdmVzKSB0aGF0IHJlbmRlcnNcbiAqIGEgdGVtcGxhdGUgZm9yIGVhY2ggaXRlbSBpbiBhIGNvbGxlY3Rpb24uXG4gKiBUaGUgZGlyZWN0aXZlIGlzIHBsYWNlZCBvbiBhbiBlbGVtZW50LCB3aGljaCBiZWNvbWVzIHRoZSBwYXJlbnRcbiAqIG9mIHRoZSBjbG9uZWQgdGVtcGxhdGVzLlxuICpcbiAqIFRoZSBgbmdGb3JPZmAgZGlyZWN0aXZlIGlzIGdlbmVyYWxseSB1c2VkIGluIHRoZVxuICogW3Nob3J0aGFuZCBmb3JtXShndWlkZS9zdHJ1Y3R1cmFsLWRpcmVjdGl2ZXMjdGhlLWFzdGVyaXNrLS1wcmVmaXgpIGAqbmdGb3JgLlxuICogSW4gdGhpcyBmb3JtLCB0aGUgdGVtcGxhdGUgdG8gYmUgcmVuZGVyZWQgZm9yIGVhY2ggaXRlcmF0aW9uIGlzIHRoZSBjb250ZW50XG4gKiBvZiBhbiBhbmNob3IgZWxlbWVudCBjb250YWluaW5nIHRoZSBkaXJlY3RpdmUuXG4gKlxuICogVGhlIGZvbGxvd2luZyBleGFtcGxlIHNob3dzIHRoZSBzaG9ydGhhbmQgc3ludGF4IHdpdGggc29tZSBvcHRpb25zLFxuICogY29udGFpbmVkIGluIGFuIGA8bGk+YCBlbGVtZW50LlxuICpcbiAqIGBgYFxuICogPGxpICpuZ0Zvcj1cImxldCBpdGVtIG9mIGl0ZW1zOyBpbmRleCBhcyBpOyB0cmFja0J5OiB0cmFja0J5Rm5cIj4uLi48L2xpPlxuICogYGBgXG4gKlxuICogVGhlIHNob3J0aGFuZCBmb3JtIGV4cGFuZHMgaW50byBhIGxvbmcgZm9ybSB0aGF0IHVzZXMgdGhlIGBuZ0Zvck9mYCBzZWxlY3RvclxuICogb24gYW4gYDxuZy10ZW1wbGF0ZT5gIGVsZW1lbnQuXG4gKiBUaGUgY29udGVudCBvZiB0aGUgYDxuZy10ZW1wbGF0ZT5gIGVsZW1lbnQgaXMgdGhlIGA8bGk+YCBlbGVtZW50IHRoYXQgaGVsZCB0aGVcbiAqIHNob3J0LWZvcm0gZGlyZWN0aXZlLlxuICpcbiAqIEhlcmUgaXMgdGhlIGV4cGFuZGVkIHZlcnNpb24gb2YgdGhlIHNob3J0LWZvcm0gZXhhbXBsZS5cbiAqXG4gKiBgYGBcbiAqIDxuZy10ZW1wbGF0ZSBuZ0ZvciBsZXQtaXRlbSBbbmdGb3JPZl09XCJpdGVtc1wiIGxldC1pPVwiaW5kZXhcIiBbbmdGb3JUcmFja0J5XT1cInRyYWNrQnlGblwiPlxuICogICA8bGk+Li4uPC9saT5cbiAqIDwvbmctdGVtcGxhdGU+XG4gKiBgYGBcbiAqXG4gKiBBbmd1bGFyIGF1dG9tYXRpY2FsbHkgZXhwYW5kcyB0aGUgc2hvcnRoYW5kIHN5bnRheCBhcyBpdCBjb21waWxlcyB0aGUgdGVtcGxhdGUuXG4gKiBUaGUgY29udGV4dCBmb3IgZWFjaCBlbWJlZGRlZCB2aWV3IGlzIGxvZ2ljYWxseSBtZXJnZWQgdG8gdGhlIGN1cnJlbnQgY29tcG9uZW50XG4gKiBjb250ZXh0IGFjY29yZGluZyB0byBpdHMgbGV4aWNhbCBwb3NpdGlvbi5cbiAqXG4gKiBXaGVuIHVzaW5nIHRoZSBzaG9ydGhhbmQgc3ludGF4LCBBbmd1bGFyIGFsbG93cyBvbmx5IFtvbmUgc3RydWN0dXJhbCBkaXJlY3RpdmVcbiAqIG9uIGFuIGVsZW1lbnRdKGd1aWRlL3N0cnVjdHVyYWwtZGlyZWN0aXZlcyNvbmUtc3RydWN0dXJhbC1kaXJlY3RpdmUtcGVyLWhvc3QtZWxlbWVudCkuXG4gKiBJZiB5b3Ugd2FudCB0byBpdGVyYXRlIGNvbmRpdGlvbmFsbHksIGZvciBleGFtcGxlLFxuICogcHV0IHRoZSBgKm5nSWZgIG9uIGEgY29udGFpbmVyIGVsZW1lbnQgdGhhdCB3cmFwcyB0aGUgYCpuZ0ZvcmAgZWxlbWVudC5cbiAqIEZvciBmdXRoZXIgZGlzY3Vzc2lvbiwgc2VlXG4gKiBbU3RydWN0dXJhbCBEaXJlY3RpdmVzXShndWlkZS9zdHJ1Y3R1cmFsLWRpcmVjdGl2ZXMjb25lLXBlci1lbGVtZW50KS5cbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICpcbiAqICMjIyBMb2NhbCB2YXJpYWJsZXNcbiAqXG4gKiBgTmdGb3JPZmAgcHJvdmlkZXMgZXhwb3J0ZWQgdmFsdWVzIHRoYXQgY2FuIGJlIGFsaWFzZWQgdG8gbG9jYWwgdmFyaWFibGVzLlxuICogRm9yIGV4YW1wbGU6XG4gKlxuICogIGBgYFxuICogPGxpICpuZ0Zvcj1cImxldCB1c2VyIG9mIHVzZXJPYnNlcnZhYmxlIHwgYXN5bmMgYXMgdXNlcnM7IGluZGV4IGFzIGk7IGZpcnN0IGFzIGlzRmlyc3RcIj5cbiAqICAgIHt7aX19L3t7dXNlcnMubGVuZ3RofX0uIHt7dXNlcn19IDxzcGFuICpuZ0lmPVwiaXNGaXJzdFwiPmRlZmF1bHQ8L3NwYW4+XG4gKiA8L2xpPlxuICogYGBgXG4gKlxuICogVGhlIGZvbGxvd2luZyBleHBvcnRlZCB2YWx1ZXMgY2FuIGJlIGFsaWFzZWQgdG8gbG9jYWwgdmFyaWFibGVzOlxuICpcbiAqIC0gYCRpbXBsaWNpdDogVGA6IFRoZSB2YWx1ZSBvZiB0aGUgaW5kaXZpZHVhbCBpdGVtcyBpbiB0aGUgaXRlcmFibGUgKGBuZ0Zvck9mYCkuXG4gKiAtIGBuZ0Zvck9mOiBOZ0l0ZXJhYmxlPFQ+YDogVGhlIHZhbHVlIG9mIHRoZSBpdGVyYWJsZSBleHByZXNzaW9uLiBVc2VmdWwgd2hlbiB0aGUgZXhwcmVzc2lvbiBpc1xuICogbW9yZSBjb21wbGV4IHRoZW4gYSBwcm9wZXJ0eSBhY2Nlc3MsIGZvciBleGFtcGxlIHdoZW4gdXNpbmcgdGhlIGFzeW5jIHBpcGUgKGB1c2VyU3RyZWFtcyB8XG4gKiBhc3luY2ApLlxuICogLSBgaW5kZXg6IG51bWJlcmA6IFRoZSBpbmRleCBvZiB0aGUgY3VycmVudCBpdGVtIGluIHRoZSBpdGVyYWJsZS5cbiAqIC0gYGZpcnN0OiBib29sZWFuYDogVHJ1ZSB3aGVuIHRoZSBpdGVtIGlzIHRoZSBmaXJzdCBpdGVtIGluIHRoZSBpdGVyYWJsZS5cbiAqIC0gYGxhc3Q6IGJvb2xlYW5gOiBUcnVlIHdoZW4gdGhlIGl0ZW0gaXMgdGhlIGxhc3QgaXRlbSBpbiB0aGUgaXRlcmFibGUuXG4gKiAtIGBldmVuOiBib29sZWFuYDogVHJ1ZSB3aGVuIHRoZSBpdGVtIGhhcyBhbiBldmVuIGluZGV4IGluIHRoZSBpdGVyYWJsZS5cbiAqIC0gYG9kZDogYm9vbGVhbmA6IFRydWUgd2hlbiB0aGUgaXRlbSBoYXMgYW4gb2RkIGluZGV4IGluIHRoZSBpdGVyYWJsZS5cbiAqXG4gKiAjIyMgQ2hhbmdlIHByb3BhZ2F0aW9uXG4gKlxuICogV2hlbiB0aGUgY29udGVudHMgb2YgdGhlIGl0ZXJhdG9yIGNoYW5nZXMsIGBOZ0Zvck9mYCBtYWtlcyB0aGUgY29ycmVzcG9uZGluZyBjaGFuZ2VzIHRvIHRoZSBET006XG4gKlxuICogKiBXaGVuIGFuIGl0ZW0gaXMgYWRkZWQsIGEgbmV3IGluc3RhbmNlIG9mIHRoZSB0ZW1wbGF0ZSBpcyBhZGRlZCB0byB0aGUgRE9NLlxuICogKiBXaGVuIGFuIGl0ZW0gaXMgcmVtb3ZlZCwgaXRzIHRlbXBsYXRlIGluc3RhbmNlIGlzIHJlbW92ZWQgZnJvbSB0aGUgRE9NLlxuICogKiBXaGVuIGl0ZW1zIGFyZSByZW9yZGVyZWQsIHRoZWlyIHJlc3BlY3RpdmUgdGVtcGxhdGVzIGFyZSByZW9yZGVyZWQgaW4gdGhlIERPTS5cbiAqXG4gKiBBbmd1bGFyIHVzZXMgb2JqZWN0IGlkZW50aXR5IHRvIHRyYWNrIGluc2VydGlvbnMgYW5kIGRlbGV0aW9ucyB3aXRoaW4gdGhlIGl0ZXJhdG9yIGFuZCByZXByb2R1Y2VcbiAqIHRob3NlIGNoYW5nZXMgaW4gdGhlIERPTS4gVGhpcyBoYXMgaW1wb3J0YW50IGltcGxpY2F0aW9ucyBmb3IgYW5pbWF0aW9ucyBhbmQgYW55IHN0YXRlZnVsXG4gKiBjb250cm9scyB0aGF0IGFyZSBwcmVzZW50LCBzdWNoIGFzIGA8aW5wdXQ+YCBlbGVtZW50cyB0aGF0IGFjY2VwdCB1c2VyIGlucHV0LiBJbnNlcnRlZCByb3dzIGNhblxuICogYmUgYW5pbWF0ZWQgaW4sIGRlbGV0ZWQgcm93cyBjYW4gYmUgYW5pbWF0ZWQgb3V0LCBhbmQgdW5jaGFuZ2VkIHJvd3MgcmV0YWluIGFueSB1bnNhdmVkIHN0YXRlXG4gKiBzdWNoIGFzIHVzZXIgaW5wdXQuXG4gKiBGb3IgbW9yZSBvbiBhbmltYXRpb25zLCBzZWUgW1RyYW5zaXRpb25zIGFuZCBUcmlnZ2Vyc10oZ3VpZGUvdHJhbnNpdGlvbi1hbmQtdHJpZ2dlcnMpLlxuICpcbiAqIFRoZSBpZGVudGl0aWVzIG9mIGVsZW1lbnRzIGluIHRoZSBpdGVyYXRvciBjYW4gY2hhbmdlIHdoaWxlIHRoZSBkYXRhIGRvZXMgbm90LlxuICogVGhpcyBjYW4gaGFwcGVuLCBmb3IgZXhhbXBsZSwgaWYgdGhlIGl0ZXJhdG9yIGlzIHByb2R1Y2VkIGZyb20gYW4gUlBDIHRvIHRoZSBzZXJ2ZXIsIGFuZCB0aGF0XG4gKiBSUEMgaXMgcmUtcnVuLiBFdmVuIGlmIHRoZSBkYXRhIGhhc24ndCBjaGFuZ2VkLCB0aGUgc2Vjb25kIHJlc3BvbnNlIHByb2R1Y2VzIG9iamVjdHMgd2l0aFxuICogZGlmZmVyZW50IGlkZW50aXRpZXMsIGFuZCBBbmd1bGFyIG11c3QgdGVhciBkb3duIHRoZSBlbnRpcmUgRE9NIGFuZCByZWJ1aWxkIGl0IChhcyBpZiBhbGwgb2xkXG4gKiBlbGVtZW50cyB3ZXJlIGRlbGV0ZWQgYW5kIGFsbCBuZXcgZWxlbWVudHMgaW5zZXJ0ZWQpLlxuICpcbiAqIFRvIGF2b2lkIHRoaXMgZXhwZW5zaXZlIG9wZXJhdGlvbiwgeW91IGNhbiBjdXN0b21pemUgdGhlIGRlZmF1bHQgdHJhY2tpbmcgYWxnb3JpdGhtLlxuICogYnkgc3VwcGx5aW5nIHRoZSBgdHJhY2tCeWAgb3B0aW9uIHRvIGBOZ0Zvck9mYC5cbiAqIGB0cmFja0J5YCB0YWtlcyBhIGZ1bmN0aW9uIHRoYXQgaGFzIHR3byBhcmd1bWVudHM6IGBpbmRleGAgYW5kIGBpdGVtYC5cbiAqIElmIGB0cmFja0J5YCBpcyBnaXZlbiwgQW5ndWxhciB0cmFja3MgY2hhbmdlcyBieSB0aGUgcmV0dXJuIHZhbHVlIG9mIHRoZSBmdW5jdGlvbi5cbiAqXG4gKiBAc2VlIFtTdHJ1Y3R1cmFsIERpcmVjdGl2ZXNdKGd1aWRlL3N0cnVjdHVyYWwtZGlyZWN0aXZlcylcbiAqIEBuZ01vZHVsZSBDb21tb25Nb2R1bGVcbiAqIEBwdWJsaWNBcGlcbiAqL1xuQERpcmVjdGl2ZSh7c2VsZWN0b3I6ICdbbmdGb3JdW25nRm9yT2ZdJ30pXG5leHBvcnQgY2xhc3MgTmdGb3JPZjxUPiBpbXBsZW1lbnRzIERvQ2hlY2sge1xuICAvKipcbiAgICogVGhlIHZhbHVlIG9mIHRoZSBpdGVyYWJsZSBleHByZXNzaW9uLCB3aGljaCBjYW4gYmUgdXNlZCBhcyBhXG4gICAqIFt0ZW1wbGF0ZSBpbnB1dCB2YXJpYWJsZV0oZ3VpZGUvc3RydWN0dXJhbC1kaXJlY3RpdmVzI3RlbXBsYXRlLWlucHV0LXZhcmlhYmxlKS5cbiAgICovXG4gIEBJbnB1dCgpXG4gIHNldCBuZ0Zvck9mKG5nRm9yT2Y6IE5nSXRlcmFibGU8VD4pIHtcbiAgICB0aGlzLl9uZ0Zvck9mID0gbmdGb3JPZjtcbiAgICB0aGlzLl9uZ0Zvck9mRGlydHkgPSB0cnVlO1xuICB9XG4gIC8qKlxuICAgKiBBIGZ1bmN0aW9uIHRoYXQgZGVmaW5lcyBob3cgdG8gdHJhY2sgY2hhbmdlcyBmb3IgaXRlbXMgaW4gdGhlIGl0ZXJhYmxlLlxuICAgKlxuICAgKiBXaGVuIGl0ZW1zIGFyZSBhZGRlZCwgbW92ZWQsIG9yIHJlbW92ZWQgaW4gdGhlIGl0ZXJhYmxlLFxuICAgKiB0aGUgZGlyZWN0aXZlIG11c3QgcmUtcmVuZGVyIHRoZSBhcHByb3ByaWF0ZSBET00gbm9kZXMuXG4gICAqIFRvIG1pbmltaXplIGNodXJuIGluIHRoZSBET00sIG9ubHkgbm9kZXMgdGhhdCBoYXZlIGNoYW5nZWRcbiAgICogYXJlIHJlLXJlbmRlcmVkLlxuICAgKlxuICAgKiBCeSBkZWZhdWx0LCB0aGUgY2hhbmdlIGRldGVjdG9yIGFzc3VtZXMgdGhhdFxuICAgKiB0aGUgb2JqZWN0IGluc3RhbmNlIGlkZW50aWZpZXMgdGhlIG5vZGUgaW4gdGhlIGl0ZXJhYmxlLlxuICAgKiBXaGVuIHRoaXMgZnVuY3Rpb24gaXMgc3VwcGxpZWQsIHRoZSBkaXJlY3RpdmUgdXNlc1xuICAgKiB0aGUgcmVzdWx0IG9mIGNhbGxpbmcgdGhpcyBmdW5jdGlvbiB0byBpZGVudGlmeSB0aGUgaXRlbSBub2RlLFxuICAgKiByYXRoZXIgdGhhbiB0aGUgaWRlbnRpdHkgb2YgdGhlIG9iamVjdCBpdHNlbGYuXG4gICAqXG4gICAqIFRoZSBmdW5jdGlvbiByZWNlaXZlcyB0d28gaW5wdXRzLFxuICAgKiB0aGUgaXRlcmF0aW9uIGluZGV4IGFuZCB0aGUgbm9kZSBvYmplY3QgSUQuXG4gICAqL1xuICBASW5wdXQoKVxuICBzZXQgbmdGb3JUcmFja0J5KGZuOiBUcmFja0J5RnVuY3Rpb248VD4pIHtcbiAgICBpZiAoaXNEZXZNb2RlKCkgJiYgZm4gIT0gbnVsbCAmJiB0eXBlb2YgZm4gIT09ICdmdW5jdGlvbicpIHtcbiAgICAgIC8vIFRPRE8odmljYik6IHVzZSBhIGxvZyBzZXJ2aWNlIG9uY2UgdGhlcmUgaXMgYSBwdWJsaWMgb25lIGF2YWlsYWJsZVxuICAgICAgaWYgKDxhbnk+Y29uc29sZSAmJiA8YW55PmNvbnNvbGUud2Fybikge1xuICAgICAgICBjb25zb2xlLndhcm4oXG4gICAgICAgICAgICBgdHJhY2tCeSBtdXN0IGJlIGEgZnVuY3Rpb24sIGJ1dCByZWNlaXZlZCAke0pTT04uc3RyaW5naWZ5KGZuKX0uIGAgK1xuICAgICAgICAgICAgYFNlZSBodHRwczovL2FuZ3VsYXIuaW8vZG9jcy90cy9sYXRlc3QvYXBpL2NvbW1vbi9pbmRleC9OZ0Zvci1kaXJlY3RpdmUuaHRtbCMhI2NoYW5nZS1wcm9wYWdhdGlvbiBmb3IgbW9yZSBpbmZvcm1hdGlvbi5gKTtcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy5fdHJhY2tCeUZuID0gZm47XG4gIH1cblxuICBnZXQgbmdGb3JUcmFja0J5KCk6IFRyYWNrQnlGdW5jdGlvbjxUPiB7IHJldHVybiB0aGlzLl90cmFja0J5Rm47IH1cblxuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfbmdGb3JPZiAhOiBOZ0l0ZXJhYmxlPFQ+O1xuICBwcml2YXRlIF9uZ0Zvck9mRGlydHk6IGJvb2xlYW4gPSB0cnVlO1xuICBwcml2YXRlIF9kaWZmZXI6IEl0ZXJhYmxlRGlmZmVyPFQ+fG51bGwgPSBudWxsO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfdHJhY2tCeUZuICE6IFRyYWNrQnlGdW5jdGlvbjxUPjtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHByaXZhdGUgX3ZpZXdDb250YWluZXI6IFZpZXdDb250YWluZXJSZWYsIHByaXZhdGUgX3RlbXBsYXRlOiBUZW1wbGF0ZVJlZjxOZ0Zvck9mQ29udGV4dDxUPj4sXG4gICAgICBwcml2YXRlIF9kaWZmZXJzOiBJdGVyYWJsZURpZmZlcnMpIHt9XG5cbiAgLyoqXG4gICAqIEEgcmVmZXJlbmNlIHRvIHRoZSB0ZW1wbGF0ZSB0aGF0IGlzIHN0YW1wZWQgb3V0IGZvciBlYWNoIGl0ZW0gaW4gdGhlIGl0ZXJhYmxlLlxuICAgKiBAc2VlIFt0ZW1wbGF0ZSByZWZlcmVuY2UgdmFyaWFibGVdKGd1aWRlL3RlbXBsYXRlLXN5bnRheCN0ZW1wbGF0ZS1yZWZlcmVuY2UtdmFyaWFibGVzLS12YXItKVxuICAgKi9cbiAgQElucHV0KClcbiAgc2V0IG5nRm9yVGVtcGxhdGUodmFsdWU6IFRlbXBsYXRlUmVmPE5nRm9yT2ZDb250ZXh0PFQ+Pikge1xuICAgIC8vIFRPRE8oVFMyLjEpOiBtYWtlIFRlbXBsYXRlUmVmPFBhcnRpYWw8TmdGb3JSb3dPZjxUPj4+IG9uY2Ugd2UgbW92ZSB0byBUUyB2Mi4xXG4gICAgLy8gVGhlIGN1cnJlbnQgdHlwZSBpcyB0b28gcmVzdHJpY3RpdmU7IGEgdGVtcGxhdGUgdGhhdCBqdXN0IHVzZXMgaW5kZXgsIGZvciBleGFtcGxlLFxuICAgIC8vIHNob3VsZCBiZSBhY2NlcHRhYmxlLlxuICAgIGlmICh2YWx1ZSkge1xuICAgICAgdGhpcy5fdGVtcGxhdGUgPSB2YWx1ZTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQXBwbGllcyB0aGUgY2hhbmdlcyB3aGVuIG5lZWRlZC5cbiAgICovXG4gIG5nRG9DaGVjaygpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5fbmdGb3JPZkRpcnR5KSB7XG4gICAgICB0aGlzLl9uZ0Zvck9mRGlydHkgPSBmYWxzZTtcbiAgICAgIC8vIFJlYWN0IG9uIG5nRm9yT2YgY2hhbmdlcyBvbmx5IG9uY2UgYWxsIGlucHV0cyBoYXZlIGJlZW4gaW5pdGlhbGl6ZWRcbiAgICAgIGNvbnN0IHZhbHVlID0gdGhpcy5fbmdGb3JPZjtcbiAgICAgIGlmICghdGhpcy5fZGlmZmVyICYmIHZhbHVlKSB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgdGhpcy5fZGlmZmVyID0gdGhpcy5fZGlmZmVycy5maW5kKHZhbHVlKS5jcmVhdGUodGhpcy5uZ0ZvclRyYWNrQnkpO1xuICAgICAgICB9IGNhdGNoIHtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgICAgIGBDYW5ub3QgZmluZCBhIGRpZmZlciBzdXBwb3J0aW5nIG9iamVjdCAnJHt2YWx1ZX0nIG9mIHR5cGUgJyR7Z2V0VHlwZU5hbWUodmFsdWUpfScuIE5nRm9yIG9ubHkgc3VwcG9ydHMgYmluZGluZyB0byBJdGVyYWJsZXMgc3VjaCBhcyBBcnJheXMuYCk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gICAgaWYgKHRoaXMuX2RpZmZlcikge1xuICAgICAgY29uc3QgY2hhbmdlcyA9IHRoaXMuX2RpZmZlci5kaWZmKHRoaXMuX25nRm9yT2YpO1xuICAgICAgaWYgKGNoYW5nZXMpIHRoaXMuX2FwcGx5Q2hhbmdlcyhjaGFuZ2VzKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIF9hcHBseUNoYW5nZXMoY2hhbmdlczogSXRlcmFibGVDaGFuZ2VzPFQ+KSB7XG4gICAgY29uc3QgaW5zZXJ0VHVwbGVzOiBSZWNvcmRWaWV3VHVwbGU8VD5bXSA9IFtdO1xuICAgIGNoYW5nZXMuZm9yRWFjaE9wZXJhdGlvbihcbiAgICAgICAgKGl0ZW06IEl0ZXJhYmxlQ2hhbmdlUmVjb3JkPGFueT4sIGFkanVzdGVkUHJldmlvdXNJbmRleDogbnVtYmVyIHwgbnVsbCxcbiAgICAgICAgIGN1cnJlbnRJbmRleDogbnVtYmVyIHwgbnVsbCkgPT4ge1xuICAgICAgICAgIGlmIChpdGVtLnByZXZpb3VzSW5kZXggPT0gbnVsbCkge1xuICAgICAgICAgICAgY29uc3QgdmlldyA9IHRoaXMuX3ZpZXdDb250YWluZXIuY3JlYXRlRW1iZWRkZWRWaWV3KFxuICAgICAgICAgICAgICAgIHRoaXMuX3RlbXBsYXRlLCBuZXcgTmdGb3JPZkNvbnRleHQ8VD4obnVsbCAhLCB0aGlzLl9uZ0Zvck9mLCAtMSwgLTEpLFxuICAgICAgICAgICAgICAgIGN1cnJlbnRJbmRleCA9PT0gbnVsbCA/IHVuZGVmaW5lZCA6IGN1cnJlbnRJbmRleCk7XG4gICAgICAgICAgICBjb25zdCB0dXBsZSA9IG5ldyBSZWNvcmRWaWV3VHVwbGU8VD4oaXRlbSwgdmlldyk7XG4gICAgICAgICAgICBpbnNlcnRUdXBsZXMucHVzaCh0dXBsZSk7XG4gICAgICAgICAgfSBlbHNlIGlmIChjdXJyZW50SW5kZXggPT0gbnVsbCkge1xuICAgICAgICAgICAgdGhpcy5fdmlld0NvbnRhaW5lci5yZW1vdmUoXG4gICAgICAgICAgICAgICAgYWRqdXN0ZWRQcmV2aW91c0luZGV4ID09PSBudWxsID8gdW5kZWZpbmVkIDogYWRqdXN0ZWRQcmV2aW91c0luZGV4KTtcbiAgICAgICAgICB9IGVsc2UgaWYgKGFkanVzdGVkUHJldmlvdXNJbmRleCAhPT0gbnVsbCkge1xuICAgICAgICAgICAgY29uc3QgdmlldyA9IHRoaXMuX3ZpZXdDb250YWluZXIuZ2V0KGFkanVzdGVkUHJldmlvdXNJbmRleCkgITtcbiAgICAgICAgICAgIHRoaXMuX3ZpZXdDb250YWluZXIubW92ZSh2aWV3LCBjdXJyZW50SW5kZXgpO1xuICAgICAgICAgICAgY29uc3QgdHVwbGUgPSBuZXcgUmVjb3JkVmlld1R1cGxlKGl0ZW0sIDxFbWJlZGRlZFZpZXdSZWY8TmdGb3JPZkNvbnRleHQ8VD4+PnZpZXcpO1xuICAgICAgICAgICAgaW5zZXJ0VHVwbGVzLnB1c2godHVwbGUpO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGluc2VydFR1cGxlcy5sZW5ndGg7IGkrKykge1xuICAgICAgdGhpcy5fcGVyVmlld0NoYW5nZShpbnNlcnRUdXBsZXNbaV0udmlldywgaW5zZXJ0VHVwbGVzW2ldLnJlY29yZCk7XG4gICAgfVxuXG4gICAgZm9yIChsZXQgaSA9IDAsIGlsZW4gPSB0aGlzLl92aWV3Q29udGFpbmVyLmxlbmd0aDsgaSA8IGlsZW47IGkrKykge1xuICAgICAgY29uc3Qgdmlld1JlZiA9IDxFbWJlZGRlZFZpZXdSZWY8TmdGb3JPZkNvbnRleHQ8VD4+PnRoaXMuX3ZpZXdDb250YWluZXIuZ2V0KGkpO1xuICAgICAgdmlld1JlZi5jb250ZXh0LmluZGV4ID0gaTtcbiAgICAgIHZpZXdSZWYuY29udGV4dC5jb3VudCA9IGlsZW47XG4gICAgICB2aWV3UmVmLmNvbnRleHQubmdGb3JPZiA9IHRoaXMuX25nRm9yT2Y7XG4gICAgfVxuXG4gICAgY2hhbmdlcy5mb3JFYWNoSWRlbnRpdHlDaGFuZ2UoKHJlY29yZDogYW55KSA9PiB7XG4gICAgICBjb25zdCB2aWV3UmVmID1cbiAgICAgICAgICA8RW1iZWRkZWRWaWV3UmVmPE5nRm9yT2ZDb250ZXh0PFQ+Pj50aGlzLl92aWV3Q29udGFpbmVyLmdldChyZWNvcmQuY3VycmVudEluZGV4KTtcbiAgICAgIHZpZXdSZWYuY29udGV4dC4kaW1wbGljaXQgPSByZWNvcmQuaXRlbTtcbiAgICB9KTtcbiAgfVxuXG4gIHByaXZhdGUgX3BlclZpZXdDaGFuZ2UoXG4gICAgICB2aWV3OiBFbWJlZGRlZFZpZXdSZWY8TmdGb3JPZkNvbnRleHQ8VD4+LCByZWNvcmQ6IEl0ZXJhYmxlQ2hhbmdlUmVjb3JkPGFueT4pIHtcbiAgICB2aWV3LmNvbnRleHQuJGltcGxpY2l0ID0gcmVjb3JkLml0ZW07XG4gIH1cblxuICAvKipcbiAgICogQXNzZXJ0cyB0aGUgY29ycmVjdCB0eXBlIG9mIHRoZSBjb250ZXh0IGZvciB0aGUgdGVtcGxhdGUgdGhhdCBgTmdGb3JPZmAgd2lsbCByZW5kZXIuXG4gICAqXG4gICAqIFRoZSBwcmVzZW5jZSBvZiB0aGlzIG1ldGhvZCBpcyBhIHNpZ25hbCB0byB0aGUgSXZ5IHRlbXBsYXRlIHR5cGUtY2hlY2sgY29tcGlsZXIgdGhhdCB0aGVcbiAgICogYE5nRm9yT2ZgIHN0cnVjdHVyYWwgZGlyZWN0aXZlIHJlbmRlcnMgaXRzIHRlbXBsYXRlIHdpdGggYSBzcGVjaWZpYyBjb250ZXh0IHR5cGUuXG4gICAqL1xuICBzdGF0aWMgbmdUZW1wbGF0ZUNvbnRleHRHdWFyZDxUPihkaXI6IE5nRm9yT2Y8VD4sIGN0eDogYW55KTogY3R4IGlzIE5nRm9yT2ZDb250ZXh0PFQ+IHtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxufVxuXG5jbGFzcyBSZWNvcmRWaWV3VHVwbGU8VD4ge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgcmVjb3JkOiBhbnksIHB1YmxpYyB2aWV3OiBFbWJlZGRlZFZpZXdSZWY8TmdGb3JPZkNvbnRleHQ8VD4+KSB7fVxufVxuXG5mdW5jdGlvbiBnZXRUeXBlTmFtZSh0eXBlOiBhbnkpOiBzdHJpbmcge1xuICByZXR1cm4gdHlwZVsnbmFtZSddIHx8IHR5cGVvZiB0eXBlO1xufVxuIl19
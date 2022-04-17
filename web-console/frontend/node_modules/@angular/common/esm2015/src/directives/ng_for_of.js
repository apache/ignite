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
import { Directive, Input, IterableDiffers, TemplateRef, ViewContainerRef, isDevMode } from '@angular/core';
/**
 * \@publicApi
 * @template T
 */
export class NgForOfContext {
    /**
     * @param {?} $implicit
     * @param {?} ngForOf
     * @param {?} index
     * @param {?} count
     */
    constructor($implicit, ngForOf, index, count) {
        this.$implicit = $implicit;
        this.ngForOf = ngForOf;
        this.index = index;
        this.count = count;
    }
    /**
     * @return {?}
     */
    get first() { return this.index === 0; }
    /**
     * @return {?}
     */
    get last() { return this.index === this.count - 1; }
    /**
     * @return {?}
     */
    get even() { return this.index % 2 === 0; }
    /**
     * @return {?}
     */
    get odd() { return !this.even; }
}
if (false) {
    /** @type {?} */
    NgForOfContext.prototype.$implicit;
    /** @type {?} */
    NgForOfContext.prototype.ngForOf;
    /** @type {?} */
    NgForOfContext.prototype.index;
    /** @type {?} */
    NgForOfContext.prototype.count;
}
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
 * \@usageNotes
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
 * \@ngModule CommonModule
 * \@publicApi
 * @template T
 */
export class NgForOf {
    /**
     * @param {?} _viewContainer
     * @param {?} _template
     * @param {?} _differs
     */
    constructor(_viewContainer, _template, _differs) {
        this._viewContainer = _viewContainer;
        this._template = _template;
        this._differs = _differs;
        this._ngForOfDirty = true;
        this._differ = null;
    }
    /**
     * The value of the iterable expression, which can be used as a
     * [template input variable](guide/structural-directives#template-input-variable).
     * @param {?} ngForOf
     * @return {?}
     */
    set ngForOf(ngForOf) {
        this._ngForOf = ngForOf;
        this._ngForOfDirty = true;
    }
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
     * @param {?} fn
     * @return {?}
     */
    set ngForTrackBy(fn) {
        if (isDevMode() && fn != null && typeof fn !== 'function') {
            // TODO(vicb): use a log service once there is a public one available
            if ((/** @type {?} */ (console)) && (/** @type {?} */ (console.warn))) {
                console.warn(`trackBy must be a function, but received ${JSON.stringify(fn)}. ` +
                    `See https://angular.io/docs/ts/latest/api/common/index/NgFor-directive.html#!#change-propagation for more information.`);
            }
        }
        this._trackByFn = fn;
    }
    /**
     * @return {?}
     */
    get ngForTrackBy() { return this._trackByFn; }
    /**
     * A reference to the template that is stamped out for each item in the iterable.
     * @see [template reference variable](guide/template-syntax#template-reference-variables--var-)
     * @param {?} value
     * @return {?}
     */
    set ngForTemplate(value) {
        // TODO(TS2.1): make TemplateRef<Partial<NgForRowOf<T>>> once we move to TS v2.1
        // The current type is too restrictive; a template that just uses index, for example,
        // should be acceptable.
        if (value) {
            this._template = value;
        }
    }
    /**
     * Applies the changes when needed.
     * @return {?}
     */
    ngDoCheck() {
        if (this._ngForOfDirty) {
            this._ngForOfDirty = false;
            // React on ngForOf changes only once all inputs have been initialized
            /** @type {?} */
            const value = this._ngForOf;
            if (!this._differ && value) {
                try {
                    this._differ = this._differs.find(value).create(this.ngForTrackBy);
                }
                catch (_a) {
                    throw new Error(`Cannot find a differ supporting object '${value}' of type '${getTypeName(value)}'. NgFor only supports binding to Iterables such as Arrays.`);
                }
            }
        }
        if (this._differ) {
            /** @type {?} */
            const changes = this._differ.diff(this._ngForOf);
            if (changes)
                this._applyChanges(changes);
        }
    }
    /**
     * @private
     * @param {?} changes
     * @return {?}
     */
    _applyChanges(changes) {
        /** @type {?} */
        const insertTuples = [];
        changes.forEachOperation((/**
         * @param {?} item
         * @param {?} adjustedPreviousIndex
         * @param {?} currentIndex
         * @return {?}
         */
        (item, adjustedPreviousIndex, currentIndex) => {
            if (item.previousIndex == null) {
                /** @type {?} */
                const view = this._viewContainer.createEmbeddedView(this._template, new NgForOfContext((/** @type {?} */ (null)), this._ngForOf, -1, -1), currentIndex === null ? undefined : currentIndex);
                /** @type {?} */
                const tuple = new RecordViewTuple(item, view);
                insertTuples.push(tuple);
            }
            else if (currentIndex == null) {
                this._viewContainer.remove(adjustedPreviousIndex === null ? undefined : adjustedPreviousIndex);
            }
            else if (adjustedPreviousIndex !== null) {
                /** @type {?} */
                const view = (/** @type {?} */ (this._viewContainer.get(adjustedPreviousIndex)));
                this._viewContainer.move(view, currentIndex);
                /** @type {?} */
                const tuple = new RecordViewTuple(item, (/** @type {?} */ (view)));
                insertTuples.push(tuple);
            }
        }));
        for (let i = 0; i < insertTuples.length; i++) {
            this._perViewChange(insertTuples[i].view, insertTuples[i].record);
        }
        for (let i = 0, ilen = this._viewContainer.length; i < ilen; i++) {
            /** @type {?} */
            const viewRef = (/** @type {?} */ (this._viewContainer.get(i)));
            viewRef.context.index = i;
            viewRef.context.count = ilen;
            viewRef.context.ngForOf = this._ngForOf;
        }
        changes.forEachIdentityChange((/**
         * @param {?} record
         * @return {?}
         */
        (record) => {
            /** @type {?} */
            const viewRef = (/** @type {?} */ (this._viewContainer.get(record.currentIndex)));
            viewRef.context.$implicit = record.item;
        }));
    }
    /**
     * @private
     * @param {?} view
     * @param {?} record
     * @return {?}
     */
    _perViewChange(view, record) {
        view.context.$implicit = record.item;
    }
    /**
     * Asserts the correct type of the context for the template that `NgForOf` will render.
     *
     * The presence of this method is a signal to the Ivy template type-check compiler that the
     * `NgForOf` structural directive renders its template with a specific context type.
     * @template T
     * @param {?} dir
     * @param {?} ctx
     * @return {?}
     */
    static ngTemplateContextGuard(dir, ctx) {
        return true;
    }
}
NgForOf.decorators = [
    { type: Directive, args: [{ selector: '[ngFor][ngForOf]' },] }
];
/** @nocollapse */
NgForOf.ctorParameters = () => [
    { type: ViewContainerRef },
    { type: TemplateRef },
    { type: IterableDiffers }
];
NgForOf.propDecorators = {
    ngForOf: [{ type: Input }],
    ngForTrackBy: [{ type: Input }],
    ngForTemplate: [{ type: Input }]
};
if (false) {
    /**
     * @type {?}
     * @private
     */
    NgForOf.prototype._ngForOf;
    /**
     * @type {?}
     * @private
     */
    NgForOf.prototype._ngForOfDirty;
    /**
     * @type {?}
     * @private
     */
    NgForOf.prototype._differ;
    /**
     * @type {?}
     * @private
     */
    NgForOf.prototype._trackByFn;
    /**
     * @type {?}
     * @private
     */
    NgForOf.prototype._viewContainer;
    /**
     * @type {?}
     * @private
     */
    NgForOf.prototype._template;
    /**
     * @type {?}
     * @private
     */
    NgForOf.prototype._differs;
}
/**
 * @template T
 */
class RecordViewTuple {
    /**
     * @param {?} record
     * @param {?} view
     */
    constructor(record, view) {
        this.record = record;
        this.view = view;
    }
}
if (false) {
    /** @type {?} */
    RecordViewTuple.prototype.record;
    /** @type {?} */
    RecordViewTuple.prototype.view;
}
/**
 * @param {?} type
 * @return {?}
 */
function getTypeName(type) {
    return type['name'] || typeof type;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfZm9yX29mLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL3NyYy9kaXJlY3RpdmVzL25nX2Zvcl9vZi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxTQUFTLEVBQTRCLEtBQUssRUFBeUQsZUFBZSxFQUFjLFdBQVcsRUFBbUIsZ0JBQWdCLEVBQWMsU0FBUyxFQUFDLE1BQU0sZUFBZSxDQUFDOzs7OztBQUtwTyxNQUFNLE9BQU8sY0FBYzs7Ozs7OztJQUN6QixZQUNXLFNBQVksRUFBUyxPQUFzQixFQUFTLEtBQWEsRUFDakUsS0FBYTtRQURiLGNBQVMsR0FBVCxTQUFTLENBQUc7UUFBUyxZQUFPLEdBQVAsT0FBTyxDQUFlO1FBQVMsVUFBSyxHQUFMLEtBQUssQ0FBUTtRQUNqRSxVQUFLLEdBQUwsS0FBSyxDQUFRO0lBQUcsQ0FBQzs7OztJQUU1QixJQUFJLEtBQUssS0FBYyxPQUFPLElBQUksQ0FBQyxLQUFLLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQzs7OztJQUVqRCxJQUFJLElBQUksS0FBYyxPQUFPLElBQUksQ0FBQyxLQUFLLEtBQUssSUFBSSxDQUFDLEtBQUssR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7O0lBRTdELElBQUksSUFBSSxLQUFjLE9BQU8sSUFBSSxDQUFDLEtBQUssR0FBRyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQzs7OztJQUVwRCxJQUFJLEdBQUcsS0FBYyxPQUFPLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUM7Q0FDMUM7OztJQVZLLG1DQUFtQjs7SUFBRSxpQ0FBNkI7O0lBQUUsK0JBQW9COztJQUN4RSwrQkFBb0I7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7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LElBQXdDO1FBQTVELFdBQU0sR0FBTixNQUFNLENBQUs7UUFBUyxTQUFJLEdBQUosSUFBSSxDQUFvQztJQUFHLENBQUM7Q0FDcEY7OztJQURhLGlDQUFrQjs7SUFBRSwrQkFBK0M7Ozs7OztBQUdqRixTQUFTLFdBQVcsQ0FBQyxJQUFTO0lBQzVCLE9BQU8sSUFBSSxDQUFDLE1BQU0sQ0FBQyxJQUFJLE9BQU8sSUFBSSxDQUFDO0FBQ3JDLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7RGlyZWN0aXZlLCBEb0NoZWNrLCBFbWJlZGRlZFZpZXdSZWYsIElucHV0LCBJdGVyYWJsZUNoYW5nZVJlY29yZCwgSXRlcmFibGVDaGFuZ2VzLCBJdGVyYWJsZURpZmZlciwgSXRlcmFibGVEaWZmZXJzLCBOZ0l0ZXJhYmxlLCBUZW1wbGF0ZVJlZiwgVHJhY2tCeUZ1bmN0aW9uLCBWaWV3Q29udGFpbmVyUmVmLCBmb3J3YXJkUmVmLCBpc0Rldk1vZGV9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG4vKipcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIE5nRm9yT2ZDb250ZXh0PFQ+IHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgJGltcGxpY2l0OiBULCBwdWJsaWMgbmdGb3JPZjogTmdJdGVyYWJsZTxUPiwgcHVibGljIGluZGV4OiBudW1iZXIsXG4gICAgICBwdWJsaWMgY291bnQ6IG51bWJlcikge31cblxuICBnZXQgZmlyc3QoKTogYm9vbGVhbiB7IHJldHVybiB0aGlzLmluZGV4ID09PSAwOyB9XG5cbiAgZ2V0IGxhc3QoKTogYm9vbGVhbiB7IHJldHVybiB0aGlzLmluZGV4ID09PSB0aGlzLmNvdW50IC0gMTsgfVxuXG4gIGdldCBldmVuKCk6IGJvb2xlYW4geyByZXR1cm4gdGhpcy5pbmRleCAlIDIgPT09IDA7IH1cblxuICBnZXQgb2RkKCk6IGJvb2xlYW4geyByZXR1cm4gIXRoaXMuZXZlbjsgfVxufVxuXG4vKipcbiAqIEEgW3N0cnVjdHVyYWwgZGlyZWN0aXZlXShndWlkZS9zdHJ1Y3R1cmFsLWRpcmVjdGl2ZXMpIHRoYXQgcmVuZGVyc1xuICogYSB0ZW1wbGF0ZSBmb3IgZWFjaCBpdGVtIGluIGEgY29sbGVjdGlvbi5cbiAqIFRoZSBkaXJlY3RpdmUgaXMgcGxhY2VkIG9uIGFuIGVsZW1lbnQsIHdoaWNoIGJlY29tZXMgdGhlIHBhcmVudFxuICogb2YgdGhlIGNsb25lZCB0ZW1wbGF0ZXMuXG4gKlxuICogVGhlIGBuZ0Zvck9mYCBkaXJlY3RpdmUgaXMgZ2VuZXJhbGx5IHVzZWQgaW4gdGhlXG4gKiBbc2hvcnRoYW5kIGZvcm1dKGd1aWRlL3N0cnVjdHVyYWwtZGlyZWN0aXZlcyN0aGUtYXN0ZXJpc2stLXByZWZpeCkgYCpuZ0ZvcmAuXG4gKiBJbiB0aGlzIGZvcm0sIHRoZSB0ZW1wbGF0ZSB0byBiZSByZW5kZXJlZCBmb3IgZWFjaCBpdGVyYXRpb24gaXMgdGhlIGNvbnRlbnRcbiAqIG9mIGFuIGFuY2hvciBlbGVtZW50IGNvbnRhaW5pbmcgdGhlIGRpcmVjdGl2ZS5cbiAqXG4gKiBUaGUgZm9sbG93aW5nIGV4YW1wbGUgc2hvd3MgdGhlIHNob3J0aGFuZCBzeW50YXggd2l0aCBzb21lIG9wdGlvbnMsXG4gKiBjb250YWluZWQgaW4gYW4gYDxsaT5gIGVsZW1lbnQuXG4gKlxuICogYGBgXG4gKiA8bGkgKm5nRm9yPVwibGV0IGl0ZW0gb2YgaXRlbXM7IGluZGV4IGFzIGk7IHRyYWNrQnk6IHRyYWNrQnlGblwiPi4uLjwvbGk+XG4gKiBgYGBcbiAqXG4gKiBUaGUgc2hvcnRoYW5kIGZvcm0gZXhwYW5kcyBpbnRvIGEgbG9uZyBmb3JtIHRoYXQgdXNlcyB0aGUgYG5nRm9yT2ZgIHNlbGVjdG9yXG4gKiBvbiBhbiBgPG5nLXRlbXBsYXRlPmAgZWxlbWVudC5cbiAqIFRoZSBjb250ZW50IG9mIHRoZSBgPG5nLXRlbXBsYXRlPmAgZWxlbWVudCBpcyB0aGUgYDxsaT5gIGVsZW1lbnQgdGhhdCBoZWxkIHRoZVxuICogc2hvcnQtZm9ybSBkaXJlY3RpdmUuXG4gKlxuICogSGVyZSBpcyB0aGUgZXhwYW5kZWQgdmVyc2lvbiBvZiB0aGUgc2hvcnQtZm9ybSBleGFtcGxlLlxuICpcbiAqIGBgYFxuICogPG5nLXRlbXBsYXRlIG5nRm9yIGxldC1pdGVtIFtuZ0Zvck9mXT1cIml0ZW1zXCIgbGV0LWk9XCJpbmRleFwiIFtuZ0ZvclRyYWNrQnldPVwidHJhY2tCeUZuXCI+XG4gKiAgIDxsaT4uLi48L2xpPlxuICogPC9uZy10ZW1wbGF0ZT5cbiAqIGBgYFxuICpcbiAqIEFuZ3VsYXIgYXV0b21hdGljYWxseSBleHBhbmRzIHRoZSBzaG9ydGhhbmQgc3ludGF4IGFzIGl0IGNvbXBpbGVzIHRoZSB0ZW1wbGF0ZS5cbiAqIFRoZSBjb250ZXh0IGZvciBlYWNoIGVtYmVkZGVkIHZpZXcgaXMgbG9naWNhbGx5IG1lcmdlZCB0byB0aGUgY3VycmVudCBjb21wb25lbnRcbiAqIGNvbnRleHQgYWNjb3JkaW5nIHRvIGl0cyBsZXhpY2FsIHBvc2l0aW9uLlxuICpcbiAqIFdoZW4gdXNpbmcgdGhlIHNob3J0aGFuZCBzeW50YXgsIEFuZ3VsYXIgYWxsb3dzIG9ubHkgW29uZSBzdHJ1Y3R1cmFsIGRpcmVjdGl2ZVxuICogb24gYW4gZWxlbWVudF0oZ3VpZGUvc3RydWN0dXJhbC1kaXJlY3RpdmVzI29uZS1zdHJ1Y3R1cmFsLWRpcmVjdGl2ZS1wZXItaG9zdC1lbGVtZW50KS5cbiAqIElmIHlvdSB3YW50IHRvIGl0ZXJhdGUgY29uZGl0aW9uYWxseSwgZm9yIGV4YW1wbGUsXG4gKiBwdXQgdGhlIGAqbmdJZmAgb24gYSBjb250YWluZXIgZWxlbWVudCB0aGF0IHdyYXBzIHRoZSBgKm5nRm9yYCBlbGVtZW50LlxuICogRm9yIGZ1dGhlciBkaXNjdXNzaW9uLCBzZWVcbiAqIFtTdHJ1Y3R1cmFsIERpcmVjdGl2ZXNdKGd1aWRlL3N0cnVjdHVyYWwtZGlyZWN0aXZlcyNvbmUtcGVyLWVsZW1lbnQpLlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKlxuICogIyMjIExvY2FsIHZhcmlhYmxlc1xuICpcbiAqIGBOZ0Zvck9mYCBwcm92aWRlcyBleHBvcnRlZCB2YWx1ZXMgdGhhdCBjYW4gYmUgYWxpYXNlZCB0byBsb2NhbCB2YXJpYWJsZXMuXG4gKiBGb3IgZXhhbXBsZTpcbiAqXG4gKiAgYGBgXG4gKiA8bGkgKm5nRm9yPVwibGV0IHVzZXIgb2YgdXNlck9ic2VydmFibGUgfCBhc3luYyBhcyB1c2VyczsgaW5kZXggYXMgaTsgZmlyc3QgYXMgaXNGaXJzdFwiPlxuICogICAge3tpfX0ve3t1c2Vycy5sZW5ndGh9fS4ge3t1c2VyfX0gPHNwYW4gKm5nSWY9XCJpc0ZpcnN0XCI+ZGVmYXVsdDwvc3Bhbj5cbiAqIDwvbGk+XG4gKiBgYGBcbiAqXG4gKiBUaGUgZm9sbG93aW5nIGV4cG9ydGVkIHZhbHVlcyBjYW4gYmUgYWxpYXNlZCB0byBsb2NhbCB2YXJpYWJsZXM6XG4gKlxuICogLSBgJGltcGxpY2l0OiBUYDogVGhlIHZhbHVlIG9mIHRoZSBpbmRpdmlkdWFsIGl0ZW1zIGluIHRoZSBpdGVyYWJsZSAoYG5nRm9yT2ZgKS5cbiAqIC0gYG5nRm9yT2Y6IE5nSXRlcmFibGU8VD5gOiBUaGUgdmFsdWUgb2YgdGhlIGl0ZXJhYmxlIGV4cHJlc3Npb24uIFVzZWZ1bCB3aGVuIHRoZSBleHByZXNzaW9uIGlzXG4gKiBtb3JlIGNvbXBsZXggdGhlbiBhIHByb3BlcnR5IGFjY2VzcywgZm9yIGV4YW1wbGUgd2hlbiB1c2luZyB0aGUgYXN5bmMgcGlwZSAoYHVzZXJTdHJlYW1zIHxcbiAqIGFzeW5jYCkuXG4gKiAtIGBpbmRleDogbnVtYmVyYDogVGhlIGluZGV4IG9mIHRoZSBjdXJyZW50IGl0ZW0gaW4gdGhlIGl0ZXJhYmxlLlxuICogLSBgZmlyc3Q6IGJvb2xlYW5gOiBUcnVlIHdoZW4gdGhlIGl0ZW0gaXMgdGhlIGZpcnN0IGl0ZW0gaW4gdGhlIGl0ZXJhYmxlLlxuICogLSBgbGFzdDogYm9vbGVhbmA6IFRydWUgd2hlbiB0aGUgaXRlbSBpcyB0aGUgbGFzdCBpdGVtIGluIHRoZSBpdGVyYWJsZS5cbiAqIC0gYGV2ZW46IGJvb2xlYW5gOiBUcnVlIHdoZW4gdGhlIGl0ZW0gaGFzIGFuIGV2ZW4gaW5kZXggaW4gdGhlIGl0ZXJhYmxlLlxuICogLSBgb2RkOiBib29sZWFuYDogVHJ1ZSB3aGVuIHRoZSBpdGVtIGhhcyBhbiBvZGQgaW5kZXggaW4gdGhlIGl0ZXJhYmxlLlxuICpcbiAqICMjIyBDaGFuZ2UgcHJvcGFnYXRpb25cbiAqXG4gKiBXaGVuIHRoZSBjb250ZW50cyBvZiB0aGUgaXRlcmF0b3IgY2hhbmdlcywgYE5nRm9yT2ZgIG1ha2VzIHRoZSBjb3JyZXNwb25kaW5nIGNoYW5nZXMgdG8gdGhlIERPTTpcbiAqXG4gKiAqIFdoZW4gYW4gaXRlbSBpcyBhZGRlZCwgYSBuZXcgaW5zdGFuY2Ugb2YgdGhlIHRlbXBsYXRlIGlzIGFkZGVkIHRvIHRoZSBET00uXG4gKiAqIFdoZW4gYW4gaXRlbSBpcyByZW1vdmVkLCBpdHMgdGVtcGxhdGUgaW5zdGFuY2UgaXMgcmVtb3ZlZCBmcm9tIHRoZSBET00uXG4gKiAqIFdoZW4gaXRlbXMgYXJlIHJlb3JkZXJlZCwgdGhlaXIgcmVzcGVjdGl2ZSB0ZW1wbGF0ZXMgYXJlIHJlb3JkZXJlZCBpbiB0aGUgRE9NLlxuICpcbiAqIEFuZ3VsYXIgdXNlcyBvYmplY3QgaWRlbnRpdHkgdG8gdHJhY2sgaW5zZXJ0aW9ucyBhbmQgZGVsZXRpb25zIHdpdGhpbiB0aGUgaXRlcmF0b3IgYW5kIHJlcHJvZHVjZVxuICogdGhvc2UgY2hhbmdlcyBpbiB0aGUgRE9NLiBUaGlzIGhhcyBpbXBvcnRhbnQgaW1wbGljYXRpb25zIGZvciBhbmltYXRpb25zIGFuZCBhbnkgc3RhdGVmdWxcbiAqIGNvbnRyb2xzIHRoYXQgYXJlIHByZXNlbnQsIHN1Y2ggYXMgYDxpbnB1dD5gIGVsZW1lbnRzIHRoYXQgYWNjZXB0IHVzZXIgaW5wdXQuIEluc2VydGVkIHJvd3MgY2FuXG4gKiBiZSBhbmltYXRlZCBpbiwgZGVsZXRlZCByb3dzIGNhbiBiZSBhbmltYXRlZCBvdXQsIGFuZCB1bmNoYW5nZWQgcm93cyByZXRhaW4gYW55IHVuc2F2ZWQgc3RhdGVcbiAqIHN1Y2ggYXMgdXNlciBpbnB1dC5cbiAqIEZvciBtb3JlIG9uIGFuaW1hdGlvbnMsIHNlZSBbVHJhbnNpdGlvbnMgYW5kIFRyaWdnZXJzXShndWlkZS90cmFuc2l0aW9uLWFuZC10cmlnZ2VycykuXG4gKlxuICogVGhlIGlkZW50aXRpZXMgb2YgZWxlbWVudHMgaW4gdGhlIGl0ZXJhdG9yIGNhbiBjaGFuZ2Ugd2hpbGUgdGhlIGRhdGEgZG9lcyBub3QuXG4gKiBUaGlzIGNhbiBoYXBwZW4sIGZvciBleGFtcGxlLCBpZiB0aGUgaXRlcmF0b3IgaXMgcHJvZHVjZWQgZnJvbSBhbiBSUEMgdG8gdGhlIHNlcnZlciwgYW5kIHRoYXRcbiAqIFJQQyBpcyByZS1ydW4uIEV2ZW4gaWYgdGhlIGRhdGEgaGFzbid0IGNoYW5nZWQsIHRoZSBzZWNvbmQgcmVzcG9uc2UgcHJvZHVjZXMgb2JqZWN0cyB3aXRoXG4gKiBkaWZmZXJlbnQgaWRlbnRpdGllcywgYW5kIEFuZ3VsYXIgbXVzdCB0ZWFyIGRvd24gdGhlIGVudGlyZSBET00gYW5kIHJlYnVpbGQgaXQgKGFzIGlmIGFsbCBvbGRcbiAqIGVsZW1lbnRzIHdlcmUgZGVsZXRlZCBhbmQgYWxsIG5ldyBlbGVtZW50cyBpbnNlcnRlZCkuXG4gKlxuICogVG8gYXZvaWQgdGhpcyBleHBlbnNpdmUgb3BlcmF0aW9uLCB5b3UgY2FuIGN1c3RvbWl6ZSB0aGUgZGVmYXVsdCB0cmFja2luZyBhbGdvcml0aG0uXG4gKiBieSBzdXBwbHlpbmcgdGhlIGB0cmFja0J5YCBvcHRpb24gdG8gYE5nRm9yT2ZgLlxuICogYHRyYWNrQnlgIHRha2VzIGEgZnVuY3Rpb24gdGhhdCBoYXMgdHdvIGFyZ3VtZW50czogYGluZGV4YCBhbmQgYGl0ZW1gLlxuICogSWYgYHRyYWNrQnlgIGlzIGdpdmVuLCBBbmd1bGFyIHRyYWNrcyBjaGFuZ2VzIGJ5IHRoZSByZXR1cm4gdmFsdWUgb2YgdGhlIGZ1bmN0aW9uLlxuICpcbiAqIEBzZWUgW1N0cnVjdHVyYWwgRGlyZWN0aXZlc10oZ3VpZGUvc3RydWN0dXJhbC1kaXJlY3RpdmVzKVxuICogQG5nTW9kdWxlIENvbW1vbk1vZHVsZVxuICogQHB1YmxpY0FwaVxuICovXG5ARGlyZWN0aXZlKHtzZWxlY3RvcjogJ1tuZ0Zvcl1bbmdGb3JPZl0nfSlcbmV4cG9ydCBjbGFzcyBOZ0Zvck9mPFQ+IGltcGxlbWVudHMgRG9DaGVjayB7XG4gIC8qKlxuICAgKiBUaGUgdmFsdWUgb2YgdGhlIGl0ZXJhYmxlIGV4cHJlc3Npb24sIHdoaWNoIGNhbiBiZSB1c2VkIGFzIGFcbiAgICogW3RlbXBsYXRlIGlucHV0IHZhcmlhYmxlXShndWlkZS9zdHJ1Y3R1cmFsLWRpcmVjdGl2ZXMjdGVtcGxhdGUtaW5wdXQtdmFyaWFibGUpLlxuICAgKi9cbiAgQElucHV0KClcbiAgc2V0IG5nRm9yT2YobmdGb3JPZjogTmdJdGVyYWJsZTxUPikge1xuICAgIHRoaXMuX25nRm9yT2YgPSBuZ0Zvck9mO1xuICAgIHRoaXMuX25nRm9yT2ZEaXJ0eSA9IHRydWU7XG4gIH1cbiAgLyoqXG4gICAqIEEgZnVuY3Rpb24gdGhhdCBkZWZpbmVzIGhvdyB0byB0cmFjayBjaGFuZ2VzIGZvciBpdGVtcyBpbiB0aGUgaXRlcmFibGUuXG4gICAqXG4gICAqIFdoZW4gaXRlbXMgYXJlIGFkZGVkLCBtb3ZlZCwgb3IgcmVtb3ZlZCBpbiB0aGUgaXRlcmFibGUsXG4gICAqIHRoZSBkaXJlY3RpdmUgbXVzdCByZS1yZW5kZXIgdGhlIGFwcHJvcHJpYXRlIERPTSBub2Rlcy5cbiAgICogVG8gbWluaW1pemUgY2h1cm4gaW4gdGhlIERPTSwgb25seSBub2RlcyB0aGF0IGhhdmUgY2hhbmdlZFxuICAgKiBhcmUgcmUtcmVuZGVyZWQuXG4gICAqXG4gICAqIEJ5IGRlZmF1bHQsIHRoZSBjaGFuZ2UgZGV0ZWN0b3IgYXNzdW1lcyB0aGF0XG4gICAqIHRoZSBvYmplY3QgaW5zdGFuY2UgaWRlbnRpZmllcyB0aGUgbm9kZSBpbiB0aGUgaXRlcmFibGUuXG4gICAqIFdoZW4gdGhpcyBmdW5jdGlvbiBpcyBzdXBwbGllZCwgdGhlIGRpcmVjdGl2ZSB1c2VzXG4gICAqIHRoZSByZXN1bHQgb2YgY2FsbGluZyB0aGlzIGZ1bmN0aW9uIHRvIGlkZW50aWZ5IHRoZSBpdGVtIG5vZGUsXG4gICAqIHJhdGhlciB0aGFuIHRoZSBpZGVudGl0eSBvZiB0aGUgb2JqZWN0IGl0c2VsZi5cbiAgICpcbiAgICogVGhlIGZ1bmN0aW9uIHJlY2VpdmVzIHR3byBpbnB1dHMsXG4gICAqIHRoZSBpdGVyYXRpb24gaW5kZXggYW5kIHRoZSBub2RlIG9iamVjdCBJRC5cbiAgICovXG4gIEBJbnB1dCgpXG4gIHNldCBuZ0ZvclRyYWNrQnkoZm46IFRyYWNrQnlGdW5jdGlvbjxUPikge1xuICAgIGlmIChpc0Rldk1vZGUoKSAmJiBmbiAhPSBudWxsICYmIHR5cGVvZiBmbiAhPT0gJ2Z1bmN0aW9uJykge1xuICAgICAgLy8gVE9ETyh2aWNiKTogdXNlIGEgbG9nIHNlcnZpY2Ugb25jZSB0aGVyZSBpcyBhIHB1YmxpYyBvbmUgYXZhaWxhYmxlXG4gICAgICBpZiAoPGFueT5jb25zb2xlICYmIDxhbnk+Y29uc29sZS53YXJuKSB7XG4gICAgICAgIGNvbnNvbGUud2FybihcbiAgICAgICAgICAgIGB0cmFja0J5IG11c3QgYmUgYSBmdW5jdGlvbiwgYnV0IHJlY2VpdmVkICR7SlNPTi5zdHJpbmdpZnkoZm4pfS4gYCArXG4gICAgICAgICAgICBgU2VlIGh0dHBzOi8vYW5ndWxhci5pby9kb2NzL3RzL2xhdGVzdC9hcGkvY29tbW9uL2luZGV4L05nRm9yLWRpcmVjdGl2ZS5odG1sIyEjY2hhbmdlLXByb3BhZ2F0aW9uIGZvciBtb3JlIGluZm9ybWF0aW9uLmApO1xuICAgICAgfVxuICAgIH1cbiAgICB0aGlzLl90cmFja0J5Rm4gPSBmbjtcbiAgfVxuXG4gIGdldCBuZ0ZvclRyYWNrQnkoKTogVHJhY2tCeUZ1bmN0aW9uPFQ+IHsgcmV0dXJuIHRoaXMuX3RyYWNrQnlGbjsgfVxuXG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9uZ0Zvck9mICE6IE5nSXRlcmFibGU8VD47XG4gIHByaXZhdGUgX25nRm9yT2ZEaXJ0eTogYm9vbGVhbiA9IHRydWU7XG4gIHByaXZhdGUgX2RpZmZlcjogSXRlcmFibGVEaWZmZXI8VD58bnVsbCA9IG51bGw7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF90cmFja0J5Rm4gITogVHJhY2tCeUZ1bmN0aW9uPFQ+O1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHJpdmF0ZSBfdmlld0NvbnRhaW5lcjogVmlld0NvbnRhaW5lclJlZiwgcHJpdmF0ZSBfdGVtcGxhdGU6IFRlbXBsYXRlUmVmPE5nRm9yT2ZDb250ZXh0PFQ+PixcbiAgICAgIHByaXZhdGUgX2RpZmZlcnM6IEl0ZXJhYmxlRGlmZmVycykge31cblxuICAvKipcbiAgICogQSByZWZlcmVuY2UgdG8gdGhlIHRlbXBsYXRlIHRoYXQgaXMgc3RhbXBlZCBvdXQgZm9yIGVhY2ggaXRlbSBpbiB0aGUgaXRlcmFibGUuXG4gICAqIEBzZWUgW3RlbXBsYXRlIHJlZmVyZW5jZSB2YXJpYWJsZV0oZ3VpZGUvdGVtcGxhdGUtc3ludGF4I3RlbXBsYXRlLXJlZmVyZW5jZS12YXJpYWJsZXMtLXZhci0pXG4gICAqL1xuICBASW5wdXQoKVxuICBzZXQgbmdGb3JUZW1wbGF0ZSh2YWx1ZTogVGVtcGxhdGVSZWY8TmdGb3JPZkNvbnRleHQ8VD4+KSB7XG4gICAgLy8gVE9ETyhUUzIuMSk6IG1ha2UgVGVtcGxhdGVSZWY8UGFydGlhbDxOZ0ZvclJvd09mPFQ+Pj4gb25jZSB3ZSBtb3ZlIHRvIFRTIHYyLjFcbiAgICAvLyBUaGUgY3VycmVudCB0eXBlIGlzIHRvbyByZXN0cmljdGl2ZTsgYSB0ZW1wbGF0ZSB0aGF0IGp1c3QgdXNlcyBpbmRleCwgZm9yIGV4YW1wbGUsXG4gICAgLy8gc2hvdWxkIGJlIGFjY2VwdGFibGUuXG4gICAgaWYgKHZhbHVlKSB7XG4gICAgICB0aGlzLl90ZW1wbGF0ZSA9IHZhbHVlO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBBcHBsaWVzIHRoZSBjaGFuZ2VzIHdoZW4gbmVlZGVkLlxuICAgKi9cbiAgbmdEb0NoZWNrKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLl9uZ0Zvck9mRGlydHkpIHtcbiAgICAgIHRoaXMuX25nRm9yT2ZEaXJ0eSA9IGZhbHNlO1xuICAgICAgLy8gUmVhY3Qgb24gbmdGb3JPZiBjaGFuZ2VzIG9ubHkgb25jZSBhbGwgaW5wdXRzIGhhdmUgYmVlbiBpbml0aWFsaXplZFxuICAgICAgY29uc3QgdmFsdWUgPSB0aGlzLl9uZ0Zvck9mO1xuICAgICAgaWYgKCF0aGlzLl9kaWZmZXIgJiYgdmFsdWUpIHtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICB0aGlzLl9kaWZmZXIgPSB0aGlzLl9kaWZmZXJzLmZpbmQodmFsdWUpLmNyZWF0ZSh0aGlzLm5nRm9yVHJhY2tCeSk7XG4gICAgICAgIH0gY2F0Y2gge1xuICAgICAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICAgICAgYENhbm5vdCBmaW5kIGEgZGlmZmVyIHN1cHBvcnRpbmcgb2JqZWN0ICcke3ZhbHVlfScgb2YgdHlwZSAnJHtnZXRUeXBlTmFtZSh2YWx1ZSl9Jy4gTmdGb3Igb25seSBzdXBwb3J0cyBiaW5kaW5nIHRvIEl0ZXJhYmxlcyBzdWNoIGFzIEFycmF5cy5gKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgICBpZiAodGhpcy5fZGlmZmVyKSB7XG4gICAgICBjb25zdCBjaGFuZ2VzID0gdGhpcy5fZGlmZmVyLmRpZmYodGhpcy5fbmdGb3JPZik7XG4gICAgICBpZiAoY2hhbmdlcykgdGhpcy5fYXBwbHlDaGFuZ2VzKGNoYW5nZXMpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgX2FwcGx5Q2hhbmdlcyhjaGFuZ2VzOiBJdGVyYWJsZUNoYW5nZXM8VD4pIHtcbiAgICBjb25zdCBpbnNlcnRUdXBsZXM6IFJlY29yZFZpZXdUdXBsZTxUPltdID0gW107XG4gICAgY2hhbmdlcy5mb3JFYWNoT3BlcmF0aW9uKFxuICAgICAgICAoaXRlbTogSXRlcmFibGVDaGFuZ2VSZWNvcmQ8YW55PiwgYWRqdXN0ZWRQcmV2aW91c0luZGV4OiBudW1iZXIgfCBudWxsLFxuICAgICAgICAgY3VycmVudEluZGV4OiBudW1iZXIgfCBudWxsKSA9PiB7XG4gICAgICAgICAgaWYgKGl0ZW0ucHJldmlvdXNJbmRleCA9PSBudWxsKSB7XG4gICAgICAgICAgICBjb25zdCB2aWV3ID0gdGhpcy5fdmlld0NvbnRhaW5lci5jcmVhdGVFbWJlZGRlZFZpZXcoXG4gICAgICAgICAgICAgICAgdGhpcy5fdGVtcGxhdGUsIG5ldyBOZ0Zvck9mQ29udGV4dDxUPihudWxsICEsIHRoaXMuX25nRm9yT2YsIC0xLCAtMSksXG4gICAgICAgICAgICAgICAgY3VycmVudEluZGV4ID09PSBudWxsID8gdW5kZWZpbmVkIDogY3VycmVudEluZGV4KTtcbiAgICAgICAgICAgIGNvbnN0IHR1cGxlID0gbmV3IFJlY29yZFZpZXdUdXBsZTxUPihpdGVtLCB2aWV3KTtcbiAgICAgICAgICAgIGluc2VydFR1cGxlcy5wdXNoKHR1cGxlKTtcbiAgICAgICAgICB9IGVsc2UgaWYgKGN1cnJlbnRJbmRleCA9PSBudWxsKSB7XG4gICAgICAgICAgICB0aGlzLl92aWV3Q29udGFpbmVyLnJlbW92ZShcbiAgICAgICAgICAgICAgICBhZGp1c3RlZFByZXZpb3VzSW5kZXggPT09IG51bGwgPyB1bmRlZmluZWQgOiBhZGp1c3RlZFByZXZpb3VzSW5kZXgpO1xuICAgICAgICAgIH0gZWxzZSBpZiAoYWRqdXN0ZWRQcmV2aW91c0luZGV4ICE9PSBudWxsKSB7XG4gICAgICAgICAgICBjb25zdCB2aWV3ID0gdGhpcy5fdmlld0NvbnRhaW5lci5nZXQoYWRqdXN0ZWRQcmV2aW91c0luZGV4KSAhO1xuICAgICAgICAgICAgdGhpcy5fdmlld0NvbnRhaW5lci5tb3ZlKHZpZXcsIGN1cnJlbnRJbmRleCk7XG4gICAgICAgICAgICBjb25zdCB0dXBsZSA9IG5ldyBSZWNvcmRWaWV3VHVwbGUoaXRlbSwgPEVtYmVkZGVkVmlld1JlZjxOZ0Zvck9mQ29udGV4dDxUPj4+dmlldyk7XG4gICAgICAgICAgICBpbnNlcnRUdXBsZXMucHVzaCh0dXBsZSk7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgaW5zZXJ0VHVwbGVzLmxlbmd0aDsgaSsrKSB7XG4gICAgICB0aGlzLl9wZXJWaWV3Q2hhbmdlKGluc2VydFR1cGxlc1tpXS52aWV3LCBpbnNlcnRUdXBsZXNbaV0ucmVjb3JkKTtcbiAgICB9XG5cbiAgICBmb3IgKGxldCBpID0gMCwgaWxlbiA9IHRoaXMuX3ZpZXdDb250YWluZXIubGVuZ3RoOyBpIDwgaWxlbjsgaSsrKSB7XG4gICAgICBjb25zdCB2aWV3UmVmID0gPEVtYmVkZGVkVmlld1JlZjxOZ0Zvck9mQ29udGV4dDxUPj4+dGhpcy5fdmlld0NvbnRhaW5lci5nZXQoaSk7XG4gICAgICB2aWV3UmVmLmNvbnRleHQuaW5kZXggPSBpO1xuICAgICAgdmlld1JlZi5jb250ZXh0LmNvdW50ID0gaWxlbjtcbiAgICAgIHZpZXdSZWYuY29udGV4dC5uZ0Zvck9mID0gdGhpcy5fbmdGb3JPZjtcbiAgICB9XG5cbiAgICBjaGFuZ2VzLmZvckVhY2hJZGVudGl0eUNoYW5nZSgocmVjb3JkOiBhbnkpID0+IHtcbiAgICAgIGNvbnN0IHZpZXdSZWYgPVxuICAgICAgICAgIDxFbWJlZGRlZFZpZXdSZWY8TmdGb3JPZkNvbnRleHQ8VD4+PnRoaXMuX3ZpZXdDb250YWluZXIuZ2V0KHJlY29yZC5jdXJyZW50SW5kZXgpO1xuICAgICAgdmlld1JlZi5jb250ZXh0LiRpbXBsaWNpdCA9IHJlY29yZC5pdGVtO1xuICAgIH0pO1xuICB9XG5cbiAgcHJpdmF0ZSBfcGVyVmlld0NoYW5nZShcbiAgICAgIHZpZXc6IEVtYmVkZGVkVmlld1JlZjxOZ0Zvck9mQ29udGV4dDxUPj4sIHJlY29yZDogSXRlcmFibGVDaGFuZ2VSZWNvcmQ8YW55Pikge1xuICAgIHZpZXcuY29udGV4dC4kaW1wbGljaXQgPSByZWNvcmQuaXRlbTtcbiAgfVxuXG4gIC8qKlxuICAgKiBBc3NlcnRzIHRoZSBjb3JyZWN0IHR5cGUgb2YgdGhlIGNvbnRleHQgZm9yIHRoZSB0ZW1wbGF0ZSB0aGF0IGBOZ0Zvck9mYCB3aWxsIHJlbmRlci5cbiAgICpcbiAgICogVGhlIHByZXNlbmNlIG9mIHRoaXMgbWV0aG9kIGlzIGEgc2lnbmFsIHRvIHRoZSBJdnkgdGVtcGxhdGUgdHlwZS1jaGVjayBjb21waWxlciB0aGF0IHRoZVxuICAgKiBgTmdGb3JPZmAgc3RydWN0dXJhbCBkaXJlY3RpdmUgcmVuZGVycyBpdHMgdGVtcGxhdGUgd2l0aCBhIHNwZWNpZmljIGNvbnRleHQgdHlwZS5cbiAgICovXG4gIHN0YXRpYyBuZ1RlbXBsYXRlQ29udGV4dEd1YXJkPFQ+KGRpcjogTmdGb3JPZjxUPiwgY3R4OiBhbnkpOiBjdHggaXMgTmdGb3JPZkNvbnRleHQ8VD4ge1xuICAgIHJldHVybiB0cnVlO1xuICB9XG59XG5cbmNsYXNzIFJlY29yZFZpZXdUdXBsZTxUPiB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyByZWNvcmQ6IGFueSwgcHVibGljIHZpZXc6IEVtYmVkZGVkVmlld1JlZjxOZ0Zvck9mQ29udGV4dDxUPj4pIHt9XG59XG5cbmZ1bmN0aW9uIGdldFR5cGVOYW1lKHR5cGU6IGFueSk6IHN0cmluZyB7XG4gIHJldHVybiB0eXBlWyduYW1lJ10gfHwgdHlwZW9mIHR5cGU7XG59XG4iXX0=
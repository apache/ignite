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
import { assertNotEqual } from '../../util/assert';
import { getLView, getSelectedIndex } from '../state';
import { NO_CHANGE } from '../tokens';
import { bind } from './property';
import { elementPropertyInternal, loadComponentRenderer } from './shared';
/**
 * Update a property on a host element. Only applies to native node properties, not inputs.
 *
 * Operates on the element selected by index via the {\@link select} instruction.
 *
 * \@codeGenApi
 * @template T
 * @param {?} propName Name of property. Because it is going to DOM, this is not subject to
 *        renaming as part of minification.
 * @param {?} value New value to write.
 * @param {?=} sanitizer An optional function used to sanitize the value.
 * @return {?} This function returns itself so that it may be chained
 * (e.g. `property('name', ctx.name)('title', ctx.title)`)
 *
 */
export function ɵɵhostProperty(propName, value, sanitizer) {
    /** @type {?} */
    const index = getSelectedIndex();
    ngDevMode && assertNotEqual(index, -1, 'selected index cannot be -1');
    /** @type {?} */
    const lView = getLView();
    /** @type {?} */
    const bindReconciledValue = bind(lView, value);
    if (bindReconciledValue !== NO_CHANGE) {
        elementPropertyInternal(index, propName, bindReconciledValue, sanitizer, true);
    }
    return ɵɵhostProperty;
}
/**
 * Updates a synthetic host binding (e.g. `[\@foo]`) on a component.
 *
 * This instruction is for compatibility purposes and is designed to ensure that a
 * synthetic host binding (e.g. `\@HostBinding('\@foo')`) properly gets rendered in
 * the component's renderer. Normally all host bindings are evaluated with the parent
 * component's renderer, but, in the case of animation \@triggers, they need to be
 * evaluated with the sub component's renderer (because that's where the animation
 * triggers are defined).
 *
 * Do not use this instruction as a replacement for `elementProperty`. This instruction
 * only exists to ensure compatibility with the ViewEngine's host binding behavior.
 *
 * \@codeGenApi
 * @template T
 * @param {?} propName Name of property. Because it is going to DOM, this is not subject to
 *        renaming as part of minification.
 * @param {?} value New value to write.
 * @param {?=} sanitizer An optional function used to sanitize the value.
 *
 * @return {?}
 */
export function ɵɵupdateSyntheticHostBinding(propName, value, sanitizer) {
    /** @type {?} */
    const index = getSelectedIndex();
    /** @type {?} */
    const lView = getLView();
    // TODO(benlesh): remove bind call here.
    /** @type {?} */
    const bound = bind(lView, value);
    if (bound !== NO_CHANGE) {
        elementPropertyInternal(index, propName, bound, sanitizer, true, loadComponentRenderer);
    }
    return ɵɵupdateSyntheticHostBinding;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaG9zdF9wcm9wZXJ0eS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3JlbmRlcjMvaW5zdHJ1Y3Rpb25zL2hvc3RfcHJvcGVydHkudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFPQSxPQUFPLEVBQUMsY0FBYyxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFFakQsT0FBTyxFQUFDLFFBQVEsRUFBRSxnQkFBZ0IsRUFBQyxNQUFNLFVBQVUsQ0FBQztBQUNwRCxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sV0FBVyxDQUFDO0FBQ3BDLE9BQU8sRUFBQyxJQUFJLEVBQUMsTUFBTSxZQUFZLENBQUM7QUFDaEMsT0FBTyxFQUFtQix1QkFBdUIsRUFBRSxxQkFBcUIsRUFBQyxNQUFNLFVBQVUsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7OztBQWdCMUYsTUFBTSxVQUFVLGNBQWMsQ0FDMUIsUUFBZ0IsRUFBRSxLQUFRLEVBQUUsU0FBOEI7O1VBQ3RELEtBQUssR0FBRyxnQkFBZ0IsRUFBRTtJQUNoQyxTQUFTLElBQUksY0FBYyxDQUFDLEtBQUssRUFBRSxDQUFDLENBQUMsRUFBRSw2QkFBNkIsQ0FBQyxDQUFDOztVQUNoRSxLQUFLLEdBQUcsUUFBUSxFQUFFOztVQUNsQixtQkFBbUIsR0FBRyxJQUFJLENBQUMsS0FBSyxFQUFFLEtBQUssQ0FBQztJQUM5QyxJQUFJLG1CQUFtQixLQUFLLFNBQVMsRUFBRTtRQUNyQyx1QkFBdUIsQ0FBQyxLQUFLLEVBQUUsUUFBUSxFQUFFLG1CQUFtQixFQUFFLFNBQVMsRUFBRSxJQUFJLENBQUMsQ0FBQztLQUNoRjtJQUNELE9BQU8sY0FBYyxDQUFDO0FBQ3hCLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBd0JELE1BQU0sVUFBVSw0QkFBNEIsQ0FDeEMsUUFBZ0IsRUFBRSxLQUFvQixFQUFFLFNBQThCOztVQUNsRSxLQUFLLEdBQUcsZ0JBQWdCLEVBQUU7O1VBQzFCLEtBQUssR0FBRyxRQUFRLEVBQUU7OztVQUVsQixLQUFLLEdBQUcsSUFBSSxDQUFDLEtBQUssRUFBRSxLQUFLLENBQUM7SUFDaEMsSUFBSSxLQUFLLEtBQUssU0FBUyxFQUFFO1FBQ3ZCLHVCQUF1QixDQUFDLEtBQUssRUFBRSxRQUFRLEVBQUUsS0FBSyxFQUFFLFNBQVMsRUFBRSxJQUFJLEVBQUUscUJBQXFCLENBQUMsQ0FBQztLQUN6RjtJQUNELE9BQU8sNEJBQTRCLENBQUM7QUFDdEMsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cbmltcG9ydCB7YXNzZXJ0Tm90RXF1YWx9IGZyb20gJy4uLy4uL3V0aWwvYXNzZXJ0JztcbmltcG9ydCB7U2FuaXRpemVyRm59IGZyb20gJy4uL2ludGVyZmFjZXMvc2FuaXRpemF0aW9uJztcbmltcG9ydCB7Z2V0TFZpZXcsIGdldFNlbGVjdGVkSW5kZXh9IGZyb20gJy4uL3N0YXRlJztcbmltcG9ydCB7Tk9fQ0hBTkdFfSBmcm9tICcuLi90b2tlbnMnO1xuaW1wb3J0IHtiaW5kfSBmcm9tICcuL3Byb3BlcnR5JztcbmltcG9ydCB7VHNpY2tsZUlzc3VlMTAwOSwgZWxlbWVudFByb3BlcnR5SW50ZXJuYWwsIGxvYWRDb21wb25lbnRSZW5kZXJlcn0gZnJvbSAnLi9zaGFyZWQnO1xuXG4vKipcbiAqIFVwZGF0ZSBhIHByb3BlcnR5IG9uIGEgaG9zdCBlbGVtZW50LiBPbmx5IGFwcGxpZXMgdG8gbmF0aXZlIG5vZGUgcHJvcGVydGllcywgbm90IGlucHV0cy5cbiAqXG4gKiBPcGVyYXRlcyBvbiB0aGUgZWxlbWVudCBzZWxlY3RlZCBieSBpbmRleCB2aWEgdGhlIHtAbGluayBzZWxlY3R9IGluc3RydWN0aW9uLlxuICpcbiAqIEBwYXJhbSBwcm9wTmFtZSBOYW1lIG9mIHByb3BlcnR5LiBCZWNhdXNlIGl0IGlzIGdvaW5nIHRvIERPTSwgdGhpcyBpcyBub3Qgc3ViamVjdCB0b1xuICogICAgICAgIHJlbmFtaW5nIGFzIHBhcnQgb2YgbWluaWZpY2F0aW9uLlxuICogQHBhcmFtIHZhbHVlIE5ldyB2YWx1ZSB0byB3cml0ZS5cbiAqIEBwYXJhbSBzYW5pdGl6ZXIgQW4gb3B0aW9uYWwgZnVuY3Rpb24gdXNlZCB0byBzYW5pdGl6ZSB0aGUgdmFsdWUuXG4gKiBAcmV0dXJucyBUaGlzIGZ1bmN0aW9uIHJldHVybnMgaXRzZWxmIHNvIHRoYXQgaXQgbWF5IGJlIGNoYWluZWRcbiAqIChlLmcuIGBwcm9wZXJ0eSgnbmFtZScsIGN0eC5uYW1lKSgndGl0bGUnLCBjdHgudGl0bGUpYClcbiAqXG4gKiBAY29kZUdlbkFwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gybXJtWhvc3RQcm9wZXJ0eTxUPihcbiAgICBwcm9wTmFtZTogc3RyaW5nLCB2YWx1ZTogVCwgc2FuaXRpemVyPzogU2FuaXRpemVyRm4gfCBudWxsKTogVHNpY2tsZUlzc3VlMTAwOSB7XG4gIGNvbnN0IGluZGV4ID0gZ2V0U2VsZWN0ZWRJbmRleCgpO1xuICBuZ0Rldk1vZGUgJiYgYXNzZXJ0Tm90RXF1YWwoaW5kZXgsIC0xLCAnc2VsZWN0ZWQgaW5kZXggY2Fubm90IGJlIC0xJyk7XG4gIGNvbnN0IGxWaWV3ID0gZ2V0TFZpZXcoKTtcbiAgY29uc3QgYmluZFJlY29uY2lsZWRWYWx1ZSA9IGJpbmQobFZpZXcsIHZhbHVlKTtcbiAgaWYgKGJpbmRSZWNvbmNpbGVkVmFsdWUgIT09IE5PX0NIQU5HRSkge1xuICAgIGVsZW1lbnRQcm9wZXJ0eUludGVybmFsKGluZGV4LCBwcm9wTmFtZSwgYmluZFJlY29uY2lsZWRWYWx1ZSwgc2FuaXRpemVyLCB0cnVlKTtcbiAgfVxuICByZXR1cm4gybXJtWhvc3RQcm9wZXJ0eTtcbn1cblxuXG4vKipcbiAqIFVwZGF0ZXMgYSBzeW50aGV0aWMgaG9zdCBiaW5kaW5nIChlLmcuIGBbQGZvb11gKSBvbiBhIGNvbXBvbmVudC5cbiAqXG4gKiBUaGlzIGluc3RydWN0aW9uIGlzIGZvciBjb21wYXRpYmlsaXR5IHB1cnBvc2VzIGFuZCBpcyBkZXNpZ25lZCB0byBlbnN1cmUgdGhhdCBhXG4gKiBzeW50aGV0aWMgaG9zdCBiaW5kaW5nIChlLmcuIGBASG9zdEJpbmRpbmcoJ0Bmb28nKWApIHByb3Blcmx5IGdldHMgcmVuZGVyZWQgaW5cbiAqIHRoZSBjb21wb25lbnQncyByZW5kZXJlci4gTm9ybWFsbHkgYWxsIGhvc3QgYmluZGluZ3MgYXJlIGV2YWx1YXRlZCB3aXRoIHRoZSBwYXJlbnRcbiAqIGNvbXBvbmVudCdzIHJlbmRlcmVyLCBidXQsIGluIHRoZSBjYXNlIG9mIGFuaW1hdGlvbiBAdHJpZ2dlcnMsIHRoZXkgbmVlZCB0byBiZVxuICogZXZhbHVhdGVkIHdpdGggdGhlIHN1YiBjb21wb25lbnQncyByZW5kZXJlciAoYmVjYXVzZSB0aGF0J3Mgd2hlcmUgdGhlIGFuaW1hdGlvblxuICogdHJpZ2dlcnMgYXJlIGRlZmluZWQpLlxuICpcbiAqIERvIG5vdCB1c2UgdGhpcyBpbnN0cnVjdGlvbiBhcyBhIHJlcGxhY2VtZW50IGZvciBgZWxlbWVudFByb3BlcnR5YC4gVGhpcyBpbnN0cnVjdGlvblxuICogb25seSBleGlzdHMgdG8gZW5zdXJlIGNvbXBhdGliaWxpdHkgd2l0aCB0aGUgVmlld0VuZ2luZSdzIGhvc3QgYmluZGluZyBiZWhhdmlvci5cbiAqXG4gKiBAcGFyYW0gaW5kZXggVGhlIGluZGV4IG9mIHRoZSBlbGVtZW50IHRvIHVwZGF0ZSBpbiB0aGUgZGF0YSBhcnJheVxuICogQHBhcmFtIHByb3BOYW1lIE5hbWUgb2YgcHJvcGVydHkuIEJlY2F1c2UgaXQgaXMgZ29pbmcgdG8gRE9NLCB0aGlzIGlzIG5vdCBzdWJqZWN0IHRvXG4gKiAgICAgICAgcmVuYW1pbmcgYXMgcGFydCBvZiBtaW5pZmljYXRpb24uXG4gKiBAcGFyYW0gdmFsdWUgTmV3IHZhbHVlIHRvIHdyaXRlLlxuICogQHBhcmFtIHNhbml0aXplciBBbiBvcHRpb25hbCBmdW5jdGlvbiB1c2VkIHRvIHNhbml0aXplIHRoZSB2YWx1ZS5cbiAqXG4gKiBAY29kZUdlbkFwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gybXJtXVwZGF0ZVN5bnRoZXRpY0hvc3RCaW5kaW5nPFQ+KFxuICAgIHByb3BOYW1lOiBzdHJpbmcsIHZhbHVlOiBUIHwgTk9fQ0hBTkdFLCBzYW5pdGl6ZXI/OiBTYW5pdGl6ZXJGbiB8IG51bGwpOiBUc2lja2xlSXNzdWUxMDA5IHtcbiAgY29uc3QgaW5kZXggPSBnZXRTZWxlY3RlZEluZGV4KCk7XG4gIGNvbnN0IGxWaWV3ID0gZ2V0TFZpZXcoKTtcbiAgLy8gVE9ETyhiZW5sZXNoKTogcmVtb3ZlIGJpbmQgY2FsbCBoZXJlLlxuICBjb25zdCBib3VuZCA9IGJpbmQobFZpZXcsIHZhbHVlKTtcbiAgaWYgKGJvdW5kICE9PSBOT19DSEFOR0UpIHtcbiAgICBlbGVtZW50UHJvcGVydHlJbnRlcm5hbChpbmRleCwgcHJvcE5hbWUsIGJvdW5kLCBzYW5pdGl6ZXIsIHRydWUsIGxvYWRDb21wb25lbnRSZW5kZXJlcik7XG4gIH1cbiAgcmV0dXJuIMm1ybV1cGRhdGVTeW50aGV0aWNIb3N0QmluZGluZztcbn1cbiJdfQ==
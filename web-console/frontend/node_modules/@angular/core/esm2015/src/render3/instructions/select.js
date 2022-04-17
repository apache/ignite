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
import { assertGreaterThan, assertLessThan } from '../../util/assert';
import { executePreOrderHooks } from '../hooks';
import { HEADER_OFFSET, TVIEW } from '../interfaces/view';
import { getCheckNoChangesMode, getLView, setSelectedIndex } from '../state';
/**
 * Selects an element for later binding instructions.
 *
 * Used in conjunction with instructions like {\@link property} to act on elements with specified
 * indices, for example those created with {\@link element} or {\@link elementStart}.
 *
 * ```ts
 * (rf: RenderFlags, ctx: any) => {
 *   if (rf & 1) {
 *     element(0, 'div');
 *   }
 *   if (rf & 2) {
 *     select(0); // Select the <div/> created above.
 *     property('title', 'test');
 *   }
 *  }
 * ```
 * \@codeGenApi
 * @param {?} index the index of the item to act on with the following instructions
 *
 * @return {?}
 */
export function ɵɵselect(index) {
    ngDevMode && assertGreaterThan(index, -1, 'Invalid index');
    ngDevMode &&
        assertLessThan(index, getLView().length - HEADER_OFFSET, 'Should be within range for the view data');
    /** @type {?} */
    const lView = getLView();
    selectInternal(lView, index);
}
/**
 * @param {?} lView
 * @param {?} index
 * @return {?}
 */
export function selectInternal(lView, index) {
    // Flush the initial hooks for elements in the view that have been added up to this point.
    executePreOrderHooks(lView, lView[TVIEW], getCheckNoChangesMode(), index);
    // We must set the selected index *after* running the hooks, because hooks may have side-effects
    // that cause other template functions to run, thus updating the selected index, which is global
    // state. If we run `setSelectedIndex` *before* we run the hooks, in some cases the selected index
    // will be altered by the time we leave the `ɵɵselect` instruction.
    setSelectedIndex(index);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2VsZWN0LmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvcmVuZGVyMy9pbnN0cnVjdGlvbnMvc2VsZWN0LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBT0EsT0FBTyxFQUFDLGlCQUFpQixFQUFFLGNBQWMsRUFBQyxNQUFNLG1CQUFtQixDQUFDO0FBQ3BFLE9BQU8sRUFBQyxvQkFBb0IsRUFBQyxNQUFNLFVBQVUsQ0FBQztBQUM5QyxPQUFPLEVBQUMsYUFBYSxFQUFTLEtBQUssRUFBQyxNQUFNLG9CQUFvQixDQUFDO0FBQy9ELE9BQU8sRUFBQyxxQkFBcUIsRUFBRSxRQUFRLEVBQUUsZ0JBQWdCLEVBQUMsTUFBTSxVQUFVLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBd0IzRSxNQUFNLFVBQVUsUUFBUSxDQUFDLEtBQWE7SUFDcEMsU0FBUyxJQUFJLGlCQUFpQixDQUFDLEtBQUssRUFBRSxDQUFDLENBQUMsRUFBRSxlQUFlLENBQUMsQ0FBQztJQUMzRCxTQUFTO1FBQ0wsY0FBYyxDQUNWLEtBQUssRUFBRSxRQUFRLEVBQUUsQ0FBQyxNQUFNLEdBQUcsYUFBYSxFQUFFLDBDQUEwQyxDQUFDLENBQUM7O1VBQ3hGLEtBQUssR0FBRyxRQUFRLEVBQUU7SUFDeEIsY0FBYyxDQUFDLEtBQUssRUFBRSxLQUFLLENBQUMsQ0FBQztBQUMvQixDQUFDOzs7Ozs7QUFHRCxNQUFNLFVBQVUsY0FBYyxDQUFDLEtBQVksRUFBRSxLQUFhO0lBQ3hELDBGQUEwRjtJQUMxRixvQkFBb0IsQ0FBQyxLQUFLLEVBQUUsS0FBSyxDQUFDLEtBQUssQ0FBQyxFQUFFLHFCQUFxQixFQUFFLEVBQUUsS0FBSyxDQUFDLENBQUM7SUFFMUUsZ0dBQWdHO0lBQ2hHLGdHQUFnRztJQUNoRyxrR0FBa0c7SUFDbEcsbUVBQW1FO0lBQ25FLGdCQUFnQixDQUFDLEtBQUssQ0FBQyxDQUFDO0FBQzFCLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5pbXBvcnQge2Fzc2VydEdyZWF0ZXJUaGFuLCBhc3NlcnRMZXNzVGhhbn0gZnJvbSAnLi4vLi4vdXRpbC9hc3NlcnQnO1xuaW1wb3J0IHtleGVjdXRlUHJlT3JkZXJIb29rc30gZnJvbSAnLi4vaG9va3MnO1xuaW1wb3J0IHtIRUFERVJfT0ZGU0VULCBMVmlldywgVFZJRVd9IGZyb20gJy4uL2ludGVyZmFjZXMvdmlldyc7XG5pbXBvcnQge2dldENoZWNrTm9DaGFuZ2VzTW9kZSwgZ2V0TFZpZXcsIHNldFNlbGVjdGVkSW5kZXh9IGZyb20gJy4uL3N0YXRlJztcblxuXG4vKipcbiAqIFNlbGVjdHMgYW4gZWxlbWVudCBmb3IgbGF0ZXIgYmluZGluZyBpbnN0cnVjdGlvbnMuXG4gKlxuICogVXNlZCBpbiBjb25qdW5jdGlvbiB3aXRoIGluc3RydWN0aW9ucyBsaWtlIHtAbGluayBwcm9wZXJ0eX0gdG8gYWN0IG9uIGVsZW1lbnRzIHdpdGggc3BlY2lmaWVkXG4gKiBpbmRpY2VzLCBmb3IgZXhhbXBsZSB0aG9zZSBjcmVhdGVkIHdpdGgge0BsaW5rIGVsZW1lbnR9IG9yIHtAbGluayBlbGVtZW50U3RhcnR9LlxuICpcbiAqIGBgYHRzXG4gKiAocmY6IFJlbmRlckZsYWdzLCBjdHg6IGFueSkgPT4ge1xuICogICBpZiAocmYgJiAxKSB7XG4gKiAgICAgZWxlbWVudCgwLCAnZGl2Jyk7XG4gKiAgIH1cbiAqICAgaWYgKHJmICYgMikge1xuICogICAgIHNlbGVjdCgwKTsgLy8gU2VsZWN0IHRoZSA8ZGl2Lz4gY3JlYXRlZCBhYm92ZS5cbiAqICAgICBwcm9wZXJ0eSgndGl0bGUnLCAndGVzdCcpO1xuICogICB9XG4gKiAgfVxuICogYGBgXG4gKiBAcGFyYW0gaW5kZXggdGhlIGluZGV4IG9mIHRoZSBpdGVtIHRvIGFjdCBvbiB3aXRoIHRoZSBmb2xsb3dpbmcgaW5zdHJ1Y3Rpb25zXG4gKlxuICogQGNvZGVHZW5BcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIMm1ybVzZWxlY3QoaW5kZXg6IG51bWJlcik6IHZvaWQge1xuICBuZ0Rldk1vZGUgJiYgYXNzZXJ0R3JlYXRlclRoYW4oaW5kZXgsIC0xLCAnSW52YWxpZCBpbmRleCcpO1xuICBuZ0Rldk1vZGUgJiZcbiAgICAgIGFzc2VydExlc3NUaGFuKFxuICAgICAgICAgIGluZGV4LCBnZXRMVmlldygpLmxlbmd0aCAtIEhFQURFUl9PRkZTRVQsICdTaG91bGQgYmUgd2l0aGluIHJhbmdlIGZvciB0aGUgdmlldyBkYXRhJyk7XG4gIGNvbnN0IGxWaWV3ID0gZ2V0TFZpZXcoKTtcbiAgc2VsZWN0SW50ZXJuYWwobFZpZXcsIGluZGV4KTtcbn1cblxuXG5leHBvcnQgZnVuY3Rpb24gc2VsZWN0SW50ZXJuYWwobFZpZXc6IExWaWV3LCBpbmRleDogbnVtYmVyKSB7XG4gIC8vIEZsdXNoIHRoZSBpbml0aWFsIGhvb2tzIGZvciBlbGVtZW50cyBpbiB0aGUgdmlldyB0aGF0IGhhdmUgYmVlbiBhZGRlZCB1cCB0byB0aGlzIHBvaW50LlxuICBleGVjdXRlUHJlT3JkZXJIb29rcyhsVmlldywgbFZpZXdbVFZJRVddLCBnZXRDaGVja05vQ2hhbmdlc01vZGUoKSwgaW5kZXgpO1xuXG4gIC8vIFdlIG11c3Qgc2V0IHRoZSBzZWxlY3RlZCBpbmRleCAqYWZ0ZXIqIHJ1bm5pbmcgdGhlIGhvb2tzLCBiZWNhdXNlIGhvb2tzIG1heSBoYXZlIHNpZGUtZWZmZWN0c1xuICAvLyB0aGF0IGNhdXNlIG90aGVyIHRlbXBsYXRlIGZ1bmN0aW9ucyB0byBydW4sIHRodXMgdXBkYXRpbmcgdGhlIHNlbGVjdGVkIGluZGV4LCB3aGljaCBpcyBnbG9iYWxcbiAgLy8gc3RhdGUuIElmIHdlIHJ1biBgc2V0U2VsZWN0ZWRJbmRleGAgKmJlZm9yZSogd2UgcnVuIHRoZSBob29rcywgaW4gc29tZSBjYXNlcyB0aGUgc2VsZWN0ZWQgaW5kZXhcbiAgLy8gd2lsbCBiZSBhbHRlcmVkIGJ5IHRoZSB0aW1lIHdlIGxlYXZlIHRoZSBgybXJtXNlbGVjdGAgaW5zdHJ1Y3Rpb24uXG4gIHNldFNlbGVjdGVkSW5kZXgoaW5kZXgpO1xufVxuIl19
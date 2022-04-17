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
/**
 * --------
 *
 * // TODO(matsko): add updateMask info
 *
 * This file contains all state-based logic for styling in Angular.
 *
 * Styling in Angular is evaluated with a series of styling-specific
 * template instructions which are called one after another each time
 * change detection occurs in Angular.
 *
 * Styling makes use of various temporary, state-based variables between
 * instructions so that it can better cache and optimize its values.
 * These values are usually populated and cleared when an element is
 * exited in change detection (once all the instructions are run for
 * that element).
 *
 * There are, however, situations where the state-based values
 * need to be stored and used at a later point. This ONLY occurs when
 * there are template-level as well as host-binding-level styling
 * instructions on the same element. The example below shows exactly
 * what could be:
 *
 * ```html
 * <!-- two sources of styling: the template and the directive -->
 * <div [style.width]="width" dir-that-sets-height></div>
 * ```
 *
 * If and when this situation occurs, the current styling state is
 * stored in a storage map value and then later accessed once the
 * host bindings are evaluated. Once styling for the current element
 * is over then the map entry will be cleared.
 *
 * To learn more about the algorithm see `TStylingContext`.
 *
 * --------
 */
/** @type {?} */
let _stylingState = null;
/** @type {?} */
const _stateStorage = new Map();
// this value is not used outside this file and is only here
// as a caching check for when the element changes.
/** @type {?} */
let _stylingElement = null;
/**
 * Used as a state reference for update values between style/class binding instructions.
 * @record
 */
export function StylingState() { }
if (false) {
    /** @type {?} */
    StylingState.prototype.classesBitMask;
    /** @type {?} */
    StylingState.prototype.classesIndex;
    /** @type {?} */
    StylingState.prototype.stylesBitMask;
    /** @type {?} */
    StylingState.prototype.stylesIndex;
}
/** @type {?} */
export const STYLING_INDEX_START_VALUE = 1;
/** @type {?} */
export const BIT_MASK_START_VALUE = 0;
/**
 * @param {?} element
 * @param {?=} readFromMap
 * @return {?}
 */
export function getStylingState(element, readFromMap) {
    if (!_stylingElement || element !== _stylingElement) {
        _stylingElement = element;
        if (readFromMap) {
            _stylingState = _stateStorage.get(element) || null;
            ngDevMode && ngDevMode.stylingReadPersistedState++;
        }
        _stylingState = _stylingState || {
            classesBitMask: BIT_MASK_START_VALUE,
            classesIndex: STYLING_INDEX_START_VALUE,
            stylesBitMask: BIT_MASK_START_VALUE,
            stylesIndex: STYLING_INDEX_START_VALUE,
        };
    }
    return (/** @type {?} */ (_stylingState));
}
/**
 * @return {?}
 */
export function resetStylingState() {
    _stylingState = null;
    _stylingElement = null;
}
/**
 * @param {?} element
 * @param {?} state
 * @return {?}
 */
export function storeStylingState(element, state) {
    ngDevMode && ngDevMode.stylingWritePersistedState++;
    _stateStorage.set(element, state);
}
/**
 * @param {?} element
 * @return {?}
 */
export function deleteStylingStateFromStorage(element) {
    _stateStorage.delete(element);
}
/**
 * @return {?}
 */
export function resetAllStylingState() {
    resetStylingState();
    _stateStorage.clear();
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3RhdGUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL3N0eWxpbmdfbmV4dC9zdGF0ZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0lBOENJLGFBQWEsR0FBc0IsSUFBSTs7TUFDckMsYUFBYSxHQUFHLElBQUksR0FBRyxFQUFxQjs7OztJQUk5QyxlQUFlLEdBQVEsSUFBSTs7Ozs7QUFLL0Isa0NBS0M7OztJQUpDLHNDQUF1Qjs7SUFDdkIsb0NBQXFCOztJQUNyQixxQ0FBc0I7O0lBQ3RCLG1DQUFvQjs7O0FBR3RCLE1BQU0sT0FBTyx5QkFBeUIsR0FBRyxDQUFDOztBQUMxQyxNQUFNLE9BQU8sb0JBQW9CLEdBQUcsQ0FBQzs7Ozs7O0FBRXJDLE1BQU0sVUFBVSxlQUFlLENBQUMsT0FBWSxFQUFFLFdBQXFCO0lBQ2pFLElBQUksQ0FBQyxlQUFlLElBQUksT0FBTyxLQUFLLGVBQWUsRUFBRTtRQUNuRCxlQUFlLEdBQUcsT0FBTyxDQUFDO1FBQzFCLElBQUksV0FBVyxFQUFFO1lBQ2YsYUFBYSxHQUFHLGFBQWEsQ0FBQyxHQUFHLENBQUMsT0FBTyxDQUFDLElBQUksSUFBSSxDQUFDO1lBQ25ELFNBQVMsSUFBSSxTQUFTLENBQUMseUJBQXlCLEVBQUUsQ0FBQztTQUNwRDtRQUNELGFBQWEsR0FBRyxhQUFhLElBQUk7WUFDL0IsY0FBYyxFQUFFLG9CQUFvQjtZQUNwQyxZQUFZLEVBQUUseUJBQXlCO1lBQ3ZDLGFBQWEsRUFBRSxvQkFBb0I7WUFDbkMsV0FBVyxFQUFFLHlCQUF5QjtTQUN2QyxDQUFDO0tBQ0g7SUFDRCxPQUFPLG1CQUFBLGFBQWEsRUFBRSxDQUFDO0FBQ3pCLENBQUM7Ozs7QUFFRCxNQUFNLFVBQVUsaUJBQWlCO0lBQy9CLGFBQWEsR0FBRyxJQUFJLENBQUM7SUFDckIsZUFBZSxHQUFHLElBQUksQ0FBQztBQUN6QixDQUFDOzs7Ozs7QUFFRCxNQUFNLFVBQVUsaUJBQWlCLENBQUMsT0FBWSxFQUFFLEtBQW1CO0lBQ2pFLFNBQVMsSUFBSSxTQUFTLENBQUMsMEJBQTBCLEVBQUUsQ0FBQztJQUNwRCxhQUFhLENBQUMsR0FBRyxDQUFDLE9BQU8sRUFBRSxLQUFLLENBQUMsQ0FBQztBQUNwQyxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSw2QkFBNkIsQ0FBQyxPQUFZO0lBQ3hELGFBQWEsQ0FBQyxNQUFNLENBQUMsT0FBTyxDQUFDLENBQUM7QUFDaEMsQ0FBQzs7OztBQUVELE1BQU0sVUFBVSxvQkFBb0I7SUFDbEMsaUJBQWlCLEVBQUUsQ0FBQztJQUNwQixhQUFhLENBQUMsS0FBSyxFQUFFLENBQUM7QUFDeEIsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuKiBAbGljZW5zZVxuKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbipcbiogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuKi9cblxuLyoqXG4gKiAtLS0tLS0tLVxuICpcbiAqIC8vIFRPRE8obWF0c2tvKTogYWRkIHVwZGF0ZU1hc2sgaW5mb1xuICpcbiAqIFRoaXMgZmlsZSBjb250YWlucyBhbGwgc3RhdGUtYmFzZWQgbG9naWMgZm9yIHN0eWxpbmcgaW4gQW5ndWxhci5cbiAqXG4gKiBTdHlsaW5nIGluIEFuZ3VsYXIgaXMgZXZhbHVhdGVkIHdpdGggYSBzZXJpZXMgb2Ygc3R5bGluZy1zcGVjaWZpY1xuICogdGVtcGxhdGUgaW5zdHJ1Y3Rpb25zIHdoaWNoIGFyZSBjYWxsZWQgb25lIGFmdGVyIGFub3RoZXIgZWFjaCB0aW1lXG4gKiBjaGFuZ2UgZGV0ZWN0aW9uIG9jY3VycyBpbiBBbmd1bGFyLlxuICpcbiAqIFN0eWxpbmcgbWFrZXMgdXNlIG9mIHZhcmlvdXMgdGVtcG9yYXJ5LCBzdGF0ZS1iYXNlZCB2YXJpYWJsZXMgYmV0d2VlblxuICogaW5zdHJ1Y3Rpb25zIHNvIHRoYXQgaXQgY2FuIGJldHRlciBjYWNoZSBhbmQgb3B0aW1pemUgaXRzIHZhbHVlcy5cbiAqIFRoZXNlIHZhbHVlcyBhcmUgdXN1YWxseSBwb3B1bGF0ZWQgYW5kIGNsZWFyZWQgd2hlbiBhbiBlbGVtZW50IGlzXG4gKiBleGl0ZWQgaW4gY2hhbmdlIGRldGVjdGlvbiAob25jZSBhbGwgdGhlIGluc3RydWN0aW9ucyBhcmUgcnVuIGZvclxuICogdGhhdCBlbGVtZW50KS5cbiAqXG4gKiBUaGVyZSBhcmUsIGhvd2V2ZXIsIHNpdHVhdGlvbnMgd2hlcmUgdGhlIHN0YXRlLWJhc2VkIHZhbHVlc1xuICogbmVlZCB0byBiZSBzdG9yZWQgYW5kIHVzZWQgYXQgYSBsYXRlciBwb2ludC4gVGhpcyBPTkxZIG9jY3VycyB3aGVuXG4gKiB0aGVyZSBhcmUgdGVtcGxhdGUtbGV2ZWwgYXMgd2VsbCBhcyBob3N0LWJpbmRpbmctbGV2ZWwgc3R5bGluZ1xuICogaW5zdHJ1Y3Rpb25zIG9uIHRoZSBzYW1lIGVsZW1lbnQuIFRoZSBleGFtcGxlIGJlbG93IHNob3dzIGV4YWN0bHlcbiAqIHdoYXQgY291bGQgYmU6XG4gKlxuICogYGBgaHRtbFxuICogPCEtLSB0d28gc291cmNlcyBvZiBzdHlsaW5nOiB0aGUgdGVtcGxhdGUgYW5kIHRoZSBkaXJlY3RpdmUgLS0+XG4gKiA8ZGl2IFtzdHlsZS53aWR0aF09XCJ3aWR0aFwiIGRpci10aGF0LXNldHMtaGVpZ2h0PjwvZGl2PlxuICogYGBgXG4gKlxuICogSWYgYW5kIHdoZW4gdGhpcyBzaXR1YXRpb24gb2NjdXJzLCB0aGUgY3VycmVudCBzdHlsaW5nIHN0YXRlIGlzXG4gKiBzdG9yZWQgaW4gYSBzdG9yYWdlIG1hcCB2YWx1ZSBhbmQgdGhlbiBsYXRlciBhY2Nlc3NlZCBvbmNlIHRoZVxuICogaG9zdCBiaW5kaW5ncyBhcmUgZXZhbHVhdGVkLiBPbmNlIHN0eWxpbmcgZm9yIHRoZSBjdXJyZW50IGVsZW1lbnRcbiAqIGlzIG92ZXIgdGhlbiB0aGUgbWFwIGVudHJ5IHdpbGwgYmUgY2xlYXJlZC5cbiAqXG4gKiBUbyBsZWFybiBtb3JlIGFib3V0IHRoZSBhbGdvcml0aG0gc2VlIGBUU3R5bGluZ0NvbnRleHRgLlxuICpcbiAqIC0tLS0tLS0tXG4gKi9cblxubGV0IF9zdHlsaW5nU3RhdGU6IFN0eWxpbmdTdGF0ZXxudWxsID0gbnVsbDtcbmNvbnN0IF9zdGF0ZVN0b3JhZ2UgPSBuZXcgTWFwPGFueSwgU3R5bGluZ1N0YXRlPigpO1xuXG4vLyB0aGlzIHZhbHVlIGlzIG5vdCB1c2VkIG91dHNpZGUgdGhpcyBmaWxlIGFuZCBpcyBvbmx5IGhlcmVcbi8vIGFzIGEgY2FjaGluZyBjaGVjayBmb3Igd2hlbiB0aGUgZWxlbWVudCBjaGFuZ2VzLlxubGV0IF9zdHlsaW5nRWxlbWVudDogYW55ID0gbnVsbDtcblxuLyoqXG4gKiBVc2VkIGFzIGEgc3RhdGUgcmVmZXJlbmNlIGZvciB1cGRhdGUgdmFsdWVzIGJldHdlZW4gc3R5bGUvY2xhc3MgYmluZGluZyBpbnN0cnVjdGlvbnMuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgU3R5bGluZ1N0YXRlIHtcbiAgY2xhc3Nlc0JpdE1hc2s6IG51bWJlcjtcbiAgY2xhc3Nlc0luZGV4OiBudW1iZXI7XG4gIHN0eWxlc0JpdE1hc2s6IG51bWJlcjtcbiAgc3R5bGVzSW5kZXg6IG51bWJlcjtcbn1cblxuZXhwb3J0IGNvbnN0IFNUWUxJTkdfSU5ERVhfU1RBUlRfVkFMVUUgPSAxO1xuZXhwb3J0IGNvbnN0IEJJVF9NQVNLX1NUQVJUX1ZBTFVFID0gMDtcblxuZXhwb3J0IGZ1bmN0aW9uIGdldFN0eWxpbmdTdGF0ZShlbGVtZW50OiBhbnksIHJlYWRGcm9tTWFwPzogYm9vbGVhbik6IFN0eWxpbmdTdGF0ZSB7XG4gIGlmICghX3N0eWxpbmdFbGVtZW50IHx8IGVsZW1lbnQgIT09IF9zdHlsaW5nRWxlbWVudCkge1xuICAgIF9zdHlsaW5nRWxlbWVudCA9IGVsZW1lbnQ7XG4gICAgaWYgKHJlYWRGcm9tTWFwKSB7XG4gICAgICBfc3R5bGluZ1N0YXRlID0gX3N0YXRlU3RvcmFnZS5nZXQoZWxlbWVudCkgfHwgbnVsbDtcbiAgICAgIG5nRGV2TW9kZSAmJiBuZ0Rldk1vZGUuc3R5bGluZ1JlYWRQZXJzaXN0ZWRTdGF0ZSsrO1xuICAgIH1cbiAgICBfc3R5bGluZ1N0YXRlID0gX3N0eWxpbmdTdGF0ZSB8fCB7XG4gICAgICBjbGFzc2VzQml0TWFzazogQklUX01BU0tfU1RBUlRfVkFMVUUsXG4gICAgICBjbGFzc2VzSW5kZXg6IFNUWUxJTkdfSU5ERVhfU1RBUlRfVkFMVUUsXG4gICAgICBzdHlsZXNCaXRNYXNrOiBCSVRfTUFTS19TVEFSVF9WQUxVRSxcbiAgICAgIHN0eWxlc0luZGV4OiBTVFlMSU5HX0lOREVYX1NUQVJUX1ZBTFVFLFxuICAgIH07XG4gIH1cbiAgcmV0dXJuIF9zdHlsaW5nU3RhdGUgITtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlc2V0U3R5bGluZ1N0YXRlKCkge1xuICBfc3R5bGluZ1N0YXRlID0gbnVsbDtcbiAgX3N0eWxpbmdFbGVtZW50ID0gbnVsbDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHN0b3JlU3R5bGluZ1N0YXRlKGVsZW1lbnQ6IGFueSwgc3RhdGU6IFN0eWxpbmdTdGF0ZSkge1xuICBuZ0Rldk1vZGUgJiYgbmdEZXZNb2RlLnN0eWxpbmdXcml0ZVBlcnNpc3RlZFN0YXRlKys7XG4gIF9zdGF0ZVN0b3JhZ2Uuc2V0KGVsZW1lbnQsIHN0YXRlKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRlbGV0ZVN0eWxpbmdTdGF0ZUZyb21TdG9yYWdlKGVsZW1lbnQ6IGFueSkge1xuICBfc3RhdGVTdG9yYWdlLmRlbGV0ZShlbGVtZW50KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlc2V0QWxsU3R5bGluZ1N0YXRlKCkge1xuICByZXNldFN0eWxpbmdTdGF0ZSgpO1xuICBfc3RhdGVTdG9yYWdlLmNsZWFyKCk7XG59XG4iXX0=
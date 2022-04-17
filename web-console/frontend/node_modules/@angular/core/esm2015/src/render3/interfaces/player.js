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
 * A shared interface which contains an animation player
 * @record
 */
export function Player() { }
if (false) {
    /** @type {?|undefined} */
    Player.prototype.parent;
    /** @type {?} */
    Player.prototype.state;
    /**
     * @return {?}
     */
    Player.prototype.play = function () { };
    /**
     * @return {?}
     */
    Player.prototype.pause = function () { };
    /**
     * @return {?}
     */
    Player.prototype.finish = function () { };
    /**
     * @return {?}
     */
    Player.prototype.destroy = function () { };
    /**
     * @param {?} state
     * @param {?} cb
     * @return {?}
     */
    Player.prototype.addEventListener = function (state, cb) { };
}
/** @enum {number} */
const BindingType = {
    Unset: 0,
    Class: 1,
    Style: 2,
};
export { BindingType };
/**
 * @record
 */
export function BindingStore() { }
if (false) {
    /**
     * @param {?} prop
     * @param {?} value
     * @return {?}
     */
    BindingStore.prototype.setValue = function (prop, value) { };
}
/**
 * Defines the shape which produces the Player.
 *
 * Used to produce a player that will be placed on an element that contains
 * styling bindings that make use of the player. This function is designed
 * to be used with `PlayerFactory`.
 * @record
 */
export function PlayerFactoryBuildFn() { }
/**
 * Used as a reference to build a player from a styling template binding
 * (`[style]` and `[class]`).
 *
 * The `fn` function will be called once any styling-related changes are
 * evaluated on an element and is expected to return a player that will
 * be then run on the element.
 *
 * `[style]`, `[style.prop]`, `[class]` and `[class.name]` template bindings
 * all accept a `PlayerFactory` as input and this player factories.
 * @record
 */
export function PlayerFactory() { }
if (false) {
    /** @type {?} */
    PlayerFactory.prototype.__brand__;
}
/**
 * @record
 */
export function PlayerBuilder() { }
if (false) {
    /**
     * @param {?} currentPlayer
     * @param {?} isFirstRender
     * @return {?}
     */
    PlayerBuilder.prototype.buildPlayer = function (currentPlayer, isFirstRender) { };
}
/** @enum {number} */
const PlayState = {
    Pending: 0, Running: 1, Paused: 2, Finished: 100, Destroyed: 200,
};
export { PlayState };
/**
 * The context that stores all the active players and queued player factories present on an element.
 * @record
 */
export function PlayerContext() { }
if (false) {
    /* Skipping unnamed member:
    [PlayerIndex.NonBuilderPlayersStart]: number;*/
    /* Skipping unnamed member:
    [PlayerIndex.ClassMapPlayerBuilderPosition]: PlayerBuilder|null;*/
    /* Skipping unnamed member:
    [PlayerIndex.ClassMapPlayerPosition]: Player|null;*/
    /* Skipping unnamed member:
    [PlayerIndex.StyleMapPlayerBuilderPosition]: PlayerBuilder|null;*/
    /* Skipping unnamed member:
    [PlayerIndex.StyleMapPlayerPosition]: Player|null;*/
}
/**
 * Designed to be used as an injection service to capture all animation players.
 *
 * When present all animation players will be passed into the flush method below.
 * This feature is designed to service application-wide animation testing, live
 * debugging as well as custom animation choreographing tools.
 * @record
 */
export function PlayerHandler() { }
if (false) {
    /**
     * Designed to kick off the player at the end of change detection
     * @return {?}
     */
    PlayerHandler.prototype.flushPlayers = function () { };
    /**
     * @param {?} player The player that has been scheduled to run within the application.
     * @param {?} context The context as to where the player was bound to
     * @return {?}
     */
    PlayerHandler.prototype.queuePlayer = function (player, context) { };
}
/** @enum {number} */
const PlayerIndex = {
    // The position where the index that reveals where players start in the PlayerContext
    NonBuilderPlayersStart: 0,
    // The position where the player builder lives (which handles {key:value} map expression) for
    // classes
    ClassMapPlayerBuilderPosition: 1,
    // The position where the last player assigned to the class player builder is stored
    ClassMapPlayerPosition: 2,
    // The position where the player builder lives (which handles {key:value} map expression) for
    // styles
    StyleMapPlayerBuilderPosition: 3,
    // The position where the last player assigned to the style player builder is stored
    StyleMapPlayerPosition: 4,
    // The position where any player builders start in the PlayerContext
    PlayerBuildersStartPosition: 1,
    // The position where non map-based player builders start in the PlayerContext
    SinglePlayerBuildersStartPosition: 5,
    // For each player builder there is a player in the player context (therefore size = 2)
    PlayerAndPlayerBuildersTupleSize: 2,
    // The player exists next to the player builder in the list
    PlayerOffsetPosition: 1,
};
export { PlayerIndex };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGxheWVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvcmVuZGVyMy9pbnRlcmZhY2VzL3BsYXllci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7QUFXQSw0QkFRQzs7O0lBUEMsd0JBQXFCOztJQUNyQix1QkFBaUI7Ozs7SUFDakIsd0NBQWE7Ozs7SUFDYix5Q0FBYzs7OztJQUNkLDBDQUFlOzs7O0lBQ2YsMkNBQWdCOzs7Ozs7SUFDaEIsNkRBQXlFOzs7O0lBSXpFLFFBQVM7SUFDVCxRQUFTO0lBQ1QsUUFBUzs7Ozs7O0FBR1gsa0NBQTJFOzs7Ozs7O0lBQTNDLDZEQUF5Qzs7Ozs7Ozs7OztBQVN6RSwwQ0FHQzs7Ozs7Ozs7Ozs7OztBQWFELG1DQUFrRzs7O0lBQWpFLGtDQUErRDs7Ozs7QUFFaEcsbUNBRUM7Ozs7Ozs7SUFEQyxrRkFBdUY7Ozs7SUFVNUQsVUFBVyxFQUFFLFVBQVcsRUFBRSxTQUFVLEVBQUUsYUFBYyxFQUFFLGNBQWU7Ozs7Ozs7QUFLbEcsbUNBTUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQVNELG1DQVdDOzs7Ozs7SUFQQyx1REFBcUI7Ozs7OztJQU1yQixxRUFBNEY7Ozs7SUFJNUYscUZBQXFGO0lBQ3JGLHlCQUEwQjtJQUMxQiw2RkFBNkY7SUFDN0YsVUFBVTtJQUNWLGdDQUFpQztJQUNqQyxvRkFBb0Y7SUFDcEYseUJBQTBCO0lBQzFCLDZGQUE2RjtJQUM3RixTQUFTO0lBQ1QsZ0NBQWlDO0lBQ2pDLG9GQUFvRjtJQUNwRix5QkFBMEI7SUFDMUIsb0VBQW9FO0lBQ3BFLDhCQUErQjtJQUMvQiw4RUFBOEU7SUFDOUUsb0NBQXFDO0lBQ3JDLHVGQUF1RjtJQUN2RixtQ0FBb0M7SUFDcEMsMkRBQTJEO0lBQzNELHVCQUF3QiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLyoqXG4gKiBBIHNoYXJlZCBpbnRlcmZhY2Ugd2hpY2ggY29udGFpbnMgYW4gYW5pbWF0aW9uIHBsYXllclxuICovXG5leHBvcnQgaW50ZXJmYWNlIFBsYXllciB7XG4gIHBhcmVudD86IFBsYXllcnxudWxsO1xuICBzdGF0ZTogUGxheVN0YXRlO1xuICBwbGF5KCk6IHZvaWQ7XG4gIHBhdXNlKCk6IHZvaWQ7XG4gIGZpbmlzaCgpOiB2b2lkO1xuICBkZXN0cm95KCk6IHZvaWQ7XG4gIGFkZEV2ZW50TGlzdGVuZXIoc3RhdGU6IFBsYXlTdGF0ZXxzdHJpbmcsIGNiOiAoZGF0YT86IGFueSkgPT4gYW55KTogdm9pZDtcbn1cblxuZXhwb3J0IGNvbnN0IGVudW0gQmluZGluZ1R5cGUge1xuICBVbnNldCA9IDAsXG4gIENsYXNzID0gMSxcbiAgU3R5bGUgPSAyLFxufVxuXG5leHBvcnQgaW50ZXJmYWNlIEJpbmRpbmdTdG9yZSB7IHNldFZhbHVlKHByb3A6IHN0cmluZywgdmFsdWU6IGFueSk6IHZvaWQ7IH1cblxuLyoqXG4gKiBEZWZpbmVzIHRoZSBzaGFwZSB3aGljaCBwcm9kdWNlcyB0aGUgUGxheWVyLlxuICpcbiAqIFVzZWQgdG8gcHJvZHVjZSBhIHBsYXllciB0aGF0IHdpbGwgYmUgcGxhY2VkIG9uIGFuIGVsZW1lbnQgdGhhdCBjb250YWluc1xuICogc3R5bGluZyBiaW5kaW5ncyB0aGF0IG1ha2UgdXNlIG9mIHRoZSBwbGF5ZXIuIFRoaXMgZnVuY3Rpb24gaXMgZGVzaWduZWRcbiAqIHRvIGJlIHVzZWQgd2l0aCBgUGxheWVyRmFjdG9yeWAuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgUGxheWVyRmFjdG9yeUJ1aWxkRm4ge1xuICAoZWxlbWVudDogSFRNTEVsZW1lbnQsIHR5cGU6IEJpbmRpbmdUeXBlLCB2YWx1ZXM6IHtba2V5OiBzdHJpbmddOiBhbnl9LCBpc0ZpcnN0UmVuZGVyOiBib29sZWFuLFxuICAgY3VycmVudFBsYXllcjogUGxheWVyfG51bGwpOiBQbGF5ZXJ8bnVsbDtcbn1cblxuLyoqXG4gKiBVc2VkIGFzIGEgcmVmZXJlbmNlIHRvIGJ1aWxkIGEgcGxheWVyIGZyb20gYSBzdHlsaW5nIHRlbXBsYXRlIGJpbmRpbmdcbiAqIChgW3N0eWxlXWAgYW5kIGBbY2xhc3NdYCkuXG4gKlxuICogVGhlIGBmbmAgZnVuY3Rpb24gd2lsbCBiZSBjYWxsZWQgb25jZSBhbnkgc3R5bGluZy1yZWxhdGVkIGNoYW5nZXMgYXJlXG4gKiBldmFsdWF0ZWQgb24gYW4gZWxlbWVudCBhbmQgaXMgZXhwZWN0ZWQgdG8gcmV0dXJuIGEgcGxheWVyIHRoYXQgd2lsbFxuICogYmUgdGhlbiBydW4gb24gdGhlIGVsZW1lbnQuXG4gKlxuICogYFtzdHlsZV1gLCBgW3N0eWxlLnByb3BdYCwgYFtjbGFzc11gIGFuZCBgW2NsYXNzLm5hbWVdYCB0ZW1wbGF0ZSBiaW5kaW5nc1xuICogYWxsIGFjY2VwdCBhIGBQbGF5ZXJGYWN0b3J5YCBhcyBpbnB1dCBhbmQgdGhpcyBwbGF5ZXIgZmFjdG9yaWVzLlxuICovXG5leHBvcnQgaW50ZXJmYWNlIFBsYXllckZhY3RvcnkgeyAnX19icmFuZF9fJzogJ0JyYW5kIGZvciBQbGF5ZXJGYWN0b3J5IHRoYXQgbm90aGluZyB3aWxsIG1hdGNoJzsgfVxuXG5leHBvcnQgaW50ZXJmYWNlIFBsYXllckJ1aWxkZXIgZXh0ZW5kcyBCaW5kaW5nU3RvcmUge1xuICBidWlsZFBsYXllcihjdXJyZW50UGxheWVyOiBQbGF5ZXJ8bnVsbCwgaXNGaXJzdFJlbmRlcjogYm9vbGVhbik6IFBsYXllcnx1bmRlZmluZWR8bnVsbDtcbn1cblxuLyoqXG4gKiBUaGUgc3RhdGUgb2YgYSBnaXZlbiBwbGF5ZXJcbiAqXG4gKiBEbyBub3QgY2hhbmdlIHRoZSBpbmNyZWFzaW5nIG5hdHVyZSBvZiB0aGUgbnVtYmVycyBzaW5jZSB0aGUgcGxheWVyXG4gKiBjb2RlIG1heSBjb21wYXJlIHN0YXRlIGJ5IGNoZWNraW5nIGlmIGEgbnVtYmVyIGlzIGhpZ2hlciBvciBsb3dlciB0aGFuXG4gKiBhIGNlcnRhaW4gbnVtZXJpYyB2YWx1ZS5cbiAqL1xuZXhwb3J0IGNvbnN0IGVudW0gUGxheVN0YXRlIHtQZW5kaW5nID0gMCwgUnVubmluZyA9IDEsIFBhdXNlZCA9IDIsIEZpbmlzaGVkID0gMTAwLCBEZXN0cm95ZWQgPSAyMDB9XG5cbi8qKlxuICogVGhlIGNvbnRleHQgdGhhdCBzdG9yZXMgYWxsIHRoZSBhY3RpdmUgcGxheWVycyBhbmQgcXVldWVkIHBsYXllciBmYWN0b3JpZXMgcHJlc2VudCBvbiBhbiBlbGVtZW50LlxuICovXG5leHBvcnQgaW50ZXJmYWNlIFBsYXllckNvbnRleHQgZXh0ZW5kcyBBcnJheTxudWxsfG51bWJlcnxQbGF5ZXJ8UGxheWVyQnVpbGRlcj4ge1xuICBbUGxheWVySW5kZXguTm9uQnVpbGRlclBsYXllcnNTdGFydF06IG51bWJlcjtcbiAgW1BsYXllckluZGV4LkNsYXNzTWFwUGxheWVyQnVpbGRlclBvc2l0aW9uXTogUGxheWVyQnVpbGRlcnxudWxsO1xuICBbUGxheWVySW5kZXguQ2xhc3NNYXBQbGF5ZXJQb3NpdGlvbl06IFBsYXllcnxudWxsO1xuICBbUGxheWVySW5kZXguU3R5bGVNYXBQbGF5ZXJCdWlsZGVyUG9zaXRpb25dOiBQbGF5ZXJCdWlsZGVyfG51bGw7XG4gIFtQbGF5ZXJJbmRleC5TdHlsZU1hcFBsYXllclBvc2l0aW9uXTogUGxheWVyfG51bGw7XG59XG5cbi8qKlxuICogRGVzaWduZWQgdG8gYmUgdXNlZCBhcyBhbiBpbmplY3Rpb24gc2VydmljZSB0byBjYXB0dXJlIGFsbCBhbmltYXRpb24gcGxheWVycy5cbiAqXG4gKiBXaGVuIHByZXNlbnQgYWxsIGFuaW1hdGlvbiBwbGF5ZXJzIHdpbGwgYmUgcGFzc2VkIGludG8gdGhlIGZsdXNoIG1ldGhvZCBiZWxvdy5cbiAqIFRoaXMgZmVhdHVyZSBpcyBkZXNpZ25lZCB0byBzZXJ2aWNlIGFwcGxpY2F0aW9uLXdpZGUgYW5pbWF0aW9uIHRlc3RpbmcsIGxpdmVcbiAqIGRlYnVnZ2luZyBhcyB3ZWxsIGFzIGN1c3RvbSBhbmltYXRpb24gY2hvcmVvZ3JhcGhpbmcgdG9vbHMuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgUGxheWVySGFuZGxlciB7XG4gIC8qKlxuICAgKiBEZXNpZ25lZCB0byBraWNrIG9mZiB0aGUgcGxheWVyIGF0IHRoZSBlbmQgb2YgY2hhbmdlIGRldGVjdGlvblxuICAgKi9cbiAgZmx1c2hQbGF5ZXJzKCk6IHZvaWQ7XG5cbiAgLyoqXG4gICAqIEBwYXJhbSBwbGF5ZXIgVGhlIHBsYXllciB0aGF0IGhhcyBiZWVuIHNjaGVkdWxlZCB0byBydW4gd2l0aGluIHRoZSBhcHBsaWNhdGlvbi5cbiAgICogQHBhcmFtIGNvbnRleHQgVGhlIGNvbnRleHQgYXMgdG8gd2hlcmUgdGhlIHBsYXllciB3YXMgYm91bmQgdG9cbiAgICovXG4gIHF1ZXVlUGxheWVyKHBsYXllcjogUGxheWVyLCBjb250ZXh0OiBDb21wb25lbnRJbnN0YW5jZXxEaXJlY3RpdmVJbnN0YW5jZXxIVE1MRWxlbWVudCk6IHZvaWQ7XG59XG5cbmV4cG9ydCBjb25zdCBlbnVtIFBsYXllckluZGV4IHtcbiAgLy8gVGhlIHBvc2l0aW9uIHdoZXJlIHRoZSBpbmRleCB0aGF0IHJldmVhbHMgd2hlcmUgcGxheWVycyBzdGFydCBpbiB0aGUgUGxheWVyQ29udGV4dFxuICBOb25CdWlsZGVyUGxheWVyc1N0YXJ0ID0gMCxcbiAgLy8gVGhlIHBvc2l0aW9uIHdoZXJlIHRoZSBwbGF5ZXIgYnVpbGRlciBsaXZlcyAod2hpY2ggaGFuZGxlcyB7a2V5OnZhbHVlfSBtYXAgZXhwcmVzc2lvbikgZm9yXG4gIC8vIGNsYXNzZXNcbiAgQ2xhc3NNYXBQbGF5ZXJCdWlsZGVyUG9zaXRpb24gPSAxLFxuICAvLyBUaGUgcG9zaXRpb24gd2hlcmUgdGhlIGxhc3QgcGxheWVyIGFzc2lnbmVkIHRvIHRoZSBjbGFzcyBwbGF5ZXIgYnVpbGRlciBpcyBzdG9yZWRcbiAgQ2xhc3NNYXBQbGF5ZXJQb3NpdGlvbiA9IDIsXG4gIC8vIFRoZSBwb3NpdGlvbiB3aGVyZSB0aGUgcGxheWVyIGJ1aWxkZXIgbGl2ZXMgKHdoaWNoIGhhbmRsZXMge2tleTp2YWx1ZX0gbWFwIGV4cHJlc3Npb24pIGZvclxuICAvLyBzdHlsZXNcbiAgU3R5bGVNYXBQbGF5ZXJCdWlsZGVyUG9zaXRpb24gPSAzLFxuICAvLyBUaGUgcG9zaXRpb24gd2hlcmUgdGhlIGxhc3QgcGxheWVyIGFzc2lnbmVkIHRvIHRoZSBzdHlsZSBwbGF5ZXIgYnVpbGRlciBpcyBzdG9yZWRcbiAgU3R5bGVNYXBQbGF5ZXJQb3NpdGlvbiA9IDQsXG4gIC8vIFRoZSBwb3NpdGlvbiB3aGVyZSBhbnkgcGxheWVyIGJ1aWxkZXJzIHN0YXJ0IGluIHRoZSBQbGF5ZXJDb250ZXh0XG4gIFBsYXllckJ1aWxkZXJzU3RhcnRQb3NpdGlvbiA9IDEsXG4gIC8vIFRoZSBwb3NpdGlvbiB3aGVyZSBub24gbWFwLWJhc2VkIHBsYXllciBidWlsZGVycyBzdGFydCBpbiB0aGUgUGxheWVyQ29udGV4dFxuICBTaW5nbGVQbGF5ZXJCdWlsZGVyc1N0YXJ0UG9zaXRpb24gPSA1LFxuICAvLyBGb3IgZWFjaCBwbGF5ZXIgYnVpbGRlciB0aGVyZSBpcyBhIHBsYXllciBpbiB0aGUgcGxheWVyIGNvbnRleHQgKHRoZXJlZm9yZSBzaXplID0gMilcbiAgUGxheWVyQW5kUGxheWVyQnVpbGRlcnNUdXBsZVNpemUgPSAyLFxuICAvLyBUaGUgcGxheWVyIGV4aXN0cyBuZXh0IHRvIHRoZSBwbGF5ZXIgYnVpbGRlciBpbiB0aGUgbGlzdFxuICBQbGF5ZXJPZmZzZXRQb3NpdGlvbiA9IDEsXG59XG5cbmV4cG9ydCBkZWNsYXJlIHR5cGUgQ29tcG9uZW50SW5zdGFuY2UgPSB7fTtcbmV4cG9ydCBkZWNsYXJlIHR5cGUgRGlyZWN0aXZlSW5zdGFuY2UgPSB7fTtcbiJdfQ==
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
/** @enum {number} */
const ChangeDetectionStrategy = {
    /**
     * Use the `CheckOnce` strategy, meaning that automatic change detection is deactivated
     * until reactivated by setting the strategy to `Default` (`CheckAlways`).
     * Change detection can still be explicitly invoked.
     * This strategy applies to all child directives and cannot be overridden.
     */
    OnPush: 0,
    /**
     * Use the default `CheckAlways` strategy, in which change detection is automatic until
     * explicitly deactivated.
     */
    Default: 1,
};
export { ChangeDetectionStrategy };
ChangeDetectionStrategy[ChangeDetectionStrategy.OnPush] = 'OnPush';
ChangeDetectionStrategy[ChangeDetectionStrategy.Default] = 'Default';
/** @enum {number} */
const ChangeDetectorStatus = {
    /**
     * A state in which, after calling `detectChanges()`, the change detector
     * state becomes `Checked`, and must be explicitly invoked or reactivated.
     */
    CheckOnce: 0,
    /**
     * A state in which change detection is skipped until the change detector mode
     * becomes `CheckOnce`.
     */
    Checked: 1,
    /**
     * A state in which change detection continues automatically until explicitly
     * deactivated.
     */
    CheckAlways: 2,
    /**
     * A state in which a change detector sub tree is not a part of the main tree and
     * should be skipped.
     */
    Detached: 3,
    /**
     * Indicates that the change detector encountered an error checking a binding
     * or calling a directive lifecycle method and is now in an inconsistent state. Change
     * detectors in this state do not detect changes.
     */
    Errored: 4,
    /**
     * Indicates that the change detector has been destroyed.
     */
    Destroyed: 5,
};
export { ChangeDetectorStatus };
ChangeDetectorStatus[ChangeDetectorStatus.CheckOnce] = 'CheckOnce';
ChangeDetectorStatus[ChangeDetectorStatus.Checked] = 'Checked';
ChangeDetectorStatus[ChangeDetectorStatus.CheckAlways] = 'CheckAlways';
ChangeDetectorStatus[ChangeDetectorStatus.Detached] = 'Detached';
ChangeDetectorStatus[ChangeDetectorStatus.Errored] = 'Errored';
ChangeDetectorStatus[ChangeDetectorStatus.Destroyed] = 'Destroyed';
/**
 * Reports whether a given strategy is currently the default for change detection.
 * @see `ChangeDetectorStatus` / `ChangeDetectorRef`
 * @param {?} changeDetectionStrategy The strategy to check.
 * @return {?} True if the given strategy is the current default, false otherwise.
 */
export function isDefaultChangeDetectionStrategy(changeDetectionStrategy) {
    return changeDetectionStrategy == null ||
        changeDetectionStrategy === ChangeDetectionStrategy.Default;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29uc3RhbnRzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvY2hhbmdlX2RldGVjdGlvbi9jb25zdGFudHMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7OztJQWdCRTs7Ozs7T0FLRztJQUNILFNBQVU7SUFFVjs7O09BR0c7SUFDSCxVQUFXOzs7Ozs7O0lBUVg7OztPQUdHO0lBQ0gsWUFBUztJQUVUOzs7T0FHRztJQUNILFVBQU87SUFFUDs7O09BR0c7SUFDSCxjQUFXO0lBRVg7OztPQUdHO0lBQ0gsV0FBUTtJQUVSOzs7O09BSUc7SUFDSCxVQUFPO0lBRVA7O09BRUc7SUFDSCxZQUFTOzs7Ozs7Ozs7Ozs7Ozs7QUFVWCxNQUFNLFVBQVUsZ0NBQWdDLENBQUMsdUJBQWdEO0lBRS9GLE9BQU8sdUJBQXVCLElBQUksSUFBSTtRQUNsQyx1QkFBdUIsS0FBSyx1QkFBdUIsQ0FBQyxPQUFPLENBQUM7QUFDbEUsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuXG4vKipcbiAqIFRoZSBzdHJhdGVneSB0aGF0IHRoZSBkZWZhdWx0IGNoYW5nZSBkZXRlY3RvciB1c2VzIHRvIGRldGVjdCBjaGFuZ2VzLlxuICogV2hlbiBzZXQsIHRha2VzIGVmZmVjdCB0aGUgbmV4dCB0aW1lIGNoYW5nZSBkZXRlY3Rpb24gaXMgdHJpZ2dlcmVkLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGVudW0gQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kge1xuICAvKipcbiAgICogVXNlIHRoZSBgQ2hlY2tPbmNlYCBzdHJhdGVneSwgbWVhbmluZyB0aGF0IGF1dG9tYXRpYyBjaGFuZ2UgZGV0ZWN0aW9uIGlzIGRlYWN0aXZhdGVkXG4gICAqIHVudGlsIHJlYWN0aXZhdGVkIGJ5IHNldHRpbmcgdGhlIHN0cmF0ZWd5IHRvIGBEZWZhdWx0YCAoYENoZWNrQWx3YXlzYCkuXG4gICAqIENoYW5nZSBkZXRlY3Rpb24gY2FuIHN0aWxsIGJlIGV4cGxpY2l0bHkgaW52b2tlZC5cbiAgICogVGhpcyBzdHJhdGVneSBhcHBsaWVzIHRvIGFsbCBjaGlsZCBkaXJlY3RpdmVzIGFuZCBjYW5ub3QgYmUgb3ZlcnJpZGRlbi5cbiAgICovXG4gIE9uUHVzaCA9IDAsXG5cbiAgLyoqXG4gICAqIFVzZSB0aGUgZGVmYXVsdCBgQ2hlY2tBbHdheXNgIHN0cmF0ZWd5LCBpbiB3aGljaCBjaGFuZ2UgZGV0ZWN0aW9uIGlzIGF1dG9tYXRpYyB1bnRpbFxuICAgKiBleHBsaWNpdGx5IGRlYWN0aXZhdGVkLlxuICAgKi9cbiAgRGVmYXVsdCA9IDEsXG59XG5cbi8qKlxuICogRGVmaW5lcyB0aGUgcG9zc2libGUgc3RhdGVzIG9mIHRoZSBkZWZhdWx0IGNoYW5nZSBkZXRlY3Rvci5cbiAqIEBzZWUgYENoYW5nZURldGVjdG9yUmVmYFxuICovXG5leHBvcnQgZW51bSBDaGFuZ2VEZXRlY3RvclN0YXR1cyB7XG4gIC8qKlxuICAgKiBBIHN0YXRlIGluIHdoaWNoLCBhZnRlciBjYWxsaW5nIGBkZXRlY3RDaGFuZ2VzKClgLCB0aGUgY2hhbmdlIGRldGVjdG9yXG4gICAqIHN0YXRlIGJlY29tZXMgYENoZWNrZWRgLCBhbmQgbXVzdCBiZSBleHBsaWNpdGx5IGludm9rZWQgb3IgcmVhY3RpdmF0ZWQuXG4gICAqL1xuICBDaGVja09uY2UsXG5cbiAgLyoqXG4gICAqIEEgc3RhdGUgaW4gd2hpY2ggY2hhbmdlIGRldGVjdGlvbiBpcyBza2lwcGVkIHVudGlsIHRoZSBjaGFuZ2UgZGV0ZWN0b3IgbW9kZVxuICAgKiBiZWNvbWVzIGBDaGVja09uY2VgLlxuICAgKi9cbiAgQ2hlY2tlZCxcblxuICAvKipcbiAgICogQSBzdGF0ZSBpbiB3aGljaCBjaGFuZ2UgZGV0ZWN0aW9uIGNvbnRpbnVlcyBhdXRvbWF0aWNhbGx5IHVudGlsIGV4cGxpY2l0bHlcbiAgICogZGVhY3RpdmF0ZWQuXG4gICAqL1xuICBDaGVja0Fsd2F5cyxcblxuICAvKipcbiAgICogQSBzdGF0ZSBpbiB3aGljaCBhIGNoYW5nZSBkZXRlY3RvciBzdWIgdHJlZSBpcyBub3QgYSBwYXJ0IG9mIHRoZSBtYWluIHRyZWUgYW5kXG4gICAqIHNob3VsZCBiZSBza2lwcGVkLlxuICAgKi9cbiAgRGV0YWNoZWQsXG5cbiAgLyoqXG4gICAqIEluZGljYXRlcyB0aGF0IHRoZSBjaGFuZ2UgZGV0ZWN0b3IgZW5jb3VudGVyZWQgYW4gZXJyb3IgY2hlY2tpbmcgYSBiaW5kaW5nXG4gICAqIG9yIGNhbGxpbmcgYSBkaXJlY3RpdmUgbGlmZWN5Y2xlIG1ldGhvZCBhbmQgaXMgbm93IGluIGFuIGluY29uc2lzdGVudCBzdGF0ZS4gQ2hhbmdlXG4gICAqIGRldGVjdG9ycyBpbiB0aGlzIHN0YXRlIGRvIG5vdCBkZXRlY3QgY2hhbmdlcy5cbiAgICovXG4gIEVycm9yZWQsXG5cbiAgLyoqXG4gICAqIEluZGljYXRlcyB0aGF0IHRoZSBjaGFuZ2UgZGV0ZWN0b3IgaGFzIGJlZW4gZGVzdHJveWVkLlxuICAgKi9cbiAgRGVzdHJveWVkLFxufVxuXG4vKipcbiAqIFJlcG9ydHMgd2hldGhlciBhIGdpdmVuIHN0cmF0ZWd5IGlzIGN1cnJlbnRseSB0aGUgZGVmYXVsdCBmb3IgY2hhbmdlIGRldGVjdGlvbi5cbiAqIEBwYXJhbSBjaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSBUaGUgc3RyYXRlZ3kgdG8gY2hlY2suXG4gKiBAcmV0dXJucyBUcnVlIGlmIHRoZSBnaXZlbiBzdHJhdGVneSBpcyB0aGUgY3VycmVudCBkZWZhdWx0LCBmYWxzZSBvdGhlcndpc2UuXG4gKiBAc2VlIGBDaGFuZ2VEZXRlY3RvclN0YXR1c2BcbiAqIEBzZWUgYENoYW5nZURldGVjdG9yUmVmYFxuICovXG5leHBvcnQgZnVuY3Rpb24gaXNEZWZhdWx0Q2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3koY2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3k6IENoYW5nZURldGVjdGlvblN0cmF0ZWd5KTpcbiAgICBib29sZWFuIHtcbiAgcmV0dXJuIGNoYW5nZURldGVjdGlvblN0cmF0ZWd5ID09IG51bGwgfHxcbiAgICAgIGNoYW5nZURldGVjdGlvblN0cmF0ZWd5ID09PSBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5EZWZhdWx0O1xufVxuIl19
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * The strategy that the default change detector uses to detect changes.
 * When set, takes effect the next time change detection is triggered.
 *
 * @publicApi
 */
export var ChangeDetectionStrategy;
(function (ChangeDetectionStrategy) {
    /**
     * Use the `CheckOnce` strategy, meaning that automatic change detection is deactivated
     * until reactivated by setting the strategy to `Default` (`CheckAlways`).
     * Change detection can still be explicitly invoked.
     * This strategy applies to all child directives and cannot be overridden.
     */
    ChangeDetectionStrategy[ChangeDetectionStrategy["OnPush"] = 0] = "OnPush";
    /**
     * Use the default `CheckAlways` strategy, in which change detection is automatic until
     * explicitly deactivated.
     */
    ChangeDetectionStrategy[ChangeDetectionStrategy["Default"] = 1] = "Default";
})(ChangeDetectionStrategy || (ChangeDetectionStrategy = {}));
/**
 * Defines the possible states of the default change detector.
 * @see `ChangeDetectorRef`
 */
export var ChangeDetectorStatus;
(function (ChangeDetectorStatus) {
    /**
     * A state in which, after calling `detectChanges()`, the change detector
     * state becomes `Checked`, and must be explicitly invoked or reactivated.
     */
    ChangeDetectorStatus[ChangeDetectorStatus["CheckOnce"] = 0] = "CheckOnce";
    /**
     * A state in which change detection is skipped until the change detector mode
     * becomes `CheckOnce`.
     */
    ChangeDetectorStatus[ChangeDetectorStatus["Checked"] = 1] = "Checked";
    /**
     * A state in which change detection continues automatically until explicitly
     * deactivated.
     */
    ChangeDetectorStatus[ChangeDetectorStatus["CheckAlways"] = 2] = "CheckAlways";
    /**
     * A state in which a change detector sub tree is not a part of the main tree and
     * should be skipped.
     */
    ChangeDetectorStatus[ChangeDetectorStatus["Detached"] = 3] = "Detached";
    /**
     * Indicates that the change detector encountered an error checking a binding
     * or calling a directive lifecycle method and is now in an inconsistent state. Change
     * detectors in this state do not detect changes.
     */
    ChangeDetectorStatus[ChangeDetectorStatus["Errored"] = 4] = "Errored";
    /**
     * Indicates that the change detector has been destroyed.
     */
    ChangeDetectorStatus[ChangeDetectorStatus["Destroyed"] = 5] = "Destroyed";
})(ChangeDetectorStatus || (ChangeDetectorStatus = {}));
/**
 * Reports whether a given strategy is currently the default for change detection.
 * @param changeDetectionStrategy The strategy to check.
 * @returns True if the given strategy is the current default, false otherwise.
 * @see `ChangeDetectorStatus`
 * @see `ChangeDetectorRef`
 */
export function isDefaultChangeDetectionStrategy(changeDetectionStrategy) {
    return changeDetectionStrategy == null ||
        changeDetectionStrategy === ChangeDetectionStrategy.Default;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29uc3RhbnRzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvY2hhbmdlX2RldGVjdGlvbi9jb25zdGFudHMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HO0FBR0g7Ozs7O0dBS0c7QUFDSCxNQUFNLENBQU4sSUFBWSx1QkFjWDtBQWRELFdBQVksdUJBQXVCO0lBQ2pDOzs7OztPQUtHO0lBQ0gseUVBQVUsQ0FBQTtJQUVWOzs7T0FHRztJQUNILDJFQUFXLENBQUE7QUFDYixDQUFDLEVBZFcsdUJBQXVCLEtBQXZCLHVCQUF1QixRQWNsQztBQUVEOzs7R0FHRztBQUNILE1BQU0sQ0FBTixJQUFZLG9CQW9DWDtBQXBDRCxXQUFZLG9CQUFvQjtJQUM5Qjs7O09BR0c7SUFDSCx5RUFBUyxDQUFBO0lBRVQ7OztPQUdHO0lBQ0gscUVBQU8sQ0FBQTtJQUVQOzs7T0FHRztJQUNILDZFQUFXLENBQUE7SUFFWDs7O09BR0c7SUFDSCx1RUFBUSxDQUFBO0lBRVI7Ozs7T0FJRztJQUNILHFFQUFPLENBQUE7SUFFUDs7T0FFRztJQUNILHlFQUFTLENBQUE7QUFDWCxDQUFDLEVBcENXLG9CQUFvQixLQUFwQixvQkFBb0IsUUFvQy9CO0FBRUQ7Ozs7OztHQU1HO0FBQ0gsTUFBTSxVQUFVLGdDQUFnQyxDQUFDLHVCQUFnRDtJQUUvRixPQUFPLHVCQUF1QixJQUFJLElBQUk7UUFDbEMsdUJBQXVCLEtBQUssdUJBQXVCLENBQUMsT0FBTyxDQUFDO0FBQ2xFLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cblxuLyoqXG4gKiBUaGUgc3RyYXRlZ3kgdGhhdCB0aGUgZGVmYXVsdCBjaGFuZ2UgZGV0ZWN0b3IgdXNlcyB0byBkZXRlY3QgY2hhbmdlcy5cbiAqIFdoZW4gc2V0LCB0YWtlcyBlZmZlY3QgdGhlIG5leHQgdGltZSBjaGFuZ2UgZGV0ZWN0aW9uIGlzIHRyaWdnZXJlZC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBlbnVtIENoYW5nZURldGVjdGlvblN0cmF0ZWd5IHtcbiAgLyoqXG4gICAqIFVzZSB0aGUgYENoZWNrT25jZWAgc3RyYXRlZ3ksIG1lYW5pbmcgdGhhdCBhdXRvbWF0aWMgY2hhbmdlIGRldGVjdGlvbiBpcyBkZWFjdGl2YXRlZFxuICAgKiB1bnRpbCByZWFjdGl2YXRlZCBieSBzZXR0aW5nIHRoZSBzdHJhdGVneSB0byBgRGVmYXVsdGAgKGBDaGVja0Fsd2F5c2ApLlxuICAgKiBDaGFuZ2UgZGV0ZWN0aW9uIGNhbiBzdGlsbCBiZSBleHBsaWNpdGx5IGludm9rZWQuXG4gICAqIFRoaXMgc3RyYXRlZ3kgYXBwbGllcyB0byBhbGwgY2hpbGQgZGlyZWN0aXZlcyBhbmQgY2Fubm90IGJlIG92ZXJyaWRkZW4uXG4gICAqL1xuICBPblB1c2ggPSAwLFxuXG4gIC8qKlxuICAgKiBVc2UgdGhlIGRlZmF1bHQgYENoZWNrQWx3YXlzYCBzdHJhdGVneSwgaW4gd2hpY2ggY2hhbmdlIGRldGVjdGlvbiBpcyBhdXRvbWF0aWMgdW50aWxcbiAgICogZXhwbGljaXRseSBkZWFjdGl2YXRlZC5cbiAgICovXG4gIERlZmF1bHQgPSAxLFxufVxuXG4vKipcbiAqIERlZmluZXMgdGhlIHBvc3NpYmxlIHN0YXRlcyBvZiB0aGUgZGVmYXVsdCBjaGFuZ2UgZGV0ZWN0b3IuXG4gKiBAc2VlIGBDaGFuZ2VEZXRlY3RvclJlZmBcbiAqL1xuZXhwb3J0IGVudW0gQ2hhbmdlRGV0ZWN0b3JTdGF0dXMge1xuICAvKipcbiAgICogQSBzdGF0ZSBpbiB3aGljaCwgYWZ0ZXIgY2FsbGluZyBgZGV0ZWN0Q2hhbmdlcygpYCwgdGhlIGNoYW5nZSBkZXRlY3RvclxuICAgKiBzdGF0ZSBiZWNvbWVzIGBDaGVja2VkYCwgYW5kIG11c3QgYmUgZXhwbGljaXRseSBpbnZva2VkIG9yIHJlYWN0aXZhdGVkLlxuICAgKi9cbiAgQ2hlY2tPbmNlLFxuXG4gIC8qKlxuICAgKiBBIHN0YXRlIGluIHdoaWNoIGNoYW5nZSBkZXRlY3Rpb24gaXMgc2tpcHBlZCB1bnRpbCB0aGUgY2hhbmdlIGRldGVjdG9yIG1vZGVcbiAgICogYmVjb21lcyBgQ2hlY2tPbmNlYC5cbiAgICovXG4gIENoZWNrZWQsXG5cbiAgLyoqXG4gICAqIEEgc3RhdGUgaW4gd2hpY2ggY2hhbmdlIGRldGVjdGlvbiBjb250aW51ZXMgYXV0b21hdGljYWxseSB1bnRpbCBleHBsaWNpdGx5XG4gICAqIGRlYWN0aXZhdGVkLlxuICAgKi9cbiAgQ2hlY2tBbHdheXMsXG5cbiAgLyoqXG4gICAqIEEgc3RhdGUgaW4gd2hpY2ggYSBjaGFuZ2UgZGV0ZWN0b3Igc3ViIHRyZWUgaXMgbm90IGEgcGFydCBvZiB0aGUgbWFpbiB0cmVlIGFuZFxuICAgKiBzaG91bGQgYmUgc2tpcHBlZC5cbiAgICovXG4gIERldGFjaGVkLFxuXG4gIC8qKlxuICAgKiBJbmRpY2F0ZXMgdGhhdCB0aGUgY2hhbmdlIGRldGVjdG9yIGVuY291bnRlcmVkIGFuIGVycm9yIGNoZWNraW5nIGEgYmluZGluZ1xuICAgKiBvciBjYWxsaW5nIGEgZGlyZWN0aXZlIGxpZmVjeWNsZSBtZXRob2QgYW5kIGlzIG5vdyBpbiBhbiBpbmNvbnNpc3RlbnQgc3RhdGUuIENoYW5nZVxuICAgKiBkZXRlY3RvcnMgaW4gdGhpcyBzdGF0ZSBkbyBub3QgZGV0ZWN0IGNoYW5nZXMuXG4gICAqL1xuICBFcnJvcmVkLFxuXG4gIC8qKlxuICAgKiBJbmRpY2F0ZXMgdGhhdCB0aGUgY2hhbmdlIGRldGVjdG9yIGhhcyBiZWVuIGRlc3Ryb3llZC5cbiAgICovXG4gIERlc3Ryb3llZCxcbn1cblxuLyoqXG4gKiBSZXBvcnRzIHdoZXRoZXIgYSBnaXZlbiBzdHJhdGVneSBpcyBjdXJyZW50bHkgdGhlIGRlZmF1bHQgZm9yIGNoYW5nZSBkZXRlY3Rpb24uXG4gKiBAcGFyYW0gY2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kgVGhlIHN0cmF0ZWd5IHRvIGNoZWNrLlxuICogQHJldHVybnMgVHJ1ZSBpZiB0aGUgZ2l2ZW4gc3RyYXRlZ3kgaXMgdGhlIGN1cnJlbnQgZGVmYXVsdCwgZmFsc2Ugb3RoZXJ3aXNlLlxuICogQHNlZSBgQ2hhbmdlRGV0ZWN0b3JTdGF0dXNgXG4gKiBAc2VlIGBDaGFuZ2VEZXRlY3RvclJlZmBcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGlzRGVmYXVsdENoYW5nZURldGVjdGlvblN0cmF0ZWd5KGNoYW5nZURldGVjdGlvblN0cmF0ZWd5OiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSk6XG4gICAgYm9vbGVhbiB7XG4gIHJldHVybiBjaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSA9PSBudWxsIHx8XG4gICAgICBjaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSA9PT0gQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuRGVmYXVsdDtcbn1cbiJdfQ==
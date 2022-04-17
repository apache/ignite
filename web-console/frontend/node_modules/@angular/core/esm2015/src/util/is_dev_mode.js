/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * This file is used to control if the default rendering pipeline should be `ViewEngine` or `Ivy`.
 *
 * For more information on how to run and debug tests with either Ivy or View Engine (legacy),
 * please see [BAZEL.md](./docs/BAZEL.md).
 */
let _devMode = true;
let _runModeLocked = false;
/**
 * Returns whether Angular is in development mode. After called once,
 * the value is locked and won't change any more.
 *
 * By default, this is true, unless a user calls `enableProdMode` before calling this.
 *
 * @publicApi
 */
export function isDevMode() {
    _runModeLocked = true;
    return _devMode;
}
/**
 * Disable Angular's development mode, which turns off assertions and other
 * checks within the framework.
 *
 * One important assertion this disables verifies that a change detection pass
 * does not result in additional changes to any bindings (also known as
 * unidirectional data flow).
 *
 * @publicApi
 */
export function enableProdMode() {
    if (_runModeLocked) {
        throw new Error('Cannot enable prod mode after platform setup.');
    }
    _devMode = false;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaXNfZGV2X21vZGUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy91dGlsL2lzX2Rldl9tb2RlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUVIOzs7OztHQUtHO0FBRUgsSUFBSSxRQUFRLEdBQVksSUFBSSxDQUFDO0FBQzdCLElBQUksY0FBYyxHQUFZLEtBQUssQ0FBQztBQUdwQzs7Ozs7OztHQU9HO0FBQ0gsTUFBTSxVQUFVLFNBQVM7SUFDdkIsY0FBYyxHQUFHLElBQUksQ0FBQztJQUN0QixPQUFPLFFBQVEsQ0FBQztBQUNsQixDQUFDO0FBRUQ7Ozs7Ozs7OztHQVNHO0FBQ0gsTUFBTSxVQUFVLGNBQWM7SUFDNUIsSUFBSSxjQUFjLEVBQUU7UUFDbEIsTUFBTSxJQUFJLEtBQUssQ0FBQywrQ0FBK0MsQ0FBQyxDQUFDO0tBQ2xFO0lBQ0QsUUFBUSxHQUFHLEtBQUssQ0FBQztBQUNuQixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG4vKipcbiAqIFRoaXMgZmlsZSBpcyB1c2VkIHRvIGNvbnRyb2wgaWYgdGhlIGRlZmF1bHQgcmVuZGVyaW5nIHBpcGVsaW5lIHNob3VsZCBiZSBgVmlld0VuZ2luZWAgb3IgYEl2eWAuXG4gKlxuICogRm9yIG1vcmUgaW5mb3JtYXRpb24gb24gaG93IHRvIHJ1biBhbmQgZGVidWcgdGVzdHMgd2l0aCBlaXRoZXIgSXZ5IG9yIFZpZXcgRW5naW5lIChsZWdhY3kpLFxuICogcGxlYXNlIHNlZSBbQkFaRUwubWRdKC4vZG9jcy9CQVpFTC5tZCkuXG4gKi9cblxubGV0IF9kZXZNb2RlOiBib29sZWFuID0gdHJ1ZTtcbmxldCBfcnVuTW9kZUxvY2tlZDogYm9vbGVhbiA9IGZhbHNlO1xuXG5cbi8qKlxuICogUmV0dXJucyB3aGV0aGVyIEFuZ3VsYXIgaXMgaW4gZGV2ZWxvcG1lbnQgbW9kZS4gQWZ0ZXIgY2FsbGVkIG9uY2UsXG4gKiB0aGUgdmFsdWUgaXMgbG9ja2VkIGFuZCB3b24ndCBjaGFuZ2UgYW55IG1vcmUuXG4gKlxuICogQnkgZGVmYXVsdCwgdGhpcyBpcyB0cnVlLCB1bmxlc3MgYSB1c2VyIGNhbGxzIGBlbmFibGVQcm9kTW9kZWAgYmVmb3JlIGNhbGxpbmcgdGhpcy5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBpc0Rldk1vZGUoKTogYm9vbGVhbiB7XG4gIF9ydW5Nb2RlTG9ja2VkID0gdHJ1ZTtcbiAgcmV0dXJuIF9kZXZNb2RlO1xufVxuXG4vKipcbiAqIERpc2FibGUgQW5ndWxhcidzIGRldmVsb3BtZW50IG1vZGUsIHdoaWNoIHR1cm5zIG9mZiBhc3NlcnRpb25zIGFuZCBvdGhlclxuICogY2hlY2tzIHdpdGhpbiB0aGUgZnJhbWV3b3JrLlxuICpcbiAqIE9uZSBpbXBvcnRhbnQgYXNzZXJ0aW9uIHRoaXMgZGlzYWJsZXMgdmVyaWZpZXMgdGhhdCBhIGNoYW5nZSBkZXRlY3Rpb24gcGFzc1xuICogZG9lcyBub3QgcmVzdWx0IGluIGFkZGl0aW9uYWwgY2hhbmdlcyB0byBhbnkgYmluZGluZ3MgKGFsc28ga25vd24gYXNcbiAqIHVuaWRpcmVjdGlvbmFsIGRhdGEgZmxvdykuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gZW5hYmxlUHJvZE1vZGUoKTogdm9pZCB7XG4gIGlmIChfcnVuTW9kZUxvY2tlZCkge1xuICAgIHRocm93IG5ldyBFcnJvcignQ2Fubm90IGVuYWJsZSBwcm9kIG1vZGUgYWZ0ZXIgcGxhdGZvcm0gc2V0dXAuJyk7XG4gIH1cbiAgX2Rldk1vZGUgPSBmYWxzZTtcbn0iXX0=
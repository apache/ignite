/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/compiler/src/render3/r3_types", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    /**
     * Comment to insert above back-patch
     */
    exports.BUILD_OPTIMIZER_COLOCATE = '@__BUILD_OPTIMIZER_COLOCATE__';
    /**
     * Comment to mark removable expressions
     */
    exports.BUILD_OPTIMIZER_REMOVE = '@__BUILD_OPTIMIZER_REMOVE__';
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicjNfdHlwZXMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvcmVuZGVyMy9yM190eXBlcy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVIOztPQUVHO0lBQ1UsUUFBQSx3QkFBd0IsR0FBRywrQkFBK0IsQ0FBQztJQUV4RTs7T0FFRztJQUNVLFFBQUEsc0JBQXNCLEdBQUcsNkJBQTZCLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8qKlxuICogQ29tbWVudCB0byBpbnNlcnQgYWJvdmUgYmFjay1wYXRjaFxuICovXG5leHBvcnQgY29uc3QgQlVJTERfT1BUSU1JWkVSX0NPTE9DQVRFID0gJ0BfX0JVSUxEX09QVElNSVpFUl9DT0xPQ0FURV9fJztcblxuLyoqXG4gKiBDb21tZW50IHRvIG1hcmsgcmVtb3ZhYmxlIGV4cHJlc3Npb25zXG4gKi9cbmV4cG9ydCBjb25zdCBCVUlMRF9PUFRJTUlaRVJfUkVNT1ZFID0gJ0BfX0JVSUxEX09QVElNSVpFUl9SRU1PVkVfXyc7XG4iXX0=
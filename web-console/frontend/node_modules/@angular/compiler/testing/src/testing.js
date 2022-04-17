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
        define("@angular/compiler/testing/src/testing", ["require", "exports", "tslib", "@angular/compiler/testing/src/resource_loader_mock", "@angular/compiler/testing/src/schema_registry_mock", "@angular/compiler/testing/src/directive_resolver_mock", "@angular/compiler/testing/src/ng_module_resolver_mock", "@angular/compiler/testing/src/pipe_resolver_mock"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    /**
     * @module
     * @description
     * Entry point for all APIs of the compiler package.
     *
     * <div class="callout is-critical">
     *   <header>Unstable APIs</header>
     *   <p>
     *     All compiler apis are currently considered experimental and private!
     *   </p>
     *   <p>
     *     We expect the APIs in this package to keep on changing. Do not rely on them.
     *   </p>
     * </div>
     */
    tslib_1.__exportStar(require("@angular/compiler/testing/src/resource_loader_mock"), exports);
    tslib_1.__exportStar(require("@angular/compiler/testing/src/schema_registry_mock"), exports);
    tslib_1.__exportStar(require("@angular/compiler/testing/src/directive_resolver_mock"), exports);
    tslib_1.__exportStar(require("@angular/compiler/testing/src/ng_module_resolver_mock"), exports);
    tslib_1.__exportStar(require("@angular/compiler/testing/src/pipe_resolver_mock"), exports);
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGVzdGluZy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3Rlc3Rpbmcvc3JjL3Rlc3RpbmcudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7O0lBRUg7Ozs7Ozs7Ozs7Ozs7O09BY0c7SUFDSCw2RkFBdUM7SUFDdkMsNkZBQXVDO0lBQ3ZDLGdHQUEwQztJQUMxQyxnR0FBMEM7SUFDMUMsMkZBQXFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG4vKipcbiAqIEBtb2R1bGVcbiAqIEBkZXNjcmlwdGlvblxuICogRW50cnkgcG9pbnQgZm9yIGFsbCBBUElzIG9mIHRoZSBjb21waWxlciBwYWNrYWdlLlxuICpcbiAqIDxkaXYgY2xhc3M9XCJjYWxsb3V0IGlzLWNyaXRpY2FsXCI+XG4gKiAgIDxoZWFkZXI+VW5zdGFibGUgQVBJczwvaGVhZGVyPlxuICogICA8cD5cbiAqICAgICBBbGwgY29tcGlsZXIgYXBpcyBhcmUgY3VycmVudGx5IGNvbnNpZGVyZWQgZXhwZXJpbWVudGFsIGFuZCBwcml2YXRlIVxuICogICA8L3A+XG4gKiAgIDxwPlxuICogICAgIFdlIGV4cGVjdCB0aGUgQVBJcyBpbiB0aGlzIHBhY2thZ2UgdG8ga2VlcCBvbiBjaGFuZ2luZy4gRG8gbm90IHJlbHkgb24gdGhlbS5cbiAqICAgPC9wPlxuICogPC9kaXY+XG4gKi9cbmV4cG9ydCAqIGZyb20gJy4vcmVzb3VyY2VfbG9hZGVyX21vY2snO1xuZXhwb3J0ICogZnJvbSAnLi9zY2hlbWFfcmVnaXN0cnlfbW9jayc7XG5leHBvcnQgKiBmcm9tICcuL2RpcmVjdGl2ZV9yZXNvbHZlcl9tb2NrJztcbmV4cG9ydCAqIGZyb20gJy4vbmdfbW9kdWxlX3Jlc29sdmVyX21vY2snO1xuZXhwb3J0ICogZnJvbSAnLi9waXBlX3Jlc29sdmVyX21vY2snO1xuIl19
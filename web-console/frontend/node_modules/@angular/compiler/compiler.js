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
        define("@angular/compiler/compiler", ["require", "exports", "tslib", "@angular/compiler/public_api"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    // This file is not used to build this module. It is only used during editing
    // by the TypeScript language service and during build for verification. `ngc`
    // replaces this file with production index.ts when it rewrites private symbol
    // names.
    tslib_1.__exportStar(require("@angular/compiler/public_api"), exports);
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tcGlsZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9jb21waWxlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFFSCw2RUFBNkU7SUFDN0UsOEVBQThFO0lBQzlFLDhFQUE4RTtJQUM5RSxTQUFTO0lBRVQsdUVBQTZCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG4vLyBUaGlzIGZpbGUgaXMgbm90IHVzZWQgdG8gYnVpbGQgdGhpcyBtb2R1bGUuIEl0IGlzIG9ubHkgdXNlZCBkdXJpbmcgZWRpdGluZ1xuLy8gYnkgdGhlIFR5cGVTY3JpcHQgbGFuZ3VhZ2Ugc2VydmljZSBhbmQgZHVyaW5nIGJ1aWxkIGZvciB2ZXJpZmljYXRpb24uIGBuZ2NgXG4vLyByZXBsYWNlcyB0aGlzIGZpbGUgd2l0aCBwcm9kdWN0aW9uIGluZGV4LnRzIHdoZW4gaXQgcmV3cml0ZXMgcHJpdmF0ZSBzeW1ib2xcbi8vIG5hbWVzLlxuXG5leHBvcnQgKiBmcm9tICcuL3B1YmxpY19hcGknO1xuIl19
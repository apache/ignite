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
        define("@angular/language-service/language-service", ["require", "exports", "tslib", "@angular/language-service/src/language_service", "@angular/language-service/src/ts_plugin", "@angular/language-service/src/typescript_host", "@angular/language-service/src/version"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    /// <reference types="node" />
    /**
     * @module
     * @description
     * Entry point for all public APIs of the language service package.
     */
    var language_service_1 = require("@angular/language-service/src/language_service");
    exports.createLanguageService = language_service_1.createLanguageService;
    tslib_1.__exportStar(require("@angular/language-service/src/ts_plugin"), exports);
    var typescript_host_1 = require("@angular/language-service/src/typescript_host");
    exports.TypeScriptServiceHost = typescript_host_1.TypeScriptServiceHost;
    exports.createLanguageServiceFromTypescript = typescript_host_1.createLanguageServiceFromTypescript;
    var version_1 = require("@angular/language-service/src/version");
    exports.VERSION = version_1.VERSION;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibGFuZ3VhZ2Utc2VydmljZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2xhbmd1YWdlLXNlcnZpY2UvbGFuZ3VhZ2Utc2VydmljZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFFSCw4QkFBOEI7SUFFOUI7Ozs7T0FJRztJQUNILG1GQUE2RDtJQUFyRCxtREFBQSxxQkFBcUIsQ0FBQTtJQUM3QixrRkFBZ0M7SUFFaEMsaUZBQWlHO0lBQXpGLGtEQUFBLHFCQUFxQixDQUFBO0lBQUUsZ0VBQUEsbUNBQW1DLENBQUE7SUFDbEUsaUVBQXNDO0lBQTlCLDRCQUFBLE9BQU8sQ0FBQSIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLy8vIDxyZWZlcmVuY2UgdHlwZXM9XCJub2RlXCIgLz5cblxuLyoqXG4gKiBAbW9kdWxlXG4gKiBAZGVzY3JpcHRpb25cbiAqIEVudHJ5IHBvaW50IGZvciBhbGwgcHVibGljIEFQSXMgb2YgdGhlIGxhbmd1YWdlIHNlcnZpY2UgcGFja2FnZS5cbiAqL1xuZXhwb3J0IHtjcmVhdGVMYW5ndWFnZVNlcnZpY2V9IGZyb20gJy4vc3JjL2xhbmd1YWdlX3NlcnZpY2UnO1xuZXhwb3J0ICogZnJvbSAnLi9zcmMvdHNfcGx1Z2luJztcbmV4cG9ydCB7Q29tcGxldGlvbiwgQ29tcGxldGlvbnMsIERlY2xhcmF0aW9uLCBEZWNsYXJhdGlvbnMsIERlZmluaXRpb24sIERpYWdub3N0aWMsIERpYWdub3N0aWNzLCBIb3ZlciwgSG92ZXJUZXh0U2VjdGlvbiwgTGFuZ3VhZ2VTZXJ2aWNlLCBMYW5ndWFnZVNlcnZpY2VIb3N0LCBMb2NhdGlvbiwgU3BhbiwgVGVtcGxhdGVTb3VyY2UsIFRlbXBsYXRlU291cmNlc30gZnJvbSAnLi9zcmMvdHlwZXMnO1xuZXhwb3J0IHtUeXBlU2NyaXB0U2VydmljZUhvc3QsIGNyZWF0ZUxhbmd1YWdlU2VydmljZUZyb21UeXBlc2NyaXB0fSBmcm9tICcuL3NyYy90eXBlc2NyaXB0X2hvc3QnO1xuZXhwb3J0IHtWRVJTSU9OfSBmcm9tICcuL3NyYy92ZXJzaW9uJztcbiJdfQ==
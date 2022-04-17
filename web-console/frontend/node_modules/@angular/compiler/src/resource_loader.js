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
        define("@angular/compiler/src/resource_loader", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    /**
     * An interface for retrieving documents by URL that the compiler uses
     * to load templates.
     */
    var ResourceLoader = /** @class */ (function () {
        function ResourceLoader() {
        }
        ResourceLoader.prototype.get = function (url) { return ''; };
        return ResourceLoader;
    }());
    exports.ResourceLoader = ResourceLoader;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVzb3VyY2VfbG9hZGVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL3Jlc291cmNlX2xvYWRlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVIOzs7T0FHRztJQUNIO1FBQUE7UUFFQSxDQUFDO1FBREMsNEJBQUcsR0FBSCxVQUFJLEdBQVcsSUFBNEIsT0FBTyxFQUFFLENBQUMsQ0FBQyxDQUFDO1FBQ3pELHFCQUFDO0lBQUQsQ0FBQyxBQUZELElBRUM7SUFGWSx3Q0FBYyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLyoqXG4gKiBBbiBpbnRlcmZhY2UgZm9yIHJldHJpZXZpbmcgZG9jdW1lbnRzIGJ5IFVSTCB0aGF0IHRoZSBjb21waWxlciB1c2VzXG4gKiB0byBsb2FkIHRlbXBsYXRlcy5cbiAqL1xuZXhwb3J0IGNsYXNzIFJlc291cmNlTG9hZGVyIHtcbiAgZ2V0KHVybDogc3RyaW5nKTogUHJvbWlzZTxzdHJpbmc+fHN0cmluZyB7IHJldHVybiAnJzsgfVxufVxuIl19
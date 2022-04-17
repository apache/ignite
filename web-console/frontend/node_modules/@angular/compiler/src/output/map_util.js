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
        define("@angular/compiler/src/output/map_util", ["require", "exports", "@angular/compiler/src/output/output_ast"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var o = require("@angular/compiler/src/output/output_ast");
    function mapEntry(key, value) {
        return { key: key, value: value, quoted: false };
    }
    exports.mapEntry = mapEntry;
    function mapLiteral(obj, quoted) {
        if (quoted === void 0) { quoted = false; }
        return o.literalMap(Object.keys(obj).map(function (key) { return ({
            key: key,
            quoted: quoted,
            value: obj[key],
        }); }));
    }
    exports.mapLiteral = mapLiteral;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWFwX3V0aWwuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvb3V0cHV0L21hcF91dGlsLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBRUgsMkRBQWtDO0lBVWxDLFNBQWdCLFFBQVEsQ0FBQyxHQUFXLEVBQUUsS0FBbUI7UUFDdkQsT0FBTyxFQUFDLEdBQUcsS0FBQSxFQUFFLEtBQUssT0FBQSxFQUFFLE1BQU0sRUFBRSxLQUFLLEVBQUMsQ0FBQztJQUNyQyxDQUFDO0lBRkQsNEJBRUM7SUFFRCxTQUFnQixVQUFVLENBQ3RCLEdBQWtDLEVBQUUsTUFBdUI7UUFBdkIsdUJBQUEsRUFBQSxjQUF1QjtRQUM3RCxPQUFPLENBQUMsQ0FBQyxVQUFVLENBQUMsTUFBTSxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQyxHQUFHLENBQUMsVUFBQSxHQUFHLElBQUksT0FBQSxDQUFDO1lBQ04sR0FBRyxLQUFBO1lBQ0gsTUFBTSxRQUFBO1lBQ04sS0FBSyxFQUFFLEdBQUcsQ0FBQyxHQUFHLENBQUM7U0FDaEIsQ0FBQyxFQUpLLENBSUwsQ0FBQyxDQUFDLENBQUM7SUFDaEQsQ0FBQztJQVBELGdDQU9DIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQgKiBhcyBvIGZyb20gJy4vb3V0cHV0X2FzdCc7XG5cbmV4cG9ydCB0eXBlIE1hcEVudHJ5ID0ge1xuICBrZXk6IHN0cmluZyxcbiAgcXVvdGVkOiBib29sZWFuLFxuICB2YWx1ZTogby5FeHByZXNzaW9uXG59O1xuXG5leHBvcnQgdHlwZSBNYXBMaXRlcmFsID0gTWFwRW50cnlbXTtcblxuZXhwb3J0IGZ1bmN0aW9uIG1hcEVudHJ5KGtleTogc3RyaW5nLCB2YWx1ZTogby5FeHByZXNzaW9uKTogTWFwRW50cnkge1xuICByZXR1cm4ge2tleSwgdmFsdWUsIHF1b3RlZDogZmFsc2V9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gbWFwTGl0ZXJhbChcbiAgICBvYmo6IHtba2V5OiBzdHJpbmddOiBvLkV4cHJlc3Npb259LCBxdW90ZWQ6IGJvb2xlYW4gPSBmYWxzZSk6IG8uRXhwcmVzc2lvbiB7XG4gIHJldHVybiBvLmxpdGVyYWxNYXAoT2JqZWN0LmtleXMob2JqKS5tYXAoa2V5ID0+ICh7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBrZXksXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBxdW90ZWQsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB2YWx1ZTogb2JqW2tleV0sXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfSkpKTtcbn1cbiJdfQ==
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
        define("@angular/compiler/src/aot/generated_file", ["require", "exports", "@angular/compiler/src/output/output_ast", "@angular/compiler/src/output/ts_emitter"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var output_ast_1 = require("@angular/compiler/src/output/output_ast");
    var ts_emitter_1 = require("@angular/compiler/src/output/ts_emitter");
    var GeneratedFile = /** @class */ (function () {
        function GeneratedFile(srcFileUrl, genFileUrl, sourceOrStmts) {
            this.srcFileUrl = srcFileUrl;
            this.genFileUrl = genFileUrl;
            if (typeof sourceOrStmts === 'string') {
                this.source = sourceOrStmts;
                this.stmts = null;
            }
            else {
                this.source = null;
                this.stmts = sourceOrStmts;
            }
        }
        GeneratedFile.prototype.isEquivalent = function (other) {
            if (this.genFileUrl !== other.genFileUrl) {
                return false;
            }
            if (this.source) {
                return this.source === other.source;
            }
            if (other.stmts == null) {
                return false;
            }
            // Note: the constructor guarantees that if this.source is not filled,
            // then this.stmts is.
            return output_ast_1.areAllEquivalent(this.stmts, other.stmts);
        };
        return GeneratedFile;
    }());
    exports.GeneratedFile = GeneratedFile;
    function toTypeScript(file, preamble) {
        if (preamble === void 0) { preamble = ''; }
        if (!file.stmts) {
            throw new Error("Illegal state: No stmts present on GeneratedFile " + file.genFileUrl);
        }
        return new ts_emitter_1.TypeScriptEmitter().emitStatements(file.genFileUrl, file.stmts, preamble);
    }
    exports.toTypeScript = toTypeScript;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZ2VuZXJhdGVkX2ZpbGUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvYW90L2dlbmVyYXRlZF9maWxlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBRUgsc0VBQWlFO0lBQ2pFLHNFQUF1RDtJQUV2RDtRQUlFLHVCQUNXLFVBQWtCLEVBQVMsVUFBa0IsRUFBRSxhQUFpQztZQUFoRixlQUFVLEdBQVYsVUFBVSxDQUFRO1lBQVMsZUFBVSxHQUFWLFVBQVUsQ0FBUTtZQUN0RCxJQUFJLE9BQU8sYUFBYSxLQUFLLFFBQVEsRUFBRTtnQkFDckMsSUFBSSxDQUFDLE1BQU0sR0FBRyxhQUFhLENBQUM7Z0JBQzVCLElBQUksQ0FBQyxLQUFLLEdBQUcsSUFBSSxDQUFDO2FBQ25CO2lCQUFNO2dCQUNMLElBQUksQ0FBQyxNQUFNLEdBQUcsSUFBSSxDQUFDO2dCQUNuQixJQUFJLENBQUMsS0FBSyxHQUFHLGFBQWEsQ0FBQzthQUM1QjtRQUNILENBQUM7UUFFRCxvQ0FBWSxHQUFaLFVBQWEsS0FBb0I7WUFDL0IsSUFBSSxJQUFJLENBQUMsVUFBVSxLQUFLLEtBQUssQ0FBQyxVQUFVLEVBQUU7Z0JBQ3hDLE9BQU8sS0FBSyxDQUFDO2FBQ2Q7WUFDRCxJQUFJLElBQUksQ0FBQyxNQUFNLEVBQUU7Z0JBQ2YsT0FBTyxJQUFJLENBQUMsTUFBTSxLQUFLLEtBQUssQ0FBQyxNQUFNLENBQUM7YUFDckM7WUFDRCxJQUFJLEtBQUssQ0FBQyxLQUFLLElBQUksSUFBSSxFQUFFO2dCQUN2QixPQUFPLEtBQUssQ0FBQzthQUNkO1lBQ0Qsc0VBQXNFO1lBQ3RFLHNCQUFzQjtZQUN0QixPQUFPLDZCQUFnQixDQUFDLElBQUksQ0FBQyxLQUFPLEVBQUUsS0FBSyxDQUFDLEtBQU8sQ0FBQyxDQUFDO1FBQ3ZELENBQUM7UUFDSCxvQkFBQztJQUFELENBQUMsQUE3QkQsSUE2QkM7SUE3Qlksc0NBQWE7SUErQjFCLFNBQWdCLFlBQVksQ0FBQyxJQUFtQixFQUFFLFFBQXFCO1FBQXJCLHlCQUFBLEVBQUEsYUFBcUI7UUFDckUsSUFBSSxDQUFDLElBQUksQ0FBQyxLQUFLLEVBQUU7WUFDZixNQUFNLElBQUksS0FBSyxDQUFDLHNEQUFvRCxJQUFJLENBQUMsVUFBWSxDQUFDLENBQUM7U0FDeEY7UUFDRCxPQUFPLElBQUksOEJBQWlCLEVBQUUsQ0FBQyxjQUFjLENBQUMsSUFBSSxDQUFDLFVBQVUsRUFBRSxJQUFJLENBQUMsS0FBSyxFQUFFLFFBQVEsQ0FBQyxDQUFDO0lBQ3ZGLENBQUM7SUFMRCxvQ0FLQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtTdGF0ZW1lbnQsIGFyZUFsbEVxdWl2YWxlbnR9IGZyb20gJy4uL291dHB1dC9vdXRwdXRfYXN0JztcbmltcG9ydCB7VHlwZVNjcmlwdEVtaXR0ZXJ9IGZyb20gJy4uL291dHB1dC90c19lbWl0dGVyJztcblxuZXhwb3J0IGNsYXNzIEdlbmVyYXRlZEZpbGUge1xuICBwdWJsaWMgc291cmNlOiBzdHJpbmd8bnVsbDtcbiAgcHVibGljIHN0bXRzOiBTdGF0ZW1lbnRbXXxudWxsO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIHNyY0ZpbGVVcmw6IHN0cmluZywgcHVibGljIGdlbkZpbGVVcmw6IHN0cmluZywgc291cmNlT3JTdG10czogc3RyaW5nfFN0YXRlbWVudFtdKSB7XG4gICAgaWYgKHR5cGVvZiBzb3VyY2VPclN0bXRzID09PSAnc3RyaW5nJykge1xuICAgICAgdGhpcy5zb3VyY2UgPSBzb3VyY2VPclN0bXRzO1xuICAgICAgdGhpcy5zdG10cyA9IG51bGw7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuc291cmNlID0gbnVsbDtcbiAgICAgIHRoaXMuc3RtdHMgPSBzb3VyY2VPclN0bXRzO1xuICAgIH1cbiAgfVxuXG4gIGlzRXF1aXZhbGVudChvdGhlcjogR2VuZXJhdGVkRmlsZSk6IGJvb2xlYW4ge1xuICAgIGlmICh0aGlzLmdlbkZpbGVVcmwgIT09IG90aGVyLmdlbkZpbGVVcmwpIHtcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgICB9XG4gICAgaWYgKHRoaXMuc291cmNlKSB7XG4gICAgICByZXR1cm4gdGhpcy5zb3VyY2UgPT09IG90aGVyLnNvdXJjZTtcbiAgICB9XG4gICAgaWYgKG90aGVyLnN0bXRzID09IG51bGwpIHtcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgICB9XG4gICAgLy8gTm90ZTogdGhlIGNvbnN0cnVjdG9yIGd1YXJhbnRlZXMgdGhhdCBpZiB0aGlzLnNvdXJjZSBpcyBub3QgZmlsbGVkLFxuICAgIC8vIHRoZW4gdGhpcy5zdG10cyBpcy5cbiAgICByZXR1cm4gYXJlQWxsRXF1aXZhbGVudCh0aGlzLnN0bXRzICEsIG90aGVyLnN0bXRzICEpO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB0b1R5cGVTY3JpcHQoZmlsZTogR2VuZXJhdGVkRmlsZSwgcHJlYW1ibGU6IHN0cmluZyA9ICcnKTogc3RyaW5nIHtcbiAgaWYgKCFmaWxlLnN0bXRzKSB7XG4gICAgdGhyb3cgbmV3IEVycm9yKGBJbGxlZ2FsIHN0YXRlOiBObyBzdG10cyBwcmVzZW50IG9uIEdlbmVyYXRlZEZpbGUgJHtmaWxlLmdlbkZpbGVVcmx9YCk7XG4gIH1cbiAgcmV0dXJuIG5ldyBUeXBlU2NyaXB0RW1pdHRlcigpLmVtaXRTdGF0ZW1lbnRzKGZpbGUuZ2VuRmlsZVVybCwgZmlsZS5zdG10cywgcHJlYW1ibGUpO1xufVxuIl19
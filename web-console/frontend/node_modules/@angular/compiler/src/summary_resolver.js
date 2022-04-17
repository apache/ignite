(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/compiler/src/summary_resolver", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var SummaryResolver = /** @class */ (function () {
        function SummaryResolver() {
        }
        return SummaryResolver;
    }());
    exports.SummaryResolver = SummaryResolver;
    var JitSummaryResolver = /** @class */ (function () {
        function JitSummaryResolver() {
            this._summaries = new Map();
        }
        JitSummaryResolver.prototype.isLibraryFile = function () { return false; };
        JitSummaryResolver.prototype.toSummaryFileName = function (fileName) { return fileName; };
        JitSummaryResolver.prototype.fromSummaryFileName = function (fileName) { return fileName; };
        JitSummaryResolver.prototype.resolveSummary = function (reference) {
            return this._summaries.get(reference) || null;
        };
        JitSummaryResolver.prototype.getSymbolsOf = function () { return []; };
        JitSummaryResolver.prototype.getImportAs = function (reference) { return reference; };
        JitSummaryResolver.prototype.getKnownModuleName = function (fileName) { return null; };
        JitSummaryResolver.prototype.addSummary = function (summary) { this._summaries.set(summary.symbol, summary); };
        return JitSummaryResolver;
    }());
    exports.JitSummaryResolver = JitSummaryResolver;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3VtbWFyeV9yZXNvbHZlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9zdW1tYXJ5X3Jlc29sdmVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0lBZ0JBO1FBQUE7UUFTQSxDQUFDO1FBQUQsc0JBQUM7SUFBRCxDQUFDLEFBVEQsSUFTQztJQVRxQiwwQ0FBZTtJQVdyQztRQUFBO1lBQ1UsZUFBVSxHQUFHLElBQUksR0FBRyxFQUF1QixDQUFDO1FBWXRELENBQUM7UUFWQywwQ0FBYSxHQUFiLGNBQTJCLE9BQU8sS0FBSyxDQUFDLENBQUMsQ0FBQztRQUMxQyw4Q0FBaUIsR0FBakIsVUFBa0IsUUFBZ0IsSUFBWSxPQUFPLFFBQVEsQ0FBQyxDQUFDLENBQUM7UUFDaEUsZ0RBQW1CLEdBQW5CLFVBQW9CLFFBQWdCLElBQVksT0FBTyxRQUFRLENBQUMsQ0FBQyxDQUFDO1FBQ2xFLDJDQUFjLEdBQWQsVUFBZSxTQUFlO1lBQzVCLE9BQU8sSUFBSSxDQUFDLFVBQVUsQ0FBQyxHQUFHLENBQUMsU0FBUyxDQUFDLElBQUksSUFBSSxDQUFDO1FBQ2hELENBQUM7UUFDRCx5Q0FBWSxHQUFaLGNBQXlCLE9BQU8sRUFBRSxDQUFDLENBQUMsQ0FBQztRQUNyQyx3Q0FBVyxHQUFYLFVBQVksU0FBZSxJQUFVLE9BQU8sU0FBUyxDQUFDLENBQUMsQ0FBQztRQUN4RCwrQ0FBa0IsR0FBbEIsVUFBbUIsUUFBZ0IsSUFBSSxPQUFPLElBQUksQ0FBQyxDQUFDLENBQUM7UUFDckQsdUNBQVUsR0FBVixVQUFXLE9BQXNCLElBQUksSUFBSSxDQUFDLFVBQVUsQ0FBQyxHQUFHLENBQUMsT0FBTyxDQUFDLE1BQU0sRUFBRSxPQUFPLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDdEYseUJBQUM7SUFBRCxDQUFDLEFBYkQsSUFhQztJQWJZLGdEQUFrQiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cbmltcG9ydCB7Q29tcGlsZVR5cGVTdW1tYXJ5fSBmcm9tICcuL2NvbXBpbGVfbWV0YWRhdGEnO1xuaW1wb3J0IHtUeXBlfSBmcm9tICcuL2NvcmUnO1xuXG5leHBvcnQgaW50ZXJmYWNlIFN1bW1hcnk8VD4ge1xuICBzeW1ib2w6IFQ7XG4gIG1ldGFkYXRhOiBhbnk7XG4gIHR5cGU/OiBDb21waWxlVHlwZVN1bW1hcnk7XG59XG5cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBTdW1tYXJ5UmVzb2x2ZXI8VD4ge1xuICBhYnN0cmFjdCBpc0xpYnJhcnlGaWxlKGZpbGVOYW1lOiBzdHJpbmcpOiBib29sZWFuO1xuICBhYnN0cmFjdCB0b1N1bW1hcnlGaWxlTmFtZShmaWxlTmFtZTogc3RyaW5nLCByZWZlcnJpbmdTcmNGaWxlTmFtZTogc3RyaW5nKTogc3RyaW5nO1xuICBhYnN0cmFjdCBmcm9tU3VtbWFyeUZpbGVOYW1lKGZpbGVOYW1lOiBzdHJpbmcsIHJlZmVycmluZ0xpYkZpbGVOYW1lOiBzdHJpbmcpOiBzdHJpbmc7XG4gIGFic3RyYWN0IHJlc29sdmVTdW1tYXJ5KHJlZmVyZW5jZTogVCk6IFN1bW1hcnk8VD58bnVsbDtcbiAgYWJzdHJhY3QgZ2V0U3ltYm9sc09mKGZpbGVQYXRoOiBzdHJpbmcpOiBUW118bnVsbDtcbiAgYWJzdHJhY3QgZ2V0SW1wb3J0QXMocmVmZXJlbmNlOiBUKTogVDtcbiAgYWJzdHJhY3QgZ2V0S25vd25Nb2R1bGVOYW1lKGZpbGVOYW1lOiBzdHJpbmcpOiBzdHJpbmd8bnVsbDtcbiAgYWJzdHJhY3QgYWRkU3VtbWFyeShzdW1tYXJ5OiBTdW1tYXJ5PFQ+KTogdm9pZDtcbn1cblxuZXhwb3J0IGNsYXNzIEppdFN1bW1hcnlSZXNvbHZlciBpbXBsZW1lbnRzIFN1bW1hcnlSZXNvbHZlcjxUeXBlPiB7XG4gIHByaXZhdGUgX3N1bW1hcmllcyA9IG5ldyBNYXA8VHlwZSwgU3VtbWFyeTxUeXBlPj4oKTtcblxuICBpc0xpYnJhcnlGaWxlKCk6IGJvb2xlYW4geyByZXR1cm4gZmFsc2U7IH1cbiAgdG9TdW1tYXJ5RmlsZU5hbWUoZmlsZU5hbWU6IHN0cmluZyk6IHN0cmluZyB7IHJldHVybiBmaWxlTmFtZTsgfVxuICBmcm9tU3VtbWFyeUZpbGVOYW1lKGZpbGVOYW1lOiBzdHJpbmcpOiBzdHJpbmcgeyByZXR1cm4gZmlsZU5hbWU7IH1cbiAgcmVzb2x2ZVN1bW1hcnkocmVmZXJlbmNlOiBUeXBlKTogU3VtbWFyeTxUeXBlPnxudWxsIHtcbiAgICByZXR1cm4gdGhpcy5fc3VtbWFyaWVzLmdldChyZWZlcmVuY2UpIHx8IG51bGw7XG4gIH1cbiAgZ2V0U3ltYm9sc09mKCk6IFR5cGVbXSB7IHJldHVybiBbXTsgfVxuICBnZXRJbXBvcnRBcyhyZWZlcmVuY2U6IFR5cGUpOiBUeXBlIHsgcmV0dXJuIHJlZmVyZW5jZTsgfVxuICBnZXRLbm93bk1vZHVsZU5hbWUoZmlsZU5hbWU6IHN0cmluZykgeyByZXR1cm4gbnVsbDsgfVxuICBhZGRTdW1tYXJ5KHN1bW1hcnk6IFN1bW1hcnk8VHlwZT4pIHsgdGhpcy5fc3VtbWFyaWVzLnNldChzdW1tYXJ5LnN5bWJvbCwgc3VtbWFyeSk7IH1cbn1cbiJdfQ==
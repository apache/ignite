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
        define("@angular/compiler/src/aot/summary_resolver", ["require", "exports", "@angular/compiler/src/aot/summary_serializer", "@angular/compiler/src/aot/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var summary_serializer_1 = require("@angular/compiler/src/aot/summary_serializer");
    var util_1 = require("@angular/compiler/src/aot/util");
    var AotSummaryResolver = /** @class */ (function () {
        function AotSummaryResolver(host, staticSymbolCache) {
            this.host = host;
            this.staticSymbolCache = staticSymbolCache;
            // Note: this will only contain StaticSymbols without members!
            this.summaryCache = new Map();
            this.loadedFilePaths = new Map();
            // Note: this will only contain StaticSymbols without members!
            this.importAs = new Map();
            this.knownFileNameToModuleNames = new Map();
        }
        AotSummaryResolver.prototype.isLibraryFile = function (filePath) {
            // Note: We need to strip the .ngfactory. file path,
            // so this method also works for generated files
            // (for which host.isSourceFile will always return false).
            return !this.host.isSourceFile(util_1.stripGeneratedFileSuffix(filePath));
        };
        AotSummaryResolver.prototype.toSummaryFileName = function (filePath, referringSrcFileName) {
            return this.host.toSummaryFileName(filePath, referringSrcFileName);
        };
        AotSummaryResolver.prototype.fromSummaryFileName = function (fileName, referringLibFileName) {
            return this.host.fromSummaryFileName(fileName, referringLibFileName);
        };
        AotSummaryResolver.prototype.resolveSummary = function (staticSymbol) {
            var rootSymbol = staticSymbol.members.length ?
                this.staticSymbolCache.get(staticSymbol.filePath, staticSymbol.name) :
                staticSymbol;
            var summary = this.summaryCache.get(rootSymbol);
            if (!summary) {
                this._loadSummaryFile(staticSymbol.filePath);
                summary = this.summaryCache.get(staticSymbol);
            }
            return (rootSymbol === staticSymbol && summary) || null;
        };
        AotSummaryResolver.prototype.getSymbolsOf = function (filePath) {
            if (this._loadSummaryFile(filePath)) {
                return Array.from(this.summaryCache.keys()).filter(function (symbol) { return symbol.filePath === filePath; });
            }
            return null;
        };
        AotSummaryResolver.prototype.getImportAs = function (staticSymbol) {
            staticSymbol.assertNoMembers();
            return this.importAs.get(staticSymbol);
        };
        /**
         * Converts a file path to a module name that can be used as an `import`.
         */
        AotSummaryResolver.prototype.getKnownModuleName = function (importedFilePath) {
            return this.knownFileNameToModuleNames.get(importedFilePath) || null;
        };
        AotSummaryResolver.prototype.addSummary = function (summary) { this.summaryCache.set(summary.symbol, summary); };
        AotSummaryResolver.prototype._loadSummaryFile = function (filePath) {
            var _this = this;
            var hasSummary = this.loadedFilePaths.get(filePath);
            if (hasSummary != null) {
                return hasSummary;
            }
            var json = null;
            if (this.isLibraryFile(filePath)) {
                var summaryFilePath = util_1.summaryFileName(filePath);
                try {
                    json = this.host.loadSummary(summaryFilePath);
                }
                catch (e) {
                    console.error("Error loading summary file " + summaryFilePath);
                    throw e;
                }
            }
            hasSummary = json != null;
            this.loadedFilePaths.set(filePath, hasSummary);
            if (json) {
                var _a = summary_serializer_1.deserializeSummaries(this.staticSymbolCache, this, filePath, json), moduleName = _a.moduleName, summaries = _a.summaries, importAs = _a.importAs;
                summaries.forEach(function (summary) { return _this.summaryCache.set(summary.symbol, summary); });
                if (moduleName) {
                    this.knownFileNameToModuleNames.set(filePath, moduleName);
                }
                importAs.forEach(function (importAs) { _this.importAs.set(importAs.symbol, importAs.importAs); });
            }
            return hasSummary;
        };
        return AotSummaryResolver;
    }());
    exports.AotSummaryResolver = AotSummaryResolver;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3VtbWFyeV9yZXNvbHZlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9hb3Qvc3VtbWFyeV9yZXNvbHZlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUtILG1GQUEwRDtJQUMxRCx1REFBaUU7SUE2QmpFO1FBUUUsNEJBQW9CLElBQTRCLEVBQVUsaUJBQW9DO1lBQTFFLFNBQUksR0FBSixJQUFJLENBQXdCO1lBQVUsc0JBQWlCLEdBQWpCLGlCQUFpQixDQUFtQjtZQVA5Riw4REFBOEQ7WUFDdEQsaUJBQVksR0FBRyxJQUFJLEdBQUcsRUFBdUMsQ0FBQztZQUM5RCxvQkFBZSxHQUFHLElBQUksR0FBRyxFQUFtQixDQUFDO1lBQ3JELDhEQUE4RDtZQUN0RCxhQUFRLEdBQUcsSUFBSSxHQUFHLEVBQThCLENBQUM7WUFDakQsK0JBQTBCLEdBQUcsSUFBSSxHQUFHLEVBQWtCLENBQUM7UUFFa0MsQ0FBQztRQUVsRywwQ0FBYSxHQUFiLFVBQWMsUUFBZ0I7WUFDNUIsb0RBQW9EO1lBQ3BELGdEQUFnRDtZQUNoRCwwREFBMEQ7WUFDMUQsT0FBTyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsWUFBWSxDQUFDLCtCQUF3QixDQUFDLFFBQVEsQ0FBQyxDQUFDLENBQUM7UUFDckUsQ0FBQztRQUVELDhDQUFpQixHQUFqQixVQUFrQixRQUFnQixFQUFFLG9CQUE0QjtZQUM5RCxPQUFPLElBQUksQ0FBQyxJQUFJLENBQUMsaUJBQWlCLENBQUMsUUFBUSxFQUFFLG9CQUFvQixDQUFDLENBQUM7UUFDckUsQ0FBQztRQUVELGdEQUFtQixHQUFuQixVQUFvQixRQUFnQixFQUFFLG9CQUE0QjtZQUNoRSxPQUFPLElBQUksQ0FBQyxJQUFJLENBQUMsbUJBQW1CLENBQUMsUUFBUSxFQUFFLG9CQUFvQixDQUFDLENBQUM7UUFDdkUsQ0FBQztR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LENBQUMsR0FBRyxDQUFDLE9BQU8sQ0FBQyxNQUFNLEVBQUUsT0FBTyxDQUFDLEVBQTlDLENBQThDLENBQUMsQ0FBQztnQkFDL0UsSUFBSSxVQUFVLEVBQUU7b0JBQ2QsSUFBSSxDQUFDLDBCQUEwQixDQUFDLEdBQUcsQ0FBQyxRQUFRLEVBQUUsVUFBVSxDQUFDLENBQUM7aUJBQzNEO2dCQUNELFFBQVEsQ0FBQyxPQUFPLENBQUMsVUFBQyxRQUFRLElBQU8sS0FBSSxDQUFDLFFBQVEsQ0FBQyxHQUFHLENBQUMsUUFBUSxDQUFDLE1BQU0sRUFBRSxRQUFRLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQzthQUM1RjtZQUNELE9BQU8sVUFBVSxDQUFDO1FBQ3BCLENBQUM7UUFDSCx5QkFBQztJQUFELENBQUMsQUF0RkQsSUFzRkM7SUF0RlksZ0RBQWtCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1N1bW1hcnksIFN1bW1hcnlSZXNvbHZlcn0gZnJvbSAnLi4vc3VtbWFyeV9yZXNvbHZlcic7XG5cbmltcG9ydCB7U3RhdGljU3ltYm9sLCBTdGF0aWNTeW1ib2xDYWNoZX0gZnJvbSAnLi9zdGF0aWNfc3ltYm9sJztcbmltcG9ydCB7ZGVzZXJpYWxpemVTdW1tYXJpZXN9IGZyb20gJy4vc3VtbWFyeV9zZXJpYWxpemVyJztcbmltcG9ydCB7c3RyaXBHZW5lcmF0ZWRGaWxlU3VmZml4LCBzdW1tYXJ5RmlsZU5hbWV9IGZyb20gJy4vdXRpbCc7XG5cbmV4cG9ydCBpbnRlcmZhY2UgQW90U3VtbWFyeVJlc29sdmVySG9zdCB7XG4gIC8qKlxuICAgKiBMb2FkcyBhbiBOZ01vZHVsZS9EaXJlY3RpdmUvUGlwZSBzdW1tYXJ5IGZpbGVcbiAgICovXG4gIGxvYWRTdW1tYXJ5KGZpbGVQYXRoOiBzdHJpbmcpOiBzdHJpbmd8bnVsbDtcblxuICAvKipcbiAgICogUmV0dXJucyB3aGV0aGVyIGEgZmlsZSBpcyBhIHNvdXJjZSBmaWxlIG9yIG5vdC5cbiAgICovXG4gIGlzU291cmNlRmlsZShzb3VyY2VGaWxlUGF0aDogc3RyaW5nKTogYm9vbGVhbjtcbiAgLyoqXG4gICAqIENvbnZlcnRzIGEgZmlsZSBuYW1lIGludG8gYSByZXByZXNlbnRhdGlvbiB0aGF0IHNob3VsZCBiZSBzdG9yZWQgaW4gYSBzdW1tYXJ5IGZpbGUuXG4gICAqIFRoaXMgaGFzIHRvIGluY2x1ZGUgY2hhbmdpbmcgdGhlIHN1ZmZpeCBhcyB3ZWxsLlxuICAgKiBFLmcuXG4gICAqIGBzb21lX2ZpbGUudHNgIC0+IGBzb21lX2ZpbGUuZC50c2BcbiAgICpcbiAgICogQHBhcmFtIHJlZmVycmluZ1NyY0ZpbGVOYW1lIHRoZSBzb3VyZSBmaWxlIHRoYXQgcmVmZXJzIHRvIGZpbGVOYW1lXG4gICAqL1xuICB0b1N1bW1hcnlGaWxlTmFtZShmaWxlTmFtZTogc3RyaW5nLCByZWZlcnJpbmdTcmNGaWxlTmFtZTogc3RyaW5nKTogc3RyaW5nO1xuXG4gIC8qKlxuICAgKiBDb252ZXJ0cyBhIGZpbGVOYW1lIHRoYXQgd2FzIHByb2Nlc3NlZCBieSBgdG9TdW1tYXJ5RmlsZU5hbWVgIGJhY2sgaW50byBhIHJlYWwgZmlsZU5hbWVcbiAgICogZ2l2ZW4gdGhlIGZpbGVOYW1lIG9mIHRoZSBsaWJyYXJ5IHRoYXQgaXMgcmVmZXJyaWcgdG8gaXQuXG4gICAqL1xuICBmcm9tU3VtbWFyeUZpbGVOYW1lKGZpbGVOYW1lOiBzdHJpbmcsIHJlZmVycmluZ0xpYkZpbGVOYW1lOiBzdHJpbmcpOiBzdHJpbmc7XG59XG5cbmV4cG9ydCBjbGFzcyBBb3RTdW1tYXJ5UmVzb2x2ZXIgaW1wbGVtZW50cyBTdW1tYXJ5UmVzb2x2ZXI8U3RhdGljU3ltYm9sPiB7XG4gIC8vIE5vdGU6IHRoaXMgd2lsbCBvbmx5IGNvbnRhaW4gU3RhdGljU3ltYm9scyB3aXRob3V0IG1lbWJlcnMhXG4gIHByaXZhdGUgc3VtbWFyeUNhY2hlID0gbmV3IE1hcDxTdGF0aWNTeW1ib2wsIFN1bW1hcnk8U3RhdGljU3ltYm9sPj4oKTtcbiAgcHJpdmF0ZSBsb2FkZWRGaWxlUGF0aHMgPSBuZXcgTWFwPHN0cmluZywgYm9vbGVhbj4oKTtcbiAgLy8gTm90ZTogdGhpcyB3aWxsIG9ubHkgY29udGFpbiBTdGF0aWNTeW1ib2xzIHdpdGhvdXQgbWVtYmVycyFcbiAgcHJpdmF0ZSBpbXBvcnRBcyA9IG5ldyBNYXA8U3RhdGljU3ltYm9sLCBTdGF0aWNTeW1ib2w+KCk7XG4gIHByaXZhdGUga25vd25GaWxlTmFtZVRvTW9kdWxlTmFtZXMgPSBuZXcgTWFwPHN0cmluZywgc3RyaW5nPigpO1xuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgaG9zdDogQW90U3VtbWFyeVJlc29sdmVySG9zdCwgcHJpdmF0ZSBzdGF0aWNTeW1ib2xDYWNoZTogU3RhdGljU3ltYm9sQ2FjaGUpIHt9XG5cbiAgaXNMaWJyYXJ5RmlsZShmaWxlUGF0aDogc3RyaW5nKTogYm9vbGVhbiB7XG4gICAgLy8gTm90ZTogV2UgbmVlZCB0byBzdHJpcCB0aGUgLm5nZmFjdG9yeS4gZmlsZSBwYXRoLFxuICAgIC8vIHNvIHRoaXMgbWV0aG9kIGFsc28gd29ya3MgZm9yIGdlbmVyYXRlZCBmaWxlc1xuICAgIC8vIChmb3Igd2hpY2ggaG9zdC5pc1NvdXJjZUZpbGUgd2lsbCBhbHdheXMgcmV0dXJuIGZhbHNlKS5cbiAgICByZXR1cm4gIXRoaXMuaG9zdC5pc1NvdXJjZUZpbGUoc3RyaXBHZW5lcmF0ZWRGaWxlU3VmZml4KGZpbGVQYXRoKSk7XG4gIH1cblxuICB0b1N1bW1hcnlGaWxlTmFtZShmaWxlUGF0aDogc3RyaW5nLCByZWZlcnJpbmdTcmNGaWxlTmFtZTogc3RyaW5nKSB7XG4gICAgcmV0dXJuIHRoaXMuaG9zdC50b1N1bW1hcnlGaWxlTmFtZShmaWxlUGF0aCwgcmVmZXJyaW5nU3JjRmlsZU5hbWUpO1xuICB9XG5cbiAgZnJvbVN1bW1hcnlGaWxlTmFtZShmaWxlTmFtZTogc3RyaW5nLCByZWZlcnJpbmdMaWJGaWxlTmFtZTogc3RyaW5nKSB7XG4gICAgcmV0dXJuIHRoaXMuaG9zdC5mcm9tU3VtbWFyeUZpbGVOYW1lKGZpbGVOYW1lLCByZWZlcnJpbmdMaWJGaWxlTmFtZSk7XG4gIH1cblxuICByZXNvbHZlU3VtbWFyeShzdGF0aWNTeW1ib2w6IFN0YXRpY1N5bWJvbCk6IFN1bW1hcnk8U3RhdGljU3ltYm9sPnxudWxsIHtcbiAgICBjb25zdCByb290U3ltYm9sID0gc3RhdGljU3ltYm9sLm1lbWJlcnMubGVuZ3RoID9cbiAgICAgICAgdGhpcy5zdGF0aWNTeW1ib2xDYWNoZS5nZXQoc3RhdGljU3ltYm9sLmZpbGVQYXRoLCBzdGF0aWNTeW1ib2wubmFtZSkgOlxuICAgICAgICBzdGF0aWNTeW1ib2w7XG4gICAgbGV0IHN1bW1hcnkgPSB0aGlzLnN1bW1hcnlDYWNoZS5nZXQocm9vdFN5bWJvbCk7XG4gICAgaWYgKCFzdW1tYXJ5KSB7XG4gICAgICB0aGlzLl9sb2FkU3VtbWFyeUZpbGUoc3RhdGljU3ltYm9sLmZpbGVQYXRoKTtcbiAgICAgIHN1bW1hcnkgPSB0aGlzLnN1bW1hcnlDYWNoZS5nZXQoc3RhdGljU3ltYm9sKSAhO1xuICAgIH1cbiAgICByZXR1cm4gKHJvb3RTeW1ib2wgPT09IHN0YXRpY1N5bWJvbCAmJiBzdW1tYXJ5KSB8fCBudWxsO1xuICB9XG5cbiAgZ2V0U3ltYm9sc09mKGZpbGVQYXRoOiBzdHJpbmcpOiBTdGF0aWNTeW1ib2xbXXxudWxsIHtcbiAgICBpZiAodGhpcy5fbG9hZFN1bW1hcnlGaWxlKGZpbGVQYXRoKSkge1xuICAgICAgcmV0dXJuIEFycmF5LmZyb20odGhpcy5zdW1tYXJ5Q2FjaGUua2V5cygpKS5maWx0ZXIoKHN5bWJvbCkgPT4gc3ltYm9sLmZpbGVQYXRoID09PSBmaWxlUGF0aCk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgZ2V0SW1wb3J0QXMoc3RhdGljU3ltYm9sOiBTdGF0aWNTeW1ib2wpOiBTdGF0aWNTeW1ib2wge1xuICAgIHN0YXRpY1N5bWJvbC5hc3NlcnROb01lbWJlcnMoKTtcbiAgICByZXR1cm4gdGhpcy5pbXBvcnRBcy5nZXQoc3RhdGljU3ltYm9sKSAhO1xuICB9XG5cbiAgLyoqXG4gICAqIENvbnZlcnRzIGEgZmlsZSBwYXRoIHRvIGEgbW9kdWxlIG5hbWUgdGhhdCBjYW4gYmUgdXNlZCBhcyBhbiBgaW1wb3J0YC5cbiAgICovXG4gIGdldEtub3duTW9kdWxlTmFtZShpbXBvcnRlZEZpbGVQYXRoOiBzdHJpbmcpOiBzdHJpbmd8bnVsbCB7XG4gICAgcmV0dXJuIHRoaXMua25vd25GaWxlTmFtZVRvTW9kdWxlTmFtZXMuZ2V0KGltcG9ydGVkRmlsZVBhdGgpIHx8IG51bGw7XG4gIH1cblxuICBhZGRTdW1tYXJ5KHN1bW1hcnk6IFN1bW1hcnk8U3RhdGljU3ltYm9sPikgeyB0aGlzLnN1bW1hcnlDYWNoZS5zZXQoc3VtbWFyeS5zeW1ib2wsIHN1bW1hcnkpOyB9XG5cbiAgcHJpdmF0ZSBfbG9hZFN1bW1hcnlGaWxlKGZpbGVQYXRoOiBzdHJpbmcpOiBib29sZWFuIHtcbiAgICBsZXQgaGFzU3VtbWFyeSA9IHRoaXMubG9hZGVkRmlsZVBhdGhzLmdldChmaWxlUGF0aCk7XG4gICAgaWYgKGhhc1N1bW1hcnkgIT0gbnVsbCkge1xuICAgICAgcmV0dXJuIGhhc1N1bW1hcnk7XG4gICAgfVxuICAgIGxldCBqc29uOiBzdHJpbmd8bnVsbCA9IG51bGw7XG4gICAgaWYgKHRoaXMuaXNMaWJyYXJ5RmlsZShmaWxlUGF0aCkpIHtcbiAgICAgIGNvbnN0IHN1bW1hcnlGaWxlUGF0aCA9IHN1bW1hcnlGaWxlTmFtZShmaWxlUGF0aCk7XG4gICAgICB0cnkge1xuICAgICAgICBqc29uID0gdGhpcy5ob3N0LmxvYWRTdW1tYXJ5KHN1bW1hcnlGaWxlUGF0aCk7XG4gICAgICB9IGNhdGNoIChlKSB7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoYEVycm9yIGxvYWRpbmcgc3VtbWFyeSBmaWxlICR7c3VtbWFyeUZpbGVQYXRofWApO1xuICAgICAgICB0aHJvdyBlO1xuICAgICAgfVxuICAgIH1cbiAgICBoYXNTdW1tYXJ5ID0ganNvbiAhPSBudWxsO1xuICAgIHRoaXMubG9hZGVkRmlsZVBhdGhzLnNldChmaWxlUGF0aCwgaGFzU3VtbWFyeSk7XG4gICAgaWYgKGpzb24pIHtcbiAgICAgIGNvbnN0IHttb2R1bGVOYW1lLCBzdW1tYXJpZXMsIGltcG9ydEFzfSA9XG4gICAgICAgICAgZGVzZXJpYWxpemVTdW1tYXJpZXModGhpcy5zdGF0aWNTeW1ib2xDYWNoZSwgdGhpcywgZmlsZVBhdGgsIGpzb24pO1xuICAgICAgc3VtbWFyaWVzLmZvckVhY2goKHN1bW1hcnkpID0+IHRoaXMuc3VtbWFyeUNhY2hlLnNldChzdW1tYXJ5LnN5bWJvbCwgc3VtbWFyeSkpO1xuICAgICAgaWYgKG1vZHVsZU5hbWUpIHtcbiAgICAgICAgdGhpcy5rbm93bkZpbGVOYW1lVG9Nb2R1bGVOYW1lcy5zZXQoZmlsZVBhdGgsIG1vZHVsZU5hbWUpO1xuICAgICAgfVxuICAgICAgaW1wb3J0QXMuZm9yRWFjaCgoaW1wb3J0QXMpID0+IHsgdGhpcy5pbXBvcnRBcy5zZXQoaW1wb3J0QXMuc3ltYm9sLCBpbXBvcnRBcy5pbXBvcnRBcyk7IH0pO1xuICAgIH1cbiAgICByZXR1cm4gaGFzU3VtbWFyeTtcbiAgfVxufVxuIl19
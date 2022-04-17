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
        define("@angular/compiler/src/aot/util", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var STRIP_SRC_FILE_SUFFIXES = /(\.ts|\.d\.ts|\.js|\.jsx|\.tsx)$/;
    var GENERATED_FILE = /\.ngfactory\.|\.ngsummary\./;
    var JIT_SUMMARY_FILE = /\.ngsummary\./;
    var JIT_SUMMARY_NAME = /NgSummary$/;
    function ngfactoryFilePath(filePath, forceSourceFile) {
        if (forceSourceFile === void 0) { forceSourceFile = false; }
        var urlWithSuffix = splitTypescriptSuffix(filePath, forceSourceFile);
        return urlWithSuffix[0] + ".ngfactory" + normalizeGenFileSuffix(urlWithSuffix[1]);
    }
    exports.ngfactoryFilePath = ngfactoryFilePath;
    function stripGeneratedFileSuffix(filePath) {
        return filePath.replace(GENERATED_FILE, '.');
    }
    exports.stripGeneratedFileSuffix = stripGeneratedFileSuffix;
    function isGeneratedFile(filePath) {
        return GENERATED_FILE.test(filePath);
    }
    exports.isGeneratedFile = isGeneratedFile;
    function splitTypescriptSuffix(path, forceSourceFile) {
        if (forceSourceFile === void 0) { forceSourceFile = false; }
        if (path.endsWith('.d.ts')) {
            return [path.slice(0, -5), forceSourceFile ? '.ts' : '.d.ts'];
        }
        var lastDot = path.lastIndexOf('.');
        if (lastDot !== -1) {
            return [path.substring(0, lastDot), path.substring(lastDot)];
        }
        return [path, ''];
    }
    exports.splitTypescriptSuffix = splitTypescriptSuffix;
    function normalizeGenFileSuffix(srcFileSuffix) {
        return srcFileSuffix === '.tsx' ? '.ts' : srcFileSuffix;
    }
    exports.normalizeGenFileSuffix = normalizeGenFileSuffix;
    function summaryFileName(fileName) {
        var fileNameWithoutSuffix = fileName.replace(STRIP_SRC_FILE_SUFFIXES, '');
        return fileNameWithoutSuffix + ".ngsummary.json";
    }
    exports.summaryFileName = summaryFileName;
    function summaryForJitFileName(fileName, forceSourceFile) {
        if (forceSourceFile === void 0) { forceSourceFile = false; }
        var urlWithSuffix = splitTypescriptSuffix(stripGeneratedFileSuffix(fileName), forceSourceFile);
        return urlWithSuffix[0] + ".ngsummary" + urlWithSuffix[1];
    }
    exports.summaryForJitFileName = summaryForJitFileName;
    function stripSummaryForJitFileSuffix(filePath) {
        return filePath.replace(JIT_SUMMARY_FILE, '.');
    }
    exports.stripSummaryForJitFileSuffix = stripSummaryForJitFileSuffix;
    function summaryForJitName(symbolName) {
        return symbolName + "NgSummary";
    }
    exports.summaryForJitName = summaryForJitName;
    function stripSummaryForJitNameSuffix(symbolName) {
        return symbolName.replace(JIT_SUMMARY_NAME, '');
    }
    exports.stripSummaryForJitNameSuffix = stripSummaryForJitNameSuffix;
    var LOWERED_SYMBOL = /\u0275\d+/;
    function isLoweredSymbol(name) {
        return LOWERED_SYMBOL.test(name);
    }
    exports.isLoweredSymbol = isLoweredSymbol;
    function createLoweredSymbol(id) {
        return "\u0275" + id;
    }
    exports.createLoweredSymbol = createLoweredSymbol;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9hb3QvdXRpbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILElBQU0sdUJBQXVCLEdBQUcsa0NBQWtDLENBQUM7SUFDbkUsSUFBTSxjQUFjLEdBQUcsNkJBQTZCLENBQUM7SUFDckQsSUFBTSxnQkFBZ0IsR0FBRyxlQUFlLENBQUM7SUFDekMsSUFBTSxnQkFBZ0IsR0FBRyxZQUFZLENBQUM7SUFFdEMsU0FBZ0IsaUJBQWlCLENBQUMsUUFBZ0IsRUFBRSxlQUF1QjtRQUF2QixnQ0FBQSxFQUFBLHVCQUF1QjtRQUN6RSxJQUFNLGFBQWEsR0FBRyxxQkFBcUIsQ0FBQyxRQUFRLEVBQUUsZUFBZSxDQUFDLENBQUM7UUFDdkUsT0FBVSxhQUFhLENBQUMsQ0FBQyxDQUFDLGtCQUFhLHNCQUFzQixDQUFDLGFBQWEsQ0FBQyxDQUFDLENBQUMsQ0FBRyxDQUFDO0lBQ3BGLENBQUM7SUFIRCw4Q0FHQztJQUVELFNBQWdCLHdCQUF3QixDQUFDLFFBQWdCO1FBQ3ZELE9BQU8sUUFBUSxDQUFDLE9BQU8sQ0FBQyxjQUFjLEVBQUUsR0FBRyxDQUFDLENBQUM7SUFDL0MsQ0FBQztJQUZELDREQUVDO0lBRUQsU0FBZ0IsZUFBZSxDQUFDLFFBQWdCO1FBQzlDLE9BQU8sY0FBYyxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsQ0FBQztJQUN2QyxDQUFDO0lBRkQsMENBRUM7SUFFRCxTQUFnQixxQkFBcUIsQ0FBQyxJQUFZLEVBQUUsZUFBdUI7UUFBdkIsZ0NBQUEsRUFBQSx1QkFBdUI7UUFDekUsSUFBSSxJQUFJLENBQUMsUUFBUSxDQUFDLE9BQU8sQ0FBQyxFQUFFO1lBQzFCLE9BQU8sQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQyxFQUFFLGVBQWUsQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxPQUFPLENBQUMsQ0FBQztTQUMvRDtRQUVELElBQU0sT0FBTyxHQUFHLElBQUksQ0FBQyxXQUFXLENBQUMsR0FBRyxDQUFDLENBQUM7UUFFdEMsSUFBSSxPQUFPLEtBQUssQ0FBQyxDQUFDLEVBQUU7WUFDbEIsT0FBTyxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQyxFQUFFLE9BQU8sQ0FBQyxFQUFFLElBQUksQ0FBQyxTQUFTLENBQUMsT0FBTyxDQUFDLENBQUMsQ0FBQztTQUM5RDtRQUVELE9BQU8sQ0FBQyxJQUFJLEVBQUUsRUFBRSxDQUFDLENBQUM7SUFDcEIsQ0FBQztJQVpELHNEQVlDO0lBRUQsU0FBZ0Isc0JBQXNCLENBQUMsYUFBcUI7UUFDMUQsT0FBTyxhQUFhLEtBQUssTUFBTSxDQUFDLENBQUMsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLGFBQWEsQ0FBQztJQUMxRCxDQUFDO0lBRkQsd0RBRUM7SUFFRCxTQUFnQixlQUFlLENBQUMsUUFBZ0I7UUFDOUMsSUFBTSxxQkFBcUIsR0FBRyxRQUFRLENBQUMsT0FBTyxDQUFDLHVCQUF1QixFQUFFLEVBQUUsQ0FBQyxDQUFDO1FBQzVFLE9BQVUscUJBQXFCLG9CQUFpQixDQUFDO0lBQ25ELENBQUM7SUFIRCwwQ0FHQztJQUVELFNBQWdCLHFCQUFxQixDQUFDLFFBQWdCLEVBQUUsZUFBdUI7UUFBdkIsZ0NBQUEsRUFBQSx1QkFBdUI7UUFDN0UsSUFBTSxhQUFhLEdBQUcscUJBQXFCLENBQUMsd0JBQXdCLENBQUMsUUFBUSxDQUFDLEVBQUUsZUFBZSxDQUFDLENBQUM7UUFDakcsT0FBVSxhQUFhLENBQUMsQ0FBQyxDQUFDLGtCQUFhLGFBQWEsQ0FBQyxDQUFDLENBQUcsQ0FBQztJQUM1RCxDQUFDO0lBSEQsc0RBR0M7SUFFRCxTQUFnQiw0QkFBNEIsQ0FBQyxRQUFnQjtRQUMzRCxPQUFPLFFBQVEsQ0FBQyxPQUFPLENBQUMsZ0JBQWdCLEVBQUUsR0FBRyxDQUFDLENBQUM7SUFDakQsQ0FBQztJQUZELG9FQUVDO0lBRUQsU0FBZ0IsaUJBQWlCLENBQUMsVUFBa0I7UUFDbEQsT0FBVSxVQUFVLGNBQVcsQ0FBQztJQUNsQyxDQUFDO0lBRkQsOENBRUM7SUFFRCxTQUFnQiw0QkFBNEIsQ0FBQyxVQUFrQjtRQUM3RCxPQUFPLFVBQVUsQ0FBQyxPQUFPLENBQUMsZ0JBQWdCLEVBQUUsRUFBRSxDQUFDLENBQUM7SUFDbEQsQ0FBQztJQUZELG9FQUVDO0lBRUQsSUFBTSxjQUFjLEdBQUcsV0FBVyxDQUFDO0lBRW5DLFNBQWdCLGVBQWUsQ0FBQyxJQUFZO1FBQzFDLE9BQU8sY0FBYyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztJQUNuQyxDQUFDO0lBRkQsMENBRUM7SUFFRCxTQUFnQixtQkFBbUIsQ0FBQyxFQUFVO1FBQzVDLE9BQU8sV0FBUyxFQUFJLENBQUM7SUFDdkIsQ0FBQztJQUZELGtEQUVDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5jb25zdCBTVFJJUF9TUkNfRklMRV9TVUZGSVhFUyA9IC8oXFwudHN8XFwuZFxcLnRzfFxcLmpzfFxcLmpzeHxcXC50c3gpJC87XG5jb25zdCBHRU5FUkFURURfRklMRSA9IC9cXC5uZ2ZhY3RvcnlcXC58XFwubmdzdW1tYXJ5XFwuLztcbmNvbnN0IEpJVF9TVU1NQVJZX0ZJTEUgPSAvXFwubmdzdW1tYXJ5XFwuLztcbmNvbnN0IEpJVF9TVU1NQVJZX05BTUUgPSAvTmdTdW1tYXJ5JC87XG5cbmV4cG9ydCBmdW5jdGlvbiBuZ2ZhY3RvcnlGaWxlUGF0aChmaWxlUGF0aDogc3RyaW5nLCBmb3JjZVNvdXJjZUZpbGUgPSBmYWxzZSk6IHN0cmluZyB7XG4gIGNvbnN0IHVybFdpdGhTdWZmaXggPSBzcGxpdFR5cGVzY3JpcHRTdWZmaXgoZmlsZVBhdGgsIGZvcmNlU291cmNlRmlsZSk7XG4gIHJldHVybiBgJHt1cmxXaXRoU3VmZml4WzBdfS5uZ2ZhY3Rvcnkke25vcm1hbGl6ZUdlbkZpbGVTdWZmaXgodXJsV2l0aFN1ZmZpeFsxXSl9YDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHN0cmlwR2VuZXJhdGVkRmlsZVN1ZmZpeChmaWxlUGF0aDogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIGZpbGVQYXRoLnJlcGxhY2UoR0VORVJBVEVEX0ZJTEUsICcuJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc0dlbmVyYXRlZEZpbGUoZmlsZVBhdGg6IHN0cmluZyk6IGJvb2xlYW4ge1xuICByZXR1cm4gR0VORVJBVEVEX0ZJTEUudGVzdChmaWxlUGF0aCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzcGxpdFR5cGVzY3JpcHRTdWZmaXgocGF0aDogc3RyaW5nLCBmb3JjZVNvdXJjZUZpbGUgPSBmYWxzZSk6IHN0cmluZ1tdIHtcbiAgaWYgKHBhdGguZW5kc1dpdGgoJy5kLnRzJykpIHtcbiAgICByZXR1cm4gW3BhdGguc2xpY2UoMCwgLTUpLCBmb3JjZVNvdXJjZUZpbGUgPyAnLnRzJyA6ICcuZC50cyddO1xuICB9XG5cbiAgY29uc3QgbGFzdERvdCA9IHBhdGgubGFzdEluZGV4T2YoJy4nKTtcblxuICBpZiAobGFzdERvdCAhPT0gLTEpIHtcbiAgICByZXR1cm4gW3BhdGguc3Vic3RyaW5nKDAsIGxhc3REb3QpLCBwYXRoLnN1YnN0cmluZyhsYXN0RG90KV07XG4gIH1cblxuICByZXR1cm4gW3BhdGgsICcnXTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIG5vcm1hbGl6ZUdlbkZpbGVTdWZmaXgoc3JjRmlsZVN1ZmZpeDogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIHNyY0ZpbGVTdWZmaXggPT09ICcudHN4JyA/ICcudHMnIDogc3JjRmlsZVN1ZmZpeDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHN1bW1hcnlGaWxlTmFtZShmaWxlTmFtZTogc3RyaW5nKTogc3RyaW5nIHtcbiAgY29uc3QgZmlsZU5hbWVXaXRob3V0U3VmZml4ID0gZmlsZU5hbWUucmVwbGFjZShTVFJJUF9TUkNfRklMRV9TVUZGSVhFUywgJycpO1xuICByZXR1cm4gYCR7ZmlsZU5hbWVXaXRob3V0U3VmZml4fS5uZ3N1bW1hcnkuanNvbmA7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzdW1tYXJ5Rm9ySml0RmlsZU5hbWUoZmlsZU5hbWU6IHN0cmluZywgZm9yY2VTb3VyY2VGaWxlID0gZmFsc2UpOiBzdHJpbmcge1xuICBjb25zdCB1cmxXaXRoU3VmZml4ID0gc3BsaXRUeXBlc2NyaXB0U3VmZml4KHN0cmlwR2VuZXJhdGVkRmlsZVN1ZmZpeChmaWxlTmFtZSksIGZvcmNlU291cmNlRmlsZSk7XG4gIHJldHVybiBgJHt1cmxXaXRoU3VmZml4WzBdfS5uZ3N1bW1hcnkke3VybFdpdGhTdWZmaXhbMV19YDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHN0cmlwU3VtbWFyeUZvckppdEZpbGVTdWZmaXgoZmlsZVBhdGg6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiBmaWxlUGF0aC5yZXBsYWNlKEpJVF9TVU1NQVJZX0ZJTEUsICcuJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzdW1tYXJ5Rm9ySml0TmFtZShzeW1ib2xOYW1lOiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gYCR7c3ltYm9sTmFtZX1OZ1N1bW1hcnlgO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc3RyaXBTdW1tYXJ5Rm9ySml0TmFtZVN1ZmZpeChzeW1ib2xOYW1lOiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gc3ltYm9sTmFtZS5yZXBsYWNlKEpJVF9TVU1NQVJZX05BTUUsICcnKTtcbn1cblxuY29uc3QgTE9XRVJFRF9TWU1CT0wgPSAvXFx1MDI3NVxcZCsvO1xuXG5leHBvcnQgZnVuY3Rpb24gaXNMb3dlcmVkU3ltYm9sKG5hbWU6IHN0cmluZykge1xuICByZXR1cm4gTE9XRVJFRF9TWU1CT0wudGVzdChuYW1lKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZUxvd2VyZWRTeW1ib2woaWQ6IG51bWJlcik6IHN0cmluZyB7XG4gIHJldHVybiBgXFx1MDI3NSR7aWR9YDtcbn1cbiJdfQ==
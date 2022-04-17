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
        define("@angular/compiler/testing/src/output/source_map_util", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var b64 = require('base64-js');
    var SourceMapConsumer = require('source-map').SourceMapConsumer;
    function originalPositionFor(sourceMap, genPosition) {
        var smc = new SourceMapConsumer(sourceMap);
        // Note: We don't return the original object as it also contains a `name` property
        // which is always null and we don't want to include that in our assertions...
        var _a = smc.originalPositionFor(genPosition), line = _a.line, column = _a.column, source = _a.source;
        return { line: line, column: column, source: source };
    }
    exports.originalPositionFor = originalPositionFor;
    function extractSourceMap(source) {
        var idx = source.lastIndexOf('\n//#');
        if (idx == -1)
            return null;
        var smComment = source.slice(idx).split('\n', 2)[1].trim();
        var smB64 = smComment.split('sourceMappingURL=data:application/json;base64,')[1];
        return smB64 ? JSON.parse(decodeB64String(smB64)) : null;
    }
    exports.extractSourceMap = extractSourceMap;
    function decodeB64String(s) {
        return b64.toByteArray(s).reduce(function (s, c) { return s + String.fromCharCode(c); }, '');
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic291cmNlX21hcF91dGlsLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvdGVzdGluZy9zcmMvb3V0cHV0L3NvdXJjZV9tYXBfdXRpbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUdILElBQU0sR0FBRyxHQUFHLE9BQU8sQ0FBQyxXQUFXLENBQUMsQ0FBQztJQUNqQyxJQUFNLGlCQUFpQixHQUFHLE9BQU8sQ0FBQyxZQUFZLENBQUMsQ0FBQyxpQkFBaUIsQ0FBQztJQVFsRSxTQUFnQixtQkFBbUIsQ0FDL0IsU0FBb0IsRUFDcEIsV0FBeUQ7UUFDM0QsSUFBTSxHQUFHLEdBQUcsSUFBSSxpQkFBaUIsQ0FBQyxTQUFTLENBQUMsQ0FBQztRQUM3QyxrRkFBa0Y7UUFDbEYsOEVBQThFO1FBQ3hFLElBQUEseUNBQTZELEVBQTVELGNBQUksRUFBRSxrQkFBTSxFQUFFLGtCQUE4QyxDQUFDO1FBQ3BFLE9BQU8sRUFBQyxJQUFJLE1BQUEsRUFBRSxNQUFNLFFBQUEsRUFBRSxNQUFNLFFBQUEsRUFBQyxDQUFDO0lBQ2hDLENBQUM7SUFSRCxrREFRQztJQUVELFNBQWdCLGdCQUFnQixDQUFDLE1BQWM7UUFDN0MsSUFBSSxHQUFHLEdBQUcsTUFBTSxDQUFDLFdBQVcsQ0FBQyxPQUFPLENBQUMsQ0FBQztRQUN0QyxJQUFJLEdBQUcsSUFBSSxDQUFDLENBQUM7WUFBRSxPQUFPLElBQUksQ0FBQztRQUMzQixJQUFNLFNBQVMsR0FBRyxNQUFNLENBQUMsS0FBSyxDQUFDLEdBQUcsQ0FBQyxDQUFDLEtBQUssQ0FBQyxJQUFJLEVBQUUsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxFQUFFLENBQUM7UUFDN0QsSUFBTSxLQUFLLEdBQUcsU0FBUyxDQUFDLEtBQUssQ0FBQyxnREFBZ0QsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQ25GLE9BQU8sS0FBSyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLGVBQWUsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUM7SUFDM0QsQ0FBQztJQU5ELDRDQU1DO0lBRUQsU0FBUyxlQUFlLENBQUMsQ0FBUztRQUNoQyxPQUFPLEdBQUcsQ0FBQyxXQUFXLENBQUMsQ0FBQyxDQUFDLENBQUMsTUFBTSxDQUFDLFVBQUMsQ0FBUyxFQUFFLENBQVMsSUFBSyxPQUFBLENBQUMsR0FBRyxNQUFNLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQyxFQUExQixDQUEwQixFQUFFLEVBQUUsQ0FBQyxDQUFDO0lBQzdGLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7U291cmNlTWFwfSBmcm9tICdAYW5ndWxhci9jb21waWxlcic7XG5jb25zdCBiNjQgPSByZXF1aXJlKCdiYXNlNjQtanMnKTtcbmNvbnN0IFNvdXJjZU1hcENvbnN1bWVyID0gcmVxdWlyZSgnc291cmNlLW1hcCcpLlNvdXJjZU1hcENvbnN1bWVyO1xuXG5leHBvcnQgaW50ZXJmYWNlIFNvdXJjZUxvY2F0aW9uIHtcbiAgbGluZTogbnVtYmVyO1xuICBjb2x1bW46IG51bWJlcjtcbiAgc291cmNlOiBzdHJpbmc7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBvcmlnaW5hbFBvc2l0aW9uRm9yKFxuICAgIHNvdXJjZU1hcDogU291cmNlTWFwLFxuICAgIGdlblBvc2l0aW9uOiB7bGluZTogbnVtYmVyIHwgbnVsbCwgY29sdW1uOiBudW1iZXIgfCBudWxsfSk6IFNvdXJjZUxvY2F0aW9uIHtcbiAgY29uc3Qgc21jID0gbmV3IFNvdXJjZU1hcENvbnN1bWVyKHNvdXJjZU1hcCk7XG4gIC8vIE5vdGU6IFdlIGRvbid0IHJldHVybiB0aGUgb3JpZ2luYWwgb2JqZWN0IGFzIGl0IGFsc28gY29udGFpbnMgYSBgbmFtZWAgcHJvcGVydHlcbiAgLy8gd2hpY2ggaXMgYWx3YXlzIG51bGwgYW5kIHdlIGRvbid0IHdhbnQgdG8gaW5jbHVkZSB0aGF0IGluIG91ciBhc3NlcnRpb25zLi4uXG4gIGNvbnN0IHtsaW5lLCBjb2x1bW4sIHNvdXJjZX0gPSBzbWMub3JpZ2luYWxQb3NpdGlvbkZvcihnZW5Qb3NpdGlvbik7XG4gIHJldHVybiB7bGluZSwgY29sdW1uLCBzb3VyY2V9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZXh0cmFjdFNvdXJjZU1hcChzb3VyY2U6IHN0cmluZyk6IFNvdXJjZU1hcHxudWxsIHtcbiAgbGV0IGlkeCA9IHNvdXJjZS5sYXN0SW5kZXhPZignXFxuLy8jJyk7XG4gIGlmIChpZHggPT0gLTEpIHJldHVybiBudWxsO1xuICBjb25zdCBzbUNvbW1lbnQgPSBzb3VyY2Uuc2xpY2UoaWR4KS5zcGxpdCgnXFxuJywgMilbMV0udHJpbSgpO1xuICBjb25zdCBzbUI2NCA9IHNtQ29tbWVudC5zcGxpdCgnc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LCcpWzFdO1xuICByZXR1cm4gc21CNjQgPyBKU09OLnBhcnNlKGRlY29kZUI2NFN0cmluZyhzbUI2NCkpIDogbnVsbDtcbn1cblxuZnVuY3Rpb24gZGVjb2RlQjY0U3RyaW5nKHM6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiBiNjQudG9CeXRlQXJyYXkocykucmVkdWNlKChzOiBzdHJpbmcsIGM6IG51bWJlcikgPT4gcyArIFN0cmluZy5mcm9tQ2hhckNvZGUoYyksICcnKTtcbn1cbiJdfQ==
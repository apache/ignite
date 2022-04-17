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
        define("@angular/compiler/src/render3/util", ["require", "exports", "@angular/compiler/src/aot/static_symbol", "@angular/compiler/src/output/output_ast"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var static_symbol_1 = require("@angular/compiler/src/aot/static_symbol");
    var o = require("@angular/compiler/src/output/output_ast");
    /**
     * Convert an object map with `Expression` values into a `LiteralMapExpr`.
     */
    function mapToMapExpression(map) {
        var result = Object.keys(map).map(function (key) { return ({ key: key, value: map[key], quoted: false }); });
        return o.literalMap(result);
    }
    exports.mapToMapExpression = mapToMapExpression;
    /**
     * Convert metadata into an `Expression` in the given `OutputContext`.
     *
     * This operation will handle arrays, references to symbols, or literal `null` or `undefined`.
     */
    function convertMetaToOutput(meta, ctx) {
        if (Array.isArray(meta)) {
            return o.literalArr(meta.map(function (entry) { return convertMetaToOutput(entry, ctx); }));
        }
        if (meta instanceof static_symbol_1.StaticSymbol) {
            return ctx.importExpr(meta);
        }
        if (meta == null) {
            return o.literal(meta);
        }
        throw new Error("Internal error: Unsupported or unknown metadata: " + meta);
    }
    exports.convertMetaToOutput = convertMetaToOutput;
    function typeWithParameters(type, numParams) {
        var params = null;
        if (numParams > 0) {
            params = [];
            for (var i = 0; i < numParams; i++) {
                params.push(o.DYNAMIC_TYPE);
            }
        }
        return o.expressionType(type, null, params);
    }
    exports.typeWithParameters = typeWithParameters;
    var ANIMATE_SYMBOL_PREFIX = '@';
    function prepareSyntheticPropertyName(name) {
        return "" + ANIMATE_SYMBOL_PREFIX + name;
    }
    exports.prepareSyntheticPropertyName = prepareSyntheticPropertyName;
    function prepareSyntheticListenerName(name, phase) {
        return "" + ANIMATE_SYMBOL_PREFIX + name + "." + phase;
    }
    exports.prepareSyntheticListenerName = prepareSyntheticListenerName;
    function isSyntheticPropertyOrListener(name) {
        return name.charAt(0) == ANIMATE_SYMBOL_PREFIX;
    }
    exports.isSyntheticPropertyOrListener = isSyntheticPropertyOrListener;
    function getSyntheticPropertyName(name) {
        // this will strip out listener phase values...
        // @foo.start => @foo
        var i = name.indexOf('.');
        name = i > 0 ? name.substring(0, i) : name;
        if (name.charAt(0) !== ANIMATE_SYMBOL_PREFIX) {
            name = ANIMATE_SYMBOL_PREFIX + name;
        }
        return name;
    }
    exports.getSyntheticPropertyName = getSyntheticPropertyName;
    function prepareSyntheticListenerFunctionName(name, phase) {
        return "animation_" + name + "_" + phase;
    }
    exports.prepareSyntheticListenerFunctionName = prepareSyntheticListenerFunctionName;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9yZW5kZXIzL3V0aWwudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7SUFFSCx5RUFBa0Q7SUFDbEQsMkRBQTBDO0lBRzFDOztPQUVHO0lBQ0gsU0FBZ0Isa0JBQWtCLENBQUMsR0FBa0M7UUFDbkUsSUFBTSxNQUFNLEdBQUcsTUFBTSxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQyxHQUFHLENBQUMsVUFBQSxHQUFHLElBQUksT0FBQSxDQUFDLEVBQUMsR0FBRyxLQUFBLEVBQUUsS0FBSyxFQUFFLEdBQUcsQ0FBQyxHQUFHLENBQUMsRUFBRSxNQUFNLEVBQUUsS0FBSyxFQUFDLENBQUMsRUFBdkMsQ0FBdUMsQ0FBQyxDQUFDO1FBQ3BGLE9BQU8sQ0FBQyxDQUFDLFVBQVUsQ0FBQyxNQUFNLENBQUMsQ0FBQztJQUM5QixDQUFDO0lBSEQsZ0RBR0M7SUFFRDs7OztPQUlHO0lBQ0gsU0FBZ0IsbUJBQW1CLENBQUMsSUFBUyxFQUFFLEdBQWtCO1FBQy9ELElBQUksS0FBSyxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsRUFBRTtZQUN2QixPQUFPLENBQUMsQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQyxVQUFBLEtBQUssSUFBSSxPQUFBLG1CQUFtQixDQUFDLEtBQUssRUFBRSxHQUFHLENBQUMsRUFBL0IsQ0FBK0IsQ0FBQyxDQUFDLENBQUM7U0FDekU7UUFDRCxJQUFJLElBQUksWUFBWSw0QkFBWSxFQUFFO1lBQ2hDLE9BQU8sR0FBRyxDQUFDLFVBQVUsQ0FBQyxJQUFJLENBQUMsQ0FBQztTQUM3QjtRQUNELElBQUksSUFBSSxJQUFJLElBQUksRUFBRTtZQUNoQixPQUFPLENBQUMsQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLENBQUM7U0FDeEI7UUFFRCxNQUFNLElBQUksS0FBSyxDQUFDLHNEQUFvRCxJQUFNLENBQUMsQ0FBQztJQUM5RSxDQUFDO0lBWkQsa0RBWUM7SUFFRCxTQUFnQixrQkFBa0IsQ0FBQyxJQUFrQixFQUFFLFNBQWlCO1FBQ3RFLElBQUksTUFBTSxHQUFrQixJQUFJLENBQUM7UUFDakMsSUFBSSxTQUFTLEdBQUcsQ0FBQyxFQUFFO1lBQ2pCLE1BQU0sR0FBRyxFQUFFLENBQUM7WUFDWixLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsU0FBUyxFQUFFLENBQUMsRUFBRSxFQUFFO2dCQUNsQyxNQUFNLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxZQUFZLENBQUMsQ0FBQzthQUM3QjtTQUNGO1FBQ0QsT0FBTyxDQUFDLENBQUMsY0FBYyxDQUFDLElBQUksRUFBRSxJQUFJLEVBQUUsTUFBTSxDQUFDLENBQUM7SUFDOUMsQ0FBQztJQVRELGdEQVNDO0lBT0QsSUFBTSxxQkFBcUIsR0FBRyxHQUFHLENBQUM7SUFDbEMsU0FBZ0IsNEJBQTRCLENBQUMsSUFBWTtRQUN2RCxPQUFPLEtBQUcscUJBQXFCLEdBQUcsSUFBTSxDQUFDO0lBQzNDLENBQUM7SUFGRCxvRUFFQztJQUVELFNBQWdCLDRCQUE0QixDQUFDLElBQVksRUFBRSxLQUFhO1FBQ3RFLE9BQU8sS0FBRyxxQkFBcUIsR0FBRyxJQUFJLFNBQUksS0FBTyxDQUFDO0lBQ3BELENBQUM7SUFGRCxvRUFFQztJQUVELFNBQWdCLDZCQUE2QixDQUFDLElBQVk7UUFDeEQsT0FBTyxJQUFJLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxJQUFJLHFCQUFxQixDQUFDO0lBQ2pELENBQUM7SUFGRCxzRUFFQztJQUVELFNBQWdCLHdCQUF3QixDQUFDLElBQVk7UUFDbkQsK0NBQStDO1FBQy9DLHFCQUFxQjtRQUNyQixJQUFNLENBQUMsR0FBRyxJQUFJLENBQUMsT0FBTyxDQUFDLEdBQUcsQ0FBQyxDQUFDO1FBQzVCLElBQUksR0FBRyxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDO1FBQzNDLElBQUksSUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsS0FBSyxxQkFBcUIsRUFBRTtZQUM1QyxJQUFJLEdBQUcscUJBQXFCLEdBQUcsSUFBSSxDQUFDO1NBQ3JDO1FBQ0QsT0FBTyxJQUFJLENBQUM7SUFDZCxDQUFDO0lBVEQsNERBU0M7SUFFRCxTQUFnQixvQ0FBb0MsQ0FBQyxJQUFZLEVBQUUsS0FBYTtRQUM5RSxPQUFPLGVBQWEsSUFBSSxTQUFJLEtBQU8sQ0FBQztJQUN0QyxDQUFDO0lBRkQsb0ZBRUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7U3RhdGljU3ltYm9sfSBmcm9tICcuLi9hb3Qvc3RhdGljX3N5bWJvbCc7XG5pbXBvcnQgKiBhcyBvIGZyb20gJy4uL291dHB1dC9vdXRwdXRfYXN0JztcbmltcG9ydCB7T3V0cHV0Q29udGV4dH0gZnJvbSAnLi4vdXRpbCc7XG5cbi8qKlxuICogQ29udmVydCBhbiBvYmplY3QgbWFwIHdpdGggYEV4cHJlc3Npb25gIHZhbHVlcyBpbnRvIGEgYExpdGVyYWxNYXBFeHByYC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIG1hcFRvTWFwRXhwcmVzc2lvbihtYXA6IHtba2V5OiBzdHJpbmddOiBvLkV4cHJlc3Npb259KTogby5MaXRlcmFsTWFwRXhwciB7XG4gIGNvbnN0IHJlc3VsdCA9IE9iamVjdC5rZXlzKG1hcCkubWFwKGtleSA9PiAoe2tleSwgdmFsdWU6IG1hcFtrZXldLCBxdW90ZWQ6IGZhbHNlfSkpO1xuICByZXR1cm4gby5saXRlcmFsTWFwKHJlc3VsdCk7XG59XG5cbi8qKlxuICogQ29udmVydCBtZXRhZGF0YSBpbnRvIGFuIGBFeHByZXNzaW9uYCBpbiB0aGUgZ2l2ZW4gYE91dHB1dENvbnRleHRgLlxuICpcbiAqIFRoaXMgb3BlcmF0aW9uIHdpbGwgaGFuZGxlIGFycmF5cywgcmVmZXJlbmNlcyB0byBzeW1ib2xzLCBvciBsaXRlcmFsIGBudWxsYCBvciBgdW5kZWZpbmVkYC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGNvbnZlcnRNZXRhVG9PdXRwdXQobWV0YTogYW55LCBjdHg6IE91dHB1dENvbnRleHQpOiBvLkV4cHJlc3Npb24ge1xuICBpZiAoQXJyYXkuaXNBcnJheShtZXRhKSkge1xuICAgIHJldHVybiBvLmxpdGVyYWxBcnIobWV0YS5tYXAoZW50cnkgPT4gY29udmVydE1ldGFUb091dHB1dChlbnRyeSwgY3R4KSkpO1xuICB9XG4gIGlmIChtZXRhIGluc3RhbmNlb2YgU3RhdGljU3ltYm9sKSB7XG4gICAgcmV0dXJuIGN0eC5pbXBvcnRFeHByKG1ldGEpO1xuICB9XG4gIGlmIChtZXRhID09IG51bGwpIHtcbiAgICByZXR1cm4gby5saXRlcmFsKG1ldGEpO1xuICB9XG5cbiAgdGhyb3cgbmV3IEVycm9yKGBJbnRlcm5hbCBlcnJvcjogVW5zdXBwb3J0ZWQgb3IgdW5rbm93biBtZXRhZGF0YTogJHttZXRhfWApO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdHlwZVdpdGhQYXJhbWV0ZXJzKHR5cGU6IG8uRXhwcmVzc2lvbiwgbnVtUGFyYW1zOiBudW1iZXIpOiBvLkV4cHJlc3Npb25UeXBlIHtcbiAgbGV0IHBhcmFtczogby5UeXBlW118bnVsbCA9IG51bGw7XG4gIGlmIChudW1QYXJhbXMgPiAwKSB7XG4gICAgcGFyYW1zID0gW107XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBudW1QYXJhbXM7IGkrKykge1xuICAgICAgcGFyYW1zLnB1c2goby5EWU5BTUlDX1RZUEUpO1xuICAgIH1cbiAgfVxuICByZXR1cm4gby5leHByZXNzaW9uVHlwZSh0eXBlLCBudWxsLCBwYXJhbXMpO1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIFIzUmVmZXJlbmNlIHtcbiAgdmFsdWU6IG8uRXhwcmVzc2lvbjtcbiAgdHlwZTogby5FeHByZXNzaW9uO1xufVxuXG5jb25zdCBBTklNQVRFX1NZTUJPTF9QUkVGSVggPSAnQCc7XG5leHBvcnQgZnVuY3Rpb24gcHJlcGFyZVN5bnRoZXRpY1Byb3BlcnR5TmFtZShuYW1lOiBzdHJpbmcpIHtcbiAgcmV0dXJuIGAke0FOSU1BVEVfU1lNQk9MX1BSRUZJWH0ke25hbWV9YDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHByZXBhcmVTeW50aGV0aWNMaXN0ZW5lck5hbWUobmFtZTogc3RyaW5nLCBwaGFzZTogc3RyaW5nKSB7XG4gIHJldHVybiBgJHtBTklNQVRFX1NZTUJPTF9QUkVGSVh9JHtuYW1lfS4ke3BoYXNlfWA7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1N5bnRoZXRpY1Byb3BlcnR5T3JMaXN0ZW5lcihuYW1lOiBzdHJpbmcpIHtcbiAgcmV0dXJuIG5hbWUuY2hhckF0KDApID09IEFOSU1BVEVfU1lNQk9MX1BSRUZJWDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFN5bnRoZXRpY1Byb3BlcnR5TmFtZShuYW1lOiBzdHJpbmcpIHtcbiAgLy8gdGhpcyB3aWxsIHN0cmlwIG91dCBsaXN0ZW5lciBwaGFzZSB2YWx1ZXMuLi5cbiAgLy8gQGZvby5zdGFydCA9PiBAZm9vXG4gIGNvbnN0IGkgPSBuYW1lLmluZGV4T2YoJy4nKTtcbiAgbmFtZSA9IGkgPiAwID8gbmFtZS5zdWJzdHJpbmcoMCwgaSkgOiBuYW1lO1xuICBpZiAobmFtZS5jaGFyQXQoMCkgIT09IEFOSU1BVEVfU1lNQk9MX1BSRUZJWCkge1xuICAgIG5hbWUgPSBBTklNQVRFX1NZTUJPTF9QUkVGSVggKyBuYW1lO1xuICB9XG4gIHJldHVybiBuYW1lO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gcHJlcGFyZVN5bnRoZXRpY0xpc3RlbmVyRnVuY3Rpb25OYW1lKG5hbWU6IHN0cmluZywgcGhhc2U6IHN0cmluZykge1xuICByZXR1cm4gYGFuaW1hdGlvbl8ke25hbWV9XyR7cGhhc2V9YDtcbn0iXX0=
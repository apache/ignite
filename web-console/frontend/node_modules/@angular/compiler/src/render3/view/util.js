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
        define("@angular/compiler/src/render3/view/util", ["require", "exports", "tslib", "@angular/compiler/src/output/output_ast", "@angular/compiler/src/util", "@angular/compiler/src/render3/r3_ast", "@angular/compiler/src/render3/view/i18n/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var o = require("@angular/compiler/src/output/output_ast");
    var util_1 = require("@angular/compiler/src/util");
    var t = require("@angular/compiler/src/render3/r3_ast");
    var util_2 = require("@angular/compiler/src/render3/view/i18n/util");
    /**
     * Checks whether an object key contains potentially unsafe chars, thus the key should be wrapped in
     * quotes. Note: we do not wrap all keys into quotes, as it may have impact on minification and may
     * bot work in some cases when object keys are mangled by minifier.
     *
     * TODO(FW-1136): this is a temporary solution, we need to come up with a better way of working with
     * inputs that contain potentially unsafe chars.
     */
    var UNSAFE_OBJECT_KEY_NAME_REGEXP = /[-.]/;
    /** Name of the temporary to use during data binding */
    exports.TEMPORARY_NAME = '_t';
    /** Name of the context parameter passed into a template function */
    exports.CONTEXT_NAME = 'ctx';
    /** Name of the RenderFlag passed into a template function */
    exports.RENDER_FLAGS = 'rf';
    /** The prefix reference variables */
    exports.REFERENCE_PREFIX = '_r';
    /** The name of the implicit context reference */
    exports.IMPLICIT_REFERENCE = '$implicit';
    /** Non bindable attribute name **/
    exports.NON_BINDABLE_ATTR = 'ngNonBindable';
    /**
     * Creates an allocator for a temporary variable.
     *
     * A variable declaration is added to the statements the first time the allocator is invoked.
     */
    function temporaryAllocator(statements, name) {
        var temp = null;
        return function () {
            if (!temp) {
                statements.push(new o.DeclareVarStmt(exports.TEMPORARY_NAME, undefined, o.DYNAMIC_TYPE));
                temp = o.variable(name);
            }
            return temp;
        };
    }
    exports.temporaryAllocator = temporaryAllocator;
    function unsupported(feature) {
        if (this) {
            throw new Error("Builder " + this.constructor.name + " doesn't support " + feature + " yet");
        }
        throw new Error("Feature " + feature + " is not supported yet");
    }
    exports.unsupported = unsupported;
    function invalid(arg) {
        throw new Error("Invalid state: Visitor " + this.constructor.name + " doesn't handle " + arg.constructor.name);
    }
    exports.invalid = invalid;
    function asLiteral(value) {
        if (Array.isArray(value)) {
            return o.literalArr(value.map(asLiteral));
        }
        return o.literal(value, o.INFERRED_TYPE);
    }
    exports.asLiteral = asLiteral;
    function conditionallyCreateMapObjectLiteral(keys, keepDeclared) {
        if (Object.getOwnPropertyNames(keys).length > 0) {
            return mapToExpression(keys, keepDeclared);
        }
        return null;
    }
    exports.conditionallyCreateMapObjectLiteral = conditionallyCreateMapObjectLiteral;
    function mapToExpression(map, keepDeclared) {
        return o.literalMap(Object.getOwnPropertyNames(map).map(function (key) {
            var _a, _b;
            // canonical syntax: `dirProp: publicProp`
            // if there is no `:`, use dirProp = elProp
            var value = map[key];
            var declaredName;
            var publicName;
            var minifiedName;
            if (Array.isArray(value)) {
                _a = tslib_1.__read(value, 2), publicName = _a[0], declaredName = _a[1];
            }
            else {
                _b = tslib_1.__read(util_1.splitAtColon(key, [key, value]), 2), declaredName = _b[0], publicName = _b[1];
            }
            minifiedName = declaredName;
            return {
                key: minifiedName,
                // put quotes around keys that contain potentially unsafe characters
                quoted: UNSAFE_OBJECT_KEY_NAME_REGEXP.test(minifiedName),
                value: (keepDeclared && publicName !== declaredName) ?
                    o.literalArr([asLiteral(publicName), asLiteral(declaredName)]) :
                    asLiteral(publicName)
            };
        }));
    }
    /**
     *  Remove trailing null nodes as they are implied.
     */
    function trimTrailingNulls(parameters) {
        while (o.isNull(parameters[parameters.length - 1])) {
            parameters.pop();
        }
        return parameters;
    }
    exports.trimTrailingNulls = trimTrailingNulls;
    function getQueryPredicate(query, constantPool) {
        if (Array.isArray(query.predicate)) {
            var predicate_1 = [];
            query.predicate.forEach(function (selector) {
                // Each item in predicates array may contain strings with comma-separated refs
                // (for ex. 'ref, ref1, ..., refN'), thus we extract individual refs and store them
                // as separate array entities
                var selectors = selector.split(',').map(function (token) { return o.literal(token.trim()); });
                predicate_1.push.apply(predicate_1, tslib_1.__spread(selectors));
            });
            return constantPool.getConstLiteral(o.literalArr(predicate_1), true);
        }
        else {
            return query.predicate;
        }
    }
    exports.getQueryPredicate = getQueryPredicate;
    function noop() { }
    exports.noop = noop;
    var DefinitionMap = /** @class */ (function () {
        function DefinitionMap() {
            this.values = [];
        }
        DefinitionMap.prototype.set = function (key, value) {
            if (value) {
                this.values.push({ key: key, value: value, quoted: false });
            }
        };
        DefinitionMap.prototype.toLiteralMap = function () { return o.literalMap(this.values); };
        return DefinitionMap;
    }());
    exports.DefinitionMap = DefinitionMap;
    /**
     * Extract a map of properties to values for a given element or template node, which can be used
     * by the directive matching machinery.
     *
     * @param elOrTpl the element or template in question
     * @return an object set up for directive matching. For attributes on the element/template, this
     * object maps a property name to its (static) value. For any bindings, this map simply maps the
     * property name to an empty string.
     */
    function getAttrsForDirectiveMatching(elOrTpl) {
        var attributesMap = {};
        if (elOrTpl instanceof t.Template && elOrTpl.tagName !== 'ng-template') {
            elOrTpl.templateAttrs.forEach(function (a) { return attributesMap[a.name] = ''; });
        }
        else {
            elOrTpl.attributes.forEach(function (a) {
                if (!util_2.isI18nAttribute(a.name)) {
                    attributesMap[a.name] = a.value;
                }
            });
            elOrTpl.inputs.forEach(function (i) { attributesMap[i.name] = ''; });
            elOrTpl.outputs.forEach(function (o) { attributesMap[o.name] = ''; });
        }
        return attributesMap;
    }
    exports.getAttrsForDirectiveMatching = getAttrsForDirectiveMatching;
    /** Returns a call expression to a chained instruction, e.g. `property(params[0])(params[1])`. */
    function chainedInstruction(reference, calls, span) {
        var expression = o.importExpr(reference, null, span);
        if (calls.length > 0) {
            for (var i = 0; i < calls.length; i++) {
                expression = expression.callFn(calls[i], span);
            }
        }
        else {
            // Add a blank invocation, in case the `calls` array is empty.
            expression = expression.callFn([], span);
        }
        return expression;
    }
    exports.chainedInstruction = chainedInstruction;
    /**
     * Gets the number of arguments expected to be passed to a generated instruction in the case of
     * interpolation instructions.
     * @param interpolation An interpolation ast
     */
    function getInterpolationArgsLength(interpolation) {
        var expressions = interpolation.expressions, strings = interpolation.strings;
        if (expressions.length === 1 && strings.length === 2 && strings[0] === '' && strings[1] === '') {
            // If the interpolation has one interpolated value, but the prefix and suffix are both empty
            // strings, we only pass one argument, to a special instruction like `propertyInterpolate` or
            // `textInterpolate`.
            return 1;
        }
        else {
            return expressions.length + strings.length;
        }
    }
    exports.getInterpolationArgsLength = getInterpolationArgsLength;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9yZW5kZXIzL3ZpZXcvdXRpbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFJSCwyREFBNkM7SUFFN0MsbURBQXdDO0lBQ3hDLHdEQUErQjtJQUcvQixxRUFBNEM7SUFHNUM7Ozs7Ozs7T0FPRztJQUNILElBQU0sNkJBQTZCLEdBQUcsTUFBTSxDQUFDO0lBRTdDLHVEQUF1RDtJQUMxQyxRQUFBLGNBQWMsR0FBRyxJQUFJLENBQUM7SUFFbkMsb0VBQW9FO0lBQ3ZELFFBQUEsWUFBWSxHQUFHLEtBQUssQ0FBQztJQUVsQyw2REFBNkQ7SUFDaEQsUUFBQSxZQUFZLEdBQUcsSUFBSSxDQUFDO0lBRWpDLHFDQUFxQztJQUN4QixRQUFBLGdCQUFnQixHQUFHLElBQUksQ0FBQztJQUVyQyxpREFBaUQ7SUFDcEMsUUFBQSxrQkFBa0IsR0FBRyxXQUFXLENBQUM7SUFFOUMsbUNBQW1DO0lBQ3RCLFFBQUEsaUJBQWlCLEdBQUcsZUFBZSxDQUFDO0lBRWpEOzs7O09BSUc7SUFDSCxTQUFnQixrQkFBa0IsQ0FBQyxVQUF5QixFQUFFLElBQVk7UUFDeEUsSUFBSSxJQUFJLEdBQXVCLElBQUksQ0FBQztRQUNwQyxPQUFPO1lBQ0wsSUFBSSxDQUFDLElBQUksRUFBRTtnQkFDVCxVQUFVLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxDQUFDLGNBQWMsQ0FBQyxzQkFBYyxFQUFFLFNBQVMsRUFBRSxDQUFDLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQztnQkFDakYsSUFBSSxHQUFHLENBQUMsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLENBQUM7YUFDekI7WUFDRCxPQUFPLElBQUksQ0FBQztRQUNkLENBQUMsQ0FBQztJQUNKLENBQUM7SUFURCxnREFTQztJQUdELFNBQWdCLFdBQVcs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LENBQWlCLENBQUM7UUFFckUsSUFBSSxLQUFLLENBQUMsTUFBTSxHQUFHLENBQUMsRUFBRTtZQUNwQixLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsS0FBSyxDQUFDLE1BQU0sRUFBRSxDQUFDLEVBQUUsRUFBRTtnQkFDckMsVUFBVSxHQUFHLFVBQVUsQ0FBQyxNQUFNLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxFQUFFLElBQUksQ0FBQyxDQUFDO2FBQ2hEO1NBQ0Y7YUFBTTtZQUNMLDhEQUE4RDtZQUM5RCxVQUFVLEdBQUcsVUFBVSxDQUFDLE1BQU0sQ0FBQyxFQUFFLEVBQUUsSUFBSSxDQUFDLENBQUM7U0FDMUM7UUFFRCxPQUFPLFVBQVUsQ0FBQztJQUNwQixDQUFDO0lBZEQsZ0RBY0M7SUFFRDs7OztPQUlHO0lBQ0gsU0FBZ0IsMEJBQTBCLENBQUMsYUFBNEI7UUFDOUQsSUFBQSx1Q0FBVyxFQUFFLCtCQUFPLENBQWtCO1FBQzdDLElBQUksV0FBVyxDQUFDLE1BQU0sS0FBSyxDQUFDLElBQUksT0FBTyxDQUFDLE1BQU0sS0FBSyxDQUFDLElBQUksT0FBTyxDQUFDLENBQUMsQ0FBQyxLQUFLLEVBQUUsSUFBSSxPQUFPLENBQUMsQ0FBQyxDQUFDLEtBQUssRUFBRSxFQUFFO1lBQzlGLDRGQUE0RjtZQUM1Riw2RkFBNkY7WUFDN0YscUJBQXFCO1lBQ3JCLE9BQU8sQ0FBQyxDQUFDO1NBQ1Y7YUFBTTtZQUNMLE9BQU8sV0FBVyxDQUFDLE1BQU0sR0FBRyxPQUFPLENBQUMsTUFBTSxDQUFDO1NBQzVDO0lBQ0gsQ0FBQztJQVZELGdFQVVDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbnN0YW50UG9vbH0gZnJvbSAnLi4vLi4vY29uc3RhbnRfcG9vbCc7XG5pbXBvcnQge0ludGVycG9sYXRpb259IGZyb20gJy4uLy4uL2V4cHJlc3Npb25fcGFyc2VyL2FzdCc7XG5pbXBvcnQgKiBhcyBvIGZyb20gJy4uLy4uL291dHB1dC9vdXRwdXRfYXN0JztcbmltcG9ydCB7UGFyc2VTb3VyY2VTcGFufSBmcm9tICcuLi8uLi9wYXJzZV91dGlsJztcbmltcG9ydCB7c3BsaXRBdENvbG9ufSBmcm9tICcuLi8uLi91dGlsJztcbmltcG9ydCAqIGFzIHQgZnJvbSAnLi4vcjNfYXN0JztcblxuaW1wb3J0IHtSM1F1ZXJ5TWV0YWRhdGF9IGZyb20gJy4vYXBpJztcbmltcG9ydCB7aXNJMThuQXR0cmlidXRlfSBmcm9tICcuL2kxOG4vdXRpbCc7XG5cblxuLyoqXG4gKiBDaGVja3Mgd2hldGhlciBhbiBvYmplY3Qga2V5IGNvbnRhaW5zIHBvdGVudGlhbGx5IHVuc2FmZSBjaGFycywgdGh1cyB0aGUga2V5IHNob3VsZCBiZSB3cmFwcGVkIGluXG4gKiBxdW90ZXMuIE5vdGU6IHdlIGRvIG5vdCB3cmFwIGFsbCBrZXlzIGludG8gcXVvdGVzLCBhcyBpdCBtYXkgaGF2ZSBpbXBhY3Qgb24gbWluaWZpY2F0aW9uIGFuZCBtYXlcbiAqIGJvdCB3b3JrIGluIHNvbWUgY2FzZXMgd2hlbiBvYmplY3Qga2V5cyBhcmUgbWFuZ2xlZCBieSBtaW5pZmllci5cbiAqXG4gKiBUT0RPKEZXLTExMzYpOiB0aGlzIGlzIGEgdGVtcG9yYXJ5IHNvbHV0aW9uLCB3ZSBuZWVkIHRvIGNvbWUgdXAgd2l0aCBhIGJldHRlciB3YXkgb2Ygd29ya2luZyB3aXRoXG4gKiBpbnB1dHMgdGhhdCBjb250YWluIHBvdGVudGlhbGx5IHVuc2FmZSBjaGFycy5cbiAqL1xuY29uc3QgVU5TQUZFX09CSkVDVF9LRVlfTkFNRV9SRUdFWFAgPSAvWy0uXS87XG5cbi8qKiBOYW1lIG9mIHRoZSB0ZW1wb3JhcnkgdG8gdXNlIGR1cmluZyBkYXRhIGJpbmRpbmcgKi9cbmV4cG9ydCBjb25zdCBURU1QT1JBUllfTkFNRSA9ICdfdCc7XG5cbi8qKiBOYW1lIG9mIHRoZSBjb250ZXh0IHBhcmFtZXRlciBwYXNzZWQgaW50byBhIHRlbXBsYXRlIGZ1bmN0aW9uICovXG5leHBvcnQgY29uc3QgQ09OVEVYVF9OQU1FID0gJ2N0eCc7XG5cbi8qKiBOYW1lIG9mIHRoZSBSZW5kZXJGbGFnIHBhc3NlZCBpbnRvIGEgdGVtcGxhdGUgZnVuY3Rpb24gKi9cbmV4cG9ydCBjb25zdCBSRU5ERVJfRkxBR1MgPSAncmYnO1xuXG4vKiogVGhlIHByZWZpeCByZWZlcmVuY2UgdmFyaWFibGVzICovXG5leHBvcnQgY29uc3QgUkVGRVJFTkNFX1BSRUZJWCA9ICdfcic7XG5cbi8qKiBUaGUgbmFtZSBvZiB0aGUgaW1wbGljaXQgY29udGV4dCByZWZlcmVuY2UgKi9cbmV4cG9ydCBjb25zdCBJTVBMSUNJVF9SRUZFUkVOQ0UgPSAnJGltcGxpY2l0JztcblxuLyoqIE5vbiBiaW5kYWJsZSBhdHRyaWJ1dGUgbmFtZSAqKi9cbmV4cG9ydCBjb25zdCBOT05fQklOREFCTEVfQVRUUiA9ICduZ05vbkJpbmRhYmxlJztcblxuLyoqXG4gKiBDcmVhdGVzIGFuIGFsbG9jYXRvciBmb3IgYSB0ZW1wb3JhcnkgdmFyaWFibGUuXG4gKlxuICogQSB2YXJpYWJsZSBkZWNsYXJhdGlvbiBpcyBhZGRlZCB0byB0aGUgc3RhdGVtZW50cyB0aGUgZmlyc3QgdGltZSB0aGUgYWxsb2NhdG9yIGlzIGludm9rZWQuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiB0ZW1wb3JhcnlBbGxvY2F0b3Ioc3RhdGVtZW50czogby5TdGF0ZW1lbnRbXSwgbmFtZTogc3RyaW5nKTogKCkgPT4gby5SZWFkVmFyRXhwciB7XG4gIGxldCB0ZW1wOiBvLlJlYWRWYXJFeHByfG51bGwgPSBudWxsO1xuICByZXR1cm4gKCkgPT4ge1xuICAgIGlmICghdGVtcCkge1xuICAgICAgc3RhdGVtZW50cy5wdXNoKG5ldyBvLkRlY2xhcmVWYXJTdG10KFRFTVBPUkFSWV9OQU1FLCB1bmRlZmluZWQsIG8uRFlOQU1JQ19UWVBFKSk7XG4gICAgICB0ZW1wID0gby52YXJpYWJsZShuYW1lKTtcbiAgICB9XG4gICAgcmV0dXJuIHRlbXA7XG4gIH07XG59XG5cblxuZXhwb3J0IGZ1bmN0aW9uIHVuc3VwcG9ydGVkKHRoaXM6IHZvaWR8RnVuY3Rpb24sIGZlYXR1cmU6IHN0cmluZyk6IG5ldmVyIHtcbiAgaWYgKHRoaXMpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoYEJ1aWxkZXIgJHt0aGlzLmNvbnN0cnVjdG9yLm5hbWV9IGRvZXNuJ3Qgc3VwcG9ydCAke2ZlYXR1cmV9IHlldGApO1xuICB9XG4gIHRocm93IG5ldyBFcnJvcihgRmVhdHVyZSAke2ZlYXR1cmV9IGlzIG5vdCBzdXBwb3J0ZWQgeWV0YCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpbnZhbGlkPFQ+KHRoaXM6IHQuVmlzaXRvciwgYXJnOiBvLkV4cHJlc3Npb24gfCBvLlN0YXRlbWVudCB8IHQuTm9kZSk6IG5ldmVyIHtcbiAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgYEludmFsaWQgc3RhdGU6IFZpc2l0b3IgJHt0aGlzLmNvbnN0cnVjdG9yLm5hbWV9IGRvZXNuJ3QgaGFuZGxlICR7YXJnLmNvbnN0cnVjdG9yLm5hbWV9YCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhc0xpdGVyYWwodmFsdWU6IGFueSk6IG8uRXhwcmVzc2lvbiB7XG4gIGlmIChBcnJheS5pc0FycmF5KHZhbHVlKSkge1xuICAgIHJldHVybiBvLmxpdGVyYWxBcnIodmFsdWUubWFwKGFzTGl0ZXJhbCkpO1xuICB9XG4gIHJldHVybiBvLmxpdGVyYWwodmFsdWUsIG8uSU5GRVJSRURfVFlQRSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjb25kaXRpb25hbGx5Q3JlYXRlTWFwT2JqZWN0TGl0ZXJhbChcbiAgICBrZXlzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nIHwgc3RyaW5nW119LCBrZWVwRGVjbGFyZWQ/OiBib29sZWFuKTogby5FeHByZXNzaW9ufG51bGwge1xuICBpZiAoT2JqZWN0LmdldE93blByb3BlcnR5TmFtZXMoa2V5cykubGVuZ3RoID4gMCkge1xuICAgIHJldHVybiBtYXBUb0V4cHJlc3Npb24oa2V5cywga2VlcERlY2xhcmVkKTtcbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cblxuZnVuY3Rpb24gbWFwVG9FeHByZXNzaW9uKFxuICAgIG1hcDoge1trZXk6IHN0cmluZ106IHN0cmluZyB8IHN0cmluZ1tdfSwga2VlcERlY2xhcmVkPzogYm9vbGVhbik6IG8uRXhwcmVzc2lvbiB7XG4gIHJldHVybiBvLmxpdGVyYWxNYXAoT2JqZWN0LmdldE93blByb3BlcnR5TmFtZXMobWFwKS5tYXAoa2V5ID0+IHtcbiAgICAvLyBjYW5vbmljYWwgc3ludGF4OiBgZGlyUHJvcDogcHVibGljUHJvcGBcbiAgICAvLyBpZiB0aGVyZSBpcyBubyBgOmAsIHVzZSBkaXJQcm9wID0gZWxQcm9wXG4gICAgY29uc3QgdmFsdWUgPSBtYXBba2V5XTtcbiAgICBsZXQgZGVjbGFyZWROYW1lOiBzdHJpbmc7XG4gICAgbGV0IHB1YmxpY05hbWU6IHN0cmluZztcbiAgICBsZXQgbWluaWZpZWROYW1lOiBzdHJpbmc7XG4gICAgaWYgKEFycmF5LmlzQXJyYXkodmFsdWUpKSB7XG4gICAgICBbcHVibGljTmFtZSwgZGVjbGFyZWROYW1lXSA9IHZhbHVlO1xuICAgIH0gZWxzZSB7XG4gICAgICBbZGVjbGFyZWROYW1lLCBwdWJsaWNOYW1lXSA9IHNwbGl0QXRDb2xvbihrZXksIFtrZXksIHZhbHVlXSk7XG4gICAgfVxuICAgIG1pbmlmaWVkTmFtZSA9IGRlY2xhcmVkTmFtZTtcbiAgICByZXR1cm4ge1xuICAgICAga2V5OiBtaW5pZmllZE5hbWUsXG4gICAgICAvLyBwdXQgcXVvdGVzIGFyb3VuZCBrZXlzIHRoYXQgY29udGFpbiBwb3RlbnRpYWxseSB1bnNhZmUgY2hhcmFjdGVyc1xuICAgICAgcXVvdGVkOiBVTlNBRkVfT0JKRUNUX0tFWV9OQU1FX1JFR0VYUC50ZXN0KG1pbmlmaWVkTmFtZSksXG4gICAgICB2YWx1ZTogKGtlZXBEZWNsYXJlZCAmJiBwdWJsaWNOYW1lICE9PSBkZWNsYXJlZE5hbWUpID9cbiAgICAgICAgICBvLmxpdGVyYWxBcnIoW2FzTGl0ZXJhbChwdWJsaWNOYW1lKSwgYXNMaXRlcmFsKGRlY2xhcmVkTmFtZSldKSA6XG4gICAgICAgICAgYXNMaXRlcmFsKHB1YmxpY05hbWUpXG4gICAgfTtcbiAgfSkpO1xufVxuXG4vKipcbiAqICBSZW1vdmUgdHJhaWxpbmcgbnVsbCBub2RlcyBhcyB0aGV5IGFyZSBpbXBsaWVkLlxuICovXG5leHBvcnQgZnVuY3Rpb24gdHJpbVRyYWlsaW5nTnVsbHMocGFyYW1ldGVyczogby5FeHByZXNzaW9uW10pOiBvLkV4cHJlc3Npb25bXSB7XG4gIHdoaWxlIChvLmlzTnVsbChwYXJhbWV0ZXJzW3BhcmFtZXRlcnMubGVuZ3RoIC0gMV0pKSB7XG4gICAgcGFyYW1ldGVycy5wb3AoKTtcbiAgfVxuICByZXR1cm4gcGFyYW1ldGVycztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFF1ZXJ5UHJlZGljYXRlKFxuICAgIHF1ZXJ5OiBSM1F1ZXJ5TWV0YWRhdGEsIGNvbnN0YW50UG9vbDogQ29uc3RhbnRQb29sKTogby5FeHByZXNzaW9uIHtcbiAgaWYgKEFycmF5LmlzQXJyYXkocXVlcnkucHJlZGljYXRlKSkge1xuICAgIGxldCBwcmVkaWNhdGU6IG8uRXhwcmVzc2lvbltdID0gW107XG4gICAgcXVlcnkucHJlZGljYXRlLmZvckVhY2goKHNlbGVjdG9yOiBzdHJpbmcpOiB2b2lkID0+IHtcbiAgICAgIC8vIEVhY2ggaXRlbSBpbiBwcmVkaWNhdGVzIGFycmF5IG1heSBjb250YWluIHN0cmluZ3Mgd2l0aCBjb21tYS1zZXBhcmF0ZWQgcmVmc1xuICAgICAgLy8gKGZvciBleC4gJ3JlZiwgcmVmMSwgLi4uLCByZWZOJyksIHRodXMgd2UgZXh0cmFjdCBpbmRpdmlkdWFsIHJlZnMgYW5kIHN0b3JlIHRoZW1cbiAgICAgIC8vIGFzIHNlcGFyYXRlIGFycmF5IGVudGl0aWVzXG4gICAgICBjb25zdCBzZWxlY3RvcnMgPSBzZWxlY3Rvci5zcGxpdCgnLCcpLm1hcCh0b2tlbiA9PiBvLmxpdGVyYWwodG9rZW4udHJpbSgpKSk7XG4gICAgICBwcmVkaWNhdGUucHVzaCguLi5zZWxlY3RvcnMpO1xuICAgIH0pO1xuICAgIHJldHVybiBjb25zdGFudFBvb2wuZ2V0Q29uc3RMaXRlcmFsKG8ubGl0ZXJhbEFycihwcmVkaWNhdGUpLCB0cnVlKTtcbiAgfSBlbHNlIHtcbiAgICByZXR1cm4gcXVlcnkucHJlZGljYXRlO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBub29wKCkge31cblxuZXhwb3J0IGNsYXNzIERlZmluaXRpb25NYXAge1xuICB2YWx1ZXM6IHtrZXk6IHN0cmluZywgcXVvdGVkOiBib29sZWFuLCB2YWx1ZTogby5FeHByZXNzaW9ufVtdID0gW107XG5cbiAgc2V0KGtleTogc3RyaW5nLCB2YWx1ZTogby5FeHByZXNzaW9ufG51bGwpOiB2b2lkIHtcbiAgICBpZiAodmFsdWUpIHtcbiAgICAgIHRoaXMudmFsdWVzLnB1c2goe2tleSwgdmFsdWUsIHF1b3RlZDogZmFsc2V9KTtcbiAgICB9XG4gIH1cblxuICB0b0xpdGVyYWxNYXAoKTogby5MaXRlcmFsTWFwRXhwciB7IHJldHVybiBvLmxpdGVyYWxNYXAodGhpcy52YWx1ZXMpOyB9XG59XG5cbi8qKlxuICogRXh0cmFjdCBhIG1hcCBvZiBwcm9wZXJ0aWVzIHRvIHZhbHVlcyBmb3IgYSBnaXZlbiBlbGVtZW50IG9yIHRlbXBsYXRlIG5vZGUsIHdoaWNoIGNhbiBiZSB1c2VkXG4gKiBieSB0aGUgZGlyZWN0aXZlIG1hdGNoaW5nIG1hY2hpbmVyeS5cbiAqXG4gKiBAcGFyYW0gZWxPclRwbCB0aGUgZWxlbWVudCBvciB0ZW1wbGF0ZSBpbiBxdWVzdGlvblxuICogQHJldHVybiBhbiBvYmplY3Qgc2V0IHVwIGZvciBkaXJlY3RpdmUgbWF0Y2hpbmcuIEZvciBhdHRyaWJ1dGVzIG9uIHRoZSBlbGVtZW50L3RlbXBsYXRlLCB0aGlzXG4gKiBvYmplY3QgbWFwcyBhIHByb3BlcnR5IG5hbWUgdG8gaXRzIChzdGF0aWMpIHZhbHVlLiBGb3IgYW55IGJpbmRpbmdzLCB0aGlzIG1hcCBzaW1wbHkgbWFwcyB0aGVcbiAqIHByb3BlcnR5IG5hbWUgdG8gYW4gZW1wdHkgc3RyaW5nLlxuICovXG5leHBvcnQgZnVuY3Rpb24gZ2V0QXR0cnNGb3JEaXJlY3RpdmVNYXRjaGluZyhlbE9yVHBsOiB0LkVsZW1lbnQgfCB0LlRlbXBsYXRlKTpcbiAgICB7W25hbWU6IHN0cmluZ106IHN0cmluZ30ge1xuICBjb25zdCBhdHRyaWJ1dGVzTWFwOiB7W25hbWU6IHN0cmluZ106IHN0cmluZ30gPSB7fTtcblxuXG4gIGlmIChlbE9yVHBsIGluc3RhbmNlb2YgdC5UZW1wbGF0ZSAmJiBlbE9yVHBsLnRhZ05hbWUgIT09ICduZy10ZW1wbGF0ZScpIHtcbiAgICBlbE9yVHBsLnRlbXBsYXRlQXR0cnMuZm9yRWFjaChhID0+IGF0dHJpYnV0ZXNNYXBbYS5uYW1lXSA9ICcnKTtcbiAgfSBlbHNlIHtcbiAgICBlbE9yVHBsLmF0dHJpYnV0ZXMuZm9yRWFjaChhID0+IHtcbiAgICAgIGlmICghaXNJMThuQXR0cmlidXRlKGEubmFtZSkpIHtcbiAgICAgICAgYXR0cmlidXRlc01hcFthLm5hbWVdID0gYS52YWx1ZTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGVsT3JUcGwuaW5wdXRzLmZvckVhY2goaSA9PiB7IGF0dHJpYnV0ZXNNYXBbaS5uYW1lXSA9ICcnOyB9KTtcbiAgICBlbE9yVHBsLm91dHB1dHMuZm9yRWFjaChvID0+IHsgYXR0cmlidXRlc01hcFtvLm5hbWVdID0gJyc7IH0pO1xuICB9XG5cbiAgcmV0dXJuIGF0dHJpYnV0ZXNNYXA7XG59XG5cbi8qKiBSZXR1cm5zIGEgY2FsbCBleHByZXNzaW9uIHRvIGEgY2hhaW5lZCBpbnN0cnVjdGlvbiwgZS5nLiBgcHJvcGVydHkocGFyYW1zWzBdKShwYXJhbXNbMV0pYC4gKi9cbmV4cG9ydCBmdW5jdGlvbiBjaGFpbmVkSW5zdHJ1Y3Rpb24oXG4gICAgcmVmZXJlbmNlOiBvLkV4dGVybmFsUmVmZXJlbmNlLCBjYWxsczogby5FeHByZXNzaW9uW11bXSwgc3Bhbj86IFBhcnNlU291cmNlU3BhbiB8IG51bGwpIHtcbiAgbGV0IGV4cHJlc3Npb24gPSBvLmltcG9ydEV4cHIocmVmZXJlbmNlLCBudWxsLCBzcGFuKSBhcyBvLkV4cHJlc3Npb247XG5cbiAgaWYgKGNhbGxzLmxlbmd0aCA+IDApIHtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGNhbGxzLmxlbmd0aDsgaSsrKSB7XG4gICAgICBleHByZXNzaW9uID0gZXhwcmVzc2lvbi5jYWxsRm4oY2FsbHNbaV0sIHNwYW4pO1xuICAgIH1cbiAgfSBlbHNlIHtcbiAgICAvLyBBZGQgYSBibGFuayBpbnZvY2F0aW9uLCBpbiBjYXNlIHRoZSBgY2FsbHNgIGFycmF5IGlzIGVtcHR5LlxuICAgIGV4cHJlc3Npb24gPSBleHByZXNzaW9uLmNhbGxGbihbXSwgc3Bhbik7XG4gIH1cblxuICByZXR1cm4gZXhwcmVzc2lvbjtcbn1cblxuLyoqXG4gKiBHZXRzIHRoZSBudW1iZXIgb2YgYXJndW1lbnRzIGV4cGVjdGVkIHRvIGJlIHBhc3NlZCB0byBhIGdlbmVyYXRlZCBpbnN0cnVjdGlvbiBpbiB0aGUgY2FzZSBvZlxuICogaW50ZXJwb2xhdGlvbiBpbnN0cnVjdGlvbnMuXG4gKiBAcGFyYW0gaW50ZXJwb2xhdGlvbiBBbiBpbnRlcnBvbGF0aW9uIGFzdFxuICovXG5leHBvcnQgZnVuY3Rpb24gZ2V0SW50ZXJwb2xhdGlvbkFyZ3NMZW5ndGgoaW50ZXJwb2xhdGlvbjogSW50ZXJwb2xhdGlvbikge1xuICBjb25zdCB7ZXhwcmVzc2lvbnMsIHN0cmluZ3N9ID0gaW50ZXJwb2xhdGlvbjtcbiAgaWYgKGV4cHJlc3Npb25zLmxlbmd0aCA9PT0gMSAmJiBzdHJpbmdzLmxlbmd0aCA9PT0gMiAmJiBzdHJpbmdzWzBdID09PSAnJyAmJiBzdHJpbmdzWzFdID09PSAnJykge1xuICAgIC8vIElmIHRoZSBpbnRlcnBvbGF0aW9uIGhhcyBvbmUgaW50ZXJwb2xhdGVkIHZhbHVlLCBidXQgdGhlIHByZWZpeCBhbmQgc3VmZml4IGFyZSBib3RoIGVtcHR5XG4gICAgLy8gc3RyaW5ncywgd2Ugb25seSBwYXNzIG9uZSBhcmd1bWVudCwgdG8gYSBzcGVjaWFsIGluc3RydWN0aW9uIGxpa2UgYHByb3BlcnR5SW50ZXJwb2xhdGVgIG9yXG4gICAgLy8gYHRleHRJbnRlcnBvbGF0ZWAuXG4gICAgcmV0dXJuIDE7XG4gIH0gZWxzZSB7XG4gICAgcmV0dXJuIGV4cHJlc3Npb25zLmxlbmd0aCArIHN0cmluZ3MubGVuZ3RoO1xuICB9XG59XG4iXX0=
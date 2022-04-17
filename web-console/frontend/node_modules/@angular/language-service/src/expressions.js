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
        define("@angular/language-service/src/expressions", ["require", "exports", "tslib", "@angular/compiler", "@angular/compiler-cli/src/language_services", "@angular/language-service/src/types", "@angular/language-service/src/utils"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var compiler_1 = require("@angular/compiler");
    var language_services_1 = require("@angular/compiler-cli/src/language_services");
    var types_1 = require("@angular/language-service/src/types");
    var utils_1 = require("@angular/language-service/src/utils");
    function findAstAt(ast, position, excludeEmpty) {
        if (excludeEmpty === void 0) { excludeEmpty = false; }
        var path = [];
        var visitor = new /** @class */ (function (_super) {
            tslib_1.__extends(class_1, _super);
            function class_1() {
                return _super !== null && _super.apply(this, arguments) || this;
            }
            class_1.prototype.visit = function (ast) {
                if ((!excludeEmpty || ast.span.start < ast.span.end) && utils_1.inSpan(position, ast.span)) {
                    path.push(ast);
                    compiler_1.visitAstChildren(ast, this);
                }
            };
            return class_1;
        }(compiler_1.NullAstVisitor));
        // We never care about the ASTWithSource node and its visit() method calls its ast's visit so
        // the visit() method above would never see it.
        if (ast instanceof compiler_1.ASTWithSource) {
            ast = ast.ast;
        }
        visitor.visit(ast);
        return new compiler_1.AstPath(path, position);
    }
    function getExpressionCompletions(scope, ast, position, query) {
        var path = findAstAt(ast, position);
        if (path.empty)
            return undefined;
        var tail = path.tail;
        var result = scope;
        function getType(ast) { return new language_services_1.AstType(scope, query, {}).getType(ast); }
        // If the completion request is in a not in a pipe or property access then the global scope
        // (that is the scope of the implicit receiver) is the right scope as the user is typing the
        // beginning of an expression.
        tail.visit({
            visitBinary: function (ast) { },
            visitChain: function (ast) { },
            visitConditional: function (ast) { },
            visitFunctionCall: function (ast) { },
            visitImplicitReceiver: function (ast) { },
            visitInterpolation: function (ast) { result = undefined; },
            visitKeyedRead: function (ast) { },
            visitKeyedWrite: function (ast) { },
            visitLiteralArray: function (ast) { },
            visitLiteralMap: function (ast) { },
            visitLiteralPrimitive: function (ast) { },
            visitMethodCall: function (ast) { },
            visitPipe: function (ast) {
                if (position >= ast.exp.span.end &&
                    (!ast.args || !ast.args.length || position < ast.args[0].span.start)) {
                    // We are in a position a pipe name is expected.
                    result = query.getPipes();
                }
            },
            visitPrefixNot: function (ast) { },
            visitNonNullAssert: function (ast) { },
            visitPropertyRead: function (ast) {
                var receiverType = getType(ast.receiver);
                result = receiverType ? receiverType.members() : scope;
            },
            visitPropertyWrite: function (ast) {
                var receiverType = getType(ast.receiver);
                result = receiverType ? receiverType.members() : scope;
            },
            visitQuote: function (ast) {
                // For a quote, return the members of any (if there are any).
                result = query.getBuiltinType(types_1.BuiltinType.Any).members();
            },
            visitSafeMethodCall: function (ast) {
                var receiverType = getType(ast.receiver);
                result = receiverType ? receiverType.members() : scope;
            },
            visitSafePropertyRead: function (ast) {
                var receiverType = getType(ast.receiver);
                result = receiverType ? receiverType.members() : scope;
            },
        });
        return result && result.values();
    }
    exports.getExpressionCompletions = getExpressionCompletions;
    function getExpressionSymbol(scope, ast, position, query) {
        var path = findAstAt(ast, position, /* excludeEmpty */ true);
        if (path.empty)
            return undefined;
        var tail = path.tail;
        function getType(ast) { return new language_services_1.AstType(scope, query, {}).getType(ast); }
        var symbol = undefined;
        var span = undefined;
        // If the completion request is in a not in a pipe or property access then the global scope
        // (that is the scope of the implicit receiver) is the right scope as the user is typing the
        // beginning of an expression.
        tail.visit({
            visitBinary: function (ast) { },
            visitChain: function (ast) { },
            visitConditional: function (ast) { },
            visitFunctionCall: function (ast) { },
            visitImplicitReceiver: function (ast) { },
            visitInterpolation: function (ast) { },
            visitKeyedRead: function (ast) { },
            visitKeyedWrite: function (ast) { },
            visitLiteralArray: function (ast) { },
            visitLiteralMap: function (ast) { },
            visitLiteralPrimitive: function (ast) { },
            visitMethodCall: function (ast) {
                var receiverType = getType(ast.receiver);
                symbol = receiverType && receiverType.members().get(ast.name);
                span = ast.span;
            },
            visitPipe: function (ast) {
                if (position >= ast.exp.span.end &&
                    (!ast.args || !ast.args.length || position < ast.args[0].span.start)) {
                    // We are in a position a pipe name is expected.
                    var pipes = query.getPipes();
                    if (pipes) {
                        symbol = pipes.get(ast.name);
                        span = ast.span;
                    }
                }
            },
            visitPrefixNot: function (ast) { },
            visitNonNullAssert: function (ast) { },
            visitPropertyRead: function (ast) {
                var receiverType = getType(ast.receiver);
                symbol = receiverType && receiverType.members().get(ast.name);
                span = ast.span;
            },
            visitPropertyWrite: function (ast) {
                var receiverType = getType(ast.receiver);
                symbol = receiverType && receiverType.members().get(ast.name);
                span = ast.span;
            },
            visitQuote: function (ast) { },
            visitSafeMethodCall: function (ast) {
                var receiverType = getType(ast.receiver);
                symbol = receiverType && receiverType.members().get(ast.name);
                span = ast.span;
            },
            visitSafePropertyRead: function (ast) {
                var receiverType = getType(ast.receiver);
                symbol = receiverType && receiverType.members().get(ast.name);
                span = ast.span;
            },
        });
        if (symbol && span) {
            return { symbol: symbol, span: span };
        }
    }
    exports.getExpressionSymbol = getExpressionSymbol;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXhwcmVzc2lvbnMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9sYW5ndWFnZS1zZXJ2aWNlL3NyYy9leHByZXNzaW9ucy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFFSCw4Q0FBK0c7SUFDL0csaUZBQW9FO0lBRXBFLDZEQUE0RTtJQUM1RSw2REFBK0I7SUFJL0IsU0FBUyxTQUFTLENBQUMsR0FBUSxFQUFFLFFBQWdCLEVBQUUsWUFBNkI7UUFBN0IsNkJBQUEsRUFBQSxvQkFBNkI7UUFDMUUsSUFBTSxJQUFJLEdBQVUsRUFBRSxDQUFDO1FBQ3ZCLElBQU0sT0FBTyxHQUFHO1lBQWtCLG1DQUFjO1lBQTVCOztZQU9wQixDQUFDO1lBTkMsdUJBQUssR0FBTCxVQUFNLEdBQVE7Z0JBQ1osSUFBSSxDQUFDLENBQUMsWUFBWSxJQUFJLEdBQUcsQ0FBQyxJQUFJLENBQUMsS0FBSyxHQUFHLEdBQUcsQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLElBQUksY0FBTSxDQUFDLFFBQVEsRUFBRSxHQUFHLENBQUMsSUFBSSxDQUFDLEVBQUU7b0JBQ2xGLElBQUksQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUM7b0JBQ2YsMkJBQWdCLENBQUMsR0FBRyxFQUFFLElBQUksQ0FBQyxDQUFDO2lCQUM3QjtZQUNILENBQUM7WUFDSCxjQUFDO1FBQUQsQ0FBQyxBQVBtQixDQUFjLHlCQUFjLEVBTy9DLENBQUM7UUFFRiw2RkFBNkY7UUFDN0YsK0NBQStDO1FBQy9DLElBQUksR0FBRyxZQUFZLHdCQUFhLEVBQUU7WUFDaEMsR0FBRyxHQUFHLEdBQUcsQ0FBQyxHQUFHLENBQUM7U0FDZjtRQUVELE9BQU8sQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDLENBQUM7UUFFbkIsT0FBTyxJQUFJLGtCQUFXLENBQU0sSUFBSSxFQUFFLFFBQVEsQ0FBQyxDQUFDO0lBQzlDLENB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O1lBQ2xCLENBQUM7U0FDRixDQUFDLENBQUM7UUFFSCxJQUFJLE1BQU0sSUFBSSxJQUFJLEVBQUU7WUFDbEIsT0FBTyxFQUFDLE1BQU0sUUFBQSxFQUFFLElBQUksTUFBQSxFQUFDLENBQUM7U0FDdkI7SUFDSCxDQUFDO0lBdkVELGtEQXVFQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtBU1QsIEFTVFdpdGhTb3VyY2UsIEFzdFBhdGggYXMgQXN0UGF0aEJhc2UsIE51bGxBc3RWaXNpdG9yLCB2aXNpdEFzdENoaWxkcmVufSBmcm9tICdAYW5ndWxhci9jb21waWxlcic7XG5pbXBvcnQge0FzdFR5cGV9IGZyb20gJ0Bhbmd1bGFyL2NvbXBpbGVyLWNsaS9zcmMvbGFuZ3VhZ2Vfc2VydmljZXMnO1xuXG5pbXBvcnQge0J1aWx0aW5UeXBlLCBTcGFuLCBTeW1ib2wsIFN5bWJvbFF1ZXJ5LCBTeW1ib2xUYWJsZX0gZnJvbSAnLi90eXBlcyc7XG5pbXBvcnQge2luU3Bhbn0gZnJvbSAnLi91dGlscyc7XG5cbnR5cGUgQXN0UGF0aCA9IEFzdFBhdGhCYXNlPEFTVD47XG5cbmZ1bmN0aW9uIGZpbmRBc3RBdChhc3Q6IEFTVCwgcG9zaXRpb246IG51bWJlciwgZXhjbHVkZUVtcHR5OiBib29sZWFuID0gZmFsc2UpOiBBc3RQYXRoIHtcbiAgY29uc3QgcGF0aDogQVNUW10gPSBbXTtcbiAgY29uc3QgdmlzaXRvciA9IG5ldyBjbGFzcyBleHRlbmRzIE51bGxBc3RWaXNpdG9yIHtcbiAgICB2aXNpdChhc3Q6IEFTVCkge1xuICAgICAgaWYgKCghZXhjbHVkZUVtcHR5IHx8IGFzdC5zcGFuLnN0YXJ0IDwgYXN0LnNwYW4uZW5kKSAmJiBpblNwYW4ocG9zaXRpb24sIGFzdC5zcGFuKSkge1xuICAgICAgICBwYXRoLnB1c2goYXN0KTtcbiAgICAgICAgdmlzaXRBc3RDaGlsZHJlbihhc3QsIHRoaXMpO1xuICAgICAgfVxuICAgIH1cbiAgfTtcblxuICAvLyBXZSBuZXZlciBjYXJlIGFib3V0IHRoZSBBU1RXaXRoU291cmNlIG5vZGUgYW5kIGl0cyB2aXNpdCgpIG1ldGhvZCBjYWxscyBpdHMgYXN0J3MgdmlzaXQgc29cbiAgLy8gdGhlIHZpc2l0KCkgbWV0aG9kIGFib3ZlIHdvdWxkIG5ldmVyIHNlZSBpdC5cbiAgaWYgKGFzdCBpbnN0YW5jZW9mIEFTVFdpdGhTb3VyY2UpIHtcbiAgICBhc3QgPSBhc3QuYXN0O1xuICB9XG5cbiAgdmlzaXRvci52aXNpdChhc3QpO1xuXG4gIHJldHVybiBuZXcgQXN0UGF0aEJhc2U8QVNUPihwYXRoLCBwb3NpdGlvbik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRFeHByZXNzaW9uQ29tcGxldGlvbnMoXG4gICAgc2NvcGU6IFN5bWJvbFRhYmxlLCBhc3Q6IEFTVCwgcG9zaXRpb246IG51bWJlciwgcXVlcnk6IFN5bWJvbFF1ZXJ5KTogU3ltYm9sW118dW5kZWZpbmVkIHtcbiAgY29uc3QgcGF0aCA9IGZpbmRBc3RBdChhc3QsIHBvc2l0aW9uKTtcbiAgaWYgKHBhdGguZW1wdHkpIHJldHVybiB1bmRlZmluZWQ7XG4gIGNvbnN0IHRhaWwgPSBwYXRoLnRhaWwgITtcbiAgbGV0IHJlc3VsdDogU3ltYm9sVGFibGV8dW5kZWZpbmVkID0gc2NvcGU7XG5cbiAgZnVuY3Rpb24gZ2V0VHlwZShhc3Q6IEFTVCk6IFN5bWJvbCB7IHJldHVybiBuZXcgQXN0VHlwZShzY29wZSwgcXVlcnksIHt9KS5nZXRUeXBlKGFzdCk7IH1cblxuICAvLyBJZiB0aGUgY29tcGxldGlvbiByZXF1ZXN0IGlzIGluIGEgbm90IGluIGEgcGlwZSBvciBwcm9wZXJ0eSBhY2Nlc3MgdGhlbiB0aGUgZ2xvYmFsIHNjb3BlXG4gIC8vICh0aGF0IGlzIHRoZSBzY29wZSBvZiB0aGUgaW1wbGljaXQgcmVjZWl2ZXIpIGlzIHRoZSByaWdodCBzY29wZSBhcyB0aGUgdXNlciBpcyB0eXBpbmcgdGhlXG4gIC8vIGJlZ2lubmluZyBvZiBhbiBleHByZXNzaW9uLlxuICB0YWlsLnZpc2l0KHtcbiAgICB2aXNpdEJpbmFyeShhc3QpIHt9LFxuICAgIHZpc2l0Q2hhaW4oYXN0KSB7fSxcbiAgICB2aXNpdENvbmRpdGlvbmFsKGFzdCkge30sXG4gICAgdmlzaXRGdW5jdGlvbkNhbGwoYXN0KSB7fSxcbiAgICB2aXNpdEltcGxpY2l0UmVjZWl2ZXIoYXN0KSB7fSxcbiAgICB2aXNpdEludGVycG9sYXRpb24oYXN0KSB7IHJlc3VsdCA9IHVuZGVmaW5lZDsgfSxcbiAgICB2aXNpdEtleWVkUmVhZChhc3QpIHt9LFxuICAgIHZpc2l0S2V5ZWRXcml0ZShhc3QpIHt9LFxuICAgIHZpc2l0TGl0ZXJhbEFycmF5KGFzdCkge30sXG4gICAgdmlzaXRMaXRlcmFsTWFwKGFzdCkge30sXG4gICAgdmlzaXRMaXRlcmFsUHJpbWl0aXZlKGFzdCkge30sXG4gICAgdmlzaXRNZXRob2RDYWxsKGFzdCkge30sXG4gICAgdmlzaXRQaXBlKGFzdCkge1xuICAgICAgaWYgKHBvc2l0aW9uID49IGFzdC5leHAuc3Bhbi5lbmQgJiZcbiAgICAgICAgICAoIWFzdC5hcmdzIHx8ICFhc3QuYXJncy5sZW5ndGggfHwgcG9zaXRpb24gPCAoPEFTVD5hc3QuYXJnc1swXSkuc3Bhbi5zdGFydCkpIHtcbiAgICAgICAgLy8gV2UgYXJlIGluIGEgcG9zaXRpb24gYSBwaXBlIG5hbWUgaXMgZXhwZWN0ZWQuXG4gICAgICAgIHJlc3VsdCA9IHF1ZXJ5LmdldFBpcGVzKCk7XG4gICAgICB9XG4gICAgfSxcbiAgICB2aXNpdFByZWZpeE5vdChhc3QpIHt9LFxuICAgIHZpc2l0Tm9uTnVsbEFzc2VydChhc3QpIHt9LFxuICAgIHZpc2l0UHJvcGVydHlSZWFkKGFzdCkge1xuICAgICAgY29uc3QgcmVjZWl2ZXJUeXBlID0gZ2V0VHlwZShhc3QucmVjZWl2ZXIpO1xuICAgICAgcmVzdWx0ID0gcmVjZWl2ZXJUeXBlID8gcmVjZWl2ZXJUeXBlLm1lbWJlcnMoKSA6IHNjb3BlO1xuICAgIH0sXG4gICAgdmlzaXRQcm9wZXJ0eVdyaXRlKGFzdCkge1xuICAgICAgY29uc3QgcmVjZWl2ZXJUeXBlID0gZ2V0VHlwZShhc3QucmVjZWl2ZXIpO1xuICAgICAgcmVzdWx0ID0gcmVjZWl2ZXJUeXBlID8gcmVjZWl2ZXJUeXBlLm1lbWJlcnMoKSA6IHNjb3BlO1xuICAgIH0sXG4gICAgdmlzaXRRdW90ZShhc3QpIHtcbiAgICAgIC8vIEZvciBhIHF1b3RlLCByZXR1cm4gdGhlIG1lbWJlcnMgb2YgYW55IChpZiB0aGVyZSBhcmUgYW55KS5cbiAgICAgIHJlc3VsdCA9IHF1ZXJ5LmdldEJ1aWx0aW5UeXBlKEJ1aWx0aW5UeXBlLkFueSkubWVtYmVycygpO1xuICAgIH0sXG4gICAgdmlzaXRTYWZlTWV0aG9kQ2FsbChhc3QpIHtcbiAgICAgIGNvbnN0IHJlY2VpdmVyVHlwZSA9IGdldFR5cGUoYXN0LnJlY2VpdmVyKTtcbiAgICAgIHJlc3VsdCA9IHJlY2VpdmVyVHlwZSA/IHJlY2VpdmVyVHlwZS5tZW1iZXJzKCkgOiBzY29wZTtcbiAgICB9LFxuICAgIHZpc2l0U2FmZVByb3BlcnR5UmVhZChhc3QpIHtcbiAgICAgIGNvbnN0IHJlY2VpdmVyVHlwZSA9IGdldFR5cGUoYXN0LnJlY2VpdmVyKTtcbiAgICAgIHJlc3VsdCA9IHJlY2VpdmVyVHlwZSA/IHJlY2VpdmVyVHlwZS5tZW1iZXJzKCkgOiBzY29wZTtcbiAgICB9LFxuICB9KTtcblxuICByZXR1cm4gcmVzdWx0ICYmIHJlc3VsdC52YWx1ZXMoKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldEV4cHJlc3Npb25TeW1ib2woXG4gICAgc2NvcGU6IFN5bWJvbFRhYmxlLCBhc3Q6IEFTVCwgcG9zaXRpb246IG51bWJlcixcbiAgICBxdWVyeTogU3ltYm9sUXVlcnkpOiB7c3ltYm9sOiBTeW1ib2wsIHNwYW46IFNwYW59fHVuZGVmaW5lZCB7XG4gIGNvbnN0IHBhdGggPSBmaW5kQXN0QXQoYXN0LCBwb3NpdGlvbiwgLyogZXhjbHVkZUVtcHR5ICovIHRydWUpO1xuICBpZiAocGF0aC5lbXB0eSkgcmV0dXJuIHVuZGVmaW5lZDtcbiAgY29uc3QgdGFpbCA9IHBhdGgudGFpbCAhO1xuXG4gIGZ1bmN0aW9uIGdldFR5cGUoYXN0OiBBU1QpOiBTeW1ib2wgeyByZXR1cm4gbmV3IEFzdFR5cGUoc2NvcGUsIHF1ZXJ5LCB7fSkuZ2V0VHlwZShhc3QpOyB9XG5cbiAgbGV0IHN5bWJvbDogU3ltYm9sfHVuZGVmaW5lZCA9IHVuZGVmaW5lZDtcbiAgbGV0IHNwYW46IFNwYW58dW5kZWZpbmVkID0gdW5kZWZpbmVkO1xuXG4gIC8vIElmIHRoZSBjb21wbGV0aW9uIHJlcXVlc3QgaXMgaW4gYSBub3QgaW4gYSBwaXBlIG9yIHByb3BlcnR5IGFjY2VzcyB0aGVuIHRoZSBnbG9iYWwgc2NvcGVcbiAgLy8gKHRoYXQgaXMgdGhlIHNjb3BlIG9mIHRoZSBpbXBsaWNpdCByZWNlaXZlcikgaXMgdGhlIHJpZ2h0IHNjb3BlIGFzIHRoZSB1c2VyIGlzIHR5cGluZyB0aGVcbiAgLy8gYmVnaW5uaW5nIG9mIGFuIGV4cHJlc3Npb24uXG4gIHRhaWwudmlzaXQoe1xuICAgIHZpc2l0QmluYXJ5KGFzdCkge30sXG4gICAgdmlzaXRDaGFpbihhc3QpIHt9LFxuICAgIHZpc2l0Q29uZGl0aW9uYWwoYXN0KSB7fSxcbiAgICB2aXNpdEZ1bmN0aW9uQ2FsbChhc3QpIHt9LFxuICAgIHZpc2l0SW1wbGljaXRSZWNlaXZlcihhc3QpIHt9LFxuICAgIHZpc2l0SW50ZXJwb2xhdGlvbihhc3QpIHt9LFxuICAgIHZpc2l0S2V5ZWRSZWFkKGFzdCkge30sXG4gICAgdmlzaXRLZXllZFdyaXRlKGFzdCkge30sXG4gICAgdmlzaXRMaXRlcmFsQXJyYXkoYXN0KSB7fSxcbiAgICB2aXNpdExpdGVyYWxNYXAoYXN0KSB7fSxcbiAgICB2aXNpdExpdGVyYWxQcmltaXRpdmUoYXN0KSB7fSxcbiAgICB2aXNpdE1ldGhvZENhbGwoYXN0KSB7XG4gICAgICBjb25zdCByZWNlaXZlclR5cGUgPSBnZXRUeXBlKGFzdC5yZWNlaXZlcik7XG4gICAgICBzeW1ib2wgPSByZWNlaXZlclR5cGUgJiYgcmVjZWl2ZXJUeXBlLm1lbWJlcnMoKS5nZXQoYXN0Lm5hbWUpO1xuICAgICAgc3BhbiA9IGFzdC5zcGFuO1xuICAgIH0sXG4gICAgdmlzaXRQaXBlKGFzdCkge1xuICAgICAgaWYgKHBvc2l0aW9uID49IGFzdC5leHAuc3Bhbi5lbmQgJiZcbiAgICAgICAgICAoIWFzdC5hcmdzIHx8ICFhc3QuYXJncy5sZW5ndGggfHwgcG9zaXRpb24gPCAoPEFTVD5hc3QuYXJnc1swXSkuc3Bhbi5zdGFydCkpIHtcbiAgICAgICAgLy8gV2UgYXJlIGluIGEgcG9zaXRpb24gYSBwaXBlIG5hbWUgaXMgZXhwZWN0ZWQuXG4gICAgICAgIGNvbnN0IHBpcGVzID0gcXVlcnkuZ2V0UGlwZXMoKTtcbiAgICAgICAgaWYgKHBpcGVzKSB7XG4gICAgICAgICAgc3ltYm9sID0gcGlwZXMuZ2V0KGFzdC5uYW1lKTtcbiAgICAgICAgICBzcGFuID0gYXN0LnNwYW47XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9LFxuICAgIHZpc2l0UHJlZml4Tm90KGFzdCkge30sXG4gICAgdmlzaXROb25OdWxsQXNzZXJ0KGFzdCkge30sXG4gICAgdmlzaXRQcm9wZXJ0eVJlYWQoYXN0KSB7XG4gICAgICBjb25zdCByZWNlaXZlclR5cGUgPSBnZXRUeXBlKGFzdC5yZWNlaXZlcik7XG4gICAgICBzeW1ib2wgPSByZWNlaXZlclR5cGUgJiYgcmVjZWl2ZXJUeXBlLm1lbWJlcnMoKS5nZXQoYXN0Lm5hbWUpO1xuICAgICAgc3BhbiA9IGFzdC5zcGFuO1xuICAgIH0sXG4gICAgdmlzaXRQcm9wZXJ0eVdyaXRlKGFzdCkge1xuICAgICAgY29uc3QgcmVjZWl2ZXJUeXBlID0gZ2V0VHlwZShhc3QucmVjZWl2ZXIpO1xuICAgICAgc3ltYm9sID0gcmVjZWl2ZXJUeXBlICYmIHJlY2VpdmVyVHlwZS5tZW1iZXJzKCkuZ2V0KGFzdC5uYW1lKTtcbiAgICAgIHNwYW4gPSBhc3Quc3BhbjtcbiAgICB9LFxuICAgIHZpc2l0UXVvdGUoYXN0KSB7fSxcbiAgICB2aXNpdFNhZmVNZXRob2RDYWxsKGFzdCkge1xuICAgICAgY29uc3QgcmVjZWl2ZXJUeXBlID0gZ2V0VHlwZShhc3QucmVjZWl2ZXIpO1xuICAgICAgc3ltYm9sID0gcmVjZWl2ZXJUeXBlICYmIHJlY2VpdmVyVHlwZS5tZW1iZXJzKCkuZ2V0KGFzdC5uYW1lKTtcbiAgICAgIHNwYW4gPSBhc3Quc3BhbjtcbiAgICB9LFxuICAgIHZpc2l0U2FmZVByb3BlcnR5UmVhZChhc3QpIHtcbiAgICAgIGNvbnN0IHJlY2VpdmVyVHlwZSA9IGdldFR5cGUoYXN0LnJlY2VpdmVyKTtcbiAgICAgIHN5bWJvbCA9IHJlY2VpdmVyVHlwZSAmJiByZWNlaXZlclR5cGUubWVtYmVycygpLmdldChhc3QubmFtZSk7XG4gICAgICBzcGFuID0gYXN0LnNwYW47XG4gICAgfSxcbiAgfSk7XG5cbiAgaWYgKHN5bWJvbCAmJiBzcGFuKSB7XG4gICAgcmV0dXJuIHtzeW1ib2wsIHNwYW59O1xuICB9XG59XG4iXX0=
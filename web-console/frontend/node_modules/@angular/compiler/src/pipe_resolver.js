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
        define("@angular/compiler/src/pipe_resolver", ["require", "exports", "@angular/compiler/src/core", "@angular/compiler/src/directive_resolver", "@angular/compiler/src/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var core_1 = require("@angular/compiler/src/core");
    var directive_resolver_1 = require("@angular/compiler/src/directive_resolver");
    var util_1 = require("@angular/compiler/src/util");
    /**
     * Resolve a `Type` for {@link Pipe}.
     *
     * This interface can be overridden by the application developer to create custom behavior.
     *
     * See {@link Compiler}
     */
    var PipeResolver = /** @class */ (function () {
        function PipeResolver(_reflector) {
            this._reflector = _reflector;
        }
        PipeResolver.prototype.isPipe = function (type) {
            var typeMetadata = this._reflector.annotations(util_1.resolveForwardRef(type));
            return typeMetadata && typeMetadata.some(core_1.createPipe.isTypeOf);
        };
        /**
         * Return {@link Pipe} for a given `Type`.
         */
        PipeResolver.prototype.resolve = function (type, throwIfNotFound) {
            if (throwIfNotFound === void 0) { throwIfNotFound = true; }
            var metas = this._reflector.annotations(util_1.resolveForwardRef(type));
            if (metas) {
                var annotation = directive_resolver_1.findLast(metas, core_1.createPipe.isTypeOf);
                if (annotation) {
                    return annotation;
                }
            }
            if (throwIfNotFound) {
                throw new Error("No Pipe decorator found on " + util_1.stringify(type));
            }
            return null;
        };
        return PipeResolver;
    }());
    exports.PipeResolver = PipeResolver;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGlwZV9yZXNvbHZlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9waXBlX3Jlc29sdmVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBR0gsbURBQThDO0lBQzlDLCtFQUE4QztJQUM5QyxtREFBb0Q7SUFFcEQ7Ozs7OztPQU1HO0lBQ0g7UUFDRSxzQkFBb0IsVUFBNEI7WUFBNUIsZUFBVSxHQUFWLFVBQVUsQ0FBa0I7UUFBRyxDQUFDO1FBRXBELDZCQUFNLEdBQU4sVUFBTyxJQUFVO1lBQ2YsSUFBTSxZQUFZLEdBQUcsSUFBSSxDQUFDLFVBQVUsQ0FBQyxXQUFXLENBQUMsd0JBQWlCLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQztZQUMxRSxPQUFPLFlBQVksSUFBSSxZQUFZLENBQUMsSUFBSSxDQUFDLGlCQUFVLENBQUMsUUFBUSxDQUFDLENBQUM7UUFDaEUsQ0FBQztRQUVEOztXQUVHO1FBQ0gsOEJBQU8sR0FBUCxVQUFRLElBQVUsRUFBRSxlQUFzQjtZQUF0QixnQ0FBQSxFQUFBLHNCQUFzQjtZQUN4QyxJQUFNLEtBQUssR0FBRyxJQUFJLENBQUMsVUFBVSxDQUFDLFdBQVcsQ0FBQyx3QkFBaUIsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDO1lBQ25FLElBQUksS0FBSyxFQUFFO2dCQUNULElBQU0sVUFBVSxHQUFHLDZCQUFRLENBQUMsS0FBSyxFQUFFLGlCQUFVLENBQUMsUUFBUSxDQUFDLENBQUM7Z0JBQ3hELElBQUksVUFBVSxFQUFFO29CQUNkLE9BQU8sVUFBVSxDQUFDO2lCQUNuQjthQUNGO1lBQ0QsSUFBSSxlQUFlLEVBQUU7Z0JBQ25CLE1BQU0sSUFBSSxLQUFLLENBQUMsZ0NBQThCLGdCQUFTLENBQUMsSUFBSSxDQUFHLENBQUMsQ0FBQzthQUNsRTtZQUNELE9BQU8sSUFBSSxDQUFDO1FBQ2QsQ0FBQztRQUNILG1CQUFDO0lBQUQsQ0FBQyxBQXhCRCxJQXdCQztJQXhCWSxvQ0FBWSIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDb21waWxlUmVmbGVjdG9yfSBmcm9tICcuL2NvbXBpbGVfcmVmbGVjdG9yJztcbmltcG9ydCB7UGlwZSwgVHlwZSwgY3JlYXRlUGlwZX0gZnJvbSAnLi9jb3JlJztcbmltcG9ydCB7ZmluZExhc3R9IGZyb20gJy4vZGlyZWN0aXZlX3Jlc29sdmVyJztcbmltcG9ydCB7cmVzb2x2ZUZvcndhcmRSZWYsIHN0cmluZ2lmeX0gZnJvbSAnLi91dGlsJztcblxuLyoqXG4gKiBSZXNvbHZlIGEgYFR5cGVgIGZvciB7QGxpbmsgUGlwZX0uXG4gKlxuICogVGhpcyBpbnRlcmZhY2UgY2FuIGJlIG92ZXJyaWRkZW4gYnkgdGhlIGFwcGxpY2F0aW9uIGRldmVsb3BlciB0byBjcmVhdGUgY3VzdG9tIGJlaGF2aW9yLlxuICpcbiAqIFNlZSB7QGxpbmsgQ29tcGlsZXJ9XG4gKi9cbmV4cG9ydCBjbGFzcyBQaXBlUmVzb2x2ZXIge1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIF9yZWZsZWN0b3I6IENvbXBpbGVSZWZsZWN0b3IpIHt9XG5cbiAgaXNQaXBlKHR5cGU6IFR5cGUpIHtcbiAgICBjb25zdCB0eXBlTWV0YWRhdGEgPSB0aGlzLl9yZWZsZWN0b3IuYW5ub3RhdGlvbnMocmVzb2x2ZUZvcndhcmRSZWYodHlwZSkpO1xuICAgIHJldHVybiB0eXBlTWV0YWRhdGEgJiYgdHlwZU1ldGFkYXRhLnNvbWUoY3JlYXRlUGlwZS5pc1R5cGVPZik7XG4gIH1cblxuICAvKipcbiAgICogUmV0dXJuIHtAbGluayBQaXBlfSBmb3IgYSBnaXZlbiBgVHlwZWAuXG4gICAqL1xuICByZXNvbHZlKHR5cGU6IFR5cGUsIHRocm93SWZOb3RGb3VuZCA9IHRydWUpOiBQaXBlfG51bGwge1xuICAgIGNvbnN0IG1ldGFzID0gdGhpcy5fcmVmbGVjdG9yLmFubm90YXRpb25zKHJlc29sdmVGb3J3YXJkUmVmKHR5cGUpKTtcbiAgICBpZiAobWV0YXMpIHtcbiAgICAgIGNvbnN0IGFubm90YXRpb24gPSBmaW5kTGFzdChtZXRhcywgY3JlYXRlUGlwZS5pc1R5cGVPZik7XG4gICAgICBpZiAoYW5ub3RhdGlvbikge1xuICAgICAgICByZXR1cm4gYW5ub3RhdGlvbjtcbiAgICAgIH1cbiAgICB9XG4gICAgaWYgKHRocm93SWZOb3RGb3VuZCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBObyBQaXBlIGRlY29yYXRvciBmb3VuZCBvbiAke3N0cmluZ2lmeSh0eXBlKX1gKTtcbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbn1cbiJdfQ==
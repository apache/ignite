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
        define("@angular/compiler/testing/src/pipe_resolver_mock", ["require", "exports", "tslib", "@angular/compiler"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var compiler_1 = require("@angular/compiler");
    var MockPipeResolver = /** @class */ (function (_super) {
        tslib_1.__extends(MockPipeResolver, _super);
        function MockPipeResolver(refector) {
            var _this = _super.call(this, refector) || this;
            _this._pipes = new Map();
            return _this;
        }
        /**
         * Overrides the {@link Pipe} for a pipe.
         */
        MockPipeResolver.prototype.setPipe = function (type, metadata) { this._pipes.set(type, metadata); };
        /**
         * Returns the {@link Pipe} for a pipe:
         * - Set the {@link Pipe} to the overridden view when it exists or fallback to the
         * default
         * `PipeResolver`, see `setPipe`.
         */
        MockPipeResolver.prototype.resolve = function (type, throwIfNotFound) {
            if (throwIfNotFound === void 0) { throwIfNotFound = true; }
            var metadata = this._pipes.get(type);
            if (!metadata) {
                metadata = _super.prototype.resolve.call(this, type, throwIfNotFound);
            }
            return metadata;
        };
        return MockPipeResolver;
    }(compiler_1.PipeResolver));
    exports.MockPipeResolver = MockPipeResolver;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGlwZV9yZXNvbHZlcl9tb2NrLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvdGVzdGluZy9zcmMvcGlwZV9yZXNvbHZlcl9tb2NrLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7OztJQUVILDhDQUF1RTtJQUV2RTtRQUFzQyw0Q0FBWTtRQUdoRCwwQkFBWSxRQUEwQjtZQUF0QyxZQUEwQyxrQkFBTSxRQUFRLENBQUMsU0FBRztZQUZwRCxZQUFNLEdBQUcsSUFBSSxHQUFHLEVBQXdCLENBQUM7O1FBRVUsQ0FBQztRQUU1RDs7V0FFRztRQUNILGtDQUFPLEdBQVAsVUFBUSxJQUFlLEVBQUUsUUFBbUIsSUFBVSxJQUFJLENBQUMsTUFBTSxDQUFDLEdBQUcsQ0FBQyxJQUFJLEVBQUUsUUFBUSxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBRXhGOzs7OztXQUtHO1FBQ0gsa0NBQU8sR0FBUCxVQUFRLElBQWUsRUFBRSxlQUFzQjtZQUF0QixnQ0FBQSxFQUFBLHNCQUFzQjtZQUM3QyxJQUFJLFFBQVEsR0FBRyxJQUFJLENBQUMsTUFBTSxDQUFDLEdBQUcsQ0FBQyxJQUFJLENBQUMsQ0FBQztZQUNyQyxJQUFJLENBQUMsUUFBUSxFQUFFO2dCQUNiLFFBQVEsR0FBRyxpQkFBTSxPQUFPLFlBQUMsSUFBSSxFQUFFLGVBQWUsQ0FBRyxDQUFDO2FBQ25EO1lBQ0QsT0FBTyxRQUFRLENBQUM7UUFDbEIsQ0FBQztRQUNILHVCQUFDO0lBQUQsQ0FBQyxBQXZCRCxDQUFzQyx1QkFBWSxHQXVCakQ7SUF2QlksNENBQWdCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBpbGVSZWZsZWN0b3IsIFBpcGVSZXNvbHZlciwgY29yZX0gZnJvbSAnQGFuZ3VsYXIvY29tcGlsZXInO1xuXG5leHBvcnQgY2xhc3MgTW9ja1BpcGVSZXNvbHZlciBleHRlbmRzIFBpcGVSZXNvbHZlciB7XG4gIHByaXZhdGUgX3BpcGVzID0gbmV3IE1hcDxjb3JlLlR5cGUsIGNvcmUuUGlwZT4oKTtcblxuICBjb25zdHJ1Y3RvcihyZWZlY3RvcjogQ29tcGlsZVJlZmxlY3RvcikgeyBzdXBlcihyZWZlY3Rvcik7IH1cblxuICAvKipcbiAgICogT3ZlcnJpZGVzIHRoZSB7QGxpbmsgUGlwZX0gZm9yIGEgcGlwZS5cbiAgICovXG4gIHNldFBpcGUodHlwZTogY29yZS5UeXBlLCBtZXRhZGF0YTogY29yZS5QaXBlKTogdm9pZCB7IHRoaXMuX3BpcGVzLnNldCh0eXBlLCBtZXRhZGF0YSk7IH1cblxuICAvKipcbiAgICogUmV0dXJucyB0aGUge0BsaW5rIFBpcGV9IGZvciBhIHBpcGU6XG4gICAqIC0gU2V0IHRoZSB7QGxpbmsgUGlwZX0gdG8gdGhlIG92ZXJyaWRkZW4gdmlldyB3aGVuIGl0IGV4aXN0cyBvciBmYWxsYmFjayB0byB0aGVcbiAgICogZGVmYXVsdFxuICAgKiBgUGlwZVJlc29sdmVyYCwgc2VlIGBzZXRQaXBlYC5cbiAgICovXG4gIHJlc29sdmUodHlwZTogY29yZS5UeXBlLCB0aHJvd0lmTm90Rm91bmQgPSB0cnVlKTogY29yZS5QaXBlIHtcbiAgICBsZXQgbWV0YWRhdGEgPSB0aGlzLl9waXBlcy5nZXQodHlwZSk7XG4gICAgaWYgKCFtZXRhZGF0YSkge1xuICAgICAgbWV0YWRhdGEgPSBzdXBlci5yZXNvbHZlKHR5cGUsIHRocm93SWZOb3RGb3VuZCkgITtcbiAgICB9XG4gICAgcmV0dXJuIG1ldGFkYXRhO1xuICB9XG59XG4iXX0=
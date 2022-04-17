/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { createPipe } from './core';
import { findLast } from './directive_resolver';
import { resolveForwardRef, stringify } from './util';
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
        var typeMetadata = this._reflector.annotations(resolveForwardRef(type));
        return typeMetadata && typeMetadata.some(createPipe.isTypeOf);
    };
    /**
     * Return {@link Pipe} for a given `Type`.
     */
    PipeResolver.prototype.resolve = function (type, throwIfNotFound) {
        if (throwIfNotFound === void 0) { throwIfNotFound = true; }
        var metas = this._reflector.annotations(resolveForwardRef(type));
        if (metas) {
            var annotation = findLast(metas, createPipe.isTypeOf);
            if (annotation) {
                return annotation;
            }
        }
        if (throwIfNotFound) {
            throw new Error("No Pipe decorator found on " + stringify(type));
        }
        return null;
    };
    return PipeResolver;
}());
export { PipeResolver };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGlwZV9yZXNvbHZlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9waXBlX3Jlc29sdmVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUdILE9BQU8sRUFBYSxVQUFVLEVBQUMsTUFBTSxRQUFRLENBQUM7QUFDOUMsT0FBTyxFQUFDLFFBQVEsRUFBQyxNQUFNLHNCQUFzQixDQUFDO0FBQzlDLE9BQU8sRUFBQyxpQkFBaUIsRUFBRSxTQUFTLEVBQUMsTUFBTSxRQUFRLENBQUM7QUFFcEQ7Ozs7OztHQU1HO0FBQ0g7SUFDRSxzQkFBb0IsVUFBNEI7UUFBNUIsZUFBVSxHQUFWLFVBQVUsQ0FBa0I7SUFBRyxDQUFDO0lBRXBELDZCQUFNLEdBQU4sVUFBTyxJQUFVO1FBQ2YsSUFBTSxZQUFZLEdBQUcsSUFBSSxDQUFDLFVBQVUsQ0FBQyxXQUFXLENBQUMsaUJBQWlCLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQztRQUMxRSxPQUFPLFlBQVksSUFBSSxZQUFZLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxRQUFRLENBQUMsQ0FBQztJQUNoRSxDQUFDO0lBRUQ7O09BRUc7SUFDSCw4QkFBTyxHQUFQLFVBQVEsSUFBVSxFQUFFLGVBQXNCO1FBQXRCLGdDQUFBLEVBQUEsc0JBQXNCO1FBQ3hDLElBQU0sS0FBSyxHQUFHLElBQUksQ0FBQyxVQUFVLENBQUMsV0FBVyxDQUFDLGlCQUFpQixDQUFDLElBQUksQ0FBQyxDQUFDLENBQUM7UUFDbkUsSUFBSSxLQUFLLEVBQUU7WUFDVCxJQUFNLFVBQVUsR0FBRyxRQUFRLENBQUMsS0FBSyxFQUFFLFVBQVUsQ0FBQyxRQUFRLENBQUMsQ0FBQztZQUN4RCxJQUFJLFVBQVUsRUFBRTtnQkFDZCxPQUFPLFVBQVUsQ0FBQzthQUNuQjtTQUNGO1FBQ0QsSUFBSSxlQUFlLEVBQUU7WUFDbkIsTUFBTSxJQUFJLEtBQUssQ0FBQyxnQ0FBOEIsU0FBUyxDQUFDLElBQUksQ0FBRyxDQUFDLENBQUM7U0FDbEU7UUFDRCxPQUFPLElBQUksQ0FBQztJQUNkLENBQUM7SUFDSCxtQkFBQztBQUFELENBQUMsQUF4QkQsSUF3QkMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Q29tcGlsZVJlZmxlY3Rvcn0gZnJvbSAnLi9jb21waWxlX3JlZmxlY3Rvcic7XG5pbXBvcnQge1BpcGUsIFR5cGUsIGNyZWF0ZVBpcGV9IGZyb20gJy4vY29yZSc7XG5pbXBvcnQge2ZpbmRMYXN0fSBmcm9tICcuL2RpcmVjdGl2ZV9yZXNvbHZlcic7XG5pbXBvcnQge3Jlc29sdmVGb3J3YXJkUmVmLCBzdHJpbmdpZnl9IGZyb20gJy4vdXRpbCc7XG5cbi8qKlxuICogUmVzb2x2ZSBhIGBUeXBlYCBmb3Ige0BsaW5rIFBpcGV9LlxuICpcbiAqIFRoaXMgaW50ZXJmYWNlIGNhbiBiZSBvdmVycmlkZGVuIGJ5IHRoZSBhcHBsaWNhdGlvbiBkZXZlbG9wZXIgdG8gY3JlYXRlIGN1c3RvbSBiZWhhdmlvci5cbiAqXG4gKiBTZWUge0BsaW5rIENvbXBpbGVyfVxuICovXG5leHBvcnQgY2xhc3MgUGlwZVJlc29sdmVyIHtcbiAgY29uc3RydWN0b3IocHJpdmF0ZSBfcmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yKSB7fVxuXG4gIGlzUGlwZSh0eXBlOiBUeXBlKSB7XG4gICAgY29uc3QgdHlwZU1ldGFkYXRhID0gdGhpcy5fcmVmbGVjdG9yLmFubm90YXRpb25zKHJlc29sdmVGb3J3YXJkUmVmKHR5cGUpKTtcbiAgICByZXR1cm4gdHlwZU1ldGFkYXRhICYmIHR5cGVNZXRhZGF0YS5zb21lKGNyZWF0ZVBpcGUuaXNUeXBlT2YpO1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHVybiB7QGxpbmsgUGlwZX0gZm9yIGEgZ2l2ZW4gYFR5cGVgLlxuICAgKi9cbiAgcmVzb2x2ZSh0eXBlOiBUeXBlLCB0aHJvd0lmTm90Rm91bmQgPSB0cnVlKTogUGlwZXxudWxsIHtcbiAgICBjb25zdCBtZXRhcyA9IHRoaXMuX3JlZmxlY3Rvci5hbm5vdGF0aW9ucyhyZXNvbHZlRm9yd2FyZFJlZih0eXBlKSk7XG4gICAgaWYgKG1ldGFzKSB7XG4gICAgICBjb25zdCBhbm5vdGF0aW9uID0gZmluZExhc3QobWV0YXMsIGNyZWF0ZVBpcGUuaXNUeXBlT2YpO1xuICAgICAgaWYgKGFubm90YXRpb24pIHtcbiAgICAgICAgcmV0dXJuIGFubm90YXRpb247XG4gICAgICB9XG4gICAgfVxuICAgIGlmICh0aHJvd0lmTm90Rm91bmQpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihgTm8gUGlwZSBkZWNvcmF0b3IgZm91bmQgb24gJHtzdHJpbmdpZnkodHlwZSl9YCk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG59XG4iXX0=
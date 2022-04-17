/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { noSideEffects } from '../util/closure';
/**
 * Adds decorator, constructor, and property metadata to a given type via static metadata fields
 * on the type.
 *
 * These metadata fields can later be read with Angular's `ReflectionCapabilities` API.
 *
 * Calls to `setClassMetadata` can be marked as pure, resulting in the metadata assignments being
 * tree-shaken away during production builds.
 */
export function setClassMetadata(type, decorators, ctorParameters, propDecorators) {
    return noSideEffects(function () {
        var _a;
        var clazz = type;
        // We determine whether a class has its own metadata by taking the metadata from the parent
        // constructor and checking whether it's the same as the subclass metadata below. We can't use
        // `hasOwnProperty` here because it doesn't work correctly in IE10 for static fields that are
        // defined by TS. See https://github.com/angular/angular/pull/28439#issuecomment-459349218.
        var parentPrototype = clazz.prototype ? Object.getPrototypeOf(clazz.prototype) : null;
        var parentConstructor = parentPrototype && parentPrototype.constructor;
        if (decorators !== null) {
            if (clazz.decorators !== undefined &&
                (!parentConstructor || parentConstructor.decorators !== clazz.decorators)) {
                (_a = clazz.decorators).push.apply(_a, tslib_1.__spread(decorators));
            }
            else {
                clazz.decorators = decorators;
            }
        }
        if (ctorParameters !== null) {
            // Rather than merging, clobber the existing parameters. If other projects exist which use
            // tsickle-style annotations and reflect over them in the same way, this could cause issues,
            // but that is vanishingly unlikely.
            clazz.ctorParameters = ctorParameters;
        }
        if (propDecorators !== null) {
            // The property decorator objects are merged as it is possible different fields have different
            // decorator types. Decorators on individual fields are not merged, as it's also incredibly
            // unlikely that a field will be decorated both with an Angular decorator and a non-Angular
            // decorator that's also been downleveled.
            if (clazz.propDecorators !== undefined &&
                (!parentConstructor || parentConstructor.propDecorators !== clazz.propDecorators)) {
                clazz.propDecorators = tslib_1.__assign({}, clazz.propDecorators, propDecorators);
            }
            else {
                clazz.propDecorators = propDecorators;
            }
        }
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWV0YWRhdGEuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL21ldGFkYXRhLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFHSCxPQUFPLEVBQUMsYUFBYSxFQUFDLE1BQU0saUJBQWlCLENBQUM7QUFROUM7Ozs7Ozs7O0dBUUc7QUFDSCxNQUFNLFVBQVUsZ0JBQWdCLENBQzVCLElBQWUsRUFBRSxVQUF3QixFQUFFLGNBQW9DLEVBQy9FLGNBQTZDO0lBQy9DLE9BQU8sYUFBYSxDQUFDOztRQUNuQixJQUFNLEtBQUssR0FBRyxJQUF3QixDQUFDO1FBRXZDLDJGQUEyRjtRQUMzRiw4RkFBOEY7UUFDOUYsNkZBQTZGO1FBQzdGLDJGQUEyRjtRQUMzRixJQUFNLGVBQWUsR0FBRyxLQUFLLENBQUMsU0FBUyxDQUFDLENBQUMsQ0FBQyxNQUFNLENBQUMsY0FBYyxDQUFDLEtBQUssQ0FBQyxTQUFTLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDO1FBQ3hGLElBQU0saUJBQWlCLEdBQTBCLGVBQWUsSUFBSSxlQUFlLENBQUMsV0FBVyxDQUFDO1FBRWhHLElBQUksVUFBVSxLQUFLLElBQUksRUFBRTtZQUN2QixJQUFJLEtBQUssQ0FBQyxVQUFVLEtBQUssU0FBUztnQkFDOUIsQ0FBQyxDQUFDLGlCQUFpQixJQUFJLGlCQUFpQixDQUFDLFVBQVUsS0FBSyxLQUFLLENBQUMsVUFBVSxDQUFDLEVBQUU7Z0JBQzdFLENBQUEsS0FBQSxLQUFLLENBQUMsVUFBVSxDQUFBLENBQUMsSUFBSSw0QkFBSSxVQUFVLEdBQUU7YUFDdEM7aUJBQU07Z0JBQ0wsS0FBSyxDQUFDLFVBQVUsR0FBRyxVQUFVLENBQUM7YUFDL0I7U0FDRjtRQUNELElBQUksY0FBYyxLQUFLLElBQUksRUFBRTtZQUMzQiwwRkFBMEY7WUFDMUYsNEZBQTRGO1lBQzVGLG9DQUFvQztZQUNwQyxLQUFLLENBQUMsY0FBYyxHQUFHLGNBQWMsQ0FBQztTQUN2QztRQUNELElBQUksY0FBYyxLQUFLLElBQUksRUFBRTtZQUMzQiw4RkFBOEY7WUFDOUYsMkZBQTJGO1lBQzNGLDJGQUEyRjtZQUMzRiwwQ0FBMEM7WUFDMUMsSUFBSSxLQUFLLENBQUMsY0FBYyxLQUFLLFNBQVM7Z0JBQ2xDLENBQUMsQ0FBQyxpQkFBaUIsSUFBSSxpQkFBaUIsQ0FBQyxjQUFjLEtBQUssS0FBSyxDQUFDLGNBQWMsQ0FBQyxFQUFFO2dCQUNyRixLQUFLLENBQUMsY0FBYyx3QkFBTyxLQUFLLENBQUMsY0FBYyxFQUFLLGNBQWMsQ0FBQyxDQUFDO2FBQ3JFO2lCQUFNO2dCQUNMLEtBQUssQ0FBQyxjQUFjLEdBQUcsY0FBYyxDQUFDO2FBQ3ZDO1NBQ0Y7SUFDSCxDQUFDLENBQVUsQ0FBQztBQUNkLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7VHlwZX0gZnJvbSAnLi4vaW50ZXJmYWNlL3R5cGUnO1xuaW1wb3J0IHtub1NpZGVFZmZlY3RzfSBmcm9tICcuLi91dGlsL2Nsb3N1cmUnO1xuXG5pbnRlcmZhY2UgVHlwZVdpdGhNZXRhZGF0YSBleHRlbmRzIFR5cGU8YW55PiB7XG4gIGRlY29yYXRvcnM/OiBhbnlbXTtcbiAgY3RvclBhcmFtZXRlcnM/OiAoKSA9PiBhbnlbXTtcbiAgcHJvcERlY29yYXRvcnM/OiB7W2ZpZWxkOiBzdHJpbmddOiBhbnl9O1xufVxuXG4vKipcbiAqIEFkZHMgZGVjb3JhdG9yLCBjb25zdHJ1Y3RvciwgYW5kIHByb3BlcnR5IG1ldGFkYXRhIHRvIGEgZ2l2ZW4gdHlwZSB2aWEgc3RhdGljIG1ldGFkYXRhIGZpZWxkc1xuICogb24gdGhlIHR5cGUuXG4gKlxuICogVGhlc2UgbWV0YWRhdGEgZmllbGRzIGNhbiBsYXRlciBiZSByZWFkIHdpdGggQW5ndWxhcidzIGBSZWZsZWN0aW9uQ2FwYWJpbGl0aWVzYCBBUEkuXG4gKlxuICogQ2FsbHMgdG8gYHNldENsYXNzTWV0YWRhdGFgIGNhbiBiZSBtYXJrZWQgYXMgcHVyZSwgcmVzdWx0aW5nIGluIHRoZSBtZXRhZGF0YSBhc3NpZ25tZW50cyBiZWluZ1xuICogdHJlZS1zaGFrZW4gYXdheSBkdXJpbmcgcHJvZHVjdGlvbiBidWlsZHMuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBzZXRDbGFzc01ldGFkYXRhKFxuICAgIHR5cGU6IFR5cGU8YW55PiwgZGVjb3JhdG9yczogYW55W10gfCBudWxsLCBjdG9yUGFyYW1ldGVyczogKCgpID0+IGFueVtdKSB8IG51bGwsXG4gICAgcHJvcERlY29yYXRvcnM6IHtbZmllbGQ6IHN0cmluZ106IGFueX0gfCBudWxsKTogdm9pZCB7XG4gIHJldHVybiBub1NpZGVFZmZlY3RzKCgpID0+IHtcbiAgICBjb25zdCBjbGF6eiA9IHR5cGUgYXMgVHlwZVdpdGhNZXRhZGF0YTtcblxuICAgIC8vIFdlIGRldGVybWluZSB3aGV0aGVyIGEgY2xhc3MgaGFzIGl0cyBvd24gbWV0YWRhdGEgYnkgdGFraW5nIHRoZSBtZXRhZGF0YSBmcm9tIHRoZSBwYXJlbnRcbiAgICAvLyBjb25zdHJ1Y3RvciBhbmQgY2hlY2tpbmcgd2hldGhlciBpdCdzIHRoZSBzYW1lIGFzIHRoZSBzdWJjbGFzcyBtZXRhZGF0YSBiZWxvdy4gV2UgY2FuJ3QgdXNlXG4gICAgLy8gYGhhc093blByb3BlcnR5YCBoZXJlIGJlY2F1c2UgaXQgZG9lc24ndCB3b3JrIGNvcnJlY3RseSBpbiBJRTEwIGZvciBzdGF0aWMgZmllbGRzIHRoYXQgYXJlXG4gICAgLy8gZGVmaW5lZCBieSBUUy4gU2VlIGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL2FuZ3VsYXIvcHVsbC8yODQzOSNpc3N1ZWNvbW1lbnQtNDU5MzQ5MjE4LlxuICAgIGNvbnN0IHBhcmVudFByb3RvdHlwZSA9IGNsYXp6LnByb3RvdHlwZSA/IE9iamVjdC5nZXRQcm90b3R5cGVPZihjbGF6ei5wcm90b3R5cGUpIDogbnVsbDtcbiAgICBjb25zdCBwYXJlbnRDb25zdHJ1Y3RvcjogVHlwZVdpdGhNZXRhZGF0YXxudWxsID0gcGFyZW50UHJvdG90eXBlICYmIHBhcmVudFByb3RvdHlwZS5jb25zdHJ1Y3RvcjtcblxuICAgIGlmIChkZWNvcmF0b3JzICE9PSBudWxsKSB7XG4gICAgICBpZiAoY2xhenouZGVjb3JhdG9ycyAhPT0gdW5kZWZpbmVkICYmXG4gICAgICAgICAgKCFwYXJlbnRDb25zdHJ1Y3RvciB8fCBwYXJlbnRDb25zdHJ1Y3Rvci5kZWNvcmF0b3JzICE9PSBjbGF6ei5kZWNvcmF0b3JzKSkge1xuICAgICAgICBjbGF6ei5kZWNvcmF0b3JzLnB1c2goLi4uZGVjb3JhdG9ycyk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjbGF6ei5kZWNvcmF0b3JzID0gZGVjb3JhdG9ycztcbiAgICAgIH1cbiAgICB9XG4gICAgaWYgKGN0b3JQYXJhbWV0ZXJzICE9PSBudWxsKSB7XG4gICAgICAvLyBSYXRoZXIgdGhhbiBtZXJnaW5nLCBjbG9iYmVyIHRoZSBleGlzdGluZyBwYXJhbWV0ZXJzLiBJZiBvdGhlciBwcm9qZWN0cyBleGlzdCB3aGljaCB1c2VcbiAgICAgIC8vIHRzaWNrbGUtc3R5bGUgYW5ub3RhdGlvbnMgYW5kIHJlZmxlY3Qgb3ZlciB0aGVtIGluIHRoZSBzYW1lIHdheSwgdGhpcyBjb3VsZCBjYXVzZSBpc3N1ZXMsXG4gICAgICAvLyBidXQgdGhhdCBpcyB2YW5pc2hpbmdseSB1bmxpa2VseS5cbiAgICAgIGNsYXp6LmN0b3JQYXJhbWV0ZXJzID0gY3RvclBhcmFtZXRlcnM7XG4gICAgfVxuICAgIGlmIChwcm9wRGVjb3JhdG9ycyAhPT0gbnVsbCkge1xuICAgICAgLy8gVGhlIHByb3BlcnR5IGRlY29yYXRvciBvYmplY3RzIGFyZSBtZXJnZWQgYXMgaXQgaXMgcG9zc2libGUgZGlmZmVyZW50IGZpZWxkcyBoYXZlIGRpZmZlcmVudFxuICAgICAgLy8gZGVjb3JhdG9yIHR5cGVzLiBEZWNvcmF0b3JzIG9uIGluZGl2aWR1YWwgZmllbGRzIGFyZSBub3QgbWVyZ2VkLCBhcyBpdCdzIGFsc28gaW5jcmVkaWJseVxuICAgICAgLy8gdW5saWtlbHkgdGhhdCBhIGZpZWxkIHdpbGwgYmUgZGVjb3JhdGVkIGJvdGggd2l0aCBhbiBBbmd1bGFyIGRlY29yYXRvciBhbmQgYSBub24tQW5ndWxhclxuICAgICAgLy8gZGVjb3JhdG9yIHRoYXQncyBhbHNvIGJlZW4gZG93bmxldmVsZWQuXG4gICAgICBpZiAoY2xhenoucHJvcERlY29yYXRvcnMgIT09IHVuZGVmaW5lZCAmJlxuICAgICAgICAgICghcGFyZW50Q29uc3RydWN0b3IgfHwgcGFyZW50Q29uc3RydWN0b3IucHJvcERlY29yYXRvcnMgIT09IGNsYXp6LnByb3BEZWNvcmF0b3JzKSkge1xuICAgICAgICBjbGF6ei5wcm9wRGVjb3JhdG9ycyA9IHsuLi5jbGF6ei5wcm9wRGVjb3JhdG9ycywgLi4ucHJvcERlY29yYXRvcnN9O1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY2xhenoucHJvcERlY29yYXRvcnMgPSBwcm9wRGVjb3JhdG9ycztcbiAgICAgIH1cbiAgICB9XG4gIH0pIGFzIG5ldmVyO1xufVxuIl19
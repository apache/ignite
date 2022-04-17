/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { createNgModule } from './core';
import { findLast } from './directive_resolver';
import { stringify } from './util';
/**
 * Resolves types to {@link NgModule}.
 */
export class NgModuleResolver {
    constructor(_reflector) {
        this._reflector = _reflector;
    }
    isNgModule(type) { return this._reflector.annotations(type).some(createNgModule.isTypeOf); }
    resolve(type, throwIfNotFound = true) {
        const ngModuleMeta = findLast(this._reflector.annotations(type), createNgModule.isTypeOf);
        if (ngModuleMeta) {
            return ngModuleMeta;
        }
        else {
            if (throwIfNotFound) {
                throw new Error(`No NgModule metadata found for '${stringify(type)}'.`);
            }
            return null;
        }
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfbW9kdWxlX3Jlc29sdmVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL25nX21vZHVsZV9yZXNvbHZlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFHSCxPQUFPLEVBQWlCLGNBQWMsRUFBQyxNQUFNLFFBQVEsQ0FBQztBQUN0RCxPQUFPLEVBQUMsUUFBUSxFQUFDLE1BQU0sc0JBQXNCLENBQUM7QUFDOUMsT0FBTyxFQUFDLFNBQVMsRUFBQyxNQUFNLFFBQVEsQ0FBQztBQUdqQzs7R0FFRztBQUNILE1BQU0sT0FBTyxnQkFBZ0I7SUFDM0IsWUFBb0IsVUFBNEI7UUFBNUIsZUFBVSxHQUFWLFVBQVUsQ0FBa0I7SUFBRyxDQUFDO0lBRXBELFVBQVUsQ0FBQyxJQUFTLElBQUksT0FBTyxJQUFJLENBQUMsVUFBVSxDQUFDLFdBQVcsQ0FBQyxJQUFJLENBQUMsQ0FBQyxJQUFJLENBQUMsY0FBYyxDQUFDLFFBQVEsQ0FBQyxDQUFDLENBQUMsQ0FBQztJQUVqRyxPQUFPLENBQUMsSUFBVSxFQUFFLGVBQWUsR0FBRyxJQUFJO1FBQ3hDLE1BQU0sWUFBWSxHQUNkLFFBQVEsQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLFdBQVcsQ0FBQyxJQUFJLENBQUMsRUFBRSxjQUFjLENBQUMsUUFBUSxDQUFDLENBQUM7UUFFekUsSUFBSSxZQUFZLEVBQUU7WUFDaEIsT0FBTyxZQUFZLENBQUM7U0FDckI7YUFBTTtZQUNMLElBQUksZUFBZSxFQUFFO2dCQUNuQixNQUFNLElBQUksS0FBSyxDQUFDLG1DQUFtQyxTQUFTLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxDQUFDO2FBQ3pFO1lBQ0QsT0FBTyxJQUFJLENBQUM7U0FDYjtJQUNILENBQUM7Q0FDRiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDb21waWxlUmVmbGVjdG9yfSBmcm9tICcuL2NvbXBpbGVfcmVmbGVjdG9yJztcbmltcG9ydCB7TmdNb2R1bGUsIFR5cGUsIGNyZWF0ZU5nTW9kdWxlfSBmcm9tICcuL2NvcmUnO1xuaW1wb3J0IHtmaW5kTGFzdH0gZnJvbSAnLi9kaXJlY3RpdmVfcmVzb2x2ZXInO1xuaW1wb3J0IHtzdHJpbmdpZnl9IGZyb20gJy4vdXRpbCc7XG5cblxuLyoqXG4gKiBSZXNvbHZlcyB0eXBlcyB0byB7QGxpbmsgTmdNb2R1bGV9LlxuICovXG5leHBvcnQgY2xhc3MgTmdNb2R1bGVSZXNvbHZlciB7XG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgX3JlZmxlY3RvcjogQ29tcGlsZVJlZmxlY3Rvcikge31cblxuICBpc05nTW9kdWxlKHR5cGU6IGFueSkgeyByZXR1cm4gdGhpcy5fcmVmbGVjdG9yLmFubm90YXRpb25zKHR5cGUpLnNvbWUoY3JlYXRlTmdNb2R1bGUuaXNUeXBlT2YpOyB9XG5cbiAgcmVzb2x2ZSh0eXBlOiBUeXBlLCB0aHJvd0lmTm90Rm91bmQgPSB0cnVlKTogTmdNb2R1bGV8bnVsbCB7XG4gICAgY29uc3QgbmdNb2R1bGVNZXRhOiBOZ01vZHVsZSA9XG4gICAgICAgIGZpbmRMYXN0KHRoaXMuX3JlZmxlY3Rvci5hbm5vdGF0aW9ucyh0eXBlKSwgY3JlYXRlTmdNb2R1bGUuaXNUeXBlT2YpO1xuXG4gICAgaWYgKG5nTW9kdWxlTWV0YSkge1xuICAgICAgcmV0dXJuIG5nTW9kdWxlTWV0YTtcbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKHRocm93SWZOb3RGb3VuZCkge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoYE5vIE5nTW9kdWxlIG1ldGFkYXRhIGZvdW5kIGZvciAnJHtzdHJpbmdpZnkodHlwZSl9Jy5gKTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cbiAgfVxufVxuIl19
/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { AbstractControlDirective } from './abstract_control_directive';
/**
 * \@description
 * A base class for directives that contain multiple registered instances of `NgControl`.
 * Only used by the forms module.
 *
 * \@publicApi
 * @abstract
 */
export class ControlContainer extends AbstractControlDirective {
    /**
     * \@description
     * The top-level form directive for the control.
     * @return {?}
     */
    get formDirective() { return null; }
    /**
     * \@description
     * The path to this group.
     * @return {?}
     */
    get path() { return null; }
}
if (false) {
    /**
     * \@description
     * The name for the control
     * @type {?}
     */
    ControlContainer.prototype.name;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29udHJvbF9jb250YWluZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9mb3Jtcy9zcmMvZGlyZWN0aXZlcy9jb250cm9sX2NvbnRhaW5lci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyx3QkFBd0IsRUFBQyxNQUFNLDhCQUE4QixDQUFDOzs7Ozs7Ozs7QUFXdEUsTUFBTSxPQUFnQixnQkFBaUIsU0FBUSx3QkFBd0I7Ozs7OztJQVlyRSxJQUFJLGFBQWEsS0FBZ0IsT0FBTyxJQUFJLENBQUMsQ0FBQyxDQUFDOzs7Ozs7SUFNL0MsSUFBSSxJQUFJLEtBQW9CLE9BQU8sSUFBSSxDQUFDLENBQUMsQ0FBQztDQUMzQzs7Ozs7OztJQWJDLGdDQUFlIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0Fic3RyYWN0Q29udHJvbERpcmVjdGl2ZX0gZnJvbSAnLi9hYnN0cmFjdF9jb250cm9sX2RpcmVjdGl2ZSc7XG5pbXBvcnQge0Zvcm19IGZyb20gJy4vZm9ybV9pbnRlcmZhY2UnO1xuXG5cbi8qKlxuICogQGRlc2NyaXB0aW9uXG4gKiBBIGJhc2UgY2xhc3MgZm9yIGRpcmVjdGl2ZXMgdGhhdCBjb250YWluIG11bHRpcGxlIHJlZ2lzdGVyZWQgaW5zdGFuY2VzIG9mIGBOZ0NvbnRyb2xgLlxuICogT25seSB1c2VkIGJ5IHRoZSBmb3JtcyBtb2R1bGUuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgQ29udHJvbENvbnRhaW5lciBleHRlbmRzIEFic3RyYWN0Q29udHJvbERpcmVjdGl2ZSB7XG4gIC8qKlxuICAgKiBAZGVzY3JpcHRpb25cbiAgICogVGhlIG5hbWUgZm9yIHRoZSBjb250cm9sXG4gICAqL1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgbmFtZSAhOiBzdHJpbmc7XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBUaGUgdG9wLWxldmVsIGZvcm0gZGlyZWN0aXZlIGZvciB0aGUgY29udHJvbC5cbiAgICovXG4gIGdldCBmb3JtRGlyZWN0aXZlKCk6IEZvcm18bnVsbCB7IHJldHVybiBudWxsOyB9XG5cbiAgLyoqXG4gICAqIEBkZXNjcmlwdGlvblxuICAgKiBUaGUgcGF0aCB0byB0aGlzIGdyb3VwLlxuICAgKi9cbiAgZ2V0IHBhdGgoKTogc3RyaW5nW118bnVsbCB7IHJldHVybiBudWxsOyB9XG59XG4iXX0=
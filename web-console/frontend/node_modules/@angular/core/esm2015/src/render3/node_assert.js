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
import { assertDefined, assertEqual } from '../util/assert';
/**
 * @param {?} tNode
 * @param {?} type
 * @return {?}
 */
export function assertNodeType(tNode, type) {
    assertDefined(tNode, 'should be called with a TNode');
    assertEqual(tNode.type, type, `should be a ${typeName(type)}`);
}
/**
 * @param {?} tNode
 * @param {...?} types
 * @return {?}
 */
export function assertNodeOfPossibleTypes(tNode, ...types) {
    assertDefined(tNode, 'should be called with a TNode');
    /** @type {?} */
    const found = types.some((/**
     * @param {?} type
     * @return {?}
     */
    type => tNode.type === type));
    assertEqual(found, true, `Should be one of ${types.map(typeName).join(', ')} but got ${typeName(tNode.type)}`);
}
/**
 * @param {?} type
 * @return {?}
 */
function typeName(type) {
    if (type == 1 /* Projection */)
        return 'Projection';
    if (type == 0 /* Container */)
        return 'Container';
    if (type == 2 /* View */)
        return 'View';
    if (type == 3 /* Element */)
        return 'Element';
    if (type == 4 /* ElementContainer */)
        return 'ElementContainer';
    return '<unknown>';
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibm9kZV9hc3NlcnQuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL25vZGVfYXNzZXJ0LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLGFBQWEsRUFBRSxXQUFXLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQzs7Ozs7O0FBRzFELE1BQU0sVUFBVSxjQUFjLENBQUMsS0FBWSxFQUFFLElBQWU7SUFDMUQsYUFBYSxDQUFDLEtBQUssRUFBRSwrQkFBK0IsQ0FBQyxDQUFDO0lBQ3RELFdBQVcsQ0FBQyxLQUFLLENBQUMsSUFBSSxFQUFFLElBQUksRUFBRSxlQUFlLFFBQVEsQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLENBQUM7QUFDakUsQ0FBQzs7Ozs7O0FBRUQsTUFBTSxVQUFVLHlCQUF5QixDQUFDLEtBQVksRUFBRSxHQUFHLEtBQWtCO0lBQzNFLGFBQWEsQ0FBQyxLQUFLLEVBQUUsK0JBQStCLENBQUMsQ0FBQzs7VUFDaEQsS0FBSyxHQUFHLEtBQUssQ0FBQyxJQUFJOzs7O0lBQUMsSUFBSSxDQUFDLEVBQUUsQ0FBQyxLQUFLLENBQUMsSUFBSSxLQUFLLElBQUksRUFBQztJQUNyRCxXQUFXLENBQ1AsS0FBSyxFQUFFLElBQUksRUFDWCxvQkFBb0IsS0FBSyxDQUFDLEdBQUcsQ0FBQyxRQUFRLENBQUMsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLFlBQVksUUFBUSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLENBQUM7QUFDNUYsQ0FBQzs7Ozs7QUFFRCxTQUFTLFFBQVEsQ0FBQyxJQUFlO0lBQy9CLElBQUksSUFBSSxzQkFBd0I7UUFBRSxPQUFPLFlBQVksQ0FBQztJQUN0RCxJQUFJLElBQUkscUJBQXVCO1FBQUUsT0FBTyxXQUFXLENBQUM7SUFDcEQsSUFBSSxJQUFJLGdCQUFrQjtRQUFFLE9BQU8sTUFBTSxDQUFDO0lBQzFDLElBQUksSUFBSSxtQkFBcUI7UUFBRSxPQUFPLFNBQVMsQ0FBQztJQUNoRCxJQUFJLElBQUksNEJBQThCO1FBQUUsT0FBTyxrQkFBa0IsQ0FBQztJQUNsRSxPQUFPLFdBQVcsQ0FBQztBQUNyQixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge2Fzc2VydERlZmluZWQsIGFzc2VydEVxdWFsfSBmcm9tICcuLi91dGlsL2Fzc2VydCc7XG5pbXBvcnQge1ROb2RlLCBUTm9kZVR5cGV9IGZyb20gJy4vaW50ZXJmYWNlcy9ub2RlJztcblxuZXhwb3J0IGZ1bmN0aW9uIGFzc2VydE5vZGVUeXBlKHROb2RlOiBUTm9kZSwgdHlwZTogVE5vZGVUeXBlKSB7XG4gIGFzc2VydERlZmluZWQodE5vZGUsICdzaG91bGQgYmUgY2FsbGVkIHdpdGggYSBUTm9kZScpO1xuICBhc3NlcnRFcXVhbCh0Tm9kZS50eXBlLCB0eXBlLCBgc2hvdWxkIGJlIGEgJHt0eXBlTmFtZSh0eXBlKX1gKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFzc2VydE5vZGVPZlBvc3NpYmxlVHlwZXModE5vZGU6IFROb2RlLCAuLi50eXBlczogVE5vZGVUeXBlW10pIHtcbiAgYXNzZXJ0RGVmaW5lZCh0Tm9kZSwgJ3Nob3VsZCBiZSBjYWxsZWQgd2l0aCBhIFROb2RlJyk7XG4gIGNvbnN0IGZvdW5kID0gdHlwZXMuc29tZSh0eXBlID0+IHROb2RlLnR5cGUgPT09IHR5cGUpO1xuICBhc3NlcnRFcXVhbChcbiAgICAgIGZvdW5kLCB0cnVlLFxuICAgICAgYFNob3VsZCBiZSBvbmUgb2YgJHt0eXBlcy5tYXAodHlwZU5hbWUpLmpvaW4oJywgJyl9IGJ1dCBnb3QgJHt0eXBlTmFtZSh0Tm9kZS50eXBlKX1gKTtcbn1cblxuZnVuY3Rpb24gdHlwZU5hbWUodHlwZTogVE5vZGVUeXBlKTogc3RyaW5nIHtcbiAgaWYgKHR5cGUgPT0gVE5vZGVUeXBlLlByb2plY3Rpb24pIHJldHVybiAnUHJvamVjdGlvbic7XG4gIGlmICh0eXBlID09IFROb2RlVHlwZS5Db250YWluZXIpIHJldHVybiAnQ29udGFpbmVyJztcbiAgaWYgKHR5cGUgPT0gVE5vZGVUeXBlLlZpZXcpIHJldHVybiAnVmlldyc7XG4gIGlmICh0eXBlID09IFROb2RlVHlwZS5FbGVtZW50KSByZXR1cm4gJ0VsZW1lbnQnO1xuICBpZiAodHlwZSA9PSBUTm9kZVR5cGUuRWxlbWVudENvbnRhaW5lcikgcmV0dXJuICdFbGVtZW50Q29udGFpbmVyJztcbiAgcmV0dXJuICc8dW5rbm93bj4nO1xufVxuIl19
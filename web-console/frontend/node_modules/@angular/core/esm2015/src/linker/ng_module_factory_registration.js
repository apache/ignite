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
import { stringify } from '../util/stringify';
/**
 * Map of module-id to the corresponding NgModule.
 * - In pre Ivy we track NgModuleFactory,
 * - In post Ivy we track the NgModuleType
 * @type {?}
 */
const modules = new Map();
/**
 * Registers a loaded module. Should only be called from generated NgModuleFactory code.
 * \@publicApi
 * @param {?} id
 * @param {?} factory
 * @return {?}
 */
export function registerModuleFactory(id, factory) {
    /** @type {?} */
    const existing = (/** @type {?} */ (modules.get(id)));
    assertSameOrNotExisting(id, existing && existing.moduleType, factory.moduleType);
    modules.set(id, factory);
}
/**
 * @param {?} id
 * @param {?} type
 * @param {?} incoming
 * @return {?}
 */
function assertSameOrNotExisting(id, type, incoming) {
    if (type && type !== incoming) {
        throw new Error(`Duplicate module registered for ${id} - ${stringify(type)} vs ${stringify(type.name)}`);
    }
}
/**
 * @param {?} ngModuleType
 * @return {?}
 */
export function registerNgModuleType(ngModuleType) {
    if (ngModuleType.ngModuleDef.id !== null) {
        /** @type {?} */
        const id = ngModuleType.ngModuleDef.id;
        /** @type {?} */
        const existing = (/** @type {?} */ (modules.get(id)));
        assertSameOrNotExisting(id, existing, ngModuleType);
        modules.set(id, ngModuleType);
    }
    /** @type {?} */
    let imports = ngModuleType.ngModuleDef.imports;
    if (imports instanceof Function) {
        imports = imports();
    }
    if (imports) {
        imports.forEach((/**
         * @param {?} i
         * @return {?}
         */
        i => registerNgModuleType((/** @type {?} */ (i)))));
    }
}
/**
 * @return {?}
 */
export function clearModulesForTest() {
    modules.clear();
}
/**
 * @param {?} id
 * @return {?}
 */
export function getRegisteredNgModuleType(id) {
    return modules.get(id);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfbW9kdWxlX2ZhY3RvcnlfcmVnaXN0cmF0aW9uLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvbGlua2VyL25nX21vZHVsZV9mYWN0b3J5X3JlZ2lzdHJhdGlvbi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVVBLE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQzs7Ozs7OztNQVV0QyxPQUFPLEdBQUcsSUFBSSxHQUFHLEVBQTZDOzs7Ozs7OztBQU1wRSxNQUFNLFVBQVUscUJBQXFCLENBQUMsRUFBVSxFQUFFLE9BQTZCOztVQUN2RSxRQUFRLEdBQUcsbUJBQUEsT0FBTyxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsRUFBd0I7SUFDeEQsdUJBQXVCLENBQUMsRUFBRSxFQUFFLFFBQVEsSUFBSSxRQUFRLENBQUMsVUFBVSxFQUFFLE9BQU8sQ0FBQyxVQUFVLENBQUMsQ0FBQztJQUNqRixPQUFPLENBQUMsR0FBRyxDQUFDLEVBQUUsRUFBRSxPQUFPLENBQUMsQ0FBQztBQUMzQixDQUFDOzs7Ozs7O0FBRUQsU0FBUyx1QkFBdUIsQ0FBQyxFQUFVLEVBQUUsSUFBcUIsRUFBRSxRQUFtQjtJQUNyRixJQUFJLElBQUksSUFBSSxJQUFJLEtBQUssUUFBUSxFQUFFO1FBQzdCLE1BQU0sSUFBSSxLQUFLLENBQ1gsbUNBQW1DLEVBQUUsTUFBTSxTQUFTLENBQUMsSUFBSSxDQUFDLE9BQU8sU0FBUyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLENBQUM7S0FDOUY7QUFDSCxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxvQkFBb0IsQ0FBQyxZQUEwQjtJQUM3RCxJQUFJLFlBQVksQ0FBQyxXQUFXLENBQUMsRUFBRSxLQUFLLElBQUksRUFBRTs7Y0FDbEMsRUFBRSxHQUFHLFlBQVksQ0FBQyxXQUFXLENBQUMsRUFBRTs7Y0FDaEMsUUFBUSxHQUFHLG1CQUFBLE9BQU8sQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEVBQXVCO1FBQ3ZELHVCQUF1QixDQUFDLEVBQUUsRUFBRSxRQUFRLEVBQUUsWUFBWSxDQUFDLENBQUM7UUFDcEQsT0FBTyxDQUFDLEdBQUcsQ0FBQyxFQUFFLEVBQUUsWUFBWSxDQUFDLENBQUM7S0FDL0I7O1FBRUcsT0FBTyxHQUFHLFlBQVksQ0FBQyxXQUFXLENBQUMsT0FBTztJQUM5QyxJQUFJLE9BQU8sWUFBWSxRQUFRLEVBQUU7UUFDL0IsT0FBTyxHQUFHLE9BQU8sRUFBRSxDQUFDO0tBQ3JCO0lBQ0QsSUFBSSxPQUFPLEVBQUU7UUFDWCxPQUFPLENBQUMsT0FBTzs7OztRQUFDLENBQUMsQ0FBQyxFQUFFLENBQUMsb0JBQW9CLENBQUMsbUJBQUEsQ0FBQyxFQUFnQixDQUFDLEVBQUMsQ0FBQztLQUMvRDtBQUNILENBQUM7Ozs7QUFFRCxNQUFNLFVBQVUsbUJBQW1CO0lBQ2pDLE9BQU8sQ0FBQyxLQUFLLEVBQUUsQ0FBQztBQUNsQixDQUFDOzs7OztBQUVELE1BQU0sVUFBVSx5QkFBeUIsQ0FBQyxFQUFVO0lBQ2xELE9BQU8sT0FBTyxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsQ0FBQztBQUN6QixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1R5cGV9IGZyb20gJy4uL2ludGVyZmFjZS90eXBlJztcbmltcG9ydCB7TmdNb2R1bGVUeXBlfSBmcm9tICcuLi9yZW5kZXIzL25nX21vZHVsZV9yZWYnO1xuaW1wb3J0IHtzdHJpbmdpZnl9IGZyb20gJy4uL3V0aWwvc3RyaW5naWZ5JztcblxuaW1wb3J0IHtOZ01vZHVsZUZhY3Rvcnl9IGZyb20gJy4vbmdfbW9kdWxlX2ZhY3RvcnknO1xuXG5cbi8qKlxuICogTWFwIG9mIG1vZHVsZS1pZCB0byB0aGUgY29ycmVzcG9uZGluZyBOZ01vZHVsZS5cbiAqIC0gSW4gcHJlIEl2eSB3ZSB0cmFjayBOZ01vZHVsZUZhY3RvcnksXG4gKiAtIEluIHBvc3QgSXZ5IHdlIHRyYWNrIHRoZSBOZ01vZHVsZVR5cGVcbiAqL1xuY29uc3QgbW9kdWxlcyA9IG5ldyBNYXA8c3RyaW5nLCBOZ01vZHVsZUZhY3Rvcnk8YW55PnxOZ01vZHVsZVR5cGU+KCk7XG5cbi8qKlxuICogUmVnaXN0ZXJzIGEgbG9hZGVkIG1vZHVsZS4gU2hvdWxkIG9ubHkgYmUgY2FsbGVkIGZyb20gZ2VuZXJhdGVkIE5nTW9kdWxlRmFjdG9yeSBjb2RlLlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gcmVnaXN0ZXJNb2R1bGVGYWN0b3J5KGlkOiBzdHJpbmcsIGZhY3Rvcnk6IE5nTW9kdWxlRmFjdG9yeTxhbnk+KSB7XG4gIGNvbnN0IGV4aXN0aW5nID0gbW9kdWxlcy5nZXQoaWQpIGFzIE5nTW9kdWxlRmFjdG9yeTxhbnk+O1xuICBhc3NlcnRTYW1lT3JOb3RFeGlzdGluZyhpZCwgZXhpc3RpbmcgJiYgZXhpc3RpbmcubW9kdWxlVHlwZSwgZmFjdG9yeS5tb2R1bGVUeXBlKTtcbiAgbW9kdWxlcy5zZXQoaWQsIGZhY3RvcnkpO1xufVxuXG5mdW5jdGlvbiBhc3NlcnRTYW1lT3JOb3RFeGlzdGluZyhpZDogc3RyaW5nLCB0eXBlOiBUeXBlPGFueT58IG51bGwsIGluY29taW5nOiBUeXBlPGFueT4pOiB2b2lkIHtcbiAgaWYgKHR5cGUgJiYgdHlwZSAhPT0gaW5jb21pbmcpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIGBEdXBsaWNhdGUgbW9kdWxlIHJlZ2lzdGVyZWQgZm9yICR7aWR9IC0gJHtzdHJpbmdpZnkodHlwZSl9IHZzICR7c3RyaW5naWZ5KHR5cGUubmFtZSl9YCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlZ2lzdGVyTmdNb2R1bGVUeXBlKG5nTW9kdWxlVHlwZTogTmdNb2R1bGVUeXBlKSB7XG4gIGlmIChuZ01vZHVsZVR5cGUubmdNb2R1bGVEZWYuaWQgIT09IG51bGwpIHtcbiAgICBjb25zdCBpZCA9IG5nTW9kdWxlVHlwZS5uZ01vZHVsZURlZi5pZDtcbiAgICBjb25zdCBleGlzdGluZyA9IG1vZHVsZXMuZ2V0KGlkKSBhcyBOZ01vZHVsZVR5cGUgfCBudWxsO1xuICAgIGFzc2VydFNhbWVPck5vdEV4aXN0aW5nKGlkLCBleGlzdGluZywgbmdNb2R1bGVUeXBlKTtcbiAgICBtb2R1bGVzLnNldChpZCwgbmdNb2R1bGVUeXBlKTtcbiAgfVxuXG4gIGxldCBpbXBvcnRzID0gbmdNb2R1bGVUeXBlLm5nTW9kdWxlRGVmLmltcG9ydHM7XG4gIGlmIChpbXBvcnRzIGluc3RhbmNlb2YgRnVuY3Rpb24pIHtcbiAgICBpbXBvcnRzID0gaW1wb3J0cygpO1xuICB9XG4gIGlmIChpbXBvcnRzKSB7XG4gICAgaW1wb3J0cy5mb3JFYWNoKGkgPT4gcmVnaXN0ZXJOZ01vZHVsZVR5cGUoaSBhcyBOZ01vZHVsZVR5cGUpKTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYXJNb2R1bGVzRm9yVGVzdCgpOiB2b2lkIHtcbiAgbW9kdWxlcy5jbGVhcigpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVnaXN0ZXJlZE5nTW9kdWxlVHlwZShpZDogc3RyaW5nKSB7XG4gIHJldHVybiBtb2R1bGVzLmdldChpZCk7XG59XG4iXX0=
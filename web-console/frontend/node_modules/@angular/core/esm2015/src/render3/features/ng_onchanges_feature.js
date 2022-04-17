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
import { SimpleChange } from '../../interface/simple_change';
import { EMPTY_OBJ } from '../empty';
/** @type {?} */
const PRIVATE_PREFIX = '__ngOnChanges_';
/**
 * The NgOnChangesFeature decorates a component with support for the ngOnChanges
 * lifecycle hook, so it should be included in any component that implements
 * that hook.
 *
 * If the component or directive uses inheritance, the NgOnChangesFeature MUST
 * be included as a feature AFTER {\@link InheritDefinitionFeature}, otherwise
 * inherited properties will not be propagated to the ngOnChanges lifecycle
 * hook.
 *
 * Example usage:
 *
 * ```
 * static ngComponentDef = defineComponent({
 *   ...
 *   inputs: {name: 'publicName'},
 *   features: [NgOnChangesFeature()]
 * });
 * ```
 *
 * \@codeGenApi
 * @template T
 * @return {?}
 */
export function ɵɵNgOnChangesFeature() {
    // This option ensures that the ngOnChanges lifecycle hook will be inherited
    // from superclasses (in InheritDefinitionFeature).
    ((/** @type {?} */ (NgOnChangesFeatureImpl))).ngInherit = true;
    return NgOnChangesFeatureImpl;
}
/**
 * @template T
 * @param {?} definition
 * @return {?}
 */
function NgOnChangesFeatureImpl(definition) {
    if (definition.type.prototype.ngOnChanges) {
        definition.setInput = ngOnChangesSetInput;
        definition.onChanges = wrapOnChanges();
    }
}
/**
 * @return {?}
 */
function wrapOnChanges() {
    return (/**
     * @this {?}
     * @return {?}
     */
    function wrapOnChangesHook_inPreviousChangesStorage() {
        /** @type {?} */
        const simpleChangesStore = getSimpleChangesStore(this);
        /** @type {?} */
        const current = simpleChangesStore && simpleChangesStore.current;
        if (current) {
            /** @type {?} */
            const previous = (/** @type {?} */ (simpleChangesStore)).previous;
            if (previous === EMPTY_OBJ) {
                (/** @type {?} */ (simpleChangesStore)).previous = current;
            }
            else {
                // New changes are copied to the previous store, so that we don't lose history for inputs
                // which were not changed this time
                for (let key in current) {
                    previous[key] = current[key];
                }
            }
            (/** @type {?} */ (simpleChangesStore)).current = null;
            this.ngOnChanges(current);
        }
    });
}
/**
 * @template T
 * @this {?}
 * @param {?} instance
 * @param {?} value
 * @param {?} publicName
 * @param {?} privateName
 * @return {?}
 */
function ngOnChangesSetInput(instance, value, publicName, privateName) {
    /** @type {?} */
    const simpleChangesStore = getSimpleChangesStore(instance) ||
        setSimpleChangesStore(instance, { previous: EMPTY_OBJ, current: null });
    /** @type {?} */
    const current = simpleChangesStore.current || (simpleChangesStore.current = {});
    /** @type {?} */
    const previous = simpleChangesStore.previous;
    /** @type {?} */
    const declaredName = ((/** @type {?} */ (this.declaredInputs)))[publicName];
    /** @type {?} */
    const previousChange = previous[declaredName];
    current[declaredName] = new SimpleChange(previousChange && previousChange.currentValue, value, previous === EMPTY_OBJ);
    ((/** @type {?} */ (instance)))[privateName] = value;
}
/** @type {?} */
const SIMPLE_CHANGES_STORE = '__ngSimpleChanges__';
/**
 * @param {?} instance
 * @return {?}
 */
function getSimpleChangesStore(instance) {
    return instance[SIMPLE_CHANGES_STORE] || null;
}
/**
 * @param {?} instance
 * @param {?} store
 * @return {?}
 */
function setSimpleChangesStore(instance, store) {
    return instance[SIMPLE_CHANGES_STORE] = store;
}
/**
 * @record
 */
function NgSimpleChangesStore() { }
if (false) {
    /** @type {?} */
    NgSimpleChangesStore.prototype.previous;
    /** @type {?} */
    NgSimpleChangesStore.prototype.current;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfb25jaGFuZ2VzX2ZlYXR1cmUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL2ZlYXR1cmVzL25nX29uY2hhbmdlc19mZWF0dXJlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBU0EsT0FBTyxFQUFDLFlBQVksRUFBZ0IsTUFBTSwrQkFBK0IsQ0FBQztBQUMxRSxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sVUFBVSxDQUFDOztNQUc3QixjQUFjLEdBQUcsZ0JBQWdCOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBOEJ2QyxNQUFNLFVBQVUsb0JBQW9CO0lBQ2xDLDRFQUE0RTtJQUM1RSxtREFBbUQ7SUFDbkQsQ0FBQyxtQkFBQSxzQkFBc0IsRUFBdUIsQ0FBQyxDQUFDLFNBQVMsR0FBRyxJQUFJLENBQUM7SUFDakUsT0FBTyxzQkFBc0IsQ0FBQztBQUNoQyxDQUFDOzs7Ozs7QUFFRCxTQUFTLHNCQUFzQixDQUFJLFVBQTJCO0lBQzVELElBQUksVUFBVSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsV0FBVyxFQUFFO1FBQ3pDLFVBQVUsQ0FBQyxRQUFRLEdBQUcsbUJBQW1CLENBQUM7UUFDMUMsVUFBVSxDQUFDLFNBQVMsR0FBRyxhQUFhLEVBQUUsQ0FBQztLQUN4QztBQUNILENBQUM7Ozs7QUFFRCxTQUFTLGFBQWE7SUFDcEI7Ozs7SUFBTyxTQUFTLDBDQUEwQzs7Y0FDbEQsa0JBQWtCLEdBQUcscUJBQXFCLENBQUMsSUFBSSxDQUFDOztjQUNoRCxPQUFPLEdBQUcsa0JBQWtCLElBQUksa0JBQWtCLENBQUMsT0FBTztRQUVoRSxJQUFJLE9BQU8sRUFBRTs7a0JBQ0wsUUFBUSxHQUFHLG1CQUFBLGtCQUFrQixFQUFFLENBQUMsUUFBUTtZQUM5QyxJQUFJLFFBQVEsS0FBSyxTQUFTLEVBQUU7Z0JBQzFCLG1CQUFBLGtCQUFrQixFQUFFLENBQUMsUUFBUSxHQUFHLE9BQU8sQ0FBQzthQUN6QztpQkFBTTtnQkFDTCx5RkFBeUY7Z0JBQ3pGLG1DQUFtQztnQkFDbkMsS0FBSyxJQUFJLEdBQUcsSUFBSSxPQUFPLEVBQUU7b0JBQ3ZCLFFBQVEsQ0FBQyxHQUFHLENBQUMsR0FBRyxPQUFPLENBQUMsR0FBRyxDQUFDLENBQUM7aUJBQzlCO2FBQ0Y7WUFDRCxtQkFBQSxrQkFBa0IsRUFBRSxDQUFDLE9BQU8sR0FBRyxJQUFJLENBQUM7WUFDcEMsSUFBSSxDQUFDLFdBQVcsQ0FBQyxPQUFPLENBQUMsQ0FBQztTQUMzQjtJQUNILENBQUMsRUFBQztBQUNKLENBQUM7Ozs7Ozs7Ozs7QUFFRCxTQUFTLG1CQUFtQixDQUNELFFBQVcsRUFBRSxLQUFVLEVBQUUsVUFBa0IsRUFBRSxXQUFtQjs7VUFDbkYsa0JBQWtCLEdBQUcscUJBQXFCLENBQUMsUUFBUSxDQUFDO1FBQ3RELHFCQUFxQixDQUFDLFFBQVEsRUFBRSxFQUFDLFFBQVEsRUFBRSxTQUFTLEVBQUUsT0FBTyxFQUFFLElBQUksRUFBQyxDQUFDOztVQUNuRSxPQUFPLEdBQUcsa0JBQWtCLENBQUMsT0FBTyxJQUFJLENBQUMsa0JBQWtCLENBQUMsT0FBTyxHQUFHLEVBQUUsQ0FBQzs7VUFDekUsUUFBUSxHQUFHLGtCQUFrQixDQUFDLFFBQVE7O1VBRXRDLFlBQVksR0FBRyxDQUFDLG1CQUFBLElBQUksQ0FBQyxjQUFjLEVBQTBCLENBQUMsQ0FBQyxVQUFVLENBQUM7O1VBQzFFLGNBQWMsR0FBRyxRQUFRLENBQUMsWUFBWSxDQUFDO0lBQzdDLE9BQU8sQ0FBQyxZQUFZLENBQUMsR0FBRyxJQUFJLFlBQVksQ0FDcEMsY0FBYyxJQUFJLGNBQWMsQ0FBQyxZQUFZLEVBQUUsS0FBSyxFQUFFLFFBQVEsS0FBSyxTQUFTLENBQUMsQ0FBQztJQUVsRixDQUFDLG1CQUFBLFFBQVEsRUFBTyxDQUFDLENBQUMsV0FBVyxDQUFDLEdBQUcsS0FBSyxDQUFDO0FBQ3pDLENBQUM7O01BRUssb0JBQW9CLEdBQUcscUJBQXFCOzs7OztBQUVsRCxTQUFTLHFCQUFxQixDQUFDLFFBQWE7SUFDMUMsT0FBTyxRQUFRLENBQUMsb0JBQW9CLENBQUMsSUFBSSxJQUFJLENBQUM7QUFDaEQsQ0FBQzs7Ozs7O0FBRUQsU0FBUyxxQkFBcUIsQ0FBQyxRQUFhLEVBQUUsS0FBMkI7SUFDdkUsT0FBTyxRQUFRLENBQUMsb0JBQW9CLENBQUMsR0FBRyxLQUFLLENBQUM7QUFDaEQsQ0FBQzs7OztBQUVELG1DQUdDOzs7SUFGQyx3Q0FBd0I7O0lBQ3hCLHVDQUE0QiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtPbkNoYW5nZXN9IGZyb20gJy4uLy4uL2ludGVyZmFjZS9saWZlY3ljbGVfaG9va3MnO1xuaW1wb3J0IHtTaW1wbGVDaGFuZ2UsIFNpbXBsZUNoYW5nZXN9IGZyb20gJy4uLy4uL2ludGVyZmFjZS9zaW1wbGVfY2hhbmdlJztcbmltcG9ydCB7RU1QVFlfT0JKfSBmcm9tICcuLi9lbXB0eSc7XG5pbXBvcnQge0RpcmVjdGl2ZURlZiwgRGlyZWN0aXZlRGVmRmVhdHVyZX0gZnJvbSAnLi4vaW50ZXJmYWNlcy9kZWZpbml0aW9uJztcblxuY29uc3QgUFJJVkFURV9QUkVGSVggPSAnX19uZ09uQ2hhbmdlc18nO1xuXG50eXBlIE9uQ2hhbmdlc0V4cGFuZG8gPSBPbkNoYW5nZXMgJiB7XG4gIF9fbmdPbkNoYW5nZXNfOiBTaW1wbGVDaGFuZ2VzfG51bGx8dW5kZWZpbmVkO1xuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55IENhbiBob2xkIGFueSB2YWx1ZVxuICBba2V5OiBzdHJpbmddOiBhbnk7XG59O1xuXG4vKipcbiAqIFRoZSBOZ09uQ2hhbmdlc0ZlYXR1cmUgZGVjb3JhdGVzIGEgY29tcG9uZW50IHdpdGggc3VwcG9ydCBmb3IgdGhlIG5nT25DaGFuZ2VzXG4gKiBsaWZlY3ljbGUgaG9vaywgc28gaXQgc2hvdWxkIGJlIGluY2x1ZGVkIGluIGFueSBjb21wb25lbnQgdGhhdCBpbXBsZW1lbnRzXG4gKiB0aGF0IGhvb2suXG4gKlxuICogSWYgdGhlIGNvbXBvbmVudCBvciBkaXJlY3RpdmUgdXNlcyBpbmhlcml0YW5jZSwgdGhlIE5nT25DaGFuZ2VzRmVhdHVyZSBNVVNUXG4gKiBiZSBpbmNsdWRlZCBhcyBhIGZlYXR1cmUgQUZURVIge0BsaW5rIEluaGVyaXREZWZpbml0aW9uRmVhdHVyZX0sIG90aGVyd2lzZVxuICogaW5oZXJpdGVkIHByb3BlcnRpZXMgd2lsbCBub3QgYmUgcHJvcGFnYXRlZCB0byB0aGUgbmdPbkNoYW5nZXMgbGlmZWN5Y2xlXG4gKiBob29rLlxuICpcbiAqIEV4YW1wbGUgdXNhZ2U6XG4gKlxuICogYGBgXG4gKiBzdGF0aWMgbmdDb21wb25lbnREZWYgPSBkZWZpbmVDb21wb25lbnQoe1xuICogICAuLi5cbiAqICAgaW5wdXRzOiB7bmFtZTogJ3B1YmxpY05hbWUnfSxcbiAqICAgZmVhdHVyZXM6IFtOZ09uQ2hhbmdlc0ZlYXR1cmUoKV1cbiAqIH0pO1xuICogYGBgXG4gKlxuICogQGNvZGVHZW5BcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIMm1ybVOZ09uQ2hhbmdlc0ZlYXR1cmU8VD4oKTogRGlyZWN0aXZlRGVmRmVhdHVyZSB7XG4gIC8vIFRoaXMgb3B0aW9uIGVuc3VyZXMgdGhhdCB0aGUgbmdPbkNoYW5nZXMgbGlmZWN5Y2xlIGhvb2sgd2lsbCBiZSBpbmhlcml0ZWRcbiAgLy8gZnJvbSBzdXBlcmNsYXNzZXMgKGluIEluaGVyaXREZWZpbml0aW9uRmVhdHVyZSkuXG4gIChOZ09uQ2hhbmdlc0ZlYXR1cmVJbXBsIGFzIERpcmVjdGl2ZURlZkZlYXR1cmUpLm5nSW5oZXJpdCA9IHRydWU7XG4gIHJldHVybiBOZ09uQ2hhbmdlc0ZlYXR1cmVJbXBsO1xufVxuXG5mdW5jdGlvbiBOZ09uQ2hhbmdlc0ZlYXR1cmVJbXBsPFQ+KGRlZmluaXRpb246IERpcmVjdGl2ZURlZjxUPik6IHZvaWQge1xuICBpZiAoZGVmaW5pdGlvbi50eXBlLnByb3RvdHlwZS5uZ09uQ2hhbmdlcykge1xuICAgIGRlZmluaXRpb24uc2V0SW5wdXQgPSBuZ09uQ2hhbmdlc1NldElucHV0O1xuICAgIGRlZmluaXRpb24ub25DaGFuZ2VzID0gd3JhcE9uQ2hhbmdlcygpO1xuICB9XG59XG5cbmZ1bmN0aW9uIHdyYXBPbkNoYW5nZXMoKSB7XG4gIHJldHVybiBmdW5jdGlvbiB3cmFwT25DaGFuZ2VzSG9va19pblByZXZpb3VzQ2hhbmdlc1N0b3JhZ2UodGhpczogT25DaGFuZ2VzKSB7XG4gICAgY29uc3Qgc2ltcGxlQ2hhbmdlc1N0b3JlID0gZ2V0U2ltcGxlQ2hhbmdlc1N0b3JlKHRoaXMpO1xuICAgIGNvbnN0IGN1cnJlbnQgPSBzaW1wbGVDaGFuZ2VzU3RvcmUgJiYgc2ltcGxlQ2hhbmdlc1N0b3JlLmN1cnJlbnQ7XG5cbiAgICBpZiAoY3VycmVudCkge1xuICAgICAgY29uc3QgcHJldmlvdXMgPSBzaW1wbGVDaGFuZ2VzU3RvcmUgIS5wcmV2aW91cztcbiAgICAgIGlmIChwcmV2aW91cyA9PT0gRU1QVFlfT0JKKSB7XG4gICAgICAgIHNpbXBsZUNoYW5nZXNTdG9yZSAhLnByZXZpb3VzID0gY3VycmVudDtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIC8vIE5ldyBjaGFuZ2VzIGFyZSBjb3BpZWQgdG8gdGhlIHByZXZpb3VzIHN0b3JlLCBzbyB0aGF0IHdlIGRvbid0IGxvc2UgaGlzdG9yeSBmb3IgaW5wdXRzXG4gICAgICAgIC8vIHdoaWNoIHdlcmUgbm90IGNoYW5nZWQgdGhpcyB0aW1lXG4gICAgICAgIGZvciAobGV0IGtleSBpbiBjdXJyZW50KSB7XG4gICAgICAgICAgcHJldmlvdXNba2V5XSA9IGN1cnJlbnRba2V5XTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgc2ltcGxlQ2hhbmdlc1N0b3JlICEuY3VycmVudCA9IG51bGw7XG4gICAgICB0aGlzLm5nT25DaGFuZ2VzKGN1cnJlbnQpO1xuICAgIH1cbiAgfTtcbn1cblxuZnVuY3Rpb24gbmdPbkNoYW5nZXNTZXRJbnB1dDxUPihcbiAgICB0aGlzOiBEaXJlY3RpdmVEZWY8VD4sIGluc3RhbmNlOiBULCB2YWx1ZTogYW55LCBwdWJsaWNOYW1lOiBzdHJpbmcsIHByaXZhdGVOYW1lOiBzdHJpbmcpOiB2b2lkIHtcbiAgY29uc3Qgc2ltcGxlQ2hhbmdlc1N0b3JlID0gZ2V0U2ltcGxlQ2hhbmdlc1N0b3JlKGluc3RhbmNlKSB8fFxuICAgICAgc2V0U2ltcGxlQ2hhbmdlc1N0b3JlKGluc3RhbmNlLCB7cHJldmlvdXM6IEVNUFRZX09CSiwgY3VycmVudDogbnVsbH0pO1xuICBjb25zdCBjdXJyZW50ID0gc2ltcGxlQ2hhbmdlc1N0b3JlLmN1cnJlbnQgfHwgKHNpbXBsZUNoYW5nZXNTdG9yZS5jdXJyZW50ID0ge30pO1xuICBjb25zdCBwcmV2aW91cyA9IHNpbXBsZUNoYW5nZXNTdG9yZS5wcmV2aW91cztcblxuICBjb25zdCBkZWNsYXJlZE5hbWUgPSAodGhpcy5kZWNsYXJlZElucHV0cyBhc3tba2V5OiBzdHJpbmddOiBzdHJpbmd9KVtwdWJsaWNOYW1lXTtcbiAgY29uc3QgcHJldmlvdXNDaGFuZ2UgPSBwcmV2aW91c1tkZWNsYXJlZE5hbWVdO1xuICBjdXJyZW50W2RlY2xhcmVkTmFtZV0gPSBuZXcgU2ltcGxlQ2hhbmdlKFxuICAgICAgcHJldmlvdXNDaGFuZ2UgJiYgcHJldmlvdXNDaGFuZ2UuY3VycmVudFZhbHVlLCB2YWx1ZSwgcHJldmlvdXMgPT09IEVNUFRZX09CSik7XG5cbiAgKGluc3RhbmNlIGFzIGFueSlbcHJpdmF0ZU5hbWVdID0gdmFsdWU7XG59XG5cbmNvbnN0IFNJTVBMRV9DSEFOR0VTX1NUT1JFID0gJ19fbmdTaW1wbGVDaGFuZ2VzX18nO1xuXG5mdW5jdGlvbiBnZXRTaW1wbGVDaGFuZ2VzU3RvcmUoaW5zdGFuY2U6IGFueSk6IG51bGx8TmdTaW1wbGVDaGFuZ2VzU3RvcmUge1xuICByZXR1cm4gaW5zdGFuY2VbU0lNUExFX0NIQU5HRVNfU1RPUkVdIHx8IG51bGw7XG59XG5cbmZ1bmN0aW9uIHNldFNpbXBsZUNoYW5nZXNTdG9yZShpbnN0YW5jZTogYW55LCBzdG9yZTogTmdTaW1wbGVDaGFuZ2VzU3RvcmUpOiBOZ1NpbXBsZUNoYW5nZXNTdG9yZSB7XG4gIHJldHVybiBpbnN0YW5jZVtTSU1QTEVfQ0hBTkdFU19TVE9SRV0gPSBzdG9yZTtcbn1cblxuaW50ZXJmYWNlIE5nU2ltcGxlQ2hhbmdlc1N0b3JlIHtcbiAgcHJldmlvdXM6IFNpbXBsZUNoYW5nZXM7XG4gIGN1cnJlbnQ6IFNpbXBsZUNoYW5nZXN8bnVsbDtcbn1cbiJdfQ==
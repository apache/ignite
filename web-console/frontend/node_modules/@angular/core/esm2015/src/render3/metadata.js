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
import { noSideEffects } from '../util/closure';
/**
 * @record
 */
function TypeWithMetadata() { }
if (false) {
    /** @type {?|undefined} */
    TypeWithMetadata.prototype.decorators;
    /** @type {?|undefined} */
    TypeWithMetadata.prototype.ctorParameters;
    /** @type {?|undefined} */
    TypeWithMetadata.prototype.propDecorators;
}
/**
 * Adds decorator, constructor, and property metadata to a given type via static metadata fields
 * on the type.
 *
 * These metadata fields can later be read with Angular's `ReflectionCapabilities` API.
 *
 * Calls to `setClassMetadata` can be marked as pure, resulting in the metadata assignments being
 * tree-shaken away during production builds.
 * @param {?} type
 * @param {?} decorators
 * @param {?} ctorParameters
 * @param {?} propDecorators
 * @return {?}
 */
export function setClassMetadata(type, decorators, ctorParameters, propDecorators) {
    return (/** @type {?} */ (noSideEffects((/**
     * @return {?}
     */
    () => {
        /** @type {?} */
        const clazz = (/** @type {?} */ (type));
        // We determine whether a class has its own metadata by taking the metadata from the parent
        // constructor and checking whether it's the same as the subclass metadata below. We can't use
        // `hasOwnProperty` here because it doesn't work correctly in IE10 for static fields that are
        // defined by TS. See https://github.com/angular/angular/pull/28439#issuecomment-459349218.
        /** @type {?} */
        const parentPrototype = clazz.prototype ? Object.getPrototypeOf(clazz.prototype) : null;
        /** @type {?} */
        const parentConstructor = parentPrototype && parentPrototype.constructor;
        if (decorators !== null) {
            if (clazz.decorators !== undefined &&
                (!parentConstructor || parentConstructor.decorators !== clazz.decorators)) {
                clazz.decorators.push(...decorators);
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
                clazz.propDecorators = Object.assign({}, clazz.propDecorators, propDecorators);
            }
            else {
                clazz.propDecorators = propDecorators;
            }
        }
    }))));
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWV0YWRhdGEuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL21ldGFkYXRhLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBU0EsT0FBTyxFQUFDLGFBQWEsRUFBQyxNQUFNLGlCQUFpQixDQUFDOzs7O0FBRTlDLCtCQUlDOzs7SUFIQyxzQ0FBbUI7O0lBQ25CLDBDQUE2Qjs7SUFDN0IsMENBQXdDOzs7Ozs7Ozs7Ozs7Ozs7O0FBWTFDLE1BQU0sVUFBVSxnQkFBZ0IsQ0FDNUIsSUFBZSxFQUFFLFVBQXdCLEVBQUUsY0FBb0MsRUFDL0UsY0FBNkM7SUFDL0MsT0FBTyxtQkFBQSxhQUFhOzs7SUFBQyxHQUFHLEVBQUU7O2NBQ2xCLEtBQUssR0FBRyxtQkFBQSxJQUFJLEVBQW9COzs7Ozs7Y0FNaEMsZUFBZSxHQUFHLEtBQUssQ0FBQyxTQUFTLENBQUMsQ0FBQyxDQUFDLE1BQU0sQ0FBQyxjQUFjLENBQUMsS0FBSyxDQUFDLFNBQVMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJOztjQUNqRixpQkFBaUIsR0FBMEIsZUFBZSxJQUFJLGVBQWUsQ0FBQyxXQUFXO1FBRS9GLElBQUksVUFBVSxLQUFLLElBQUksRUFBRTtZQUN2QixJQUFJLEtBQUssQ0FBQyxVQUFVLEtBQUssU0FBUztnQkFDOUIsQ0FBQyxDQUFDLGlCQUFpQixJQUFJLGlCQUFpQixDQUFDLFVBQVUsS0FBSyxLQUFLLENBQUMsVUFBVSxDQUFDLEVBQUU7Z0JBQzdFLEtBQUssQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDLEdBQUcsVUFBVSxDQUFDLENBQUM7YUFDdEM7aUJBQU07Z0JBQ0wsS0FBSyxDQUFDLFVBQVUsR0FBRyxVQUFVLENBQUM7YUFDL0I7U0FDRjtRQUNELElBQUksY0FBYyxLQUFLLElBQUksRUFBRTtZQUMzQiwwRkFBMEY7WUFDMUYsNEZBQTRGO1lBQzVGLG9DQUFvQztZQUNwQyxLQUFLLENBQUMsY0FBYyxHQUFHLGNBQWMsQ0FBQztTQUN2QztRQUNELElBQUksY0FBYyxLQUFLLElBQUksRUFBRTtZQUMzQiw4RkFBOEY7WUFDOUYsMkZBQTJGO1lBQzNGLDJGQUEyRjtZQUMzRiwwQ0FBMEM7WUFDMUMsSUFBSSxLQUFLLENBQUMsY0FBYyxLQUFLLFNBQVM7Z0JBQ2xDLENBQUMsQ0FBQyxpQkFBaUIsSUFBSSxpQkFBaUIsQ0FBQyxjQUFjLEtBQUssS0FBSyxDQUFDLGNBQWMsQ0FBQyxFQUFFO2dCQUNyRixLQUFLLENBQUMsY0FBYyxxQkFBTyxLQUFLLENBQUMsY0FBYyxFQUFLLGNBQWMsQ0FBQyxDQUFDO2FBQ3JFO2lCQUFNO2dCQUNMLEtBQUssQ0FBQyxjQUFjLEdBQUcsY0FBYyxDQUFDO2FBQ3ZDO1NBQ0Y7SUFDSCxDQUFDLEVBQUMsRUFBUyxDQUFDO0FBQ2QsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtUeXBlfSBmcm9tICcuLi9pbnRlcmZhY2UvdHlwZSc7XG5pbXBvcnQge25vU2lkZUVmZmVjdHN9IGZyb20gJy4uL3V0aWwvY2xvc3VyZSc7XG5cbmludGVyZmFjZSBUeXBlV2l0aE1ldGFkYXRhIGV4dGVuZHMgVHlwZTxhbnk+IHtcbiAgZGVjb3JhdG9ycz86IGFueVtdO1xuICBjdG9yUGFyYW1ldGVycz86ICgpID0+IGFueVtdO1xuICBwcm9wRGVjb3JhdG9ycz86IHtbZmllbGQ6IHN0cmluZ106IGFueX07XG59XG5cbi8qKlxuICogQWRkcyBkZWNvcmF0b3IsIGNvbnN0cnVjdG9yLCBhbmQgcHJvcGVydHkgbWV0YWRhdGEgdG8gYSBnaXZlbiB0eXBlIHZpYSBzdGF0aWMgbWV0YWRhdGEgZmllbGRzXG4gKiBvbiB0aGUgdHlwZS5cbiAqXG4gKiBUaGVzZSBtZXRhZGF0YSBmaWVsZHMgY2FuIGxhdGVyIGJlIHJlYWQgd2l0aCBBbmd1bGFyJ3MgYFJlZmxlY3Rpb25DYXBhYmlsaXRpZXNgIEFQSS5cbiAqXG4gKiBDYWxscyB0byBgc2V0Q2xhc3NNZXRhZGF0YWAgY2FuIGJlIG1hcmtlZCBhcyBwdXJlLCByZXN1bHRpbmcgaW4gdGhlIG1ldGFkYXRhIGFzc2lnbm1lbnRzIGJlaW5nXG4gKiB0cmVlLXNoYWtlbiBhd2F5IGR1cmluZyBwcm9kdWN0aW9uIGJ1aWxkcy5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHNldENsYXNzTWV0YWRhdGEoXG4gICAgdHlwZTogVHlwZTxhbnk+LCBkZWNvcmF0b3JzOiBhbnlbXSB8IG51bGwsIGN0b3JQYXJhbWV0ZXJzOiAoKCkgPT4gYW55W10pIHwgbnVsbCxcbiAgICBwcm9wRGVjb3JhdG9yczoge1tmaWVsZDogc3RyaW5nXTogYW55fSB8IG51bGwpOiB2b2lkIHtcbiAgcmV0dXJuIG5vU2lkZUVmZmVjdHMoKCkgPT4ge1xuICAgIGNvbnN0IGNsYXp6ID0gdHlwZSBhcyBUeXBlV2l0aE1ldGFkYXRhO1xuXG4gICAgLy8gV2UgZGV0ZXJtaW5lIHdoZXRoZXIgYSBjbGFzcyBoYXMgaXRzIG93biBtZXRhZGF0YSBieSB0YWtpbmcgdGhlIG1ldGFkYXRhIGZyb20gdGhlIHBhcmVudFxuICAgIC8vIGNvbnN0cnVjdG9yIGFuZCBjaGVja2luZyB3aGV0aGVyIGl0J3MgdGhlIHNhbWUgYXMgdGhlIHN1YmNsYXNzIG1ldGFkYXRhIGJlbG93LiBXZSBjYW4ndCB1c2VcbiAgICAvLyBgaGFzT3duUHJvcGVydHlgIGhlcmUgYmVjYXVzZSBpdCBkb2Vzbid0IHdvcmsgY29ycmVjdGx5IGluIElFMTAgZm9yIHN0YXRpYyBmaWVsZHMgdGhhdCBhcmVcbiAgICAvLyBkZWZpbmVkIGJ5IFRTLiBTZWUgaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvYW5ndWxhci9wdWxsLzI4NDM5I2lzc3VlY29tbWVudC00NTkzNDkyMTguXG4gICAgY29uc3QgcGFyZW50UHJvdG90eXBlID0gY2xhenoucHJvdG90eXBlID8gT2JqZWN0LmdldFByb3RvdHlwZU9mKGNsYXp6LnByb3RvdHlwZSkgOiBudWxsO1xuICAgIGNvbnN0IHBhcmVudENvbnN0cnVjdG9yOiBUeXBlV2l0aE1ldGFkYXRhfG51bGwgPSBwYXJlbnRQcm90b3R5cGUgJiYgcGFyZW50UHJvdG90eXBlLmNvbnN0cnVjdG9yO1xuXG4gICAgaWYgKGRlY29yYXRvcnMgIT09IG51bGwpIHtcbiAgICAgIGlmIChjbGF6ei5kZWNvcmF0b3JzICE9PSB1bmRlZmluZWQgJiZcbiAgICAgICAgICAoIXBhcmVudENvbnN0cnVjdG9yIHx8IHBhcmVudENvbnN0cnVjdG9yLmRlY29yYXRvcnMgIT09IGNsYXp6LmRlY29yYXRvcnMpKSB7XG4gICAgICAgIGNsYXp6LmRlY29yYXRvcnMucHVzaCguLi5kZWNvcmF0b3JzKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNsYXp6LmRlY29yYXRvcnMgPSBkZWNvcmF0b3JzO1xuICAgICAgfVxuICAgIH1cbiAgICBpZiAoY3RvclBhcmFtZXRlcnMgIT09IG51bGwpIHtcbiAgICAgIC8vIFJhdGhlciB0aGFuIG1lcmdpbmcsIGNsb2JiZXIgdGhlIGV4aXN0aW5nIHBhcmFtZXRlcnMuIElmIG90aGVyIHByb2plY3RzIGV4aXN0IHdoaWNoIHVzZVxuICAgICAgLy8gdHNpY2tsZS1zdHlsZSBhbm5vdGF0aW9ucyBhbmQgcmVmbGVjdCBvdmVyIHRoZW0gaW4gdGhlIHNhbWUgd2F5LCB0aGlzIGNvdWxkIGNhdXNlIGlzc3VlcyxcbiAgICAgIC8vIGJ1dCB0aGF0IGlzIHZhbmlzaGluZ2x5IHVubGlrZWx5LlxuICAgICAgY2xhenouY3RvclBhcmFtZXRlcnMgPSBjdG9yUGFyYW1ldGVycztcbiAgICB9XG4gICAgaWYgKHByb3BEZWNvcmF0b3JzICE9PSBudWxsKSB7XG4gICAgICAvLyBUaGUgcHJvcGVydHkgZGVjb3JhdG9yIG9iamVjdHMgYXJlIG1lcmdlZCBhcyBpdCBpcyBwb3NzaWJsZSBkaWZmZXJlbnQgZmllbGRzIGhhdmUgZGlmZmVyZW50XG4gICAgICAvLyBkZWNvcmF0b3IgdHlwZXMuIERlY29yYXRvcnMgb24gaW5kaXZpZHVhbCBmaWVsZHMgYXJlIG5vdCBtZXJnZWQsIGFzIGl0J3MgYWxzbyBpbmNyZWRpYmx5XG4gICAgICAvLyB1bmxpa2VseSB0aGF0IGEgZmllbGQgd2lsbCBiZSBkZWNvcmF0ZWQgYm90aCB3aXRoIGFuIEFuZ3VsYXIgZGVjb3JhdG9yIGFuZCBhIG5vbi1Bbmd1bGFyXG4gICAgICAvLyBkZWNvcmF0b3IgdGhhdCdzIGFsc28gYmVlbiBkb3dubGV2ZWxlZC5cbiAgICAgIGlmIChjbGF6ei5wcm9wRGVjb3JhdG9ycyAhPT0gdW5kZWZpbmVkICYmXG4gICAgICAgICAgKCFwYXJlbnRDb25zdHJ1Y3RvciB8fCBwYXJlbnRDb25zdHJ1Y3Rvci5wcm9wRGVjb3JhdG9ycyAhPT0gY2xhenoucHJvcERlY29yYXRvcnMpKSB7XG4gICAgICAgIGNsYXp6LnByb3BEZWNvcmF0b3JzID0gey4uLmNsYXp6LnByb3BEZWNvcmF0b3JzLCAuLi5wcm9wRGVjb3JhdG9yc307XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjbGF6ei5wcm9wRGVjb3JhdG9ycyA9IHByb3BEZWNvcmF0b3JzO1xuICAgICAgfVxuICAgIH1cbiAgfSkgYXMgbmV2ZXI7XG59XG4iXX0=
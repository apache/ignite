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
import { core } from '@angular/compiler';
export class MockSchemaRegistry {
    /**
     * @param {?} existingProperties
     * @param {?} attrPropMapping
     * @param {?} existingElements
     * @param {?} invalidProperties
     * @param {?} invalidAttributes
     */
    constructor(existingProperties, attrPropMapping, existingElements, invalidProperties, invalidAttributes) {
        this.existingProperties = existingProperties;
        this.attrPropMapping = attrPropMapping;
        this.existingElements = existingElements;
        this.invalidProperties = invalidProperties;
        this.invalidAttributes = invalidAttributes;
    }
    /**
     * @param {?} tagName
     * @param {?} property
     * @param {?} schemas
     * @return {?}
     */
    hasProperty(tagName, property, schemas) {
        /** @type {?} */
        const value = this.existingProperties[property];
        return value === void 0 ? true : value;
    }
    /**
     * @param {?} tagName
     * @param {?} schemaMetas
     * @return {?}
     */
    hasElement(tagName, schemaMetas) {
        /** @type {?} */
        const value = this.existingElements[tagName.toLowerCase()];
        return value === void 0 ? true : value;
    }
    /**
     * @return {?}
     */
    allKnownElementNames() { return Object.keys(this.existingElements); }
    /**
     * @param {?} selector
     * @param {?} property
     * @param {?} isAttribute
     * @return {?}
     */
    securityContext(selector, property, isAttribute) {
        return core.SecurityContext.NONE;
    }
    /**
     * @param {?} attrName
     * @return {?}
     */
    getMappedPropName(attrName) { return this.attrPropMapping[attrName] || attrName; }
    /**
     * @return {?}
     */
    getDefaultComponentElementName() { return 'ng-component'; }
    /**
     * @param {?} name
     * @return {?}
     */
    validateProperty(name) {
        if (this.invalidProperties.indexOf(name) > -1) {
            return { error: true, msg: `Binding to property '${name}' is disallowed for security reasons` };
        }
        else {
            return { error: false };
        }
    }
    /**
     * @param {?} name
     * @return {?}
     */
    validateAttribute(name) {
        if (this.invalidAttributes.indexOf(name) > -1) {
            return {
                error: true,
                msg: `Binding to attribute '${name}' is disallowed for security reasons`
            };
        }
        else {
            return { error: false };
        }
    }
    /**
     * @param {?} propName
     * @return {?}
     */
    normalizeAnimationStyleProperty(propName) { return propName; }
    /**
     * @param {?} camelCaseProp
     * @param {?} userProvidedProp
     * @param {?} val
     * @return {?}
     */
    normalizeAnimationStyleValue(camelCaseProp, userProvidedProp, val) {
        return { error: (/** @type {?} */ (null)), value: val.toString() };
    }
}
if (false) {
    /** @type {?} */
    MockSchemaRegistry.prototype.existingProperties;
    /** @type {?} */
    MockSchemaRegistry.prototype.attrPropMapping;
    /** @type {?} */
    MockSchemaRegistry.prototype.existingElements;
    /** @type {?} */
    MockSchemaRegistry.prototype.invalidProperties;
    /** @type {?} */
    MockSchemaRegistry.prototype.invalidAttributes;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2NoZW1hX3JlZ2lzdHJ5X21vY2suanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci90ZXN0aW5nL3NyYy9zY2hlbWFfcmVnaXN0cnlfbW9jay50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBd0IsSUFBSSxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFFOUQsTUFBTSxPQUFPLGtCQUFrQjs7Ozs7Ozs7SUFDN0IsWUFDVyxrQkFBNEMsRUFDNUMsZUFBd0MsRUFDeEMsZ0JBQTBDLEVBQVMsaUJBQWdDLEVBQ25GLGlCQUFnQztRQUhoQyx1QkFBa0IsR0FBbEIsa0JBQWtCLENBQTBCO1FBQzVDLG9CQUFlLEdBQWYsZUFBZSxDQUF5QjtRQUN4QyxxQkFBZ0IsR0FBaEIsZ0JBQWdCLENBQTBCO1FBQVMsc0JBQWlCLEdBQWpCLGlCQUFpQixDQUFlO1FBQ25GLHNCQUFpQixHQUFqQixpQkFBaUIsQ0FBZTtJQUFHLENBQUM7Ozs7Ozs7SUFFL0MsV0FBVyxDQUFDLE9BQWUsRUFBRSxRQUFnQixFQUFFLE9BQThCOztjQUNyRSxLQUFLLEdBQUcsSUFBSSxDQUFDLGtCQUFrQixDQUFDLFFBQVEsQ0FBQztRQUMvQyxPQUFPLEtBQUssS0FBSyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxLQUFLLENBQUM7SUFDekMsQ0FBQzs7Ozs7O0lBRUQsVUFBVSxDQUFDLE9BQWUsRUFBRSxXQUFrQzs7Y0FDdEQsS0FBSyxHQUFHLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxPQUFPLENBQUMsV0FBVyxFQUFFLENBQUM7UUFDMUQsT0FBTyxLQUFLLEtBQUssS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDO0lBQ3pDLENBQUM7Ozs7SUFFRCxvQkFBb0IsS0FBZSxPQUFPLE1BQU0sQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLGdCQUFnQixDQUFDLENBQUMsQ0FBQyxDQUFDOzs7Ozs7O0lBRS9FLGVBQWUsQ0FBQyxRQUFnQixFQUFFLFFBQWdCLEVBQUUsV0FBb0I7UUFDdEUsT0FBTyxJQUFJLENBQUMsZUFBZSxDQUFDLElBQUksQ0FBQztJQUNuQyxDQUFDOzs7OztJQUVELGlCQUFpQixDQUFDLFFBQWdCLElBQVksT0FBTyxJQUFJLENBQUMsZUFBZSxDQUFDLFFBQVEsQ0FBQyxJQUFJLFFBQVEsQ0FBQyxDQUFDLENBQUM7Ozs7SUFFbEcsOEJBQThCLEtBQWEsT0FBTyxjQUFjLENBQUMsQ0FBQyxDQUFDOzs7OztJQUVuRSxnQkFBZ0IsQ0FBQyxJQUFZO1FBQzNCLElBQUksSUFBSSxDQUFDLGlCQUFpQixDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsRUFBRTtZQUM3QyxPQUFPLEVBQUMsS0FBSyxFQUFFLElBQUksRUFBRSxHQUFHLEVBQUUsd0JBQXdCLElBQUksc0NBQXNDLEVBQUMsQ0FBQztTQUMvRjthQUFNO1lBQ0wsT0FBTyxFQUFDLEtBQUssRUFBRSxLQUFLLEVBQUMsQ0FBQztTQUN2QjtJQUNILENBQUM7Ozs7O0lBRUQsaUJBQWlCLENBQUMsSUFBWTtRQUM1QixJQUFJLElBQUksQ0FBQyxpQkFBaUIsQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQyxDQUFDLEVBQUU7WUFDN0MsT0FBTztnQkFDTCxLQUFLLEVBQUUsSUFBSTtnQkFDWCxHQUFHLEVBQUUseUJBQXlCLElBQUksc0NBQXNDO2FBQ3pFLENBQUM7U0FDSDthQUFNO1lBQ0wsT0FBTyxFQUFDLEtBQUssRUFBRSxLQUFLLEVBQUMsQ0FBQztTQUN2QjtJQUNILENBQUM7Ozs7O0lBRUQsK0JBQStCLENBQUMsUUFBZ0IsSUFBWSxPQUFPLFFBQVEsQ0FBQyxDQUFDLENBQUM7Ozs7Ozs7SUFDOUUsNEJBQTRCLENBQUMsYUFBcUIsRUFBRSxnQkFBd0IsRUFBRSxHQUFrQjtRQUU5RixPQUFPLEVBQUMsS0FBSyxFQUFFLG1CQUFBLElBQUksRUFBRSxFQUFFLEtBQUssRUFBRSxHQUFHLENBQUMsUUFBUSxFQUFFLEVBQUMsQ0FBQztJQUNoRCxDQUFDO0NBQ0Y7OztJQWpESyxnREFBbUQ7O0lBQ25ELDZDQUErQzs7SUFDL0MsOENBQWlEOztJQUFFLCtDQUF1Qzs7SUFDMUYsK0NBQXVDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0VsZW1lbnRTY2hlbWFSZWdpc3RyeSwgY29yZX0gZnJvbSAnQGFuZ3VsYXIvY29tcGlsZXInO1xuXG5leHBvcnQgY2xhc3MgTW9ja1NjaGVtYVJlZ2lzdHJ5IGltcGxlbWVudHMgRWxlbWVudFNjaGVtYVJlZ2lzdHJ5IHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgZXhpc3RpbmdQcm9wZXJ0aWVzOiB7W2tleTogc3RyaW5nXTogYm9vbGVhbn0sXG4gICAgICBwdWJsaWMgYXR0clByb3BNYXBwaW5nOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSxcbiAgICAgIHB1YmxpYyBleGlzdGluZ0VsZW1lbnRzOiB7W2tleTogc3RyaW5nXTogYm9vbGVhbn0sIHB1YmxpYyBpbnZhbGlkUHJvcGVydGllczogQXJyYXk8c3RyaW5nPixcbiAgICAgIHB1YmxpYyBpbnZhbGlkQXR0cmlidXRlczogQXJyYXk8c3RyaW5nPikge31cblxuICBoYXNQcm9wZXJ0eSh0YWdOYW1lOiBzdHJpbmcsIHByb3BlcnR5OiBzdHJpbmcsIHNjaGVtYXM6IGNvcmUuU2NoZW1hTWV0YWRhdGFbXSk6IGJvb2xlYW4ge1xuICAgIGNvbnN0IHZhbHVlID0gdGhpcy5leGlzdGluZ1Byb3BlcnRpZXNbcHJvcGVydHldO1xuICAgIHJldHVybiB2YWx1ZSA9PT0gdm9pZCAwID8gdHJ1ZSA6IHZhbHVlO1xuICB9XG5cbiAgaGFzRWxlbWVudCh0YWdOYW1lOiBzdHJpbmcsIHNjaGVtYU1ldGFzOiBjb3JlLlNjaGVtYU1ldGFkYXRhW10pOiBib29sZWFuIHtcbiAgICBjb25zdCB2YWx1ZSA9IHRoaXMuZXhpc3RpbmdFbGVtZW50c1t0YWdOYW1lLnRvTG93ZXJDYXNlKCldO1xuICAgIHJldHVybiB2YWx1ZSA9PT0gdm9pZCAwID8gdHJ1ZSA6IHZhbHVlO1xuICB9XG5cbiAgYWxsS25vd25FbGVtZW50TmFtZXMoKTogc3RyaW5nW10geyByZXR1cm4gT2JqZWN0LmtleXModGhpcy5leGlzdGluZ0VsZW1lbnRzKTsgfVxuXG4gIHNlY3VyaXR5Q29udGV4dChzZWxlY3Rvcjogc3RyaW5nLCBwcm9wZXJ0eTogc3RyaW5nLCBpc0F0dHJpYnV0ZTogYm9vbGVhbik6IGNvcmUuU2VjdXJpdHlDb250ZXh0IHtcbiAgICByZXR1cm4gY29yZS5TZWN1cml0eUNvbnRleHQuTk9ORTtcbiAgfVxuXG4gIGdldE1hcHBlZFByb3BOYW1lKGF0dHJOYW1lOiBzdHJpbmcpOiBzdHJpbmcgeyByZXR1cm4gdGhpcy5hdHRyUHJvcE1hcHBpbmdbYXR0ck5hbWVdIHx8IGF0dHJOYW1lOyB9XG5cbiAgZ2V0RGVmYXVsdENvbXBvbmVudEVsZW1lbnROYW1lKCk6IHN0cmluZyB7IHJldHVybiAnbmctY29tcG9uZW50JzsgfVxuXG4gIHZhbGlkYXRlUHJvcGVydHkobmFtZTogc3RyaW5nKToge2Vycm9yOiBib29sZWFuLCBtc2c/OiBzdHJpbmd9IHtcbiAgICBpZiAodGhpcy5pbnZhbGlkUHJvcGVydGllcy5pbmRleE9mKG5hbWUpID4gLTEpIHtcbiAgICAgIHJldHVybiB7ZXJyb3I6IHRydWUsIG1zZzogYEJpbmRpbmcgdG8gcHJvcGVydHkgJyR7bmFtZX0nIGlzIGRpc2FsbG93ZWQgZm9yIHNlY3VyaXR5IHJlYXNvbnNgfTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHtlcnJvcjogZmFsc2V9O1xuICAgIH1cbiAgfVxuXG4gIHZhbGlkYXRlQXR0cmlidXRlKG5hbWU6IHN0cmluZyk6IHtlcnJvcjogYm9vbGVhbiwgbXNnPzogc3RyaW5nfSB7XG4gICAgaWYgKHRoaXMuaW52YWxpZEF0dHJpYnV0ZXMuaW5kZXhPZihuYW1lKSA+IC0xKSB7XG4gICAgICByZXR1cm4ge1xuICAgICAgICBlcnJvcjogdHJ1ZSxcbiAgICAgICAgbXNnOiBgQmluZGluZyB0byBhdHRyaWJ1dGUgJyR7bmFtZX0nIGlzIGRpc2FsbG93ZWQgZm9yIHNlY3VyaXR5IHJlYXNvbnNgXG4gICAgICB9O1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4ge2Vycm9yOiBmYWxzZX07XG4gICAgfVxuICB9XG5cbiAgbm9ybWFsaXplQW5pbWF0aW9uU3R5bGVQcm9wZXJ0eShwcm9wTmFtZTogc3RyaW5nKTogc3RyaW5nIHsgcmV0dXJuIHByb3BOYW1lOyB9XG4gIG5vcm1hbGl6ZUFuaW1hdGlvblN0eWxlVmFsdWUoY2FtZWxDYXNlUHJvcDogc3RyaW5nLCB1c2VyUHJvdmlkZWRQcm9wOiBzdHJpbmcsIHZhbDogc3RyaW5nfG51bWJlcik6XG4gICAgICB7ZXJyb3I6IHN0cmluZywgdmFsdWU6IHN0cmluZ30ge1xuICAgIHJldHVybiB7ZXJyb3I6IG51bGwgISwgdmFsdWU6IHZhbC50b1N0cmluZygpfTtcbiAgfVxufVxuIl19
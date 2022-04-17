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
/**
 * Store contextual information about a `RouterOutlet`
 *
 * \@publicApi
 */
export class OutletContext {
    constructor() {
        this.outlet = null;
        this.route = null;
        this.resolver = null;
        this.children = new ChildrenOutletContexts();
        this.attachRef = null;
    }
}
if (false) {
    /** @type {?} */
    OutletContext.prototype.outlet;
    /** @type {?} */
    OutletContext.prototype.route;
    /** @type {?} */
    OutletContext.prototype.resolver;
    /** @type {?} */
    OutletContext.prototype.children;
    /** @type {?} */
    OutletContext.prototype.attachRef;
}
/**
 * Store contextual information about the children (= nested) `RouterOutlet`
 *
 * \@publicApi
 */
export class ChildrenOutletContexts {
    constructor() {
        // contexts for child outlets, by name.
        this.contexts = new Map();
    }
    /**
     * Called when a `RouterOutlet` directive is instantiated
     * @param {?} childName
     * @param {?} outlet
     * @return {?}
     */
    onChildOutletCreated(childName, outlet) {
        /** @type {?} */
        const context = this.getOrCreateContext(childName);
        context.outlet = outlet;
        this.contexts.set(childName, context);
    }
    /**
     * Called when a `RouterOutlet` directive is destroyed.
     * We need to keep the context as the outlet could be destroyed inside a NgIf and might be
     * re-created later.
     * @param {?} childName
     * @return {?}
     */
    onChildOutletDestroyed(childName) {
        /** @type {?} */
        const context = this.getContext(childName);
        if (context) {
            context.outlet = null;
        }
    }
    /**
     * Called when the corresponding route is deactivated during navigation.
     * Because the component get destroyed, all children outlet are destroyed.
     * @return {?}
     */
    onOutletDeactivated() {
        /** @type {?} */
        const contexts = this.contexts;
        this.contexts = new Map();
        return contexts;
    }
    /**
     * @param {?} contexts
     * @return {?}
     */
    onOutletReAttached(contexts) { this.contexts = contexts; }
    /**
     * @param {?} childName
     * @return {?}
     */
    getOrCreateContext(childName) {
        /** @type {?} */
        let context = this.getContext(childName);
        if (!context) {
            context = new OutletContext();
            this.contexts.set(childName, context);
        }
        return context;
    }
    /**
     * @param {?} childName
     * @return {?}
     */
    getContext(childName) { return this.contexts.get(childName) || null; }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    ChildrenOutletContexts.prototype.contexts;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicm91dGVyX291dGxldF9jb250ZXh0LmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcm91dGVyL3NyYy9yb3V0ZXJfb3V0bGV0X2NvbnRleHQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7Ozs7OztBQW1CQSxNQUFNLE9BQU8sYUFBYTtJQUExQjtRQUNFLFdBQU0sR0FBc0IsSUFBSSxDQUFDO1FBQ2pDLFVBQUssR0FBd0IsSUFBSSxDQUFDO1FBQ2xDLGFBQVEsR0FBa0MsSUFBSSxDQUFDO1FBQy9DLGFBQVEsR0FBRyxJQUFJLHNCQUFzQixFQUFFLENBQUM7UUFDeEMsY0FBUyxHQUEyQixJQUFJLENBQUM7SUFDM0MsQ0FBQztDQUFBOzs7SUFMQywrQkFBaUM7O0lBQ2pDLDhCQUFrQzs7SUFDbEMsaUNBQStDOztJQUMvQyxpQ0FBd0M7O0lBQ3hDLGtDQUF5Qzs7Ozs7OztBQVEzQyxNQUFNLE9BQU8sc0JBQXNCO0lBQW5DOztRQUVVLGFBQVEsR0FBRyxJQUFJLEdBQUcsRUFBeUIsQ0FBQztJQTZDdEQsQ0FBQzs7Ozs7OztJQTFDQyxvQkFBb0IsQ0FBQyxTQUFpQixFQUFFLE1BQW9COztjQUNwRCxPQUFPLEdBQUcsSUFBSSxDQUFDLGtCQUFrQixDQUFDLFNBQVMsQ0FBQztRQUNsRCxPQUFPLENBQUMsTUFBTSxHQUFHLE1BQU0sQ0FBQztRQUN4QixJQUFJLENBQUMsUUFBUSxDQUFDLEdBQUcsQ0FBQyxTQUFTLEVBQUUsT0FBTyxDQUFDLENBQUM7SUFDeEMsQ0FBQzs7Ozs7Ozs7SUFPRCxzQkFBc0IsQ0FBQyxTQUFpQjs7Y0FDaEMsT0FBTyxHQUFHLElBQUksQ0FBQyxVQUFVLENBQUMsU0FBUyxDQUFDO1FBQzFDLElBQUksT0FBTyxFQUFFO1lBQ1gsT0FBTyxDQUFDLE1BQU0sR0FBRyxJQUFJLENBQUM7U0FDdkI7SUFDSCxDQUFDOzs7Ozs7SUFNRCxtQkFBbUI7O2NBQ1gsUUFBUSxHQUFHLElBQUksQ0FBQyxRQUFRO1FBQzlCLElBQUksQ0FBQyxRQUFRLEdBQUcsSUFBSSxHQUFHLEVBQUUsQ0FBQztRQUMxQixPQUFPLFFBQVEsQ0FBQztJQUNsQixDQUFDOzs7OztJQUVELGtCQUFrQixDQUFDLFFBQW9DLElBQUksSUFBSSxDQUFDLFFBQVEsR0FBRyxRQUFRLENBQUMsQ0FBQyxDQUFDOzs7OztJQUV0RixrQkFBa0IsQ0FBQyxTQUFpQjs7WUFDOUIsT0FBTyxHQUFHLElBQUksQ0FBQyxVQUFVLENBQUMsU0FBUyxDQUFDO1FBRXhDLElBQUksQ0FBQyxPQUFPLEVBQUU7WUFDWixPQUFPLEdBQUcsSUFBSSxhQUFhLEVBQUUsQ0FBQztZQUM5QixJQUFJLENBQUMsUUFBUSxDQUFDLEdBQUcsQ0FBQyxTQUFTLEVBQUUsT0FBTyxDQUFDLENBQUM7U0FDdkM7UUFFRCxPQUFPLE9BQU8sQ0FBQztJQUNqQixDQUFDOzs7OztJQUVELFVBQVUsQ0FBQyxTQUFpQixJQUF3QixPQUFPLElBQUksQ0FBQyxRQUFRLENBQUMsR0FBRyxDQUFDLFNBQVMsQ0FBQyxJQUFJLElBQUksQ0FBQyxDQUFDLENBQUM7Q0FDbkc7Ozs7OztJQTdDQywwQ0FBb0QiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Q29tcG9uZW50RmFjdG9yeVJlc29sdmVyLCBDb21wb25lbnRSZWZ9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQge1JvdXRlck91dGxldH0gZnJvbSAnLi9kaXJlY3RpdmVzL3JvdXRlcl9vdXRsZXQnO1xuaW1wb3J0IHtBY3RpdmF0ZWRSb3V0ZX0gZnJvbSAnLi9yb3V0ZXJfc3RhdGUnO1xuXG5cbi8qKlxuICogU3RvcmUgY29udGV4dHVhbCBpbmZvcm1hdGlvbiBhYm91dCBhIGBSb3V0ZXJPdXRsZXRgXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgT3V0bGV0Q29udGV4dCB7XG4gIG91dGxldDogUm91dGVyT3V0bGV0fG51bGwgPSBudWxsO1xuICByb3V0ZTogQWN0aXZhdGVkUm91dGV8bnVsbCA9IG51bGw7XG4gIHJlc29sdmVyOiBDb21wb25lbnRGYWN0b3J5UmVzb2x2ZXJ8bnVsbCA9IG51bGw7XG4gIGNoaWxkcmVuID0gbmV3IENoaWxkcmVuT3V0bGV0Q29udGV4dHMoKTtcbiAgYXR0YWNoUmVmOiBDb21wb25lbnRSZWY8YW55PnxudWxsID0gbnVsbDtcbn1cblxuLyoqXG4gKiBTdG9yZSBjb250ZXh0dWFsIGluZm9ybWF0aW9uIGFib3V0IHRoZSBjaGlsZHJlbiAoPSBuZXN0ZWQpIGBSb3V0ZXJPdXRsZXRgXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgQ2hpbGRyZW5PdXRsZXRDb250ZXh0cyB7XG4gIC8vIGNvbnRleHRzIGZvciBjaGlsZCBvdXRsZXRzLCBieSBuYW1lLlxuICBwcml2YXRlIGNvbnRleHRzID0gbmV3IE1hcDxzdHJpbmcsIE91dGxldENvbnRleHQ+KCk7XG5cbiAgLyoqIENhbGxlZCB3aGVuIGEgYFJvdXRlck91dGxldGAgZGlyZWN0aXZlIGlzIGluc3RhbnRpYXRlZCAqL1xuICBvbkNoaWxkT3V0bGV0Q3JlYXRlZChjaGlsZE5hbWU6IHN0cmluZywgb3V0bGV0OiBSb3V0ZXJPdXRsZXQpOiB2b2lkIHtcbiAgICBjb25zdCBjb250ZXh0ID0gdGhpcy5nZXRPckNyZWF0ZUNvbnRleHQoY2hpbGROYW1lKTtcbiAgICBjb250ZXh0Lm91dGxldCA9IG91dGxldDtcbiAgICB0aGlzLmNvbnRleHRzLnNldChjaGlsZE5hbWUsIGNvbnRleHQpO1xuICB9XG5cbiAgLyoqXG4gICAqIENhbGxlZCB3aGVuIGEgYFJvdXRlck91dGxldGAgZGlyZWN0aXZlIGlzIGRlc3Ryb3llZC5cbiAgICogV2UgbmVlZCB0byBrZWVwIHRoZSBjb250ZXh0IGFzIHRoZSBvdXRsZXQgY291bGQgYmUgZGVzdHJveWVkIGluc2lkZSBhIE5nSWYgYW5kIG1pZ2h0IGJlXG4gICAqIHJlLWNyZWF0ZWQgbGF0ZXIuXG4gICAqL1xuICBvbkNoaWxkT3V0bGV0RGVzdHJveWVkKGNoaWxkTmFtZTogc3RyaW5nKTogdm9pZCB7XG4gICAgY29uc3QgY29udGV4dCA9IHRoaXMuZ2V0Q29udGV4dChjaGlsZE5hbWUpO1xuICAgIGlmIChjb250ZXh0KSB7XG4gICAgICBjb250ZXh0Lm91dGxldCA9IG51bGw7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIENhbGxlZCB3aGVuIHRoZSBjb3JyZXNwb25kaW5nIHJvdXRlIGlzIGRlYWN0aXZhdGVkIGR1cmluZyBuYXZpZ2F0aW9uLlxuICAgKiBCZWNhdXNlIHRoZSBjb21wb25lbnQgZ2V0IGRlc3Ryb3llZCwgYWxsIGNoaWxkcmVuIG91dGxldCBhcmUgZGVzdHJveWVkLlxuICAgKi9cbiAgb25PdXRsZXREZWFjdGl2YXRlZCgpOiBNYXA8c3RyaW5nLCBPdXRsZXRDb250ZXh0PiB7XG4gICAgY29uc3QgY29udGV4dHMgPSB0aGlzLmNvbnRleHRzO1xuICAgIHRoaXMuY29udGV4dHMgPSBuZXcgTWFwKCk7XG4gICAgcmV0dXJuIGNvbnRleHRzO1xuICB9XG5cbiAgb25PdXRsZXRSZUF0dGFjaGVkKGNvbnRleHRzOiBNYXA8c3RyaW5nLCBPdXRsZXRDb250ZXh0PikgeyB0aGlzLmNvbnRleHRzID0gY29udGV4dHM7IH1cblxuICBnZXRPckNyZWF0ZUNvbnRleHQoY2hpbGROYW1lOiBzdHJpbmcpOiBPdXRsZXRDb250ZXh0IHtcbiAgICBsZXQgY29udGV4dCA9IHRoaXMuZ2V0Q29udGV4dChjaGlsZE5hbWUpO1xuXG4gICAgaWYgKCFjb250ZXh0KSB7XG4gICAgICBjb250ZXh0ID0gbmV3IE91dGxldENvbnRleHQoKTtcbiAgICAgIHRoaXMuY29udGV4dHMuc2V0KGNoaWxkTmFtZSwgY29udGV4dCk7XG4gICAgfVxuXG4gICAgcmV0dXJuIGNvbnRleHQ7XG4gIH1cblxuICBnZXRDb250ZXh0KGNoaWxkTmFtZTogc3RyaW5nKTogT3V0bGV0Q29udGV4dHxudWxsIHsgcmV0dXJuIHRoaXMuY29udGV4dHMuZ2V0KGNoaWxkTmFtZSkgfHwgbnVsbDsgfVxufVxuIl19
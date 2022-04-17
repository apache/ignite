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
import { map } from 'rxjs/operators';
import { ActivationEnd, ChildActivationEnd } from '../events';
import { advanceActivatedRoute } from '../router_state';
import { forEach } from '../utils/collection';
import { nodeChildrenAsMap } from '../utils/tree';
/** @type {?} */
export const activateRoutes = (/**
 * @param {?} rootContexts
 * @param {?} routeReuseStrategy
 * @param {?} forwardEvent
 * @return {?}
 */
(rootContexts, routeReuseStrategy, forwardEvent) => map((/**
 * @param {?} t
 * @return {?}
 */
t => {
    new ActivateRoutes(routeReuseStrategy, (/** @type {?} */ (t.targetRouterState)), t.currentRouterState, forwardEvent)
        .activate(rootContexts);
    return t;
})));
export class ActivateRoutes {
    /**
     * @param {?} routeReuseStrategy
     * @param {?} futureState
     * @param {?} currState
     * @param {?} forwardEvent
     */
    constructor(routeReuseStrategy, futureState, currState, forwardEvent) {
        this.routeReuseStrategy = routeReuseStrategy;
        this.futureState = futureState;
        this.currState = currState;
        this.forwardEvent = forwardEvent;
    }
    /**
     * @param {?} parentContexts
     * @return {?}
     */
    activate(parentContexts) {
        /** @type {?} */
        const futureRoot = this.futureState._root;
        /** @type {?} */
        const currRoot = this.currState ? this.currState._root : null;
        this.deactivateChildRoutes(futureRoot, currRoot, parentContexts);
        advanceActivatedRoute(this.futureState.root);
        this.activateChildRoutes(futureRoot, currRoot, parentContexts);
    }
    // De-activate the child route that are not re-used for the future state
    /**
     * @private
     * @param {?} futureNode
     * @param {?} currNode
     * @param {?} contexts
     * @return {?}
     */
    deactivateChildRoutes(futureNode, currNode, contexts) {
        /** @type {?} */
        const children = nodeChildrenAsMap(currNode);
        // Recurse on the routes active in the future state to de-activate deeper children
        futureNode.children.forEach((/**
         * @param {?} futureChild
         * @return {?}
         */
        futureChild => {
            /** @type {?} */
            const childOutletName = futureChild.value.outlet;
            this.deactivateRoutes(futureChild, children[childOutletName], contexts);
            delete children[childOutletName];
        }));
        // De-activate the routes that will not be re-used
        forEach(children, (/**
         * @param {?} v
         * @param {?} childName
         * @return {?}
         */
        (v, childName) => {
            this.deactivateRouteAndItsChildren(v, contexts);
        }));
    }
    /**
     * @private
     * @param {?} futureNode
     * @param {?} currNode
     * @param {?} parentContext
     * @return {?}
     */
    deactivateRoutes(futureNode, currNode, parentContext) {
        /** @type {?} */
        const future = futureNode.value;
        /** @type {?} */
        const curr = currNode ? currNode.value : null;
        if (future === curr) {
            // Reusing the node, check to see if the children need to be de-activated
            if (future.component) {
                // If we have a normal route, we need to go through an outlet.
                /** @type {?} */
                const context = parentContext.getContext(future.outlet);
                if (context) {
                    this.deactivateChildRoutes(futureNode, currNode, context.children);
                }
            }
            else {
                // if we have a componentless route, we recurse but keep the same outlet map.
                this.deactivateChildRoutes(futureNode, currNode, parentContext);
            }
        }
        else {
            if (curr) {
                // Deactivate the current route which will not be re-used
                this.deactivateRouteAndItsChildren(currNode, parentContext);
            }
        }
    }
    /**
     * @private
     * @param {?} route
     * @param {?} parentContexts
     * @return {?}
     */
    deactivateRouteAndItsChildren(route, parentContexts) {
        if (this.routeReuseStrategy.shouldDetach(route.value.snapshot)) {
            this.detachAndStoreRouteSubtree(route, parentContexts);
        }
        else {
            this.deactivateRouteAndOutlet(route, parentContexts);
        }
    }
    /**
     * @private
     * @param {?} route
     * @param {?} parentContexts
     * @return {?}
     */
    detachAndStoreRouteSubtree(route, parentContexts) {
        /** @type {?} */
        const context = parentContexts.getContext(route.value.outlet);
        if (context && context.outlet) {
            /** @type {?} */
            const componentRef = context.outlet.detach();
            /** @type {?} */
            const contexts = context.children.onOutletDeactivated();
            this.routeReuseStrategy.store(route.value.snapshot, { componentRef, route, contexts });
        }
    }
    /**
     * @private
     * @param {?} route
     * @param {?} parentContexts
     * @return {?}
     */
    deactivateRouteAndOutlet(route, parentContexts) {
        /** @type {?} */
        const context = parentContexts.getContext(route.value.outlet);
        if (context) {
            /** @type {?} */
            const children = nodeChildrenAsMap(route);
            /** @type {?} */
            const contexts = route.value.component ? context.children : parentContexts;
            forEach(children, (/**
             * @param {?} v
             * @param {?} k
             * @return {?}
             */
            (v, k) => this.deactivateRouteAndItsChildren(v, contexts)));
            if (context.outlet) {
                // Destroy the component
                context.outlet.deactivate();
                // Destroy the contexts for all the outlets that were in the component
                context.children.onOutletDeactivated();
            }
        }
    }
    /**
     * @private
     * @param {?} futureNode
     * @param {?} currNode
     * @param {?} contexts
     * @return {?}
     */
    activateChildRoutes(futureNode, currNode, contexts) {
        /** @type {?} */
        const children = nodeChildrenAsMap(currNode);
        futureNode.children.forEach((/**
         * @param {?} c
         * @return {?}
         */
        c => {
            this.activateRoutes(c, children[c.value.outlet], contexts);
            this.forwardEvent(new ActivationEnd(c.value.snapshot));
        }));
        if (futureNode.children.length) {
            this.forwardEvent(new ChildActivationEnd(futureNode.value.snapshot));
        }
    }
    /**
     * @private
     * @param {?} futureNode
     * @param {?} currNode
     * @param {?} parentContexts
     * @return {?}
     */
    activateRoutes(futureNode, currNode, parentContexts) {
        /** @type {?} */
        const future = futureNode.value;
        /** @type {?} */
        const curr = currNode ? currNode.value : null;
        advanceActivatedRoute(future);
        // reusing the node
        if (future === curr) {
            if (future.component) {
                // If we have a normal route, we need to go through an outlet.
                /** @type {?} */
                const context = parentContexts.getOrCreateContext(future.outlet);
                this.activateChildRoutes(futureNode, currNode, context.children);
            }
            else {
                // if we have a componentless route, we recurse but keep the same outlet map.
                this.activateChildRoutes(futureNode, currNode, parentContexts);
            }
        }
        else {
            if (future.component) {
                // if we have a normal route, we need to place the component into the outlet and recurse.
                /** @type {?} */
                const context = parentContexts.getOrCreateContext(future.outlet);
                if (this.routeReuseStrategy.shouldAttach(future.snapshot)) {
                    /** @type {?} */
                    const stored = ((/** @type {?} */ (this.routeReuseStrategy.retrieve(future.snapshot))));
                    this.routeReuseStrategy.store(future.snapshot, null);
                    context.children.onOutletReAttached(stored.contexts);
                    context.attachRef = stored.componentRef;
                    context.route = stored.route.value;
                    if (context.outlet) {
                        // Attach right away when the outlet has already been instantiated
                        // Otherwise attach from `RouterOutlet.ngOnInit` when it is instantiated
                        context.outlet.attach(stored.componentRef, stored.route.value);
                    }
                    advanceActivatedRouteNodeAndItsChildren(stored.route);
                }
                else {
                    /** @type {?} */
                    const config = parentLoadedConfig(future.snapshot);
                    /** @type {?} */
                    const cmpFactoryResolver = config ? config.module.componentFactoryResolver : null;
                    context.attachRef = null;
                    context.route = future;
                    context.resolver = cmpFactoryResolver;
                    if (context.outlet) {
                        // Activate the outlet when it has already been instantiated
                        // Otherwise it will get activated from its `ngOnInit` when instantiated
                        context.outlet.activateWith(future, cmpFactoryResolver);
                    }
                    this.activateChildRoutes(futureNode, null, context.children);
                }
            }
            else {
                // if we have a componentless route, we recurse but keep the same outlet map.
                this.activateChildRoutes(futureNode, null, parentContexts);
            }
        }
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    ActivateRoutes.prototype.routeReuseStrategy;
    /**
     * @type {?}
     * @private
     */
    ActivateRoutes.prototype.futureState;
    /**
     * @type {?}
     * @private
     */
    ActivateRoutes.prototype.currState;
    /**
     * @type {?}
     * @private
     */
    ActivateRoutes.prototype.forwardEvent;
}
/**
 * @param {?} node
 * @return {?}
 */
function advanceActivatedRouteNodeAndItsChildren(node) {
    advanceActivatedRoute(node.value);
    node.children.forEach(advanceActivatedRouteNodeAndItsChildren);
}
/**
 * @param {?} snapshot
 * @return {?}
 */
function parentLoadedConfig(snapshot) {
    for (let s = snapshot.parent; s; s = s.parent) {
        /** @type {?} */
        const route = s.routeConfig;
        if (route && route._loadedConfig)
            return route._loadedConfig;
        if (route && route.component)
            return null;
    }
    return null;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYWN0aXZhdGVfcm91dGVzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcm91dGVyL3NyYy9vcGVyYXRvcnMvYWN0aXZhdGVfcm91dGVzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBU0EsT0FBTyxFQUFDLEdBQUcsRUFBQyxNQUFNLGdCQUFnQixDQUFDO0FBR25DLE9BQU8sRUFBQyxhQUFhLEVBQUUsa0JBQWtCLEVBQVEsTUFBTSxXQUFXLENBQUM7QUFJbkUsT0FBTyxFQUFzRCxxQkFBcUIsRUFBQyxNQUFNLGlCQUFpQixDQUFDO0FBQzNHLE9BQU8sRUFBQyxPQUFPLEVBQUMsTUFBTSxxQkFBcUIsQ0FBQztBQUM1QyxPQUFPLEVBQVcsaUJBQWlCLEVBQUMsTUFBTSxlQUFlLENBQUM7O0FBRTFELE1BQU0sT0FBTyxjQUFjOzs7Ozs7QUFDdkIsQ0FBQyxZQUFvQyxFQUFFLGtCQUFzQyxFQUM1RSxZQUFrQyxFQUFrRCxFQUFFLENBQ25GLEdBQUc7Ozs7QUFBQyxDQUFDLENBQUMsRUFBRTtJQUNOLElBQUksY0FBYyxDQUNkLGtCQUFrQixFQUFFLG1CQUFBLENBQUMsQ0FBQyxpQkFBaUIsRUFBRSxFQUFFLENBQUMsQ0FBQyxrQkFBa0IsRUFBRSxZQUFZLENBQUM7U0FDN0UsUUFBUSxDQUFDLFlBQVksQ0FBQyxDQUFDO0lBQzVCLE9BQU8sQ0FBQyxDQUFDO0FBQ1gsQ0FBQyxFQUFDLENBQUE7QUFFVixNQUFNLE9BQU8sY0FBYzs7Ozs7OztJQUN6QixZQUNZLGtCQUFzQyxFQUFVLFdBQXdCLEVBQ3hFLFNBQXNCLEVBQVUsWUFBa0M7UUFEbEUsdUJBQWtCLEdBQWxCLGtCQUFrQixDQUFvQjtRQUFVLGdCQUFXLEdBQVgsV0FBVyxDQUFhO1FBQ3hFLGNBQVMsR0FBVCxTQUFTLENBQWE7UUFBVSxpQkFBWSxHQUFaLFlBQVksQ0FBc0I7SUFBRyxDQUFDOzs7OztJQUVsRixRQUFRLENB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cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge01vbm9UeXBlT3BlcmF0b3JGdW5jdGlvbn0gZnJvbSAncnhqcyc7XG5pbXBvcnQge21hcH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuXG5pbXBvcnQge0xvYWRlZFJvdXRlckNvbmZpZ30gZnJvbSAnLi4vY29uZmlnJztcbmltcG9ydCB7QWN0aXZhdGlvbkVuZCwgQ2hpbGRBY3RpdmF0aW9uRW5kLCBFdmVudH0gZnJvbSAnLi4vZXZlbnRzJztcbmltcG9ydCB7RGV0YWNoZWRSb3V0ZUhhbmRsZUludGVybmFsLCBSb3V0ZVJldXNlU3RyYXRlZ3l9IGZyb20gJy4uL3JvdXRlX3JldXNlX3N0cmF0ZWd5JztcbmltcG9ydCB7TmF2aWdhdGlvblRyYW5zaXRpb259IGZyb20gJy4uL3JvdXRlcic7XG5pbXBvcnQge0NoaWxkcmVuT3V0bGV0Q29udGV4dHN9IGZyb20gJy4uL3JvdXRlcl9vdXRsZXRfY29udGV4dCc7XG5pbXBvcnQge0FjdGl2YXRlZFJvdXRlLCBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90LCBSb3V0ZXJTdGF0ZSwgYWR2YW5jZUFjdGl2YXRlZFJvdXRlfSBmcm9tICcuLi9yb3V0ZXJfc3RhdGUnO1xuaW1wb3J0IHtmb3JFYWNofSBmcm9tICcuLi91dGlscy9jb2xsZWN0aW9uJztcbmltcG9ydCB7VHJlZU5vZGUsIG5vZGVDaGlsZHJlbkFzTWFwfSBmcm9tICcuLi91dGlscy90cmVlJztcblxuZXhwb3J0IGNvbnN0IGFjdGl2YXRlUm91dGVzID1cbiAgICAocm9vdENvbnRleHRzOiBDaGlsZHJlbk91dGxldENvbnRleHRzLCByb3V0ZVJldXNlU3RyYXRlZ3k6IFJvdXRlUmV1c2VTdHJhdGVneSxcbiAgICAgZm9yd2FyZEV2ZW50OiAoZXZ0OiBFdmVudCkgPT4gdm9pZCk6IE1vbm9UeXBlT3BlcmF0b3JGdW5jdGlvbjxOYXZpZ2F0aW9uVHJhbnNpdGlvbj4gPT5cbiAgICAgICAgbWFwKHQgPT4ge1xuICAgICAgICAgIG5ldyBBY3RpdmF0ZVJvdXRlcyhcbiAgICAgICAgICAgICAgcm91dGVSZXVzZVN0cmF0ZWd5LCB0LnRhcmdldFJvdXRlclN0YXRlICEsIHQuY3VycmVudFJvdXRlclN0YXRlLCBmb3J3YXJkRXZlbnQpXG4gICAgICAgICAgICAgIC5hY3RpdmF0ZShyb290Q29udGV4dHMpO1xuICAgICAgICAgIHJldHVybiB0O1xuICAgICAgICB9KTtcblxuZXhwb3J0IGNsYXNzIEFjdGl2YXRlUm91dGVzIHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIHJvdXRlUmV1c2VTdHJhdGVneTogUm91dGVSZXVzZVN0cmF0ZWd5LCBwcml2YXRlIGZ1dHVyZVN0YXRlOiBSb3V0ZXJTdGF0ZSxcbiAgICAgIHByaXZhdGUgY3VyclN0YXRlOiBSb3V0ZXJTdGF0ZSwgcHJpdmF0ZSBmb3J3YXJkRXZlbnQ6IChldnQ6IEV2ZW50KSA9PiB2b2lkKSB7fVxuXG4gIGFjdGl2YXRlKHBhcmVudENvbnRleHRzOiBDaGlsZHJlbk91dGxldENvbnRleHRzKTogdm9pZCB7XG4gICAgY29uc3QgZnV0dXJlUm9vdCA9IHRoaXMuZnV0dXJlU3RhdGUuX3Jvb3Q7XG4gICAgY29uc3QgY3VyclJvb3QgPSB0aGlzLmN1cnJTdGF0ZSA/IHRoaXMuY3VyclN0YXRlLl9yb290IDogbnVsbDtcblxuICAgIHRoaXMuZGVhY3RpdmF0ZUNoaWxkUm91dGVzKGZ1dHVyZVJvb3QsIGN1cnJSb290LCBwYXJlbnRDb250ZXh0cyk7XG4gICAgYWR2YW5jZUFjdGl2YXRlZFJvdXRlKHRoaXMuZnV0dXJlU3RhdGUucm9vdCk7XG4gICAgdGhpcy5hY3RpdmF0ZUNoaWxkUm91dGVzKGZ1dHVyZVJvb3QsIGN1cnJSb290LCBwYXJlbnRDb250ZXh0cyk7XG4gIH1cblxuICAvLyBEZS1hY3RpdmF0ZSB0aGUgY2hpbGQgcm91dGUgdGhhdCBhcmUgbm90IHJlLXVzZWQgZm9yIHRoZSBmdXR1cmUgc3RhdGVcbiAgcHJpdmF0ZSBkZWFjdGl2YXRlQ2hpbGRSb3V0ZXMoXG4gICAgICBmdXR1cmVOb2RlOiBUcmVlTm9kZTxBY3RpdmF0ZWRSb3V0ZT4sIGN1cnJOb2RlOiBUcmVlTm9kZTxBY3RpdmF0ZWRSb3V0ZT58bnVsbCxcbiAgICAgIGNvbnRleHRzOiBDaGlsZHJlbk91dGxldENvbnRleHRzKTogdm9pZCB7XG4gICAgY29uc3QgY2hpbGRyZW46IHtbb3V0bGV0TmFtZTogc3RyaW5nXTogVHJlZU5vZGU8QWN0aXZhdGVkUm91dGU+fSA9IG5vZGVDaGlsZHJlbkFzTWFwKGN1cnJOb2RlKTtcblxuICAgIC8vIFJlY3Vyc2Ugb24gdGhlIHJvdXRlcyBhY3RpdmUgaW4gdGhlIGZ1dHVyZSBzdGF0ZSB0byBkZS1hY3RpdmF0ZSBkZWVwZXIgY2hpbGRyZW5cbiAgICBmdXR1cmVOb2RlLmNoaWxkcmVuLmZvckVhY2goZnV0dXJlQ2hpbGQgPT4ge1xuICAgICAgY29uc3QgY2hpbGRPdXRsZXROYW1lID0gZnV0dXJlQ2hpbGQudmFsdWUub3V0bGV0O1xuICAgICAgdGhpcy5kZWFjdGl2YXRlUm91dGVzKGZ1dHVyZUNoaWxkLCBjaGlsZHJlbltjaGlsZE91dGxldE5hbWVdLCBjb250ZXh0cyk7XG4gICAgICBkZWxldGUgY2hpbGRyZW5bY2hpbGRPdXRsZXROYW1lXTtcbiAgICB9KTtcblxuICAgIC8vIERlLWFjdGl2YXRlIHRoZSByb3V0ZXMgdGhhdCB3aWxsIG5vdCBiZSByZS11c2VkXG4gICAgZm9yRWFjaChjaGlsZHJlbiwgKHY6IFRyZWVOb2RlPEFjdGl2YXRlZFJvdXRlPiwgY2hpbGROYW1lOiBzdHJpbmcpID0+IHtcbiAgICAgIHRoaXMuZGVhY3RpdmF0ZVJvdXRlQW5kSXRzQ2hpbGRyZW4odiwgY29udGV4dHMpO1xuICAgIH0pO1xuICB9XG5cbiAgcHJpdmF0ZSBkZWFjdGl2YXRlUm91dGVzKFxuICAgICAgZnV0dXJlTm9kZTogVHJlZU5vZGU8QWN0aXZhdGVkUm91dGU+LCBjdXJyTm9kZTogVHJlZU5vZGU8QWN0aXZhdGVkUm91dGU+LFxuICAgICAgcGFyZW50Q29udGV4dDogQ2hpbGRyZW5PdXRsZXRDb250ZXh0cyk6IHZvaWQge1xuICAgIGNvbnN0IGZ1dHVyZSA9IGZ1dHVyZU5vZGUudmFsdWU7XG4gICAgY29uc3QgY3VyciA9IGN1cnJOb2RlID8gY3Vyck5vZGUudmFsdWUgOiBudWxsO1xuXG4gICAgaWYgKGZ1dHVyZSA9PT0gY3Vycikge1xuICAgICAgLy8gUmV1c2luZyB0aGUgbm9kZSwgY2hlY2sgdG8gc2VlIGlmIHRoZSBjaGlsZHJlbiBuZWVkIHRvIGJlIGRlLWFjdGl2YXRlZFxuICAgICAgaWYgKGZ1dHVyZS5jb21wb25lbnQpIHtcbiAgICAgICAgLy8gSWYgd2UgaGF2ZSBhIG5vcm1hbCByb3V0ZSwgd2UgbmVlZCB0byBnbyB0aHJvdWdoIGFuIG91dGxldC5cbiAgICAgICAgY29uc3QgY29udGV4dCA9IHBhcmVudENvbnRleHQuZ2V0Q29udGV4dChmdXR1cmUub3V0bGV0KTtcbiAgICAgICAgaWYgKGNvbnRleHQpIHtcbiAgICAgICAgICB0aGlzLmRlYWN0aXZhdGVDaGlsZFJvdXRlcyhmdXR1cmVOb2RlLCBjdXJyTm9kZSwgY29udGV4dC5jaGlsZHJlbik7XG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIC8vIGlmIHdlIGhhdmUgYSBjb21wb25lbnRsZXNzIHJvdXRlLCB3ZSByZWN1cnNlIGJ1dCBrZWVwIHRoZSBzYW1lIG91dGxldCBtYXAuXG4gICAgICAgIHRoaXMuZGVhY3RpdmF0ZUNoaWxkUm91dGVzKGZ1dHVyZU5vZGUsIGN1cnJOb2RlLCBwYXJlbnRDb250ZXh0KTtcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKGN1cnIpIHtcbiAgICAgICAgLy8gRGVhY3RpdmF0ZSB0aGUgY3VycmVudCByb3V0ZSB3aGljaCB3aWxsIG5vdCBiZSByZS11c2VkXG4gICAgICAgIHRoaXMuZGVhY3RpdmF0ZVJvdXRlQW5kSXRzQ2hpbGRyZW4oY3Vyck5vZGUsIHBhcmVudENvbnRleHQpO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgZGVhY3RpdmF0ZVJvdXRlQW5kSXRzQ2hpbGRyZW4oXG4gICAgICByb3V0ZTogVHJlZU5vZGU8QWN0aXZhdGVkUm91dGU+LCBwYXJlbnRDb250ZXh0czogQ2hpbGRyZW5PdXRsZXRDb250ZXh0cyk6IHZvaWQge1xuICAgIGlmICh0aGlzLnJvdXRlUmV1c2VTdHJhdGVneS5zaG91bGREZXRhY2gocm91dGUudmFsdWUuc25hcHNob3QpKSB7XG4gICAgICB0aGlzLmRldGFjaEFuZFN0b3JlUm91dGVTdWJ0cmVlKHJvdXRlLCBwYXJlbnRDb250ZXh0cyk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuZGVhY3RpdmF0ZVJvdXRlQW5kT3V0bGV0KHJvdXRlLCBwYXJlbnRDb250ZXh0cyk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBkZXRhY2hBbmRTdG9yZVJvdXRlU3VidHJlZShcbiAgICAgIHJvdXRlOiBUcmVlTm9kZTxBY3RpdmF0ZWRSb3V0ZT4sIHBhcmVudENvbnRleHRzOiBDaGlsZHJlbk91dGxldENvbnRleHRzKTogdm9pZCB7XG4gICAgY29uc3QgY29udGV4dCA9IHBhcmVudENvbnRleHRzLmdldENvbnRleHQocm91dGUudmFsdWUub3V0bGV0KTtcbiAgICBpZiAoY29udGV4dCAmJiBjb250ZXh0Lm91dGxldCkge1xuICAgICAgY29uc3QgY29tcG9uZW50UmVmID0gY29udGV4dC5vdXRsZXQuZGV0YWNoKCk7XG4gICAgICBjb25zdCBjb250ZXh0cyA9IGNvbnRleHQuY2hpbGRyZW4ub25PdXRsZXREZWFjdGl2YXRlZCgpO1xuICAgICAgdGhpcy5yb3V0ZVJldXNlU3RyYXRlZ3kuc3RvcmUocm91dGUudmFsdWUuc25hcHNob3QsIHtjb21wb25lbnRSZWYsIHJvdXRlLCBjb250ZXh0c30pO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgZGVhY3RpdmF0ZVJvdXRlQW5kT3V0bGV0KFxuICAgICAgcm91dGU6IFRyZWVOb2RlPEFjdGl2YXRlZFJvdXRlPiwgcGFyZW50Q29udGV4dHM6IENoaWxkcmVuT3V0bGV0Q29udGV4dHMpOiB2b2lkIHtcbiAgICBjb25zdCBjb250ZXh0ID0gcGFyZW50Q29udGV4dHMuZ2V0Q29udGV4dChyb3V0ZS52YWx1ZS5vdXRsZXQpO1xuXG4gICAgaWYgKGNvbnRleHQpIHtcbiAgICAgIGNvbnN0IGNoaWxkcmVuOiB7W291dGxldE5hbWU6IHN0cmluZ106IGFueX0gPSBub2RlQ2hpbGRyZW5Bc01hcChyb3V0ZSk7XG4gICAgICBjb25zdCBjb250ZXh0cyA9IHJvdXRlLnZhbHVlLmNvbXBvbmVudCA/IGNvbnRleHQuY2hpbGRyZW4gOiBwYXJlbnRDb250ZXh0cztcblxuICAgICAgZm9yRWFjaChjaGlsZHJlbiwgKHY6IGFueSwgazogc3RyaW5nKSA9PiB0aGlzLmRlYWN0aXZhdGVSb3V0ZUFuZEl0c0NoaWxkcmVuKHYsIGNvbnRleHRzKSk7XG5cbiAgICAgIGlmIChjb250ZXh0Lm91dGxldCkge1xuICAgICAgICAvLyBEZXN0cm95IHRoZSBjb21wb25lbnRcbiAgICAgICAgY29udGV4dC5vdXRsZXQuZGVhY3RpdmF0ZSgpO1xuICAgICAgICAvLyBEZXN0cm95IHRoZSBjb250ZXh0cyBmb3IgYWxsIHRoZSBvdXRsZXRzIHRoYXQgd2VyZSBpbiB0aGUgY29tcG9uZW50XG4gICAgICAgIGNvbnRleHQuY2hpbGRyZW4ub25PdXRsZXREZWFjdGl2YXRlZCgpO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgYWN0aXZhdGVDaGlsZFJvdXRlcyhcbiAgICAgIGZ1dHVyZU5vZGU6IFRyZWVOb2RlPEFjdGl2YXRlZFJvdXRlPiwgY3Vyck5vZGU6IFRyZWVOb2RlPEFjdGl2YXRlZFJvdXRlPnxudWxsLFxuICAgICAgY29udGV4dHM6IENoaWxkcmVuT3V0bGV0Q29udGV4dHMpOiB2b2lkIHtcbiAgICBjb25zdCBjaGlsZHJlbjoge1tvdXRsZXQ6IHN0cmluZ106IGFueX0gPSBub2RlQ2hpbGRyZW5Bc01hcChjdXJyTm9kZSk7XG4gICAgZnV0dXJlTm9kZS5jaGlsZHJlbi5mb3JFYWNoKGMgPT4ge1xuICAgICAgdGhpcy5hY3RpdmF0ZVJvdXRlcyhjLCBjaGlsZHJlbltjLnZhbHVlLm91dGxldF0sIGNvbnRleHRzKTtcbiAgICAgIHRoaXMuZm9yd2FyZEV2ZW50KG5ldyBBY3RpdmF0aW9uRW5kKGMudmFsdWUuc25hcHNob3QpKTtcbiAgICB9KTtcbiAgICBpZiAoZnV0dXJlTm9kZS5jaGlsZHJlbi5sZW5ndGgpIHtcbiAgICAgIHRoaXMuZm9yd2FyZEV2ZW50KG5ldyBDaGlsZEFjdGl2YXRpb25FbmQoZnV0dXJlTm9kZS52YWx1ZS5zbmFwc2hvdCkpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgYWN0aXZhdGVSb3V0ZXMoXG4gICAgICBmdXR1cmVOb2RlOiBUcmVlTm9kZTxBY3RpdmF0ZWRSb3V0ZT4sIGN1cnJOb2RlOiBUcmVlTm9kZTxBY3RpdmF0ZWRSb3V0ZT4sXG4gICAgICBwYXJlbnRDb250ZXh0czogQ2hpbGRyZW5PdXRsZXRDb250ZXh0cyk6IHZvaWQge1xuICAgIGNvbnN0IGZ1dHVyZSA9IGZ1dHVyZU5vZGUudmFsdWU7XG4gICAgY29uc3QgY3VyciA9IGN1cnJOb2RlID8gY3Vyck5vZGUudmFsdWUgOiBudWxsO1xuXG4gICAgYWR2YW5jZUFjdGl2YXRlZFJvdXRlKGZ1dHVyZSk7XG5cbiAgICAvLyByZXVzaW5nIHRoZSBub2RlXG4gICAgaWYgKGZ1dHVyZSA9PT0gY3Vycikge1xuICAgICAgaWYgKGZ1dHVyZS5jb21wb25lbnQpIHtcbiAgICAgICAgLy8gSWYgd2UgaGF2ZSBhIG5vcm1hbCByb3V0ZSwgd2UgbmVlZCB0byBnbyB0aHJvdWdoIGFuIG91dGxldC5cbiAgICAgICAgY29uc3QgY29udGV4dCA9IHBhcmVudENvbnRleHRzLmdldE9yQ3JlYXRlQ29udGV4dChmdXR1cmUub3V0bGV0KTtcbiAgICAgICAgdGhpcy5hY3RpdmF0ZUNoaWxkUm91dGVzKGZ1dHVyZU5vZGUsIGN1cnJOb2RlLCBjb250ZXh0LmNoaWxkcmVuKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIC8vIGlmIHdlIGhhdmUgYSBjb21wb25lbnRsZXNzIHJvdXRlLCB3ZSByZWN1cnNlIGJ1dCBrZWVwIHRoZSBzYW1lIG91dGxldCBtYXAuXG4gICAgICAgIHRoaXMuYWN0aXZhdGVDaGlsZFJvdXRlcyhmdXR1cmVOb2RlLCBjdXJyTm9kZSwgcGFyZW50Q29udGV4dHMpO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICBpZiAoZnV0dXJlLmNvbXBvbmVudCkge1xuICAgICAgICAvLyBpZiB3ZSBoYXZlIGEgbm9ybWFsIHJvdXRlLCB3ZSBuZWVkIHRvIHBsYWNlIHRoZSBjb21wb25lbnQgaW50byB0aGUgb3V0bGV0IGFuZCByZWN1cnNlLlxuICAgICAgICBjb25zdCBjb250ZXh0ID0gcGFyZW50Q29udGV4dHMuZ2V0T3JDcmVhdGVDb250ZXh0KGZ1dHVyZS5vdXRsZXQpO1xuXG4gICAgICAgIGlmICh0aGlzLnJvdXRlUmV1c2VTdHJhdGVneS5zaG91bGRBdHRhY2goZnV0dXJlLnNuYXBzaG90KSkge1xuICAgICAgICAgIGNvbnN0IHN0b3JlZCA9XG4gICAgICAgICAgICAgICg8RGV0YWNoZWRSb3V0ZUhhbmRsZUludGVybmFsPnRoaXMucm91dGVSZXVzZVN0cmF0ZWd5LnJldHJpZXZlKGZ1dHVyZS5zbmFwc2hvdCkpO1xuICAgICAgICAgIHRoaXMucm91dGVSZXVzZVN0cmF0ZWd5LnN0b3JlKGZ1dHVyZS5zbmFwc2hvdCwgbnVsbCk7XG4gICAgICAgICAgY29udGV4dC5jaGlsZHJlbi5vbk91dGxldFJlQXR0YWNoZWQoc3RvcmVkLmNvbnRleHRzKTtcbiAgICAgICAgICBjb250ZXh0LmF0dGFjaFJlZiA9IHN0b3JlZC5jb21wb25lbnRSZWY7XG4gICAgICAgICAgY29udGV4dC5yb3V0ZSA9IHN0b3JlZC5yb3V0ZS52YWx1ZTtcbiAgICAgICAgICBpZiAoY29udGV4dC5vdXRsZXQpIHtcbiAgICAgICAgICAgIC8vIEF0dGFjaCByaWdodCBhd2F5IHdoZW4gdGhlIG91dGxldCBoYXMgYWxyZWFkeSBiZWVuIGluc3RhbnRpYXRlZFxuICAgICAgICAgICAgLy8gT3RoZXJ3aXNlIGF0dGFjaCBmcm9tIGBSb3V0ZXJPdXRsZXQubmdPbkluaXRgIHdoZW4gaXQgaXMgaW5zdGFudGlhdGVkXG4gICAgICAgICAgICBjb250ZXh0Lm91dGxldC5hdHRhY2goc3RvcmVkLmNvbXBvbmVudFJlZiwgc3RvcmVkLnJvdXRlLnZhbHVlKTtcbiAgICAgICAgICB9XG4gICAgICAgICAgYWR2YW5jZUFjdGl2YXRlZFJvdXRlTm9kZUFuZEl0c0NoaWxkcmVuKHN0b3JlZC5yb3V0ZSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgY29uc3QgY29uZmlnID0gcGFyZW50TG9hZGVkQ29uZmlnKGZ1dHVyZS5zbmFwc2hvdCk7XG4gICAgICAgICAgY29uc3QgY21wRmFjdG9yeVJlc29sdmVyID0gY29uZmlnID8gY29uZmlnLm1vZHVsZS5jb21wb25lbnRGYWN0b3J5UmVzb2x2ZXIgOiBudWxsO1xuXG4gICAgICAgICAgY29udGV4dC5hdHRhY2hSZWYgPSBudWxsO1xuICAgICAgICAgIGNvbnRleHQucm91dGUgPSBmdXR1cmU7XG4gICAgICAgICAgY29udGV4dC5yZXNvbHZlciA9IGNtcEZhY3RvcnlSZXNvbHZlcjtcbiAgICAgICAgICBpZiAoY29udGV4dC5vdXRsZXQpIHtcbiAgICAgICAgICAgIC8vIEFjdGl2YXRlIHRoZSBvdXRsZXQgd2hlbiBpdCBoYXMgYWxyZWFkeSBiZWVuIGluc3RhbnRpYXRlZFxuICAgICAgICAgICAgLy8gT3RoZXJ3aXNlIGl0IHdpbGwgZ2V0IGFjdGl2YXRlZCBmcm9tIGl0cyBgbmdPbkluaXRgIHdoZW4gaW5zdGFudGlhdGVkXG4gICAgICAgICAgICBjb250ZXh0Lm91dGxldC5hY3RpdmF0ZVdpdGgoZnV0dXJlLCBjbXBGYWN0b3J5UmVzb2x2ZXIpO1xuICAgICAgICAgIH1cblxuICAgICAgICAgIHRoaXMuYWN0aXZhdGVDaGlsZFJvdXRlcyhmdXR1cmVOb2RlLCBudWxsLCBjb250ZXh0LmNoaWxkcmVuKTtcbiAgICAgICAgfVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgLy8gaWYgd2UgaGF2ZSBhIGNvbXBvbmVudGxlc3Mgcm91dGUsIHdlIHJlY3Vyc2UgYnV0IGtlZXAgdGhlIHNhbWUgb3V0bGV0IG1hcC5cbiAgICAgICAgdGhpcy5hY3RpdmF0ZUNoaWxkUm91dGVzKGZ1dHVyZU5vZGUsIG51bGwsIHBhcmVudENvbnRleHRzKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cbn1cblxuZnVuY3Rpb24gYWR2YW5jZUFjdGl2YXRlZFJvdXRlTm9kZUFuZEl0c0NoaWxkcmVuKG5vZGU6IFRyZWVOb2RlPEFjdGl2YXRlZFJvdXRlPik6IHZvaWQge1xuICBhZHZhbmNlQWN0aXZhdGVkUm91dGUobm9kZS52YWx1ZSk7XG4gIG5vZGUuY2hpbGRyZW4uZm9yRWFjaChhZHZhbmNlQWN0aXZhdGVkUm91dGVOb2RlQW5kSXRzQ2hpbGRyZW4pO1xufVxuXG5mdW5jdGlvbiBwYXJlbnRMb2FkZWRDb25maWcoc25hcHNob3Q6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QpOiBMb2FkZWRSb3V0ZXJDb25maWd8bnVsbCB7XG4gIGZvciAobGV0IHMgPSBzbmFwc2hvdC5wYXJlbnQ7IHM7IHMgPSBzLnBhcmVudCkge1xuICAgIGNvbnN0IHJvdXRlID0gcy5yb3V0ZUNvbmZpZztcbiAgICBpZiAocm91dGUgJiYgcm91dGUuX2xvYWRlZENvbmZpZykgcmV0dXJuIHJvdXRlLl9sb2FkZWRDb25maWc7XG4gICAgaWYgKHJvdXRlICYmIHJvdXRlLmNvbXBvbmVudCkgcmV0dXJuIG51bGw7XG4gIH1cblxuICByZXR1cm4gbnVsbDtcbn0iXX0=
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
import { assertLView } from '../assert';
import { discoverLocalRefs, getComponentAtNodeIndex, getDirectivesAtNodeIndex, getLContext } from '../context_discovery';
import { NodeInjector } from '../di';
import { CLEANUP, CONTEXT, FLAGS, HOST, TVIEW } from '../interfaces/view';
import { stringifyForError } from './misc_utils';
import { getLViewParent, getRootContext } from './view_traversal_utils';
import { unwrapRNode } from './view_utils';
/**
 * Returns the component instance associated with a given DOM host element.
 * Elements which don't represent components return `null`.
 *
 * \@publicApi
 * @template T
 * @param {?} element Host DOM element from which the component should be retrieved.
 *
 * ```
 * <my-app>
 *   #VIEW
 *     <div>
 *       <child-comp></child-comp>
 *     </div>
 * </mp-app>
 *
 * expect(getComponent(<child-comp>) instanceof ChildComponent).toBeTruthy();
 * expect(getComponent(<my-app>) instanceof MyApp).toBeTruthy();
 * ```
 *
 * @return {?}
 */
export function getComponent(element) {
    /** @type {?} */
    const context = loadLContextFromNode(element);
    if (context.component === undefined) {
        context.component = getComponentAtNodeIndex(context.nodeIndex, context.lView);
    }
    return (/** @type {?} */ (context.component));
}
/**
 * Returns the component instance associated with a given DOM host element.
 * Elements which don't represent components return `null`.
 *
 * \@publicApi
 * @template T
 * @param {?} element Host DOM element from which the component should be retrieved.
 *
 * ```
 * <my-app>
 *   #VIEW
 *     <div>
 *       <child-comp></child-comp>
 *     </div>
 * </mp-app>
 *
 * expect(getComponent(<child-comp>) instanceof ChildComponent).toBeTruthy();
 * expect(getComponent(<my-app>) instanceof MyApp).toBeTruthy();
 * ```
 *
 * @return {?}
 */
export function getContext(element) {
    /** @type {?} */
    const context = (/** @type {?} */ (loadLContextFromNode(element)));
    return (/** @type {?} */ (context.lView[CONTEXT]));
}
/**
 * Returns the component instance associated with view which owns the DOM element (`null`
 * otherwise).
 *
 * \@publicApi
 * @template T
 * @param {?} element DOM element which is owned by an existing component's view.
 *
 * ```
 * <my-app>
 *   #VIEW
 *     <div>
 *       <child-comp></child-comp>
 *     </div>
 * </mp-app>
 *
 * expect(getViewComponent(<child-comp>) instanceof MyApp).toBeTruthy();
 * expect(getViewComponent(<my-app>)).toEqual(null);
 * ```
 *
 * @return {?}
 */
export function getViewComponent(element) {
    /** @type {?} */
    const context = (/** @type {?} */ (loadLContext(element)));
    /** @type {?} */
    let lView = context.lView;
    /** @type {?} */
    let parent;
    ngDevMode && assertLView(lView);
    while (lView[HOST] === null && (parent = (/** @type {?} */ (getLViewParent(lView))))) {
        // As long as lView[HOST] is null we know we are part of sub-template such as `*ngIf`
        lView = parent;
    }
    return lView[FLAGS] & 512 /* IsRoot */ ? null : (/** @type {?} */ (lView[CONTEXT]));
}
/**
 * Retrieve all root components.
 *
 * Root components are those which have been bootstrapped by Angular.
 *
 * \@publicApi
 * @param {?} target A DOM element, component or directive instance.
 *
 * @return {?}
 */
export function getRootComponents(target) {
    return [...getRootContext(target).components];
}
/**
 * Retrieves an `Injector` associated with the element, component or directive.
 *
 * \@publicApi
 * @param {?} target A DOM element, component or directive instance.
 *
 * @return {?}
 */
export function getInjector(target) {
    /** @type {?} */
    const context = loadLContext(target);
    /** @type {?} */
    const tNode = (/** @type {?} */ (context.lView[TVIEW].data[context.nodeIndex]));
    return new NodeInjector(tNode, context.lView);
}
/**
 * Retrieve a set of injection tokens at a given DOM node.
 *
 * \@publicApi
 * @param {?} element Element for which the injection tokens should be retrieved.
 * @return {?}
 */
export function getInjectionTokens(element) {
    /** @type {?} */
    const context = loadLContext(element, false);
    if (!context)
        return [];
    /** @type {?} */
    const lView = context.lView;
    /** @type {?} */
    const tView = lView[TVIEW];
    /** @type {?} */
    const tNode = (/** @type {?} */ (tView.data[context.nodeIndex]));
    /** @type {?} */
    const providerTokens = [];
    /** @type {?} */
    const startIndex = tNode.providerIndexes & 65535 /* ProvidersStartIndexMask */;
    /** @type {?} */
    const endIndex = tNode.directiveEnd;
    for (let i = startIndex; i < endIndex; i++) {
        /** @type {?} */
        let value = tView.data[i];
        if (isDirectiveDefHack(value)) {
            // The fact that we sometimes store Type and sometimes DirectiveDef in this location is a
            // design flaw.  We should always store same type so that we can be monomorphic. The issue
            // is that for Components/Directives we store the def instead the type. The correct behavior
            // is that we should always be storing injectable type in this location.
            value = value.type;
        }
        providerTokens.push(value);
    }
    return providerTokens;
}
/**
 * Retrieves directives associated with a given DOM host element.
 *
 * \@publicApi
 * @param {?} target A DOM element, component or directive instance.
 *
 * @return {?}
 */
export function getDirectives(target) {
    /** @type {?} */
    const context = (/** @type {?} */ (loadLContext(target)));
    if (context.directives === undefined) {
        context.directives = getDirectivesAtNodeIndex(context.nodeIndex, context.lView, false);
    }
    return context.directives || [];
}
/**
 * @param {?} target
 * @param {?=} throwOnNotFound
 * @return {?}
 */
export function loadLContext(target, throwOnNotFound = true) {
    /** @type {?} */
    const context = getLContext(target);
    if (!context && throwOnNotFound) {
        throw new Error(ngDevMode ? `Unable to find context associated with ${stringifyForError(target)}` :
            'Invalid ng target');
    }
    return context;
}
/**
 * Retrieve map of local references.
 *
 * The references are retrieved as a map of local reference name to element or directive instance.
 *
 * \@publicApi
 * @param {?} target A DOM element, component or directive instance.
 *
 * @return {?}
 */
export function getLocalRefs(target) {
    /** @type {?} */
    const context = (/** @type {?} */ (loadLContext(target)));
    if (context.localRefs === undefined) {
        context.localRefs = discoverLocalRefs(context.lView, context.nodeIndex);
    }
    return context.localRefs || {};
}
/**
 * Retrieve the host element of the component.
 *
 * Use this function to retrieve the host element of the component. The host
 * element is the element which the component is associated with.
 *
 * \@publicApi
 * @template T
 * @param {?} directive Component or Directive for which the host element should be retrieved.
 *
 * @return {?}
 */
export function getHostElement(directive) {
    return (/** @type {?} */ ((/** @type {?} */ ((/** @type {?} */ (getLContext(directive))).native))));
}
/**
 * Retrieves the rendered text for a given component.
 *
 * This function retrieves the host element of a component and
 * and then returns the `textContent` for that element. This implies
 * that the text returned will include re-projected content of
 * the component as well.
 *
 * @param {?} component The component to return the content text for.
 * @return {?}
 */
export function getRenderedText(component) {
    /** @type {?} */
    const hostElement = getHostElement(component);
    return hostElement.textContent || '';
}
/**
 * @param {?} node
 * @return {?}
 */
export function loadLContextFromNode(node) {
    if (!(node instanceof Node))
        throw new Error('Expecting instance of DOM Node');
    return (/** @type {?} */ (loadLContext(node)));
}
/**
 * @record
 */
export function Listener() { }
if (false) {
    /** @type {?} */
    Listener.prototype.name;
    /** @type {?} */
    Listener.prototype.element;
    /** @type {?} */
    Listener.prototype.callback;
    /** @type {?} */
    Listener.prototype.useCapture;
}
/**
 * @param {?} listener
 * @return {?}
 */
export function isBrowserEvents(listener) {
    // Browser events are those which don't have `useCapture` as boolean.
    return typeof listener.useCapture === 'boolean';
}
/**
 * Retrieves a list of DOM listeners.
 *
 * ```
 * <my-app>
 *   #VIEW
 *     <div (click)="doSomething()">
 *     </div>
 * </mp-app>
 *
 * expect(getListeners(<div>)).toEqual({
 *   name: 'click',
 *   element: <div>,
 *   callback: () => doSomething(),
 *   useCapture: false
 * });
 * ```
 *
 * \@publicApi
 * @param {?} element Element for which the DOM listeners should be retrieved.
 * @return {?}
 */
export function getListeners(element) {
    /** @type {?} */
    const lContext = loadLContextFromNode(element);
    /** @type {?} */
    const lView = lContext.lView;
    /** @type {?} */
    const tView = lView[TVIEW];
    /** @type {?} */
    const lCleanup = lView[CLEANUP];
    /** @type {?} */
    const tCleanup = tView.cleanup;
    /** @type {?} */
    const listeners = [];
    if (tCleanup && lCleanup) {
        for (let i = 0; i < tCleanup.length;) {
            /** @type {?} */
            const firstParam = tCleanup[i++];
            /** @type {?} */
            const secondParam = tCleanup[i++];
            if (typeof firstParam === 'string') {
                /** @type {?} */
                const name = firstParam;
                /** @type {?} */
                const listenerElement = (/** @type {?} */ ((/** @type {?} */ (unwrapRNode(lView[secondParam])))));
                /** @type {?} */
                const callback = lCleanup[tCleanup[i++]];
                /** @type {?} */
                const useCaptureOrIndx = tCleanup[i++];
                // if useCaptureOrIndx is boolean then report it as is.
                // if useCaptureOrIndx is positive number then it in unsubscribe method
                // if useCaptureOrIndx is negative number then it is a Subscription
                /** @type {?} */
                const useCapture = typeof useCaptureOrIndx === 'boolean' ?
                    useCaptureOrIndx :
                    (useCaptureOrIndx >= 0 ? false : null);
                if (element == listenerElement) {
                    listeners.push({ element, name, callback, useCapture });
                }
            }
        }
    }
    listeners.sort(sortListeners);
    return listeners;
}
/**
 * @param {?} a
 * @param {?} b
 * @return {?}
 */
function sortListeners(a, b) {
    if (a.name == b.name)
        return 0;
    return a.name < b.name ? -1 : 1;
}
/**
 * This function should not exist because it is megamorphic and only mostly correct.
 *
 * See call site for more info.
 * @param {?} obj
 * @return {?}
 */
function isDirectiveDefHack(obj) {
    return obj.type !== undefined && obj.template !== undefined && obj.declaredInputs !== undefined;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGlzY292ZXJ5X3V0aWxzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvcmVuZGVyMy91dGlsL2Rpc2NvdmVyeV91dGlscy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVNBLE9BQU8sRUFBQyxXQUFXLEVBQUMsTUFBTSxXQUFXLENBQUM7QUFDdEMsT0FBTyxFQUFDLGlCQUFpQixFQUFFLHVCQUF1QixFQUFFLHdCQUF3QixFQUFFLFdBQVcsRUFBQyxNQUFNLHNCQUFzQixDQUFDO0FBQ3ZILE9BQU8sRUFBQyxZQUFZLEVBQUMsTUFBTSxPQUFPLENBQUM7QUFJbkMsT0FBTyxFQUFDLE9BQU8sRUFBRSxPQUFPLEVBQUUsS0FBSyxFQUFFLElBQUksRUFBcUIsS0FBSyxFQUFDLE1BQU0sb0JBQW9CLENBQUM7QUFFM0YsT0FBTyxFQUFDLGlCQUFpQixFQUFDLE1BQU0sY0FBYyxDQUFDO0FBQy9DLE9BQU8sRUFBQyxjQUFjLEVBQUUsY0FBYyxFQUFDLE1BQU0sd0JBQXdCLENBQUM7QUFDdEUsT0FBTyxFQUFDLFdBQVcsRUFBQyxNQUFNLGNBQWMsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUF3QnpDLE1BQU0sVUFBVSxZQUFZLENBQVMsT0FBZ0I7O1VBQzdDLE9BQU8sR0FBRyxvQkFBb0IsQ0FBQyxPQUFPLENBQUM7SUFFN0MsSUFBSSxPQUFPLENBQUMsU0FBUyxLQUFLLFNBQVMsRUFBRTtRQUNuQyxPQUFPLENBQUMsU0FBUyxHQUFHLHVCQUF1QixDQUFDLE9BQU8sQ0FBQyxTQUFTLEVBQUUsT0FBTyxDQUFDLEtBQUssQ0FBQyxDQUFDO0tBQy9FO0lBRUQsT0FBTyxtQkFBQSxPQUFPLENBQUMsU0FBUyxFQUFLLENBQUM7QUFDaEMsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFzQkQsTUFBTSxVQUFVLFVBQVUsQ0FBUyxPQUFnQjs7VUFDM0MsT0FB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QUFPLEVBQVc7O3NCQUNuRSxRQUFRLEdBQXdCLFFBQVEsQ0FBQyxRQUFRLENBQUMsQ0FBQyxFQUFFLENBQUMsQ0FBQzs7c0JBQ3ZELGdCQUFnQixHQUFHLFFBQVEsQ0FBQyxDQUFDLEVBQUUsQ0FBQzs7Ozs7c0JBSWhDLFVBQVUsR0FBRyxPQUFPLGdCQUFnQixLQUFLLFNBQVMsQ0FBQyxDQUFDO29CQUN0RCxnQkFBZ0IsQ0FBQyxDQUFDO29CQUNsQixDQUFDLGdCQUFnQixJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUM7Z0JBQzFDLElBQUksT0FBTyxJQUFJLGVBQWUsRUFBRTtvQkFDOUIsU0FBUyxDQUFDLElBQUksQ0FBQyxFQUFDLE9BQU8sRUFBRSxJQUFJLEVBQUUsUUFBUSxFQUFFLFVBQVUsRUFBQyxDQUFDLENBQUM7aUJBQ3ZEO2FBQ0Y7U0FDRjtLQUNGO0lBQ0QsU0FBUyxDQUFDLElBQUksQ0FBQyxhQUFhLENBQUMsQ0FBQztJQUM5QixPQUFPLFNBQVMsQ0FBQztBQUNuQixDQUFDOzs7Ozs7QUFFRCxTQUFTLGFBQWEsQ0FBQyxDQUFXLEVBQUUsQ0FBVztJQUM3QyxJQUFJLENBQUMsQ0FBQyxJQUFJLElBQUksQ0FBQyxDQUFDLElBQUk7UUFBRSxPQUFPLENBQUMsQ0FBQztJQUMvQixPQUFPLENBQUMsQ0FBQyxJQUFJLEdBQUcsQ0FBQyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztBQUNsQyxDQUFDOzs7Ozs7OztBQU9ELFNBQVMsa0JBQWtCLENBQUMsR0FBUTtJQUNsQyxPQUFPLEdBQUcsQ0FBQyxJQUFJLEtBQUssU0FBUyxJQUFJLEdBQUcsQ0FBQyxRQUFRLEtBQUssU0FBUyxJQUFJLEdBQUcsQ0FBQyxjQUFjLEtBQUssU0FBUyxDQUFDO0FBQ2xHLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SW5qZWN0b3J9IGZyb20gJy4uLy4uL2RpL2luamVjdG9yJztcbmltcG9ydCB7YXNzZXJ0TFZpZXd9IGZyb20gJy4uL2Fzc2VydCc7XG5pbXBvcnQge2Rpc2NvdmVyTG9jYWxSZWZzLCBnZXRDb21wb25lbnRBdE5vZGVJbmRleCwgZ2V0RGlyZWN0aXZlc0F0Tm9kZUluZGV4LCBnZXRMQ29udGV4dH0gZnJvbSAnLi4vY29udGV4dF9kaXNjb3ZlcnknO1xuaW1wb3J0IHtOb2RlSW5qZWN0b3J9IGZyb20gJy4uL2RpJztcbmltcG9ydCB7TENvbnRleHR9IGZyb20gJy4uL2ludGVyZmFjZXMvY29udGV4dCc7XG5pbXBvcnQge0RpcmVjdGl2ZURlZn0gZnJvbSAnLi4vaW50ZXJmYWNlcy9kZWZpbml0aW9uJztcbmltcG9ydCB7VEVsZW1lbnROb2RlLCBUTm9kZSwgVE5vZGVQcm92aWRlckluZGV4ZXN9IGZyb20gJy4uL2ludGVyZmFjZXMvbm9kZSc7XG5pbXBvcnQge0NMRUFOVVAsIENPTlRFWFQsIEZMQUdTLCBIT1NULCBMVmlldywgTFZpZXdGbGFncywgVFZJRVd9IGZyb20gJy4uL2ludGVyZmFjZXMvdmlldyc7XG5cbmltcG9ydCB7c3RyaW5naWZ5Rm9yRXJyb3J9IGZyb20gJy4vbWlzY191dGlscyc7XG5pbXBvcnQge2dldExWaWV3UGFyZW50LCBnZXRSb290Q29udGV4dH0gZnJvbSAnLi92aWV3X3RyYXZlcnNhbF91dGlscyc7XG5pbXBvcnQge3Vud3JhcFJOb2RlfSBmcm9tICcuL3ZpZXdfdXRpbHMnO1xuXG5cblxuLyoqXG4gKiBSZXR1cm5zIHRoZSBjb21wb25lbnQgaW5zdGFuY2UgYXNzb2NpYXRlZCB3aXRoIGEgZ2l2ZW4gRE9NIGhvc3QgZWxlbWVudC5cbiAqIEVsZW1lbnRzIHdoaWNoIGRvbid0IHJlcHJlc2VudCBjb21wb25lbnRzIHJldHVybiBgbnVsbGAuXG4gKlxuICogQHBhcmFtIGVsZW1lbnQgSG9zdCBET00gZWxlbWVudCBmcm9tIHdoaWNoIHRoZSBjb21wb25lbnQgc2hvdWxkIGJlIHJldHJpZXZlZC5cbiAqXG4gKiBgYGBcbiAqIDxteS1hcHA+XG4gKiAgICNWSUVXXG4gKiAgICAgPGRpdj5cbiAqICAgICAgIDxjaGlsZC1jb21wPjwvY2hpbGQtY29tcD5cbiAqICAgICA8L2Rpdj5cbiAqIDwvbXAtYXBwPlxuICpcbiAqIGV4cGVjdChnZXRDb21wb25lbnQoPGNoaWxkLWNvbXA+KSBpbnN0YW5jZW9mIENoaWxkQ29tcG9uZW50KS50b0JlVHJ1dGh5KCk7XG4gKiBleHBlY3QoZ2V0Q29tcG9uZW50KDxteS1hcHA+KSBpbnN0YW5jZW9mIE15QXBwKS50b0JlVHJ1dGh5KCk7XG4gKiBgYGBcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBnZXRDb21wb25lbnQ8VCA9IHt9PihlbGVtZW50OiBFbGVtZW50KTogVHxudWxsIHtcbiAgY29uc3QgY29udGV4dCA9IGxvYWRMQ29udGV4dEZyb21Ob2RlKGVsZW1lbnQpO1xuXG4gIGlmIChjb250ZXh0LmNvbXBvbmVudCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgY29udGV4dC5jb21wb25lbnQgPSBnZXRDb21wb25lbnRBdE5vZGVJbmRleChjb250ZXh0Lm5vZGVJbmRleCwgY29udGV4dC5sVmlldyk7XG4gIH1cblxuICByZXR1cm4gY29udGV4dC5jb21wb25lbnQgYXMgVDtcbn1cblxuLyoqXG4gKiBSZXR1cm5zIHRoZSBjb21wb25lbnQgaW5zdGFuY2UgYXNzb2NpYXRlZCB3aXRoIGEgZ2l2ZW4gRE9NIGhvc3QgZWxlbWVudC5cbiAqIEVsZW1lbnRzIHdoaWNoIGRvbid0IHJlcHJlc2VudCBjb21wb25lbnRzIHJldHVybiBgbnVsbGAuXG4gKlxuICogQHBhcmFtIGVsZW1lbnQgSG9zdCBET00gZWxlbWVudCBmcm9tIHdoaWNoIHRoZSBjb21wb25lbnQgc2hvdWxkIGJlIHJldHJpZXZlZC5cbiAqXG4gKiBgYGBcbiAqIDxteS1hcHA+XG4gKiAgICNWSUVXXG4gKiAgICAgPGRpdj5cbiAqICAgICAgIDxjaGlsZC1jb21wPjwvY2hpbGQtY29tcD5cbiAqICAgICA8L2Rpdj5cbiAqIDwvbXAtYXBwPlxuICpcbiAqIGV4cGVjdChnZXRDb21wb25lbnQoPGNoaWxkLWNvbXA+KSBpbnN0YW5jZW9mIENoaWxkQ29tcG9uZW50KS50b0JlVHJ1dGh5KCk7XG4gKiBleHBlY3QoZ2V0Q29tcG9uZW50KDxteS1hcHA+KSBpbnN0YW5jZW9mIE15QXBwKS50b0JlVHJ1dGh5KCk7XG4gKiBgYGBcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBnZXRDb250ZXh0PFQgPSB7fT4oZWxlbWVudDogRWxlbWVudCk6IFR8bnVsbCB7XG4gIGNvbnN0IGNvbnRleHQgPSBsb2FkTENvbnRleHRGcm9tTm9kZShlbGVtZW50KSAhO1xuICByZXR1cm4gY29udGV4dC5sVmlld1tDT05URVhUXSBhcyBUO1xufVxuXG4vKipcbiAqIFJldHVybnMgdGhlIGNvbXBvbmVudCBpbnN0YW5jZSBhc3NvY2lhdGVkIHdpdGggdmlldyB3aGljaCBvd25zIHRoZSBET00gZWxlbWVudCAoYG51bGxgXG4gKiBvdGhlcndpc2UpLlxuICpcbiAqIEBwYXJhbSBlbGVtZW50IERPTSBlbGVtZW50IHdoaWNoIGlzIG93bmVkIGJ5IGFuIGV4aXN0aW5nIGNvbXBvbmVudCdzIHZpZXcuXG4gKlxuICogYGBgXG4gKiA8bXktYXBwPlxuICogICAjVklFV1xuICogICAgIDxkaXY+XG4gKiAgICAgICA8Y2hpbGQtY29tcD48L2NoaWxkLWNvbXA+XG4gKiAgICAgPC9kaXY+XG4gKiA8L21wLWFwcD5cbiAqXG4gKiBleHBlY3QoZ2V0Vmlld0NvbXBvbmVudCg8Y2hpbGQtY29tcD4pIGluc3RhbmNlb2YgTXlBcHApLnRvQmVUcnV0aHkoKTtcbiAqIGV4cGVjdChnZXRWaWV3Q29tcG9uZW50KDxteS1hcHA+KSkudG9FcXVhbChudWxsKTtcbiAqIGBgYFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGdldFZpZXdDb21wb25lbnQ8VCA9IHt9PihlbGVtZW50OiBFbGVtZW50IHwge30pOiBUfG51bGwge1xuICBjb25zdCBjb250ZXh0ID0gbG9hZExDb250ZXh0KGVsZW1lbnQpICE7XG4gIGxldCBsVmlldyA9IGNvbnRleHQubFZpZXc7XG4gIGxldCBwYXJlbnQ6IExWaWV3fG51bGw7XG4gIG5nRGV2TW9kZSAmJiBhc3NlcnRMVmlldyhsVmlldyk7XG4gIHdoaWxlIChsVmlld1tIT1NUXSA9PT0gbnVsbCAmJiAocGFyZW50ID0gZ2V0TFZpZXdQYXJlbnQobFZpZXcpICEpKSB7XG4gICAgLy8gQXMgbG9uZyBhcyBsVmlld1tIT1NUXSBpcyBudWxsIHdlIGtub3cgd2UgYXJlIHBhcnQgb2Ygc3ViLXRlbXBsYXRlIHN1Y2ggYXMgYCpuZ0lmYFxuICAgIGxWaWV3ID0gcGFyZW50O1xuICB9XG4gIHJldHVybiBsVmlld1tGTEFHU10gJiBMVmlld0ZsYWdzLklzUm9vdCA/IG51bGwgOiBsVmlld1tDT05URVhUXSBhcyBUO1xufVxuXG4vKipcbiAqIFJldHJpZXZlIGFsbCByb290IGNvbXBvbmVudHMuXG4gKlxuICogUm9vdCBjb21wb25lbnRzIGFyZSB0aG9zZSB3aGljaCBoYXZlIGJlZW4gYm9vdHN0cmFwcGVkIGJ5IEFuZ3VsYXIuXG4gKlxuICogQHBhcmFtIHRhcmdldCBBIERPTSBlbGVtZW50LCBjb21wb25lbnQgb3IgZGlyZWN0aXZlIGluc3RhbmNlLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGdldFJvb3RDb21wb25lbnRzKHRhcmdldDoge30pOiBhbnlbXSB7XG4gIHJldHVybiBbLi4uZ2V0Um9vdENvbnRleHQodGFyZ2V0KS5jb21wb25lbnRzXTtcbn1cblxuLyoqXG4gKiBSZXRyaWV2ZXMgYW4gYEluamVjdG9yYCBhc3NvY2lhdGVkIHdpdGggdGhlIGVsZW1lbnQsIGNvbXBvbmVudCBvciBkaXJlY3RpdmUuXG4gKlxuICogQHBhcmFtIHRhcmdldCBBIERPTSBlbGVtZW50LCBjb21wb25lbnQgb3IgZGlyZWN0aXZlIGluc3RhbmNlLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGdldEluamVjdG9yKHRhcmdldDoge30pOiBJbmplY3RvciB7XG4gIGNvbnN0IGNvbnRleHQgPSBsb2FkTENvbnRleHQodGFyZ2V0KTtcbiAgY29uc3QgdE5vZGUgPSBjb250ZXh0LmxWaWV3W1RWSUVXXS5kYXRhW2NvbnRleHQubm9kZUluZGV4XSBhcyBURWxlbWVudE5vZGU7XG4gIHJldHVybiBuZXcgTm9kZUluamVjdG9yKHROb2RlLCBjb250ZXh0LmxWaWV3KTtcbn1cblxuLyoqXG4gKiBSZXRyaWV2ZSBhIHNldCBvZiBpbmplY3Rpb24gdG9rZW5zIGF0IGEgZ2l2ZW4gRE9NIG5vZGUuXG4gKlxuICogQHBhcmFtIGVsZW1lbnQgRWxlbWVudCBmb3Igd2hpY2ggdGhlIGluamVjdGlvbiB0b2tlbnMgc2hvdWxkIGJlIHJldHJpZXZlZC5cbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGdldEluamVjdGlvblRva2VucyhlbGVtZW50OiBFbGVtZW50KTogYW55W10ge1xuICBjb25zdCBjb250ZXh0ID0gbG9hZExDb250ZXh0KGVsZW1lbnQsIGZhbHNlKTtcbiAgaWYgKCFjb250ZXh0KSByZXR1cm4gW107XG4gIGNvbnN0IGxWaWV3ID0gY29udGV4dC5sVmlldztcbiAgY29uc3QgdFZpZXcgPSBsVmlld1tUVklFV107XG4gIGNvbnN0IHROb2RlID0gdFZpZXcuZGF0YVtjb250ZXh0Lm5vZGVJbmRleF0gYXMgVE5vZGU7XG4gIGNvbnN0IHByb3ZpZGVyVG9rZW5zOiBhbnlbXSA9IFtdO1xuICBjb25zdCBzdGFydEluZGV4ID0gdE5vZGUucHJvdmlkZXJJbmRleGVzICYgVE5vZGVQcm92aWRlckluZGV4ZXMuUHJvdmlkZXJzU3RhcnRJbmRleE1hc2s7XG4gIGNvbnN0IGVuZEluZGV4ID0gdE5vZGUuZGlyZWN0aXZlRW5kO1xuICBmb3IgKGxldCBpID0gc3RhcnRJbmRleDsgaSA8IGVuZEluZGV4OyBpKyspIHtcbiAgICBsZXQgdmFsdWUgPSB0Vmlldy5kYXRhW2ldO1xuICAgIGlmIChpc0RpcmVjdGl2ZURlZkhhY2sodmFsdWUpKSB7XG4gICAgICAvLyBUaGUgZmFjdCB0aGF0IHdlIHNvbWV0aW1lcyBzdG9yZSBUeXBlIGFuZCBzb21ldGltZXMgRGlyZWN0aXZlRGVmIGluIHRoaXMgbG9jYXRpb24gaXMgYVxuICAgICAgLy8gZGVzaWduIGZsYXcuICBXZSBzaG91bGQgYWx3YXlzIHN0b3JlIHNhbWUgdHlwZSBzbyB0aGF0IHdlIGNhbiBiZSBtb25vbW9ycGhpYy4gVGhlIGlzc3VlXG4gICAgICAvLyBpcyB0aGF0IGZvciBDb21wb25lbnRzL0RpcmVjdGl2ZXMgd2Ugc3RvcmUgdGhlIGRlZiBpbnN0ZWFkIHRoZSB0eXBlLiBUaGUgY29ycmVjdCBiZWhhdmlvclxuICAgICAgLy8gaXMgdGhhdCB3ZSBzaG91bGQgYWx3YXlzIGJlIHN0b3JpbmcgaW5qZWN0YWJsZSB0eXBlIGluIHRoaXMgbG9jYXRpb24uXG4gICAgICB2YWx1ZSA9IHZhbHVlLnR5cGU7XG4gICAgfVxuICAgIHByb3ZpZGVyVG9rZW5zLnB1c2godmFsdWUpO1xuICB9XG4gIHJldHVybiBwcm92aWRlclRva2Vucztcbn1cblxuLyoqXG4gKiBSZXRyaWV2ZXMgZGlyZWN0aXZlcyBhc3NvY2lhdGVkIHdpdGggYSBnaXZlbiBET00gaG9zdCBlbGVtZW50LlxuICpcbiAqIEBwYXJhbSB0YXJnZXQgQSBET00gZWxlbWVudCwgY29tcG9uZW50IG9yIGRpcmVjdGl2ZSBpbnN0YW5jZS5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBnZXREaXJlY3RpdmVzKHRhcmdldDoge30pOiBBcnJheTx7fT4ge1xuICBjb25zdCBjb250ZXh0ID0gbG9hZExDb250ZXh0KHRhcmdldCkgITtcblxuICBpZiAoY29udGV4dC5kaXJlY3RpdmVzID09PSB1bmRlZmluZWQpIHtcbiAgICBjb250ZXh0LmRpcmVjdGl2ZXMgPSBnZXREaXJlY3RpdmVzQXROb2RlSW5kZXgoY29udGV4dC5ub2RlSW5kZXgsIGNvbnRleHQubFZpZXcsIGZhbHNlKTtcbiAgfVxuXG4gIHJldHVybiBjb250ZXh0LmRpcmVjdGl2ZXMgfHwgW107XG59XG5cbi8qKlxuICogUmV0dXJucyBMQ29udGV4dCBhc3NvY2lhdGVkIHdpdGggYSB0YXJnZXQgcGFzc2VkIGFzIGFuIGFyZ3VtZW50LlxuICogVGhyb3dzIGlmIGEgZ2l2ZW4gdGFyZ2V0IGRvZXNuJ3QgaGF2ZSBhc3NvY2lhdGVkIExDb250ZXh0LlxuICpcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGxvYWRMQ29udGV4dCh0YXJnZXQ6IHt9KTogTENvbnRleHQ7XG5leHBvcnQgZnVuY3Rpb24gbG9hZExDb250ZXh0KHRhcmdldDoge30sIHRocm93T25Ob3RGb3VuZDogZmFsc2UpOiBMQ29udGV4dHxudWxsO1xuZXhwb3J0IGZ1bmN0aW9uIGxvYWRMQ29udGV4dCh0YXJnZXQ6IHt9LCB0aHJvd09uTm90Rm91bmQ6IGJvb2xlYW4gPSB0cnVlKTogTENvbnRleHR8bnVsbCB7XG4gIGNvbnN0IGNvbnRleHQgPSBnZXRMQ29udGV4dCh0YXJnZXQpO1xuICBpZiAoIWNvbnRleHQgJiYgdGhyb3dPbk5vdEZvdW5kKSB7XG4gICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICBuZ0Rldk1vZGUgPyBgVW5hYmxlIHRvIGZpbmQgY29udGV4dCBhc3NvY2lhdGVkIHdpdGggJHtzdHJpbmdpZnlGb3JFcnJvcih0YXJnZXQpfWAgOlxuICAgICAgICAgICAgICAgICAgICAnSW52YWxpZCBuZyB0YXJnZXQnKTtcbiAgfVxuICByZXR1cm4gY29udGV4dDtcbn1cblxuLyoqXG4gKiBSZXRyaWV2ZSBtYXAgb2YgbG9jYWwgcmVmZXJlbmNlcy5cbiAqXG4gKiBUaGUgcmVmZXJlbmNlcyBhcmUgcmV0cmlldmVkIGFzIGEgbWFwIG9mIGxvY2FsIHJlZmVyZW5jZSBuYW1lIHRvIGVsZW1lbnQgb3IgZGlyZWN0aXZlIGluc3RhbmNlLlxuICpcbiAqIEBwYXJhbSB0YXJnZXQgQSBET00gZWxlbWVudCwgY29tcG9uZW50IG9yIGRpcmVjdGl2ZSBpbnN0YW5jZS5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBnZXRMb2NhbFJlZnModGFyZ2V0OiB7fSk6IHtba2V5OiBzdHJpbmddOiBhbnl9IHtcbiAgY29uc3QgY29udGV4dCA9IGxvYWRMQ29udGV4dCh0YXJnZXQpICE7XG5cbiAgaWYgKGNvbnRleHQubG9jYWxSZWZzID09PSB1bmRlZmluZWQpIHtcbiAgICBjb250ZXh0LmxvY2FsUmVmcyA9IGRpc2NvdmVyTG9jYWxSZWZzKGNvbnRleHQubFZpZXcsIGNvbnRleHQubm9kZUluZGV4KTtcbiAgfVxuXG4gIHJldHVybiBjb250ZXh0LmxvY2FsUmVmcyB8fCB7fTtcbn1cblxuLyoqXG4gKiBSZXRyaWV2ZSB0aGUgaG9zdCBlbGVtZW50IG9mIHRoZSBjb21wb25lbnQuXG4gKlxuICogVXNlIHRoaXMgZnVuY3Rpb24gdG8gcmV0cmlldmUgdGhlIGhvc3QgZWxlbWVudCBvZiB0aGUgY29tcG9uZW50LiBUaGUgaG9zdFxuICogZWxlbWVudCBpcyB0aGUgZWxlbWVudCB3aGljaCB0aGUgY29tcG9uZW50IGlzIGFzc29jaWF0ZWQgd2l0aC5cbiAqXG4gKiBAcGFyYW0gZGlyZWN0aXZlIENvbXBvbmVudCBvciBEaXJlY3RpdmUgZm9yIHdoaWNoIHRoZSBob3N0IGVsZW1lbnQgc2hvdWxkIGJlIHJldHJpZXZlZC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBnZXRIb3N0RWxlbWVudDxUPihkaXJlY3RpdmU6IFQpOiBFbGVtZW50IHtcbiAgcmV0dXJuIGdldExDb250ZXh0KGRpcmVjdGl2ZSkgIS5uYXRpdmUgYXMgbmV2ZXIgYXMgRWxlbWVudDtcbn1cblxuLyoqXG4gKiBSZXRyaWV2ZXMgdGhlIHJlbmRlcmVkIHRleHQgZm9yIGEgZ2l2ZW4gY29tcG9uZW50LlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gcmV0cmlldmVzIHRoZSBob3N0IGVsZW1lbnQgb2YgYSBjb21wb25lbnQgYW5kXG4gKiBhbmQgdGhlbiByZXR1cm5zIHRoZSBgdGV4dENvbnRlbnRgIGZvciB0aGF0IGVsZW1lbnQuIFRoaXMgaW1wbGllc1xuICogdGhhdCB0aGUgdGV4dCByZXR1cm5lZCB3aWxsIGluY2x1ZGUgcmUtcHJvamVjdGVkIGNvbnRlbnQgb2ZcbiAqIHRoZSBjb21wb25lbnQgYXMgd2VsbC5cbiAqXG4gKiBAcGFyYW0gY29tcG9uZW50IFRoZSBjb21wb25lbnQgdG8gcmV0dXJuIHRoZSBjb250ZW50IHRleHQgZm9yLlxuICovXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVuZGVyZWRUZXh0KGNvbXBvbmVudDogYW55KTogc3RyaW5nIHtcbiAgY29uc3QgaG9zdEVsZW1lbnQgPSBnZXRIb3N0RWxlbWVudChjb21wb25lbnQpO1xuICByZXR1cm4gaG9zdEVsZW1lbnQudGV4dENvbnRlbnQgfHwgJyc7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBsb2FkTENvbnRleHRGcm9tTm9kZShub2RlOiBOb2RlKTogTENvbnRleHQge1xuICBpZiAoIShub2RlIGluc3RhbmNlb2YgTm9kZSkpIHRocm93IG5ldyBFcnJvcignRXhwZWN0aW5nIGluc3RhbmNlIG9mIERPTSBOb2RlJyk7XG4gIHJldHVybiBsb2FkTENvbnRleHQobm9kZSkgITtcbn1cblxuZXhwb3J0IGludGVyZmFjZSBMaXN0ZW5lciB7XG4gIG5hbWU6IHN0cmluZztcbiAgZWxlbWVudDogRWxlbWVudDtcbiAgY2FsbGJhY2s6ICh2YWx1ZTogYW55KSA9PiBhbnk7XG4gIHVzZUNhcHR1cmU6IGJvb2xlYW58bnVsbDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzQnJvd3NlckV2ZW50cyhsaXN0ZW5lcjogTGlzdGVuZXIpOiBib29sZWFuIHtcbiAgLy8gQnJvd3NlciBldmVudHMgYXJlIHRob3NlIHdoaWNoIGRvbid0IGhhdmUgYHVzZUNhcHR1cmVgIGFzIGJvb2xlYW4uXG4gIHJldHVybiB0eXBlb2YgbGlzdGVuZXIudXNlQ2FwdHVyZSA9PT0gJ2Jvb2xlYW4nO1xufVxuXG5cbi8qKlxuICogUmV0cmlldmVzIGEgbGlzdCBvZiBET00gbGlzdGVuZXJzLlxuICpcbiAqIGBgYFxuICogPG15LWFwcD5cbiAqICAgI1ZJRVdcbiAqICAgICA8ZGl2IChjbGljayk9XCJkb1NvbWV0aGluZygpXCI+XG4gKiAgICAgPC9kaXY+XG4gKiA8L21wLWFwcD5cbiAqXG4gKiBleHBlY3QoZ2V0TGlzdGVuZXJzKDxkaXY+KSkudG9FcXVhbCh7XG4gKiAgIG5hbWU6ICdjbGljaycsXG4gKiAgIGVsZW1lbnQ6IDxkaXY+LFxuICogICBjYWxsYmFjazogKCkgPT4gZG9Tb21ldGhpbmcoKSxcbiAqICAgdXNlQ2FwdHVyZTogZmFsc2VcbiAqIH0pO1xuICogYGBgXG4gKlxuICogQHBhcmFtIGVsZW1lbnQgRWxlbWVudCBmb3Igd2hpY2ggdGhlIERPTSBsaXN0ZW5lcnMgc2hvdWxkIGJlIHJldHJpZXZlZC5cbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGdldExpc3RlbmVycyhlbGVtZW50OiBFbGVtZW50KTogTGlzdGVuZXJbXSB7XG4gIGNvbnN0IGxDb250ZXh0ID0gbG9hZExDb250ZXh0RnJvbU5vZGUoZWxlbWVudCk7XG4gIGNvbnN0IGxWaWV3ID0gbENvbnRleHQubFZpZXc7XG4gIGNvbnN0IHRWaWV3ID0gbFZpZXdbVFZJRVddO1xuICBjb25zdCBsQ2xlYW51cCA9IGxWaWV3W0NMRUFOVVBdO1xuICBjb25zdCB0Q2xlYW51cCA9IHRWaWV3LmNsZWFudXA7XG4gIGNvbnN0IGxpc3RlbmVyczogTGlzdGVuZXJbXSA9IFtdO1xuICBpZiAodENsZWFudXAgJiYgbENsZWFudXApIHtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHRDbGVhbnVwLmxlbmd0aDspIHtcbiAgICAgIGNvbnN0IGZpcnN0UGFyYW0gPSB0Q2xlYW51cFtpKytdO1xuICAgICAgY29uc3Qgc2Vjb25kUGFyYW0gPSB0Q2xlYW51cFtpKytdO1xuICAgICAgaWYgKHR5cGVvZiBmaXJzdFBhcmFtID09PSAnc3RyaW5nJykge1xuICAgICAgICBjb25zdCBuYW1lOiBzdHJpbmcgPSBmaXJzdFBhcmFtO1xuICAgICAgICBjb25zdCBsaXN0ZW5lckVsZW1lbnQgPSB1bndyYXBSTm9kZShsVmlld1tzZWNvbmRQYXJhbV0pIGFzIGFueSBhcyBFbGVtZW50O1xuICAgICAgICBjb25zdCBjYWxsYmFjazogKHZhbHVlOiBhbnkpID0+IGFueSA9IGxDbGVhbnVwW3RDbGVhbnVwW2krK11dO1xuICAgICAgICBjb25zdCB1c2VDYXB0dXJlT3JJbmR4ID0gdENsZWFudXBbaSsrXTtcbiAgICAgICAgLy8gaWYgdXNlQ2FwdHVyZU9ySW5keCBpcyBib29sZWFuIHRoZW4gcmVwb3J0IGl0IGFzIGlzLlxuICAgICAgICAvLyBpZiB1c2VDYXB0dXJlT3JJbmR4IGlzIHBvc2l0aXZlIG51bWJlciB0aGVuIGl0IGluIHVuc3Vic2NyaWJlIG1ldGhvZFxuICAgICAgICAvLyBpZiB1c2VDYXB0dXJlT3JJbmR4IGlzIG5lZ2F0aXZlIG51bWJlciB0aGVuIGl0IGlzIGEgU3Vic2NyaXB0aW9uXG4gICAgICAgIGNvbnN0IHVzZUNhcHR1cmUgPSB0eXBlb2YgdXNlQ2FwdHVyZU9ySW5keCA9PT0gJ2Jvb2xlYW4nID9cbiAgICAgICAgICAgIHVzZUNhcHR1cmVPckluZHggOlxuICAgICAgICAgICAgKHVzZUNhcHR1cmVPckluZHggPj0gMCA/IGZhbHNlIDogbnVsbCk7XG4gICAgICAgIGlmIChlbGVtZW50ID09IGxpc3RlbmVyRWxlbWVudCkge1xuICAgICAgICAgIGxpc3RlbmVycy5wdXNoKHtlbGVtZW50LCBuYW1lLCBjYWxsYmFjaywgdXNlQ2FwdHVyZX0pO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuICB9XG4gIGxpc3RlbmVycy5zb3J0KHNvcnRMaXN0ZW5lcnMpO1xuICByZXR1cm4gbGlzdGVuZXJzO1xufVxuXG5mdW5jdGlvbiBzb3J0TGlzdGVuZXJzKGE6IExpc3RlbmVyLCBiOiBMaXN0ZW5lcikge1xuICBpZiAoYS5uYW1lID09IGIubmFtZSkgcmV0dXJuIDA7XG4gIHJldHVybiBhLm5hbWUgPCBiLm5hbWUgPyAtMSA6IDE7XG59XG5cbi8qKlxuICogVGhpcyBmdW5jdGlvbiBzaG91bGQgbm90IGV4aXN0IGJlY2F1c2UgaXQgaXMgbWVnYW1vcnBoaWMgYW5kIG9ubHkgbW9zdGx5IGNvcnJlY3QuXG4gKlxuICogU2VlIGNhbGwgc2l0ZSBmb3IgbW9yZSBpbmZvLlxuICovXG5mdW5jdGlvbiBpc0RpcmVjdGl2ZURlZkhhY2sob2JqOiBhbnkpOiBvYmogaXMgRGlyZWN0aXZlRGVmPGFueT4ge1xuICByZXR1cm4gb2JqLnR5cGUgIT09IHVuZGVmaW5lZCAmJiBvYmoudGVtcGxhdGUgIT09IHVuZGVmaW5lZCAmJiBvYmouZGVjbGFyZWRJbnB1dHMgIT09IHVuZGVmaW5lZDtcbn1cbiJdfQ==
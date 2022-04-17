/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { assertDataInRange } from '../../util/assert';
import { isObservable } from '../../util/lang';
import { EMPTY_OBJ } from '../empty';
import { isProceduralRenderer } from '../interfaces/renderer';
import { CLEANUP, FLAGS, RENDERER, TVIEW } from '../interfaces/view';
import { assertNodeOfPossibleTypes } from '../node_assert';
import { getLView, getPreviousOrParentTNode } from '../state';
import { getComponentViewByIndex, getNativeByTNode, hasDirectives, unwrapRNode } from '../util/view_utils';
import { generatePropertyAliases, getCleanup, handleError, loadComponentRenderer, markViewDirty } from './shared';
/**
 * Adds an event listener to the current node.
 *
 * If an output exists on one of the node's directives, it also subscribes to the output
 * and saves the subscription for later cleanup.
 *
 * @param eventName Name of the event
 * @param listenerFn The function to be called when event emits
 * @param useCapture Whether or not to use capture in event listener
 * @param eventTargetResolver Function that returns global target information in case this listener
 * should be attached to a global object like window, document or body
 *
 * @codeGenApi
 */
export function ɵɵlistener(eventName, listenerFn, useCapture, eventTargetResolver) {
    if (useCapture === void 0) { useCapture = false; }
    listenerInternal(eventName, listenerFn, useCapture, eventTargetResolver);
}
/**
* Registers a synthetic host listener (e.g. `(@foo.start)`) on a component.
*
* This instruction is for compatibility purposes and is designed to ensure that a
* synthetic host listener (e.g. `@HostListener('@foo.start')`) properly gets rendered
* in the component's renderer. Normally all host listeners are evaluated with the
* parent component's renderer, but, in the case of animation @triggers, they need
* to be evaluated with the sub component's renderer (because that's where the
* animation triggers are defined).
*
* Do not use this instruction as a replacement for `listener`. This instruction
* only exists to ensure compatibility with the ViewEngine's host binding behavior.
*
* @param eventName Name of the event
* @param listenerFn The function to be called when event emits
* @param useCapture Whether or not to use capture in event listener
* @param eventTargetResolver Function that returns global target information in case this listener
* should be attached to a global object like window, document or body
 *
 * @codeGenApi
*/
export function ɵɵcomponentHostSyntheticListener(eventName, listenerFn, useCapture, eventTargetResolver) {
    if (useCapture === void 0) { useCapture = false; }
    listenerInternal(eventName, listenerFn, useCapture, eventTargetResolver, loadComponentRenderer);
}
/**
 * A utility function that checks if a given element has already an event handler registered for an
 * event with a specified name. The TView.cleanup data structure is used to find out which events
 * are registered for a given element.
 */
function findExistingListener(lView, eventName, tNodeIdx) {
    var tView = lView[TVIEW];
    var tCleanup = tView.cleanup;
    if (tCleanup != null) {
        for (var i = 0; i < tCleanup.length - 1; i += 2) {
            var cleanupEventName = tCleanup[i];
            if (cleanupEventName === eventName && tCleanup[i + 1] === tNodeIdx) {
                // We have found a matching event name on the same node but it might not have been
                // registered yet, so we must explicitly verify entries in the LView cleanup data
                // structures.
                var lCleanup = lView[CLEANUP];
                var listenerIdxInLCleanup = tCleanup[i + 2];
                return lCleanup.length > listenerIdxInLCleanup ? lCleanup[listenerIdxInLCleanup] : null;
            }
            // TView.cleanup can have a mix of 4-elements entries (for event handler cleanups) or
            // 2-element entries (for directive and queries destroy hooks). As such we can encounter
            // blocks of 4 or 2 items in the tView.cleanup and this is why we iterate over 2 elements
            // first and jump another 2 elements if we detect listeners cleanup (4 elements). Also check
            // documentation of TView.cleanup for more details of this data structure layout.
            if (typeof cleanupEventName === 'string') {
                i += 2;
            }
        }
    }
    return null;
}
function listenerInternal(eventName, listenerFn, useCapture, eventTargetResolver, loadRendererFn) {
    if (useCapture === void 0) { useCapture = false; }
    var lView = getLView();
    var tNode = getPreviousOrParentTNode();
    var tView = lView[TVIEW];
    var firstTemplatePass = tView.firstTemplatePass;
    var tCleanup = firstTemplatePass && (tView.cleanup || (tView.cleanup = []));
    ngDevMode && assertNodeOfPossibleTypes(tNode, 3 /* Element */, 0 /* Container */, 4 /* ElementContainer */);
    var processOutputs = true;
    // add native event listener - applicable to elements only
    if (tNode.type === 3 /* Element */) {
        var native = getNativeByTNode(tNode, lView);
        var resolved = eventTargetResolver ? eventTargetResolver(native) : EMPTY_OBJ;
        var target = resolved.target || native;
        var renderer = loadRendererFn ? loadRendererFn(tNode, lView) : lView[RENDERER];
        var lCleanup = getCleanup(lView);
        var lCleanupIndex = lCleanup.length;
        var idxOrTargetGetter = eventTargetResolver ?
            function (_lView) { return eventTargetResolver(unwrapRNode(_lView[tNode.index])).target; } :
            tNode.index;
        // In order to match current behavior, native DOM event listeners must be added for all
        // events (including outputs).
        if (isProceduralRenderer(renderer)) {
            // There might be cases where multiple directives on the same element try to register an event
            // handler function for the same event. In this situation we want to avoid registration of
            // several native listeners as each registration would be intercepted by NgZone and
            // trigger change detection. This would mean that a single user action would result in several
            // change detections being invoked. To avoid this situation we want to have only one call to
            // native handler registration (for the same element and same type of event).
            //
            // In order to have just one native event handler in presence of multiple handler functions,
            // we just register a first handler function as a native event listener and then chain
            // (coalesce) other handler functions on top of the first native handler function.
            var existingListener = null;
            // Please note that the coalescing described here doesn't happen for events specifying an
            // alternative target (ex. (document:click)) - this is to keep backward compatibility with the
            // view engine.
            // Also, we don't have to search for existing listeners is there are no directives
            // matching on a given node as we can't register multiple event handlers for the same event in
            // a template (this would mean having duplicate attributes).
            if (!eventTargetResolver && hasDirectives(tNode)) {
                existingListener = findExistingListener(lView, eventName, tNode.index);
            }
            if (existingListener !== null) {
                // Attach a new listener at the head of the coalesced listeners list.
                listenerFn.__ngNextListenerFn__ = existingListener.__ngNextListenerFn__;
                existingListener.__ngNextListenerFn__ = listenerFn;
                processOutputs = false;
            }
            else {
                // The first argument of `listen` function in Procedural Renderer is:
                // - either a target name (as a string) in case of global target (window, document, body)
                // - or element reference (in all other cases)
                listenerFn = wrapListener(tNode, lView, listenerFn, false /** preventDefault */);
                var cleanupFn = renderer.listen(resolved.name || target, eventName, listenerFn);
                ngDevMode && ngDevMode.rendererAddEventListener++;
                lCleanup.push(listenerFn, cleanupFn);
                tCleanup && tCleanup.push(eventName, idxOrTargetGetter, lCleanupIndex, lCleanupIndex + 1);
            }
        }
        else {
            listenerFn = wrapListener(tNode, lView, listenerFn, true /** preventDefault */);
            target.addEventListener(eventName, listenerFn, useCapture);
            ngDevMode && ngDevMode.rendererAddEventListener++;
            lCleanup.push(listenerFn);
            tCleanup && tCleanup.push(eventName, idxOrTargetGetter, lCleanupIndex, useCapture);
        }
    }
    // subscribe to directive outputs
    if (tNode.outputs === undefined) {
        // if we create TNode here, inputs must be undefined so we know they still need to be
        // checked
        tNode.outputs = generatePropertyAliases(tNode, 1 /* Output */);
    }
    var outputs = tNode.outputs;
    var props;
    if (processOutputs && outputs && (props = outputs[eventName])) {
        var propsLength = props.length;
        if (propsLength) {
            var lCleanup = getCleanup(lView);
            for (var i = 0; i < propsLength; i += 3) {
                var index = props[i];
                ngDevMode && assertDataInRange(lView, index);
                var minifiedName = props[i + 2];
                var directiveInstance = lView[index];
                var output = directiveInstance[minifiedName];
                if (ngDevMode && !isObservable(output)) {
                    throw new Error("@Output " + minifiedName + " not initialized in '" + directiveInstance.constructor.name + "'.");
                }
                var subscription = output.subscribe(listenerFn);
                var idx = lCleanup.length;
                lCleanup.push(listenerFn, subscription);
                tCleanup && tCleanup.push(eventName, tNode.index, idx, -(idx + 1));
            }
        }
    }
}
function executeListenerWithErrorHandling(lView, listenerFn, e) {
    try {
        // Only explicitly returning false from a listener should preventDefault
        return listenerFn(e) !== false;
    }
    catch (error) {
        handleError(lView, error);
        return false;
    }
}
/**
 * Wraps an event listener with a function that marks ancestors dirty and prevents default behavior,
 * if applicable.
 *
 * @param tNode The TNode associated with this listener
 * @param lView The LView that contains this listener
 * @param listenerFn The listener function to call
 * @param wrapWithPreventDefault Whether or not to prevent default behavior
 * (the procedural renderer does this already, so in those cases, we should skip)
 */
function wrapListener(tNode, lView, listenerFn, wrapWithPreventDefault) {
    // Note: we are performing most of the work in the listener function itself
    // to optimize listener registration.
    return function wrapListenerIn_markDirtyAndPreventDefault(e) {
        // In order to be backwards compatible with View Engine, events on component host nodes
        // must also mark the component view itself dirty (i.e. the view that it owns).
        var startView = tNode.flags & 1 /* isComponent */ ? getComponentViewByIndex(tNode.index, lView) : lView;
        // See interfaces/view.ts for more on LViewFlags.ManualOnPush
        if ((lView[FLAGS] & 32 /* ManualOnPush */) === 0) {
            markViewDirty(startView);
        }
        var result = executeListenerWithErrorHandling(lView, listenerFn, e);
        // A just-invoked listener function might have coalesced listeners so we need to check for
        // their presence and invoke as needed.
        var nextListenerFn = wrapListenerIn_markDirtyAndPreventDefault.__ngNextListenerFn__;
        while (nextListenerFn) {
            // We should prevent default if any of the listeners explicitly return false
            result = executeListenerWithErrorHandling(lView, nextListenerFn, e) && result;
            nextListenerFn = nextListenerFn.__ngNextListenerFn__;
        }
        if (wrapWithPreventDefault && result === false) {
            e.preventDefault();
            // Necessary for legacy browsers that don't support preventDefault (e.g. IE)
            e.returnValue = false;
        }
        return result;
    };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibGlzdGVuZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL2luc3RydWN0aW9ucy9saXN0ZW5lci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFHSCxPQUFPLEVBQUMsaUJBQWlCLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUNwRCxPQUFPLEVBQUMsWUFBWSxFQUFDLE1BQU0saUJBQWlCLENBQUM7QUFDN0MsT0FBTyxFQUFDLFNBQVMsRUFBQyxNQUFNLFVBQVUsQ0FBQztBQUVuQyxPQUFPLEVBQTRDLG9CQUFvQixFQUFDLE1BQU0sd0JBQXdCLENBQUM7QUFDdkcsT0FBTyxFQUFDLE9BQU8sRUFBRSxLQUFLLEVBQXFCLFFBQVEsRUFBRSxLQUFLLEVBQUMsTUFBTSxvQkFBb0IsQ0FBQztBQUN0RixPQUFPLEVBQUMseUJBQXlCLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUN6RCxPQUFPLEVBQUMsUUFBUSxFQUFFLHdCQUF3QixFQUFDLE1BQU0sVUFBVSxDQUFDO0FBQzVELE9BQU8sRUFBQyx1QkFBdUIsRUFBRSxnQkFBZ0IsRUFBRSxhQUFhLEVBQUUsV0FBVyxFQUFDLE1BQU0sb0JBQW9CLENBQUM7QUFDekcsT0FBTyxFQUFtQix1QkFBdUIsRUFBRSxVQUFVLEVBQUUsV0FBVyxFQUFFLHFCQUFxQixFQUFFLGFBQWEsRUFBQyxNQUFNLFVBQVUsQ0FBQztBQUVsSTs7Ozs7Ozs7Ozs7OztHQWFHO0FBQ0gsTUFBTSxVQUFVLFVBQVUsQ0FDdEIsU0FBaUIsRUFBRSxVQUE0QixFQUFFLFVBQWtCLEVBQ25FLG1CQUEwQztJQURPLDJCQUFBLEVBQUEsa0JBQWtCO0lBRXJFLGdCQUFnQixDQUFDLFNBQVMsRUFBRSxVQUFVLEVBQUUsVUFBVSxFQUFFLG1CQUFtQixDQUFDLENBQUM7QUFDM0UsQ0FBQztBQUVEOzs7Ozs7Ozs7Ozs7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WUFDckIsNEVBQTRFO1lBQzVFLE1BQU0sR0FBRyxnQ0FBZ0MsQ0FBQyxLQUFLLEVBQUUsY0FBYyxFQUFFLENBQUMsQ0FBQyxJQUFJLE1BQU0sQ0FBQztZQUM5RSxjQUFjLEdBQVMsY0FBZSxDQUFDLG9CQUFvQixDQUFDO1NBQzdEO1FBRUQsSUFBSSxzQkFBc0IsSUFBSSxNQUFNLEtBQUssS0FBSyxFQUFFO1lBQzlDLENBQUMsQ0FBQyxjQUFjLEVBQUUsQ0FBQztZQUNuQiw0RUFBNEU7WUFDNUUsQ0FBQyxDQUFDLFdBQVcsR0FBRyxLQUFLLENBQUM7U0FDdkI7UUFFRCxPQUFPLE1BQU0sQ0FBQztJQUNoQixDQUFDLENBQUM7QUFDSixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5cbmltcG9ydCB7YXNzZXJ0RGF0YUluUmFuZ2V9IGZyb20gJy4uLy4uL3V0aWwvYXNzZXJ0JztcbmltcG9ydCB7aXNPYnNlcnZhYmxlfSBmcm9tICcuLi8uLi91dGlsL2xhbmcnO1xuaW1wb3J0IHtFTVBUWV9PQkp9IGZyb20gJy4uL2VtcHR5JztcbmltcG9ydCB7UHJvcGVydHlBbGlhc1ZhbHVlLCBUTm9kZSwgVE5vZGVGbGFncywgVE5vZGVUeXBlfSBmcm9tICcuLi9pbnRlcmZhY2VzL25vZGUnO1xuaW1wb3J0IHtHbG9iYWxUYXJnZXRSZXNvbHZlciwgUkVsZW1lbnQsIFJlbmRlcmVyMywgaXNQcm9jZWR1cmFsUmVuZGVyZXJ9IGZyb20gJy4uL2ludGVyZmFjZXMvcmVuZGVyZXInO1xuaW1wb3J0IHtDTEVBTlVQLCBGTEFHUywgTFZpZXcsIExWaWV3RmxhZ3MsIFJFTkRFUkVSLCBUVklFV30gZnJvbSAnLi4vaW50ZXJmYWNlcy92aWV3JztcbmltcG9ydCB7YXNzZXJ0Tm9kZU9mUG9zc2libGVUeXBlc30gZnJvbSAnLi4vbm9kZV9hc3NlcnQnO1xuaW1wb3J0IHtnZXRMVmlldywgZ2V0UHJldmlvdXNPclBhcmVudFROb2RlfSBmcm9tICcuLi9zdGF0ZSc7XG5pbXBvcnQge2dldENvbXBvbmVudFZpZXdCeUluZGV4LCBnZXROYXRpdmVCeVROb2RlLCBoYXNEaXJlY3RpdmVzLCB1bndyYXBSTm9kZX0gZnJvbSAnLi4vdXRpbC92aWV3X3V0aWxzJztcbmltcG9ydCB7QmluZGluZ0RpcmVjdGlvbiwgZ2VuZXJhdGVQcm9wZXJ0eUFsaWFzZXMsIGdldENsZWFudXAsIGhhbmRsZUVycm9yLCBsb2FkQ29tcG9uZW50UmVuZGVyZXIsIG1hcmtWaWV3RGlydHl9IGZyb20gJy4vc2hhcmVkJztcblxuLyoqXG4gKiBBZGRzIGFuIGV2ZW50IGxpc3RlbmVyIHRvIHRoZSBjdXJyZW50IG5vZGUuXG4gKlxuICogSWYgYW4gb3V0cHV0IGV4aXN0cyBvbiBvbmUgb2YgdGhlIG5vZGUncyBkaXJlY3RpdmVzLCBpdCBhbHNvIHN1YnNjcmliZXMgdG8gdGhlIG91dHB1dFxuICogYW5kIHNhdmVzIHRoZSBzdWJzY3JpcHRpb24gZm9yIGxhdGVyIGNsZWFudXAuXG4gKlxuICogQHBhcmFtIGV2ZW50TmFtZSBOYW1lIG9mIHRoZSBldmVudFxuICogQHBhcmFtIGxpc3RlbmVyRm4gVGhlIGZ1bmN0aW9uIHRvIGJlIGNhbGxlZCB3aGVuIGV2ZW50IGVtaXRzXG4gKiBAcGFyYW0gdXNlQ2FwdHVyZSBXaGV0aGVyIG9yIG5vdCB0byB1c2UgY2FwdHVyZSBpbiBldmVudCBsaXN0ZW5lclxuICogQHBhcmFtIGV2ZW50VGFyZ2V0UmVzb2x2ZXIgRnVuY3Rpb24gdGhhdCByZXR1cm5zIGdsb2JhbCB0YXJnZXQgaW5mb3JtYXRpb24gaW4gY2FzZSB0aGlzIGxpc3RlbmVyXG4gKiBzaG91bGQgYmUgYXR0YWNoZWQgdG8gYSBnbG9iYWwgb2JqZWN0IGxpa2Ugd2luZG93LCBkb2N1bWVudCBvciBib2R5XG4gKlxuICogQGNvZGVHZW5BcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIMm1ybVsaXN0ZW5lcihcbiAgICBldmVudE5hbWU6IHN0cmluZywgbGlzdGVuZXJGbjogKGU/OiBhbnkpID0+IGFueSwgdXNlQ2FwdHVyZSA9IGZhbHNlLFxuICAgIGV2ZW50VGFyZ2V0UmVzb2x2ZXI/OiBHbG9iYWxUYXJnZXRSZXNvbHZlcik6IHZvaWQge1xuICBsaXN0ZW5lckludGVybmFsKGV2ZW50TmFtZSwgbGlzdGVuZXJGbiwgdXNlQ2FwdHVyZSwgZXZlbnRUYXJnZXRSZXNvbHZlcik7XG59XG5cbi8qKlxuKiBSZWdpc3RlcnMgYSBzeW50aGV0aWMgaG9zdCBsaXN0ZW5lciAoZS5nLiBgKEBmb28uc3RhcnQpYCkgb24gYSBjb21wb25lbnQuXG4qXG4qIFRoaXMgaW5zdHJ1Y3Rpb24gaXMgZm9yIGNvbXBhdGliaWxpdHkgcHVycG9zZXMgYW5kIGlzIGRlc2lnbmVkIHRvIGVuc3VyZSB0aGF0IGFcbiogc3ludGhldGljIGhvc3QgbGlzdGVuZXIgKGUuZy4gYEBIb3N0TGlzdGVuZXIoJ0Bmb28uc3RhcnQnKWApIHByb3Blcmx5IGdldHMgcmVuZGVyZWRcbiogaW4gdGhlIGNvbXBvbmVudCdzIHJlbmRlcmVyLiBOb3JtYWxseSBhbGwgaG9zdCBsaXN0ZW5lcnMgYXJlIGV2YWx1YXRlZCB3aXRoIHRoZVxuKiBwYXJlbnQgY29tcG9uZW50J3MgcmVuZGVyZXIsIGJ1dCwgaW4gdGhlIGNhc2Ugb2YgYW5pbWF0aW9uIEB0cmlnZ2VycywgdGhleSBuZWVkXG4qIHRvIGJlIGV2YWx1YXRlZCB3aXRoIHRoZSBzdWIgY29tcG9uZW50J3MgcmVuZGVyZXIgKGJlY2F1c2UgdGhhdCdzIHdoZXJlIHRoZVxuKiBhbmltYXRpb24gdHJpZ2dlcnMgYXJlIGRlZmluZWQpLlxuKlxuKiBEbyBub3QgdXNlIHRoaXMgaW5zdHJ1Y3Rpb24gYXMgYSByZXBsYWNlbWVudCBmb3IgYGxpc3RlbmVyYC4gVGhpcyBpbnN0cnVjdGlvblxuKiBvbmx5IGV4aXN0cyB0byBlbnN1cmUgY29tcGF0aWJpbGl0eSB3aXRoIHRoZSBWaWV3RW5naW5lJ3MgaG9zdCBiaW5kaW5nIGJlaGF2aW9yLlxuKlxuKiBAcGFyYW0gZXZlbnROYW1lIE5hbWUgb2YgdGhlIGV2ZW50XG4qIEBwYXJhbSBsaXN0ZW5lckZuIFRoZSBmdW5jdGlvbiB0byBiZSBjYWxsZWQgd2hlbiBldmVudCBlbWl0c1xuKiBAcGFyYW0gdXNlQ2FwdHVyZSBXaGV0aGVyIG9yIG5vdCB0byB1c2UgY2FwdHVyZSBpbiBldmVudCBsaXN0ZW5lclxuKiBAcGFyYW0gZXZlbnRUYXJnZXRSZXNvbHZlciBGdW5jdGlvbiB0aGF0IHJldHVybnMgZ2xvYmFsIHRhcmdldCBpbmZvcm1hdGlvbiBpbiBjYXNlIHRoaXMgbGlzdGVuZXJcbiogc2hvdWxkIGJlIGF0dGFjaGVkIHRvIGEgZ2xvYmFsIG9iamVjdCBsaWtlIHdpbmRvdywgZG9jdW1lbnQgb3IgYm9keVxuICpcbiAqIEBjb2RlR2VuQXBpXG4qL1xuZXhwb3J0IGZ1bmN0aW9uIMm1ybVjb21wb25lbnRIb3N0U3ludGhldGljTGlzdGVuZXIoXG4gICAgZXZlbnROYW1lOiBzdHJpbmcsIGxpc3RlbmVyRm46IChlPzogYW55KSA9PiBhbnksIHVzZUNhcHR1cmUgPSBmYWxzZSxcbiAgICBldmVudFRhcmdldFJlc29sdmVyPzogR2xvYmFsVGFyZ2V0UmVzb2x2ZXIpOiB2b2lkIHtcbiAgbGlzdGVuZXJJbnRlcm5hbChldmVudE5hbWUsIGxpc3RlbmVyRm4sIHVzZUNhcHR1cmUsIGV2ZW50VGFyZ2V0UmVzb2x2ZXIsIGxvYWRDb21wb25lbnRSZW5kZXJlcik7XG59XG5cbi8qKlxuICogQSB1dGlsaXR5IGZ1bmN0aW9uIHRoYXQgY2hlY2tzIGlmIGEgZ2l2ZW4gZWxlbWVudCBoYXMgYWxyZWFkeSBhbiBldmVudCBoYW5kbGVyIHJlZ2lzdGVyZWQgZm9yIGFuXG4gKiBldmVudCB3aXRoIGEgc3BlY2lmaWVkIG5hbWUuIFRoZSBUVmlldy5jbGVhbnVwIGRhdGEgc3RydWN0dXJlIGlzIHVzZWQgdG8gZmluZCBvdXQgd2hpY2ggZXZlbnRzXG4gKiBhcmUgcmVnaXN0ZXJlZCBmb3IgYSBnaXZlbiBlbGVtZW50LlxuICovXG5mdW5jdGlvbiBmaW5kRXhpc3RpbmdMaXN0ZW5lcihcbiAgICBsVmlldzogTFZpZXcsIGV2ZW50TmFtZTogc3RyaW5nLCB0Tm9kZUlkeDogbnVtYmVyKTogKChlPzogYW55KSA9PiBhbnkpfG51bGwge1xuICBjb25zdCB0VmlldyA9IGxWaWV3W1RWSUVXXTtcbiAgY29uc3QgdENsZWFudXAgPSB0Vmlldy5jbGVhbnVwO1xuICBpZiAodENsZWFudXAgIT0gbnVsbCkge1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgdENsZWFudXAubGVuZ3RoIC0gMTsgaSArPSAyKSB7XG4gICAgICBjb25zdCBjbGVhbnVwRXZlbnROYW1lID0gdENsZWFudXBbaV07XG4gICAgICBpZiAoY2xlYW51cEV2ZW50TmFtZSA9PT0gZXZlbnROYW1lICYmIHRDbGVhbnVwW2kgKyAxXSA9PT0gdE5vZGVJZHgpIHtcbiAgICAgICAgLy8gV2UgaGF2ZSBmb3VuZCBhIG1hdGNoaW5nIGV2ZW50IG5hbWUgb24gdGhlIHNhbWUgbm9kZSBidXQgaXQgbWlnaHQgbm90IGhhdmUgYmVlblxuICAgICAgICAvLyByZWdpc3RlcmVkIHlldCwgc28gd2UgbXVzdCBleHBsaWNpdGx5IHZlcmlmeSBlbnRyaWVzIGluIHRoZSBMVmlldyBjbGVhbnVwIGRhdGFcbiAgICAgICAgLy8gc3RydWN0dXJlcy5cbiAgICAgICAgY29uc3QgbENsZWFudXAgPSBsVmlld1tDTEVBTlVQXSAhO1xuICAgICAgICBjb25zdCBsaXN0ZW5lcklkeEluTENsZWFudXAgPSB0Q2xlYW51cFtpICsgMl07XG4gICAgICAgIHJldHVybiBsQ2xlYW51cC5sZW5ndGggPiBsaXN0ZW5lcklkeEluTENsZWFudXAgPyBsQ2xlYW51cFtsaXN0ZW5lcklkeEluTENsZWFudXBdIDogbnVsbDtcbiAgICAgIH1cbiAgICAgIC8vIFRWaWV3LmNsZWFudXAgY2FuIGhhdmUgYSBtaXggb2YgNC1lbGVtZW50cyBlbnRyaWVzIChmb3IgZXZlbnQgaGFuZGxlciBjbGVhbnVwcykgb3JcbiAgICAgIC8vIDItZWxlbWVudCBlbnRyaWVzIChmb3IgZGlyZWN0aXZlIGFuZCBxdWVyaWVzIGRlc3Ryb3kgaG9va3MpLiBBcyBzdWNoIHdlIGNhbiBlbmNvdW50ZXJcbiAgICAgIC8vIGJsb2NrcyBvZiA0IG9yIDIgaXRlbXMgaW4gdGhlIHRWaWV3LmNsZWFudXAgYW5kIHRoaXMgaXMgd2h5IHdlIGl0ZXJhdGUgb3ZlciAyIGVsZW1lbnRzXG4gICAgICAvLyBmaXJzdCBhbmQganVtcCBhbm90aGVyIDIgZWxlbWVudHMgaWYgd2UgZGV0ZWN0IGxpc3RlbmVycyBjbGVhbnVwICg0IGVsZW1lbnRzKS4gQWxzbyBjaGVja1xuICAgICAgLy8gZG9jdW1lbnRhdGlvbiBvZiBUVmlldy5jbGVhbnVwIGZvciBtb3JlIGRldGFpbHMgb2YgdGhpcyBkYXRhIHN0cnVjdHVyZSBsYXlvdXQuXG4gICAgICBpZiAodHlwZW9mIGNsZWFudXBFdmVudE5hbWUgPT09ICdzdHJpbmcnKSB7XG4gICAgICAgIGkgKz0gMjtcbiAgICAgIH1cbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbmZ1bmN0aW9uIGxpc3RlbmVySW50ZXJuYWwoXG4gICAgZXZlbnROYW1lOiBzdHJpbmcsIGxpc3RlbmVyRm46IChlPzogYW55KSA9PiBhbnksIHVzZUNhcHR1cmUgPSBmYWxzZSxcbiAgICBldmVudFRhcmdldFJlc29sdmVyPzogR2xvYmFsVGFyZ2V0UmVzb2x2ZXIsXG4gICAgbG9hZFJlbmRlcmVyRm4/OiAoKHROb2RlOiBUTm9kZSwgbFZpZXc6IExWaWV3KSA9PiBSZW5kZXJlcjMpIHwgbnVsbCk6IHZvaWQge1xuICBjb25zdCBsVmlldyA9IGdldExWaWV3KCk7XG4gIGNvbnN0IHROb2RlID0gZ2V0UHJldmlvdXNPclBhcmVudFROb2RlKCk7XG4gIGNvbnN0IHRWaWV3ID0gbFZpZXdbVFZJRVddO1xuICBjb25zdCBmaXJzdFRlbXBsYXRlUGFzcyA9IHRWaWV3LmZpcnN0VGVtcGxhdGVQYXNzO1xuICBjb25zdCB0Q2xlYW51cDogZmFsc2V8YW55W10gPSBmaXJzdFRlbXBsYXRlUGFzcyAmJiAodFZpZXcuY2xlYW51cCB8fCAodFZpZXcuY2xlYW51cCA9IFtdKSk7XG5cbiAgbmdEZXZNb2RlICYmIGFzc2VydE5vZGVPZlBvc3NpYmxlVHlwZXMoXG4gICAgICAgICAgICAgICAgICAgdE5vZGUsIFROb2RlVHlwZS5FbGVtZW50LCBUTm9kZVR5cGUuQ29udGFpbmVyLCBUTm9kZVR5cGUuRWxlbWVudENvbnRhaW5lcik7XG5cbiAgbGV0IHByb2Nlc3NPdXRwdXRzID0gdHJ1ZTtcblxuICAvLyBhZGQgbmF0aXZlIGV2ZW50IGxpc3RlbmVyIC0gYXBwbGljYWJsZSB0byBlbGVtZW50cyBvbmx5XG4gIGlmICh0Tm9kZS50eXBlID09PSBUTm9kZVR5cGUuRWxlbWVudCkge1xuICAgIGNvbnN0IG5hdGl2ZSA9IGdldE5hdGl2ZUJ5VE5vZGUodE5vZGUsIGxWaWV3KSBhcyBSRWxlbWVudDtcbiAgICBjb25zdCByZXNvbHZlZCA9IGV2ZW50VGFyZ2V0UmVzb2x2ZXIgPyBldmVudFRhcmdldFJlc29sdmVyKG5hdGl2ZSkgOiBFTVBUWV9PQkogYXMgYW55O1xuICAgIGNvbnN0IHRhcmdldCA9IHJlc29sdmVkLnRhcmdldCB8fCBuYXRpdmU7XG4gICAgY29uc3QgcmVuZGVyZXIgPSBsb2FkUmVuZGVyZXJGbiA/IGxvYWRSZW5kZXJlckZuKHROb2RlLCBsVmlldykgOiBsVmlld1tSRU5ERVJFUl07XG4gICAgY29uc3QgbENsZWFudXAgPSBnZXRDbGVhbnVwKGxWaWV3KTtcbiAgICBjb25zdCBsQ2xlYW51cEluZGV4ID0gbENsZWFudXAubGVuZ3RoO1xuICAgIGNvbnN0IGlkeE9yVGFyZ2V0R2V0dGVyID0gZXZlbnRUYXJnZXRSZXNvbHZlciA/XG4gICAgICAgIChfbFZpZXc6IExWaWV3KSA9PiBldmVudFRhcmdldFJlc29sdmVyKHVud3JhcFJOb2RlKF9sVmlld1t0Tm9kZS5pbmRleF0pKS50YXJnZXQgOlxuICAgICAgICB0Tm9kZS5pbmRleDtcblxuICAgIC8vIEluIG9yZGVyIHRvIG1hdGNoIGN1cnJlbnQgYmVoYXZpb3IsIG5hdGl2ZSBET00gZXZlbnQgbGlzdGVuZXJzIG11c3QgYmUgYWRkZWQgZm9yIGFsbFxuICAgIC8vIGV2ZW50cyAoaW5jbHVkaW5nIG91dHB1dHMpLlxuICAgIGlmIChpc1Byb2NlZHVyYWxSZW5kZXJlcihyZW5kZXJlcikpIHtcbiAgICAgIC8vIFRoZXJlIG1pZ2h0IGJlIGNhc2VzIHdoZXJlIG11bHRpcGxlIGRpcmVjdGl2ZXMgb24gdGhlIHNhbWUgZWxlbWVudCB0cnkgdG8gcmVnaXN0ZXIgYW4gZXZlbnRcbiAgICAgIC8vIGhhbmRsZXIgZnVuY3Rpb24gZm9yIHRoZSBzYW1lIGV2ZW50LiBJbiB0aGlzIHNpdHVhdGlvbiB3ZSB3YW50IHRvIGF2b2lkIHJlZ2lzdHJhdGlvbiBvZlxuICAgICAgLy8gc2V2ZXJhbCBuYXRpdmUgbGlzdGVuZXJzIGFzIGVhY2ggcmVnaXN0cmF0aW9uIHdvdWxkIGJlIGludGVyY2VwdGVkIGJ5IE5nWm9uZSBhbmRcbiAgICAgIC8vIHRyaWdnZXIgY2hhbmdlIGRldGVjdGlvbi4gVGhpcyB3b3VsZCBtZWFuIHRoYXQgYSBzaW5nbGUgdXNlciBhY3Rpb24gd291bGQgcmVzdWx0IGluIHNldmVyYWxcbiAgICAgIC8vIGNoYW5nZSBkZXRlY3Rpb25zIGJlaW5nIGludm9rZWQuIFRvIGF2b2lkIHRoaXMgc2l0dWF0aW9uIHdlIHdhbnQgdG8gaGF2ZSBvbmx5IG9uZSBjYWxsIHRvXG4gICAgICAvLyBuYXRpdmUgaGFuZGxlciByZWdpc3RyYXRpb24gKGZvciB0aGUgc2FtZSBlbGVtZW50IGFuZCBzYW1lIHR5cGUgb2YgZXZlbnQpLlxuICAgICAgLy9cbiAgICAgIC8vIEluIG9yZGVyIHRvIGhhdmUganVzdCBvbmUgbmF0aXZlIGV2ZW50IGhhbmRsZXIgaW4gcHJlc2VuY2Ugb2YgbXVsdGlwbGUgaGFuZGxlciBmdW5jdGlvbnMsXG4gICAgICAvLyB3ZSBqdXN0IHJlZ2lzdGVyIGEgZmlyc3QgaGFuZGxlciBmdW5jdGlvbiBhcyBhIG5hdGl2ZSBldmVudCBsaXN0ZW5lciBhbmQgdGhlbiBjaGFpblxuICAgICAgLy8gKGNvYWxlc2NlKSBvdGhlciBoYW5kbGVyIGZ1bmN0aW9ucyBvbiB0b3Agb2YgdGhlIGZpcnN0IG5hdGl2ZSBoYW5kbGVyIGZ1bmN0aW9uLlxuICAgICAgbGV0IGV4aXN0aW5nTGlzdGVuZXIgPSBudWxsO1xuICAgICAgLy8gUGxlYXNlIG5vdGUgdGhhdCB0aGUgY29hbGVzY2luZyBkZXNjcmliZWQgaGVyZSBkb2Vzbid0IGhhcHBlbiBmb3IgZXZlbnRzIHNwZWNpZnlpbmcgYW5cbiAgICAgIC8vIGFsdGVybmF0aXZlIHRhcmdldCAoZXguIChkb2N1bWVudDpjbGljaykpIC0gdGhpcyBpcyB0byBrZWVwIGJhY2t3YXJkIGNvbXBhdGliaWxpdHkgd2l0aCB0aGVcbiAgICAgIC8vIHZpZXcgZW5naW5lLlxuICAgICAgLy8gQWxzbywgd2UgZG9uJ3QgaGF2ZSB0byBzZWFyY2ggZm9yIGV4aXN0aW5nIGxpc3RlbmVycyBpcyB0aGVyZSBhcmUgbm8gZGlyZWN0aXZlc1xuICAgICAgLy8gbWF0Y2hpbmcgb24gYSBnaXZlbiBub2RlIGFzIHdlIGNhbid0IHJlZ2lzdGVyIG11bHRpcGxlIGV2ZW50IGhhbmRsZXJzIGZvciB0aGUgc2FtZSBldmVudCBpblxuICAgICAgLy8gYSB0ZW1wbGF0ZSAodGhpcyB3b3VsZCBtZWFuIGhhdmluZyBkdXBsaWNhdGUgYXR0cmlidXRlcykuXG4gICAgICBpZiAoIWV2ZW50VGFyZ2V0UmVzb2x2ZXIgJiYgaGFzRGlyZWN0aXZlcyh0Tm9kZSkpIHtcbiAgICAgICAgZXhpc3RpbmdMaXN0ZW5lciA9IGZpbmRFeGlzdGluZ0xpc3RlbmVyKGxWaWV3LCBldmVudE5hbWUsIHROb2RlLmluZGV4KTtcbiAgICAgIH1cbiAgICAgIGlmIChleGlzdGluZ0xpc3RlbmVyICE9PSBudWxsKSB7XG4gICAgICAgIC8vIEF0dGFjaCBhIG5ldyBsaXN0ZW5lciBhdCB0aGUgaGVhZCBvZiB0aGUgY29hbGVzY2VkIGxpc3RlbmVycyBsaXN0LlxuICAgICAgICAoPGFueT5saXN0ZW5lckZuKS5fX25nTmV4dExpc3RlbmVyRm5fXyA9ICg8YW55PmV4aXN0aW5nTGlzdGVuZXIpLl9fbmdOZXh0TGlzdGVuZXJGbl9fO1xuICAgICAgICAoPGFueT5leGlzdGluZ0xpc3RlbmVyKS5fX25nTmV4dExpc3RlbmVyRm5fXyA9IGxpc3RlbmVyRm47XG4gICAgICAgIHByb2Nlc3NPdXRwdXRzID0gZmFsc2U7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICAvLyBUaGUgZmlyc3QgYXJndW1lbnQgb2YgYGxpc3RlbmAgZnVuY3Rpb24gaW4gUHJvY2VkdXJhbCBSZW5kZXJlciBpczpcbiAgICAgICAgLy8gLSBlaXRoZXIgYSB0YXJnZXQgbmFtZSAoYXMgYSBzdHJpbmcpIGluIGNhc2Ugb2YgZ2xvYmFsIHRhcmdldCAod2luZG93LCBkb2N1bWVudCwgYm9keSlcbiAgICAgICAgLy8gLSBvciBlbGVtZW50IHJlZmVyZW5jZSAoaW4gYWxsIG90aGVyIGNhc2VzKVxuICAgICAgICBsaXN0ZW5lckZuID0gd3JhcExpc3RlbmVyKHROb2RlLCBsVmlldywgbGlzdGVuZXJGbiwgZmFsc2UgLyoqIHByZXZlbnREZWZhdWx0ICovKTtcbiAgICAgICAgY29uc3QgY2xlYW51cEZuID0gcmVuZGVyZXIubGlzdGVuKHJlc29sdmVkLm5hbWUgfHwgdGFyZ2V0LCBldmVudE5hbWUsIGxpc3RlbmVyRm4pO1xuICAgICAgICBuZ0Rldk1vZGUgJiYgbmdEZXZNb2RlLnJlbmRlcmVyQWRkRXZlbnRMaXN0ZW5lcisrO1xuXG4gICAgICAgIGxDbGVhbnVwLnB1c2gobGlzdGVuZXJGbiwgY2xlYW51cEZuKTtcbiAgICAgICAgdENsZWFudXAgJiYgdENsZWFudXAucHVzaChldmVudE5hbWUsIGlkeE9yVGFyZ2V0R2V0dGVyLCBsQ2xlYW51cEluZGV4LCBsQ2xlYW51cEluZGV4ICsgMSk7XG4gICAgICB9XG5cbiAgICB9IGVsc2Uge1xuICAgICAgbGlzdGVuZXJGbiA9IHdyYXBMaXN0ZW5lcih0Tm9kZSwgbFZpZXcsIGxpc3RlbmVyRm4sIHRydWUgLyoqIHByZXZlbnREZWZhdWx0ICovKTtcbiAgICAgIHRhcmdldC5hZGRFdmVudExpc3RlbmVyKGV2ZW50TmFtZSwgbGlzdGVuZXJGbiwgdXNlQ2FwdHVyZSk7XG4gICAgICBuZ0Rldk1vZGUgJiYgbmdEZXZNb2RlLnJlbmRlcmVyQWRkRXZlbnRMaXN0ZW5lcisrO1xuXG4gICAgICBsQ2xlYW51cC5wdXNoKGxpc3RlbmVyRm4pO1xuICAgICAgdENsZWFudXAgJiYgdENsZWFudXAucHVzaChldmVudE5hbWUsIGlkeE9yVGFyZ2V0R2V0dGVyLCBsQ2xlYW51cEluZGV4LCB1c2VDYXB0dXJlKTtcbiAgICB9XG4gIH1cblxuICAvLyBzdWJzY3JpYmUgdG8gZGlyZWN0aXZlIG91dHB1dHNcbiAgaWYgKHROb2RlLm91dHB1dHMgPT09IHVuZGVmaW5lZCkge1xuICAgIC8vIGlmIHdlIGNyZWF0ZSBUTm9kZSBoZXJlLCBpbnB1dHMgbXVzdCBiZSB1bmRlZmluZWQgc28gd2Uga25vdyB0aGV5IHN0aWxsIG5lZWQgdG8gYmVcbiAgICAvLyBjaGVja2VkXG4gICAgdE5vZGUub3V0cHV0cyA9IGdlbmVyYXRlUHJvcGVydHlBbGlhc2VzKHROb2RlLCBCaW5kaW5nRGlyZWN0aW9uLk91dHB1dCk7XG4gIH1cblxuICBjb25zdCBvdXRwdXRzID0gdE5vZGUub3V0cHV0cztcbiAgbGV0IHByb3BzOiBQcm9wZXJ0eUFsaWFzVmFsdWV8dW5kZWZpbmVkO1xuICBpZiAocHJvY2Vzc091dHB1dHMgJiYgb3V0cHV0cyAmJiAocHJvcHMgPSBvdXRwdXRzW2V2ZW50TmFtZV0pKSB7XG4gICAgY29uc3QgcHJvcHNMZW5ndGggPSBwcm9wcy5sZW5ndGg7XG4gICAgaWYgKHByb3BzTGVuZ3RoKSB7XG4gICAgICBjb25zdCBsQ2xlYW51cCA9IGdldENsZWFudXAobFZpZXcpO1xuICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCBwcm9wc0xlbmd0aDsgaSArPSAzKSB7XG4gICAgICAgIGNvbnN0IGluZGV4ID0gcHJvcHNbaV0gYXMgbnVtYmVyO1xuICAgICAgICBuZ0Rldk1vZGUgJiYgYXNzZXJ0RGF0YUluUmFuZ2UobFZpZXcsIGluZGV4KTtcbiAgICAgICAgY29uc3QgbWluaWZpZWROYW1lID0gcHJvcHNbaSArIDJdO1xuICAgICAgICBjb25zdCBkaXJlY3RpdmVJbnN0YW5jZSA9IGxWaWV3W2luZGV4XTtcbiAgICAgICAgY29uc3Qgb3V0cHV0ID0gZGlyZWN0aXZlSW5zdGFuY2VbbWluaWZpZWROYW1lXTtcblxuICAgICAgICBpZiAobmdEZXZNb2RlICYmICFpc09ic2VydmFibGUob3V0cHV0KSkge1xuICAgICAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICAgICAgYEBPdXRwdXQgJHttaW5pZmllZE5hbWV9IG5vdCBpbml0aWFsaXplZCBpbiAnJHtkaXJlY3RpdmVJbnN0YW5jZS5jb25zdHJ1Y3Rvci5uYW1lfScuYCk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCBzdWJzY3JpcHRpb24gPSBvdXRwdXQuc3Vic2NyaWJlKGxpc3RlbmVyRm4pO1xuICAgICAgICBjb25zdCBpZHggPSBsQ2xlYW51cC5sZW5ndGg7XG4gICAgICAgIGxDbGVhbnVwLnB1c2gobGlzdGVuZXJGbiwgc3Vic2NyaXB0aW9uKTtcbiAgICAgICAgdENsZWFudXAgJiYgdENsZWFudXAucHVzaChldmVudE5hbWUsIHROb2RlLmluZGV4LCBpZHgsIC0oaWR4ICsgMSkpO1xuICAgICAgfVxuICAgIH1cbiAgfVxufVxuXG5mdW5jdGlvbiBleGVjdXRlTGlzdGVuZXJXaXRoRXJyb3JIYW5kbGluZyhcbiAgICBsVmlldzogTFZpZXcsIGxpc3RlbmVyRm46IChlPzogYW55KSA9PiBhbnksIGU6IGFueSk6IGJvb2xlYW4ge1xuICB0cnkge1xuICAgIC8vIE9ubHkgZXhwbGljaXRseSByZXR1cm5pbmcgZmFsc2UgZnJvbSBhIGxpc3RlbmVyIHNob3VsZCBwcmV2ZW50RGVmYXVsdFxuICAgIHJldHVybiBsaXN0ZW5lckZuKGUpICE9PSBmYWxzZTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBoYW5kbGVFcnJvcihsVmlldywgZXJyb3IpO1xuICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuXG4vKipcbiAqIFdyYXBzIGFuIGV2ZW50IGxpc3RlbmVyIHdpdGggYSBmdW5jdGlvbiB0aGF0IG1hcmtzIGFuY2VzdG9ycyBkaXJ0eSBhbmQgcHJldmVudHMgZGVmYXVsdCBiZWhhdmlvcixcbiAqIGlmIGFwcGxpY2FibGUuXG4gKlxuICogQHBhcmFtIHROb2RlIFRoZSBUTm9kZSBhc3NvY2lhdGVkIHdpdGggdGhpcyBsaXN0ZW5lclxuICogQHBhcmFtIGxWaWV3IFRoZSBMVmlldyB0aGF0IGNvbnRhaW5zIHRoaXMgbGlzdGVuZXJcbiAqIEBwYXJhbSBsaXN0ZW5lckZuIFRoZSBsaXN0ZW5lciBmdW5jdGlvbiB0byBjYWxsXG4gKiBAcGFyYW0gd3JhcFdpdGhQcmV2ZW50RGVmYXVsdCBXaGV0aGVyIG9yIG5vdCB0byBwcmV2ZW50IGRlZmF1bHQgYmVoYXZpb3JcbiAqICh0aGUgcHJvY2VkdXJhbCByZW5kZXJlciBkb2VzIHRoaXMgYWxyZWFkeSwgc28gaW4gdGhvc2UgY2FzZXMsIHdlIHNob3VsZCBza2lwKVxuICovXG5mdW5jdGlvbiB3cmFwTGlzdGVuZXIoXG4gICAgdE5vZGU6IFROb2RlLCBsVmlldzogTFZpZXcsIGxpc3RlbmVyRm46IChlPzogYW55KSA9PiBhbnksXG4gICAgd3JhcFdpdGhQcmV2ZW50RGVmYXVsdDogYm9vbGVhbik6IEV2ZW50TGlzdGVuZXIge1xuICAvLyBOb3RlOiB3ZSBhcmUgcGVyZm9ybWluZyBtb3N0IG9mIHRoZSB3b3JrIGluIHRoZSBsaXN0ZW5lciBmdW5jdGlvbiBpdHNlbGZcbiAgLy8gdG8gb3B0aW1pemUgbGlzdGVuZXIgcmVnaXN0cmF0aW9uLlxuICByZXR1cm4gZnVuY3Rpb24gd3JhcExpc3RlbmVySW5fbWFya0RpcnR5QW5kUHJldmVudERlZmF1bHQoZTogRXZlbnQpIHtcbiAgICAvLyBJbiBvcmRlciB0byBiZSBiYWNrd2FyZHMgY29tcGF0aWJsZSB3aXRoIFZpZXcgRW5naW5lLCBldmVudHMgb24gY29tcG9uZW50IGhvc3Qgbm9kZXNcbiAgICAvLyBtdXN0IGFsc28gbWFyayB0aGUgY29tcG9uZW50IHZpZXcgaXRzZWxmIGRpcnR5IChpLmUuIHRoZSB2aWV3IHRoYXQgaXQgb3ducykuXG4gICAgY29uc3Qgc3RhcnRWaWV3ID1cbiAgICAgICAgdE5vZGUuZmxhZ3MgJiBUTm9kZUZsYWdzLmlzQ29tcG9uZW50ID8gZ2V0Q29tcG9uZW50Vmlld0J5SW5kZXgodE5vZGUuaW5kZXgsIGxWaWV3KSA6IGxWaWV3O1xuXG4gICAgLy8gU2VlIGludGVyZmFjZXMvdmlldy50cyBmb3IgbW9yZSBvbiBMVmlld0ZsYWdzLk1hbnVhbE9uUHVzaFxuICAgIGlmICgobFZpZXdbRkxBR1NdICYgTFZpZXdGbGFncy5NYW51YWxPblB1c2gpID09PSAwKSB7XG4gICAgICBtYXJrVmlld0RpcnR5KHN0YXJ0Vmlldyk7XG4gICAgfVxuXG4gICAgbGV0IHJlc3VsdCA9IGV4ZWN1dGVMaXN0ZW5lcldpdGhFcnJvckhhbmRsaW5nKGxWaWV3LCBsaXN0ZW5lckZuLCBlKTtcbiAgICAvLyBBIGp1c3QtaW52b2tlZCBsaXN0ZW5lciBmdW5jdGlvbiBtaWdodCBoYXZlIGNvYWxlc2NlZCBsaXN0ZW5lcnMgc28gd2UgbmVlZCB0byBjaGVjayBmb3JcbiAgICAvLyB0aGVpciBwcmVzZW5jZSBhbmQgaW52b2tlIGFzIG5lZWRlZC5cbiAgICBsZXQgbmV4dExpc3RlbmVyRm4gPSAoPGFueT53cmFwTGlzdGVuZXJJbl9tYXJrRGlydHlBbmRQcmV2ZW50RGVmYXVsdCkuX19uZ05leHRMaXN0ZW5lckZuX187XG4gICAgd2hpbGUgKG5leHRMaXN0ZW5lckZuKSB7XG4gICAgICAvLyBXZSBzaG91bGQgcHJldmVudCBkZWZhdWx0IGlmIGFueSBvZiB0aGUgbGlzdGVuZXJzIGV4cGxpY2l0bHkgcmV0dXJuIGZhbHNlXG4gICAgICByZXN1bHQgPSBleGVjdXRlTGlzdGVuZXJXaXRoRXJyb3JIYW5kbGluZyhsVmlldywgbmV4dExpc3RlbmVyRm4sIGUpICYmIHJlc3VsdDtcbiAgICAgIG5leHRMaXN0ZW5lckZuID0gKDxhbnk+bmV4dExpc3RlbmVyRm4pLl9fbmdOZXh0TGlzdGVuZXJGbl9fO1xuICAgIH1cblxuICAgIGlmICh3cmFwV2l0aFByZXZlbnREZWZhdWx0ICYmIHJlc3VsdCA9PT0gZmFsc2UpIHtcbiAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgIC8vIE5lY2Vzc2FyeSBmb3IgbGVnYWN5IGJyb3dzZXJzIHRoYXQgZG9uJ3Qgc3VwcG9ydCBwcmV2ZW50RGVmYXVsdCAoZS5nLiBJRSlcbiAgICAgIGUucmV0dXJuVmFsdWUgPSBmYWxzZTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9O1xufVxuIl19
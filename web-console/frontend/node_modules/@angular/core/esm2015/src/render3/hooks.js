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
import { assertEqual } from '../util/assert';
import { FLAGS, PREORDER_HOOK_FLAGS } from './interfaces/view';
/**
 * Adds all directive lifecycle hooks from the given `DirectiveDef` to the given `TView`.
 *
 * Must be run *only* on the first template pass.
 *
 * Sets up the pre-order hooks on the provided `tView`,
 * see {\@link HookData} for details about the data structure.
 *
 * @param {?} directiveIndex The index of the directive in LView
 * @param {?} directiveDef The definition containing the hooks to setup in tView
 * @param {?} tView The current TView
 * @param {?} nodeIndex The index of the node to which the directive is attached
 * @param {?} initialPreOrderHooksLength the number of pre-order hooks already registered before the
 * current process, used to know if the node index has to be added to the array. If it is -1,
 * the node index is never added.
 * @param {?} initialPreOrderCheckHooksLength same as previous for pre-order check hooks
 * @return {?}
 */
export function registerPreOrderHooks(directiveIndex, directiveDef, tView, nodeIndex, initialPreOrderHooksLength, initialPreOrderCheckHooksLength) {
    ngDevMode &&
        assertEqual(tView.firstTemplatePass, true, 'Should only be called on first template pass');
    const { onChanges, onInit, doCheck } = directiveDef;
    if (initialPreOrderHooksLength >= 0 &&
        (!tView.preOrderHooks || initialPreOrderHooksLength === tView.preOrderHooks.length) &&
        (onChanges || onInit || doCheck)) {
        (tView.preOrderHooks || (tView.preOrderHooks = [])).push(nodeIndex);
    }
    if (initialPreOrderCheckHooksLength >= 0 &&
        (!tView.preOrderCheckHooks ||
            initialPreOrderCheckHooksLength === tView.preOrderCheckHooks.length) &&
        (onChanges || doCheck)) {
        (tView.preOrderCheckHooks || (tView.preOrderCheckHooks = [])).push(nodeIndex);
    }
    if (onChanges) {
        (tView.preOrderHooks || (tView.preOrderHooks = [])).push(directiveIndex, onChanges);
        (tView.preOrderCheckHooks || (tView.preOrderCheckHooks = [])).push(directiveIndex, onChanges);
    }
    if (onInit) {
        (tView.preOrderHooks || (tView.preOrderHooks = [])).push(-directiveIndex, onInit);
    }
    if (doCheck) {
        (tView.preOrderHooks || (tView.preOrderHooks = [])).push(directiveIndex, doCheck);
        (tView.preOrderCheckHooks || (tView.preOrderCheckHooks = [])).push(directiveIndex, doCheck);
    }
}
/**
 *
 * Loops through the directives on the provided `tNode` and queues hooks to be
 * run that are not initialization hooks.
 *
 * Should be executed during `elementEnd()` and similar to
 * preserve hook execution order. Content, view, and destroy hooks for projected
 * components and directives must be called *before* their hosts.
 *
 * Sets up the content, view, and destroy hooks on the provided `tView`,
 * see {\@link HookData} for details about the data structure.
 *
 * NOTE: This does not set up `onChanges`, `onInit` or `doCheck`, those are set up
 * separately at `elementStart`.
 *
 * @param {?} tView The current TView
 * @param {?} tNode The TNode whose directives are to be searched for hooks to queue
 * @return {?}
 */
export function registerPostOrderHooks(tView, tNode) {
    if (tView.firstTemplatePass) {
        // It's necessary to loop through the directives at elementEnd() (rather than processing in
        // directiveCreate) so we can preserve the current hook order. Content, view, and destroy
        // hooks for projected components and directives must be called *before* their hosts.
        for (let i = tNode.directiveStart, end = tNode.directiveEnd; i < end; i++) {
            /** @type {?} */
            const directiveDef = (/** @type {?} */ (tView.data[i]));
            if (directiveDef.afterContentInit) {
                (tView.contentHooks || (tView.contentHooks = [])).push(-i, directiveDef.afterContentInit);
            }
            if (directiveDef.afterContentChecked) {
                (tView.contentHooks || (tView.contentHooks = [])).push(i, directiveDef.afterContentChecked);
                (tView.contentCheckHooks || (tView.contentCheckHooks = [])).push(i, directiveDef.afterContentChecked);
            }
            if (directiveDef.afterViewInit) {
                (tView.viewHooks || (tView.viewHooks = [])).push(-i, directiveDef.afterViewInit);
            }
            if (directiveDef.afterViewChecked) {
                (tView.viewHooks || (tView.viewHooks = [])).push(i, directiveDef.afterViewChecked);
                (tView.viewCheckHooks || (tView.viewCheckHooks = [])).push(i, directiveDef.afterViewChecked);
            }
            if (directiveDef.onDestroy != null) {
                (tView.destroyHooks || (tView.destroyHooks = [])).push(i, directiveDef.onDestroy);
            }
        }
    }
}
/**
 * Executing hooks requires complex logic as we need to deal with 2 constraints.
 *
 * 1. Init hooks (ngOnInit, ngAfterContentInit, ngAfterViewInit) must all be executed once and only
 * once, across many change detection cycles. This must be true even if some hooks throw, or if
 * some recursively trigger a change detection cycle.
 * To solve that, it is required to track the state of the execution of these init hooks.
 * This is done by storing and maintaining flags in the view: the {@link InitPhaseState},
 * and the index within that phase. They can be seen as a cursor in the following structure:
 * [[onInit1, onInit2], [afterContentInit1], [afterViewInit1, afterViewInit2, afterViewInit3]]
 * They are are stored as flags in LView[FLAGS].
 *
 * 2. Pre-order hooks can be executed in batches, because of the select instruction.
 * To be able to pause and resume their execution, we also need some state about the hook's array
 * that is being processed:
 * - the index of the next hook to be executed
 * - the number of init hooks already found in the processed part of the  array
 * They are are stored as flags in LView[PREORDER_HOOK_FLAGS].
 */
/**
 * Executes necessary hooks at the start of executing a template.
 *
 * Executes hooks that are to be run during the initialization of a directive such
 * as `onChanges`, `onInit`, and `doCheck`.
 *
 * @param {?} currentView
 * @param {?} tView Static data for the view containing the hooks to be executed
 * @param {?} checkNoChangesMode Whether or not we're in checkNoChanges mode.
 * @param {?} currentNodeIndex
 * @return {?}
 */
export function executePreOrderHooks(currentView, tView, checkNoChangesMode, currentNodeIndex) {
    if (!checkNoChangesMode) {
        executeHooks(currentView, tView.preOrderHooks, tView.preOrderCheckHooks, checkNoChangesMode, 0 /* OnInitHooksToBeRun */, currentNodeIndex !== undefined ? currentNodeIndex : null);
    }
}
/**
 * Executes hooks against the given `LView` based off of whether or not
 * This is the first pass.
 *
 * @param {?} currentView The view instance data to run the hooks against
 * @param {?} firstPassHooks An array of hooks to run if we're in the first view pass
 * @param {?} checkHooks An Array of hooks to run if we're not in the first view pass.
 * @param {?} checkNoChangesMode Whether or not we're in no changes mode.
 * @param {?} initPhaseState the current state of the init phase
 * @param {?} currentNodeIndex 3 cases depending the the value:
 * - undefined: all hooks from the array should be executed (post-order case)
 * - null: execute hooks only from the saved index until the end of the array (pre-order case, when
 * flushing the remaining hooks)
 * - number: execute hooks only from the saved index until that node index exclusive (pre-order
 * case, when executing select(number))
 * @return {?}
 */
export function executeHooks(currentView, firstPassHooks, checkHooks, checkNoChangesMode, initPhaseState, currentNodeIndex) {
    if (checkNoChangesMode)
        return;
    /** @type {?} */
    const hooksToCall = (currentView[FLAGS] & 3 /* InitPhaseStateMask */) === initPhaseState ?
        firstPassHooks :
        checkHooks;
    if (hooksToCall) {
        callHooks(currentView, hooksToCall, initPhaseState, currentNodeIndex);
    }
    // The init phase state must be always checked here as it may have been recursively updated
    if (currentNodeIndex == null &&
        (currentView[FLAGS] & 3 /* InitPhaseStateMask */) === initPhaseState &&
        initPhaseState !== 3 /* InitPhaseCompleted */) {
        currentView[FLAGS] &= 1023 /* IndexWithinInitPhaseReset */;
        currentView[FLAGS] += 1 /* InitPhaseStateIncrementer */;
    }
}
/**
 * Calls lifecycle hooks with their contexts, skipping init hooks if it's not
 * the first LView pass
 *
 * @param {?} currentView The current view
 * @param {?} arr The array in which the hooks are found
 * @param {?} initPhase
 * @param {?} currentNodeIndex 3 cases depending the the value:
 * - undefined: all hooks from the array should be executed (post-order case)
 * - null: execute hooks only from the saved index until the end of the array (pre-order case, when
 * flushing the remaining hooks)
 * - number: execute hooks only from the saved index until that node index exclusive (pre-order
 * case, when executing select(number))
 * @return {?}
 */
function callHooks(currentView, arr, initPhase, currentNodeIndex) {
    /** @type {?} */
    const startIndex = currentNodeIndex !== undefined ?
        (currentView[PREORDER_HOOK_FLAGS] & 65535 /* IndexOfTheNextPreOrderHookMaskMask */) :
        0;
    /** @type {?} */
    const nodeIndexLimit = currentNodeIndex != null ? currentNodeIndex : -1;
    /** @type {?} */
    let lastNodeIndexFound = 0;
    for (let i = startIndex; i < arr.length; i++) {
        /** @type {?} */
        const hook = (/** @type {?} */ (arr[i + 1]));
        if (typeof hook === 'number') {
            lastNodeIndexFound = (/** @type {?} */ (arr[i]));
            if (currentNodeIndex != null && lastNodeIndexFound >= currentNodeIndex) {
                break;
            }
        }
        else {
            /** @type {?} */
            const isInitHook = arr[i] < 0;
            if (isInitHook)
                currentView[PREORDER_HOOK_FLAGS] += 65536 /* NumberOfInitHooksCalledIncrementer */;
            if (lastNodeIndexFound < nodeIndexLimit || nodeIndexLimit == -1) {
                callHook(currentView, initPhase, arr, i);
                currentView[PREORDER_HOOK_FLAGS] =
                    (currentView[PREORDER_HOOK_FLAGS] & 4294901760 /* NumberOfInitHooksCalledMask */) + i +
                        2;
            }
            i++;
        }
    }
}
/**
 * Execute one hook against the current `LView`.
 *
 * @param {?} currentView The current view
 * @param {?} initPhase
 * @param {?} arr The array in which the hooks are found
 * @param {?} i The current index within the hook data array
 * @return {?}
 */
function callHook(currentView, initPhase, arr, i) {
    /** @type {?} */
    const isInitHook = arr[i] < 0;
    /** @type {?} */
    const hook = (/** @type {?} */ (arr[i + 1]));
    /** @type {?} */
    const directiveIndex = isInitHook ? -arr[i] : (/** @type {?} */ (arr[i]));
    /** @type {?} */
    const directive = currentView[directiveIndex];
    if (isInitHook) {
        /** @type {?} */
        const indexWithintInitPhase = currentView[FLAGS] >> 10 /* IndexWithinInitPhaseShift */;
        // The init phase state must be always checked here as it may have been recursively
        // updated
        if (indexWithintInitPhase <
            (currentView[PREORDER_HOOK_FLAGS] >> 16 /* NumberOfInitHooksCalledShift */) &&
            (currentView[FLAGS] & 3 /* InitPhaseStateMask */) === initPhase) {
            currentView[FLAGS] += 1024 /* IndexWithinInitPhaseIncrementer */;
            hook.call(directive);
        }
    }
    else {
        hook.call(directive);
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaG9va3MuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL2hvb2tzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLFdBQVcsRUFBQyxNQUFNLGdCQUFnQixDQUFDO0FBSTNDLE9BQU8sRUFBQyxLQUFLLEVBQStDLG1CQUFtQixFQUEyQixNQUFNLG1CQUFtQixDQUFDOzs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBcUJwSSxNQUFNLFVBQVUscUJBQXFCLENBQ2pDLGNBQXNCLEVBQUUsWUFBK0IsRUFBRSxLQUFZLEVBQUUsU0FBaUIsRUFDeEYsMEJBQWtDLEVBQUUsK0JBQXVDO0lBQzdFLFNBQVM7UUFDTCxXQUFXLENBQUMsS0FBSyxDQUFDLGlCQUFpQixFQUFFLElBQUksRUFBRSw4Q0FBOEMsQ0FBQyxDQUFDO1VBRXpGLEVBQUMsU0FBUyxFQUFFLE1BQU0sRUFBRSxPQUFPLEVBQUMsR0FBRyxZQUFZO0lBQ2pELElBQUksMEJBQTBCLElBQUksQ0FBQztRQUMvQixDQUFDLENBQUMsS0FBSyxDQUFDLGFBQWEsSUFBSSwwQkFBMEIsS0FBSyxLQUFLLENBQUMsYUFBYSxDQUFDLE1BQU0sQ0FBQztRQUNuRixDQUFDLFNBQVMsSUFBSSxNQUFNLElBQUksT0FBTyxDQUFDLEVBQUU7UUFDcEMsQ0FBQyxLQUFLLENBQUMsYUFBYSxJQUFJLENBQUMsS0FBSyxDQUFDLGFBQWEsR0FBRyxFQUFFLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQztLQUNyRTtJQUVELElBQUksK0JBQStCLElBQUksQ0FBQztRQUNwQyxDQUFDLENBQUMsS0FBSyxDQUFDLGtCQUFrQjtZQUN6QiwrQkFBK0IsS0FBSyxLQUFLLENBQUMsa0JBQWtCLENBQUMsTUFBTSxDQUFDO1FBQ3JFLENBQUMsU0FBUyxJQUFJLE9BQU8sQ0FBQyxFQUFFO1FBQzFCLENB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QVcsQ0FBQyxLQUFLLENBQUMsOENBQThDLENBQUM7WUFDakUsSUFBSSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQztTQUN0QjtLQUNGO1NBQU07UUFDTCxJQUFJLENBQUMsSUFBSSxDQUFDLFNBQVMsQ0FBQyxDQUFDO0tBQ3RCO0FBQ0gsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHthc3NlcnRFcXVhbH0gZnJvbSAnLi4vdXRpbC9hc3NlcnQnO1xuXG5pbXBvcnQge0RpcmVjdGl2ZURlZn0gZnJvbSAnLi9pbnRlcmZhY2VzL2RlZmluaXRpb24nO1xuaW1wb3J0IHtUTm9kZX0gZnJvbSAnLi9pbnRlcmZhY2VzL25vZGUnO1xuaW1wb3J0IHtGTEFHUywgSG9va0RhdGEsIEluaXRQaGFzZVN0YXRlLCBMVmlldywgTFZpZXdGbGFncywgUFJFT1JERVJfSE9PS19GTEFHUywgUHJlT3JkZXJIb29rRmxhZ3MsIFRWaWV3fSBmcm9tICcuL2ludGVyZmFjZXMvdmlldyc7XG5cblxuXG4vKipcbiAqIEFkZHMgYWxsIGRpcmVjdGl2ZSBsaWZlY3ljbGUgaG9va3MgZnJvbSB0aGUgZ2l2ZW4gYERpcmVjdGl2ZURlZmAgdG8gdGhlIGdpdmVuIGBUVmlld2AuXG4gKlxuICogTXVzdCBiZSBydW4gKm9ubHkqIG9uIHRoZSBmaXJzdCB0ZW1wbGF0ZSBwYXNzLlxuICpcbiAqIFNldHMgdXAgdGhlIHByZS1vcmRlciBob29rcyBvbiB0aGUgcHJvdmlkZWQgYHRWaWV3YCxcbiAqIHNlZSB7QGxpbmsgSG9va0RhdGF9IGZvciBkZXRhaWxzIGFib3V0IHRoZSBkYXRhIHN0cnVjdHVyZS5cbiAqXG4gKiBAcGFyYW0gZGlyZWN0aXZlSW5kZXggVGhlIGluZGV4IG9mIHRoZSBkaXJlY3RpdmUgaW4gTFZpZXdcbiAqIEBwYXJhbSBkaXJlY3RpdmVEZWYgVGhlIGRlZmluaXRpb24gY29udGFpbmluZyB0aGUgaG9va3MgdG8gc2V0dXAgaW4gdFZpZXdcbiAqIEBwYXJhbSB0VmlldyBUaGUgY3VycmVudCBUVmlld1xuICogQHBhcmFtIG5vZGVJbmRleCBUaGUgaW5kZXggb2YgdGhlIG5vZGUgdG8gd2hpY2ggdGhlIGRpcmVjdGl2ZSBpcyBhdHRhY2hlZFxuICogQHBhcmFtIGluaXRpYWxQcmVPcmRlckhvb2tzTGVuZ3RoIHRoZSBudW1iZXIgb2YgcHJlLW9yZGVyIGhvb2tzIGFscmVhZHkgcmVnaXN0ZXJlZCBiZWZvcmUgdGhlXG4gKiBjdXJyZW50IHByb2Nlc3MsIHVzZWQgdG8ga25vdyBpZiB0aGUgbm9kZSBpbmRleCBoYXMgdG8gYmUgYWRkZWQgdG8gdGhlIGFycmF5LiBJZiBpdCBpcyAtMSxcbiAqIHRoZSBub2RlIGluZGV4IGlzIG5ldmVyIGFkZGVkLlxuICogQHBhcmFtIGluaXRpYWxQcmVPcmRlckNoZWNrSG9va3NMZW5ndGggc2FtZSBhcyBwcmV2aW91cyBmb3IgcHJlLW9yZGVyIGNoZWNrIGhvb2tzXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiByZWdpc3RlclByZU9yZGVySG9va3MoXG4gICAgZGlyZWN0aXZlSW5kZXg6IG51bWJlciwgZGlyZWN0aXZlRGVmOiBEaXJlY3RpdmVEZWY8YW55PiwgdFZpZXc6IFRWaWV3LCBub2RlSW5kZXg6IG51bWJlcixcbiAgICBpbml0aWFsUHJlT3JkZXJIb29rc0xlbmd0aDogbnVtYmVyLCBpbml0aWFsUHJlT3JkZXJDaGVja0hvb2tzTGVuZ3RoOiBudW1iZXIpOiB2b2lkIHtcbiAgbmdEZXZNb2RlICYmXG4gICAgICBhc3NlcnRFcXVhbCh0Vmlldy5maXJzdFRlbXBsYXRlUGFzcywgdHJ1ZSwgJ1Nob3VsZCBvbmx5IGJlIGNhbGxlZCBvbiBmaXJzdCB0ZW1wbGF0ZSBwYXNzJyk7XG5cbiAgY29uc3Qge29uQ2hhbmdlcywgb25Jbml0LCBkb0NoZWNrfSA9IGRpcmVjdGl2ZURlZjtcbiAgaWYgKGluaXRpYWxQcmVPcmRlckhvb2tzTGVuZ3RoID49IDAgJiZcbiAgICAgICghdFZpZXcucHJlT3JkZXJIb29rcyB8fCBpbml0aWFsUHJlT3JkZXJIb29rc0xlbmd0aCA9PT0gdFZpZXcucHJlT3JkZXJIb29rcy5sZW5ndGgpICYmXG4gICAgICAob25DaGFuZ2VzIHx8IG9uSW5pdCB8fCBkb0NoZWNrKSkge1xuICAgICh0Vmlldy5wcmVPcmRlckhvb2tzIHx8ICh0Vmlldy5wcmVPcmRlckhvb2tzID0gW10pKS5wdXNoKG5vZGVJbmRleCk7XG4gIH1cblxuICBpZiAoaW5pdGlhbFByZU9yZGVyQ2hlY2tIb29rc0xlbmd0aCA+PSAwICYmXG4gICAgICAoIXRWaWV3LnByZU9yZGVyQ2hlY2tIb29rcyB8fFxuICAgICAgIGluaXRpYWxQcmVPcmRlckNoZWNrSG9va3NMZW5ndGggPT09IHRWaWV3LnByZU9yZGVyQ2hlY2tIb29rcy5sZW5ndGgpICYmXG4gICAgICAob25DaGFuZ2VzIHx8IGRvQ2hlY2spKSB7XG4gICAgKHRWaWV3LnByZU9yZGVyQ2hlY2tIb29rcyB8fCAodFZpZXcucHJlT3JkZXJDaGVja0hvb2tzID0gW10pKS5wdXNoKG5vZGVJbmRleCk7XG4gIH1cblxuICBpZiAob25DaGFuZ2VzKSB7XG4gICAgKHRWaWV3LnByZU9yZGVySG9va3MgfHwgKHRWaWV3LnByZU9yZGVySG9va3MgPSBbXSkpLnB1c2goZGlyZWN0aXZlSW5kZXgsIG9uQ2hhbmdlcyk7XG4gICAgKHRWaWV3LnByZU9yZGVyQ2hlY2tIb29rcyB8fCAodFZpZXcucHJlT3JkZXJDaGVja0hvb2tzID0gW10pKS5wdXNoKGRpcmVjdGl2ZUluZGV4LCBvbkNoYW5nZXMpO1xuICB9XG5cbiAgaWYgKG9uSW5pdCkge1xuICAgICh0Vmlldy5wcmVPcmRlckhvb2tzIHx8ICh0Vmlldy5wcmVPcmRlckhvb2tzID0gW10pKS5wdXNoKC1kaXJlY3RpdmVJbmRleCwgb25Jbml0KTtcbiAgfVxuXG4gIGlmIChkb0NoZWNrKSB7XG4gICAgKHRWaWV3LnByZU9yZGVySG9va3MgfHwgKHRWaWV3LnByZU9yZGVySG9va3MgPSBbXSkpLnB1c2goZGlyZWN0aXZlSW5kZXgsIGRvQ2hlY2spO1xuICAgICh0Vmlldy5wcmVPcmRlckNoZWNrSG9va3MgfHwgKHRWaWV3LnByZU9yZGVyQ2hlY2tIb29rcyA9IFtdKSkucHVzaChkaXJlY3RpdmVJbmRleCwgZG9DaGVjayk7XG4gIH1cbn1cblxuLyoqXG4gKlxuICogTG9vcHMgdGhyb3VnaCB0aGUgZGlyZWN0aXZlcyBvbiB0aGUgcHJvdmlkZWQgYHROb2RlYCBhbmQgcXVldWVzIGhvb2tzIHRvIGJlXG4gKiBydW4gdGhhdCBhcmUgbm90IGluaXRpYWxpemF0aW9uIGhvb2tzLlxuICpcbiAqIFNob3VsZCBiZSBleGVjdXRlZCBkdXJpbmcgYGVsZW1lbnRFbmQoKWAgYW5kIHNpbWlsYXIgdG9cbiAqIHByZXNlcnZlIGhvb2sgZXhlY3V0aW9uIG9yZGVyLiBDb250ZW50LCB2aWV3LCBhbmQgZGVzdHJveSBob29rcyBmb3IgcHJvamVjdGVkXG4gKiBjb21wb25lbnRzIGFuZCBkaXJlY3RpdmVzIG11c3QgYmUgY2FsbGVkICpiZWZvcmUqIHRoZWlyIGhvc3RzLlxuICpcbiAqIFNldHMgdXAgdGhlIGNvbnRlbnQsIHZpZXcsIGFuZCBkZXN0cm95IGhvb2tzIG9uIHRoZSBwcm92aWRlZCBgdFZpZXdgLFxuICogc2VlIHtAbGluayBIb29rRGF0YX0gZm9yIGRldGFpbHMgYWJvdXQgdGhlIGRhdGEgc3RydWN0dXJlLlxuICpcbiAqIE5PVEU6IFRoaXMgZG9lcyBub3Qgc2V0IHVwIGBvbkNoYW5nZXNgLCBgb25Jbml0YCBvciBgZG9DaGVja2AsIHRob3NlIGFyZSBzZXQgdXBcbiAqIHNlcGFyYXRlbHkgYXQgYGVsZW1lbnRTdGFydGAuXG4gKlxuICogQHBhcmFtIHRWaWV3IFRoZSBjdXJyZW50IFRWaWV3XG4gKiBAcGFyYW0gdE5vZGUgVGhlIFROb2RlIHdob3NlIGRpcmVjdGl2ZXMgYXJlIHRvIGJlIHNlYXJjaGVkIGZvciBob29rcyB0byBxdWV1ZVxuICovXG5leHBvcnQgZnVuY3Rpb24gcmVnaXN0ZXJQb3N0T3JkZXJIb29rcyh0VmlldzogVFZpZXcsIHROb2RlOiBUTm9kZSk6IHZvaWQge1xuICBpZiAodFZpZXcuZmlyc3RUZW1wbGF0ZVBhc3MpIHtcbiAgICAvLyBJdCdzIG5lY2Vzc2FyeSB0byBsb29wIHRocm91Z2ggdGhlIGRpcmVjdGl2ZXMgYXQgZWxlbWVudEVuZCgpIChyYXRoZXIgdGhhbiBwcm9jZXNzaW5nIGluXG4gICAgLy8gZGlyZWN0aXZlQ3JlYXRlKSBzbyB3ZSBjYW4gcHJlc2VydmUgdGhlIGN1cnJlbnQgaG9vayBvcmRlci4gQ29udGVudCwgdmlldywgYW5kIGRlc3Ryb3lcbiAgICAvLyBob29rcyBmb3IgcHJvamVjdGVkIGNvbXBvbmVudHMgYW5kIGRpcmVjdGl2ZXMgbXVzdCBiZSBjYWxsZWQgKmJlZm9yZSogdGhlaXIgaG9zdHMuXG4gICAgZm9yIChsZXQgaSA9IHROb2RlLmRpcmVjdGl2ZVN0YXJ0LCBlbmQgPSB0Tm9kZS5kaXJlY3RpdmVFbmQ7IGkgPCBlbmQ7IGkrKykge1xuICAgICAgY29uc3QgZGlyZWN0aXZlRGVmID0gdFZpZXcuZGF0YVtpXSBhcyBEaXJlY3RpdmVEZWY8YW55PjtcbiAgICAgIGlmIChkaXJlY3RpdmVEZWYuYWZ0ZXJDb250ZW50SW5pdCkge1xuICAgICAgICAodFZpZXcuY29udGVudEhvb2tzIHx8ICh0Vmlldy5jb250ZW50SG9va3MgPSBbXSkpLnB1c2goLWksIGRpcmVjdGl2ZURlZi5hZnRlckNvbnRlbnRJbml0KTtcbiAgICAgIH1cblxuICAgICAgaWYgKGRpcmVjdGl2ZURlZi5hZnRlckNvbnRlbnRDaGVja2VkKSB7XG4gICAgICAgICh0Vmlldy5jb250ZW50SG9va3MgfHwgKHRWaWV3LmNvbnRlbnRIb29rcyA9IFtdKSkucHVzaChpLCBkaXJlY3RpdmVEZWYuYWZ0ZXJDb250ZW50Q2hlY2tlZCk7XG4gICAgICAgICh0Vmlldy5jb250ZW50Q2hlY2tIb29rcyB8fCAodFZpZXcuY29udGVudENoZWNrSG9va3MgPSBbXG4gICAgICAgICBdKSkucHVzaChpLCBkaXJlY3RpdmVEZWYuYWZ0ZXJDb250ZW50Q2hlY2tlZCk7XG4gICAgICB9XG5cbiAgICAgIGlmIChkaXJlY3RpdmVEZWYuYWZ0ZXJWaWV3SW5pdCkge1xuICAgICAgICAodFZpZXcudmlld0hvb2tzIHx8ICh0Vmlldy52aWV3SG9va3MgPSBbXSkpLnB1c2goLWksIGRpcmVjdGl2ZURlZi5hZnRlclZpZXdJbml0KTtcbiAgICAgIH1cblxuICAgICAgaWYgKGRpcmVjdGl2ZURlZi5hZnRlclZpZXdDaGVja2VkKSB7XG4gICAgICAgICh0Vmlldy52aWV3SG9va3MgfHwgKHRWaWV3LnZpZXdIb29rcyA9IFtdKSkucHVzaChpLCBkaXJlY3RpdmVEZWYuYWZ0ZXJWaWV3Q2hlY2tlZCk7XG4gICAgICAgICh0Vmlldy52aWV3Q2hlY2tIb29rcyB8fCAodFZpZXcudmlld0NoZWNrSG9va3MgPSBbXG4gICAgICAgICBdKSkucHVzaChpLCBkaXJlY3RpdmVEZWYuYWZ0ZXJWaWV3Q2hlY2tlZCk7XG4gICAgICB9XG5cbiAgICAgIGlmIChkaXJlY3RpdmVEZWYub25EZXN0cm95ICE9IG51bGwpIHtcbiAgICAgICAgKHRWaWV3LmRlc3Ryb3lIb29rcyB8fCAodFZpZXcuZGVzdHJveUhvb2tzID0gW10pKS5wdXNoKGksIGRpcmVjdGl2ZURlZi5vbkRlc3Ryb3kpO1xuICAgICAgfVxuICAgIH1cbiAgfVxufVxuXG4vKipcbiAqIEV4ZWN1dGluZyBob29rcyByZXF1aXJlcyBjb21wbGV4IGxvZ2ljIGFzIHdlIG5lZWQgdG8gZGVhbCB3aXRoIDIgY29uc3RyYWludHMuXG4gKlxuICogMS4gSW5pdCBob29rcyAobmdPbkluaXQsIG5nQWZ0ZXJDb250ZW50SW5pdCwgbmdBZnRlclZpZXdJbml0KSBtdXN0IGFsbCBiZSBleGVjdXRlZCBvbmNlIGFuZCBvbmx5XG4gKiBvbmNlLCBhY3Jvc3MgbWFueSBjaGFuZ2UgZGV0ZWN0aW9uIGN5Y2xlcy4gVGhpcyBtdXN0IGJlIHRydWUgZXZlbiBpZiBzb21lIGhvb2tzIHRocm93LCBvciBpZlxuICogc29tZSByZWN1cnNpdmVseSB0cmlnZ2VyIGEgY2hhbmdlIGRldGVjdGlvbiBjeWNsZS5cbiAqIFRvIHNvbHZlIHRoYXQsIGl0IGlzIHJlcXVpcmVkIHRvIHRyYWNrIHRoZSBzdGF0ZSBvZiB0aGUgZXhlY3V0aW9uIG9mIHRoZXNlIGluaXQgaG9va3MuXG4gKiBUaGlzIGlzIGRvbmUgYnkgc3RvcmluZyBhbmQgbWFpbnRhaW5pbmcgZmxhZ3MgaW4gdGhlIHZpZXc6IHRoZSB7QGxpbmsgSW5pdFBoYXNlU3RhdGV9LFxuICogYW5kIHRoZSBpbmRleCB3aXRoaW4gdGhhdCBwaGFzZS4gVGhleSBjYW4gYmUgc2VlbiBhcyBhIGN1cnNvciBpbiB0aGUgZm9sbG93aW5nIHN0cnVjdHVyZTpcbiAqIFtbb25Jbml0MSwgb25Jbml0Ml0sIFthZnRlckNvbnRlbnRJbml0MV0sIFthZnRlclZpZXdJbml0MSwgYWZ0ZXJWaWV3SW5pdDIsIGFmdGVyVmlld0luaXQzXV1cbiAqIFRoZXkgYXJlIGFyZSBzdG9yZWQgYXMgZmxhZ3MgaW4gTFZpZXdbRkxBR1NdLlxuICpcbiAqIDIuIFByZS1vcmRlciBob29rcyBjYW4gYmUgZXhlY3V0ZWQgaW4gYmF0Y2hlcywgYmVjYXVzZSBvZiB0aGUgc2VsZWN0IGluc3RydWN0aW9uLlxuICogVG8gYmUgYWJsZSB0byBwYXVzZSBhbmQgcmVzdW1lIHRoZWlyIGV4ZWN1dGlvbiwgd2UgYWxzbyBuZWVkIHNvbWUgc3RhdGUgYWJvdXQgdGhlIGhvb2sncyBhcnJheVxuICogdGhhdCBpcyBiZWluZyBwcm9jZXNzZWQ6XG4gKiAtIHRoZSBpbmRleCBvZiB0aGUgbmV4dCBob29rIHRvIGJlIGV4ZWN1dGVkXG4gKiAtIHRoZSBudW1iZXIgb2YgaW5pdCBob29rcyBhbHJlYWR5IGZvdW5kIGluIHRoZSBwcm9jZXNzZWQgcGFydCBvZiB0aGUgIGFycmF5XG4gKiBUaGV5IGFyZSBhcmUgc3RvcmVkIGFzIGZsYWdzIGluIExWaWV3W1BSRU9SREVSX0hPT0tfRkxBR1NdLlxuICovXG5cbi8qKlxuICogRXhlY3V0ZXMgbmVjZXNzYXJ5IGhvb2tzIGF0IHRoZSBzdGFydCBvZiBleGVjdXRpbmcgYSB0ZW1wbGF0ZS5cbiAqXG4gKiBFeGVjdXRlcyBob29rcyB0aGF0IGFyZSB0byBiZSBydW4gZHVyaW5nIHRoZSBpbml0aWFsaXphdGlvbiBvZiBhIGRpcmVjdGl2ZSBzdWNoXG4gKiBhcyBgb25DaGFuZ2VzYCwgYG9uSW5pdGAsIGFuZCBgZG9DaGVja2AuXG4gKlxuICogQHBhcmFtIGxWaWV3IFRoZSBjdXJyZW50IHZpZXdcbiAqIEBwYXJhbSB0VmlldyBTdGF0aWMgZGF0YSBmb3IgdGhlIHZpZXcgY29udGFpbmluZyB0aGUgaG9va3MgdG8gYmUgZXhlY3V0ZWRcbiAqIEBwYXJhbSBjaGVja05vQ2hhbmdlc01vZGUgV2hldGhlciBvciBub3Qgd2UncmUgaW4gY2hlY2tOb0NoYW5nZXMgbW9kZS5cbiAqIEBwYXJhbSBAcGFyYW0gY3VycmVudE5vZGVJbmRleCAyIGNhc2VzIGRlcGVuZGluZyB0aGUgdGhlIHZhbHVlOlxuICogLSB1bmRlZmluZWQ6IGV4ZWN1dGUgaG9va3Mgb25seSBmcm9tIHRoZSBzYXZlZCBpbmRleCB1bnRpbCB0aGUgZW5kIG9mIHRoZSBhcnJheSAocHJlLW9yZGVyIGNhc2UsXG4gKiB3aGVuIGZsdXNoaW5nIHRoZSByZW1haW5pbmcgaG9va3MpXG4gKiAtIG51bWJlcjogZXhlY3V0ZSBob29rcyBvbmx5IGZyb20gdGhlIHNhdmVkIGluZGV4IHVudGlsIHRoYXQgbm9kZSBpbmRleCBleGNsdXNpdmUgKHByZS1vcmRlclxuICogY2FzZSwgd2hlbiBleGVjdXRpbmcgc2VsZWN0KG51bWJlcikpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBleGVjdXRlUHJlT3JkZXJIb29rcyhcbiAgICBjdXJyZW50VmlldzogTFZpZXcsIHRWaWV3OiBUVmlldywgY2hlY2tOb0NoYW5nZXNNb2RlOiBib29sZWFuLFxuICAgIGN1cnJlbnROb2RlSW5kZXg6IG51bWJlciB8IHVuZGVmaW5lZCk6IHZvaWQge1xuICBpZiAoIWNoZWNrTm9DaGFuZ2VzTW9kZSkge1xuICAgIGV4ZWN1dGVIb29rcyhcbiAgICAgICAgY3VycmVudFZpZXcsIHRWaWV3LnByZU9yZGVySG9va3MsIHRWaWV3LnByZU9yZGVyQ2hlY2tIb29rcywgY2hlY2tOb0NoYW5nZXNNb2RlLFxuICAgICAgICBJbml0UGhhc2VTdGF0ZS5PbkluaXRIb29rc1RvQmVSdW4sXG4gICAgICAgIGN1cnJlbnROb2RlSW5kZXggIT09IHVuZGVmaW5lZCA/IGN1cnJlbnROb2RlSW5kZXggOiBudWxsKTtcbiAgfVxufVxuXG4vKipcbiAqIEV4ZWN1dGVzIGhvb2tzIGFnYWluc3QgdGhlIGdpdmVuIGBMVmlld2AgYmFzZWQgb2ZmIG9mIHdoZXRoZXIgb3Igbm90XG4gKiBUaGlzIGlzIHRoZSBmaXJzdCBwYXNzLlxuICpcbiAqIEBwYXJhbSBjdXJyZW50VmlldyBUaGUgdmlldyBpbnN0YW5jZSBkYXRhIHRvIHJ1biB0aGUgaG9va3MgYWdhaW5zdFxuICogQHBhcmFtIGZpcnN0UGFzc0hvb2tzIEFuIGFycmF5IG9mIGhvb2tzIHRvIHJ1biBpZiB3ZSdyZSBpbiB0aGUgZmlyc3QgdmlldyBwYXNzXG4gKiBAcGFyYW0gY2hlY2tIb29rcyBBbiBBcnJheSBvZiBob29rcyB0byBydW4gaWYgd2UncmUgbm90IGluIHRoZSBmaXJzdCB2aWV3IHBhc3MuXG4gKiBAcGFyYW0gY2hlY2tOb0NoYW5nZXNNb2RlIFdoZXRoZXIgb3Igbm90IHdlJ3JlIGluIG5vIGNoYW5nZXMgbW9kZS5cbiAqIEBwYXJhbSBpbml0UGhhc2VTdGF0ZSB0aGUgY3VycmVudCBzdGF0ZSBvZiB0aGUgaW5pdCBwaGFzZVxuICogQHBhcmFtIGN1cnJlbnROb2RlSW5kZXggMyBjYXNlcyBkZXBlbmRpbmcgdGhlIHRoZSB2YWx1ZTpcbiAqIC0gdW5kZWZpbmVkOiBhbGwgaG9va3MgZnJvbSB0aGUgYXJyYXkgc2hvdWxkIGJlIGV4ZWN1dGVkIChwb3N0LW9yZGVyIGNhc2UpXG4gKiAtIG51bGw6IGV4ZWN1dGUgaG9va3Mgb25seSBmcm9tIHRoZSBzYXZlZCBpbmRleCB1bnRpbCB0aGUgZW5kIG9mIHRoZSBhcnJheSAocHJlLW9yZGVyIGNhc2UsIHdoZW5cbiAqIGZsdXNoaW5nIHRoZSByZW1haW5pbmcgaG9va3MpXG4gKiAtIG51bWJlcjogZXhlY3V0ZSBob29rcyBvbmx5IGZyb20gdGhlIHNhdmVkIGluZGV4IHVudGlsIHRoYXQgbm9kZSBpbmRleCBleGNsdXNpdmUgKHByZS1vcmRlclxuICogY2FzZSwgd2hlbiBleGVjdXRpbmcgc2VsZWN0KG51bWJlcikpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBleGVjdXRlSG9va3MoXG4gICAgY3VycmVudFZpZXc6IExWaWV3LCBmaXJzdFBhc3NIb29rczogSG9va0RhdGEgfCBudWxsLCBjaGVja0hvb2tzOiBIb29rRGF0YSB8IG51bGwsXG4gICAgY2hlY2tOb0NoYW5nZXNNb2RlOiBib29sZWFuLCBpbml0UGhhc2VTdGF0ZTogSW5pdFBoYXNlU3RhdGUsXG4gICAgY3VycmVudE5vZGVJbmRleDogbnVtYmVyIHwgbnVsbCB8IHVuZGVmaW5lZCk6IHZvaWQge1xuICBpZiAoY2hlY2tOb0NoYW5nZXNNb2RlKSByZXR1cm47XG4gIGNvbnN0IGhvb2tzVG9DYWxsID0gKGN1cnJlbnRWaWV3W0ZMQUdTXSAmIExWaWV3RmxhZ3MuSW5pdFBoYXNlU3RhdGVNYXNrKSA9PT0gaW5pdFBoYXNlU3RhdGUgP1xuICAgICAgZmlyc3RQYXNzSG9va3MgOlxuICAgICAgY2hlY2tIb29rcztcbiAgaWYgKGhvb2tzVG9DYWxsKSB7XG4gICAgY2FsbEhvb2tzKGN1cnJlbnRWaWV3LCBob29rc1RvQ2FsbCwgaW5pdFBoYXNlU3RhdGUsIGN1cnJlbnROb2RlSW5kZXgpO1xuICB9XG4gIC8vIFRoZSBpbml0IHBoYXNlIHN0YXRlIG11c3QgYmUgYWx3YXlzIGNoZWNrZWQgaGVyZSBhcyBpdCBtYXkgaGF2ZSBiZWVuIHJlY3Vyc2l2ZWx5IHVwZGF0ZWRcbiAgaWYgKGN1cnJlbnROb2RlSW5kZXggPT0gbnVsbCAmJlxuICAgICAgKGN1cnJlbnRWaWV3W0ZMQUdTXSAmIExWaWV3RmxhZ3MuSW5pdFBoYXNlU3RhdGVNYXNrKSA9PT0gaW5pdFBoYXNlU3RhdGUgJiZcbiAgICAgIGluaXRQaGFzZVN0YXRlICE9PSBJbml0UGhhc2VTdGF0ZS5Jbml0UGhhc2VDb21wbGV0ZWQpIHtcbiAgICBjdXJyZW50Vmlld1tGTEFHU10gJj0gTFZpZXdGbGFncy5JbmRleFdpdGhpbkluaXRQaGFzZVJlc2V0O1xuICAgIGN1cnJlbnRWaWV3W0ZMQUdTXSArPSBMVmlld0ZsYWdzLkluaXRQaGFzZVN0YXRlSW5jcmVtZW50ZXI7XG4gIH1cbn1cblxuLyoqXG4gKiBDYWxscyBsaWZlY3ljbGUgaG9va3Mgd2l0aCB0aGVpciBjb250ZXh0cywgc2tpcHBpbmcgaW5pdCBob29rcyBpZiBpdCdzIG5vdFxuICogdGhlIGZpcnN0IExWaWV3IHBhc3NcbiAqXG4gKiBAcGFyYW0gY3VycmVudFZpZXcgVGhlIGN1cnJlbnQgdmlld1xuICogQHBhcmFtIGFyciBUaGUgYXJyYXkgaW4gd2hpY2ggdGhlIGhvb2tzIGFyZSBmb3VuZFxuICogQHBhcmFtIGluaXRQaGFzZVN0YXRlIHRoZSBjdXJyZW50IHN0YXRlIG9mIHRoZSBpbml0IHBoYXNlXG4gKiBAcGFyYW0gY3VycmVudE5vZGVJbmRleCAzIGNhc2VzIGRlcGVuZGluZyB0aGUgdGhlIHZhbHVlOlxuICogLSB1bmRlZmluZWQ6IGFsbCBob29rcyBmcm9tIHRoZSBhcnJheSBzaG91bGQgYmUgZXhlY3V0ZWQgKHBvc3Qtb3JkZXIgY2FzZSlcbiAqIC0gbnVsbDogZXhlY3V0ZSBob29rcyBvbmx5IGZyb20gdGhlIHNhdmVkIGluZGV4IHVudGlsIHRoZSBlbmQgb2YgdGhlIGFycmF5IChwcmUtb3JkZXIgY2FzZSwgd2hlblxuICogZmx1c2hpbmcgdGhlIHJlbWFpbmluZyBob29rcylcbiAqIC0gbnVtYmVyOiBleGVjdXRlIGhvb2tzIG9ubHkgZnJvbSB0aGUgc2F2ZWQgaW5kZXggdW50aWwgdGhhdCBub2RlIGluZGV4IGV4Y2x1c2l2ZSAocHJlLW9yZGVyXG4gKiBjYXNlLCB3aGVuIGV4ZWN1dGluZyBzZWxlY3QobnVtYmVyKSlcbiAqL1xuZnVuY3Rpb24gY2FsbEhvb2tzKFxuICAgIGN1cnJlbnRWaWV3OiBMVmlldywgYXJyOiBIb29rRGF0YSwgaW5pdFBoYXNlOiBJbml0UGhhc2VTdGF0ZSxcbiAgICBjdXJyZW50Tm9kZUluZGV4OiBudW1iZXIgfCBudWxsIHwgdW5kZWZpbmVkKTogdm9pZCB7XG4gIGNvbnN0IHN0YXJ0SW5kZXggPSBjdXJyZW50Tm9kZUluZGV4ICE9PSB1bmRlZmluZWQgP1xuICAgICAgKGN1cnJlbnRWaWV3W1BSRU9SREVSX0hPT0tfRkxBR1NdICYgUHJlT3JkZXJIb29rRmxhZ3MuSW5kZXhPZlRoZU5leHRQcmVPcmRlckhvb2tNYXNrTWFzaykgOlxuICAgICAgMDtcbiAgY29uc3Qgbm9kZUluZGV4TGltaXQgPSBjdXJyZW50Tm9kZUluZGV4ICE9IG51bGwgPyBjdXJyZW50Tm9kZUluZGV4IDogLTE7XG4gIGxldCBsYXN0Tm9kZUluZGV4Rm91bmQgPSAwO1xuICBmb3IgKGxldCBpID0gc3RhcnRJbmRleDsgaSA8IGFyci5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IGhvb2sgPSBhcnJbaSArIDFdIGFzKCkgPT4gdm9pZDtcbiAgICBpZiAodHlwZW9mIGhvb2sgPT09ICdudW1iZXInKSB7XG4gICAgICBsYXN0Tm9kZUluZGV4Rm91bmQgPSBhcnJbaV0gYXMgbnVtYmVyO1xuICAgICAgaWYgKGN1cnJlbnROb2RlSW5kZXggIT0gbnVsbCAmJiBsYXN0Tm9kZUluZGV4Rm91bmQgPj0gY3VycmVudE5vZGVJbmRleCkge1xuICAgICAgICBicmVhaztcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgY29uc3QgaXNJbml0SG9vayA9IGFycltpXSA8IDA7XG4gICAgICBpZiAoaXNJbml0SG9vaylcbiAgICAgICAgY3VycmVudFZpZXdbUFJFT1JERVJfSE9PS19GTEFHU10gKz0gUHJlT3JkZXJIb29rRmxhZ3MuTnVtYmVyT2ZJbml0SG9va3NDYWxsZWRJbmNyZW1lbnRlcjtcbiAgICAgIGlmIChsYXN0Tm9kZUluZGV4Rm91bmQgPCBub2RlSW5kZXhMaW1pdCB8fCBub2RlSW5kZXhMaW1pdCA9PSAtMSkge1xuICAgICAgICBjYWxsSG9vayhjdXJyZW50VmlldywgaW5pdFBoYXNlLCBhcnIsIGkpO1xuICAgICAgICBjdXJyZW50Vmlld1tQUkVPUkRFUl9IT09LX0ZMQUdTXSA9XG4gICAgICAgICAgICAoY3VycmVudFZpZXdbUFJFT1JERVJfSE9PS19GTEFHU10gJiBQcmVPcmRlckhvb2tGbGFncy5OdW1iZXJPZkluaXRIb29rc0NhbGxlZE1hc2spICsgaSArXG4gICAgICAgICAgICAyO1xuICAgICAgfVxuICAgICAgaSsrO1xuICAgIH1cbiAgfVxufVxuXG4vKipcbiAqIEV4ZWN1dGUgb25lIGhvb2sgYWdhaW5zdCB0aGUgY3VycmVudCBgTFZpZXdgLlxuICpcbiAqIEBwYXJhbSBjdXJyZW50VmlldyBUaGUgY3VycmVudCB2aWV3XG4gKiBAcGFyYW0gaW5pdFBoYXNlU3RhdGUgdGhlIGN1cnJlbnQgc3RhdGUgb2YgdGhlIGluaXQgcGhhc2VcbiAqIEBwYXJhbSBhcnIgVGhlIGFycmF5IGluIHdoaWNoIHRoZSBob29rcyBhcmUgZm91bmRcbiAqIEBwYXJhbSBpIFRoZSBjdXJyZW50IGluZGV4IHdpdGhpbiB0aGUgaG9vayBkYXRhIGFycmF5XG4gKi9cbmZ1bmN0aW9uIGNhbGxIb29rKGN1cnJlbnRWaWV3OiBMVmlldywgaW5pdFBoYXNlOiBJbml0UGhhc2VTdGF0ZSwgYXJyOiBIb29rRGF0YSwgaTogbnVtYmVyKSB7XG4gIGNvbnN0IGlzSW5pdEhvb2sgPSBhcnJbaV0gPCAwO1xuICBjb25zdCBob29rID0gYXJyW2kgKyAxXSBhcygpID0+IHZvaWQ7XG4gIGNvbnN0IGRpcmVjdGl2ZUluZGV4ID0gaXNJbml0SG9vayA/IC1hcnJbaV0gOiBhcnJbaV0gYXMgbnVtYmVyO1xuICBjb25zdCBkaXJlY3RpdmUgPSBjdXJyZW50Vmlld1tkaXJlY3RpdmVJbmRleF07XG4gIGlmIChpc0luaXRIb29rKSB7XG4gICAgY29uc3QgaW5kZXhXaXRoaW50SW5pdFBoYXNlID0gY3VycmVudFZpZXdbRkxBR1NdID4+IExWaWV3RmxhZ3MuSW5kZXhXaXRoaW5Jbml0UGhhc2VTaGlmdDtcbiAgICAvLyBUaGUgaW5pdCBwaGFzZSBzdGF0ZSBtdXN0IGJlIGFsd2F5cyBjaGVja2VkIGhlcmUgYXMgaXQgbWF5IGhhdmUgYmVlbiByZWN1cnNpdmVseVxuICAgIC8vIHVwZGF0ZWRcbiAgICBpZiAoaW5kZXhXaXRoaW50SW5pdFBoYXNlIDxcbiAgICAgICAgICAgIChjdXJyZW50Vmlld1tQUkVPUkRFUl9IT09LX0ZMQUdTXSA+PiBQcmVPcmRlckhvb2tGbGFncy5OdW1iZXJPZkluaXRIb29rc0NhbGxlZFNoaWZ0KSAmJlxuICAgICAgICAoY3VycmVudFZpZXdbRkxBR1NdICYgTFZpZXdGbGFncy5Jbml0UGhhc2VTdGF0ZU1hc2spID09PSBpbml0UGhhc2UpIHtcbiAgICAgIGN1cnJlbnRWaWV3W0ZMQUdTXSArPSBMVmlld0ZsYWdzLkluZGV4V2l0aGluSW5pdFBoYXNlSW5jcmVtZW50ZXI7XG4gICAgICBob29rLmNhbGwoZGlyZWN0aXZlKTtcbiAgICB9XG4gIH0gZWxzZSB7XG4gICAgaG9vay5jYWxsKGRpcmVjdGl2ZSk7XG4gIH1cbn1cbiJdfQ==
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
 * see {@link HookData} for details about the data structure.
 *
 * @param directiveIndex The index of the directive in LView
 * @param directiveDef The definition containing the hooks to setup in tView
 * @param tView The current TView
 * @param nodeIndex The index of the node to which the directive is attached
 * @param initialPreOrderHooksLength the number of pre-order hooks already registered before the
 * current process, used to know if the node index has to be added to the array. If it is -1,
 * the node index is never added.
 * @param initialPreOrderCheckHooksLength same as previous for pre-order check hooks
 */
export function registerPreOrderHooks(directiveIndex, directiveDef, tView, nodeIndex, initialPreOrderHooksLength, initialPreOrderCheckHooksLength) {
    ngDevMode &&
        assertEqual(tView.firstTemplatePass, true, 'Should only be called on first template pass');
    var onChanges = directiveDef.onChanges, onInit = directiveDef.onInit, doCheck = directiveDef.doCheck;
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
 * see {@link HookData} for details about the data structure.
 *
 * NOTE: This does not set up `onChanges`, `onInit` or `doCheck`, those are set up
 * separately at `elementStart`.
 *
 * @param tView The current TView
 * @param tNode The TNode whose directives are to be searched for hooks to queue
 */
export function registerPostOrderHooks(tView, tNode) {
    if (tView.firstTemplatePass) {
        // It's necessary to loop through the directives at elementEnd() (rather than processing in
        // directiveCreate) so we can preserve the current hook order. Content, view, and destroy
        // hooks for projected components and directives must be called *before* their hosts.
        for (var i = tNode.directiveStart, end = tNode.directiveEnd; i < end; i++) {
            var directiveDef = tView.data[i];
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
 * @param lView The current view
 * @param tView Static data for the view containing the hooks to be executed
 * @param checkNoChangesMode Whether or not we're in checkNoChanges mode.
 * @param @param currentNodeIndex 2 cases depending the the value:
 * - undefined: execute hooks only from the saved index until the end of the array (pre-order case,
 * when flushing the remaining hooks)
 * - number: execute hooks only from the saved index until that node index exclusive (pre-order
 * case, when executing select(number))
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
 * @param currentView The view instance data to run the hooks against
 * @param firstPassHooks An array of hooks to run if we're in the first view pass
 * @param checkHooks An Array of hooks to run if we're not in the first view pass.
 * @param checkNoChangesMode Whether or not we're in no changes mode.
 * @param initPhaseState the current state of the init phase
 * @param currentNodeIndex 3 cases depending the the value:
 * - undefined: all hooks from the array should be executed (post-order case)
 * - null: execute hooks only from the saved index until the end of the array (pre-order case, when
 * flushing the remaining hooks)
 * - number: execute hooks only from the saved index until that node index exclusive (pre-order
 * case, when executing select(number))
 */
export function executeHooks(currentView, firstPassHooks, checkHooks, checkNoChangesMode, initPhaseState, currentNodeIndex) {
    if (checkNoChangesMode)
        return;
    var hooksToCall = (currentView[FLAGS] & 3 /* InitPhaseStateMask */) === initPhaseState ?
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
 * @param currentView The current view
 * @param arr The array in which the hooks are found
 * @param initPhaseState the current state of the init phase
 * @param currentNodeIndex 3 cases depending the the value:
 * - undefined: all hooks from the array should be executed (post-order case)
 * - null: execute hooks only from the saved index until the end of the array (pre-order case, when
 * flushing the remaining hooks)
 * - number: execute hooks only from the saved index until that node index exclusive (pre-order
 * case, when executing select(number))
 */
function callHooks(currentView, arr, initPhase, currentNodeIndex) {
    var startIndex = currentNodeIndex !== undefined ?
        (currentView[PREORDER_HOOK_FLAGS] & 65535 /* IndexOfTheNextPreOrderHookMaskMask */) :
        0;
    var nodeIndexLimit = currentNodeIndex != null ? currentNodeIndex : -1;
    var lastNodeIndexFound = 0;
    for (var i = startIndex; i < arr.length; i++) {
        var hook = arr[i + 1];
        if (typeof hook === 'number') {
            lastNodeIndexFound = arr[i];
            if (currentNodeIndex != null && lastNodeIndexFound >= currentNodeIndex) {
                break;
            }
        }
        else {
            var isInitHook = arr[i] < 0;
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
 * @param currentView The current view
 * @param initPhaseState the current state of the init phase
 * @param arr The array in which the hooks are found
 * @param i The current index within the hook data array
 */
function callHook(currentView, initPhase, arr, i) {
    var isInitHook = arr[i] < 0;
    var hook = arr[i + 1];
    var directiveIndex = isInitHook ? -arr[i] : arr[i];
    var directive = currentView[directiveIndex];
    if (isInitHook) {
        var indexWithintInitPhase = currentView[FLAGS] >> 10 /* IndexWithinInitPhaseShift */;
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaG9va3MuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL2hvb2tzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUVILE9BQU8sRUFBQyxXQUFXLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUkzQyxPQUFPLEVBQUMsS0FBSyxFQUErQyxtQkFBbUIsRUFBMkIsTUFBTSxtQkFBbUIsQ0FBQztBQUlwSTs7Ozs7Ozs7Ozs7Ozs7OztHQWdCRztBQUNILE1BQU0sVUFBVSxxQkFBcUIsQ0FDakMsY0FBc0IsRUFBRSxZQUErQixFQUFFLEtBQVksRUFBRSxTQUFpQixFQUN4RiwwQkFBa0MsRUFBRSwrQkFBdUM7SUFDN0UsU0FBUztRQUNMLFdBQVcsQ0FBQyxLQUFLLENBQUMsaUJBQWlCLEVBQUUsSUFBSSxFQUFFLDhDQUE4QyxDQUFDLENBQUM7SUFFeEYsSUFBQSxrQ0FBUyxFQUFFLDRCQUFNLEVBQUUsOEJBQU8sQ0FBaUI7SUFDbEQsSUFBSSwwQkFBMEIsSUFBSSxDQUFDO1FBQy9CLENBQUMsQ0FBQyxLQUFLLENBQUMsYUFBYSxJQUFJLDBCQUEwQixLQUFLLEtBQUssQ0FBQyxhQUFhLENBQUMsTUFBTSxDQUFDO1FBQ25GLENBQUMsU0FBUyxJQUFJLE1BQU0sSUFBSSxPQUFPLENBQUMsRUFBRTtRQUNwQyxDQUFDLEtBQUssQ0FBQyxhQUFhLElBQUksQ0FBQyxLQUFLLENBQUMsYUFBYSxHQUFHLEVBQUUsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLFNBQVMsQ0FBQyxDQUFDO0tBQ3JFO0lBRUQsSUFBSSwrQkFBK0IsSUFBSSxDQUFDO1FBQ3BDLENBQUMsQ0FBQyxLQUFLLENBQUMsa0JBQWtCO1lBQ3pCLCtCQUErQixLQUFLLEtBQUssQ0FBQyxrQkFBa0IsQ0FBQyxNQUFNLENBQUM7UUFDckUsQ0FBQyxTQUFTLElBQUksT0FBTyxDQUFD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QUNqQixDQUFDLFdBQVcsQ0FBQyxtQkFBbUIsQ0FBQyx5Q0FBa0QsQ0FBQztZQUN4RixDQUFDLFdBQVcsQ0FBQyxLQUFLLENBQUMsNkJBQWdDLENBQUMsS0FBSyxTQUFTLEVBQUU7WUFDdEUsV0FBVyxDQUFDLEtBQUssQ0FBQyw4Q0FBOEMsQ0FBQztZQUNqRSxJQUFJLENBQUMsSUFBSSxDQUFDLFNBQVMsQ0FBQyxDQUFDO1NBQ3RCO0tBQ0Y7U0FBTTtRQUNMLElBQUksQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLENBQUM7S0FDdEI7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge2Fzc2VydEVxdWFsfSBmcm9tICcuLi91dGlsL2Fzc2VydCc7XG5cbmltcG9ydCB7RGlyZWN0aXZlRGVmfSBmcm9tICcuL2ludGVyZmFjZXMvZGVmaW5pdGlvbic7XG5pbXBvcnQge1ROb2RlfSBmcm9tICcuL2ludGVyZmFjZXMvbm9kZSc7XG5pbXBvcnQge0ZMQUdTLCBIb29rRGF0YSwgSW5pdFBoYXNlU3RhdGUsIExWaWV3LCBMVmlld0ZsYWdzLCBQUkVPUkRFUl9IT09LX0ZMQUdTLCBQcmVPcmRlckhvb2tGbGFncywgVFZpZXd9IGZyb20gJy4vaW50ZXJmYWNlcy92aWV3JztcblxuXG5cbi8qKlxuICogQWRkcyBhbGwgZGlyZWN0aXZlIGxpZmVjeWNsZSBob29rcyBmcm9tIHRoZSBnaXZlbiBgRGlyZWN0aXZlRGVmYCB0byB0aGUgZ2l2ZW4gYFRWaWV3YC5cbiAqXG4gKiBNdXN0IGJlIHJ1biAqb25seSogb24gdGhlIGZpcnN0IHRlbXBsYXRlIHBhc3MuXG4gKlxuICogU2V0cyB1cCB0aGUgcHJlLW9yZGVyIGhvb2tzIG9uIHRoZSBwcm92aWRlZCBgdFZpZXdgLFxuICogc2VlIHtAbGluayBIb29rRGF0YX0gZm9yIGRldGFpbHMgYWJvdXQgdGhlIGRhdGEgc3RydWN0dXJlLlxuICpcbiAqIEBwYXJhbSBkaXJlY3RpdmVJbmRleCBUaGUgaW5kZXggb2YgdGhlIGRpcmVjdGl2ZSBpbiBMVmlld1xuICogQHBhcmFtIGRpcmVjdGl2ZURlZiBUaGUgZGVmaW5pdGlvbiBjb250YWluaW5nIHRoZSBob29rcyB0byBzZXR1cCBpbiB0Vmlld1xuICogQHBhcmFtIHRWaWV3IFRoZSBjdXJyZW50IFRWaWV3XG4gKiBAcGFyYW0gbm9kZUluZGV4IFRoZSBpbmRleCBvZiB0aGUgbm9kZSB0byB3aGljaCB0aGUgZGlyZWN0aXZlIGlzIGF0dGFjaGVkXG4gKiBAcGFyYW0gaW5pdGlhbFByZU9yZGVySG9va3NMZW5ndGggdGhlIG51bWJlciBvZiBwcmUtb3JkZXIgaG9va3MgYWxyZWFkeSByZWdpc3RlcmVkIGJlZm9yZSB0aGVcbiAqIGN1cnJlbnQgcHJvY2VzcywgdXNlZCB0byBrbm93IGlmIHRoZSBub2RlIGluZGV4IGhhcyB0byBiZSBhZGRlZCB0byB0aGUgYXJyYXkuIElmIGl0IGlzIC0xLFxuICogdGhlIG5vZGUgaW5kZXggaXMgbmV2ZXIgYWRkZWQuXG4gKiBAcGFyYW0gaW5pdGlhbFByZU9yZGVyQ2hlY2tIb29rc0xlbmd0aCBzYW1lIGFzIHByZXZpb3VzIGZvciBwcmUtb3JkZXIgY2hlY2sgaG9va3NcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHJlZ2lzdGVyUHJlT3JkZXJIb29rcyhcbiAgICBkaXJlY3RpdmVJbmRleDogbnVtYmVyLCBkaXJlY3RpdmVEZWY6IERpcmVjdGl2ZURlZjxhbnk+LCB0VmlldzogVFZpZXcsIG5vZGVJbmRleDogbnVtYmVyLFxuICAgIGluaXRpYWxQcmVPcmRlckhvb2tzTGVuZ3RoOiBudW1iZXIsIGluaXRpYWxQcmVPcmRlckNoZWNrSG9va3NMZW5ndGg6IG51bWJlcik6IHZvaWQge1xuICBuZ0Rldk1vZGUgJiZcbiAgICAgIGFzc2VydEVxdWFsKHRWaWV3LmZpcnN0VGVtcGxhdGVQYXNzLCB0cnVlLCAnU2hvdWxkIG9ubHkgYmUgY2FsbGVkIG9uIGZpcnN0IHRlbXBsYXRlIHBhc3MnKTtcblxuICBjb25zdCB7b25DaGFuZ2VzLCBvbkluaXQsIGRvQ2hlY2t9ID0gZGlyZWN0aXZlRGVmO1xuICBpZiAoaW5pdGlhbFByZU9yZGVySG9va3NMZW5ndGggPj0gMCAmJlxuICAgICAgKCF0Vmlldy5wcmVPcmRlckhvb2tzIHx8IGluaXRpYWxQcmVPcmRlckhvb2tzTGVuZ3RoID09PSB0Vmlldy5wcmVPcmRlckhvb2tzLmxlbmd0aCkgJiZcbiAgICAgIChvbkNoYW5nZXMgfHwgb25Jbml0IHx8IGRvQ2hlY2spKSB7XG4gICAgKHRWaWV3LnByZU9yZGVySG9va3MgfHwgKHRWaWV3LnByZU9yZGVySG9va3MgPSBbXSkpLnB1c2gobm9kZUluZGV4KTtcbiAgfVxuXG4gIGlmIChpbml0aWFsUHJlT3JkZXJDaGVja0hvb2tzTGVuZ3RoID49IDAgJiZcbiAgICAgICghdFZpZXcucHJlT3JkZXJDaGVja0hvb2tzIHx8XG4gICAgICAgaW5pdGlhbFByZU9yZGVyQ2hlY2tIb29rc0xlbmd0aCA9PT0gdFZpZXcucHJlT3JkZXJDaGVja0hvb2tzLmxlbmd0aCkgJiZcbiAgICAgIChvbkNoYW5nZXMgfHwgZG9DaGVjaykpIHtcbiAgICAodFZpZXcucHJlT3JkZXJDaGVja0hvb2tzIHx8ICh0Vmlldy5wcmVPcmRlckNoZWNrSG9va3MgPSBbXSkpLnB1c2gobm9kZUluZGV4KTtcbiAgfVxuXG4gIGlmIChvbkNoYW5nZXMpIHtcbiAgICAodFZpZXcucHJlT3JkZXJIb29rcyB8fCAodFZpZXcucHJlT3JkZXJIb29rcyA9IFtdKSkucHVzaChkaXJlY3RpdmVJbmRleCwgb25DaGFuZ2VzKTtcbiAgICAodFZpZXcucHJlT3JkZXJDaGVja0hvb2tzIHx8ICh0Vmlldy5wcmVPcmRlckNoZWNrSG9va3MgPSBbXSkpLnB1c2goZGlyZWN0aXZlSW5kZXgsIG9uQ2hhbmdlcyk7XG4gIH1cblxuICBpZiAob25Jbml0KSB7XG4gICAgKHRWaWV3LnByZU9yZGVySG9va3MgfHwgKHRWaWV3LnByZU9yZGVySG9va3MgPSBbXSkpLnB1c2goLWRpcmVjdGl2ZUluZGV4LCBvbkluaXQpO1xuICB9XG5cbiAgaWYgKGRvQ2hlY2spIHtcbiAgICAodFZpZXcucHJlT3JkZXJIb29rcyB8fCAodFZpZXcucHJlT3JkZXJIb29rcyA9IFtdKSkucHVzaChkaXJlY3RpdmVJbmRleCwgZG9DaGVjayk7XG4gICAgKHRWaWV3LnByZU9yZGVyQ2hlY2tIb29rcyB8fCAodFZpZXcucHJlT3JkZXJDaGVja0hvb2tzID0gW10pKS5wdXNoKGRpcmVjdGl2ZUluZGV4LCBkb0NoZWNrKTtcbiAgfVxufVxuXG4vKipcbiAqXG4gKiBMb29wcyB0aHJvdWdoIHRoZSBkaXJlY3RpdmVzIG9uIHRoZSBwcm92aWRlZCBgdE5vZGVgIGFuZCBxdWV1ZXMgaG9va3MgdG8gYmVcbiAqIHJ1biB0aGF0IGFyZSBub3QgaW5pdGlhbGl6YXRpb24gaG9va3MuXG4gKlxuICogU2hvdWxkIGJlIGV4ZWN1dGVkIGR1cmluZyBgZWxlbWVudEVuZCgpYCBhbmQgc2ltaWxhciB0b1xuICogcHJlc2VydmUgaG9vayBleGVjdXRpb24gb3JkZXIuIENvbnRlbnQsIHZpZXcsIGFuZCBkZXN0cm95IGhvb2tzIGZvciBwcm9qZWN0ZWRcbiAqIGNvbXBvbmVudHMgYW5kIGRpcmVjdGl2ZXMgbXVzdCBiZSBjYWxsZWQgKmJlZm9yZSogdGhlaXIgaG9zdHMuXG4gKlxuICogU2V0cyB1cCB0aGUgY29udGVudCwgdmlldywgYW5kIGRlc3Ryb3kgaG9va3Mgb24gdGhlIHByb3ZpZGVkIGB0Vmlld2AsXG4gKiBzZWUge0BsaW5rIEhvb2tEYXRhfSBmb3IgZGV0YWlscyBhYm91dCB0aGUgZGF0YSBzdHJ1Y3R1cmUuXG4gKlxuICogTk9URTogVGhpcyBkb2VzIG5vdCBzZXQgdXAgYG9uQ2hhbmdlc2AsIGBvbkluaXRgIG9yIGBkb0NoZWNrYCwgdGhvc2UgYXJlIHNldCB1cFxuICogc2VwYXJhdGVseSBhdCBgZWxlbWVudFN0YXJ0YC5cbiAqXG4gKiBAcGFyYW0gdFZpZXcgVGhlIGN1cnJlbnQgVFZpZXdcbiAqIEBwYXJhbSB0Tm9kZSBUaGUgVE5vZGUgd2hvc2UgZGlyZWN0aXZlcyBhcmUgdG8gYmUgc2VhcmNoZWQgZm9yIGhvb2tzIHRvIHF1ZXVlXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiByZWdpc3RlclBvc3RPcmRlckhvb2tzKHRWaWV3OiBUVmlldywgdE5vZGU6IFROb2RlKTogdm9pZCB7XG4gIGlmICh0Vmlldy5maXJzdFRlbXBsYXRlUGFzcykge1xuICAgIC8vIEl0J3MgbmVjZXNzYXJ5IHRvIGxvb3AgdGhyb3VnaCB0aGUgZGlyZWN0aXZlcyBhdCBlbGVtZW50RW5kKCkgKHJhdGhlciB0aGFuIHByb2Nlc3NpbmcgaW5cbiAgICAvLyBkaXJlY3RpdmVDcmVhdGUpIHNvIHdlIGNhbiBwcmVzZXJ2ZSB0aGUgY3VycmVudCBob29rIG9yZGVyLiBDb250ZW50LCB2aWV3LCBhbmQgZGVzdHJveVxuICAgIC8vIGhvb2tzIGZvciBwcm9qZWN0ZWQgY29tcG9uZW50cyBhbmQgZGlyZWN0aXZlcyBtdXN0IGJlIGNhbGxlZCAqYmVmb3JlKiB0aGVpciBob3N0cy5cbiAgICBmb3IgKGxldCBpID0gdE5vZGUuZGlyZWN0aXZlU3RhcnQsIGVuZCA9IHROb2RlLmRpcmVjdGl2ZUVuZDsgaSA8IGVuZDsgaSsrKSB7XG4gICAgICBjb25zdCBkaXJlY3RpdmVEZWYgPSB0Vmlldy5kYXRhW2ldIGFzIERpcmVjdGl2ZURlZjxhbnk+O1xuICAgICAgaWYgKGRpcmVjdGl2ZURlZi5hZnRlckNvbnRlbnRJbml0KSB7XG4gICAgICAgICh0Vmlldy5jb250ZW50SG9va3MgfHwgKHRWaWV3LmNvbnRlbnRIb29rcyA9IFtdKSkucHVzaCgtaSwgZGlyZWN0aXZlRGVmLmFmdGVyQ29udGVudEluaXQpO1xuICAgICAgfVxuXG4gICAgICBpZiAoZGlyZWN0aXZlRGVmLmFmdGVyQ29udGVudENoZWNrZWQpIHtcbiAgICAgICAgKHRWaWV3LmNvbnRlbnRIb29rcyB8fCAodFZpZXcuY29udGVudEhvb2tzID0gW10pKS5wdXNoKGksIGRpcmVjdGl2ZURlZi5hZnRlckNvbnRlbnRDaGVja2VkKTtcbiAgICAgICAgKHRWaWV3LmNvbnRlbnRDaGVja0hvb2tzIHx8ICh0Vmlldy5jb250ZW50Q2hlY2tIb29rcyA9IFtcbiAgICAgICAgIF0pKS5wdXNoKGksIGRpcmVjdGl2ZURlZi5hZnRlckNvbnRlbnRDaGVja2VkKTtcbiAgICAgIH1cblxuICAgICAgaWYgKGRpcmVjdGl2ZURlZi5hZnRlclZpZXdJbml0KSB7XG4gICAgICAgICh0Vmlldy52aWV3SG9va3MgfHwgKHRWaWV3LnZpZXdIb29rcyA9IFtdKSkucHVzaCgtaSwgZGlyZWN0aXZlRGVmLmFmdGVyVmlld0luaXQpO1xuICAgICAgfVxuXG4gICAgICBpZiAoZGlyZWN0aXZlRGVmLmFmdGVyVmlld0NoZWNrZWQpIHtcbiAgICAgICAgKHRWaWV3LnZpZXdIb29rcyB8fCAodFZpZXcudmlld0hvb2tzID0gW10pKS5wdXNoKGksIGRpcmVjdGl2ZURlZi5hZnRlclZpZXdDaGVja2VkKTtcbiAgICAgICAgKHRWaWV3LnZpZXdDaGVja0hvb2tzIHx8ICh0Vmlldy52aWV3Q2hlY2tIb29rcyA9IFtcbiAgICAgICAgIF0pKS5wdXNoKGksIGRpcmVjdGl2ZURlZi5hZnRlclZpZXdDaGVja2VkKTtcbiAgICAgIH1cblxuICAgICAgaWYgKGRpcmVjdGl2ZURlZi5vbkRlc3Ryb3kgIT0gbnVsbCkge1xuICAgICAgICAodFZpZXcuZGVzdHJveUhvb2tzIHx8ICh0Vmlldy5kZXN0cm95SG9va3MgPSBbXSkpLnB1c2goaSwgZGlyZWN0aXZlRGVmLm9uRGVzdHJveSk7XG4gICAgICB9XG4gICAgfVxuICB9XG59XG5cbi8qKlxuICogRXhlY3V0aW5nIGhvb2tzIHJlcXVpcmVzIGNvbXBsZXggbG9naWMgYXMgd2UgbmVlZCB0byBkZWFsIHdpdGggMiBjb25zdHJhaW50cy5cbiAqXG4gKiAxLiBJbml0IGhvb2tzIChuZ09uSW5pdCwgbmdBZnRlckNvbnRlbnRJbml0LCBuZ0FmdGVyVmlld0luaXQpIG11c3QgYWxsIGJlIGV4ZWN1dGVkIG9uY2UgYW5kIG9ubHlcbiAqIG9uY2UsIGFjcm9zcyBtYW55IGNoYW5nZSBkZXRlY3Rpb24gY3ljbGVzLiBUaGlzIG11c3QgYmUgdHJ1ZSBldmVuIGlmIHNvbWUgaG9va3MgdGhyb3csIG9yIGlmXG4gKiBzb21lIHJlY3Vyc2l2ZWx5IHRyaWdnZXIgYSBjaGFuZ2UgZGV0ZWN0aW9uIGN5Y2xlLlxuICogVG8gc29sdmUgdGhhdCwgaXQgaXMgcmVxdWlyZWQgdG8gdHJhY2sgdGhlIHN0YXRlIG9mIHRoZSBleGVjdXRpb24gb2YgdGhlc2UgaW5pdCBob29rcy5cbiAqIFRoaXMgaXMgZG9uZSBieSBzdG9yaW5nIGFuZCBtYWludGFpbmluZyBmbGFncyBpbiB0aGUgdmlldzogdGhlIHtAbGluayBJbml0UGhhc2VTdGF0ZX0sXG4gKiBhbmQgdGhlIGluZGV4IHdpdGhpbiB0aGF0IHBoYXNlLiBUaGV5IGNhbiBiZSBzZWVuIGFzIGEgY3Vyc29yIGluIHRoZSBmb2xsb3dpbmcgc3RydWN0dXJlOlxuICogW1tvbkluaXQxLCBvbkluaXQyXSwgW2FmdGVyQ29udGVudEluaXQxXSwgW2FmdGVyVmlld0luaXQxLCBhZnRlclZpZXdJbml0MiwgYWZ0ZXJWaWV3SW5pdDNdXVxuICogVGhleSBhcmUgYXJlIHN0b3JlZCBhcyBmbGFncyBpbiBMVmlld1tGTEFHU10uXG4gKlxuICogMi4gUHJlLW9yZGVyIGhvb2tzIGNhbiBiZSBleGVjdXRlZCBpbiBiYXRjaGVzLCBiZWNhdXNlIG9mIHRoZSBzZWxlY3QgaW5zdHJ1Y3Rpb24uXG4gKiBUbyBiZSBhYmxlIHRvIHBhdXNlIGFuZCByZXN1bWUgdGhlaXIgZXhlY3V0aW9uLCB3ZSBhbHNvIG5lZWQgc29tZSBzdGF0ZSBhYm91dCB0aGUgaG9vaydzIGFycmF5XG4gKiB0aGF0IGlzIGJlaW5nIHByb2Nlc3NlZDpcbiAqIC0gdGhlIGluZGV4IG9mIHRoZSBuZXh0IGhvb2sgdG8gYmUgZXhlY3V0ZWRcbiAqIC0gdGhlIG51bWJlciBvZiBpbml0IGhvb2tzIGFscmVhZHkgZm91bmQgaW4gdGhlIHByb2Nlc3NlZCBwYXJ0IG9mIHRoZSAgYXJyYXlcbiAqIFRoZXkgYXJlIGFyZSBzdG9yZWQgYXMgZmxhZ3MgaW4gTFZpZXdbUFJFT1JERVJfSE9PS19GTEFHU10uXG4gKi9cblxuLyoqXG4gKiBFeGVjdXRlcyBuZWNlc3NhcnkgaG9va3MgYXQgdGhlIHN0YXJ0IG9mIGV4ZWN1dGluZyBhIHRlbXBsYXRlLlxuICpcbiAqIEV4ZWN1dGVzIGhvb2tzIHRoYXQgYXJlIHRvIGJlIHJ1biBkdXJpbmcgdGhlIGluaXRpYWxpemF0aW9uIG9mIGEgZGlyZWN0aXZlIHN1Y2hcbiAqIGFzIGBvbkNoYW5nZXNgLCBgb25Jbml0YCwgYW5kIGBkb0NoZWNrYC5cbiAqXG4gKiBAcGFyYW0gbFZpZXcgVGhlIGN1cnJlbnQgdmlld1xuICogQHBhcmFtIHRWaWV3IFN0YXRpYyBkYXRhIGZvciB0aGUgdmlldyBjb250YWluaW5nIHRoZSBob29rcyB0byBiZSBleGVjdXRlZFxuICogQHBhcmFtIGNoZWNrTm9DaGFuZ2VzTW9kZSBXaGV0aGVyIG9yIG5vdCB3ZSdyZSBpbiBjaGVja05vQ2hhbmdlcyBtb2RlLlxuICogQHBhcmFtIEBwYXJhbSBjdXJyZW50Tm9kZUluZGV4IDIgY2FzZXMgZGVwZW5kaW5nIHRoZSB0aGUgdmFsdWU6XG4gKiAtIHVuZGVmaW5lZDogZXhlY3V0ZSBob29rcyBvbmx5IGZyb20gdGhlIHNhdmVkIGluZGV4IHVudGlsIHRoZSBlbmQgb2YgdGhlIGFycmF5IChwcmUtb3JkZXIgY2FzZSxcbiAqIHdoZW4gZmx1c2hpbmcgdGhlIHJlbWFpbmluZyBob29rcylcbiAqIC0gbnVtYmVyOiBleGVjdXRlIGhvb2tzIG9ubHkgZnJvbSB0aGUgc2F2ZWQgaW5kZXggdW50aWwgdGhhdCBub2RlIGluZGV4IGV4Y2x1c2l2ZSAocHJlLW9yZGVyXG4gKiBjYXNlLCB3aGVuIGV4ZWN1dGluZyBzZWxlY3QobnVtYmVyKSlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGV4ZWN1dGVQcmVPcmRlckhvb2tzKFxuICAgIGN1cnJlbnRWaWV3OiBMVmlldywgdFZpZXc6IFRWaWV3LCBjaGVja05vQ2hhbmdlc01vZGU6IGJvb2xlYW4sXG4gICAgY3VycmVudE5vZGVJbmRleDogbnVtYmVyIHwgdW5kZWZpbmVkKTogdm9pZCB7XG4gIGlmICghY2hlY2tOb0NoYW5nZXNNb2RlKSB7XG4gICAgZXhlY3V0ZUhvb2tzKFxuICAgICAgICBjdXJyZW50VmlldywgdFZpZXcucHJlT3JkZXJIb29rcywgdFZpZXcucHJlT3JkZXJDaGVja0hvb2tzLCBjaGVja05vQ2hhbmdlc01vZGUsXG4gICAgICAgIEluaXRQaGFzZVN0YXRlLk9uSW5pdEhvb2tzVG9CZVJ1bixcbiAgICAgICAgY3VycmVudE5vZGVJbmRleCAhPT0gdW5kZWZpbmVkID8gY3VycmVudE5vZGVJbmRleCA6IG51bGwpO1xuICB9XG59XG5cbi8qKlxuICogRXhlY3V0ZXMgaG9va3MgYWdhaW5zdCB0aGUgZ2l2ZW4gYExWaWV3YCBiYXNlZCBvZmYgb2Ygd2hldGhlciBvciBub3RcbiAqIFRoaXMgaXMgdGhlIGZpcnN0IHBhc3MuXG4gKlxuICogQHBhcmFtIGN1cnJlbnRWaWV3IFRoZSB2aWV3IGluc3RhbmNlIGRhdGEgdG8gcnVuIHRoZSBob29rcyBhZ2FpbnN0XG4gKiBAcGFyYW0gZmlyc3RQYXNzSG9va3MgQW4gYXJyYXkgb2YgaG9va3MgdG8gcnVuIGlmIHdlJ3JlIGluIHRoZSBmaXJzdCB2aWV3IHBhc3NcbiAqIEBwYXJhbSBjaGVja0hvb2tzIEFuIEFycmF5IG9mIGhvb2tzIHRvIHJ1biBpZiB3ZSdyZSBub3QgaW4gdGhlIGZpcnN0IHZpZXcgcGFzcy5cbiAqIEBwYXJhbSBjaGVja05vQ2hhbmdlc01vZGUgV2hldGhlciBvciBub3Qgd2UncmUgaW4gbm8gY2hhbmdlcyBtb2RlLlxuICogQHBhcmFtIGluaXRQaGFzZVN0YXRlIHRoZSBjdXJyZW50IHN0YXRlIG9mIHRoZSBpbml0IHBoYXNlXG4gKiBAcGFyYW0gY3VycmVudE5vZGVJbmRleCAzIGNhc2VzIGRlcGVuZGluZyB0aGUgdGhlIHZhbHVlOlxuICogLSB1bmRlZmluZWQ6IGFsbCBob29rcyBmcm9tIHRoZSBhcnJheSBzaG91bGQgYmUgZXhlY3V0ZWQgKHBvc3Qtb3JkZXIgY2FzZSlcbiAqIC0gbnVsbDogZXhlY3V0ZSBob29rcyBvbmx5IGZyb20gdGhlIHNhdmVkIGluZGV4IHVudGlsIHRoZSBlbmQgb2YgdGhlIGFycmF5IChwcmUtb3JkZXIgY2FzZSwgd2hlblxuICogZmx1c2hpbmcgdGhlIHJlbWFpbmluZyBob29rcylcbiAqIC0gbnVtYmVyOiBleGVjdXRlIGhvb2tzIG9ubHkgZnJvbSB0aGUgc2F2ZWQgaW5kZXggdW50aWwgdGhhdCBub2RlIGluZGV4IGV4Y2x1c2l2ZSAocHJlLW9yZGVyXG4gKiBjYXNlLCB3aGVuIGV4ZWN1dGluZyBzZWxlY3QobnVtYmVyKSlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGV4ZWN1dGVIb29rcyhcbiAgICBjdXJyZW50VmlldzogTFZpZXcsIGZpcnN0UGFzc0hvb2tzOiBIb29rRGF0YSB8IG51bGwsIGNoZWNrSG9va3M6IEhvb2tEYXRhIHwgbnVsbCxcbiAgICBjaGVja05vQ2hhbmdlc01vZGU6IGJvb2xlYW4sIGluaXRQaGFzZVN0YXRlOiBJbml0UGhhc2VTdGF0ZSxcbiAgICBjdXJyZW50Tm9kZUluZGV4OiBudW1iZXIgfCBudWxsIHwgdW5kZWZpbmVkKTogdm9pZCB7XG4gIGlmIChjaGVja05vQ2hhbmdlc01vZGUpIHJldHVybjtcbiAgY29uc3QgaG9va3NUb0NhbGwgPSAoY3VycmVudFZpZXdbRkxBR1NdICYgTFZpZXdGbGFncy5Jbml0UGhhc2VTdGF0ZU1hc2spID09PSBpbml0UGhhc2VTdGF0ZSA/XG4gICAgICBmaXJzdFBhc3NIb29rcyA6XG4gICAgICBjaGVja0hvb2tzO1xuICBpZiAoaG9va3NUb0NhbGwpIHtcbiAgICBjYWxsSG9va3MoY3VycmVudFZpZXcsIGhvb2tzVG9DYWxsLCBpbml0UGhhc2VTdGF0ZSwgY3VycmVudE5vZGVJbmRleCk7XG4gIH1cbiAgLy8gVGhlIGluaXQgcGhhc2Ugc3RhdGUgbXVzdCBiZSBhbHdheXMgY2hlY2tlZCBoZXJlIGFzIGl0IG1heSBoYXZlIGJlZW4gcmVjdXJzaXZlbHkgdXBkYXRlZFxuICBpZiAoY3VycmVudE5vZGVJbmRleCA9PSBudWxsICYmXG4gICAgICAoY3VycmVudFZpZXdbRkxBR1NdICYgTFZpZXdGbGFncy5Jbml0UGhhc2VTdGF0ZU1hc2spID09PSBpbml0UGhhc2VTdGF0ZSAmJlxuICAgICAgaW5pdFBoYXNlU3RhdGUgIT09IEluaXRQaGFzZVN0YXRlLkluaXRQaGFzZUNvbXBsZXRlZCkge1xuICAgIGN1cnJlbnRWaWV3W0ZMQUdTXSAmPSBMVmlld0ZsYWdzLkluZGV4V2l0aGluSW5pdFBoYXNlUmVzZXQ7XG4gICAgY3VycmVudFZpZXdbRkxBR1NdICs9IExWaWV3RmxhZ3MuSW5pdFBoYXNlU3RhdGVJbmNyZW1lbnRlcjtcbiAgfVxufVxuXG4vKipcbiAqIENhbGxzIGxpZmVjeWNsZSBob29rcyB3aXRoIHRoZWlyIGNvbnRleHRzLCBza2lwcGluZyBpbml0IGhvb2tzIGlmIGl0J3Mgbm90XG4gKiB0aGUgZmlyc3QgTFZpZXcgcGFzc1xuICpcbiAqIEBwYXJhbSBjdXJyZW50VmlldyBUaGUgY3VycmVudCB2aWV3XG4gKiBAcGFyYW0gYXJyIFRoZSBhcnJheSBpbiB3aGljaCB0aGUgaG9va3MgYXJlIGZvdW5kXG4gKiBAcGFyYW0gaW5pdFBoYXNlU3RhdGUgdGhlIGN1cnJlbnQgc3RhdGUgb2YgdGhlIGluaXQgcGhhc2VcbiAqIEBwYXJhbSBjdXJyZW50Tm9kZUluZGV4IDMgY2FzZXMgZGVwZW5kaW5nIHRoZSB0aGUgdmFsdWU6XG4gKiAtIHVuZGVmaW5lZDogYWxsIGhvb2tzIGZyb20gdGhlIGFycmF5IHNob3VsZCBiZSBleGVjdXRlZCAocG9zdC1vcmRlciBjYXNlKVxuICogLSBudWxsOiBleGVjdXRlIGhvb2tzIG9ubHkgZnJvbSB0aGUgc2F2ZWQgaW5kZXggdW50aWwgdGhlIGVuZCBvZiB0aGUgYXJyYXkgKHByZS1vcmRlciBjYXNlLCB3aGVuXG4gKiBmbHVzaGluZyB0aGUgcmVtYWluaW5nIGhvb2tzKVxuICogLSBudW1iZXI6IGV4ZWN1dGUgaG9va3Mgb25seSBmcm9tIHRoZSBzYXZlZCBpbmRleCB1bnRpbCB0aGF0IG5vZGUgaW5kZXggZXhjbHVzaXZlIChwcmUtb3JkZXJcbiAqIGNhc2UsIHdoZW4gZXhlY3V0aW5nIHNlbGVjdChudW1iZXIpKVxuICovXG5mdW5jdGlvbiBjYWxsSG9va3MoXG4gICAgY3VycmVudFZpZXc6IExWaWV3LCBhcnI6IEhvb2tEYXRhLCBpbml0UGhhc2U6IEluaXRQaGFzZVN0YXRlLFxuICAgIGN1cnJlbnROb2RlSW5kZXg6IG51bWJlciB8IG51bGwgfCB1bmRlZmluZWQpOiB2b2lkIHtcbiAgY29uc3Qgc3RhcnRJbmRleCA9IGN1cnJlbnROb2RlSW5kZXggIT09IHVuZGVmaW5lZCA/XG4gICAgICAoY3VycmVudFZpZXdbUFJFT1JERVJfSE9PS19GTEFHU10gJiBQcmVPcmRlckhvb2tGbGFncy5JbmRleE9mVGhlTmV4dFByZU9yZGVySG9va01hc2tNYXNrKSA6XG4gICAgICAwO1xuICBjb25zdCBub2RlSW5kZXhMaW1pdCA9IGN1cnJlbnROb2RlSW5kZXggIT0gbnVsbCA/IGN1cnJlbnROb2RlSW5kZXggOiAtMTtcbiAgbGV0IGxhc3ROb2RlSW5kZXhGb3VuZCA9IDA7XG4gIGZvciAobGV0IGkgPSBzdGFydEluZGV4OyBpIDwgYXJyLmxlbmd0aDsgaSsrKSB7XG4gICAgY29uc3QgaG9vayA9IGFycltpICsgMV0gYXMoKSA9PiB2b2lkO1xuICAgIGlmICh0eXBlb2YgaG9vayA9PT0gJ251bWJlcicpIHtcbiAgICAgIGxhc3ROb2RlSW5kZXhGb3VuZCA9IGFycltpXSBhcyBudW1iZXI7XG4gICAgICBpZiAoY3VycmVudE5vZGVJbmRleCAhPSBudWxsICYmIGxhc3ROb2RlSW5kZXhGb3VuZCA+PSBjdXJyZW50Tm9kZUluZGV4KSB7XG4gICAgICAgIGJyZWFrO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICBjb25zdCBpc0luaXRIb29rID0gYXJyW2ldIDwgMDtcbiAgICAgIGlmIChpc0luaXRIb29rKVxuICAgICAgICBjdXJyZW50Vmlld1tQUkVPUkRFUl9IT09LX0ZMQUdTXSArPSBQcmVPcmRlckhvb2tGbGFncy5OdW1iZXJPZkluaXRIb29rc0NhbGxlZEluY3JlbWVudGVyO1xuICAgICAgaWYgKGxhc3ROb2RlSW5kZXhGb3VuZCA8IG5vZGVJbmRleExpbWl0IHx8IG5vZGVJbmRleExpbWl0ID09IC0xKSB7XG4gICAgICAgIGNhbGxIb29rKGN1cnJlbnRWaWV3LCBpbml0UGhhc2UsIGFyciwgaSk7XG4gICAgICAgIGN1cnJlbnRWaWV3W1BSRU9SREVSX0hPT0tfRkxBR1NdID1cbiAgICAgICAgICAgIChjdXJyZW50Vmlld1tQUkVPUkRFUl9IT09LX0ZMQUdTXSAmIFByZU9yZGVySG9va0ZsYWdzLk51bWJlck9mSW5pdEhvb2tzQ2FsbGVkTWFzaykgKyBpICtcbiAgICAgICAgICAgIDI7XG4gICAgICB9XG4gICAgICBpKys7XG4gICAgfVxuICB9XG59XG5cbi8qKlxuICogRXhlY3V0ZSBvbmUgaG9vayBhZ2FpbnN0IHRoZSBjdXJyZW50IGBMVmlld2AuXG4gKlxuICogQHBhcmFtIGN1cnJlbnRWaWV3IFRoZSBjdXJyZW50IHZpZXdcbiAqIEBwYXJhbSBpbml0UGhhc2VTdGF0ZSB0aGUgY3VycmVudCBzdGF0ZSBvZiB0aGUgaW5pdCBwaGFzZVxuICogQHBhcmFtIGFyciBUaGUgYXJyYXkgaW4gd2hpY2ggdGhlIGhvb2tzIGFyZSBmb3VuZFxuICogQHBhcmFtIGkgVGhlIGN1cnJlbnQgaW5kZXggd2l0aGluIHRoZSBob29rIGRhdGEgYXJyYXlcbiAqL1xuZnVuY3Rpb24gY2FsbEhvb2soY3VycmVudFZpZXc6IExWaWV3LCBpbml0UGhhc2U6IEluaXRQaGFzZVN0YXRlLCBhcnI6IEhvb2tEYXRhLCBpOiBudW1iZXIpIHtcbiAgY29uc3QgaXNJbml0SG9vayA9IGFycltpXSA8IDA7XG4gIGNvbnN0IGhvb2sgPSBhcnJbaSArIDFdIGFzKCkgPT4gdm9pZDtcbiAgY29uc3QgZGlyZWN0aXZlSW5kZXggPSBpc0luaXRIb29rID8gLWFycltpXSA6IGFycltpXSBhcyBudW1iZXI7XG4gIGNvbnN0IGRpcmVjdGl2ZSA9IGN1cnJlbnRWaWV3W2RpcmVjdGl2ZUluZGV4XTtcbiAgaWYgKGlzSW5pdEhvb2spIHtcbiAgICBjb25zdCBpbmRleFdpdGhpbnRJbml0UGhhc2UgPSBjdXJyZW50Vmlld1tGTEFHU10gPj4gTFZpZXdGbGFncy5JbmRleFdpdGhpbkluaXRQaGFzZVNoaWZ0O1xuICAgIC8vIFRoZSBpbml0IHBoYXNlIHN0YXRlIG11c3QgYmUgYWx3YXlzIGNoZWNrZWQgaGVyZSBhcyBpdCBtYXkgaGF2ZSBiZWVuIHJlY3Vyc2l2ZWx5XG4gICAgLy8gdXBkYXRlZFxuICAgIGlmIChpbmRleFdpdGhpbnRJbml0UGhhc2UgPFxuICAgICAgICAgICAgKGN1cnJlbnRWaWV3W1BSRU9SREVSX0hPT0tfRkxBR1NdID4+IFByZU9yZGVySG9va0ZsYWdzLk51bWJlck9mSW5pdEhvb2tzQ2FsbGVkU2hpZnQpICYmXG4gICAgICAgIChjdXJyZW50Vmlld1tGTEFHU10gJiBMVmlld0ZsYWdzLkluaXRQaGFzZVN0YXRlTWFzaykgPT09IGluaXRQaGFzZSkge1xuICAgICAgY3VycmVudFZpZXdbRkxBR1NdICs9IExWaWV3RmxhZ3MuSW5kZXhXaXRoaW5Jbml0UGhhc2VJbmNyZW1lbnRlcjtcbiAgICAgIGhvb2suY2FsbChkaXJlY3RpdmUpO1xuICAgIH1cbiAgfSBlbHNlIHtcbiAgICBob29rLmNhbGwoZGlyZWN0aXZlKTtcbiAgfVxufVxuIl19
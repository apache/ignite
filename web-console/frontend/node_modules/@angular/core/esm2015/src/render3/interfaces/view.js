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
// Below are constants for LView indices to help us look up LView members
// without having to remember the specific indices.
// Uglify will inline these when minifying so there shouldn't be a cost.
/** @type {?} */
export const HOST = 0;
/** @type {?} */
export const TVIEW = 1;
/** @type {?} */
export const FLAGS = 2;
/** @type {?} */
export const PARENT = 3;
/** @type {?} */
export const NEXT = 4;
/** @type {?} */
export const QUERIES = 5;
/** @type {?} */
export const T_HOST = 6;
/** @type {?} */
export const BINDING_INDEX = 7;
/** @type {?} */
export const CLEANUP = 8;
/** @type {?} */
export const CONTEXT = 9;
/** @type {?} */
export const INJECTOR = 10;
/** @type {?} */
export const RENDERER_FACTORY = 11;
/** @type {?} */
export const RENDERER = 12;
/** @type {?} */
export const SANITIZER = 13;
/** @type {?} */
export const CHILD_HEAD = 14;
/** @type {?} */
export const CHILD_TAIL = 15;
/** @type {?} */
export const DECLARATION_VIEW = 16;
/** @type {?} */
export const DECLARATION_LCONTAINER = 17;
/** @type {?} */
export const PREORDER_HOOK_FLAGS = 18;
/**
 * Size of LView's header. Necessary to adjust for it when setting slots.
 * @type {?}
 */
export const HEADER_OFFSET = 19;
/**
 * @record
 */
export function OpaqueViewState() { }
if (false) {
    /** @type {?} */
    OpaqueViewState.prototype.__brand__;
}
/**
 * `LView` stores all of the information needed to process the instructions as
 * they are invoked from the template. Each embedded view and component view has its
 * own `LView`. When processing a particular view, we set the `viewData` to that
 * `LView`. When that view is done processing, the `viewData` is set back to
 * whatever the original `viewData` was before (the parent `LView`).
 *
 * Keeping separate state for each view facilities view insertion / deletion, so we
 * don't have to edit the data array based on which views are present.
 * @record
 */
export function LView() { }
if (false) {
    /* Skipping unnamed member:
    [HOST]: RElement|null;*/
    /* Skipping unnamed member:
    readonly[TVIEW]: TView;*/
    /* Skipping unnamed member:
    [FLAGS]: LViewFlags;*/
    /* Skipping unnamed member:
    [PARENT]: LView|LContainer|null;*/
    /* Skipping unnamed member:
    [NEXT]: LView|LContainer|null;*/
    /* Skipping unnamed member:
    [QUERIES]: LQueries|null;*/
    /* Skipping unnamed member:
    [T_HOST]: TViewNode|TElementNode|null;*/
    /* Skipping unnamed member:
    [BINDING_INDEX]: number;*/
    /* Skipping unnamed member:
    [CLEANUP]: any[]|null;*/
    /* Skipping unnamed member:
    [CONTEXT]: {}|RootContext|null;*/
    /* Skipping unnamed member:
    readonly[INJECTOR]: Injector|null;*/
    /* Skipping unnamed member:
    [RENDERER_FACTORY]: RendererFactory3;*/
    /* Skipping unnamed member:
    [RENDERER]: Renderer3;*/
    /* Skipping unnamed member:
    [SANITIZER]: Sanitizer|null;*/
    /* Skipping unnamed member:
    [CHILD_HEAD]: LView|LContainer|null;*/
    /* Skipping unnamed member:
    [CHILD_TAIL]: LView|LContainer|null;*/
    /* Skipping unnamed member:
    [DECLARATION_VIEW]: LView|null;*/
    /* Skipping unnamed member:
    [DECLARATION_LCONTAINER]: LContainer|null;*/
    /* Skipping unnamed member:
    [PREORDER_HOOK_FLAGS]: PreOrderHookFlags;*/
}
/** @enum {number} */
const LViewFlags = {
    /** The state of the init phase on the first 2 bits */
    InitPhaseStateIncrementer: 1,
    InitPhaseStateMask: 3,
    /**
     * Whether or not the view is in creationMode.
     *
     * This must be stored in the view rather than using `data` as a marker so that
     * we can properly support embedded views. Otherwise, when exiting a child view
     * back into the parent view, `data` will be defined and `creationMode` will be
     * improperly reported as false.
     */
    CreationMode: 4,
    /**
     * Whether or not this LView instance is on its first processing pass.
     *
     * An LView instance is considered to be on its "first pass" until it
     * has completed one creation mode run and one update mode run. At this
     * time, the flag is turned off.
     */
    FirstLViewPass: 8,
    /** Whether this view has default change detection strategy (checks always) or onPush */
    CheckAlways: 16,
    /**
     * Whether or not manual change detection is turned on for onPush components.
     *
     * This is a special mode that only marks components dirty in two cases:
     * 1) There has been a change to an @Input property
     * 2) `markDirty()` has been called manually by the user
     *
     * Note that in this mode, the firing of events does NOT mark components
     * dirty automatically.
     *
     * Manual mode is turned off by default for backwards compatibility, as events
     * automatically mark OnPush components dirty in View Engine.
     *
     * TODO: Add a public API to ChangeDetectionStrategy to turn this mode on
     */
    ManualOnPush: 32,
    /** Whether or not this view is currently dirty (needing check) */
    Dirty: 64,
    /** Whether or not this view is currently attached to change detection tree. */
    Attached: 128,
    /** Whether or not this view is destroyed. */
    Destroyed: 256,
    /** Whether or not this view is the root view */
    IsRoot: 512,
    /**
     * Index of the current init phase on last 22 bits
     */
    IndexWithinInitPhaseIncrementer: 1024,
    IndexWithinInitPhaseShift: 10,
    IndexWithinInitPhaseReset: 1023,
};
export { LViewFlags };
/** @enum {number} */
const InitPhaseState = {
    OnInitHooksToBeRun: 0,
    AfterContentInitHooksToBeRun: 1,
    AfterViewInitHooksToBeRun: 2,
    InitPhaseCompleted: 3,
};
export { InitPhaseState };
/** @enum {number} */
const PreOrderHookFlags = {
    /** The index of the next pre-order hook to be called in the hooks array, on the first 16
       bits */
    IndexOfTheNextPreOrderHookMaskMask: 65535,
    /**
     * The number of init hooks that have already been called, on the last 16 bits
     */
    NumberOfInitHooksCalledIncrementer: 65536,
    NumberOfInitHooksCalledShift: 16,
    NumberOfInitHooksCalledMask: 4294901760,
};
export { PreOrderHookFlags };
/**
 * Set of instructions used to process host bindings efficiently.
 *
 * See VIEW_DATA.md for more information.
 * @record
 */
export function ExpandoInstructions() { }
/**
 * The static data for an LView (shared between all templates of a
 * given type).
 *
 * Stored on the `ComponentDef.tView`.
 * @record
 */
export function TView() { }
if (false) {
    /**
     * ID for inline views to determine whether a view is the same as the previous view
     * in a certain position. If it's not, we know the new view needs to be inserted
     * and the one that exists needs to be removed (e.g. if/else statements)
     *
     * If this is -1, then this is a component view or a dynamically created view.
     * @type {?}
     */
    TView.prototype.id;
    /**
     * This is a blueprint used to generate LView instances for this TView. Copying this
     * blueprint is faster than creating a new LView from scratch.
     * @type {?}
     */
    TView.prototype.blueprint;
    /**
     * The template function used to refresh the view of dynamically created views
     * and components. Will be null for inline views.
     * @type {?}
     */
    TView.prototype.template;
    /**
     * A function containing query-related instructions.
     * @type {?}
     */
    TView.prototype.viewQuery;
    /**
     * Pointer to the host `TNode` (not part of this TView).
     *
     * If this is a `TViewNode` for an `LViewNode`, this is an embedded view of a container.
     * We need this pointer to be able to efficiently find this node when inserting the view
     * into an anchor.
     *
     * If this is a `TElementNode`, this is the view of a root component. It has exactly one
     * root TNode.
     *
     * If this is null, this is the view of a component that is not at root. We do not store
     * the host TNodes for child component views because they can potentially have several
     * different host TNodes, depending on where the component is being used. These host
     * TNodes cannot be shared (due to different indices, etc).
     * @type {?}
     */
    TView.prototype.node;
    /**
     * Whether or not this template has been processed.
     * @type {?}
     */
    TView.prototype.firstTemplatePass;
    /**
     * Static data equivalent of LView.data[]. Contains TNodes, PipeDefInternal or TI18n.
     * @type {?}
     */
    TView.prototype.data;
    /**
     * The binding start index is the index at which the data array
     * starts to store bindings only. Saving this value ensures that we
     * will begin reading bindings at the correct point in the array when
     * we are in update mode.
     * @type {?}
     */
    TView.prototype.bindingStartIndex;
    /**
     * The index where the "expando" section of `LView` begins. The expando
     * section contains injectors, directive instances, and host binding values.
     * Unlike the "consts" and "vars" sections of `LView`, the length of this
     * section cannot be calculated at compile-time because directives are matched
     * at runtime to preserve locality.
     *
     * We store this start index so we know where to start checking host bindings
     * in `setHostBindings`.
     * @type {?}
     */
    TView.prototype.expandoStartIndex;
    /**
     * Whether or not there are any static view queries tracked on this view.
     *
     * We store this so we know whether or not we should do a view query
     * refresh after creation mode to collect static query results.
     * @type {?}
     */
    TView.prototype.staticViewQueries;
    /**
     * Whether or not there are any static content queries tracked on this view.
     *
     * We store this so we know whether or not we should do a content query
     * refresh after creation mode to collect static query results.
     * @type {?}
     */
    TView.prototype.staticContentQueries;
    /**
     * A reference to the first child node located in the view.
     * @type {?}
     */
    TView.prototype.firstChild;
    /**
     * Set of instructions used to process host bindings efficiently.
     *
     * See VIEW_DATA.md for more information.
     * @type {?}
     */
    TView.prototype.expandoInstructions;
    /**
     * Full registry of directives and components that may be found in this view.
     *
     * It's necessary to keep a copy of the full def list on the TView so it's possible
     * to render template functions without a host component.
     * @type {?}
     */
    TView.prototype.directiveRegistry;
    /**
     * Full registry of pipes that may be found in this view.
     *
     * The property is either an array of `PipeDefs`s or a function which returns the array of
     * `PipeDefs`s. The function is necessary to be able to support forward declarations.
     *
     * It's necessary to keep a copy of the full def list on the TView so it's possible
     * to render template functions without a host component.
     * @type {?}
     */
    TView.prototype.pipeRegistry;
    /**
     * Array of ngOnInit, ngOnChanges and ngDoCheck hooks that should be executed for this view in
     * creation mode.
     *
     * Even indices: Directive index
     * Odd indices: Hook function
     * @type {?}
     */
    TView.prototype.preOrderHooks;
    /**
     * Array of ngOnChanges and ngDoCheck hooks that should be executed for this view in update mode.
     *
     * Even indices: Directive index
     * Odd indices: Hook function
     * @type {?}
     */
    TView.prototype.preOrderCheckHooks;
    /**
     * Array of ngAfterContentInit and ngAfterContentChecked hooks that should be executed
     * for this view in creation mode.
     *
     * Even indices: Directive index
     * Odd indices: Hook function
     * @type {?}
     */
    TView.prototype.contentHooks;
    /**
     * Array of ngAfterContentChecked hooks that should be executed for this view in update
     * mode.
     *
     * Even indices: Directive index
     * Odd indices: Hook function
     * @type {?}
     */
    TView.prototype.contentCheckHooks;
    /**
     * Array of ngAfterViewInit and ngAfterViewChecked hooks that should be executed for
     * this view in creation mode.
     *
     * Even indices: Directive index
     * Odd indices: Hook function
     * @type {?}
     */
    TView.prototype.viewHooks;
    /**
     * Array of ngAfterViewChecked hooks that should be executed for this view in
     * update mode.
     *
     * Even indices: Directive index
     * Odd indices: Hook function
     * @type {?}
     */
    TView.prototype.viewCheckHooks;
    /**
     * Array of ngOnDestroy hooks that should be executed when this view is destroyed.
     *
     * Even indices: Directive index
     * Odd indices: Hook function
     * @type {?}
     */
    TView.prototype.destroyHooks;
    /**
     * When a view is destroyed, listeners need to be released and outputs need to be
     * unsubscribed. This cleanup array stores both listener data (in chunks of 4)
     * and output data (in chunks of 2) for a particular view. Combining the arrays
     * saves on memory (70 bytes per array) and on a few bytes of code size (for two
     * separate for loops).
     *
     * If it's a native DOM listener or output subscription being stored:
     * 1st index is: event name  `name = tView.cleanup[i+0]`
     * 2nd index is: index of native element or a function that retrieves global target (window,
     *               document or body) reference based on the native element:
     *    `typeof idxOrTargetGetter === 'function'`: global target getter function
     *    `typeof idxOrTargetGetter === 'number'`: index of native element
     *
     * 3rd index is: index of listener function `listener = lView[CLEANUP][tView.cleanup[i+2]]`
     * 4th index is: `useCaptureOrIndx = tView.cleanup[i+3]`
     *    `typeof useCaptureOrIndx == 'boolean' : useCapture boolean
     *    `typeof useCaptureOrIndx == 'number':
     *         `useCaptureOrIndx >= 0` `removeListener = LView[CLEANUP][useCaptureOrIndx]`
     *         `useCaptureOrIndx <  0` `subscription = LView[CLEANUP][-useCaptureOrIndx]`
     *
     * If it's an output subscription or query list destroy hook:
     * 1st index is: output unsubscribe function / query list destroy function
     * 2nd index is: index of function context in LView.cleanupInstances[]
     *               `tView.cleanup[i+0].call(lView[CLEANUP][tView.cleanup[i+1]])`
     * @type {?}
     */
    TView.prototype.cleanup;
    /**
     * A list of element indices for child components that will need to be
     * refreshed when the current view has finished its check. These indices have
     * already been adjusted for the HEADER_OFFSET.
     *
     * @type {?}
     */
    TView.prototype.components;
    /**
     * A collection of queries tracked in a given view.
     * @type {?}
     */
    TView.prototype.queries;
    /**
     * An array of indices pointing to directives with content queries alongside with the
     * corresponding
     * query index. Each entry in this array is a tuple of:
     * - index of the first content query index declared by a given directive;
     * - index of a directive.
     *
     * We are storing those indexes so we can refresh content queries as part of a view refresh
     * process.
     * @type {?}
     */
    TView.prototype.contentQueries;
    /**
     * Set of schemas that declare elements to be allowed inside the view.
     * @type {?}
     */
    TView.prototype.schemas;
}
/** @enum {number} */
const RootContextFlags = {
    Empty: 0, DetectChanges: 1, FlushPlayers: 2,
};
export { RootContextFlags };
/**
 * RootContext contains information which is shared for all components which
 * were bootstrapped with {\@link renderComponent}.
 * @record
 */
export function RootContext() { }
if (false) {
    /**
     * A function used for scheduling change detection in the future. Usually
     * this is `requestAnimationFrame`.
     * @type {?}
     */
    RootContext.prototype.scheduler;
    /**
     * A promise which is resolved when all components are considered clean (not dirty).
     *
     * This promise is overwritten every time a first call to {\@link markDirty} is invoked.
     * @type {?}
     */
    RootContext.prototype.clean;
    /**
     * RootComponents - The components that were instantiated by the call to
     * {\@link renderComponent}.
     * @type {?}
     */
    RootContext.prototype.components;
    /**
     * The player flushing handler to kick off all animations
     * @type {?}
     */
    RootContext.prototype.playerHandler;
    /**
     * What render-related operations to run once a scheduler has been set
     * @type {?}
     */
    RootContext.prototype.flags;
}
// Note: This hack is necessary so we don't erroneously get a circular dependency
// failure based on types.
/** @type {?} */
export const unusedValueExportToPlacateAjd = 1;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidmlldy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3JlbmRlcjMvaW50ZXJmYWNlcy92aWV3LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7Ozs7OztBQTJCQSxNQUFNLE9BQU8sSUFBSSxHQUFHLENBQUM7O0FBQ3JCLE1BQU0sT0FBTyxLQUFLLEdBQUcsQ0FBQzs7QUFDdEIsTUFBTSxPQUFPLEtBQUssR0FBRyxDQUFDOztBQUN0QixNQUFNLE9BQU8sTUFBTSxHQUFHLENBQUM7O0FBQ3ZCLE1BQU0sT0FBTyxJQUFJLEdBQUcsQ0FBQzs7QUFDckIsTUFBTSxPQUFPLE9BQU8sR0FBRyxDQUFDOztBQUN4QixNQUFNLE9BQU8sTUFBTSxHQUFHLENBQUM7O0FBQ3ZCLE1BQU0sT0FBTyxhQUFhLEdBQUcsQ0FBQzs7QUFDOUIsTUFBTSxPQUFPLE9BQU8sR0FBRyxDQUFDOztBQUN4QixNQUFNLE9BQU8sT0FBTyxHQUFHLENBQUM7O0FBQ3hCLE1BQU0sT0FBTyxRQUFRLEdBQUcsRUFBRTs7QUFDMUIsTUFBTSxPQUFPLGdCQUFnQixHQUFHLEVBQUU7O0FBQ2xDLE1BQU0sT0FBTyxRQUFRLEdBQUcsRUFBRTs7QUFDMUIsTUFBTSxPQUFPLFNBQVMsR0FBRyxFQUFFOztBQUMzQixNQUFNLE9BQU8sVUFBVSxHQUFHLEVBQUU7O0FBQzVCLE1BQU0sT0FBTyxVQUFVLEdBQUcsRUFBRTs7QUFDNUIsTUFBTSxPQUFPLGdCQUFnQixHQUFHLEVBQUU7O0FBQ2xDLE1BQU0sT0FBTyxzQkFBc0IsR0FBRyxFQUFFOztBQUN4QyxNQUFNLE9BQU8sbUJBQW1CLEdBQUcsRUFBRTs7Ozs7QUFFckMsTUFBTSxPQUFPLGFBQWEsR0FBRyxFQUFFOzs7O0FBTS9CLHFDQUVDOzs7SUFEQyxvQ0FBaUU7Ozs7Ozs7Ozs7Ozs7QUFjbkUsMkJBd0pDOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0lBSUMsc0RBQXNEO0lBQ3RELDRCQUF5QztJQUN6QyxxQkFBa0M7SUFFbEM7Ozs7Ozs7T0FPRztJQUNILGVBQTRCO0lBRTVCOzs7Ozs7T0FNRztJQUNILGlCQUE4QjtJQUU5Qix3RkFBd0Y7SUFDeEYsZUFBMkI7SUFFM0I7Ozs7Ozs7Ozs7Ozs7O09BY0c7SUFDSCxnQkFBNEI7SUFFNUIsa0VBQWtFO0lBQ2xFLFNBQXNCO0lBRXRCLCtFQUErRTtJQUMvRSxhQUF5QjtJQUV6Qiw2Q0FBNkM7SUFDN0MsY0FBMEI7SUFFMUIsZ0RBQWdEO0lBQ2hELFdBQXVCO0lBRXZCOztPQUVHO0lBQ0gscUNBQWdEO0lBQ2hELDZCQUE4QjtJQUM5QiwrQkFBMEM7Ozs7O0lBVzFDLHFCQUF5QjtJQUN6QiwrQkFBbUM7SUFDbkMsNEJBQWdDO0lBQ2hDLHFCQUF5Qjs7Ozs7SUFLekI7Y0FDVTtJQUNWLHlDQUF3RDtJQUV4RDs7T0FFRztJQUNILHlDQUF5RDtJQUN6RCxnQ0FBaUM7SUFDakMsdUNBQWdFOzs7Ozs7Ozs7QUFRbEUseUNBQTRGOzs7Ozs7OztBQVE1RiwyQkEyT0M7Ozs7Ozs7Ozs7SUFuT0MsbUJBQW9COzs7Ozs7SUFNcEIsMEJBQWlCOzs7Ozs7SUFNakIseUJBQXFDOzs7OztJQUtyQywwQkFBd0M7Ozs7Ozs7Ozs7Ozs7Ozs7O0lBaUJ4QyxxQkFBa0M7Ozs7O0lBR2xDLGtDQUEyQjs7Ozs7SUFHM0IscUJBQVk7Ozs7Ozs7O0lBUVosa0NBQTBCOzs7Ozs7Ozs7Ozs7SUFZMUIsa0NBQTBCOzs7Ozs7OztJQVExQixrQ0FBMkI7Ozs7Ozs7O0lBUTNCLHFDQUE4Qjs7Ozs7SUFLOUIsMkJBQXVCOzs7Ozs7O0lBT3ZCLG9DQUE4Qzs7Ozs7Ozs7SUFROUMsa0NBQXlDOzs7Ozs7Ozs7OztJQVd6Qyw2QkFBK0I7Ozs7Ozs7OztJQVMvQiw4QkFBNkI7Ozs7Ozs7O0lBUTdCLG1DQUFrQzs7Ozs7Ozs7O0lBU2xDLDZCQUE0Qjs7Ozs7Ozs7O0lBUzVCLGtDQUFpQzs7Ozs7Ozs7O0lBU2pDLDBCQUF5Qjs7Ozs7Ozs7O0lBU3pCLCtCQUE4Qjs7Ozs7Ozs7SUFROUIsNkJBQTRCOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0lBNEI1Qix3QkFBb0I7Ozs7Ozs7O0lBUXBCLDJCQUEwQjs7Ozs7SUFLMUIsd0JBQXVCOzs7Ozs7Ozs7Ozs7SUFZdkIsK0JBQThCOzs7OztJQUs5Qix3QkFBK0I7Ozs7SUFHRyxRQUFZLEVBQUUsZ0JBQW9CLEVBQUUsZUFBbUI7Ozs7Ozs7O0FBTzNGLGlDQTZCQzs7Ozs7OztJQXhCQyxnQ0FBd0M7Ozs7Ozs7SUFPeEMsNEJBQXFCOzs7Ozs7SUFNckIsaUNBQWlCOzs7OztJQUtqQixvQ0FBa0M7Ozs7O0lBS2xDLDRCQUF3Qjs7Ozs7QUFrRDFCLE1BQU0sT0FBTyw2QkFBNkIsR0FBRyxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0luamVjdGlvblRva2VufSBmcm9tICcuLi8uLi9kaS9pbmplY3Rpb25fdG9rZW4nO1xuaW1wb3J0IHtJbmplY3Rvcn0gZnJvbSAnLi4vLi4vZGkvaW5qZWN0b3InO1xuaW1wb3J0IHtUeXBlfSBmcm9tICcuLi8uLi9pbnRlcmZhY2UvdHlwZSc7XG5pbXBvcnQge1NjaGVtYU1ldGFkYXRhfSBmcm9tICcuLi8uLi9tZXRhZGF0YSc7XG5pbXBvcnQge1Nhbml0aXplcn0gZnJvbSAnLi4vLi4vc2FuaXRpemF0aW9uL3NlY3VyaXR5JztcblxuaW1wb3J0IHtMQ29udGFpbmVyfSBmcm9tICcuL2NvbnRhaW5lcic7XG5pbXBvcnQge0NvbXBvbmVudERlZiwgQ29tcG9uZW50VGVtcGxhdGUsIERpcmVjdGl2ZURlZiwgRGlyZWN0aXZlRGVmTGlzdCwgSG9zdEJpbmRpbmdzRnVuY3Rpb24sIFBpcGVEZWYsIFBpcGVEZWZMaXN0LCBWaWV3UXVlcmllc0Z1bmN0aW9ufSBmcm9tICcuL2RlZmluaXRpb24nO1xuaW1wb3J0IHtJMThuVXBkYXRlT3BDb2RlcywgVEkxOG59IGZyb20gJy4vaTE4bic7XG5pbXBvcnQge1RFbGVtZW50Tm9kZSwgVE5vZGUsIFRWaWV3Tm9kZX0gZnJvbSAnLi9ub2RlJztcbmltcG9ydCB7UGxheWVySGFuZGxlcn0gZnJvbSAnLi9wbGF5ZXInO1xuaW1wb3J0IHtMUXVlcmllcywgVFF1ZXJpZXN9IGZyb20gJy4vcXVlcnknO1xuaW1wb3J0IHtSRWxlbWVudCwgUmVuZGVyZXIzLCBSZW5kZXJlckZhY3RvcnkzfSBmcm9tICcuL3JlbmRlcmVyJztcblxuXG5cbi8vIEJlbG93IGFyZSBjb25zdGFudHMgZm9yIExWaWV3IGluZGljZXMgdG8gaGVscCB1cyBsb29rIHVwIExWaWV3IG1lbWJlcnNcbi8vIHdpdGhvdXQgaGF2aW5nIHRvIHJlbWVtYmVyIHRoZSBzcGVjaWZpYyBpbmRpY2VzLlxuLy8gVWdsaWZ5IHdpbGwgaW5saW5lIHRoZXNlIHdoZW4gbWluaWZ5aW5nIHNvIHRoZXJlIHNob3VsZG4ndCBiZSBhIGNvc3QuXG5leHBvcnQgY29uc3QgSE9TVCA9IDA7XG5leHBvcnQgY29uc3QgVFZJRVcgPSAxO1xuZXhwb3J0IGNvbnN0IEZMQUdTID0gMjtcbmV4cG9ydCBjb25zdCBQQVJFTlQgPSAzO1xuZXhwb3J0IGNvbnN0IE5FWFQgPSA0O1xuZXhwb3J0IGNvbnN0IFFVRVJJRVMgPSA1O1xuZXhwb3J0IGNvbnN0IFRfSE9TVCA9IDY7XG5leHBvcnQgY29uc3QgQklORElOR19JTkRFWCA9IDc7XG5leHBvcnQgY29uc3QgQ0xFQU5VUCA9IDg7XG5leHBvcnQgY29uc3QgQ09OVEVYVCA9IDk7XG5leHBvcnQgY29uc3QgSU5KRUNUT1IgPSAxMDtcbmV4cG9ydCBjb25zdCBSRU5ERVJFUl9GQUNUT1JZID0gMTE7XG5leHBvcnQgY29uc3QgUkVOREVSRVIgPSAxMjtcbmV4cG9ydCBjb25zdCBTQU5JVElaRVIgPSAxMztcbmV4cG9ydCBjb25zdCBDSElMRF9IRUFEID0gMTQ7XG5leHBvcnQgY29uc3QgQ0hJTERfVEFJTCA9IDE1O1xuZXhwb3J0IGNvbnN0IERFQ0xBUkFUSU9OX1ZJRVcgPSAxNjtcbmV4cG9ydCBjb25zdCBERUNMQVJBVElPTl9MQ09OVEFJTkVSID0gMTc7XG5leHBvcnQgY29uc3QgUFJFT1JERVJfSE9PS19GTEFHUyA9IDE4O1xuLyoqIFNpemUgb2YgTFZpZXcncyBoZWFkZXIuIE5lY2Vzc2FyeSB0byBhZGp1c3QgZm9yIGl0IHdoZW4gc2V0dGluZyBzbG90cy4gICovXG5leHBvcnQgY29uc3QgSEVBREVSX09GRlNFVCA9IDE5O1xuXG5cbi8vIFRoaXMgaW50ZXJmYWNlIHJlcGxhY2VzIHRoZSByZWFsIExWaWV3IGludGVyZmFjZSBpZiBpdCBpcyBhbiBhcmcgb3IgYVxuLy8gcmV0dXJuIHZhbHVlIG9mIGEgcHVibGljIGluc3RydWN0aW9uLiBUaGlzIGVuc3VyZXMgd2UgZG9uJ3QgbmVlZCB0byBleHBvc2Vcbi8vIHRoZSBhY3R1YWwgaW50ZXJmYWNlLCB3aGljaCBzaG91bGQgYmUga2VwdCBwcml2YXRlLlxuZXhwb3J0IGludGVyZmFjZSBPcGFxdWVWaWV3U3RhdGUge1xuICAnX19icmFuZF9fJzogJ0JyYW5kIGZvciBPcGFxdWVWaWV3U3RhdGUgdGhhdCBub3RoaW5nIHdpbGwgbWF0Y2gnO1xufVxuXG5cbi8qKlxuICogYExWaWV3YCBzdG9yZXMgYWxsIG9mIHRoZSBpbmZvcm1hdGlvbiBuZWVkZWQgdG8gcHJvY2VzcyB0aGUgaW5zdHJ1Y3Rpb25zIGFzXG4gKiB0aGV5IGFyZSBpbnZva2VkIGZyb20gdGhlIHRlbXBsYXRlLiBFYWNoIGVtYmVkZGVkIHZpZXcgYW5kIGNvbXBvbmVudCB2aWV3IGhhcyBpdHNcbiAqIG93biBgTFZpZXdgLiBXaGVuIHByb2Nlc3NpbmcgYSBwYXJ0aWN1bGFyIHZpZXcsIHdlIHNldCB0aGUgYHZpZXdEYXRhYCB0byB0aGF0XG4gKiBgTFZpZXdgLiBXaGVuIHRoYXQgdmlldyBpcyBkb25lIHByb2Nlc3NpbmcsIHRoZSBgdmlld0RhdGFgIGlzIHNldCBiYWNrIHRvXG4gKiB3aGF0ZXZlciB0aGUgb3JpZ2luYWwgYHZpZXdEYXRhYCB3YXMgYmVmb3JlICh0aGUgcGFyZW50IGBMVmlld2ApLlxuICpcbiAqIEtlZXBpbmcgc2VwYXJhdGUgc3RhdGUgZm9yIGVhY2ggdmlldyBmYWNpbGl0aWVzIHZpZXcgaW5zZXJ0aW9uIC8gZGVsZXRpb24sIHNvIHdlXG4gKiBkb24ndCBoYXZlIHRvIGVkaXQgdGhlIGRhdGEgYXJyYXkgYmFzZWQgb24gd2hpY2ggdmlld3MgYXJlIHByZXNlbnQuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgTFZpZXcgZXh0ZW5kcyBBcnJheTxhbnk+IHtcbiAgLyoqXG4gICAqIFRoZSBob3N0IG5vZGUgZm9yIHRoaXMgTFZpZXcgaW5zdGFuY2UsIGlmIHRoaXMgaXMgYSBjb21wb25lbnQgdmlldy5cbiAgICogSWYgdGhpcyBpcyBhbiBlbWJlZGRlZCB2aWV3LCBIT1NUIHdpbGwgYmUgbnVsbC5cbiAgICovXG4gIFtIT1NUXTogUkVsZW1lbnR8bnVsbDtcblxuICAvKipcbiAgICogVGhlIHN0YXRpYyBkYXRhIGZvciB0aGlzIHZpZXcuIFdlIG5lZWQgYSByZWZlcmVuY2UgdG8gdGhpcyBzbyB3ZSBjYW4gZWFzaWx5IHdhbGsgdXAgdGhlXG4gICAqIG5vZGUgdHJlZSBpbiBESSBhbmQgZ2V0IHRoZSBUVmlldy5kYXRhIGFycmF5IGFzc29jaWF0ZWQgd2l0aCBhIG5vZGUgKHdoZXJlIHRoZVxuICAgKiBkaXJlY3RpdmUgZGVmcyBhcmUgc3RvcmVkKS5cbiAgICovXG4gIHJlYWRvbmx5W1RWSUVXXTogVFZpZXc7XG5cbiAgLyoqIEZsYWdzIGZvciB0aGlzIHZpZXcuIFNlZSBMVmlld0ZsYWdzIGZvciBtb3JlIGluZm8uICovXG4gIFtGTEFHU106IExWaWV3RmxhZ3M7XG5cbiAgLyoqXG4gICAqIFRoaXMgbWF5IHN0b3JlIGFuIHtAbGluayBMVmlld30gb3Ige0BsaW5rIExDb250YWluZXJ9LlxuICAgKlxuICAgKiBgTFZpZXdgIC0gVGhlIHBhcmVudCB2aWV3LiBUaGlzIGlzIG5lZWRlZCB3aGVuIHdlIGV4aXQgdGhlIHZpZXcgYW5kIG11c3QgcmVzdG9yZSB0aGUgcHJldmlvdXNcbiAgICogTFZpZXcuIFdpdGhvdXQgdGhpcywgdGhlIHJlbmRlciBtZXRob2Qgd291bGQgaGF2ZSB0byBrZWVwIGEgc3RhY2sgb2ZcbiAgICogdmlld3MgYXMgaXQgaXMgcmVjdXJzaXZlbHkgcmVuZGVyaW5nIHRlbXBsYXRlcy5cbiAgICpcbiAgICogYExDb250YWluZXJgIC0gVGhlIGN1cnJlbnQgdmlldyBpcyBwYXJ0IG9mIGEgY29udGFpbmVyLCBhbmQgaXMgYW4gZW1iZWRkZWQgdmlldy5cbiAgICovXG4gIFtQQVJFTlRdOiBMVmlld3xMQ29udGFpbmVyfG51bGw7XG5cbiAgLyoqXG4gICAqXG4gICAqIFRoZSBuZXh0IHNpYmxpbmcgTFZpZXcgb3IgTENvbnRhaW5lci5cbiAgICpcbiAgICogQWxsb3dzIHVzIHRvIHByb3BhZ2F0ZSBiZXR3ZWVuIHNpYmxpbmcgdmlldyBzdGF0ZXMgdGhhdCBhcmVuJ3QgaW4gdGhlIHNhbWVcbiAgICogY29udGFpbmVyLiBFbWJlZGRlZCB2aWV3cyBhbHJlYWR5IGhhdmUgYSBub2RlLm5leHQsIGJ1dCBpdCBpcyBvbmx5IHNldCBmb3JcbiAgICogdmlld3MgaW4gdGhlIHNhbWUgY29udGFpbmVyLiBXZSBuZWVkIGEgd2F5IHRvIGxpbmsgY29tcG9uZW50IHZpZXdzIGFuZCB2aWV3c1xuICAgKiBhY3Jvc3MgY29udGFpbmVycyBhcyB3ZWxsLlxuICAgKi9cbiAgW05FWFRdOiBMVmlld3xMQ29udGFpbmVyfG51bGw7XG5cbiAgLyoqIFF1ZXJpZXMgYWN0aXZlIGZvciB0aGlzIHZpZXcgLSBub2RlcyBmcm9tIGEgdmlldyBhcmUgcmVwb3J0ZWQgdG8gdGhvc2UgcXVlcmllcy4gKi9cbiAgW1FVRVJJRVNdOiBMUXVlcmllc3xudWxsO1xuXG4gIC8qKlxuICAgKiBQb2ludGVyIHRvIHRoZSBgVFZpZXdOb2RlYCBvciBgVEVsZW1lbnROb2RlYCB3aGljaCByZXByZXNlbnRzIHRoZSByb290IG9mIHRoZSB2aWV3LlxuICAgKlxuICAgKiBJZiBgVFZpZXdOb2RlYCwgdGhpcyBpcyBhbiBlbWJlZGRlZCB2aWV3IG9mIGEgY29udGFpbmVyLiBXZSBuZWVkIHRoaXMgdG8gYmUgYWJsZSB0b1xuICAgKiBlZmZpY2llbnRseSBmaW5kIHRoZSBgTFZpZXdOb2RlYCB3aGVuIGluc2VydGluZyB0aGUgdmlldyBpbnRvIGFuIGFuY2hvci5cbiAgICpcbiAgICogSWYgYFRFbGVtZW50Tm9kZWAsIHRoaXMgaXMgdGhlIExWaWV3IG9mIGEgY29tcG9uZW50LlxuICAgKlxuICAgKiBJZiBudWxsLCB0aGlzIGlzIHRoZSByb290IHZpZXcgb2YgYW4gYXBwbGljYXRpb24gKHJvb3QgY29tcG9uZW50IGlzIGluIHRoaXMgdmlldykuXG4gICAqL1xuICBbVF9IT1NUXTogVFZpZXdOb2RlfFRFbGVtZW50Tm9kZXxudWxsO1xuXG4gIC8qKlxuICAgKiBUaGUgYmluZGluZyBpbmRleCB3ZSBzaG91bGQgYWNjZXNzIG5leHQuXG4gICAqXG4gICAqIFRoaXMgaXMgc3RvcmVkIHNvIHRoYXQgYmluZGluZ3MgY2FuIGNvbnRpbnVlIHdoZXJlIHRoZXkgbGVmdCBvZmZcbiAgICogaWYgYSB2aWV3IGlzIGxlZnQgbWlkd2F5IHRocm91Z2ggcHJvY2Vzc2luZyBiaW5kaW5ncyAoZS5nLiBpZiB0aGVyZSBpc1xuICAgKiBhIHNldHRlciB0aGF0IGNyZWF0ZXMgYW4gZW1iZWRkZWQgdmlldywgbGlrZSBpbiBuZ0lmKS5cbiAgICovXG4gIFtCSU5ESU5HX0lOREVYXTogbnVtYmVyO1xuXG4gIC8qKlxuICAgKiBXaGVuIGEgdmlldyBpcyBkZXN0cm95ZWQsIGxpc3RlbmVycyBuZWVkIHRvIGJlIHJlbGVhc2VkIGFuZCBvdXRwdXRzIG5lZWQgdG8gYmVcbiAgICogdW5zdWJzY3JpYmVkLiBUaGlzIGNvbnRleHQgYXJyYXkgc3RvcmVzIGJvdGggbGlzdGVuZXIgZnVuY3Rpb25zIHdyYXBwZWQgd2l0aFxuICAgKiB0aGVpciBjb250ZXh0IGFuZCBvdXRwdXQgc3Vic2NyaXB0aW9uIGluc3RhbmNlcyBmb3IgYSBwYXJ0aWN1bGFyIHZpZXcuXG4gICAqXG4gICAqIFRoZXNlIGNoYW5nZSBwZXIgTFZpZXcgaW5zdGFuY2UsIHNvIHRoZXkgY2Fubm90IGJlIHN0b3JlZCBvbiBUVmlldy4gSW5zdGVhZCxcbiAgICogVFZpZXcuY2xlYW51cCBzYXZlcyBhbiBpbmRleCB0byB0aGUgbmVjZXNzYXJ5IGNvbnRleHQgaW4gdGhpcyBhcnJheS5cbiAgICovXG4gIC8vIFRPRE86IGZsYXR0ZW4gaW50byBMVmlld1tdXG4gIFtDTEVBTlVQXTogYW55W118bnVsbDtcblxuICAvKipcbiAgICogLSBGb3IgZHluYW1pYyB2aWV3cywgdGhpcyBpcyB0aGUgY29udGV4dCB3aXRoIHdoaWNoIHRvIHJlbmRlciB0aGUgdGVtcGxhdGUgKGUuZy5cbiAgICogICBgTmdGb3JDb250ZXh0YCksIG9yIGB7fWAgaWYgbm90IGRlZmluZWQgZXhwbGljaXRseS5cbiAgICogLSBGb3Igcm9vdCB2aWV3IG9mIHRoZSByb290IGNvbXBvbmVudCB0aGUgY29udGV4dCBjb250YWlucyBjaGFuZ2UgZGV0ZWN0aW9uIGRhdGEuXG4gICAqIC0gRm9yIG5vbi1yb290IGNvbXBvbmVudHMsIHRoZSBjb250ZXh0IGlzIHRoZSBjb21wb25lbnQgaW5zdGFuY2UsXG4gICAqIC0gRm9yIGlubGluZSB2aWV3cywgdGhlIGNvbnRleHQgaXMgbnVsbC5cbiAgICovXG4gIFtDT05URVhUXToge318Um9vdENvbnRleHR8bnVsbDtcblxuICAvKiogQW4gb3B0aW9uYWwgTW9kdWxlIEluamVjdG9yIHRvIGJlIHVzZWQgYXMgZmFsbCBiYWNrIGFmdGVyIEVsZW1lbnQgSW5qZWN0b3JzIGFyZSBjb25zdWx0ZWQuICovXG4gIHJlYWRvbmx5W0lOSkVDVE9SXTogSW5qZWN0b3J8bnVsbDtcblxuICAvKiogUmVuZGVyZXIgdG8gYmUgdXNlZCBmb3IgdGhpcyB2aWV3LiAqL1xuICBbUkVOREVSRVJfRkFDVE9SWV06IFJlbmRlcmVyRmFjdG9yeTM7XG5cbiAgLyoqIFJlbmRlcmVyIHRvIGJlIHVzZWQgZm9yIHRoaXMgdmlldy4gKi9cbiAgW1JFTkRFUkVSXTogUmVuZGVyZXIzO1xuXG4gIC8qKiBBbiBvcHRpb25hbCBjdXN0b20gc2FuaXRpemVyLiAqL1xuICBbU0FOSVRJWkVSXTogU2FuaXRpemVyfG51bGw7XG5cbiAgLyoqXG4gICAqIFJlZmVyZW5jZSB0byB0aGUgZmlyc3QgTFZpZXcgb3IgTENvbnRhaW5lciBiZW5lYXRoIHRoaXMgTFZpZXcgaW5cbiAgICogdGhlIGhpZXJhcmNoeS5cbiAgICpcbiAgICogTmVjZXNzYXJ5IHRvIHN0b3JlIHRoaXMgc28gdmlld3MgY2FuIHRyYXZlcnNlIHRocm91Z2ggdGhlaXIgbmVzdGVkIHZpZXdzXG4gICAqIHRvIHJlbW92ZSBsaXN0ZW5lcnMgYW5kIGNhbGwgb25EZXN0cm95IGNhbGxiYWNrcy5cbiAgICovXG4gIFtDSElMRF9IRUFEXTogTFZpZXd8TENvbnRhaW5lcnxudWxsO1xuXG4gIC8qKlxuICAgKiBUaGUgbGFzdCBMVmlldyBvciBMQ29udGFpbmVyIGJlbmVhdGggdGhpcyBMVmlldyBpbiB0aGUgaGllcmFyY2h5LlxuICAgKlxuICAgKiBUaGUgdGFpbCBhbGxvd3MgdXMgdG8gcXVpY2tseSBhZGQgYSBuZXcgc3RhdGUgdG8gdGhlIGVuZCBvZiB0aGUgdmlldyBsaXN0XG4gICAqIHdpdGhvdXQgaGF2aW5nIHRvIHByb3BhZ2F0ZSBzdGFydGluZyBmcm9tIHRoZSBmaXJzdCBjaGlsZC5cbiAgICovXG4gIFtDSElMRF9UQUlMXTogTFZpZXd8TENvbnRhaW5lcnxudWxsO1xuXG4gIC8qKlxuICAgKiBWaWV3IHdoZXJlIHRoaXMgdmlldydzIHRlbXBsYXRlIHdhcyBkZWNsYXJlZC5cbiAgICpcbiAgICogT25seSBhcHBsaWNhYmxlIGZvciBkeW5hbWljYWxseSBjcmVhdGVkIHZpZXdzLiBXaWxsIGJlIG51bGwgZm9yIGlubGluZS9jb21wb25lbnQgdmlld3MuXG4gICAqXG4gICAqIFRoZSB0ZW1wbGF0ZSBmb3IgYSBkeW5hbWljYWxseSBjcmVhdGVkIHZpZXcgbWF5IGJlIGRlY2xhcmVkIGluIGEgZGlmZmVyZW50IHZpZXcgdGhhblxuICAgKiBpdCBpcyBpbnNlcnRlZC4gV2UgYWxyZWFkeSB0cmFjayB0aGUgXCJpbnNlcnRpb24gdmlld1wiICh2aWV3IHdoZXJlIHRoZSB0ZW1wbGF0ZSB3YXNcbiAgICogaW5zZXJ0ZWQpIGluIExWaWV3W1BBUkVOVF0sIGJ1dCB3ZSBhbHNvIG5lZWQgYWNjZXNzIHRvIHRoZSBcImRlY2xhcmF0aW9uIHZpZXdcIlxuICAgKiAodmlldyB3aGVyZSB0aGUgdGVtcGxhdGUgd2FzIGRlY2xhcmVkKS4gT3RoZXJ3aXNlLCB3ZSB3b3VsZG4ndCBiZSBhYmxlIHRvIGNhbGwgdGhlXG4gICAqIHZpZXcncyB0ZW1wbGF0ZSBmdW5jdGlvbiB3aXRoIHRoZSBwcm9wZXIgY29udGV4dHMuIENvbnRleHQgc2hvdWxkIGJlIGluaGVyaXRlZCBmcm9tXG4gICAqIHRoZSBkZWNsYXJhdGlvbiB2aWV3IHRyZWUsIG5vdCB0aGUgaW5zZXJ0aW9uIHZpZXcgdHJlZS5cbiAgICpcbiAgICogRXhhbXBsZSAoQXBwQ29tcG9uZW50IHRlbXBsYXRlKTpcbiAgICpcbiAgICogPG5nLXRlbXBsYXRlICNmb28+PC9uZy10ZW1wbGF0ZT4gICAgICAgPC0tIGRlY2xhcmVkIGhlcmUgLS0+XG4gICAqIDxzb21lLWNvbXAgW3RwbF09XCJmb29cIj48L3NvbWUtY29tcD4gICAgPC0tIGluc2VydGVkIGluc2lkZSB0aGlzIGNvbXBvbmVudCAtLT5cbiAgICpcbiAgICogVGhlIDxuZy10ZW1wbGF0ZT4gYWJvdmUgaXMgZGVjbGFyZWQgaW4gdGhlIEFwcENvbXBvbmVudCB0ZW1wbGF0ZSwgYnV0IGl0IHdpbGwgYmUgcGFzc2VkIGludG9cbiAgICogU29tZUNvbXAgYW5kIGluc2VydGVkIHRoZXJlLiBJbiB0aGlzIGNhc2UsIHRoZSBkZWNsYXJhdGlvbiB2aWV3IHdvdWxkIGJlIHRoZSBBcHBDb21wb25lbnQsXG4gICAqIGJ1dCB0aGUgaW5zZXJ0aW9uIHZpZXcgd291bGQgYmUgU29tZUNvbXAuIFdoZW4gd2UgYXJlIHJlbW92aW5nIHZpZXdzLCB3ZSB3b3VsZCB3YW50IHRvXG4gICAqIHRyYXZlcnNlIHRocm91Z2ggdGhlIGluc2VydGlvbiB2aWV3IHRvIGNsZWFuIHVwIGxpc3RlbmVycy4gV2hlbiB3ZSBhcmUgY2FsbGluZyB0aGVcbiAgICogdGVtcGxhdGUgZnVuY3Rpb24gZHVyaW5nIGNoYW5nZSBkZXRlY3Rpb24sIHdlIG5lZWQgdGhlIGRlY2xhcmF0aW9uIHZpZXcgdG8gZ2V0IGluaGVyaXRlZFxuICAgKiBjb250ZXh0LlxuICAgKi9cbiAgW0RFQ0xBUkFUSU9OX1ZJRVddOiBMVmlld3xudWxsO1xuXG4gIC8qKlxuICAgKiBBIGRlY2xhcmF0aW9uIHBvaW50IG9mIGVtYmVkZGVkIHZpZXdzIChvbmVzIGluc3RhbnRpYXRlZCBiYXNlZCBvbiB0aGUgY29udGVudCBvZiBhXG4gICAqIDxuZy10ZW1wbGF0ZT4pLCBudWxsIGZvciBvdGhlciB0eXBlcyBvZiB2aWV3cy5cbiAgICpcbiAgICogV2UgbmVlZCB0byB0cmFjayBhbGwgZW1iZWRkZWQgdmlld3MgY3JlYXRlZCBmcm9tIGEgZ2l2ZW4gZGVjbGFyYXRpb24gcG9pbnQgc28gd2UgY2FuIHByZXBhcmVcbiAgICogcXVlcnkgbWF0Y2hlcyBpbiBhIHByb3BlciBvcmRlciAocXVlcnkgbWF0Y2hlcyBhcmUgb3JkZXJlZCBiYXNlZCBvbiB0aGVpciBkZWNsYXJhdGlvbiBwb2ludCBhbmRcbiAgICogX25vdF8gdGhlIGluc2VydGlvbiBwb2ludCkuXG4gICAqL1xuICBbREVDTEFSQVRJT05fTENPTlRBSU5FUl06IExDb250YWluZXJ8bnVsbDtcblxuICAvKipcbiAgICogTW9yZSBmbGFncyBmb3IgdGhpcyB2aWV3LiBTZWUgUHJlT3JkZXJIb29rRmxhZ3MgZm9yIG1vcmUgaW5mby5cbiAgICovXG4gIFtQUkVPUkRFUl9IT09LX0ZMQUdTXTogUHJlT3JkZXJIb29rRmxhZ3M7XG59XG5cbi8qKiBGbGFncyBhc3NvY2lhdGVkIHdpdGggYW4gTFZpZXcgKHNhdmVkIGluIExWaWV3W0ZMQUdTXSkgKi9cbmV4cG9ydCBjb25zdCBlbnVtIExWaWV3RmxhZ3Mge1xuICAvKiogVGhlIHN0YXRlIG9mIHRoZSBpbml0IHBoYXNlIG9uIHRoZSBmaXJzdCAyIGJpdHMgKi9cbiAgSW5pdFBoYXNlU3RhdGVJbmNyZW1lbnRlciA9IDBiMDAwMDAwMDAwMDEsXG4gIEluaXRQaGFzZVN0YXRlTWFzayA9IDBiMDAwMDAwMDAwMTEsXG5cbiAgLyoqXG4gICAqIFdoZXRoZXIgb3Igbm90IHRoZSB2aWV3IGlzIGluIGNyZWF0aW9uTW9kZS5cbiAgICpcbiAgICogVGhpcyBtdXN0IGJlIHN0b3JlZCBpbiB0aGUgdmlldyByYXRoZXIgdGhhbiB1c2luZyBgZGF0YWAgYXMgYSBtYXJrZXIgc28gdGhhdFxuICAgKiB3ZSBjYW4gcHJvcGVybHkgc3VwcG9ydCBlbWJlZGRlZCB2aWV3cy4gT3RoZXJ3aXNlLCB3aGVuIGV4aXRpbmcgYSBjaGlsZCB2aWV3XG4gICAqIGJhY2sgaW50byB0aGUgcGFyZW50IHZpZXcsIGBkYXRhYCB3aWxsIGJlIGRlZmluZWQgYW5kIGBjcmVhdGlvbk1vZGVgIHdpbGwgYmVcbiAgICogaW1wcm9wZXJseSByZXBvcnRlZCBhcyBmYWxzZS5cbiAgICovXG4gIENyZWF0aW9uTW9kZSA9IDBiMDAwMDAwMDAxMDAsXG5cbiAgLyoqXG4gICAqIFdoZXRoZXIgb3Igbm90IHRoaXMgTFZpZXcgaW5zdGFuY2UgaXMgb24gaXRzIGZpcnN0IHByb2Nlc3NpbmcgcGFzcy5cbiAgICpcbiAgICogQW4gTFZpZXcgaW5zdGFuY2UgaXMgY29uc2lkZXJlZCB0byBiZSBvbiBpdHMgXCJmaXJzdCBwYXNzXCIgdW50aWwgaXRcbiAgICogaGFzIGNvbXBsZXRlZCBvbmUgY3JlYXRpb24gbW9kZSBydW4gYW5kIG9uZSB1cGRhdGUgbW9kZSBydW4uIEF0IHRoaXNcbiAgICogdGltZSwgdGhlIGZsYWcgaXMgdHVybmVkIG9mZi5cbiAgICovXG4gIEZpcnN0TFZpZXdQYXNzID0gMGIwMDAwMDAwMTAwMCxcblxuICAvKiogV2hldGhlciB0aGlzIHZpZXcgaGFzIGRlZmF1bHQgY2hhbmdlIGRldGVjdGlvbiBzdHJhdGVneSAoY2hlY2tzIGFsd2F5cykgb3Igb25QdXNoICovXG4gIENoZWNrQWx3YXlzID0gMGIwMDAwMDAxMDAwMCxcblxuICAvKipcbiAgICogV2hldGhlciBvciBub3QgbWFudWFsIGNoYW5nZSBkZXRlY3Rpb24gaXMgdHVybmVkIG9uIGZvciBvblB1c2ggY29tcG9uZW50cy5cbiAgICpcbiAgICogVGhpcyBpcyBhIHNwZWNpYWwgbW9kZSB0aGF0IG9ubHkgbWFya3MgY29tcG9uZW50cyBkaXJ0eSBpbiB0d28gY2FzZXM6XG4gICAqIDEpIFRoZXJlIGhhcyBiZWVuIGEgY2hhbmdlIHRvIGFuIEBJbnB1dCBwcm9wZXJ0eVxuICAgKiAyKSBgbWFya0RpcnR5KClgIGhhcyBiZWVuIGNhbGxlZCBtYW51YWxseSBieSB0aGUgdXNlclxuICAgKlxuICAgKiBOb3RlIHRoYXQgaW4gdGhpcyBtb2RlLCB0aGUgZmlyaW5nIG9mIGV2ZW50cyBkb2VzIE5PVCBtYXJrIGNvbXBvbmVudHNcbiAgICogZGlydHkgYXV0b21hdGljYWxseS5cbiAgICpcbiAgICogTWFudWFsIG1vZGUgaXMgdHVybmVkIG9mZiBieSBkZWZhdWx0IGZvciBiYWNrd2FyZHMgY29tcGF0aWJpbGl0eSwgYXMgZXZlbnRzXG4gICAqIGF1dG9tYXRpY2FsbHkgbWFyayBPblB1c2ggY29tcG9uZW50cyBkaXJ0eSBpbiBWaWV3IEVuZ2luZS5cbiAgICpcbiAgICogVE9ETzogQWRkIGEgcHVibGljIEFQSSB0byBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSB0byB0dXJuIHRoaXMgbW9kZSBvblxuICAgKi9cbiAgTWFudWFsT25QdXNoID0gMGIwMDAwMDEwMDAwMCxcblxuICAvKiogV2hldGhlciBvciBub3QgdGhpcyB2aWV3IGlzIGN1cnJlbnRseSBkaXJ0eSAobmVlZGluZyBjaGVjaykgKi9cbiAgRGlydHkgPSAwYjAwMDAwMTAwMDAwMCxcblxuICAvKiogV2hldGhlciBvciBub3QgdGhpcyB2aWV3IGlzIGN1cnJlbnRseSBhdHRhY2hlZCB0byBjaGFuZ2UgZGV0ZWN0aW9uIHRyZWUuICovXG4gIEF0dGFjaGVkID0gMGIwMDAwMTAwMDAwMDAsXG5cbiAgLyoqIFdoZXRoZXIgb3Igbm90IHRoaXMgdmlldyBpcyBkZXN0cm95ZWQuICovXG4gIERlc3Ryb3llZCA9IDBiMDAwMTAwMDAwMDAwLFxuXG4gIC8qKiBXaGV0aGVyIG9yIG5vdCB0aGlzIHZpZXcgaXMgdGhlIHJvb3QgdmlldyAqL1xuICBJc1Jvb3QgPSAwYjAwMTAwMDAwMDAwMCxcblxuICAvKipcbiAgICogSW5kZXggb2YgdGhlIGN1cnJlbnQgaW5pdCBwaGFzZSBvbiBsYXN0IDIyIGJpdHNcbiAgICovXG4gIEluZGV4V2l0aGluSW5pdFBoYXNlSW5jcmVtZW50ZXIgPSAwYjAxMDAwMDAwMDAwMCxcbiAgSW5kZXhXaXRoaW5Jbml0UGhhc2VTaGlmdCA9IDEwLFxuICBJbmRleFdpdGhpbkluaXRQaGFzZVJlc2V0ID0gMGIwMDExMTExMTExMTEsXG59XG5cbi8qKlxuICogUG9zc2libGUgc3RhdGVzIG9mIHRoZSBpbml0IHBoYXNlOlxuICogLSAwMDogT25Jbml0IGhvb2tzIHRvIGJlIHJ1bi5cbiAqIC0gMDE6IEFmdGVyQ29udGVudEluaXQgaG9va3MgdG8gYmUgcnVuXG4gKiAtIDEwOiBBZnRlclZpZXdJbml0IGhvb2tzIHRvIGJlIHJ1blxuICogLSAxMTogQWxsIGluaXQgaG9va3MgaGF2ZSBiZWVuIHJ1blxuICovXG5leHBvcnQgY29uc3QgZW51bSBJbml0UGhhc2VTdGF0ZSB7XG4gIE9uSW5pdEhvb2tzVG9CZVJ1biA9IDBiMDAsXG4gIEFmdGVyQ29udGVudEluaXRIb29rc1RvQmVSdW4gPSAwYjAxLFxuICBBZnRlclZpZXdJbml0SG9va3NUb0JlUnVuID0gMGIxMCxcbiAgSW5pdFBoYXNlQ29tcGxldGVkID0gMGIxMSxcbn1cblxuLyoqIE1vcmUgZmxhZ3MgYXNzb2NpYXRlZCB3aXRoIGFuIExWaWV3IChzYXZlZCBpbiBMVmlld1tGTEFHU19NT1JFXSkgKi9cbmV4cG9ydCBjb25zdCBlbnVtIFByZU9yZGVySG9va0ZsYWdzIHtcbiAgLyoqIFRoZSBpbmRleCBvZiB0aGUgbmV4dCBwcmUtb3JkZXIgaG9vayB0byBiZSBjYWxsZWQgaW4gdGhlIGhvb2tzIGFycmF5LCBvbiB0aGUgZmlyc3QgMTZcbiAgICAgYml0cyAqL1xuICBJbmRleE9mVGhlTmV4dFByZU9yZGVySG9va01hc2tNYXNrID0gMGIwMTExMTExMTExMTExMTExMSxcblxuICAvKipcbiAgICogVGhlIG51bWJlciBvZiBpbml0IGhvb2tzIHRoYXQgaGF2ZSBhbHJlYWR5IGJlZW4gY2FsbGVkLCBvbiB0aGUgbGFzdCAxNiBiaXRzXG4gICAqL1xuICBOdW1iZXJPZkluaXRIb29rc0NhbGxlZEluY3JlbWVudGVyID0gMGIwMTAwMDAwMDAwMDAwMDAwMDAsXG4gIE51bWJlck9mSW5pdEhvb2tzQ2FsbGVkU2hpZnQgPSAxNixcbiAgTnVtYmVyT2ZJbml0SG9va3NDYWxsZWRNYXNrID0gMGIxMTExMTExMTExMTExMTExMDAwMDAwMDAwMDAwMDAwMCxcbn1cblxuLyoqXG4gKiBTZXQgb2YgaW5zdHJ1Y3Rpb25zIHVzZWQgdG8gcHJvY2VzcyBob3N0IGJpbmRpbmdzIGVmZmljaWVudGx5LlxuICpcbiAqIFNlZSBWSUVXX0RBVEEubWQgZm9yIG1vcmUgaW5mb3JtYXRpb24uXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgRXhwYW5kb0luc3RydWN0aW9ucyBleHRlbmRzIEFycmF5PG51bWJlcnxIb3N0QmluZGluZ3NGdW5jdGlvbjxhbnk+fG51bGw+IHt9XG5cbi8qKlxuICogVGhlIHN0YXRpYyBkYXRhIGZvciBhbiBMVmlldyAoc2hhcmVkIGJldHdlZW4gYWxsIHRlbXBsYXRlcyBvZiBhXG4gKiBnaXZlbiB0eXBlKS5cbiAqXG4gKiBTdG9yZWQgb24gdGhlIGBDb21wb25lbnREZWYudFZpZXdgLlxuICovXG5leHBvcnQgaW50ZXJmYWNlIFRWaWV3IHtcbiAgLyoqXG4gICAqIElEIGZvciBpbmxpbmUgdmlld3MgdG8gZGV0ZXJtaW5lIHdoZXRoZXIgYSB2aWV3IGlzIHRoZSBzYW1lIGFzIHRoZSBwcmV2aW91cyB2aWV3XG4gICAqIGluIGEgY2VydGFpbiBwb3NpdGlvbi4gSWYgaXQncyBub3QsIHdlIGtub3cgdGhlIG5ldyB2aWV3IG5lZWRzIHRvIGJlIGluc2VydGVkXG4gICAqIGFuZCB0aGUgb25lIHRoYXQgZXhpc3RzIG5lZWRzIHRvIGJlIHJlbW92ZWQgKGUuZy4gaWYvZWxzZSBzdGF0ZW1lbnRzKVxuICAgKlxuICAgKiBJZiB0aGlzIGlzIC0xLCB0aGVuIHRoaXMgaXMgYSBjb21wb25lbnQgdmlldyBvciBhIGR5bmFtaWNhbGx5IGNyZWF0ZWQgdmlldy5cbiAgICovXG4gIHJlYWRvbmx5IGlkOiBudW1iZXI7XG5cbiAgLyoqXG4gICAqIFRoaXMgaXMgYSBibHVlcHJpbnQgdXNlZCB0byBnZW5lcmF0ZSBMVmlldyBpbnN0YW5jZXMgZm9yIHRoaXMgVFZpZXcuIENvcHlpbmcgdGhpc1xuICAgKiBibHVlcHJpbnQgaXMgZmFzdGVyIHRoYW4gY3JlYXRpbmcgYSBuZXcgTFZpZXcgZnJvbSBzY3JhdGNoLlxuICAgKi9cbiAgYmx1ZXByaW50OiBMVmlldztcblxuICAvKipcbiAgICogVGhlIHRlbXBsYXRlIGZ1bmN0aW9uIHVzZWQgdG8gcmVmcmVzaCB0aGUgdmlldyBvZiBkeW5hbWljYWxseSBjcmVhdGVkIHZpZXdzXG4gICAqIGFuZCBjb21wb25lbnRzLiBXaWxsIGJlIG51bGwgZm9yIGlubGluZSB2aWV3cy5cbiAgICovXG4gIHRlbXBsYXRlOiBDb21wb25lbnRUZW1wbGF0ZTx7fT58bnVsbDtcblxuICAvKipcbiAgICogQSBmdW5jdGlvbiBjb250YWluaW5nIHF1ZXJ5LXJlbGF0ZWQgaW5zdHJ1Y3Rpb25zLlxuICAgKi9cbiAgdmlld1F1ZXJ5OiBWaWV3UXVlcmllc0Z1bmN0aW9uPHt9PnxudWxsO1xuXG4gIC8qKlxuICAgKiBQb2ludGVyIHRvIHRoZSBob3N0IGBUTm9kZWAgKG5vdCBwYXJ0IG9mIHRoaXMgVFZpZXcpLlxuICAgKlxuICAgKiBJZiB0aGlzIGlzIGEgYFRWaWV3Tm9kZWAgZm9yIGFuIGBMVmlld05vZGVgLCB0aGlzIGlzIGFuIGVtYmVkZGVkIHZpZXcgb2YgYSBjb250YWluZXIuXG4gICAqIFdlIG5lZWQgdGhpcyBwb2ludGVyIHRvIGJlIGFibGUgdG8gZWZmaWNpZW50bHkgZmluZCB0aGlzIG5vZGUgd2hlbiBpbnNlcnRpbmcgdGhlIHZpZXdcbiAgICogaW50byBhbiBhbmNob3IuXG4gICAqXG4gICAqIElmIHRoaXMgaXMgYSBgVEVsZW1lbnROb2RlYCwgdGhpcyBpcyB0aGUgdmlldyBvZiBhIHJvb3QgY29tcG9uZW50LiBJdCBoYXMgZXhhY3RseSBvbmVcbiAgICogcm9vdCBUTm9kZS5cbiAgICpcbiAgICogSWYgdGhpcyBpcyBudWxsLCB0aGlzIGlzIHRoZSB2aWV3IG9mIGEgY29tcG9uZW50IHRoYXQgaXMgbm90IGF0IHJvb3QuIFdlIGRvIG5vdCBzdG9yZVxuICAgKiB0aGUgaG9zdCBUTm9kZXMgZm9yIGNoaWxkIGNvbXBvbmVudCB2aWV3cyBiZWNhdXNlIHRoZXkgY2FuIHBvdGVudGlhbGx5IGhhdmUgc2V2ZXJhbFxuICAgKiBkaWZmZXJlbnQgaG9zdCBUTm9kZXMsIGRlcGVuZGluZyBvbiB3aGVyZSB0aGUgY29tcG9uZW50IGlzIGJlaW5nIHVzZWQuIFRoZXNlIGhvc3RcbiAgICogVE5vZGVzIGNhbm5vdCBiZSBzaGFyZWQgKGR1ZSB0byBkaWZmZXJlbnQgaW5kaWNlcywgZXRjKS5cbiAgICovXG4gIG5vZGU6IFRWaWV3Tm9kZXxURWxlbWVudE5vZGV8bnVsbDtcblxuICAvKiogV2hldGhlciBvciBub3QgdGhpcyB0ZW1wbGF0ZSBoYXMgYmVlbiBwcm9jZXNzZWQuICovXG4gIGZpcnN0VGVtcGxhdGVQYXNzOiBib29sZWFuO1xuXG4gIC8qKiBTdGF0aWMgZGF0YSBlcXVpdmFsZW50IG9mIExWaWV3LmRhdGFbXS4gQ29udGFpbnMgVE5vZGVzLCBQaXBlRGVmSW50ZXJuYWwgb3IgVEkxOG4uICovXG4gIGRhdGE6IFREYXRhO1xuXG4gIC8qKlxuICAgKiBUaGUgYmluZGluZyBzdGFydCBpbmRleCBpcyB0aGUgaW5kZXggYXQgd2hpY2ggdGhlIGRhdGEgYXJyYXlcbiAgICogc3RhcnRzIHRvIHN0b3JlIGJpbmRpbmdzIG9ubHkuIFNhdmluZyB0aGlzIHZhbHVlIGVuc3VyZXMgdGhhdCB3ZVxuICAgKiB3aWxsIGJlZ2luIHJlYWRpbmcgYmluZGluZ3MgYXQgdGhlIGNvcnJlY3QgcG9pbnQgaW4gdGhlIGFycmF5IHdoZW5cbiAgICogd2UgYXJlIGluIHVwZGF0ZSBtb2RlLlxuICAgKi9cbiAgYmluZGluZ1N0YXJ0SW5kZXg6IG51bWJlcjtcblxuICAvKipcbiAgICogVGhlIGluZGV4IHdoZXJlIHRoZSBcImV4cGFuZG9cIiBzZWN0aW9uIG9mIGBMVmlld2AgYmVnaW5zLiBUaGUgZXhwYW5kb1xuICAgKiBzZWN0aW9uIGNvbnRhaW5zIGluamVjdG9ycywgZGlyZWN0aXZlIGluc3RhbmNlcywgYW5kIGhvc3QgYmluZGluZyB2YWx1ZXMuXG4gICAqIFVubGlrZSB0aGUgXCJjb25zdHNcIiBhbmQgXCJ2YXJzXCIgc2VjdGlvbnMgb2YgYExWaWV3YCwgdGhlIGxlbmd0aCBvZiB0aGlzXG4gICAqIHNlY3Rpb24gY2Fubm90IGJlIGNhbGN1bGF0ZWQgYXQgY29tcGlsZS10aW1lIGJlY2F1c2UgZGlyZWN0aXZlcyBhcmUgbWF0Y2hlZFxuICAgKiBhdCBydW50aW1lIHRvIHByZXNlcnZlIGxvY2FsaXR5LlxuICAgKlxuICAgKiBXZSBzdG9yZSB0aGlzIHN0YXJ0IGluZGV4IHNvIHdlIGtub3cgd2hlcmUgdG8gc3RhcnQgY2hlY2tpbmcgaG9zdCBiaW5kaW5nc1xuICAgKiBpbiBgc2V0SG9zdEJpbmRpbmdzYC5cbiAgICovXG4gIGV4cGFuZG9TdGFydEluZGV4OiBudW1iZXI7XG5cbiAgLyoqXG4gICAqIFdoZXRoZXIgb3Igbm90IHRoZXJlIGFyZSBhbnkgc3RhdGljIHZpZXcgcXVlcmllcyB0cmFja2VkIG9uIHRoaXMgdmlldy5cbiAgICpcbiAgICogV2Ugc3RvcmUgdGhpcyBzbyB3ZSBrbm93IHdoZXRoZXIgb3Igbm90IHdlIHNob3VsZCBkbyBhIHZpZXcgcXVlcnlcbiAgICogcmVmcmVzaCBhZnRlciBjcmVhdGlvbiBtb2RlIHRvIGNvbGxlY3Qgc3RhdGljIHF1ZXJ5IHJlc3VsdHMuXG4gICAqL1xuICBzdGF0aWNWaWV3UXVlcmllczogYm9vbGVhbjtcblxuICAvKipcbiAgICogV2hldGhlciBvciBub3QgdGhlcmUgYXJlIGFueSBzdGF0aWMgY29udGVudCBxdWVyaWVzIHRyYWNrZWQgb24gdGhpcyB2aWV3LlxuICAgKlxuICAgKiBXZSBzdG9yZSB0aGlzIHNvIHdlIGtub3cgd2hldGhlciBvciBub3Qgd2Ugc2hvdWxkIGRvIGEgY29udGVudCBxdWVyeVxuICAgKiByZWZyZXNoIGFmdGVyIGNyZWF0aW9uIG1vZGUgdG8gY29sbGVjdCBzdGF0aWMgcXVlcnkgcmVzdWx0cy5cbiAgICovXG4gIHN0YXRpY0NvbnRlbnRRdWVyaWVzOiBib29sZWFuO1xuXG4gIC8qKlxuICAgKiBBIHJlZmVyZW5jZSB0byB0aGUgZmlyc3QgY2hpbGQgbm9kZSBsb2NhdGVkIGluIHRoZSB2aWV3LlxuICAgKi9cbiAgZmlyc3RDaGlsZDogVE5vZGV8bnVsbDtcblxuICAvKipcbiAgICogU2V0IG9mIGluc3RydWN0aW9ucyB1c2VkIHRvIHByb2Nlc3MgaG9zdCBiaW5kaW5ncyBlZmZpY2llbnRseS5cbiAgICpcbiAgICogU2VlIFZJRVdfREFUQS5tZCBmb3IgbW9yZSBpbmZvcm1hdGlvbi5cbiAgICovXG4gIGV4cGFuZG9JbnN0cnVjdGlvbnM6IEV4cGFuZG9JbnN0cnVjdGlvbnN8bnVsbDtcblxuICAvKipcbiAgICogRnVsbCByZWdpc3RyeSBvZiBkaXJlY3RpdmVzIGFuZCBjb21wb25lbnRzIHRoYXQgbWF5IGJlIGZvdW5kIGluIHRoaXMgdmlldy5cbiAgICpcbiAgICogSXQncyBuZWNlc3NhcnkgdG8ga2VlcCBhIGNvcHkgb2YgdGhlIGZ1bGwgZGVmIGxpc3Qgb24gdGhlIFRWaWV3IHNvIGl0J3MgcG9zc2libGVcbiAgICogdG8gcmVuZGVyIHRlbXBsYXRlIGZ1bmN0aW9ucyB3aXRob3V0IGEgaG9zdCBjb21wb25lbnQuXG4gICAqL1xuICBkaXJlY3RpdmVSZWdpc3RyeTogRGlyZWN0aXZlRGVmTGlzdHxudWxsO1xuXG4gIC8qKlxuICAgKiBGdWxsIHJlZ2lzdHJ5IG9mIHBpcGVzIHRoYXQgbWF5IGJlIGZvdW5kIGluIHRoaXMgdmlldy5cbiAgICpcbiAgICogVGhlIHByb3BlcnR5IGlzIGVpdGhlciBhbiBhcnJheSBvZiBgUGlwZURlZnNgcyBvciBhIGZ1bmN0aW9uIHdoaWNoIHJldHVybnMgdGhlIGFycmF5IG9mXG4gICAqIGBQaXBlRGVmc2BzLiBUaGUgZnVuY3Rpb24gaXMgbmVjZXNzYXJ5IHRvIGJlIGFibGUgdG8gc3VwcG9ydCBmb3J3YXJkIGRlY2xhcmF0aW9ucy5cbiAgICpcbiAgICogSXQncyBuZWNlc3NhcnkgdG8ga2VlcCBhIGNvcHkgb2YgdGhlIGZ1bGwgZGVmIGxpc3Qgb24gdGhlIFRWaWV3IHNvIGl0J3MgcG9zc2libGVcbiAgICogdG8gcmVuZGVyIHRlbXBsYXRlIGZ1bmN0aW9ucyB3aXRob3V0IGEgaG9zdCBjb21wb25lbnQuXG4gICAqL1xuICBwaXBlUmVnaXN0cnk6IFBpcGVEZWZMaXN0fG51bGw7XG5cbiAgLyoqXG4gICAqIEFycmF5IG9mIG5nT25Jbml0LCBuZ09uQ2hhbmdlcyBhbmQgbmdEb0NoZWNrIGhvb2tzIHRoYXQgc2hvdWxkIGJlIGV4ZWN1dGVkIGZvciB0aGlzIHZpZXcgaW5cbiAgICogY3JlYXRpb24gbW9kZS5cbiAgICpcbiAgICogRXZlbiBpbmRpY2VzOiBEaXJlY3RpdmUgaW5kZXhcbiAgICogT2RkIGluZGljZXM6IEhvb2sgZnVuY3Rpb25cbiAgICovXG4gIHByZU9yZGVySG9va3M6IEhvb2tEYXRhfG51bGw7XG5cbiAgLyoqXG4gICAqIEFycmF5IG9mIG5nT25DaGFuZ2VzIGFuZCBuZ0RvQ2hlY2sgaG9va3MgdGhhdCBzaG91bGQgYmUgZXhlY3V0ZWQgZm9yIHRoaXMgdmlldyBpbiB1cGRhdGUgbW9kZS5cbiAgICpcbiAgICogRXZlbiBpbmRpY2VzOiBEaXJlY3RpdmUgaW5kZXhcbiAgICogT2RkIGluZGljZXM6IEhvb2sgZnVuY3Rpb25cbiAgICovXG4gIHByZU9yZGVyQ2hlY2tIb29rczogSG9va0RhdGF8bnVsbDtcblxuICAvKipcbiAgICogQXJyYXkgb2YgbmdBZnRlckNvbnRlbnRJbml0IGFuZCBuZ0FmdGVyQ29udGVudENoZWNrZWQgaG9va3MgdGhhdCBzaG91bGQgYmUgZXhlY3V0ZWRcbiAgICogZm9yIHRoaXMgdmlldyBpbiBjcmVhdGlvbiBtb2RlLlxuICAgKlxuICAgKiBFdmVuIGluZGljZXM6IERpcmVjdGl2ZSBpbmRleFxuICAgKiBPZGQgaW5kaWNlczogSG9vayBmdW5jdGlvblxuICAgKi9cbiAgY29udGVudEhvb2tzOiBIb29rRGF0YXxudWxsO1xuXG4gIC8qKlxuICAgKiBBcnJheSBvZiBuZ0FmdGVyQ29udGVudENoZWNrZWQgaG9va3MgdGhhdCBzaG91bGQgYmUgZXhlY3V0ZWQgZm9yIHRoaXMgdmlldyBpbiB1cGRhdGVcbiAgICogbW9kZS5cbiAgICpcbiAgICogRXZlbiBpbmRpY2VzOiBEaXJlY3RpdmUgaW5kZXhcbiAgICogT2RkIGluZGljZXM6IEhvb2sgZnVuY3Rpb25cbiAgICovXG4gIGNvbnRlbnRDaGVja0hvb2tzOiBIb29rRGF0YXxudWxsO1xuXG4gIC8qKlxuICAgKiBBcnJheSBvZiBuZ0FmdGVyVmlld0luaXQgYW5kIG5nQWZ0ZXJWaWV3Q2hlY2tlZCBob29rcyB0aGF0IHNob3VsZCBiZSBleGVjdXRlZCBmb3JcbiAgICogdGhpcyB2aWV3IGluIGNyZWF0aW9uIG1vZGUuXG4gICAqXG4gICAqIEV2ZW4gaW5kaWNlczogRGlyZWN0aXZlIGluZGV4XG4gICAqIE9kZCBpbmRpY2VzOiBIb29rIGZ1bmN0aW9uXG4gICAqL1xuICB2aWV3SG9va3M6IEhvb2tEYXRhfG51bGw7XG5cbiAgLyoqXG4gICAqIEFycmF5IG9mIG5nQWZ0ZXJWaWV3Q2hlY2tlZCBob29rcyB0aGF0IHNob3VsZCBiZSBleGVjdXRlZCBmb3IgdGhpcyB2aWV3IGluXG4gICAqIHVwZGF0ZSBtb2RlLlxuICAgKlxuICAgKiBFdmVuIGluZGljZXM6IERpcmVjdGl2ZSBpbmRleFxuICAgKiBPZGQgaW5kaWNlczogSG9vayBmdW5jdGlvblxuICAgKi9cbiAgdmlld0NoZWNrSG9va3M6IEhvb2tEYXRhfG51bGw7XG5cbiAgLyoqXG4gICAqIEFycmF5IG9mIG5nT25EZXN0cm95IGhvb2tzIHRoYXQgc2hvdWxkIGJlIGV4ZWN1dGVkIHdoZW4gdGhpcyB2aWV3IGlzIGRlc3Ryb3llZC5cbiAgICpcbiAgICogRXZlbiBpbmRpY2VzOiBEaXJlY3RpdmUgaW5kZXhcbiAgICogT2RkIGluZGljZXM6IEhvb2sgZnVuY3Rpb25cbiAgICovXG4gIGRlc3Ryb3lIb29rczogSG9va0RhdGF8bnVsbDtcblxuICAvKipcbiAgICogV2hlbiBhIHZpZXcgaXMgZGVzdHJveWVkLCBsaXN0ZW5lcnMgbmVlZCB0byBiZSByZWxlYXNlZCBhbmQgb3V0cHV0cyBuZWVkIHRvIGJlXG4gICAqIHVuc3Vic2NyaWJlZC4gVGhpcyBjbGVhbnVwIGFycmF5IHN0b3JlcyBib3RoIGxpc3RlbmVyIGRhdGEgKGluIGNodW5rcyBvZiA0KVxuICAgKiBhbmQgb3V0cHV0IGRhdGEgKGluIGNodW5rcyBvZiAyKSBmb3IgYSBwYXJ0aWN1bGFyIHZpZXcuIENvbWJpbmluZyB0aGUgYXJyYXlzXG4gICAqIHNhdmVzIG9uIG1lbW9yeSAoNzAgYnl0ZXMgcGVyIGFycmF5KSBhbmQgb24gYSBmZXcgYnl0ZXMgb2YgY29kZSBzaXplIChmb3IgdHdvXG4gICAqIHNlcGFyYXRlIGZvciBsb29wcykuXG4gICAqXG4gICAqIElmIGl0J3MgYSBuYXRpdmUgRE9NIGxpc3RlbmVyIG9yIG91dHB1dCBzdWJzY3JpcHRpb24gYmVpbmcgc3RvcmVkOlxuICAgKiAxc3QgaW5kZXggaXM6IGV2ZW50IG5hbWUgIGBuYW1lID0gdFZpZXcuY2xlYW51cFtpKzBdYFxuICAgKiAybmQgaW5kZXggaXM6IGluZGV4IG9mIG5hdGl2ZSBlbGVtZW50IG9yIGEgZnVuY3Rpb24gdGhhdCByZXRyaWV2ZXMgZ2xvYmFsIHRhcmdldCAod2luZG93LFxuICAgKiAgICAgICAgICAgICAgIGRvY3VtZW50IG9yIGJvZHkpIHJlZmVyZW5jZSBiYXNlZCBvbiB0aGUgbmF0aXZlIGVsZW1lbnQ6XG4gICAqICAgIGB0eXBlb2YgaWR4T3JUYXJnZXRHZXR0ZXIgPT09ICdmdW5jdGlvbidgOiBnbG9iYWwgdGFyZ2V0IGdldHRlciBmdW5jdGlvblxuICAgKiAgICBgdHlwZW9mIGlkeE9yVGFyZ2V0R2V0dGVyID09PSAnbnVtYmVyJ2A6IGluZGV4IG9mIG5hdGl2ZSBlbGVtZW50XG4gICAqXG4gICAqIDNyZCBpbmRleCBpczogaW5kZXggb2YgbGlzdGVuZXIgZnVuY3Rpb24gYGxpc3RlbmVyID0gbFZpZXdbQ0xFQU5VUF1bdFZpZXcuY2xlYW51cFtpKzJdXWBcbiAgICogNHRoIGluZGV4IGlzOiBgdXNlQ2FwdHVyZU9ySW5keCA9IHRWaWV3LmNsZWFudXBbaSszXWBcbiAgICogICAgYHR5cGVvZiB1c2VDYXB0dXJlT3JJbmR4ID09ICdib29sZWFuJyA6IHVzZUNhcHR1cmUgYm9vbGVhblxuICAgKiAgICBgdHlwZW9mIHVzZUNhcHR1cmVPckluZHggPT0gJ251bWJlcic6XG4gICAqICAgICAgICAgYHVzZUNhcHR1cmVPckluZHggPj0gMGAgYHJlbW92ZUxpc3RlbmVyID0gTFZpZXdbQ0xFQU5VUF1bdXNlQ2FwdHVyZU9ySW5keF1gXG4gICAqICAgICAgICAgYHVzZUNhcHR1cmVPckluZHggPCAgMGAgYHN1YnNjcmlwdGlvbiA9IExWaWV3W0NMRUFOVVBdWy11c2VDYXB0dXJlT3JJbmR4XWBcbiAgICpcbiAgICogSWYgaXQncyBhbiBvdXRwdXQgc3Vic2NyaXB0aW9uIG9yIHF1ZXJ5IGxpc3QgZGVzdHJveSBob29rOlxuICAgKiAxc3QgaW5kZXggaXM6IG91dHB1dCB1bnN1YnNjcmliZSBmdW5jdGlvbiAvIHF1ZXJ5IGxpc3QgZGVzdHJveSBmdW5jdGlvblxuICAgKiAybmQgaW5kZXggaXM6IGluZGV4IG9mIGZ1bmN0aW9uIGNvbnRleHQgaW4gTFZpZXcuY2xlYW51cEluc3RhbmNlc1tdXG4gICAqICAgICAgICAgICAgICAgYHRWaWV3LmNsZWFudXBbaSswXS5jYWxsKGxWaWV3W0NMRUFOVVBdW3RWaWV3LmNsZWFudXBbaSsxXV0pYFxuICAgKi9cbiAgY2xlYW51cDogYW55W118bnVsbDtcblxuICAvKipcbiAgICogQSBsaXN0IG9mIGVsZW1lbnQgaW5kaWNlcyBmb3IgY2hpbGQgY29tcG9uZW50cyB0aGF0IHdpbGwgbmVlZCB0byBiZVxuICAgKiByZWZyZXNoZWQgd2hlbiB0aGUgY3VycmVudCB2aWV3IGhhcyBmaW5pc2hlZCBpdHMgY2hlY2suIFRoZXNlIGluZGljZXMgaGF2ZVxuICAgKiBhbHJlYWR5IGJlZW4gYWRqdXN0ZWQgZm9yIHRoZSBIRUFERVJfT0ZGU0VULlxuICAgKlxuICAgKi9cbiAgY29tcG9uZW50czogbnVtYmVyW118bnVsbDtcblxuICAvKipcbiAgICogQSBjb2xsZWN0aW9uIG9mIHF1ZXJpZXMgdHJhY2tlZCBpbiBhIGdpdmVuIHZpZXcuXG4gICAqL1xuICBxdWVyaWVzOiBUUXVlcmllc3xudWxsO1xuXG4gIC8qKlxuICAgKiBBbiBhcnJheSBvZiBpbmRpY2VzIHBvaW50aW5nIHRvIGRpcmVjdGl2ZXMgd2l0aCBjb250ZW50IHF1ZXJpZXMgYWxvbmdzaWRlIHdpdGggdGhlXG4gICAqIGNvcnJlc3BvbmRpbmdcbiAgICogcXVlcnkgaW5kZXguIEVhY2ggZW50cnkgaW4gdGhpcyBhcnJheSBpcyBhIHR1cGxlIG9mOlxuICAgKiAtIGluZGV4IG9mIHRoZSBmaXJzdCBjb250ZW50IHF1ZXJ5IGluZGV4IGRlY2xhcmVkIGJ5IGEgZ2l2ZW4gZGlyZWN0aXZlO1xuICAgKiAtIGluZGV4IG9mIGEgZGlyZWN0aXZlLlxuICAgKlxuICAgKiBXZSBhcmUgc3RvcmluZyB0aG9zZSBpbmRleGVzIHNvIHdlIGNhbiByZWZyZXNoIGNvbnRlbnQgcXVlcmllcyBhcyBwYXJ0IG9mIGEgdmlldyByZWZyZXNoXG4gICAqIHByb2Nlc3MuXG4gICAqL1xuICBjb250ZW50UXVlcmllczogbnVtYmVyW118bnVsbDtcblxuICAvKipcbiAgICogU2V0IG9mIHNjaGVtYXMgdGhhdCBkZWNsYXJlIGVsZW1lbnRzIHRvIGJlIGFsbG93ZWQgaW5zaWRlIHRoZSB2aWV3LlxuICAgKi9cbiAgc2NoZW1hczogU2NoZW1hTWV0YWRhdGFbXXxudWxsO1xufVxuXG5leHBvcnQgY29uc3QgZW51bSBSb290Q29udGV4dEZsYWdzIHtFbXB0eSA9IDBiMDAsIERldGVjdENoYW5nZXMgPSAwYjAxLCBGbHVzaFBsYXllcnMgPSAwYjEwfVxuXG5cbi8qKlxuICogUm9vdENvbnRleHQgY29udGFpbnMgaW5mb3JtYXRpb24gd2hpY2ggaXMgc2hhcmVkIGZvciBhbGwgY29tcG9uZW50cyB3aGljaFxuICogd2VyZSBib290c3RyYXBwZWQgd2l0aCB7QGxpbmsgcmVuZGVyQ29tcG9uZW50fS5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBSb290Q29udGV4dCB7XG4gIC8qKlxuICAgKiBBIGZ1bmN0aW9uIHVzZWQgZm9yIHNjaGVkdWxpbmcgY2hhbmdlIGRldGVjdGlvbiBpbiB0aGUgZnV0dXJlLiBVc3VhbGx5XG4gICAqIHRoaXMgaXMgYHJlcXVlc3RBbmltYXRpb25GcmFtZWAuXG4gICAqL1xuICBzY2hlZHVsZXI6ICh3b3JrRm46ICgpID0+IHZvaWQpID0+IHZvaWQ7XG5cbiAgLyoqXG4gICAqIEEgcHJvbWlzZSB3aGljaCBpcyByZXNvbHZlZCB3aGVuIGFsbCBjb21wb25lbnRzIGFyZSBjb25zaWRlcmVkIGNsZWFuIChub3QgZGlydHkpLlxuICAgKlxuICAgKiBUaGlzIHByb21pc2UgaXMgb3ZlcndyaXR0ZW4gZXZlcnkgdGltZSBhIGZpcnN0IGNhbGwgdG8ge0BsaW5rIG1hcmtEaXJ0eX0gaXMgaW52b2tlZC5cbiAgICovXG4gIGNsZWFuOiBQcm9taXNlPG51bGw+O1xuXG4gIC8qKlxuICAgKiBSb290Q29tcG9uZW50cyAtIFRoZSBjb21wb25lbnRzIHRoYXQgd2VyZSBpbnN0YW50aWF0ZWQgYnkgdGhlIGNhbGwgdG9cbiAgICoge0BsaW5rIHJlbmRlckNvbXBvbmVudH0uXG4gICAqL1xuICBjb21wb25lbnRzOiB7fVtdO1xuXG4gIC8qKlxuICAgKiBUaGUgcGxheWVyIGZsdXNoaW5nIGhhbmRsZXIgdG8ga2ljayBvZmYgYWxsIGFuaW1hdGlvbnNcbiAgICovXG4gIHBsYXllckhhbmRsZXI6IFBsYXllckhhbmRsZXJ8bnVsbDtcblxuICAvKipcbiAgICogV2hhdCByZW5kZXItcmVsYXRlZCBvcGVyYXRpb25zIHRvIHJ1biBvbmNlIGEgc2NoZWR1bGVyIGhhcyBiZWVuIHNldFxuICAgKi9cbiAgZmxhZ3M6IFJvb3RDb250ZXh0RmxhZ3M7XG59XG5cbi8qKlxuICogQXJyYXkgb2YgaG9va3MgdGhhdCBzaG91bGQgYmUgZXhlY3V0ZWQgZm9yIGEgdmlldyBhbmQgdGhlaXIgZGlyZWN0aXZlIGluZGljZXMuXG4gKlxuICogRm9yIGVhY2ggbm9kZSBvZiB0aGUgdmlldywgdGhlIGZvbGxvd2luZyBkYXRhIGlzIHN0b3JlZDpcbiAqIDEpIE5vZGUgaW5kZXggKG9wdGlvbmFsKVxuICogMikgQSBzZXJpZXMgb2YgbnVtYmVyL2Z1bmN0aW9uIHBhaXJzIHdoZXJlOlxuICogIC0gZXZlbiBpbmRpY2VzIGFyZSBkaXJlY3RpdmUgaW5kaWNlc1xuICogIC0gb2RkIGluZGljZXMgYXJlIGhvb2sgZnVuY3Rpb25zXG4gKlxuICogU3BlY2lhbCBjYXNlczpcbiAqICAtIGEgbmVnYXRpdmUgZGlyZWN0aXZlIGluZGV4IGZsYWdzIGFuIGluaXQgaG9vayAobmdPbkluaXQsIG5nQWZ0ZXJDb250ZW50SW5pdCwgbmdBZnRlclZpZXdJbml0KVxuICovXG5leHBvcnQgdHlwZSBIb29rRGF0YSA9IChudW1iZXIgfCAoKCkgPT4gdm9pZCkpW107XG5cbi8qKlxuICogU3RhdGljIGRhdGEgdGhhdCBjb3JyZXNwb25kcyB0byB0aGUgaW5zdGFuY2Utc3BlY2lmaWMgZGF0YSBhcnJheSBvbiBhbiBMVmlldy5cbiAqXG4gKiBFYWNoIG5vZGUncyBzdGF0aWMgZGF0YSBpcyBzdG9yZWQgaW4gdERhdGEgYXQgdGhlIHNhbWUgaW5kZXggdGhhdCBpdCdzIHN0b3JlZFxuICogaW4gdGhlIGRhdGEgYXJyYXkuICBBbnkgbm9kZXMgdGhhdCBkbyBub3QgaGF2ZSBzdGF0aWMgZGF0YSBzdG9yZSBhIG51bGwgdmFsdWUgaW5cbiAqIHREYXRhIHRvIGF2b2lkIGEgc3BhcnNlIGFycmF5LlxuICpcbiAqIEVhY2ggcGlwZSdzIGRlZmluaXRpb24gaXMgc3RvcmVkIGhlcmUgYXQgdGhlIHNhbWUgaW5kZXggYXMgaXRzIHBpcGUgaW5zdGFuY2UgaW5cbiAqIHRoZSBkYXRhIGFycmF5LlxuICpcbiAqIEVhY2ggaG9zdCBwcm9wZXJ0eSdzIG5hbWUgaXMgc3RvcmVkIGhlcmUgYXQgdGhlIHNhbWUgaW5kZXggYXMgaXRzIHZhbHVlIGluIHRoZVxuICogZGF0YSBhcnJheS5cbiAqXG4gKiBFYWNoIHByb3BlcnR5IGJpbmRpbmcgbmFtZSBpcyBzdG9yZWQgaGVyZSBhdCB0aGUgc2FtZSBpbmRleCBhcyBpdHMgdmFsdWUgaW5cbiAqIHRoZSBkYXRhIGFycmF5LiBJZiB0aGUgYmluZGluZyBpcyBhbiBpbnRlcnBvbGF0aW9uLCB0aGUgc3RhdGljIHN0cmluZyB2YWx1ZXNcbiAqIGFyZSBzdG9yZWQgcGFyYWxsZWwgdG8gdGhlIGR5bmFtaWMgdmFsdWVzLiBFeGFtcGxlOlxuICpcbiAqIGlkPVwicHJlZml4IHt7IHYwIH19IGEge3sgdjEgfX0gYiB7eyB2MiB9fSBzdWZmaXhcIlxuICpcbiAqIExWaWV3ICAgICAgIHwgICBUVmlldy5kYXRhXG4gKi0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuICogIHYwIHZhbHVlICAgfCAgICdhJ1xuICogIHYxIHZhbHVlICAgfCAgICdiJ1xuICogIHYyIHZhbHVlICAgfCAgIGlkIO+/vSBwcmVmaXgg77+9IHN1ZmZpeFxuICpcbiAqIEluamVjdG9yIGJsb29tIGZpbHRlcnMgYXJlIGFsc28gc3RvcmVkIGhlcmUuXG4gKi9cbmV4cG9ydCB0eXBlIFREYXRhID1cbiAgICAoVE5vZGUgfCBQaXBlRGVmPGFueT58IERpcmVjdGl2ZURlZjxhbnk+fCBDb21wb25lbnREZWY8YW55PnwgbnVtYmVyIHwgVHlwZTxhbnk+fFxuICAgICBJbmplY3Rpb25Ub2tlbjxhbnk+fCBUSTE4biB8IEkxOG5VcGRhdGVPcENvZGVzIHwgbnVsbCB8IHN0cmluZylbXTtcblxuLy8gTm90ZTogVGhpcyBoYWNrIGlzIG5lY2Vzc2FyeSBzbyB3ZSBkb24ndCBlcnJvbmVvdXNseSBnZXQgYSBjaXJjdWxhciBkZXBlbmRlbmN5XG4vLyBmYWlsdXJlIGJhc2VkIG9uIHR5cGVzLlxuZXhwb3J0IGNvbnN0IHVudXNlZFZhbHVlRXhwb3J0VG9QbGFjYXRlQWpkID0gMTtcbiJdfQ==
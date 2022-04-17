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
import { checkNoChangesInRootView, checkNoChangesInternal, detectChangesInRootView, detectChangesInternal, markViewDirty, storeCleanupFn } from './instructions/shared';
import { FLAGS, HOST, T_HOST } from './interfaces/view';
import { destroyLView, renderDetachView } from './node_manipulation';
import { findComponentView, getLViewParent } from './util/view_traversal_utils';
import { getNativeByTNode, getNativeByTNodeOrNull } from './util/view_utils';
/**
 * @record
 */
export function viewEngine_ChangeDetectorRef_interface() { }
/**
 * @template T
 */
export class ViewRef {
    /**
     * @param {?} _lView
     * @param {?} _context
     * @param {?} _componentIndex
     */
    constructor(_lView, _context, _componentIndex) {
        this._context = _context;
        this._componentIndex = _componentIndex;
        this._appRef = null;
        this._viewContainerRef = null;
        /**
         * \@internal
         */
        this._tViewNode = null;
        this._lView = _lView;
    }
    /**
     * @return {?}
     */
    get rootNodes() {
        if (this._lView[HOST] == null) {
            /** @type {?} */
            const tView = (/** @type {?} */ (this._lView[T_HOST]));
            return collectNativeNodes(this._lView, tView, []);
        }
        return [];
    }
    /**
     * @return {?}
     */
    get context() { return this._context ? this._context : this._lookUpContext(); }
    /**
     * @return {?}
     */
    get destroyed() {
        return (this._lView[FLAGS] & 256 /* Destroyed */) === 256 /* Destroyed */;
    }
    /**
     * @return {?}
     */
    destroy() {
        if (this._appRef) {
            this._appRef.detachView(this);
        }
        else if (this._viewContainerRef) {
            /** @type {?} */
            const index = this._viewContainerRef.indexOf(this);
            if (index > -1) {
                this._viewContainerRef.detach(index);
            }
            this._viewContainerRef = null;
        }
        destroyLView(this._lView);
    }
    /**
     * @param {?} callback
     * @return {?}
     */
    onDestroy(callback) { storeCleanupFn(this._lView, callback); }
    /**
     * Marks a view and all of its ancestors dirty.
     *
     * It also triggers change detection by calling `scheduleTick` internally, which coalesces
     * multiple `markForCheck` calls to into one change detection run.
     *
     * This can be used to ensure an {\@link ChangeDetectionStrategy#OnPush OnPush} component is
     * checked when it needs to be re-rendered but the two normal triggers haven't marked it
     * dirty (i.e. inputs haven't changed and events haven't fired in the view).
     *
     * <!-- TODO: Add a link to a chapter on OnPush components -->
     *
     * \@usageNotes
     * ### Example
     *
     * ```typescript
     * \@Component({
     *   selector: 'my-app',
     *   template: `Number of ticks: {{numberOfTicks}}`
     *   changeDetection: ChangeDetectionStrategy.OnPush,
     * })
     * class AppComponent {
     *   numberOfTicks = 0;
     *
     *   constructor(private ref: ChangeDetectorRef) {
     *     setInterval(() => {
     *       this.numberOfTicks++;
     *       // the following is required, otherwise the view will not be updated
     *       this.ref.markForCheck();
     *     }, 1000);
     *   }
     * }
     * ```
     * @return {?}
     */
    markForCheck() { markViewDirty(this._lView); }
    /**
     * Detaches the view from the change detection tree.
     *
     * Detached views will not be checked during change detection runs until they are
     * re-attached, even if they are dirty. `detach` can be used in combination with
     * {\@link ChangeDetectorRef#detectChanges detectChanges} to implement local change
     * detection checks.
     *
     * <!-- TODO: Add a link to a chapter on detach/reattach/local digest -->
     * <!-- TODO: Add a live demo once ref.detectChanges is merged into master -->
     *
     * \@usageNotes
     * ### Example
     *
     * The following example defines a component with a large list of readonly data.
     * Imagine the data changes constantly, many times per second. For performance reasons,
     * we want to check and update the list every five seconds. We can do that by detaching
     * the component's change detector and doing a local check every five seconds.
     *
     * ```typescript
     * class DataProvider {
     *   // in a real application the returned data will be different every time
     *   get data() {
     *     return [1,2,3,4,5];
     *   }
     * }
     *
     * \@Component({
     *   selector: 'giant-list',
     *   template: `
     *     <li *ngFor="let d of dataProvider.data">Data {{d}}</li>
     *   `,
     * })
     * class GiantList {
     *   constructor(private ref: ChangeDetectorRef, private dataProvider: DataProvider) {
     *     ref.detach();
     *     setInterval(() => {
     *       this.ref.detectChanges();
     *     }, 5000);
     *   }
     * }
     *  /
     *   selector: 'app',
     *   providers: [DataProvider],
     *   template: `
     *     <giant-list><giant-list>
     *   `,
     * })
     * class App {
     * }
     * ```
     * @return {?}
     */
    detach() { this._lView[FLAGS] &= ~128 /* Attached */; }
    /**
     * Re-attaches a view to the change detection tree.
     *
     * This can be used to re-attach views that were previously detached from the tree
     * using {\@link ChangeDetectorRef#detach detach}. Views are attached to the tree by default.
     *
     * <!-- TODO: Add a link to a chapter on detach/reattach/local digest -->
     *
     * \@usageNotes
     * ### Example
     *
     * The following example creates a component displaying `live` data. The component will detach
     * its change detector from the main change detector tree when the component's live property
     * is set to false.
     *
     * ```typescript
     * class DataProvider {
     *   data = 1;
     *
     *   constructor() {
     *     setInterval(() => {
     *       this.data = this.data * 2;
     *     }, 500);
     *   }
     * }
     *
     * \@Component({
     *   selector: 'live-data',
     *   inputs: ['live'],
     *   template: 'Data: {{dataProvider.data}}'
     * })
     * class LiveData {
     *   constructor(private ref: ChangeDetectorRef, private dataProvider: DataProvider) {}
     *
     *   set live(value) {
     *     if (value) {
     *       this.ref.reattach();
     *     } else {
     *       this.ref.detach();
     *     }
     *   }
     * }
     *  /
     *   selector: 'my-app',
     *   providers: [DataProvider],
     *   template: `
     *     Live Update: <input type="checkbox" [(ngModel)]="live">
     *     <live-data [live]="live"><live-data>
     *   `,
     * })
     * class AppComponent {
     *   live = true;
     * }
     * ```
     * @return {?}
     */
    reattach() { this._lView[FLAGS] |= 128 /* Attached */; }
    /**
     * Checks the view and its children.
     *
     * This can also be used in combination with {\@link ChangeDetectorRef#detach detach} to implement
     * local change detection checks.
     *
     * <!-- TODO: Add a link to a chapter on detach/reattach/local digest -->
     * <!-- TODO: Add a live demo once ref.detectChanges is merged into master -->
     *
     * \@usageNotes
     * ### Example
     *
     * The following example defines a component with a large list of readonly data.
     * Imagine, the data changes constantly, many times per second. For performance reasons,
     * we want to check and update the list every five seconds.
     *
     * We can do that by detaching the component's change detector and doing a local change detection
     * check every five seconds.
     *
     * See {\@link ChangeDetectorRef#detach detach} for more information.
     * @return {?}
     */
    detectChanges() { detectChangesInternal(this._lView, this.context); }
    /**
     * Checks the change detector and its children, and throws if any changes are detected.
     *
     * This is used in development mode to verify that running change detection doesn't
     * introduce other changes.
     * @return {?}
     */
    checkNoChanges() { checkNoChangesInternal(this._lView, this.context); }
    /**
     * @param {?} vcRef
     * @return {?}
     */
    attachToViewContainerRef(vcRef) {
        if (this._appRef) {
            throw new Error('This view is already attached directly to the ApplicationRef!');
        }
        this._viewContainerRef = vcRef;
    }
    /**
     * @return {?}
     */
    detachFromAppRef() {
        this._appRef = null;
        renderDetachView(this._lView);
    }
    /**
     * @param {?} appRef
     * @return {?}
     */
    attachToAppRef(appRef) {
        if (this._viewContainerRef) {
            throw new Error('This view is already attached to a ViewContainer!');
        }
        this._appRef = appRef;
    }
    /**
     * @private
     * @return {?}
     */
    _lookUpContext() {
        return this._context = (/** @type {?} */ ((/** @type {?} */ (getLViewParent(this._lView)))[this._componentIndex]));
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    ViewRef.prototype._appRef;
    /**
     * @type {?}
     * @private
     */
    ViewRef.prototype._viewContainerRef;
    /**
     * \@internal
     * @type {?}
     */
    ViewRef.prototype._tViewNode;
    /**
     * \@internal
     * @type {?}
     */
    ViewRef.prototype._lView;
    /**
     * @type {?}
     * @private
     */
    ViewRef.prototype._context;
    /**
     * @type {?}
     * @private
     */
    ViewRef.prototype._componentIndex;
}
/**
 * \@internal
 * @template T
 */
export class RootViewRef extends ViewRef {
    /**
     * @param {?} _view
     */
    constructor(_view) {
        super(_view, null, -1);
        this._view = _view;
    }
    /**
     * @return {?}
     */
    detectChanges() { detectChangesInRootView(this._view); }
    /**
     * @return {?}
     */
    checkNoChanges() { checkNoChangesInRootView(this._view); }
    /**
     * @return {?}
     */
    get context() { return (/** @type {?} */ (null)); }
}
if (false) {
    /** @type {?} */
    RootViewRef.prototype._view;
}
/**
 * @param {?} lView
 * @param {?} parentTNode
 * @param {?} result
 * @return {?}
 */
function collectNativeNodes(lView, parentTNode, result) {
    /** @type {?} */
    let tNodeChild = parentTNode.child;
    while (tNodeChild) {
        /** @type {?} */
        const nativeNode = getNativeByTNodeOrNull(tNodeChild, lView);
        nativeNode && result.push(nativeNode);
        if (tNodeChild.type === 4 /* ElementContainer */) {
            collectNativeNodes(lView, tNodeChild, result);
        }
        else if (tNodeChild.type === 1 /* Projection */) {
            /** @type {?} */
            const componentView = findComponentView(lView);
            /** @type {?} */
            const componentHost = (/** @type {?} */ (componentView[T_HOST]));
            /** @type {?} */
            const parentView = getLViewParent(componentView);
            /** @type {?} */
            let currentProjectedNode = ((/** @type {?} */ (componentHost.projection)))[(/** @type {?} */ (tNodeChild.projection))];
            while (currentProjectedNode && parentView) {
                result.push(getNativeByTNode(currentProjectedNode, parentView));
                currentProjectedNode = currentProjectedNode.next;
            }
        }
        tNodeChild = tNodeChild.next;
    }
    return result;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidmlld19yZWYuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL3ZpZXdfcmVmLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBYUEsT0FBTyxFQUFDLHdCQUF3QixFQUFFLHNCQUFzQixFQUFFLHVCQUF1QixFQUFFLHFCQUFxQixFQUFFLGFBQWEsRUFBRSxjQUFjLEVBQUMsTUFBTSx1QkFBdUIsQ0FBQztBQUV0SyxPQUFPLEVBQUMsS0FBSyxFQUFFLElBQUksRUFBcUIsTUFBTSxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFDekUsT0FBTyxFQUFDLFlBQVksRUFBRSxnQkFBZ0IsRUFBQyxNQUFNLHFCQUFxQixDQUFDO0FBQ25FLE9BQU8sRUFBQyxpQkFBaUIsRUFBRSxjQUFjLEVBQUMsTUFBTSw2QkFBNkIsQ0FBQztBQUM5RSxPQUFPLEVBQUMsZ0JBQWdCLEVBQUUsc0JBQXNCLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQzs7OztBQU8zRSw0REFBK0Y7Ozs7QUFFL0YsTUFBTSxPQUFPLE9BQU87Ozs7OztJQXVCbEIsWUFBWSxNQUFhLEVBQVUsUUFBZ0IsRUFBVSxlQUF1QjtRQUFqRCxhQUFRLEdBQVIsUUFBUSxDQUFRO1FBQVUsb0JBQWUsR0FBZixlQUFlLENBQVE7UUFyQjVFLFlBQU8sR0FBd0IsSUFBSSxDQUFDO1FBQ3BDLHNCQUFpQixHQUFxQyxJQUFJLENBQUM7Ozs7UUFLNUQsZUFBVSxHQUFtQixJQUFJLENBQUM7UUFnQnZDLElBQUksQ0FBQyxNQUFNLEdBQUcsTUFBTSxDQUFDO0lBQ3ZCLENBQUM7Ozs7SUFWRCxJQUFJLFNBQVM7UUFDWCxJQUFJLElBQUksQ0FBQyxNQUFNLENBQUMsSUFBSSxDQUFDLElBQUksSUFBSSxFQUFFOztrQkFDdkIsS0FBSyxHQUFHLG1CQUFBLElBQUksQ0FBQyxNQUFNLENBQUMsTUFBTSxDQUFDLEVBQWE7WUFDOUMsT0FB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LEdBQUcsaUJBQWlCLENBQUMsS0FBSyxDQUFDOztrQkFDeEMsYUFBYSxHQUFHLG1CQUFBLGFBQWEsQ0FBQyxNQUFNLENBQUMsRUFBZ0I7O2tCQUNyRCxVQUFVLEdBQUcsY0FBYyxDQUFDLGFBQWEsQ0FBQzs7Z0JBQzVDLG9CQUFvQixHQUNwQixDQUFDLG1CQUFBLGFBQWEsQ0FBQyxVQUFVLEVBQW1CLENBQUMsQ0FBQyxtQkFBQSxVQUFVLENBQUMsVUFBVSxFQUFVLENBQUM7WUFFbEYsT0FBTyxvQkFBb0IsSUFBSSxVQUFVLEVBQUU7Z0JBQ3pDLE1BQU0sQ0FBQyxJQUFJLENBQUMsZ0JBQWdCLENBQUMsb0JBQW9CLEVBQUUsVUFBVSxDQUFDLENBQUMsQ0FBQztnQkFDaEUsb0JBQW9CLEdBQUcsb0JBQW9CLENBQUMsSUFBSSxDQUFDO2FBQ2xEO1NBQ0Y7UUFDRCxVQUFVLEdBQUcsVUFBVSxDQUFDLElBQUksQ0FBQztLQUM5QjtJQUVELE9BQU8sTUFBTSxDQUFDO0FBQ2hCLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7QXBwbGljYXRpb25SZWZ9IGZyb20gJy4uL2FwcGxpY2F0aW9uX3JlZic7XG5pbXBvcnQge0NoYW5nZURldGVjdG9yUmVmIGFzIHZpZXdFbmdpbmVfQ2hhbmdlRGV0ZWN0b3JSZWZ9IGZyb20gJy4uL2NoYW5nZV9kZXRlY3Rpb24vY2hhbmdlX2RldGVjdG9yX3JlZic7XG5pbXBvcnQge1ZpZXdDb250YWluZXJSZWYgYXMgdmlld0VuZ2luZV9WaWV3Q29udGFpbmVyUmVmfSBmcm9tICcuLi9saW5rZXIvdmlld19jb250YWluZXJfcmVmJztcbmltcG9ydCB7RW1iZWRkZWRWaWV3UmVmIGFzIHZpZXdFbmdpbmVfRW1iZWRkZWRWaWV3UmVmLCBJbnRlcm5hbFZpZXdSZWYgYXMgdmlld0VuZ2luZV9JbnRlcm5hbFZpZXdSZWZ9IGZyb20gJy4uL2xpbmtlci92aWV3X3JlZic7XG5cbmltcG9ydCB7Y2hlY2tOb0NoYW5nZXNJblJvb3RWaWV3LCBjaGVja05vQ2hhbmdlc0ludGVybmFsLCBkZXRlY3RDaGFuZ2VzSW5Sb290VmlldywgZGV0ZWN0Q2hhbmdlc0ludGVybmFsLCBtYXJrVmlld0RpcnR5LCBzdG9yZUNsZWFudXBGbn0gZnJvbSAnLi9pbnN0cnVjdGlvbnMvc2hhcmVkJztcbmltcG9ydCB7VEVsZW1lbnROb2RlLCBUTm9kZSwgVE5vZGVUeXBlLCBUVmlld05vZGV9IGZyb20gJy4vaW50ZXJmYWNlcy9ub2RlJztcbmltcG9ydCB7RkxBR1MsIEhPU1QsIExWaWV3LCBMVmlld0ZsYWdzLCBUX0hPU1R9IGZyb20gJy4vaW50ZXJmYWNlcy92aWV3JztcbmltcG9ydCB7ZGVzdHJveUxWaWV3LCByZW5kZXJEZXRhY2hWaWV3fSBmcm9tICcuL25vZGVfbWFuaXB1bGF0aW9uJztcbmltcG9ydCB7ZmluZENvbXBvbmVudFZpZXcsIGdldExWaWV3UGFyZW50fSBmcm9tICcuL3V0aWwvdmlld190cmF2ZXJzYWxfdXRpbHMnO1xuaW1wb3J0IHtnZXROYXRpdmVCeVROb2RlLCBnZXROYXRpdmVCeVROb2RlT3JOdWxsfSBmcm9tICcuL3V0aWwvdmlld191dGlscyc7XG5cblxuXG4vLyBOZWVkZWQgZHVlIHRvIHRzaWNrbGUgZG93bmxldmVsaW5nIHdoZXJlIG11bHRpcGxlIGBpbXBsZW1lbnRzYCB3aXRoIGNsYXNzZXMgY3JlYXRlc1xuLy8gbXVsdGlwbGUgQGV4dGVuZHMgaW4gQ2xvc3VyZSBhbm5vdGF0aW9ucywgd2hpY2ggaXMgaWxsZWdhbC4gVGhpcyB3b3JrYXJvdW5kIGZpeGVzXG4vLyB0aGUgbXVsdGlwbGUgQGV4dGVuZHMgYnkgbWFraW5nIHRoZSBhbm5vdGF0aW9uIEBpbXBsZW1lbnRzIGluc3RlYWRcbmV4cG9ydCBpbnRlcmZhY2Ugdmlld0VuZ2luZV9DaGFuZ2VEZXRlY3RvclJlZl9pbnRlcmZhY2UgZXh0ZW5kcyB2aWV3RW5naW5lX0NoYW5nZURldGVjdG9yUmVmIHt9XG5cbmV4cG9ydCBjbGFzcyBWaWV3UmVmPFQ+IGltcGxlbWVudHMgdmlld0VuZ2luZV9FbWJlZGRlZFZpZXdSZWY8VD4sIHZpZXdFbmdpbmVfSW50ZXJuYWxWaWV3UmVmLFxuICAgIHZpZXdFbmdpbmVfQ2hhbmdlRGV0ZWN0b3JSZWZfaW50ZXJmYWNlIHtcbiAgcHJpdmF0ZSBfYXBwUmVmOiBBcHBsaWNhdGlvblJlZnxudWxsID0gbnVsbDtcbiAgcHJpdmF0ZSBfdmlld0NvbnRhaW5lclJlZjogdmlld0VuZ2luZV9WaWV3Q29udGFpbmVyUmVmfG51bGwgPSBudWxsO1xuXG4gIC8qKlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIHB1YmxpYyBfdFZpZXdOb2RlOiBUVmlld05vZGV8bnVsbCA9IG51bGw7XG5cbiAgLyoqXG4gICAqIEBpbnRlcm5hbFxuICAgKi9cbiAgcHVibGljIF9sVmlldzogTFZpZXc7XG5cbiAgZ2V0IHJvb3ROb2RlcygpOiBhbnlbXSB7XG4gICAgaWYgKHRoaXMuX2xWaWV3W0hPU1RdID09IG51bGwpIHtcbiAgICAgIGNvbnN0IHRWaWV3ID0gdGhpcy5fbFZpZXdbVF9IT1NUXSBhcyBUVmlld05vZGU7XG4gICAgICByZXR1cm4gY29sbGVjdE5hdGl2ZU5vZGVzKHRoaXMuX2xWaWV3LCB0VmlldywgW10pO1xuICAgIH1cbiAgICByZXR1cm4gW107XG4gIH1cblxuICBjb25zdHJ1Y3RvcihfbFZpZXc6IExWaWV3LCBwcml2YXRlIF9jb250ZXh0OiBUfG51bGwsIHByaXZhdGUgX2NvbXBvbmVudEluZGV4OiBudW1iZXIpIHtcbiAgICB0aGlzLl9sVmlldyA9IF9sVmlldztcbiAgfVxuXG4gIGdldCBjb250ZXh0KCk6IFQgeyByZXR1cm4gdGhpcy5fY29udGV4dCA/IHRoaXMuX2NvbnRleHQgOiB0aGlzLl9sb29rVXBDb250ZXh0KCk7IH1cblxuICBnZXQgZGVzdHJveWVkKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiAodGhpcy5fbFZpZXdbRkxBR1NdICYgTFZpZXdGbGFncy5EZXN0cm95ZWQpID09PSBMVmlld0ZsYWdzLkRlc3Ryb3llZDtcbiAgfVxuXG4gIGRlc3Ryb3koKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuX2FwcFJlZikge1xuICAgICAgdGhpcy5fYXBwUmVmLmRldGFjaFZpZXcodGhpcyk7XG4gICAgfSBlbHNlIGlmICh0aGlzLl92aWV3Q29udGFpbmVyUmVmKSB7XG4gICAgICBjb25zdCBpbmRleCA9IHRoaXMuX3ZpZXdDb250YWluZXJSZWYuaW5kZXhPZih0aGlzKTtcblxuICAgICAgaWYgKGluZGV4ID4gLTEpIHtcbiAgICAgICAgdGhpcy5fdmlld0NvbnRhaW5lclJlZi5kZXRhY2goaW5kZXgpO1xuICAgICAgfVxuXG4gICAgICB0aGlzLl92aWV3Q29udGFpbmVyUmVmID0gbnVsbDtcbiAgICB9XG4gICAgZGVzdHJveUxWaWV3KHRoaXMuX2xWaWV3KTtcbiAgfVxuXG4gIG9uRGVzdHJveShjYWxsYmFjazogRnVuY3Rpb24pIHsgc3RvcmVDbGVhbnVwRm4odGhpcy5fbFZpZXcsIGNhbGxiYWNrKTsgfVxuXG4gIC8qKlxuICAgKiBNYXJrcyBhIHZpZXcgYW5kIGFsbCBvZiBpdHMgYW5jZXN0b3JzIGRpcnR5LlxuICAgKlxuICAgKiBJdCBhbHNvIHRyaWdnZXJzIGNoYW5nZSBkZXRlY3Rpb24gYnkgY2FsbGluZyBgc2NoZWR1bGVUaWNrYCBpbnRlcm5hbGx5LCB3aGljaCBjb2FsZXNjZXNcbiAgICogbXVsdGlwbGUgYG1hcmtGb3JDaGVja2AgY2FsbHMgdG8gaW50byBvbmUgY2hhbmdlIGRldGVjdGlvbiBydW4uXG4gICAqXG4gICAqIFRoaXMgY2FuIGJlIHVzZWQgdG8gZW5zdXJlIGFuIHtAbGluayBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSNPblB1c2ggT25QdXNofSBjb21wb25lbnQgaXNcbiAgICogY2hlY2tlZCB3aGVuIGl0IG5lZWRzIHRvIGJlIHJlLXJlbmRlcmVkIGJ1dCB0aGUgdHdvIG5vcm1hbCB0cmlnZ2VycyBoYXZlbid0IG1hcmtlZCBpdFxuICAgKiBkaXJ0eSAoaS5lLiBpbnB1dHMgaGF2ZW4ndCBjaGFuZ2VkIGFuZCBldmVudHMgaGF2ZW4ndCBmaXJlZCBpbiB0aGUgdmlldykuXG4gICAqXG4gICAqIDwhLS0gVE9ETzogQWRkIGEgbGluayB0byBhIGNoYXB0ZXIgb24gT25QdXNoIGNvbXBvbmVudHMgLS0+XG4gICAqXG4gICAqIEB1c2FnZU5vdGVzXG4gICAqICMjIyBFeGFtcGxlXG4gICAqXG4gICAqIGBgYHR5cGVzY3JpcHRcbiAgICogQENvbXBvbmVudCh7XG4gICAqICAgc2VsZWN0b3I6ICdteS1hcHAnLFxuICAgKiAgIHRlbXBsYXRlOiBgTnVtYmVyIG9mIHRpY2tzOiB7e251bWJlck9mVGlja3N9fWBcbiAgICogICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgICogfSlcbiAgICogY2xhc3MgQXBwQ29tcG9uZW50IHtcbiAgICogICBudW1iZXJPZlRpY2tzID0gMDtcbiAgICpcbiAgICogICBjb25zdHJ1Y3Rvcihwcml2YXRlIHJlZjogQ2hhbmdlRGV0ZWN0b3JSZWYpIHtcbiAgICogICAgIHNldEludGVydmFsKCgpID0+IHtcbiAgICogICAgICAgdGhpcy5udW1iZXJPZlRpY2tzKys7XG4gICAqICAgICAgIC8vIHRoZSBmb2xsb3dpbmcgaXMgcmVxdWlyZWQsIG90aGVyd2lzZSB0aGUgdmlldyB3aWxsIG5vdCBiZSB1cGRhdGVkXG4gICAqICAgICAgIHRoaXMucmVmLm1hcmtGb3JDaGVjaygpO1xuICAgKiAgICAgfSwgMTAwMCk7XG4gICAqICAgfVxuICAgKiB9XG4gICAqIGBgYFxuICAgKi9cbiAgbWFya0ZvckNoZWNrKCk6IHZvaWQgeyBtYXJrVmlld0RpcnR5KHRoaXMuX2xWaWV3KTsgfVxuXG4gIC8qKlxuICAgKiBEZXRhY2hlcyB0aGUgdmlldyBmcm9tIHRoZSBjaGFuZ2UgZGV0ZWN0aW9uIHRyZWUuXG4gICAqXG4gICAqIERldGFjaGVkIHZpZXdzIHdpbGwgbm90IGJlIGNoZWNrZWQgZHVyaW5nIGNoYW5nZSBkZXRlY3Rpb24gcnVucyB1bnRpbCB0aGV5IGFyZVxuICAgKiByZS1hdHRhY2hlZCwgZXZlbiBpZiB0aGV5IGFyZSBkaXJ0eS4gYGRldGFjaGAgY2FuIGJlIHVzZWQgaW4gY29tYmluYXRpb24gd2l0aFxuICAgKiB7QGxpbmsgQ2hhbmdlRGV0ZWN0b3JSZWYjZGV0ZWN0Q2hhbmdlcyBkZXRlY3RDaGFuZ2VzfSB0byBpbXBsZW1lbnQgbG9jYWwgY2hhbmdlXG4gICAqIGRldGVjdGlvbiBjaGVja3MuXG4gICAqXG4gICAqIDwhLS0gVE9ETzogQWRkIGEgbGluayB0byBhIGNoYXB0ZXIgb24gZGV0YWNoL3JlYXR0YWNoL2xvY2FsIGRpZ2VzdCAtLT5cbiAgICogPCEtLSBUT0RPOiBBZGQgYSBsaXZlIGRlbW8gb25jZSByZWYuZGV0ZWN0Q2hhbmdlcyBpcyBtZXJnZWQgaW50byBtYXN0ZXIgLS0+XG4gICAqXG4gICAqIEB1c2FnZU5vdGVzXG4gICAqICMjIyBFeGFtcGxlXG4gICAqXG4gICAqIFRoZSBmb2xsb3dpbmcgZXhhbXBsZSBkZWZpbmVzIGEgY29tcG9uZW50IHdpdGggYSBsYXJnZSBsaXN0IG9mIHJlYWRvbmx5IGRhdGEuXG4gICAqIEltYWdpbmUgdGhlIGRhdGEgY2hhbmdlcyBjb25zdGFudGx5LCBtYW55IHRpbWVzIHBlciBzZWNvbmQuIEZvciBwZXJmb3JtYW5jZSByZWFzb25zLFxuICAgKiB3ZSB3YW50IHRvIGNoZWNrIGFuZCB1cGRhdGUgdGhlIGxpc3QgZXZlcnkgZml2ZSBzZWNvbmRzLiBXZSBjYW4gZG8gdGhhdCBieSBkZXRhY2hpbmdcbiAgICogdGhlIGNvbXBvbmVudCdzIGNoYW5nZSBkZXRlY3RvciBhbmQgZG9pbmcgYSBsb2NhbCBjaGVjayBldmVyeSBmaXZlIHNlY29uZHMuXG4gICAqXG4gICAqIGBgYHR5cGVzY3JpcHRcbiAgICogY2xhc3MgRGF0YVByb3ZpZGVyIHtcbiAgICogICAvLyBpbiBhIHJlYWwgYXBwbGljYXRpb24gdGhlIHJldHVybmVkIGRhdGEgd2lsbCBiZSBkaWZmZXJlbnQgZXZlcnkgdGltZVxuICAgKiAgIGdldCBkYXRhKCkge1xuICAgKiAgICAgcmV0dXJuIFsxLDIsMyw0LDVdO1xuICAgKiAgIH1cbiAgICogfVxuICAgKlxuICAgKiBAQ29tcG9uZW50KHtcbiAgICogICBzZWxlY3RvcjogJ2dpYW50LWxpc3QnLFxuICAgKiAgIHRlbXBsYXRlOiBgXG4gICAqICAgICA8bGkgKm5nRm9yPVwibGV0IGQgb2YgZGF0YVByb3ZpZGVyLmRhdGFcIj5EYXRhIHt7ZH19PC9saT5cbiAgICogICBgLFxuICAgKiB9KVxuICAgKiBjbGFzcyBHaWFudExpc3Qge1xuICAgKiAgIGNvbnN0cnVjdG9yKHByaXZhdGUgcmVmOiBDaGFuZ2VEZXRlY3RvclJlZiwgcHJpdmF0ZSBkYXRhUHJvdmlkZXI6IERhdGFQcm92aWRlcikge1xuICAgKiAgICAgcmVmLmRldGFjaCgpO1xuICAgKiAgICAgc2V0SW50ZXJ2YWwoKCkgPT4ge1xuICAgKiAgICAgICB0aGlzLnJlZi5kZXRlY3RDaGFuZ2VzKCk7XG4gICAqICAgICB9LCA1MDAwKTtcbiAgICogICB9XG4gICAqIH1cbiAgICpcbiAgICogQENvbXBvbmVudCh7XG4gICAqICAgc2VsZWN0b3I6ICdhcHAnLFxuICAgKiAgIHByb3ZpZGVyczogW0RhdGFQcm92aWRlcl0sXG4gICAqICAgdGVtcGxhdGU6IGBcbiAgICogICAgIDxnaWFudC1saXN0PjxnaWFudC1saXN0PlxuICAgKiAgIGAsXG4gICAqIH0pXG4gICAqIGNsYXNzIEFwcCB7XG4gICAqIH1cbiAgICogYGBgXG4gICAqL1xuICBkZXRhY2goKTogdm9pZCB7IHRoaXMuX2xWaWV3W0ZMQUdTXSAmPSB+TFZpZXdGbGFncy5BdHRhY2hlZDsgfVxuXG4gIC8qKlxuICAgKiBSZS1hdHRhY2hlcyBhIHZpZXcgdG8gdGhlIGNoYW5nZSBkZXRlY3Rpb24gdHJlZS5cbiAgICpcbiAgICogVGhpcyBjYW4gYmUgdXNlZCB0byByZS1hdHRhY2ggdmlld3MgdGhhdCB3ZXJlIHByZXZpb3VzbHkgZGV0YWNoZWQgZnJvbSB0aGUgdHJlZVxuICAgKiB1c2luZyB7QGxpbmsgQ2hhbmdlRGV0ZWN0b3JSZWYjZGV0YWNoIGRldGFjaH0uIFZpZXdzIGFyZSBhdHRhY2hlZCB0byB0aGUgdHJlZSBieSBkZWZhdWx0LlxuICAgKlxuICAgKiA8IS0tIFRPRE86IEFkZCBhIGxpbmsgdG8gYSBjaGFwdGVyIG9uIGRldGFjaC9yZWF0dGFjaC9sb2NhbCBkaWdlc3QgLS0+XG4gICAqXG4gICAqIEB1c2FnZU5vdGVzXG4gICAqICMjIyBFeGFtcGxlXG4gICAqXG4gICAqIFRoZSBmb2xsb3dpbmcgZXhhbXBsZSBjcmVhdGVzIGEgY29tcG9uZW50IGRpc3BsYXlpbmcgYGxpdmVgIGRhdGEuIFRoZSBjb21wb25lbnQgd2lsbCBkZXRhY2hcbiAgICogaXRzIGNoYW5nZSBkZXRlY3RvciBmcm9tIHRoZSBtYWluIGNoYW5nZSBkZXRlY3RvciB0cmVlIHdoZW4gdGhlIGNvbXBvbmVudCdzIGxpdmUgcHJvcGVydHlcbiAgICogaXMgc2V0IHRvIGZhbHNlLlxuICAgKlxuICAgKiBgYGB0eXBlc2NyaXB0XG4gICAqIGNsYXNzIERhdGFQcm92aWRlciB7XG4gICAqICAgZGF0YSA9IDE7XG4gICAqXG4gICAqICAgY29uc3RydWN0b3IoKSB7XG4gICAqICAgICBzZXRJbnRlcnZhbCgoKSA9PiB7XG4gICAqICAgICAgIHRoaXMuZGF0YSA9IHRoaXMuZGF0YSAqIDI7XG4gICAqICAgICB9LCA1MDApO1xuICAgKiAgIH1cbiAgICogfVxuICAgKlxuICAgKiBAQ29tcG9uZW50KHtcbiAgICogICBzZWxlY3RvcjogJ2xpdmUtZGF0YScsXG4gICAqICAgaW5wdXRzOiBbJ2xpdmUnXSxcbiAgICogICB0ZW1wbGF0ZTogJ0RhdGE6IHt7ZGF0YVByb3ZpZGVyLmRhdGF9fSdcbiAgICogfSlcbiAgICogY2xhc3MgTGl2ZURhdGEge1xuICAgKiAgIGNvbnN0cnVjdG9yKHByaXZhdGUgcmVmOiBDaGFuZ2VEZXRlY3RvclJlZiwgcHJpdmF0ZSBkYXRhUHJvdmlkZXI6IERhdGFQcm92aWRlcikge31cbiAgICpcbiAgICogICBzZXQgbGl2ZSh2YWx1ZSkge1xuICAgKiAgICAgaWYgKHZhbHVlKSB7XG4gICAqICAgICAgIHRoaXMucmVmLnJlYXR0YWNoKCk7XG4gICAqICAgICB9IGVsc2Uge1xuICAgKiAgICAgICB0aGlzLnJlZi5kZXRhY2goKTtcbiAgICogICAgIH1cbiAgICogICB9XG4gICAqIH1cbiAgICpcbiAgICogQENvbXBvbmVudCh7XG4gICAqICAgc2VsZWN0b3I6ICdteS1hcHAnLFxuICAgKiAgIHByb3ZpZGVyczogW0RhdGFQcm92aWRlcl0sXG4gICAqICAgdGVtcGxhdGU6IGBcbiAgICogICAgIExpdmUgVXBkYXRlOiA8aW5wdXQgdHlwZT1cImNoZWNrYm94XCIgWyhuZ01vZGVsKV09XCJsaXZlXCI+XG4gICAqICAgICA8bGl2ZS1kYXRhIFtsaXZlXT1cImxpdmVcIj48bGl2ZS1kYXRhPlxuICAgKiAgIGAsXG4gICAqIH0pXG4gICAqIGNsYXNzIEFwcENvbXBvbmVudCB7XG4gICAqICAgbGl2ZSA9IHRydWU7XG4gICAqIH1cbiAgICogYGBgXG4gICAqL1xuICByZWF0dGFjaCgpOiB2b2lkIHsgdGhpcy5fbFZpZXdbRkxBR1NdIHw9IExWaWV3RmxhZ3MuQXR0YWNoZWQ7IH1cblxuICAvKipcbiAgICogQ2hlY2tzIHRoZSB2aWV3IGFuZCBpdHMgY2hpbGRyZW4uXG4gICAqXG4gICAqIFRoaXMgY2FuIGFsc28gYmUgdXNlZCBpbiBjb21iaW5hdGlvbiB3aXRoIHtAbGluayBDaGFuZ2VEZXRlY3RvclJlZiNkZXRhY2ggZGV0YWNofSB0byBpbXBsZW1lbnRcbiAgICogbG9jYWwgY2hhbmdlIGRldGVjdGlvbiBjaGVja3MuXG4gICAqXG4gICAqIDwhLS0gVE9ETzogQWRkIGEgbGluayB0byBhIGNoYXB0ZXIgb24gZGV0YWNoL3JlYXR0YWNoL2xvY2FsIGRpZ2VzdCAtLT5cbiAgICogPCEtLSBUT0RPOiBBZGQgYSBsaXZlIGRlbW8gb25jZSByZWYuZGV0ZWN0Q2hhbmdlcyBpcyBtZXJnZWQgaW50byBtYXN0ZXIgLS0+XG4gICAqXG4gICAqIEB1c2FnZU5vdGVzXG4gICAqICMjIyBFeGFtcGxlXG4gICAqXG4gICAqIFRoZSBmb2xsb3dpbmcgZXhhbXBsZSBkZWZpbmVzIGEgY29tcG9uZW50IHdpdGggYSBsYXJnZSBsaXN0IG9mIHJlYWRvbmx5IGRhdGEuXG4gICAqIEltYWdpbmUsIHRoZSBkYXRhIGNoYW5nZXMgY29uc3RhbnRseSwgbWFueSB0aW1lcyBwZXIgc2Vjb25kLiBGb3IgcGVyZm9ybWFuY2UgcmVhc29ucyxcbiAgICogd2Ugd2FudCB0byBjaGVjayBhbmQgdXBkYXRlIHRoZSBsaXN0IGV2ZXJ5IGZpdmUgc2Vjb25kcy5cbiAgICpcbiAgICogV2UgY2FuIGRvIHRoYXQgYnkgZGV0YWNoaW5nIHRoZSBjb21wb25lbnQncyBjaGFuZ2UgZGV0ZWN0b3IgYW5kIGRvaW5nIGEgbG9jYWwgY2hhbmdlIGRldGVjdGlvblxuICAgKiBjaGVjayBldmVyeSBmaXZlIHNlY29uZHMuXG4gICAqXG4gICAqIFNlZSB7QGxpbmsgQ2hhbmdlRGV0ZWN0b3JSZWYjZGV0YWNoIGRldGFjaH0gZm9yIG1vcmUgaW5mb3JtYXRpb24uXG4gICAqL1xuICBkZXRlY3RDaGFuZ2VzKCk6IHZvaWQgeyBkZXRlY3RDaGFuZ2VzSW50ZXJuYWwodGhpcy5fbFZpZXcsIHRoaXMuY29udGV4dCk7IH1cblxuICAvKipcbiAgICogQ2hlY2tzIHRoZSBjaGFuZ2UgZGV0ZWN0b3IgYW5kIGl0cyBjaGlsZHJlbiwgYW5kIHRocm93cyBpZiBhbnkgY2hhbmdlcyBhcmUgZGV0ZWN0ZWQuXG4gICAqXG4gICAqIFRoaXMgaXMgdXNlZCBpbiBkZXZlbG9wbWVudCBtb2RlIHRvIHZlcmlmeSB0aGF0IHJ1bm5pbmcgY2hhbmdlIGRldGVjdGlvbiBkb2Vzbid0XG4gICAqIGludHJvZHVjZSBvdGhlciBjaGFuZ2VzLlxuICAgKi9cbiAgY2hlY2tOb0NoYW5nZXMoKTogdm9pZCB7IGNoZWNrTm9DaGFuZ2VzSW50ZXJuYWwodGhpcy5fbFZpZXcsIHRoaXMuY29udGV4dCk7IH1cblxuICBhdHRhY2hUb1ZpZXdDb250YWluZXJSZWYodmNSZWY6IHZpZXdFbmdpbmVfVmlld0NvbnRhaW5lclJlZikge1xuICAgIGlmICh0aGlzLl9hcHBSZWYpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignVGhpcyB2aWV3IGlzIGFscmVhZHkgYXR0YWNoZWQgZGlyZWN0bHkgdG8gdGhlIEFwcGxpY2F0aW9uUmVmIScpO1xuICAgIH1cbiAgICB0aGlzLl92aWV3Q29udGFpbmVyUmVmID0gdmNSZWY7XG4gIH1cblxuICBkZXRhY2hGcm9tQXBwUmVmKCkge1xuICAgIHRoaXMuX2FwcFJlZiA9IG51bGw7XG4gICAgcmVuZGVyRGV0YWNoVmlldyh0aGlzLl9sVmlldyk7XG4gIH1cblxuICBhdHRhY2hUb0FwcFJlZihhcHBSZWY6IEFwcGxpY2F0aW9uUmVmKSB7XG4gICAgaWYgKHRoaXMuX3ZpZXdDb250YWluZXJSZWYpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignVGhpcyB2aWV3IGlzIGFscmVhZHkgYXR0YWNoZWQgdG8gYSBWaWV3Q29udGFpbmVyIScpO1xuICAgIH1cbiAgICB0aGlzLl9hcHBSZWYgPSBhcHBSZWY7XG4gIH1cblxuICBwcml2YXRlIF9sb29rVXBDb250ZXh0KCk6IFQge1xuICAgIHJldHVybiB0aGlzLl9jb250ZXh0ID0gZ2V0TFZpZXdQYXJlbnQodGhpcy5fbFZpZXcpICFbdGhpcy5fY29tcG9uZW50SW5kZXhdIGFzIFQ7XG4gIH1cbn1cblxuLyoqIEBpbnRlcm5hbCAqL1xuZXhwb3J0IGNsYXNzIFJvb3RWaWV3UmVmPFQ+IGV4dGVuZHMgVmlld1JlZjxUPiB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBfdmlldzogTFZpZXcpIHsgc3VwZXIoX3ZpZXcsIG51bGwsIC0xKTsgfVxuXG4gIGRldGVjdENoYW5nZXMoKTogdm9pZCB7IGRldGVjdENoYW5nZXNJblJvb3RWaWV3KHRoaXMuX3ZpZXcpOyB9XG5cbiAgY2hlY2tOb0NoYW5nZXMoKTogdm9pZCB7IGNoZWNrTm9DaGFuZ2VzSW5Sb290Vmlldyh0aGlzLl92aWV3KTsgfVxuXG4gIGdldCBjb250ZXh0KCk6IFQgeyByZXR1cm4gbnVsbCAhOyB9XG59XG5cbmZ1bmN0aW9uIGNvbGxlY3ROYXRpdmVOb2RlcyhsVmlldzogTFZpZXcsIHBhcmVudFROb2RlOiBUTm9kZSwgcmVzdWx0OiBhbnlbXSk6IGFueVtdIHtcbiAgbGV0IHROb2RlQ2hpbGQgPSBwYXJlbnRUTm9kZS5jaGlsZDtcblxuICB3aGlsZSAodE5vZGVDaGlsZCkge1xuICAgIGNvbnN0IG5hdGl2ZU5vZGUgPSBnZXROYXRpdmVCeVROb2RlT3JOdWxsKHROb2RlQ2hpbGQsIGxWaWV3KTtcbiAgICBuYXRpdmVOb2RlICYmIHJlc3VsdC5wdXNoKG5hdGl2ZU5vZGUpO1xuICAgIGlmICh0Tm9kZUNoaWxkLnR5cGUgPT09IFROb2RlVHlwZS5FbGVtZW50Q29udGFpbmVyKSB7XG4gICAgICBjb2xsZWN0TmF0aXZlTm9kZXMobFZpZXcsIHROb2RlQ2hpbGQsIHJlc3VsdCk7XG4gICAgfSBlbHNlIGlmICh0Tm9kZUNoaWxkLnR5cGUgPT09IFROb2RlVHlwZS5Qcm9qZWN0aW9uKSB7XG4gICAgICBjb25zdCBjb21wb25lbnRWaWV3ID0gZmluZENvbXBvbmVudFZpZXcobFZpZXcpO1xuICAgICAgY29uc3QgY29tcG9uZW50SG9zdCA9IGNvbXBvbmVudFZpZXdbVF9IT1NUXSBhcyBURWxlbWVudE5vZGU7XG4gICAgICBjb25zdCBwYXJlbnRWaWV3ID0gZ2V0TFZpZXdQYXJlbnQoY29tcG9uZW50Vmlldyk7XG4gICAgICBsZXQgY3VycmVudFByb2plY3RlZE5vZGU6IFROb2RlfG51bGwgPVxuICAgICAgICAgIChjb21wb25lbnRIb3N0LnByb2plY3Rpb24gYXMoVE5vZGUgfCBudWxsKVtdKVt0Tm9kZUNoaWxkLnByb2plY3Rpb24gYXMgbnVtYmVyXTtcblxuICAgICAgd2hpbGUgKGN1cnJlbnRQcm9qZWN0ZWROb2RlICYmIHBhcmVudFZpZXcpIHtcbiAgICAgICAgcmVzdWx0LnB1c2goZ2V0TmF0aXZlQnlUTm9kZShjdXJyZW50UHJvamVjdGVkTm9kZSwgcGFyZW50VmlldykpO1xuICAgICAgICBjdXJyZW50UHJvamVjdGVkTm9kZSA9IGN1cnJlbnRQcm9qZWN0ZWROb2RlLm5leHQ7XG4gICAgICB9XG4gICAgfVxuICAgIHROb2RlQ2hpbGQgPSB0Tm9kZUNoaWxkLm5leHQ7XG4gIH1cblxuICByZXR1cm4gcmVzdWx0O1xufVxuIl19
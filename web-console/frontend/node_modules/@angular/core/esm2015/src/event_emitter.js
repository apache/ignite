/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/// <reference types="rxjs" />
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/// <reference types="rxjs" />
import { Subject, Subscription } from 'rxjs';
/**
 * Use in components with the `\@Output` directive to emit custom events
 * synchronously or asynchronously, and register handlers for those events
 * by subscribing to an instance.
 *
 * \@usageNotes
 *
 * Extends
 * [RxJS `Subject`](https://rxjs.dev/api/index/class/Subject)
 * for Angular by adding the `emit()` method.
 *
 * In the following example, a component defines two output properties
 * that create event emitters. When the title is clicked, the emitter
 * emits an open or close event to toggle the current visibility state.
 *
 * ```html
 * \@Component({
 *   selector: 'zippy',
 *   template: `
 *   <div class="zippy">
 *     <div (click)="toggle()">Toggle</div>
 *     <div [hidden]="!visible">
 *       <ng-content></ng-content>
 *     </div>
 *  </div>`})
 * export class Zippy {
 *   visible: boolean = true;
 * \@Output() open: EventEmitter<any> = new EventEmitter();
 * \@Output() close: EventEmitter<any> = new EventEmitter();
 *
 *   toggle() {
 *     this.visible = !this.visible;
 *     if (this.visible) {
 *       this.open.emit(null);
 *     } else {
 *       this.close.emit(null);
 *     }
 *   }
 * }
 * ```
 *
 * Access the event object with the `$event` argument passed to the output event
 * handler:
 *
 * ```html
 * <zippy (open)="onOpen($event)" (close)="onClose($event)"></zippy>
 * ```
 *
 * @see [Observables in Angular](guide/observables-in-angular)
 * \@publicApi
 * @template T
 */
export class EventEmitter extends Subject {
    // tslint:disable-line
    /**
     * Creates an instance of this class that can
     * deliver events synchronously or asynchronously.
     *
     * @param {?=} isAsync When true, deliver events asynchronously.
     *
     */
    constructor(isAsync = false) {
        super();
        this.__isAsync = isAsync;
    }
    /**
     * Emits an event containing a given value.
     * @param {?=} value The value to emit.
     * @return {?}
     */
    emit(value) { super.next(value); }
    /**
     * Registers handlers for events emitted by this instance.
     * @param {?=} generatorOrNext When supplied, a custom handler for emitted events.
     * @param {?=} error When supplied, a custom handler for an error notification
     * from this emitter.
     * @param {?=} complete When supplied, a custom handler for a completion
     * notification from this emitter.
     * @return {?}
     */
    subscribe(generatorOrNext, error, complete) {
        /** @type {?} */
        let schedulerFn;
        /** @type {?} */
        let errorFn = (/**
         * @param {?} err
         * @return {?}
         */
        (err) => null);
        /** @type {?} */
        let completeFn = (/**
         * @return {?}
         */
        () => null);
        if (generatorOrNext && typeof generatorOrNext === 'object') {
            schedulerFn = this.__isAsync ? (/**
             * @param {?} value
             * @return {?}
             */
            (value) => {
                setTimeout((/**
                 * @return {?}
                 */
                () => generatorOrNext.next(value)));
            }) : (/**
             * @param {?} value
             * @return {?}
             */
            (value) => { generatorOrNext.next(value); });
            if (generatorOrNext.error) {
                errorFn = this.__isAsync ? (/**
                 * @param {?} err
                 * @return {?}
                 */
                (err) => { setTimeout((/**
                 * @return {?}
                 */
                () => generatorOrNext.error(err))); }) :
                    (/**
                     * @param {?} err
                     * @return {?}
                     */
                    (err) => { generatorOrNext.error(err); });
            }
            if (generatorOrNext.complete) {
                completeFn = this.__isAsync ? (/**
                 * @return {?}
                 */
                () => { setTimeout((/**
                 * @return {?}
                 */
                () => generatorOrNext.complete())); }) :
                    (/**
                     * @return {?}
                     */
                    () => { generatorOrNext.complete(); });
            }
        }
        else {
            schedulerFn = this.__isAsync ? (/**
             * @param {?} value
             * @return {?}
             */
            (value) => { setTimeout((/**
             * @return {?}
             */
            () => generatorOrNext(value))); }) :
                (/**
                 * @param {?} value
                 * @return {?}
                 */
                (value) => { generatorOrNext(value); });
            if (error) {
                errorFn =
                    this.__isAsync ? (/**
                     * @param {?} err
                     * @return {?}
                     */
                    (err) => { setTimeout((/**
                     * @return {?}
                     */
                    () => error(err))); }) : (/**
                     * @param {?} err
                     * @return {?}
                     */
                    (err) => { error(err); });
            }
            if (complete) {
                completeFn =
                    this.__isAsync ? (/**
                     * @return {?}
                     */
                    () => { setTimeout((/**
                     * @return {?}
                     */
                    () => complete())); }) : (/**
                     * @return {?}
                     */
                    () => { complete(); });
            }
        }
        /** @type {?} */
        const sink = super.subscribe(schedulerFn, errorFn, completeFn);
        if (generatorOrNext instanceof Subscription) {
            generatorOrNext.add(sink);
        }
        return sink;
    }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    EventEmitter.prototype.__isAsync;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXZlbnRfZW1pdHRlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL2V2ZW50X2VtaXR0ZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQVFBLDhCQUE4Qjs7Ozs7Ozs7O0FBRTlCLE9BQU8sRUFBQyxPQUFPLEVBQUUsWUFBWSxFQUFDLE1BQU0sTUFBTSxDQUFDOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQXFEM0MsTUFBTSxPQUFPLFlBQTRCLFNBQVEsT0FBVTs7Ozs7Ozs7O0lBYXpELFlBQVksVUFBbUIsS0FBSztRQUNsQyxLQUFLLEVBQUUsQ0FBQztRQUNSLElBQUksQ0FBQyxTQUFTLEdBQUcsT0FBTyxDQUFDO0lBQzNCLENBQUM7Ozs7OztJQU1ELElBQUksQ0FBQyxLQUFTLElBQUksS0FBSyxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUM7Ozs7Ozs7Ozs7SUFVdEMsU0FBUyxDQUFDLGVBQXFCLEVBQUUsS0FBVyxFQUFFLFFBQWM7O1lBQ3RELFdBQTRCOztZQUM1QixPQUFPOzs7O1FBQUcsQ0FBQyxHQUFRLEVBQU8sRUFBRSxDQUFDLElBQUksQ0FBQTs7WUFDakMsVUFBVTs7O1FBQUcsR0FBUSxFQUFFLENBQUMsSUFBSSxDQUFBO1FBRWhDLElBQUksZUFBZSxJQUFJLE9BQU8sZUFBZSxLQUFLLFFBQVEsRUFBRTtZQUMxRCxXQUFXLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxDQUFDOzs7O1lBQUMsQ0FBQyxLQUFVLEVBQUUsRUFBRTtnQkFDNUMsVUFBVTs7O2dCQUFDLEdBQUcsRUFBRSxDQUFDLGVBQWUsQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLEVBQUMsQ0FBQztZQUNoRCxDQUFDLEVBQUMsQ0FBQzs7OztZQUFDLENBQUMsS0FBVSxFQUFFLEVBQUUsR0FBRyxlQUFlLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFBLENBQUM7WUFFckQsSUFBSSxlQUFlLENBQUMsS0FBSyxFQUFFO2dCQUN6QixPQUFPLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxDQUFDOzs7O2dCQUFDLENBQUMsR0FBRyxFQUFFLEVBQUUsR0FBRyxVQUFVOzs7Z0JBQUMsR0FBRyxFQUFFLENBQUMsZUFBZSxDQUFDLEtBQUssQ0FBQyxHQUFHLENBQUMsRUFBQyxDQUFDLENBQUMsQ0FBQyxFQUFDLENBQUM7Ozs7O29CQUM1RCxDQUFDLEdBQUcsRUFBRSxFQUFFLEdBQUcsZUFBZSxDQUFDLEtBQUssQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQSxDQUFDO2FBQ3JFO1lBRUQsSUFBSSxlQUFlLENBQUMsUUFBUSxFQUFFO2dCQUM1QixVQUFVLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxDQUFDOzs7Z0JBQUMsR0FBRyxFQUFFLEdBQUcsVUFBVTs7O2dCQUFDLEdBQUcsRUFBRSxDQUFDLGVBQWUsQ0FBQyxRQUFRLEVBQUUsRUFBQyxDQUFDLENBQUMsQ0FBQyxFQUFDLENBQUM7Ozs7b0JBQ3pELEdBQUcsRUFBRSxHQUFHLGVBQWUsQ0FBQyxRQUFRLEVBQUUsQ0FBQyxDQUFDLENBQUMsQ0FBQSxDQUFDO2FBQ3JFO1NBQ0Y7YUFBTTtZQUNMLFdBQVcsR0FBRyxJQUFJLENBQUMsU0FBUyxDQUFDLENBQUM7Ozs7WUFBQyxDQUFDLEtBQVUsRUFBRSxFQUFFLEdBQUcsVUFBVTs7O1lBQUMsR0FBRyxFQUFFLENBQUMsZUFBZSxDQUFDLEtBQUssQ0FBQyxFQUFDLENBQUMsQ0FBQyxDQUFDLEVBQUMsQ0FBQzs7Ozs7Z0JBQy9ELENBQUMsS0FBVSxFQUFFLEVBQUUsR0FBRyxlQUFlLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUEsQ0FBQztZQUUzRSxJQUFJLEtBQUssRUFBRTtnQkFDVCxPQUFPO29CQUNILElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQzs7OztvQkFBQyxDQUFDLEdBQUcsRUFBRSxFQUFFLEdBQUcsVUFBVTs7O29CQUFDLEdBQUcsRUFBRSxDQUFDLEtBQUssQ0FBQyxHQUFHLENBQUMsRUFBQyxDQUFDLENBQUMsQ0FBQyxFQUFDLENBQUM7Ozs7b0JBQUMsQ0FBQyxHQUFHLEVBQUUsRUFBRSxHQUFHLEtBQUssQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQSxDQUFDO2FBQzVGO1lBRUQsSUFBSSxRQUFRLEVBQUU7Z0JBQ1osVUFBVTtvQkFDTixJQUFJLENBQUMsU0FBUyxDQUFDLENBQUM7OztvQkFBQyxHQUFHLEVBQUUsR0FBRyxVQUFVOzs7b0JBQUMsR0FBRyxFQUFFLENBQUMsUUFBUSxFQUFFLEVBQUMsQ0FBQyxDQUFDLENBQUMsRUFBQyxDQUFDOzs7b0JBQUMsR0FBRyxFQUFFLEdBQUcsUUFBUSxFQUFFLENBQUMsQ0FBQyxDQUFDLENBQUEsQ0FBQzthQUN0RjtTQUNGOztjQUVLLElBQUksR0FBRyxLQUFLLENBQUMsU0FBUyxDQUFDLFdBQVcsRUFBRSxPQUFPLEVBQUUsVUFBVSxDQUFDO1FBRTlELElBQUksZUFBZSxZQUFZLFlBQVksRUFBRTtZQUMzQyxlQUFlLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQyxDQUFDO1NBQzNCO1FBRUQsT0FBTyxJQUFJLENBQUM7SUFDZCxDQUFDO0NBQ0Y7Ozs7OztJQXRFQyxpQ0FBbUIiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8vLyA8cmVmZXJlbmNlIHR5cGVzPVwicnhqc1wiIC8+XG5cbmltcG9ydCB7U3ViamVjdCwgU3Vic2NyaXB0aW9ufSBmcm9tICdyeGpzJztcblxuLyoqXG4gKiBVc2UgaW4gY29tcG9uZW50cyB3aXRoIHRoZSBgQE91dHB1dGAgZGlyZWN0aXZlIHRvIGVtaXQgY3VzdG9tIGV2ZW50c1xuICogc3luY2hyb25vdXNseSBvciBhc3luY2hyb25vdXNseSwgYW5kIHJlZ2lzdGVyIGhhbmRsZXJzIGZvciB0aG9zZSBldmVudHNcbiAqIGJ5IHN1YnNjcmliaW5nIHRvIGFuIGluc3RhbmNlLlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKlxuICogRXh0ZW5kc1xuICogW1J4SlMgYFN1YmplY3RgXShodHRwczovL3J4anMuZGV2L2FwaS9pbmRleC9jbGFzcy9TdWJqZWN0KVxuICogZm9yIEFuZ3VsYXIgYnkgYWRkaW5nIHRoZSBgZW1pdCgpYCBtZXRob2QuXG4gKlxuICogSW4gdGhlIGZvbGxvd2luZyBleGFtcGxlLCBhIGNvbXBvbmVudCBkZWZpbmVzIHR3byBvdXRwdXQgcHJvcGVydGllc1xuICogdGhhdCBjcmVhdGUgZXZlbnQgZW1pdHRlcnMuIFdoZW4gdGhlIHRpdGxlIGlzIGNsaWNrZWQsIHRoZSBlbWl0dGVyXG4gKiBlbWl0cyBhbiBvcGVuIG9yIGNsb3NlIGV2ZW50IHRvIHRvZ2dsZSB0aGUgY3VycmVudCB2aXNpYmlsaXR5IHN0YXRlLlxuICpcbiAqIGBgYGh0bWxcbiAqIEBDb21wb25lbnQoe1xuICogICBzZWxlY3RvcjogJ3ppcHB5JyxcbiAqICAgdGVtcGxhdGU6IGBcbiAqICAgPGRpdiBjbGFzcz1cInppcHB5XCI+XG4gKiAgICAgPGRpdiAoY2xpY2spPVwidG9nZ2xlKClcIj5Ub2dnbGU8L2Rpdj5cbiAqICAgICA8ZGl2IFtoaWRkZW5dPVwiIXZpc2libGVcIj5cbiAqICAgICAgIDxuZy1jb250ZW50PjwvbmctY29udGVudD5cbiAqICAgICA8L2Rpdj5cbiAqICA8L2Rpdj5gfSlcbiAqIGV4cG9ydCBjbGFzcyBaaXBweSB7XG4gKiAgIHZpc2libGU6IGJvb2xlYW4gPSB0cnVlO1xuICogICBAT3V0cHV0KCkgb3BlbjogRXZlbnRFbWl0dGVyPGFueT4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gKiAgIEBPdXRwdXQoKSBjbG9zZTogRXZlbnRFbWl0dGVyPGFueT4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gKlxuICogICB0b2dnbGUoKSB7XG4gKiAgICAgdGhpcy52aXNpYmxlID0gIXRoaXMudmlzaWJsZTtcbiAqICAgICBpZiAodGhpcy52aXNpYmxlKSB7XG4gKiAgICAgICB0aGlzLm9wZW4uZW1pdChudWxsKTtcbiAqICAgICB9IGVsc2Uge1xuICogICAgICAgdGhpcy5jbG9zZS5lbWl0KG51bGwpO1xuICogICAgIH1cbiAqICAgfVxuICogfVxuICogYGBgXG4gKlxuICogQWNjZXNzIHRoZSBldmVudCBvYmplY3Qgd2l0aCB0aGUgYCRldmVudGAgYXJndW1lbnQgcGFzc2VkIHRvIHRoZSBvdXRwdXQgZXZlbnRcbiAqIGhhbmRsZXI6XG4gKlxuICogYGBgaHRtbFxuICogPHppcHB5IChvcGVuKT1cIm9uT3BlbigkZXZlbnQpXCIgKGNsb3NlKT1cIm9uQ2xvc2UoJGV2ZW50KVwiPjwvemlwcHk+XG4gKiBgYGBcbiAqXG4gKiBAc2VlIFtPYnNlcnZhYmxlcyBpbiBBbmd1bGFyXShndWlkZS9vYnNlcnZhYmxlcy1pbi1hbmd1bGFyKVxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgRXZlbnRFbWl0dGVyPFQgZXh0ZW5kcyBhbnk+IGV4dGVuZHMgU3ViamVjdDxUPiB7XG4gIC8qKlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIF9faXNBc3luYzogYm9vbGVhbjsgIC8vIHRzbGludDpkaXNhYmxlLWxpbmVcblxuICAvKipcbiAgICogQ3JlYXRlcyBhbiBpbnN0YW5jZSBvZiB0aGlzIGNsYXNzIHRoYXQgY2FuXG4gICAqIGRlbGl2ZXIgZXZlbnRzIHN5bmNocm9ub3VzbHkgb3IgYXN5bmNocm9ub3VzbHkuXG4gICAqXG4gICAqIEBwYXJhbSBpc0FzeW5jIFdoZW4gdHJ1ZSwgZGVsaXZlciBldmVudHMgYXN5bmNocm9ub3VzbHkuXG4gICAqXG4gICAqL1xuICBjb25zdHJ1Y3Rvcihpc0FzeW5jOiBib29sZWFuID0gZmFsc2UpIHtcbiAgICBzdXBlcigpO1xuICAgIHRoaXMuX19pc0FzeW5jID0gaXNBc3luYztcbiAgfVxuXG4gIC8qKlxuICAgKiBFbWl0cyBhbiBldmVudCBjb250YWluaW5nIGEgZ2l2ZW4gdmFsdWUuXG4gICAqIEBwYXJhbSB2YWx1ZSBUaGUgdmFsdWUgdG8gZW1pdC5cbiAgICovXG4gIGVtaXQodmFsdWU/OiBUKSB7IHN1cGVyLm5leHQodmFsdWUpOyB9XG5cbiAgLyoqXG4gICAqIFJlZ2lzdGVycyBoYW5kbGVycyBmb3IgZXZlbnRzIGVtaXR0ZWQgYnkgdGhpcyBpbnN0YW5jZS5cbiAgICogQHBhcmFtIGdlbmVyYXRvck9yTmV4dCBXaGVuIHN1cHBsaWVkLCBhIGN1c3RvbSBoYW5kbGVyIGZvciBlbWl0dGVkIGV2ZW50cy5cbiAgICogQHBhcmFtIGVycm9yIFdoZW4gc3VwcGxpZWQsIGEgY3VzdG9tIGhhbmRsZXIgZm9yIGFuIGVycm9yIG5vdGlmaWNhdGlvblxuICAgKiBmcm9tIHRoaXMgZW1pdHRlci5cbiAgICogQHBhcmFtIGNvbXBsZXRlIFdoZW4gc3VwcGxpZWQsIGEgY3VzdG9tIGhhbmRsZXIgZm9yIGEgY29tcGxldGlvblxuICAgKiBub3RpZmljYXRpb24gZnJvbSB0aGlzIGVtaXR0ZXIuXG4gICAqL1xuICBzdWJzY3JpYmUoZ2VuZXJhdG9yT3JOZXh0PzogYW55LCBlcnJvcj86IGFueSwgY29tcGxldGU/OiBhbnkpOiBTdWJzY3JpcHRpb24ge1xuICAgIGxldCBzY2hlZHVsZXJGbjogKHQ6IGFueSkgPT4gYW55O1xuICAgIGxldCBlcnJvckZuID0gKGVycjogYW55KTogYW55ID0+IG51bGw7XG4gICAgbGV0IGNvbXBsZXRlRm4gPSAoKTogYW55ID0+IG51bGw7XG5cbiAgICBpZiAoZ2VuZXJhdG9yT3JOZXh0ICYmIHR5cGVvZiBnZW5lcmF0b3JPck5leHQgPT09ICdvYmplY3QnKSB7XG4gICAgICBzY2hlZHVsZXJGbiA9IHRoaXMuX19pc0FzeW5jID8gKHZhbHVlOiBhbnkpID0+IHtcbiAgICAgICAgc2V0VGltZW91dCgoKSA9PiBnZW5lcmF0b3JPck5leHQubmV4dCh2YWx1ZSkpO1xuICAgICAgfSA6ICh2YWx1ZTogYW55KSA9PiB7IGdlbmVyYXRvck9yTmV4dC5uZXh0KHZhbHVlKTsgfTtcblxuICAgICAgaWYgKGdlbmVyYXRvck9yTmV4dC5lcnJvcikge1xuICAgICAgICBlcnJvckZuID0gdGhpcy5fX2lzQXN5bmMgPyAoZXJyKSA9PiB7IHNldFRpbWVvdXQoKCkgPT4gZ2VuZXJhdG9yT3JOZXh0LmVycm9yKGVycikpOyB9IDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgKGVycikgPT4geyBnZW5lcmF0b3JPck5leHQuZXJyb3IoZXJyKTsgfTtcbiAgICAgIH1cblxuICAgICAgaWYgKGdlbmVyYXRvck9yTmV4dC5jb21wbGV0ZSkge1xuICAgICAgICBjb21wbGV0ZUZuID0gdGhpcy5fX2lzQXN5bmMgPyAoKSA9PiB7IHNldFRpbWVvdXQoKCkgPT4gZ2VuZXJhdG9yT3JOZXh0LmNvbXBsZXRlKCkpOyB9IDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgKCkgPT4geyBnZW5lcmF0b3JPck5leHQuY29tcGxldGUoKTsgfTtcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgc2NoZWR1bGVyRm4gPSB0aGlzLl9faXNBc3luYyA/ICh2YWx1ZTogYW55KSA9PiB7IHNldFRpbWVvdXQoKCkgPT4gZ2VuZXJhdG9yT3JOZXh0KHZhbHVlKSk7IH0gOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICh2YWx1ZTogYW55KSA9PiB7IGdlbmVyYXRvck9yTmV4dCh2YWx1ZSk7IH07XG5cbiAgICAgIGlmIChlcnJvcikge1xuICAgICAgICBlcnJvckZuID1cbiAgICAgICAgICAgIHRoaXMuX19pc0FzeW5jID8gKGVycikgPT4geyBzZXRUaW1lb3V0KCgpID0+IGVycm9yKGVycikpOyB9IDogKGVycikgPT4geyBlcnJvcihlcnIpOyB9O1xuICAgICAgfVxuXG4gICAgICBpZiAoY29tcGxldGUpIHtcbiAgICAgICAgY29tcGxldGVGbiA9XG4gICAgICAgICAgICB0aGlzLl9faXNBc3luYyA/ICgpID0+IHsgc2V0VGltZW91dCgoKSA9PiBjb21wbGV0ZSgpKTsgfSA6ICgpID0+IHsgY29tcGxldGUoKTsgfTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBzaW5rID0gc3VwZXIuc3Vic2NyaWJlKHNjaGVkdWxlckZuLCBlcnJvckZuLCBjb21wbGV0ZUZuKTtcblxuICAgIGlmIChnZW5lcmF0b3JPck5leHQgaW5zdGFuY2VvZiBTdWJzY3JpcHRpb24pIHtcbiAgICAgIGdlbmVyYXRvck9yTmV4dC5hZGQoc2luayk7XG4gICAgfVxuXG4gICAgcmV0dXJuIHNpbms7XG4gIH1cbn1cbiJdfQ==
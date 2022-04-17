/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { providersResolver } from '../di_setup';
/**
 * This feature resolves the providers of a directive (or component),
 * and publish them into the DI system, making it visible to others for injection.
 *
 * For example:
 * ```ts
 * class ComponentWithProviders {
 *   constructor(private greeter: GreeterDE) {}
 *
 *   static ngComponentDef = defineComponent({
 *     type: ComponentWithProviders,
 *     selectors: [['component-with-providers']],
 *    factory: () => new ComponentWithProviders(directiveInject(GreeterDE as any)),
 *    consts: 1,
 *    vars: 1,
 *    template: function(fs: RenderFlags, ctx: ComponentWithProviders) {
 *      if (fs & RenderFlags.Create) {
 *        ɵɵtext(0);
 *      }
 *      if (fs & RenderFlags.Update) {
 *        ɵɵselect(0);
 *        ɵɵtextBinding(ctx.greeter.greet());
 *      }
 *    },
 *    features: [ProvidersFeature([GreeterDE])]
 *  });
 * }
 * ```
 *
 * \@codeGenApi
 * @template T
 * @param {?} providers
 * @param {?=} viewProviders
 * @return {?}
 */
export function ɵɵProvidersFeature(providers, viewProviders = []) {
    return (/**
     * @param {?} definition
     * @return {?}
     */
    (definition) => {
        definition.providersResolver =
            (/**
             * @param {?} def
             * @param {?=} processProvidersFn
             * @return {?}
             */
            (def, processProvidersFn) => {
                return providersResolver(def, //
                processProvidersFn ? processProvidersFn(providers) : providers, //
                viewProviders);
            });
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicHJvdmlkZXJzX2ZlYXR1cmUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL2ZlYXR1cmVzL3Byb3ZpZGVyc19mZWF0dXJlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFRQSxPQUFPLEVBQUMsaUJBQWlCLEVBQUMsTUFBTSxhQUFhLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQW9DOUMsTUFBTSxVQUFVLGtCQUFrQixDQUFJLFNBQXFCLEVBQUUsZ0JBQTRCLEVBQUU7SUFDekY7Ozs7SUFBTyxDQUFDLFVBQTJCLEVBQUUsRUFBRTtRQUNyQyxVQUFVLENBQUMsaUJBQWlCOzs7Ozs7WUFDeEIsQ0FBQyxHQUFvQixFQUFFLGtCQUE2QyxFQUFFLEVBQUU7Z0JBQ3RFLE9BQU8saUJBQWlCLENBQ3BCLEdBQUcsRUFBOEQsRUFBRTtnQkFDbkUsa0JBQWtCLENBQUMsQ0FBQyxDQUFDLGtCQUFrQixDQUFDLFNBQVMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxTQUFTLEVBQUcsRUFBRTtnQkFDbkUsYUFBYSxDQUFDLENBQUM7WUFDckIsQ0FBQyxDQUFBLENBQUM7SUFDUixDQUFDLEVBQUM7QUFDSixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuaW1wb3J0IHtQcm9jZXNzUHJvdmlkZXJzRnVuY3Rpb24sIFByb3ZpZGVyfSBmcm9tICcuLi8uLi9kaS9pbnRlcmZhY2UvcHJvdmlkZXInO1xuaW1wb3J0IHtwcm92aWRlcnNSZXNvbHZlcn0gZnJvbSAnLi4vZGlfc2V0dXAnO1xuaW1wb3J0IHtEaXJlY3RpdmVEZWZ9IGZyb20gJy4uL2ludGVyZmFjZXMvZGVmaW5pdGlvbic7XG5cbi8qKlxuICogVGhpcyBmZWF0dXJlIHJlc29sdmVzIHRoZSBwcm92aWRlcnMgb2YgYSBkaXJlY3RpdmUgKG9yIGNvbXBvbmVudCksXG4gKiBhbmQgcHVibGlzaCB0aGVtIGludG8gdGhlIERJIHN5c3RlbSwgbWFraW5nIGl0IHZpc2libGUgdG8gb3RoZXJzIGZvciBpbmplY3Rpb24uXG4gKlxuICogRm9yIGV4YW1wbGU6XG4gKiBgYGB0c1xuICogY2xhc3MgQ29tcG9uZW50V2l0aFByb3ZpZGVycyB7XG4gKiAgIGNvbnN0cnVjdG9yKHByaXZhdGUgZ3JlZXRlcjogR3JlZXRlckRFKSB7fVxuICpcbiAqICAgc3RhdGljIG5nQ29tcG9uZW50RGVmID0gZGVmaW5lQ29tcG9uZW50KHtcbiAqICAgICB0eXBlOiBDb21wb25lbnRXaXRoUHJvdmlkZXJzLFxuICogICAgIHNlbGVjdG9yczogW1snY29tcG9uZW50LXdpdGgtcHJvdmlkZXJzJ11dLFxuICogICAgZmFjdG9yeTogKCkgPT4gbmV3IENvbXBvbmVudFdpdGhQcm92aWRlcnMoZGlyZWN0aXZlSW5qZWN0KEdyZWV0ZXJERSBhcyBhbnkpKSxcbiAqICAgIGNvbnN0czogMSxcbiAqICAgIHZhcnM6IDEsXG4gKiAgICB0ZW1wbGF0ZTogZnVuY3Rpb24oZnM6IFJlbmRlckZsYWdzLCBjdHg6IENvbXBvbmVudFdpdGhQcm92aWRlcnMpIHtcbiAqICAgICAgaWYgKGZzICYgUmVuZGVyRmxhZ3MuQ3JlYXRlKSB7XG4gKiAgICAgICAgybXJtXRleHQoMCk7XG4gKiAgICAgIH1cbiAqICAgICAgaWYgKGZzICYgUmVuZGVyRmxhZ3MuVXBkYXRlKSB7XG4gKiAgICAgICAgybXJtXNlbGVjdCgwKTtcbiAqICAgICAgICDJtcm1dGV4dEJpbmRpbmcoY3R4LmdyZWV0ZXIuZ3JlZXQoKSk7XG4gKiAgICAgIH1cbiAqICAgIH0sXG4gKiAgICBmZWF0dXJlczogW1Byb3ZpZGVyc0ZlYXR1cmUoW0dyZWV0ZXJERV0pXVxuICogIH0pO1xuICogfVxuICogYGBgXG4gKlxuICogQHBhcmFtIGRlZmluaXRpb25cbiAqXG4gKiBAY29kZUdlbkFwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gybXJtVByb3ZpZGVyc0ZlYXR1cmU8VD4ocHJvdmlkZXJzOiBQcm92aWRlcltdLCB2aWV3UHJvdmlkZXJzOiBQcm92aWRlcltdID0gW10pIHtcbiAgcmV0dXJuIChkZWZpbml0aW9uOiBEaXJlY3RpdmVEZWY8VD4pID0+IHtcbiAgICBkZWZpbml0aW9uLnByb3ZpZGVyc1Jlc29sdmVyID1cbiAgICAgICAgKGRlZjogRGlyZWN0aXZlRGVmPFQ+LCBwcm9jZXNzUHJvdmlkZXJzRm4/OiBQcm9jZXNzUHJvdmlkZXJzRnVuY3Rpb24pID0+IHtcbiAgICAgICAgICByZXR1cm4gcHJvdmlkZXJzUmVzb2x2ZXIoXG4gICAgICAgICAgICAgIGRlZiwgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLy9cbiAgICAgICAgICAgICAgcHJvY2Vzc1Byb3ZpZGVyc0ZuID8gcHJvY2Vzc1Byb3ZpZGVyc0ZuKHByb3ZpZGVycykgOiBwcm92aWRlcnMsICAvL1xuICAgICAgICAgICAgICB2aWV3UHJvdmlkZXJzKTtcbiAgICAgICAgfTtcbiAgfTtcbn1cbiJdfQ==


export default class UserNotificationsController {
    static $inject = ['deferred', 'message', 'visible'];

    constructor(deferred, message, visible) {
        this.deferred = deferred;
        this.message = message;
        this.visible = visible;
    }

    onLoad(editor) {
        editor.setHighlightActiveLine(false);
        editor.setAutoScrollEditorIntoView(true);
        editor.$blockScrolling = Infinity;

        const renderer = editor.renderer;

        renderer.setPadding(7);
        renderer.setScrollMargin(7, 12);
        renderer.setHighlightGutterLine(false);
        renderer.setShowPrintMargin(false);
        renderer.setShowGutter(false);
        renderer.setOption('fontFamily', 'monospace');
        renderer.setOption('fontSize', '14px');
        renderer.setOption('minLines', '3');
        renderer.setOption('maxLines', '3');

        editor.focus();
    }

    submit() {
        this.deferred.resolve({message: this.message, visible: this.visible});
    }
}

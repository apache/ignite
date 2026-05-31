

import ResizeObserver from 'resize-observer-polyfill';

export default class WebCOnsoleSidebarOverflow {
    static $inject = ['$element', 'gridUtil', '$window'];

    constructor(private el: JQLite, private gridUtil: {getScrollbarWidth(): number}, private $win: ng.IWindowService) {}

    scrollEl!: JQLite;

    resizeObserver: ResizeObserver;

    $onInit() {
        this.el.css('--scrollbar-width', `${this.gridUtil.getScrollbarWidth()}px`);
    }

    $postLink() {
        this.scrollEl[0].addEventListener('scroll', this.onScroll, {passive: true});
        this.resizeObserver = new ResizeObserver(() => this.applyStyles(this.scrollEl[0]));
        this.resizeObserver.observe(this.el[0]);
    }
    $onDestroy() {
        this.scrollEl[0].removeEventListener('scroll', this.onScroll);
        this.resizeObserver.disconnect();
    }
    applyStyles(target: HTMLElement) {
        const {offsetHeight, scrollTop, scrollHeight} = target;
        const top = scrollTop !== 0;
        const bottom = Math.floor((offsetHeight + scrollTop)) !== Math.floor(scrollHeight);

        target.classList.toggle('top', top);
        target.classList.toggle('bottom', bottom);
    }
    onScroll = (e: UIEvent) => {
        this.$win.requestAnimationFrame(() => {
            this.applyStyles(e.target as HTMLElement);
        });
    }
}

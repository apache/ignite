const rightNav = document.querySelector('.right-nav .sectlevel1')

if (IntersectionObserver && rightNav) {
    const tocAnchors = [...rightNav.querySelectorAll('a[href]')]
    let last;
    tocAnchors.forEach((a, i, all) => {
        const target = document.querySelector(`${a.hash}`)
        if (!target) return
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                a.classList.toggle('visible', entry.isIntersecting)
                if (entry.isIntersecting) last = a

                const firstVisible = rightNav.querySelector('.visible')
                tocAnchors.forEach(a => a.classList.remove('active'))
                if (firstVisible) firstVisible.classList.add('active')
                else if (last) last.classList.add('active')
            })
        });
        observer.observe(target)
    })
}
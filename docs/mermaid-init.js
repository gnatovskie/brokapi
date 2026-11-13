// Draws the ```mermaid blocks in the colors of the book theme (brokapi, design variant A: dark "coal" by default,
// light as a variant). The library itself, mermaid.min.js (2.7 MB), lies next to this file: no request to
// other sites. It is fetched only on a page that has a diagram (the address is in theme/index.hbs, #mermaid-lib): the
// other pages of the book do not pay for it.
// `mdbook-mermaid install docs` rewrites this file with its stock version: after running it, restore this one
// from git (git checkout docs/mermaid-init.js).
(() => {
    if (!document.querySelector('pre.mermaid')) return;
    const lib = document.getElementById('mermaid-lib');
    if (!lib || !lib.dataset.src) return;
    const isDark = !document.documentElement.classList.contains('light');
    // Palette of the design "Specification": white page, ink-blue lines of the boxes, blue wires, the signal only for the
    // points that mean something (notes, activations). Dark: the same tones on the cold dark page.
    const light = {
        background: '#fbfbfa', primaryColor: '#ffffff', primaryTextColor: '#0d0f12', primaryBorderColor: '#0d0f12',
        secondaryColor: '#f3f4f5', tertiaryColor: '#ffffff', lineColor: '#2d4ee0', textColor: '#0d0f12',
        mainBkg: '#ffffff', nodeBorder: '#0d0f12', clusterBkg: '#f3f4f5', clusterBorder: '#d3d7dc',
        edgeLabelBackground: '#fbfbfa', titleColor: '#0d0f12', noteBkgColor: '#f3f4f5', noteTextColor: '#0d0f12',
        nodeTextColor: '#0d0f12', stateLabelColor: '#0d0f12', noteBorderColor: '#646b75', actorBkg: '#ffffff', actorBorder: '#0d0f12', actorTextColor: '#0d0f12',
        actorLineColor: '#868d98', signalColor: '#2d4ee0', signalTextColor: '#0d0f12', labelBoxBkgColor: '#f3f4f5',
        labelBoxBorderColor: '#646b75', labelTextColor: '#0d0f12', loopTextColor: '#0d0f12',
        activationBkgColor: '#dfe5ff', activationBorderColor: '#2d4ee0', sequenceNumberColor: '#ffffff',
        stateBkg: '#ffffff', altBackground: '#f3f4f5', compositeBackground: '#f3f4f5', labelColor: '#0d0f12',
        transitionColor: '#2d4ee0', transitionLabelColor: '#0d0f12', specialStateColor: '#ff6b2c',
    };
    const dark = {
        background: '#0e0f12', primaryColor: '#14161a', primaryTextColor: '#eceef1', primaryBorderColor: '#3a4049',
        secondaryColor: '#1a1d22', tertiaryColor: '#101215', lineColor: '#8fa8ff', textColor: '#d5d8dd',
        mainBkg: '#14161a', nodeBorder: '#3a4049', clusterBkg: '#0b0c0e', clusterBorder: '#2a2f36',
        edgeLabelBackground: '#0e0f12', titleColor: '#eceef1', noteBkgColor: '#1a1d22', noteTextColor: '#eceef1',
        nodeTextColor: '#eceef1', stateLabelColor: '#eceef1', noteBorderColor: '#5b636e', actorBkg: '#14161a', actorBorder: '#3a4049', actorTextColor: '#eceef1',
        actorLineColor: '#5b636e', signalColor: '#8fa8ff', signalTextColor: '#d5d8dd', labelBoxBkgColor: '#14161a',
        labelBoxBorderColor: '#3a4049', labelTextColor: '#eceef1', loopTextColor: '#d5d8dd',
        activationBkgColor: '#26346b', activationBorderColor: '#8fa8ff', sequenceNumberColor: '#0a0b0d',
        stateBkg: '#14161a', altBackground: '#0b0c0e', compositeBackground: '#0b0c0e', labelColor: '#eceef1',
        transitionColor: '#8fa8ff', transitionLabelColor: '#d5d8dd', specialStateColor: '#ff6b2c',
    };
    const setup = () => mermaid.initialize({
        startOnLoad: false,
        theme: 'base',
        themeVariables: Object.assign({ fontFamily: "'Geist', system-ui, sans-serif", fontSize: '14px' }, isDark ? dark : light),
    });
    // The library is fetched while the fonts load: the two waits go side by side.
    const library = () => new Promise((resolve, reject) => {
        const tag = document.createElement('script');
        tag.src = lib.dataset.src;
        tag.onload = resolve;
        tag.onerror = () => reject(new Error('mermaid.min.js did not load'));
        document.head.appendChild(tag);
    });

    // Wide diagrams: squeezed to the page they become unreadable on a phone (text around 7 px). If the natural width
    // is more than 1.4 of the column, draw at 80% of the natural width and let the block scroll sideways.
    const widen = () => {
        for (const svg of document.querySelectorAll('pre.mermaid svg')) {
            const box = svg.parentElement;
            const natural = parseFloat(svg.style.maxWidth) || (svg.viewBox && svg.viewBox.baseVal.width) || 0;
            if (!natural || natural <= box.clientWidth * 1.4 || box.dataset.wide) continue;
            box.dataset.wide = '1';
            svg.style.maxWidth = 'none';
            svg.style.width = Math.round(natural * 0.8) + 'px';
            box.style.overflowX = 'auto';
            const hint = document.createElement('div');
            hint.className = 'mermaid-scroll-hint';
            hint.textContent = 'Схема шире экрана: листайте вбок';
            box.insertAdjacentElement('afterend', hint);
            // Open at the left edge: a diagram is read from the left, and the hint under it says there is more to the right.
            // (Opened at the middle it was cut off on both sides and nothing said so.)
            box.scrollLeft = 0;
            box.classList.add('is-wide');
        }
    };
    // Mermaid sizes every box by measuring its label at drawing time. Drawn before Geist has loaded, the
    // boxes fit the fallback font, then the wider web font clips the last letters. So wait for the fonts first
    // (with a time limit, so a font that never loads does not keep the diagrams from drawing).
    const fontsReady = () => {
        if (!document.fonts) return Promise.resolve();
        // The font is split by alphabet (unicode-range): ask for Cyrillic and Latin text, or only one file loads.
        const loads = ["14px 'Geist'", "600 14px 'Geist'"]
            .map((f) => document.fonts.load(f, 'Аа Яя Aa Zz').catch(() => {}));
        const limit = new Promise((resolve) => setTimeout(resolve, 6000));
        return Promise.race([Promise.all(loads).then(() => document.fonts.ready), limit]);
    };
    const draw = () => Promise.all([library(), fontsReady()])
        .then(() => { setup(); return mermaid.run({ querySelector: 'pre.mermaid' }); })
        .then(widen)
        .catch(() => { /* the diagrams stay as text of the source: the page reads without them */ });
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', draw); else draw();

    // A change of the theme draws the diagrams again: the simplest way is to reload the page.
    for (const id of ['coal', 'light']) {
        const button = document.getElementById('mdbook-theme-' + id);
        if (!button) continue;
        button.addEventListener('click', () => {
            if ((id === 'light') === isDark) window.location.reload();
        });
    }
})();

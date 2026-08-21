/* Site helpers: a tiny syntax highlighter, the pattern-book tabs, docs sidebar
   tracking, and heading anchors. Deliberately dependency-free — the site is
   plain static files. */

'use strict';

/* ---------- minimal syntax highlighting ---------- */

// Per-language keyword sets; unknown languages fall back to Go's.
const KEYWORDS_BY_LANG = {
  go: 'func|return|if|else|for|range|var|const|type|struct|interface|package|' +
      'import|go|defer|switch|case|default|break|continue|map|chan|nil|true|false|' +
      'error|string|int|int64|int32|byte|bool|make|len|append|panic|recover',
  js: 'async|await|function|return|if|else|for|while|const|let|var|new|throw|try|' +
      'catch|finally|import|from|export|class|extends|typeof|null|undefined|true|false',
  py: 'def|return|if|elif|else|for|while|in|not|and|or|import|from|as|class|raise|' +
      'try|except|finally|with|pass|lambda|None|True|False|self|yield',
  json: 'true|false|null',
};

function escapeHTML(s) {
  return s.replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
}

// One pass: find comments / strings first (they win), then keywords and numbers
// inside whatever is left. Everything is escaped as it is emitted.
function highlight(src, lang) {
  const kw = KEYWORDS_BY_LANG[lang] || KEYWORDS_BY_LANG.go;
  const hashComments = lang === 'sh' || lang === 'py';
  const outer = new RegExp(
    (hashComments ? '(#[^\\n]*)|' : '(\\/\\/[^\\n]*)|') +
    '(`[^`]*`|"(?:[^"\\\\]|\\\\.)*"|\'(?:[^\'\\\\]|\\\\.)*\')', 'g');

  let html = '';
  let last = 0;
  let m;
  while ((m = outer.exec(src)) !== null) {
    html += inner(src.slice(last, m.index));
    const cls = m[1] ? 'tok-com' : 'tok-str';
    html += `<span class="${cls}">${escapeHTML(m[0])}</span>`;
    last = m.index + m[0].length;
  }
  html += inner(src.slice(last));
  return html;

  function inner(text) {
    let out = escapeHTML(text);
    if (lang !== 'sh') {
      out = out.replace(new RegExp(`\\b(${kw})\\b`, 'g'), '<span class="tok-kw">$1</span>');
      out = out.replace(/\b(\d+(?:\.\d+)?)\b/g, '<span class="tok-num">$1</span>');
    }
    return out;
  }
}

function applyHighlighting() {
  for (const pre of document.querySelectorAll('pre[data-lang]')) {
    const code = pre.querySelector('code') || pre;
    code.innerHTML = highlight(code.textContent, pre.dataset.lang);
  }
}

/* ---------- pattern book tabs ---------- */

function initPatternTabs() {
  const tablist = document.querySelector('.ptabs');
  if (!tablist) return;
  const tabs = [...tablist.querySelectorAll('[role="tab"]')];

  function select(tab) {
    for (const t of tabs) {
      const selected = t === tab;
      t.setAttribute('aria-selected', String(selected));
      t.tabIndex = selected ? 0 : -1;
      document.getElementById(t.getAttribute('aria-controls')).hidden = !selected;
    }
    tab.focus();
  }

  tabs.forEach((tab, i) => {
    tab.addEventListener('click', () => select(tab));
    tab.addEventListener('keydown', (e) => {
      const dir = { ArrowRight: 1, ArrowLeft: -1 }[e.key];
      if (dir) { e.preventDefault(); select(tabs[(i + dir + tabs.length) % tabs.length]); }
      else if (e.key === 'Home') { e.preventDefault(); select(tabs[0]); }
      else if (e.key === 'End') { e.preventDefault(); select(tabs[tabs.length - 1]); }
    });
  });
}

/* ---------- docs: anchors + active sidebar link ---------- */

function addHeadingAnchors() {
  for (const h of document.querySelectorAll('.content h2[id], .content h3[id]')) {
    const a = document.createElement('a');
    a.href = `#${h.id}`;
    a.className = 'anchor';
    a.textContent = '#';
    a.setAttribute('aria-label', `Link to ${h.textContent}`);
    h.appendChild(a);
  }
}

function trackActiveSection() {
  const links = [...document.querySelectorAll('.sidebar a[href^="#"]')];
  if (!links.length) return;

  const byId = new Map(links.map((a) => [a.getAttribute('href').slice(1), a]));
  const targets = [...byId.keys()]
    .map((id) => document.getElementById(id))
    .filter(Boolean);

  const observer = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      if (!entry.isIntersecting) continue;
      links.forEach((a) => a.classList.remove('active'));
      byId.get(entry.target.id)?.classList.add('active');
    }
  }, { rootMargin: '-80px 0px -70% 0px', threshold: 0 });

  targets.forEach((t) => observer.observe(t));
}

document.addEventListener('DOMContentLoaded', () => {
  applyHighlighting();
  initPatternTabs();
  addHeadingAnchors();
  trackActiveSection();
});

/* Site helpers: a tiny syntax highlighter, docs sidebar tracking, and heading
   anchors. Deliberately dependency-free — the site is plain static files. */

'use strict';

/* ---------- minimal syntax highlighting ---------- */

const KEYWORDS = new RegExp(
  '\\b(func|return|if|else|for|range|var|const|type|struct|interface|package|' +
  'import|go|defer|switch|case|default|break|continue|map|chan|nil|true|false|' +
  'error|string|int|int64|int32|byte|bool|make|len|append|panic|recover)\\b', 'g');

function escapeHTML(s) {
  return s.replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
}

// One pass: find comments / strings first (they win), then keywords and numbers
// inside whatever is left. Everything is escaped as it is emitted.
function highlight(src, lang) {
  const outer = lang === 'sh'
    ? /(#[^\n]*)|('[^']*'|"[^"]*")/g
    : /(\/\/[^\n]*)|(`[^`]*`|"(?:[^"\\]|\\.)*")/g;

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
      out = out.replace(KEYWORDS, '<span class="tok-kw">$1</span>');
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
  addHeadingAnchors();
  trackActiveSection();
});

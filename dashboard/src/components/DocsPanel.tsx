import { useState, useEffect, useRef } from 'react';
import { marked } from 'marked';
import mermaid from 'mermaid';
import diagramsRaw  from '../docs/diagrams.md?raw';
import readmeRaw    from '../../../README.md?raw';
import docsRaw      from '../../../docs/raft-go-documentation.md?raw';

// ── Mermaid dark theme matching the dashboard palette ────────────────────────

mermaid.initialize({
  startOnLoad: false,
  theme: 'dark',
  themeVariables: {
    darkMode:              true,
    background:            '#0d1117',
    mainBkg:               '#161b22',
    nodeBorder:            '#30363d',
    clusterBkg:            '#1c2128',
    titleColor:            '#e6edf3',
    edgeLabelBackground:   '#161b22',
    primaryColor:          '#1c2128',
    primaryTextColor:      '#e6edf3',
    primaryBorderColor:    '#6e40c9',
    lineColor:             '#58a6ff',
    secondaryColor:        '#161b22',
    tertiaryColor:         '#0d1117',
    fontFamily:            'Inter, system-ui, sans-serif',
    fontSize:              '13px',
    // Sequence diagrams
    actorBkg:              '#1c2128',
    actorBorder:           '#6e40c9',
    actorTextColor:        '#e6edf3',
    actorLineColor:        '#30363d',
    signalColor:           '#58a6ff',
    signalTextColor:       '#e6edf3',
    labelBoxBkgColor:      '#161b22',
    labelBoxBorderColor:   '#30363d',
    labelTextColor:        '#e6edf3',
    loopTextColor:         '#e6edf3',
    noteBorderColor:       '#6e40c9',
    noteBkgColor:          '#1c2128',
    noteTextColor:         '#e6edf3',
    activationBorderColor: '#6e40c9',
    activationBkgColor:    '#1c2128',
    sequenceNumberColor:   '#7d8590',
  },
  sequence: { mirrorActors: false, actorMargin: 60, messageMargin: 40 },
  flowchart: { htmlLabels: true, curve: 'basis' },
});

// ── Marked renderer — intercept ```mermaid blocks ────────────────────────────

marked.use({
  renderer: {
    code(token) {
      if (token.lang === 'mermaid') {
        return `<div class="mermaid-block" data-def="${encodeURIComponent(token.text)}"></div>`;
      }
      // other fences: fall back to default
      return `<pre><code class="hlcode lang-${token.lang ?? ''}">${
        token.text.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
      }</code></pre>`;
    },
  },
});

// ── Markdown renderer with Mermaid ───────────────────────────────────────────

function MarkdownDoc({ content }: { content: string }) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    // Set innerHTML directly so React never touches it again on re-renders.
    // dangerouslySetInnerHTML would reset innerHTML on every parent re-render
    // (App polls every 2s), wiping the Mermaid SVGs we inject below.
    el.innerHTML = marked.parse(content) as string;

    let alive = true;
    const blocks = el.querySelectorAll<HTMLElement>('.mermaid-block');
    blocks.forEach(async (block) => {
      const def = decodeURIComponent(block.dataset.def ?? '');
      if (!def) return;
      const id = `mr${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
      try {
        const { svg } = await mermaid.render(id, def);
        if (!alive) return;
        block.innerHTML = svg;
        block.classList.add('mermaid-done');
      } catch (err) {
        if (!alive) return;
        block.innerHTML = `<pre style="color:var(--offline);font-size:11px">${String(err)}</pre>`;
        block.classList.add('mermaid-done');
      }
    });

    return () => { alive = false; };
  }, [content]); // content is a static import — stable; re-runs only on tab switch

  return <div ref={ref} className="docs-content" />;
}

// ── DocsPanel ─────────────────────────────────────────────────────────────────

type DocTab = 'diagrams' | 'readme' | 'fulldocs';

const DOC_TABS: { id: DocTab; label: string }[] = [
  { id: 'diagrams', label: 'Diagrams'  },
  { id: 'readme',   label: 'README'    },
  { id: 'fulldocs', label: 'Full Docs' },
];

export default function DocsPanel() {
  const [tab, setTab] = useState<DocTab>('diagrams');

  return (
    <div>
      <div className="doc-subtabs">
        {DOC_TABS.map(t => (
          <button
            key={t.id}
            className={`doc-subtab-btn${tab === t.id ? ' active' : ''}`}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'diagrams' && <MarkdownDoc content={diagramsRaw} />}
      {tab === 'readme'   && <MarkdownDoc content={readmeRaw} />}
      {tab === 'fulldocs' && <MarkdownDoc content={docsRaw} />}
    </div>
  );
}

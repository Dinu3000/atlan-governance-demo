def mermaid(edges, highlight=None):
    highlight = set([h.upper() for h in (highlight or [])])
    lines = ['```mermaid','graph LR']
    for u,v in edges:
        lines.append(f'  "{u}" --> "{v}"')
    for h in highlight:
        lines.append(f'  style "{h}" fill:#fdd,stroke:#f66,stroke-width:2px')
    lines.append('```'); return '\n'.join(lines)

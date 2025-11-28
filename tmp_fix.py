from pathlib import Path
path = Path('docs/niagara_copilot_edge_snapshot_2025-11-27.md')
text = path.read_text(encoding='utf-8')
replacements = {
    '±': '+/-',
    '→': '->',
    '–': '-',
    '“': '"',
    '”': '"',
    '\u009c': ' ',
    '\u009d': ' ',
    '\x1a': ' ',
    '±': '+/-',
    'ã': 'a',
    'á': 'a',
    'í': 'i',
    'é': 'e',
    'ó': 'o',
    'ú': 'u',
    'ñ': 'n',
    'ï': 'i',
}
for old, new in replacements.items():
    text = text.replace(old, new)
path.write_text(text, encoding='utf-8')

"""Embed shared mobile assets so cPanel uploads remain self-contained."""
from pathlib import Path
import re

BASE = Path(__file__).resolve().parent.parent

def replace_block(text, label, content, before=None):
    start, end = f'<!-- NP_MOBILE_{label}_START -->', f'<!-- NP_MOBILE_{label}_END -->'
    block = f'{start}\n{content}\n{end}'
    pattern = re.escape(start) + r'[\s\S]*?' + re.escape(end)
    if re.search(pattern, text):
        return re.sub(pattern, lambda _: block, text)
    if before:
        return text.replace(before, block + '\n' + before, 1)
    return text.rstrip() + '\n\n' + block + '\n'

def build():
    css = (BASE / 'tools/mobile-panels.css').read_text()
    js = (BASE / 'tools/mobile-panels.js').read_text()
    for name in ('admin_clean.html', 'admin_clean_cpanel.html', 'trader_clean.html'):
        path = BASE / 'website-downloads' / name
        text = path.read_text()
        text = text.replace('width=device-width, initial-scale=1.0',
                            'width=device-width, initial-scale=1.0, viewport-fit=cover') if 'viewport-fit=cover' not in text else text
        text = replace_block(text, 'STYLE', '<style id="np-mobile-panels">\n' + css + '\n</style>', '</head>')
        text = replace_block(text, 'SCRIPT', '<script>\n' + js + '\n</script>')
        path.write_text(text)
        print('Updated', name)

if __name__ == '__main__':
    build()

"""One shared landing bundle for hosted delivery and the local recovery fallback."""
import json
from pathlib import Path

def build_landing_bundle(assets):
    assets=Path(assets)
    css=(assets/'landing-luxury.css').read_text()
    return ((assets/'account-plan-selector.js').read_text()+'\nwindow.NPLuxuryThemeCSS='+json.dumps(css,ensure_ascii=False)+';\n'+
        (assets/'landing-luxury.js').read_text()+'\n'+(assets/'landing-catalogue-sync.js').read_text())

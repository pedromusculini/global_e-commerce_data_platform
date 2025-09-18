"""Export the comparative report markdown to HTML and PDF.

PDF export uses optional dependencies. Strategy:
1. Convert Markdown -> HTML (Python markdown lib, fallback basic converter if missing)
2. Inline a simple CSS with brand colors
3. For PDF: try weasyprint if available; otherwise optionally fallback to reportlab minimal text export.

Usage:
  python scripts/export_report.py --html --pdf
  python scripts/export_report.py --html-only
  python scripts/export_report.py --pdf-only
"""
from __future__ import annotations
from pathlib import Path
import argparse, sys, subprocess, shutil

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORT_MD = PROJECT_ROOT / 'reports' / 'comparative_report.md'
REPORT_HTML = PROJECT_ROOT / 'reports' / 'comparative_report.html'
REPORT_PDF = PROJECT_ROOT / 'reports' / 'comparative_report.pdf'
BRAND_LOGO = PROJECT_ROOT / 'brand' / 'png' / 'icon_cart_growth_default_256.png'

CSS = """
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Cantarell, 'Helvetica Neue', Arial, sans-serif; margin: 2rem; line-height:1.5; }
h1,h2,h3 { font-weight:600; }
code, pre { background:#f5f5f7; padding:2px 4px; border-radius:4px; font-size:90%; }
table { border-collapse: collapse; margin:1rem 0; width:100%; }
th, td { border:1px solid #ddd; padding:6px 8px; font-size: 0.9rem; }
th { background:#0f172a; color:#fff; }
img { max-width:100%; }
.header-brand { display:flex; justify-content: space-between; align-items:center; }
.badge { background:#2563eb; color:#fff; padding:4px 8px; border-radius:16px; font-size:0.75rem; letter-spacing:0.05em; }
.footer { margin-top:3rem; font-size:0.75rem; color:#555; text-align:center; }
"""

def load_markdown() -> str:
    if not REPORT_MD.exists():
        print('Markdown report not found:', REPORT_MD, file=sys.stderr)
        sys.exit(1)
    return REPORT_MD.read_text(encoding='utf-8')


def md_to_html(md_text: str) -> str:
    try:
        import markdown
        html_body = markdown.markdown(md_text, extensions=['tables', 'fenced_code'])
    except Exception:
        # Fallback extremely simple converter
        lines = []
        for line in md_text.splitlines():
            if line.startswith('#'):
                level = len(line) - len(line.lstrip('#'))
                content = line.lstrip('#').strip()
                lines.append(f'<h{level}>{content}</h{level}>')
            else:
                lines.append(f'<p>{line}</p>')
        html_body = '\n'.join(lines)
    logo_tag = ''
    if BRAND_LOGO.exists():
        rel_logo = BRAND_LOGO.relative_to(PROJECT_ROOT).as_posix()
        logo_tag = f"<div class='header-brand'><div><span class='badge'>DATA REPORT</span></div><div><img src='../{rel_logo}' alt='Logo' style='height:72px;'></div></div>"
    html = f"""<!DOCTYPE html><html><head><meta charset='utf-8'><title>Comparative Report</title><style>{CSS}</style></head><body>{logo_tag}{html_body}<div class='footer'>Generated from markdown. Brand integrated.</div></body></html>"""
    return html


def write_html(html: str):
    REPORT_HTML.write_text(html, encoding='utf-8')
    print('HTML written to', REPORT_HTML)


def html_to_pdf():
    # Try weasyprint first
    try:
        import weasyprint  # type: ignore
        weasyprint.HTML(filename=str(REPORT_HTML)).write_pdf(str(REPORT_PDF))
        print('PDF written to', REPORT_PDF)
        return
    except Exception as e:
        print('WeasyPrint not available or failed:', e)
    # Try fallback: pandoc if installed
    if shutil.which('pandoc'):
        cmd = ['pandoc', str(REPORT_HTML), '-o', str(REPORT_PDF)]
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode == 0:
            print('PDF written via pandoc to', REPORT_PDF)
            return
        else:
            print('Pandoc failed:', res.stderr)
    # Minimal fallback: create plain-text PDF using reportlab (if available)
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
        from textwrap import wrap
        text_content = REPORT_MD.read_text(encoding='utf-8')
        c = canvas.Canvas(str(REPORT_PDF), pagesize=A4)
        width, height = A4
        y = height - 50
        for line in text_content.splitlines():
            for sub in wrap(line, 100):
                c.drawString(40, y, sub)
                y -= 14
                if y < 60:
                    c.showPage(); y = height - 50
        c.save()
        print('PDF written (fallback plain text) to', REPORT_PDF)
    except Exception as e:
        print('Could not generate PDF (all methods failed):', e, file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description='Export comparative report to HTML and/or PDF.')
    parser.add_argument('--html', action='store_true', help='Generate HTML (alias for no flag).')
    parser.add_argument('--pdf', action='store_true', help='Generate PDF (needs weasyprint or pandoc or reportlab).')
    parser.add_argument('--html-only', action='store_true', help='Only HTML export.')
    parser.add_argument('--pdf-only', action='store_true', help='Only PDF export (implies HTML first).')
    args = parser.parse_args()

    do_html = args.html or args.html_only or (not args.pdf_only and not args.pdf)
    do_pdf = args.pdf or args.pdf_only

    md_text = load_markdown()
    if do_html or do_pdf:
        html = md_to_html(md_text)
        if do_html or args.pdf_only:
            write_html(html)
    if do_pdf:
        if not REPORT_HTML.exists():  # safety
            write_html(md_to_html(md_text))
        html_to_pdf()

if __name__ == '__main__':
    main()

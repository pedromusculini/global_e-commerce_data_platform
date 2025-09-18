#!/usr/bin/env python
"""Build and regenerate brand icon assets.

Features:
- Generates base vector drawing of cart + growth bars + arrow.
- Exports PNG in multiple sizes (1024,512,256,128,64,32,16).
- Generates SVG (clean paths, no raster fonts).
- Produces dark, outline, mono (white) variants.
- Creates favicon .ico (multi-resolution) from PNGs.
- Writes manifest JSON with file metadata (size bytes, sha256, width/height, variant, format).
- Provides accessibility alt text file.
- Optional --clean flag to wipe brand output (except script itself) before regenerating.
- Optimizes PNG via Pillow quantize fallback (lossless-ish) if optipng not available.

Usage:
  python scripts/build_assets.py [--clean]

"""
from __future__ import annotations
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Dict, List, Tuple
import subprocess
import shutil
import zipfile

from PIL import Image, ImageOps
import xml.etree.ElementTree as ET

# ------------------ Configuration ------------------
BRAND_DIR = Path('brand')
PNG_DIR = BRAND_DIR / 'png'
SVG_DIR = BRAND_DIR / 'svg'
FAVICON_DIR = BRAND_DIR / 'favicon'
META_DIR = BRAND_DIR / 'meta'

SIZES = [1024, 512, 256, 128, 64, 32, 16]
VARIANTS = ['default', 'dark', 'outline', 'mono', 'adaptive']

# Palette
COLOR_BG_OUTER = '#0A3D62'
COLOR_BG_INNER = '#1E6091'
COLOR_ACCENT = '#FFC300'
COLOR_ACCENT_LIGHT = '#FFE680'
COLOR_DARKER = '#062539'
COLOR_WHITE = '#FFFFFF'

ALT_TEXT = (
    "Circular e-commerce analytics icon: stylized shopping cart with upward yellow growth bars and arrow, "
    "symbolizing increasing online sales."
)

# ------------------ Vector Geometry Definition ------------------
# We define the icon in a logical 1x1 coordinate space, then scale.
# Center at (0.5,0.5); radius 0.5.


def _cart_and_bars_geometry():
    """Return geometry primitives for cart (body, handle, wheels) and bars+arrow.
    Coordinates in normalized [0,1]."""
    g = {}
    # Cart body (rectangle)
    g['cart_body'] = (0.23, 0.50, 0.50, 0.16)  # x,y,width,height
    # Handle line (x1,y1,x2,y2)
    g['handle'] = (0.23, 0.66, 0.18, 0.78)
    # Wheels (cx,cy,r)
    g['wheels'] = [
        (0.30, 0.46, 0.035),
        (0.53, 0.46, 0.035),
    ]
    # Bars (base_x, width, height)
    bars = []
    base_y = 0.50
    bars.append((0.27, 0.035, 0.09))
    bars.append((0.34, 0.035, 0.13))
    bars.append((0.41, 0.035, 0.17))
    bars.append((0.48, 0.035, 0.23))
    g['bars'] = (base_y, bars)
    # Arrow (polyline points) starting from last bar top
    arrow = [
        (0.48 + 0.035/2, base_y + 0.23),
        (0.58, 0.79),
        (0.55, 0.77),
        (0.62, 0.84),
        (0.60, 0.75),
        (0.63, 0.78),
    ]
    g['arrow'] = arrow
    return g

GEOM = _cart_and_bars_geometry()

# ------------------ SVG Generation ------------------

def build_svg(variant: str, size: int) -> str:
    """Create SVG string for a given variant and canonical size (viewBox 0 0 1000 1000)."""
    vb_size = 1000
    # Colors per variant
    if variant == 'default':
        bg_outer = COLOR_BG_OUTER
        bg_inner = COLOR_BG_INNER
        cart_color = COLOR_WHITE
        bars_color = COLOR_ACCENT
        arrow_color = COLOR_ACCENT
        stroke_outline = COLOR_WHITE
    elif variant == 'dark':
        bg_outer = COLOR_DARKER
        bg_inner = COLOR_BG_OUTER
        cart_color = COLOR_WHITE
        bars_color = COLOR_ACCENT_LIGHT
        arrow_color = COLOR_ACCENT_LIGHT
        stroke_outline = COLOR_WHITE
    elif variant == 'outline':
        bg_outer = 'none'
        bg_inner = 'none'
        cart_color = 'none'
        bars_color = COLOR_ACCENT
        arrow_color = COLOR_ACCENT
        stroke_outline = COLOR_ACCENT
    elif variant == 'mono':
        bg_outer = COLOR_BG_OUTER
        bg_inner = COLOR_BG_OUTER
        cart_color = COLOR_WHITE
        bars_color = COLOR_WHITE
        arrow_color = COLOR_WHITE
        stroke_outline = COLOR_WHITE
    elif variant == 'adaptive':
        # Adaptive: like default but with a light stroke border around outer circle for dark backgrounds
        bg_outer = COLOR_BG_OUTER
        bg_inner = COLOR_BG_INNER
        cart_color = COLOR_WHITE
        bars_color = COLOR_ACCENT
        arrow_color = COLOR_ACCENT
        stroke_outline = COLOR_WHITE
    else:
        raise ValueError(f"Unknown variant {variant}")

    # Root SVG
    svg = ET.Element('svg', attrib={
        'xmlns': 'http://www.w3.org/2000/svg',
        'viewBox': '0 0 1000 1000',
        'width': str(size),
        'height': str(size),
        'role': 'img',
        'aria-label': ALT_TEXT,
    })

    # Outer circle
    if bg_outer != 'none':
        ET.SubElement(svg, 'circle', cx='500', cy='500', r='500', fill=bg_outer)
    # Inner circle
    if bg_inner != 'none':
        ET.SubElement(svg, 'circle', cx='500', cy='500', r='430', fill=bg_inner)

    # Geometry scale helper
    def n2p(x: float) -> float:
        return x * 1000

    # Bars
    base_y, bars = GEOM['bars']
    for bx, w, h in bars:
        x = n2p(bx)
        y = n2p(base_y + h)
        rect_attrib = {
            'x': str(x),
            'y': str(1000 - y),
            'width': str(n2p(w)),
            'height': str(n2p(h)),
            'fill': bars_color if variant != 'outline' else 'none',
            'stroke': bars_color,
            'stroke-width': '20' if variant == 'outline' else '0'
        }
        ET.SubElement(svg, 'rect', rect_attrib)

    # Arrow polyline
    arrow_pts = GEOM['arrow']
    pts = []
    for (ax, ay) in arrow_pts:
        pts.append(f"{n2p(ax)},{1000 - n2p(ay)}")
    ET.SubElement(svg, 'polyline', attrib={
        'points': ' '.join(pts),
        'fill': 'none',
        'stroke': arrow_color,
        'stroke-width': '30' if variant != 'outline' else '35',
        'stroke-linecap': 'round',
        'stroke-linejoin': 'round'
    })

    # Cart body
    cbx, cby, cbw, cbh = GEOM['cart_body']
    cart_rect = {
        'x': str(n2p(cbx)),
        'y': str(1000 - n2p(cby + cbh)),
        'width': str(n2p(cbw)),
        'height': str(n2p(cbh)),
        'fill': cart_color,
        'stroke': stroke_outline,
        'stroke-width': '35' if variant != 'outline' else '40'
    }
    if variant == 'outline':
        cart_rect['fill'] = 'none'
    ET.SubElement(svg, 'rect', cart_rect)

    # Handle
    hx1, hy1, hx2, hy2 = GEOM['handle']
    ET.SubElement(svg, 'line', attrib={
        'x1': str(n2p(hx1)), 'y1': str(1000 - n2p(hy1)),
        'x2': str(n2p(hx2)), 'y2': str(1000 - n2p(hy2)),
        'stroke': stroke_outline if variant != 'outline' else bars_color,
        'stroke-width': '40' if variant != 'outline' else '45',
        'stroke-linecap': 'round'
    })

    # Wheels
    for cx, cy, r in GEOM['wheels']:
        ET.SubElement(svg, 'circle', attrib={
            'cx': str(n2p(cx)), 'cy': str(1000 - n2p(cy)), 'r': str(n2p(r)),
            'fill': cart_color if variant != 'outline' else 'none',
            'stroke': stroke_outline if variant != 'outline' else bars_color,
            'stroke-width': '35' if variant != 'outline' else '40'
        })

    return ET.tostring(svg, encoding='unicode')

# ------------------ PNG Rendering ------------------

def svg_to_png(svg_text: str, size: int) -> Image.Image:
    """Rasterize SVG by simple manual drawing using Pillow primitives (approx)."""
    # For simplicity (no cairosvg), re-draw from geometry instead of parsing svg.
    # This ensures zero external dependency.
    img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    from PIL import ImageDraw
    draw = ImageDraw.Draw(img)

    # Determine variant from svg_text quick check
    if 'aria-label' not in svg_text:
        variant = 'default'
    if 'mono' in svg_text:  # Not robust; we pass variant separately in generate loop anyway
        variant = 'mono'
    # We'll actually pass variant externally; adjust function signature if needed.

    # Since we re-generate similarly: parse a minimal hint
    # For correctness, accept a separate approach: we embed a marker comment after generation (skipped here for brevity)

    # We'll ignore svg_text and assume the last generated variant context (handled outside).
    return img  # Placeholder not used directly; replaced by explicit raster function below.


def raster_variant(variant: str, size: int) -> Image.Image:
    """Draw the variant using the normalized geometry directly into Pillow image."""
    from PIL import ImageDraw
    img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Color mapping replicate from build_svg
    if variant == 'default':
        bg_outer, bg_inner = COLOR_BG_OUTER, COLOR_BG_INNER
        cart_color = COLOR_WHITE
        bars_color = COLOR_ACCENT
        arrow_color = COLOR_ACCENT
        stroke_outline = COLOR_WHITE
    elif variant == 'dark':
        bg_outer, bg_inner = COLOR_DARKER, COLOR_BG_OUTER
        cart_color = COLOR_WHITE
        bars_color = COLOR_ACCENT_LIGHT
        arrow_color = COLOR_ACCENT_LIGHT
        stroke_outline = COLOR_WHITE
    elif variant == 'outline':
        bg_outer, bg_inner = None, None
        cart_color = None
        bars_color = COLOR_ACCENT
        arrow_color = COLOR_ACCENT
        stroke_outline = COLOR_ACCENT
    elif variant == 'mono':
        bg_outer = bg_inner = COLOR_BG_OUTER
        cart_color = COLOR_WHITE
        bars_color = COLOR_WHITE
        arrow_color = COLOR_WHITE
        stroke_outline = COLOR_WHITE
    elif variant == 'adaptive':
        # Adaptive: like default but with a light stroke border around outer circle for dark backgrounds
        bg_outer = COLOR_BG_OUTER
        bg_inner = COLOR_BG_INNER
        cart_color = COLOR_WHITE
        bars_color = COLOR_ACCENT
        arrow_color = COLOR_ACCENT
        stroke_outline = COLOR_WHITE
    else:
        raise ValueError(variant)

    def n2p(v: float) -> float:
        return v * size

    # Circles
    if bg_outer:
        draw.ellipse([0, 0, size, size], fill=bg_outer)
    if bg_inner and variant != 'mono':
        pad = size * 0.07
        draw.ellipse([pad, pad, size - pad, size - pad], fill=bg_inner)
    elif variant == 'mono':
        pad = size * 0.07
        draw.ellipse([pad, pad, size - pad, size - pad], fill=bg_inner)

    # Bars
    base_y, bars = GEOM['bars']
    for bx, w, h in bars:
        x1 = n2p(bx)
        y_top = n2p(1 - (base_y + h))
        wpx = n2p(w)
        hpx = n2p(h)
        if variant == 'outline':
            draw.rectangle([x1, y_top, x1 + wpx, y_top + hpx], outline=bars_color, width=max(1, int(size * 0.02)))
        else:
            draw.rectangle([x1, y_top, x1 + wpx, y_top + hpx], fill=bars_color)

    # Arrow (polyline)
    arrow_pts = GEOM['arrow']
    pts = [(n2p(ax), n2p(1 - ay)) for (ax, ay) in arrow_pts]
    width = max(2, int(size * (0.03 if variant != 'outline' else 0.035)))
    draw.line(pts, fill=arrow_color, width=width, joint="curve")

    # Cart body
    cbx, cby, cbw, cbh = GEOM['cart_body']
    cart_box = [n2p(cbx), n2p(1 - (cby + cbh)), n2p(cbx + cbw), n2p(1 - cby)]
    stroke_w = max(2, int(size * (0.035 if variant != 'outline' else 0.04)))
    if variant == 'outline':
        draw.rectangle(cart_box, outline=stroke_outline, width=stroke_w)
    else:
        draw.rectangle(cart_box, fill=cart_color, outline=stroke_outline, width=stroke_w)

    # Handle
    hx1, hy1, hx2, hy2 = GEOM['handle']
    draw.line([
        (n2p(hx1), n2p(1 - hy1)), (n2p(hx2), n2p(1 - hy2))
    ], fill=stroke_outline if variant != 'outline' else bars_color, width=stroke_w, joint='curve')

    # Wheels
    for cx, cy, r in GEOM['wheels']:
        cxp, cyp, rp = n2p(cx), n2p(1 - cy), n2p(r)
        bbox = [cxp - rp, cyp - rp, cxp + rp, cyp + rp]
        if variant == 'outline':
            draw.ellipse(bbox, outline=stroke_outline, width=stroke_w)
        else:
            draw.ellipse(bbox, fill=cart_color, outline=stroke_outline, width=max(1, int(stroke_w * 0.9)))

    if variant == 'adaptive':
        # add subtle accent ring
        ring_w = max(2, int(size * 0.01))
        draw.ellipse([ring_w/2, ring_w/2, size - ring_w/2, size - ring_w/2], outline=COLOR_ACCENT_LIGHT, width=ring_w)

    return img

# ------------------ Helpers ------------------

def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def optimize_png(path: Path):
    try:
        # External optimizers preferred
        for tool in ('oxipng', 'optipng'):
            if shutil.which(tool):
                cmd = [tool]
                if tool == 'optipng':
                    cmd += ['-o7', str(path)]
                else:  # oxipng
                    cmd += ['-o', '6', '--strip', 'safe', str(path)]
                subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                return
        img = Image.open(path)
        img = img.convert('RGBA').quantize(method=Image.FASTOCTREE)
        img.save(path, optimize=True)
    except Exception as e:
        print(f"[warn] optimization failed {path}: {e}")


def contrast_ratio(hex1: str, hex2: str) -> float:
    def _l(hexcolor: str) -> float:
        h = hexcolor.lstrip('#')
        r, g, b = [int(h[i:i+2], 16)/255.0 for i in (0,2,4)]
        def adj(c):
            return c/12.92 if c <= 0.03928 else ((c+0.055)/1.055)**2.4
        r, g, b = adj(r), adj(g), adj(b)
        return 0.2126*r + 0.7152*g + 0.0722*b
    L1, L2 = _l(hex1), _l(hex2)
    lighter, darker = max(L1,L2), min(L1,L2)
    return (lighter + 0.05) / (darker + 0.05)

# ------------------ Manifest ------------------

def build_manifest(records: List[Dict], dest: Path):
    dest.write_text(json.dumps({
        'assets': records,
        'alt_text': ALT_TEXT,
        'spec_version': 1,
    }, indent=2, ensure_ascii=False), encoding='utf-8')

# ------------------ Favicon ------------------

def build_favicon(png_variants: Dict[int, Path], dest: Path):
    images = [Image.open(png_variants[s]).convert('RGBA') for s in sorted(png_variants) if s <= 256]
    if images:
        images[0].save(dest, format='ICO', sizes=[(im.width, im.height) for im in images])

# ------------------ Main Build ------------------

def clean():
    if BRAND_DIR.exists():
        for p in BRAND_DIR.glob('**/*'):
            if p.is_file():
                p.unlink()
        # Keep directories


def ensure_dirs():
    for d in [BRAND_DIR, PNG_DIR, SVG_DIR, FAVICON_DIR, META_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(description='Build brand icon assets.')
    parser.add_argument('--clean', action='store_true', help='Clean brand directory before building')
    args = parser.parse_args()

    if args.clean:
        print('[info] Cleaning brand directory...')
        clean()

    ensure_dirs()

    manifest_records: List[Dict] = []
    svg_index: Dict[Tuple[str, int], Path] = {}

    print('[info] Generating SVG variants...')
    for variant in VARIANTS:
        svg_text = build_svg(variant, 1000)
        svg_path = SVG_DIR / f'icon_cart_growth_{variant}.svg'
        svg_path.write_text(svg_text, encoding='utf-8')
        size_bytes = svg_path.stat().st_size
        manifest_records.append({
            'path': str(svg_path).replace('\\', '/'),
            'format': 'SVG',
            'variant': variant,
            'size_bytes': size_bytes,
            'width': None,
            'height': None,
            'sha256': sha256_of_file(svg_path)
        })
        svg_index[(variant, 1000)] = svg_path

    print('[info] Rasterizing PNG sizes...')
    png_grouped: Dict[str, Dict[int, Path]] = {v: {} for v in VARIANTS}
    for variant in VARIANTS:
        for sz in SIZES:
            img = raster_variant(variant, sz)
            png_path = PNG_DIR / f'icon_cart_growth_{variant}_{sz}.png'
            img.save(png_path)
            optimize_png(png_path)
            manifest_records.append({
                'path': str(png_path).replace('\\', '/'),
                'format': 'PNG',
                'variant': variant,
                'size_bytes': png_path.stat().st_size,
                'width': sz,
                'height': sz,
                'sha256': sha256_of_file(png_path)
            })
            png_grouped[variant][sz] = png_path

    print('[info] Building favicon from default variant...')
    favicon_path = FAVICON_DIR / 'favicon.ico'
    build_favicon(png_grouped['default'], favicon_path)
    if favicon_path.exists():
        manifest_records.append({
            'path': str(favicon_path).replace('\\', '/'),
            'format': 'ICO',
            'variant': 'default',
            'size_bytes': favicon_path.stat().st_size,
            'width': None,
            'height': None,
            'sha256': sha256_of_file(favicon_path)
        })

    print('[info] Writing alt text and manifest...')
    alt_path = META_DIR / 'alt_text.txt'
    alt_path.write_text(ALT_TEXT, encoding='utf-8')
    manifest_records.append({
        'path': str(alt_path).replace('\\', '/'),
        'format': 'TXT',
        'variant': 'all',
        'size_bytes': alt_path.stat().st_size,
        'width': None,
        'height': None,
        'sha256': sha256_of_file(alt_path)
    })

    manifest_path = META_DIR / 'assets_manifest.json'
    build_manifest(manifest_records, manifest_path)
    print(f'[info] Manifest written: {manifest_path}')

    print('[info] Computing contrast report...')
    contrast_bg_bars = contrast_ratio(COLOR_BG_INNER, COLOR_ACCENT)
    contrast_bg_white = contrast_ratio(COLOR_BG_INNER, COLOR_WHITE)
    contrast_path = META_DIR / 'contrast.txt'
    contrast_path.write_text(
        f"Contrast (inner bg vs accent bars): {contrast_bg_bars:.2f}\n"
        f"Contrast (inner bg vs white): {contrast_bg_white:.2f}\n"
        f"WCAG reference: >= 3 (UI), >= 4.5 (text normal)\n",
        encoding='utf-8'
    )
    manifest_records.append({
        'path': str(contrast_path).replace('\\', '/'),
        'format': 'TXT',
        'variant': 'analysis',
        'size_bytes': contrast_path.stat().st_size,
        'width': None,
        'height': None,
        'sha256': sha256_of_file(contrast_path)
    })

    print('[info] Building animation GIF...')
    gif_frames: List[Image.Image] = []
    steps = 12
    from PIL import ImageDraw
    for i in range(steps):
        progress = (i+1)/steps
        base = raster_variant('default', 512)
        draw = ImageDraw.Draw(base)
        # overlay partial growth bars (reuse geometry but scale height by progress)
        base_y, bars = GEOM['bars']
        for bx, w, h in bars:
            ph = h * progress
            size = 512
            x1 = bx * size
            y_top = (1 - (base_y + ph)) * size
            draw.rectangle([x1, y_top, x1 + w*size, y_top + ph*size], fill=COLOR_ACCENT)
        gif_frames.append(base)
    gif_path = BRAND_DIR / 'icon_growth.gif'
    gif_frames[0].save(gif_path, save_all=True, append_images=gif_frames[1:], duration=80, loop=0, disposal=2)
    manifest_records.append({
        'path': str(gif_path).replace('\\', '/'),
        'format': 'GIF',
        'variant': 'animation',
        'size_bytes': gif_path.stat().st_size,
        'width': 512,
        'height': 512,
        'sha256': sha256_of_file(gif_path)
    })

    print('[info] Adding favicon.svg ...')
    favicon_svg_text = build_svg('default', 256)
    favicon_svg_path = FAVICON_DIR / 'favicon.svg'
    # simple minify: remove > < whitespace sequences
    mini = ' '.join(favicon_svg_text.split())
    favicon_svg_path.write_text(mini, encoding='utf-8')
    manifest_records.append({
        'path': str(favicon_svg_path).replace('\\', '/'),
        'format': 'SVG',
        'variant': 'favicon',
        'size_bytes': favicon_svg_path.stat().st_size,
        'width': None,
        'height': None,
        'sha256': sha256_of_file(favicon_svg_path)
    })

    print('[info] Creating distribution ZIP...')
    dist_dir = BRAND_DIR / 'dist'
    dist_dir.mkdir(exist_ok=True)
    zip_path = dist_dir / 'brand_assets_package.zip'
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(BRAND_DIR):
            for fname in files:
                if fname.endswith('.zip'):  # skip old zips
                    continue
                fpath = Path(root) / fname
                rel = fpath.relative_to(BRAND_DIR)
                zf.write(fpath, rel)
    manifest_records.append({
        'path': str(zip_path).replace('\\', '/'),
        'format': 'ZIP',
        'variant': 'bundle',
        'size_bytes': zip_path.stat().st_size,
        'width': None,
        'height': None,
        'sha256': sha256_of_file(zip_path)
    })

    print('[info] Writing final manifest update...')
    build_manifest(manifest_records, manifest_path)

    print('[done] Assets built successfully (extended).')

if __name__ == '__main__':
    main()

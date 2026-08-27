#!/usr/bin/env python3
"""Regenerates the "*-scaled.png" walking/sitting spritesheets used by
frontend/critters.js.

Why this exists: the shipped source art draws each cat in only ~25-28px
of a 64x64 tile (a lot of transparent padding baked into the purchased
"8BIT Pixel Cats" pack), and the 4 non-yellow colors' raw walking/sitting
sheets are gridded at 64x55 per frame instead of a clean 64x64 -- neither
divides evenly, which makes Phaser slice frames at the wrong offsets
(visible as a "bouncing"/torn sprite). This script crops each frame to
its actual content, scales it up with nearest-neighbor (keeps the pixel
art crisp), and re-centers it in a proper 64x64 tile, using one fixed
crop window per sheet so animation frames don't jitter relative to each
other.

Run from anywhere; paths are relative to this file's directory:
    python3 generate-scaled-sheets.py

Requires Pillow (`pip install pillow`).
"""
from pathlib import Path
from PIL import Image

ROOT = Path(__file__).parent
TARGET = 48  # target max content dimension inside the 64x64 output tile


def build_from_raw_grid(src, cols, rows, cw, ch, n, out_path):
    im = Image.open(src).convert("RGBA")
    cells, bboxes = [], []
    idx = 0
    for r in range(rows):
        for c in range(cols):
            if idx >= n:
                break
            cell = im.crop((c * cw, r * ch, c * cw + cw, r * ch + ch))
            cells.append(cell)
            bboxes.append(cell.getbbox())
            idx += 1
    _write(cells, bboxes, cw, ch, out_path)


def build_from_clean_sheet(src, n, out_path):
    im = Image.open(src).convert("RGBA")
    frames = [im.crop((i * 64, 0, i * 64 + 64, 64)) for i in range(n)]
    bboxes = [f.getbbox() for f in frames]
    _write(frames, bboxes, 64, 64, out_path)


def _write(cells, bboxes, cw, ch, out_path):
    x0s = [b[0] for b in bboxes]
    x1s = [b[2] for b in bboxes]
    y0s = [b[1] for b in bboxes]
    y1s = [b[3] for b in bboxes]
    w = max(x1s) - min(x0s)
    h = max(y1s) - min(y0s)
    scale = TARGET / max(w, h)

    avg_cx = sum((b[0] + b[2]) / 2 for b in bboxes) / len(bboxes)
    avg_cy = sum((b[1] + b[3]) / 2 for b in bboxes) / len(bboxes)
    scaled_cw, scaled_ch = round(cw * scale), round(ch * scale)
    crop_x0 = round(avg_cx * scale - 32)
    crop_y0 = round(avg_cy * scale - 32)
    crop_x0 = max(0, min(crop_x0, scaled_cw - 64))
    crop_y0 = max(0, min(crop_y0, scaled_ch - 64))

    n = len(cells)
    out = Image.new("RGBA", (64 * n, 64), (0, 0, 0, 0))
    for i, cell in enumerate(cells):
        scaled = cell.resize((scaled_cw, scaled_ch), Image.NEAREST)
        canvas = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
        canvas.paste(scaled, (-crop_x0, -crop_y0), scaled)
        out.paste(canvas, (i * 64, 0), canvas)
    out.save(out_path)
    print(f"wrote {out_path} scale={scale:.3f} crop=({crop_x0},{crop_y0})")


def main():
    for color, sitfile, walkfile in [
        ("grey", "Sitting Grey Cat.png", "walking grey cat.png"),
        ("siamese", "Sitting Siamese.png", "Walking Siamese.png"),
        ("pinkie", "Sitting Pinkie.png", "Walking Pinkie.png"),
        ("black", "Sitting Black Cat.png", "Walking Black Cat.png"),
    ]:
        base = ROOT / color / "unclothed"
        build_from_raw_grid(base / sitfile, 2, 3, 64, 55, 5, base / "sitting-scaled.png")
        build_from_raw_grid(base / walkfile, 3, 3, 64, 55, 8, base / "walking-scaled.png")

    yellow = ROOT / "yellow" / "unclothed"
    build_from_clean_sheet(yellow / "sitting.png", 5, yellow / "sitting-scaled.png")
    build_from_clean_sheet(yellow / "walking.png", 8, yellow / "walking-scaled.png")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


def main() -> int:
    if len(sys.argv) < 4:
        print("Usage: render_proof.py <input.log> <output.png> <title>")
        return 1

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    title = sys.argv[3]

    text = input_path.read_text(errors="replace")
    lines = [title, "=" * len(title), ""] + text.splitlines()
    lines = lines[:180]

    font = ImageFont.load_default()
    line_height = 16
    width = 1400
    height = max(400, line_height * (len(lines) + 4))

    image = Image.new("RGB", (width, height), (18, 18, 18))
    draw = ImageDraw.Draw(image)

    y = 20
    for line in lines:
      draw.text((20, y), line[:200], fill=(224, 224, 224), font=font)
      y += line_height

    output_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Format-conversie voor gedownloade media-bestanden.

- Foto's: Pillow → target format (jpg, png, webp)
- Video's: ffmpeg → target format (mp4, webm, mkv)
- GIFs: behouden of converteren naar video (ffmpeg)

Wordt aangeroepen door de engine na succesvolle download.
"""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
from typing import Optional

log = logging.getLogger("rmm.convert")


class ConvertError(Exception):
    """Fout bij format-conversie."""


def _target_ext(target_format: str) -> str:
    """Normaliseer het doel-formaat naar een extensie (zonder punt)."""
    fmt = (target_format or "").strip().lstrip(".").lower()
    # "jpeg" → "jpg" voor consistentie
    if fmt == "jpeg":
        fmt = "jpg"
    return fmt or "jpg"


# ---------------------------------------------------------------------------
# Foto-conversie (Pillow)
# ---------------------------------------------------------------------------

def convert_photo(src_path: str, target_format: str = "jpg", quality: int = 95) -> str:
    """Converteer een foto naar het target-formaat.

    Retourneert het nieuwe bestandspad. Als het bestandsformaat al matcht,
    wordt het pad ongewijzigd teruggegeven (geen kopie).
    """
    from PIL import Image

    target = _target_ext(target_format)
    src_ext = os.path.splitext(src_path)[1].lstrip(".").lower()

    # Geen conversie nodig
    if src_ext == target:
        return src_path

    dst_path = os.path.splitext(src_path)[0] + f".{target}"

    try:
        img = Image.open(src_path)

        # JPG ondersteunt geen transparantie → flatten op wit
        if target in ("jpg", "jpeg") and img.mode in ("RGBA", "LA", "P"):
            img = img.convert("RGBA")
            background = Image.new("RGB", img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1] if img.mode == "RGBA" else None)
            img = background
        elif target in ("jpg", "jpeg") and img.mode == "P":
            img = img.convert("RGB")
        elif target == "png" and img.mode == "RGB":
            pass  # PNG kan RGB
        elif target == "webp":
            pass  # WebP kan alle modi

        # Opslaan (hoge kwaliteit — default 95)
        q = max(1, min(100, int(quality or 95)))
        save_kwargs: dict = {"quality": q} if target in ("jpg", "jpeg", "webp") else {"optimize": True}
        img.save(dst_path, **save_kwargs)
        img.close()

        # Oorspronkelijk bestand verwijderen
        if dst_path != src_path and os.path.exists(src_path):
            os.remove(src_path)

        log.info("Foto geconverteerd: %s → %s", os.path.basename(src_path), os.path.basename(dst_path))
        return dst_path

    except ImportError:
        raise ConvertError("Pillow is niet geïnstalleerd — foto-conversie niet mogelijk")
    except Exception as exc:
        # Cleanup partial file
        if os.path.exists(dst_path):
            try:
                os.remove(dst_path)
            except OSError:
                pass
        raise ConvertError(f"Foto-conversie gefaald: {exc}") from exc


# ---------------------------------------------------------------------------
# Video/GIF-conversie (ffmpeg)
# ---------------------------------------------------------------------------

def _find_ffmpeg() -> Optional[str]:
    """Zoek naar het ffmpeg-binaire."""
    return shutil.which("ffmpeg")


async def convert_video(src_path: str, target_format: str = "mp4", crf: int = 18) -> str:
    """Converteer een video/GIF naar het target-formaat via ffmpeg.

    `crf` = bitrate/qualiteits-instelling (lager = beter). Default 18 ≈ visueel verliesloos.
    Retourneert het nieuwe bestandspad. Als het formaat al matcht, wordt het pad
    ongewijzigd teruggegeven.
    """
    target = _target_ext(target_format)
    src_ext = os.path.splitext(src_path)[1].lstrip(".").lower()

    # Geen conversie nodig
    if src_ext == target:
        return src_path

    ffmpeg = _find_ffmpeg()
    if not ffmpeg:
        raise ConvertError("ffmpeg niet gevonden — video-conversie niet mogelijk")

    dst_path = os.path.splitext(src_path)[0] + f".{target}"

    # Codec-selectie per doel-formaat
    # NB: VP9-schaal verschilt van H.264 — cap op 30
    x264_crf = max(0, min(51, int(crf or 18)))
    vp9_crf = max(0, min(30, int(crf or 18)))
    if target == "mp4":
        codec_args = ["-c:v", "libx264", "-preset", "slow", "-crf", str(x264_crf), "-c:a", "aac", "-movflags", "+faststart"]
    elif target == "webm":
        codec_args = ["-c:v", "libvpx-vp9", "-crf", str(vp9_crf), "-b:v", "0", "-c:a", "libopus"]
    elif target == "mkv":
        codec_args = ["-c:v", "libx264", "-preset", "slow", "-crf", str(x264_crf), "-c:a", "aac"]
    else:
        codec_args = ["-c:v", "libx264", "-preset", "slow", "-crf", str(x264_crf)]

    cmd = [ffmpeg, "-y", "-i", src_path, *codec_args, dst_path]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0:
        err_msg = stderr.decode(errors="replace")[-500:] if stderr else "onbekende fout"
        if os.path.exists(dst_path):
            try:
                os.remove(dst_path)
            except OSError:
                pass
        raise ConvertError(f"ffmpeg gefaald (rc={proc.returncode}): {err_msg}")

    # Oorspronkelijk bestand verwijderen
    if dst_path != src_path and os.path.exists(src_path):
        os.remove(src_path)

    log.info("Video geconverteerd: %s → %s", os.path.basename(src_path), os.path.basename(dst_path))
    return dst_path


async def convert_gif_to_video(src_path: str, target_format: str = "mp4", crf: int = 18) -> str:
    """Converteer een GIF naar een video-formaat via ffmpeg."""
    return await convert_video(src_path, target_format, crf=crf)


# ---------------------------------------------------------------------------
# Hoofdapit: determine + execute conversie
# ---------------------------------------------------------------------------

async def convert_file(
    file_path: str,
    media_type: str | None,
    photo_format: str = "jpg",
    video_format: str = "mp4",
    gif_mode: str = "keep",
    photo_quality: int = 95,
    video_quality: int = 18,
) -> str:
    """Converteer een gedownloaid bestand naar het ingestelde doel-formaat.

    Parameters:
      file_path    — pad naar het gedownloade bestand
      media_type   — "photo", "video", "gif" of None
      photo_format — doel-formaat voor foto's
      video_format — doel-formaat voor video's
      gif_mode     — "keep" of "convert"

    Retourneert het (mogelijk nieuwe) bestandspad.
    """
    if media_type == "photo":
        # Synchronus (Pillow is sync) — in een thread uitvoeren
        return await asyncio.to_thread(convert_photo, file_path, photo_format, photo_quality)

    elif media_type == "video":
        return await convert_video(file_path, video_format, crf=video_quality)

    elif media_type == "gif":
        if gif_mode == "convert":
            return await convert_gif_to_video(file_path, video_format, crf=video_quality)
        # gif_mode == "keep" → geen conversie
        return file_path

    # Onbekend type → geen conversie
    return file_path

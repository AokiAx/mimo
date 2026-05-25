from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class AudioSpeechRequest(BaseModel):
    input: str = Field(min_length=1)
    model: Optional[str] = None
    voice: Optional[str] = None
    response_format: Literal["mp3", "opus", "aac", "flac", "wav", "pcm16", "pcm"] = "wav"
    instructions: Optional[str] = None
    voice_description: Optional[str] = None
    voice_sample_base64: Optional[str] = None
    voice_sample_mime_type: Optional[str] = None
    optimize_text_preview: Optional[bool] = None


def map_openai_tts_voice(voice: Optional[str]) -> str:
    default_voice_map = {
        "alloy": "mimo_default",
        "ash": "mimo_default",
        "ballad": "mimo_default",
        "coral": "mimo_default",
        "echo": "mimo_default",
        "fable": "mimo_default",
        "nova": "mimo_default",
        "onyx": "mimo_default",
        "sage": "mimo_default",
        "shimmer": "mimo_default",
        "verse": "mimo_default",
    }
    if not voice:
        return "mimo_default"
    return default_voice_map.get(voice, voice)


def map_openai_tts_model(model: Optional[str]) -> str:
    if not model:
        return "mimo-v2.5-tts"
    model_map = {
        "tts-1": "mimo-v2.5-tts",
        "tts-1-hd": "mimo-v2.5-tts",
        "gpt-4o-mini-tts": "mimo-v2.5-tts",
        "mimo-v2-tts": "mimo-v2-tts",
        "mimo-v2.5-tts": "mimo-v2.5-tts",
        "mimo-v2.5-tts-voicedesign": "mimo-v2.5-tts-voicedesign",
        "mimo-v2.5-tts-voiceclone": "mimo-v2.5-tts-voiceclone",
    }
    return model_map.get(model, model)


def audio_media_type(audio_format: str) -> str:
    media_types = {
        "aac": "audio/aac",
        "flac": "audio/flac",
        "mp3": "audio/mpeg",
        "opus": "audio/ogg",
        "pcm": "audio/pcm",
        "pcm16": "audio/pcm",
        "wav": "audio/wav",
    }
    return media_types.get(audio_format.lower(), "application/octet-stream")

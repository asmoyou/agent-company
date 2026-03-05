import os
import base64
import json
import asyncio
import time
import cv2
from typing import List
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Mount the data directory to serve video files
app.mount("/data", StaticFiles(directory="../data"), name="data")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = AsyncOpenAI(
    api_key=os.getenv("OPENAI_API_KEY", "your-api-key"),
    base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
)

AVAILABLE_MODELS = [
    "Qwen/Qwen3-VL-30B-A3B-Instruct",
    "Qwen/Qwen3-VL-30B-A3B-Thinking",
    "Qwen/Qwen3-VL-32B-Instruct",
    "Qwen/Qwen3-VL-32B-Thinking",
    "Qwen/Qwen3-VL-8B-Instruct",
    "Qwen/Qwen3-VL-8B-Thinking",
]

@app.get("/api/prompt")
async def get_prompt():
    try:
        with open("../prompt.md", "r", encoding="utf-8") as f:
            return {"prompt": f.read()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/models")
async def list_models():
    return {"models": AVAILABLE_MODELS, "default": AVAILABLE_MODELS[0]}

@app.get("/api/files")
async def list_files():
    try:
        data_dir = "../data"
        files = [f for f in os.listdir(data_dir) if f.endswith(('.mp4', '.webm'))]
        return {"files": sorted(files)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def extract_frames(video_path: str, fps: int = 1, target_width: int = 720, target_height: int = 480, roi: dict = None) -> List[str]:
    """Extract frames by seeking directly instead of reading every frame."""
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise ValueError(f"Cannot open video: {video_path}")

    video_fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if video_fps <= 0:
        video_fps = 25.0

    frame_interval = int(video_fps / fps)
    if frame_interval < 1:
        frame_interval = 1

    frames_base64 = []
    frame_pos = 0

    while frame_pos < total_frames:
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_pos)
        ret, frame = cap.read()
        if not ret:
            break
        # ROI crop
        if roi:
            h, w = frame.shape[:2]
            x1 = max(0, int(roi['x'] * w))
            y1 = max(0, int(roi['y'] * h))
            x2 = min(w, int((roi['x'] + roi['w']) * w))
            y2 = min(h, int((roi['y'] + roi['h']) * h))
            if x2 > x1 and y2 > y1:
                frame = frame[y1:y2, x1:x2]
        resized = cv2.resize(frame, (target_width, target_height))
        _, buffer = cv2.imencode('.jpg', resized, [cv2.IMWRITE_JPEG_QUALITY, 85])
        frames_base64.append(base64.b64encode(buffer).decode('utf-8'))
        frame_pos += frame_interval

    cap.release()
    return frames_base64


@app.post("/api/analyze")
async def analyze_video(
    video: UploadFile = File(...),
    prompt: str = Form(...),
    model: str = Form("Qwen/Qwen3-VL-30B-A3B-Instruct"),
    roi: str = Form("")
):
    temp_file = f"temp_{video.filename}"
    with open(temp_file, "wb") as f:
        f.write(await video.read())

    system_prompt = prompt
    selected_model = model if model in AVAILABLE_MODELS else AVAILABLE_MODELS[0]
    roi_rect = json.loads(roi) if roi else None

    async def generate_chunks():
        try:
            # Stage 1: Extract frames
            if roi_rect:
                yield json.dumps({"type": "progress", "text": "Extracting frames with ROI crop (1 FPS, 720x480)..."}, ensure_ascii=False) + "\n"
            else:
                yield json.dumps({"type": "progress", "text": "Extracting video frames (1 FPS, 720x480)..."}, ensure_ascii=False) + "\n"

            t0 = time.time()
            frames = await asyncio.to_thread(
                extract_frames, temp_file, 1, 720, 480, roi_rect
            )
            t1 = time.time()

            yield json.dumps({"type": "progress", "text": f"Extracted {len(frames)} frames in {t1-t0:.1f}s. Sending to {selected_model}..."}, ensure_ascii=False) + "\n"
            yield json.dumps({"type": "stats", "frames": len(frames)}, ensure_ascii=False) + "\n"

            # Stage 2: Build messages and call LLM
            content_parts = [{"type": "text", "text": system_prompt}]
            for frame_b64 in frames:
                content_parts.append({
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{frame_b64}"
                    }
                })

            messages = [
                {
                    "role": "user",
                    "content": content_parts
                }
            ]

            stream = await client.chat.completions.create(
                model=selected_model,
                messages=messages,
                stream=True,
            )

            t_llm_start = time.time()
            first_token = True

            async for chunk in stream:
                if chunk.choices and len(chunk.choices) > 0:
                    delta = chunk.choices[0].delta

                    reasoning = getattr(delta, 'reasoning_content', None) or \
                                getattr(delta, 'thinking', None)

                    if first_token and (reasoning or delta.content):
                        ttft = time.time() - t_llm_start
                        yield json.dumps({"type": "stats", "ttft": round(ttft, 1)}, ensure_ascii=False) + "\n"
                        first_token = False

                    if reasoning:
                        yield json.dumps({"type": "thinking", "text": reasoning}, ensure_ascii=False) + "\n"

                    if delta.content:
                        yield json.dumps({"type": "content", "text": delta.content}, ensure_ascii=False) + "\n"

        except Exception as e:
            yield json.dumps({"type": "error", "text": str(e)}, ensure_ascii=False) + "\n"
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

    return StreamingResponse(generate_chunks(), media_type="application/x-ndjson")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

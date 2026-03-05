import React, { useState, useEffect, useRef } from 'react';
import { Upload, Play, Activity, CheckCircle2, Loader2, FileVideo, Brain, Terminal, Settings, X, ChevronDown, Code, Crop, Trash2 } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

const API_BASE_URL = 'http://localhost:8000';

const MODELS = [
  { value: 'Qwen/Qwen3-VL-30B-A3B-Instruct', label: '30B-Instruct' },
  { value: 'Qwen/Qwen3-VL-30B-A3B-Thinking', label: '30B-Thinking' },
  { value: 'Qwen/Qwen3-VL-32B-Instruct', label: '32B-Instruct' },
  { value: 'Qwen/Qwen3-VL-32B-Thinking', label: '32B-Thinking' },
  { value: 'Qwen/Qwen3-VL-8B-Instruct', label: '8B-Instruct' },
  { value: 'Qwen/Qwen3-VL-8B-Thinking', label: '8B-Thinking' },
];

function App() {
  const [videoFile, setVideoFile] = useState<File | null>(null);
  const [previewUrl, setPreviewUrl] = useState<string | null>(null);
  const [analyzing, setAnalyzing] = useState(false);
  const [thinking, setThinking] = useState('');
  const [content, setContent] = useState('');
  const [systemPrompt, setSystemPrompt] = useState('');
  const [dataFiles, setDataFiles] = useState<string[]>([]);
  const [jsonResult, setJsonResult] = useState<any>(null);
  const [selectedModel, setSelectedModel] = useState(MODELS[0].value);
  const [showPromptEditor, setShowPromptEditor] = useState(false);
  const [promptDraft, setPromptDraft] = useState('');
  const [progress, setProgress] = useState('');
  const [showRawContent, setShowRawContent] = useState(false);
  const [analysisStats, setAnalysisStats] = useState<{ frames?: number; ttft?: number }>({});

  // ROI state
  const [roi, setRoi] = useState<{ x: number; y: number; w: number; h: number } | null>(null);
  const [drawingRoi, setDrawingRoi] = useState(false);
  const [roiStart, setRoiStart] = useState<{ x: number; y: number } | null>(null);
  const [roiPreview, setRoiPreview] = useState<{ x: number; y: number; w: number; h: number } | null>(null);

  const thinkingRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const videoRef = useRef<HTMLVideoElement>(null);
  const videoContainerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/api/prompt`)
      .then(res => res.json())
      .then(data => setSystemPrompt(data.prompt))
      .catch(err => console.error("Failed to fetch prompt:", err));

    fetch(`${API_BASE_URL}/api/files`)
      .then(res => res.json())
      .then(data => setDataFiles(data.files))
      .catch(err => console.error("Failed to fetch files:", err));
  }, []);

  useEffect(() => {
    if (thinkingRef.current) thinkingRef.current.scrollTop = thinkingRef.current.scrollHeight;
  }, [thinking]);

  useEffect(() => {
    if (contentRef.current) contentRef.current.scrollTop = contentRef.current.scrollHeight;
  }, [content]);

  useEffect(() => {
    if (!content) return;
    try {
      const firstBrace = content.indexOf('{');
      const lastBrace = content.lastIndexOf('}');
      if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace) {
        const jsonStr = content.substring(firstBrace, lastBrace + 1);
        const parsed = JSON.parse(jsonStr);
        setJsonResult(parsed);
      }
    } catch {
      // incomplete JSON, wait for more data
    }
  }, [content]);

  // Coordinate conversion: compute actual video display rect within container (object-contain)
  const getVideoDisplayRect = () => {
    const video = videoRef.current;
    const container = videoContainerRef.current;
    if (!video || !container) return null;
    const vw = video.videoWidth;
    const vh = video.videoHeight;
    if (!vw || !vh) return null;
    const cw = container.clientWidth;
    const ch = container.clientHeight;
    const videoAspect = vw / vh;
    const containerAspect = cw / ch;
    let displayW: number, displayH: number, offsetX: number, offsetY: number;
    if (videoAspect > containerAspect) {
      displayW = cw;
      displayH = cw / videoAspect;
      offsetX = 0;
      offsetY = (ch - displayH) / 2;
    } else {
      displayH = ch;
      displayW = ch * videoAspect;
      offsetX = (cw - displayW) / 2;
      offsetY = 0;
    }
    return { displayW, displayH, offsetX, offsetY };
  };

  // Convert mouse event to normalized video coordinates (0-1)
  const getVideoNormalizedCoords = (e: React.MouseEvent) => {
    const container = videoContainerRef.current;
    const rect = getVideoDisplayRect();
    if (!container || !rect) return null;
    const containerRect = container.getBoundingClientRect();
    const mx = e.clientX - containerRect.left - rect.offsetX;
    const my = e.clientY - containerRect.top - rect.offsetY;
    const nx = Math.max(0, Math.min(1, mx / rect.displayW));
    const ny = Math.max(0, Math.min(1, my / rect.displayH));
    return { x: nx, y: ny };
  };

  // Convert normalized ROI to pixel CSS within the container
  const getRoiPixelStyle = (roiRect: { x: number; y: number; w: number; h: number }) => {
    const rect = getVideoDisplayRect();
    if (!rect) return {};
    return {
      left: rect.offsetX + roiRect.x * rect.displayW,
      top: rect.offsetY + roiRect.y * rect.displayH,
      width: roiRect.w * rect.displayW,
      height: roiRect.h * rect.displayH,
    };
  };

  // ROI mouse handlers
  const handleRoiMouseDown = (e: React.MouseEvent) => {
    if (!drawingRoi) return;
    e.preventDefault();
    const coords = getVideoNormalizedCoords(e);
    if (!coords) return;
    setRoiStart(coords);
    setRoi(null);
    setRoiPreview(null);
  };

  const handleRoiMouseMove = (e: React.MouseEvent) => {
    if (!drawingRoi || !roiStart) return;
    e.preventDefault();
    const coords = getVideoNormalizedCoords(e);
    if (!coords) return;
    const x = Math.min(roiStart.x, coords.x);
    const y = Math.min(roiStart.y, coords.y);
    const w = Math.abs(coords.x - roiStart.x);
    const h = Math.abs(coords.y - roiStart.y);
    setRoiPreview({ x, y, w, h });
  };

  const handleRoiMouseUp = (e: React.MouseEvent) => {
    if (!drawingRoi || !roiStart) return;
    e.preventDefault();
    const coords = getVideoNormalizedCoords(e);
    if (!coords) return;
    const x = Math.min(roiStart.x, coords.x);
    const y = Math.min(roiStart.y, coords.y);
    const w = Math.abs(coords.x - roiStart.x);
    const h = Math.abs(coords.y - roiStart.y);
    if (w * h > 0.001) {
      setRoi({ x, y, w, h });
    }
    setRoiStart(null);
    setRoiPreview(null);
    setDrawingRoi(false);
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      const file = e.target.files[0];
      setVideoFile(file);
      setPreviewUrl(URL.createObjectURL(file));
      resetAnalysis();
      setRoi(null);
      setDrawingRoi(false);
      setRoiPreview(null);
      setRoiStart(null);
    }
  };

  const selectDataFile = async (fileName: string) => {
    try {
      const fileUrl = `${API_BASE_URL}/data/${fileName}`;
      setPreviewUrl(fileUrl);
      const response = await fetch(fileUrl);
      const blob = await response.blob();
      const file = new File([blob], fileName, { type: 'video/mp4' });
      setVideoFile(file);
      resetAnalysis();
      setRoi(null);
      setDrawingRoi(false);
      setRoiPreview(null);
      setRoiStart(null);
    } catch (err) {
      console.error("Error selecting file:", err);
    }
  };

  const resetAnalysis = () => {
    setThinking('');
    setContent('');
    setJsonResult(null);
    setProgress('');
    setAnalysisStats({});
  };

  const openPromptEditor = () => {
    setPromptDraft(systemPrompt);
    setShowPromptEditor(true);
  };

  const savePrompt = () => {
    setSystemPrompt(promptDraft);
    setShowPromptEditor(false);
  };

  const startAnalysis = async () => {
    if (!videoFile) return;

    setAnalyzing(true);
    resetAnalysis();

    const formData = new FormData();
    formData.append('video', videoFile);
    formData.append('prompt', systemPrompt);
    formData.append('model', selectedModel);
    if (roi) {
      formData.append('roi', JSON.stringify(roi));
    }

    try {
      const response = await fetch(`${API_BASE_URL}/api/analyze`, {
        method: 'POST',
        body: formData,
      });

      if (!response.body) return;

      const reader = response.body.getReader();
      const decoder = new TextDecoder();

      let buffer = '';

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });

        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed) continue;
          try {
            const msg = JSON.parse(trimmed);
            if (msg.type === 'thinking') {
              setProgress('');
              setThinking(prev => prev + msg.text);
            } else if (msg.type === 'content') {
              setProgress('');
              setContent(prev => prev + msg.text);
            } else if (msg.type === 'progress') {
              setProgress(msg.text);
            } else if (msg.type === 'stats') {
              const { type, ...stats } = msg;
              setAnalysisStats(prev => ({ ...prev, ...stats }));
            } else if (msg.type === 'error') {
              setProgress('');
              setContent(prev => prev + `Error: ${msg.text}`);
            }
          } catch {
            // ignore malformed lines
          }
        }
      }
    } catch (error) {
      console.error('Analysis failed:', error);
      setContent('Error during analysis. Please check backend connection.');
    } finally {
      setAnalyzing(false);
      setProgress('');
    }
  };

  const currentModelLabel = MODELS.find(m => m.value === selectedModel)?.label || '';

  return (
    <div className="min-h-screen bg-[#F5F5F7] text-[#1D1D1F] font-sans selection:bg-blue-100 selection:text-blue-900">
      {/* Header */}
      <header className="bg-white/80 backdrop-blur-md border-b border-gray-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-6 h-16 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="bg-black text-white p-1.5 rounded-lg">
              <Activity className="w-5 h-5" />
            </div>
            <h1 className="text-lg font-semibold tracking-tight">AccidentAI <span className="text-gray-400 font-normal">Pro</span></h1>
          </div>
          <div className="flex items-center gap-3">
            {/* Model Selector */}
            <div className="relative">
              <select
                value={selectedModel}
                onChange={e => setSelectedModel(e.target.value)}
                disabled={analyzing}
                className="appearance-none text-xs font-medium pl-3 pr-7 py-1.5 bg-gray-100 rounded-full text-gray-600 border border-gray-200/50 cursor-pointer hover:bg-gray-200 transition-colors disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
              >
                {MODELS.map(m => (
                  <option key={m.value} value={m.value}>{m.label}</option>
                ))}
              </select>
              <ChevronDown className="w-3 h-3 absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 pointer-events-none" />
            </div>
            {/* Prompt Editor Button */}
            <button
              onClick={openPromptEditor}
              className="text-xs font-medium px-3 py-1.5 bg-gray-100 rounded-full text-gray-600 border border-gray-200/50 hover:bg-gray-200 transition-colors flex items-center gap-1.5"
            >
              <Settings className="w-3.5 h-3.5" />
              Prompt
            </button>
          </div>
        </div>
      </header>

      {/* Prompt Editor Modal */}
      {showPromptEditor && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/40 backdrop-blur-sm">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl mx-4 max-h-[80vh] flex flex-col">
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
              <h2 className="text-sm font-semibold text-gray-900">System Prompt</h2>
              <button onClick={() => setShowPromptEditor(false)} className="text-gray-400 hover:text-gray-600 transition-colors">
                <X className="w-5 h-5" />
              </button>
            </div>
            <div className="flex-1 p-6 overflow-hidden">
              <textarea
                value={promptDraft}
                onChange={e => setPromptDraft(e.target.value)}
                className="w-full h-full min-h-[300px] text-sm font-mono text-gray-700 bg-gray-50 rounded-xl p-4 border border-gray-200 resize-none focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-300"
              />
            </div>
            <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-100">
              <button
                onClick={() => setShowPromptEditor(false)}
                className="text-sm px-4 py-2 text-gray-600 hover:text-gray-900 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={savePrompt}
                className="text-sm px-5 py-2 bg-[#0071e3] text-white rounded-lg font-medium hover:bg-[#0077ED] transition-colors"
              >
                Save
              </button>
            </div>
          </div>
        </div>
      )}

      <main className="max-w-7xl mx-auto px-6 py-10 grid grid-cols-1 lg:grid-cols-12 gap-10">

        {/* Left Panel: Input */}
        <div className="lg:col-span-5 space-y-8">

          {/* Video Preview / Upload */}
          <div className="bg-white rounded-3xl shadow-[0_8px_30px_rgb(0,0,0,0.04)] border border-gray-100 overflow-hidden">
            <div className="p-1">
              {previewUrl ? (
                <div ref={videoContainerRef} className="relative aspect-video rounded-2xl overflow-hidden bg-black group">
                  <video
                    ref={videoRef}
                    key={previewUrl}
                    src={previewUrl}
                    controls
                    className="w-full h-full object-contain"
                  />
                  {/* ROI overlay */}
                  <div
                    className="absolute inset-0"
                    style={{ pointerEvents: drawingRoi ? 'all' : 'none', cursor: drawingRoi ? 'crosshair' : 'default' }}
                    onMouseDown={handleRoiMouseDown}
                    onMouseMove={handleRoiMouseMove}
                    onMouseUp={handleRoiMouseUp}
                  >
                    {/* Drawing preview */}
                    {roiPreview && (() => {
                      const style = getRoiPixelStyle(roiPreview);
                      return (
                        <div
                          className="absolute border-2 border-dashed border-blue-400 bg-blue-400/15"
                          style={{ left: style.left, top: style.top, width: style.width, height: style.height }}
                        />
                      );
                    })()}
                    {/* Confirmed ROI */}
                    {roi && !roiPreview && (() => {
                      const style = getRoiPixelStyle(roi);
                      return (
                        <div
                          className="absolute border-2 border-green-400 bg-green-400/10"
                          style={{ left: style.left, top: style.top, width: style.width, height: style.height }}
                        >
                          {/* Corner handles */}
                          <div className="absolute -top-1 -left-1 w-2.5 h-2.5 bg-green-400 rounded-sm" />
                          <div className="absolute -top-1 -right-1 w-2.5 h-2.5 bg-green-400 rounded-sm" />
                          <div className="absolute -bottom-1 -left-1 w-2.5 h-2.5 bg-green-400 rounded-sm" />
                          <div className="absolute -bottom-1 -right-1 w-2.5 h-2.5 bg-green-400 rounded-sm" />
                        </div>
                      );
                    })()}
                  </div>
                </div>
              ) : (
                <div
                  onClick={() => document.getElementById('video-upload')?.click()}
                  className="aspect-video rounded-2xl border-2 border-dashed border-gray-200 bg-gray-50/50 flex flex-col items-center justify-center cursor-pointer hover:bg-gray-50 transition-colors group"
                >
                  <div className="w-16 h-16 rounded-full bg-white shadow-sm flex items-center justify-center mb-4 group-hover:scale-105 transition-transform">
                    <Upload className="w-6 h-6 text-blue-500" />
                  </div>
                  <p className="text-sm font-medium text-gray-900">Upload Video</p>
                  <p className="text-xs text-gray-500 mt-1">MP4, WebM</p>
                </div>
              )}
            </div>

            {/* ROI Toolbar */}
            {previewUrl && (
              <div className="flex items-center gap-2 px-4 py-2 border-t border-gray-100">
                <button
                  onClick={() => {
                    if (drawingRoi) {
                      setDrawingRoi(false);
                      setRoiStart(null);
                      setRoiPreview(null);
                    } else {
                      setDrawingRoi(true);
                    }
                  }}
                  disabled={analyzing}
                  className={`text-xs font-medium px-3 py-1.5 rounded-lg border transition-colors flex items-center gap-1.5 disabled:opacity-50
                    ${drawingRoi
                      ? 'bg-blue-50 text-blue-600 border-blue-200'
                      : 'bg-gray-50 text-gray-600 border-gray-200 hover:bg-gray-100'}`}
                >
                  <Crop className="w-3.5 h-3.5" />
                  {drawingRoi ? 'Cancel' : 'Draw ROI'}
                </button>
                {roi && (
                  <>
                    <button
                      onClick={() => setRoi(null)}
                      disabled={analyzing}
                      className="text-xs font-medium px-3 py-1.5 rounded-lg border border-gray-200 bg-gray-50 text-gray-600 hover:bg-red-50 hover:text-red-600 hover:border-red-200 transition-colors flex items-center gap-1.5 disabled:opacity-50"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                      Clear
                    </button>
                    <span className="text-xs text-gray-400 ml-auto font-mono">
                      {Math.round(roi.w * 100)}% × {Math.round(roi.h * 100)}%
                    </span>
                  </>
                )}
              </div>
            )}

            <input
              type="file"
              id="video-upload"
              className="hidden"
              accept="video/*"
              onChange={handleFileChange}
            />

            <div className="p-6">
              {videoFile && (
                <div className="flex items-center justify-between mb-6 p-4 bg-gray-50 rounded-xl border border-gray-100">
                   <div className="flex items-center gap-3">
                     <FileVideo className="w-5 h-5 text-gray-400" />
                     <div className="text-sm">
                       <p className="font-medium text-gray-900 truncate max-w-[200px]">{videoFile.name}</p>
                       <p className="text-xs text-gray-500">Ready for analysis</p>
                     </div>
                   </div>
                   <button
                     onClick={() => { setVideoFile(null); setPreviewUrl(null); setRoi(null); setDrawingRoi(false); }}
                     className="text-xs text-gray-400 hover:text-red-500 transition-colors"
                   >
                     Remove
                   </button>
                </div>
              )}

              <button
                onClick={startAnalysis}
                disabled={analyzing || !videoFile}
                className={`w-full py-4 rounded-xl font-semibold text-[15px] flex items-center justify-center gap-2 transition-all transform active:scale-[0.98]
                  ${analyzing
                    ? 'bg-gray-100 text-gray-400 cursor-not-allowed'
                    : 'bg-[#0071e3] text-white hover:bg-[#0077ED] shadow-lg shadow-blue-500/20'}`}
              >
                {analyzing ? (
                  <>
                    <Loader2 className="w-5 h-5 animate-spin" />
                    Processing...
                  </>
                ) : (
                  <>
                    <Play className="w-5 h-5 fill-current" />
                    Start Detection{roi ? ' (ROI)' : ''}
                  </>
                )}
              </button>
            </div>
          </div>

          {/* Quick Select */}
          <div>
            <h3 className="text-sm font-semibold text-gray-900 mb-4 px-1">Sample Clips</h3>
            <div className="grid grid-cols-2 gap-3">
              {dataFiles.map(file => (
                <button
                  key={file}
                  onClick={() => selectDataFile(file)}
                  className="group relative p-3 rounded-xl bg-white border border-gray-200 hover:border-blue-500/30 hover:shadow-md transition-all text-left"
                >
                  <div className="flex items-start justify-between">
                    <span className="text-xs font-medium text-gray-600 group-hover:text-blue-600 truncate w-full">{file}</span>
                  </div>
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Right Panel: Output */}
        <div className="lg:col-span-7 flex flex-col gap-6 h-[calc(100vh-140px)] sticky top-24">

          {/* Progress Banner */}
          {progress && (
            <div className="flex items-center gap-3 px-5 py-3 bg-blue-50 border border-blue-100 rounded-2xl">
              <Loader2 className="w-4 h-4 animate-spin text-blue-500 shrink-0" />
              <span className="text-sm text-blue-700">{progress}</span>
            </div>
          )}

          {/* Analysis Stats */}
          {(analysisStats.frames != null || analysisStats.ttft != null) && (
            <div className="flex items-center gap-4 px-5 py-2 bg-gray-50 border border-gray-200 rounded-xl text-xs text-gray-500 font-mono">
              {analysisStats.frames != null && <span>{analysisStats.frames} frames sent</span>}
              {analysisStats.frames != null && analysisStats.ttft != null && <span className="text-gray-300">|</span>}
              {analysisStats.ttft != null && <span>TTFT {analysisStats.ttft}s</span>}
            </div>
          )}

          {/* Thinking Process */}
          <div className="flex-1 min-h-0 bg-gray-900 rounded-3xl shadow-2xl shadow-black/10 overflow-hidden flex flex-col border border-gray-800">
            <div className="px-5 py-4 border-b border-gray-800 flex items-center justify-between bg-gray-900/50 backdrop-blur-sm">
              <div className="flex items-center gap-2">
                <Brain className="w-4 h-4 text-pink-500" />
                <span className="text-xs font-medium text-gray-400 tracking-wide uppercase">Reasoning Engine</span>
              </div>
              {analyzing && <span className="flex h-2 w-2 rounded-full bg-pink-500 animate-pulse"></span>}
            </div>
            <div
              ref={thinkingRef}
              className="flex-1 overflow-y-auto p-6 font-mono text-sm leading-relaxed text-gray-300 scroll-smooth"
            >
               {thinking ? (
                 <div className="whitespace-pre-wrap">{thinking}</div>
               ) : (
                 <div className="h-full flex flex-col items-center justify-center text-gray-700">
                   <p className="text-xs">Model reasoning will appear here...</p>
                 </div>
               )}
            </div>
          </div>

          {/* Final Result */}
          <div className="flex-1 min-h-0 bg-white rounded-3xl shadow-[0_8px_30px_rgb(0,0,0,0.04)] border border-gray-100 overflow-hidden flex flex-col">
            <div className="px-5 py-4 border-b border-gray-100 flex items-center justify-between bg-white/50 backdrop-blur-sm">
              <div className="flex items-center gap-2">
                <Terminal className="w-4 h-4 text-blue-500" />
                <span className="text-xs font-medium text-gray-500 tracking-wide uppercase">Analysis Report</span>
              </div>
              {content && (
                <button
                  onClick={() => setShowRawContent(prev => !prev)}
                  className={`text-xs px-2.5 py-1 rounded-lg border transition-colors flex items-center gap-1 ${showRawContent ? 'bg-gray-900 text-white border-gray-900' : 'bg-white text-gray-500 border-gray-200 hover:bg-gray-50'}`}
                >
                  <Code className="w-3 h-3" />
                  Raw
                </button>
              )}
            </div>

            <div
              ref={contentRef}
              className="flex-1 overflow-y-auto p-6 scroll-smooth"
            >
              {showRawContent && content ? (
                <pre className="text-xs font-mono text-gray-600 whitespace-pre-wrap break-all bg-gray-50 rounded-xl p-4 border border-gray-200">{content}</pre>
              ) : jsonResult ? (
                <div className="space-y-6">
                  <div className={`p-5 rounded-2xl border-l-4 shadow-sm ${jsonResult.accident ? 'bg-red-50 border-red-500' : 'bg-green-50 border-green-500'}`}>
                    <div className="flex items-center justify-between mb-2">
                      <h3 className={`text-lg font-bold ${jsonResult.accident ? 'text-red-700' : 'text-green-700'}`}>
                        {jsonResult.accident ? 'Accident Detected' : 'No Accident Detected'}
                      </h3>
                      <span className="px-3 py-1 bg-white/50 rounded-full text-xs font-mono font-bold">
                        {jsonResult.confidence != null
                          ? `${(jsonResult.confidence > 1 ? jsonResult.confidence : jsonResult.confidence * 100).toFixed(0)}%`
                          : 'N/A'}
                      </span>
                    </div>
                    <p className="text-sm opacity-80 leading-relaxed">
                      {jsonResult.description}
                    </p>
                  </div>

                  <div className="grid grid-cols-2 gap-4 text-sm">
                     <div className="p-4 bg-gray-50 rounded-xl">
                       <span className="block text-xs text-gray-400 uppercase mb-1">Date/Time</span>
                       <span className="font-mono text-gray-700">{jsonResult.date || 'Unknown'}</span>
                     </div>
                     <div className="p-4 bg-gray-50 rounded-xl">
                       <span className="block text-xs text-gray-400 uppercase mb-1">Congestion</span>
                       <span className={`font-medium ${jsonResult.congestion ? 'text-orange-600' : 'text-gray-700'}`}>
                         {jsonResult.congestion ? 'Detected' : 'None'}
                       </span>
                     </div>
                  </div>
                </div>
              ) : content ? (
                <div className="prose prose-sm max-w-none text-gray-600">
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>{content}</ReactMarkdown>
                </div>
              ) : (
                <div className="h-full flex flex-col items-center justify-center text-gray-300">
                  <CheckCircle2 className="w-8 h-8 mb-3 opacity-20" />
                  <p className="text-sm">Final JSON output will render here</p>
                </div>
              )}
            </div>
          </div>

        </div>
      </main>

      <style>{`
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #e5e7eb; border-radius: 10px; }
        ::-webkit-scrollbar-thumb:hover { background: #d1d5db; }
        .bg-gray-900 ::-webkit-scrollbar-thumb { background: #374151; }
        .bg-gray-900 ::-webkit-scrollbar-thumb:hover { background: #4b5563; }
      `}</style>
    </div>
  );
}

export default App;

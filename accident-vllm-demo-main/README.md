# AccidentAI Pro

基于 vLLM + Qwen3-VL 视觉大模型的高速公路交通事故智能检测系统。上传监控视频，AI 自动分析是否发生交通事故，并输出结构化 JSON 报告。

## 项目结构

```
accident-vllm/
├── backend/          # FastAPI 后端
│   ├── main.py       # API 服务（视频抽帧 + LLM 调用）
│   ├── .env.example  # 环境变量模板
│   └── requirements.txt
├── frontend/         # React + Vite 前端
│   ├── src/App.tsx   # 主界面组件
│   └── package.json
├── data/             # 示例监控视频
└── prompt.md         # 系统 Prompt（交通事故检测专家）
```

## 功能

- 上传或选择示例监控视频进行分析
- 支持 ROI（感兴趣区域）裁剪，聚焦关键画面
- 多模型切换（Qwen3-VL 8B/30B/32B，Instruct/Thinking 版本）
- 实时流式输出：推理过程 + 最终 JSON 结果
- 可编辑系统 Prompt

## 环境要求

- Python 3.10+
- Node.js 18+
- 可用的 vLLM 推理服务（提供 OpenAI 兼容 API）

## 快速启动

### 1. 配置后端

```bash
cd backend

# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt

# 配置环境变量
cp .env.example .env
# 编辑 .env，填入你的 API Key 和 vLLM 服务地址：
#   OPENAI_API_KEY=你的API密钥
#   OPENAI_BASE_URL=你的vLLM服务地址
```

### 2. 启动后端

```bash
cd backend
source .venv/bin/activate
python main.py
```

后端默认运行在 `http://localhost:8000`。

### 3. 配置并启动前端

```bash
cd frontend

# 安装依赖
npm install

# 启动开发服务器
npm run dev
```

前端默认运行在 `http://localhost:5173`。

### 4. 使用

浏览器打开 `http://localhost:5173`，上传视频或选择示例视频，点击 **Start Detection** 开始分析。

## API 说明

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/prompt` | GET | 获取系统 Prompt |
| `/api/models` | GET | 获取可用模型列表 |
| `/api/files` | GET | 获取示例视频列表 |
| `/api/analyze` | POST | 视频分析（流式返回 NDJSON） |

### `/api/analyze` 参数

- `video`: 视频文件（multipart/form-data）
- `prompt`: 系统 Prompt
- `model`: 模型名称
- `roi`: 可选，ROI 区域 JSON（`{"x":0.1,"y":0.2,"w":0.5,"h":0.6}`）

## 输出格式

分析结果为 JSON：

```json
{
  "accident": false,
  "date": "2025-08-18 20:03:24",
  "description": "车流正常通行，未发现异常",
  "congestion": false,
  "confidence": 0.15
}
```

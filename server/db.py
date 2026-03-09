import json
import hashlib
import hmac
import os
import re
import secrets
import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import prompt_registry
from task_intelligence import (
    RETRY_STRATEGY_DEFAULT,
    UNRESOLVED_ISSUE_STATUSES,
    compute_failure_fingerprint,
    cooldown_until_for_streak,
    extract_task_contract_from_description,
    normalize_allowed_surface,
    normalize_issue_list,
)

DB_PATH = Path(__file__).parent.parent / "tasks.db"
CANCELLED_STATUS = "cancelled"
ACTIONABLE_FEEDBACK_STATUSES = {"needs_changes", "blocked"}
FEEDBACK_RESOLVE_STATUSES = {"approved", "pending_acceptance", "completed", CANCELLED_STATUS}
PATCHSET_STATUS_DRAFT = "draft"
PATCHSET_STATUS_SUBMITTED = "submitted"
PATCHSET_STATUS_APPROVED = "approved"
PATCHSET_STATUS_REJECTED = "rejected"
PATCHSET_STATUS_STALE = "stale"
PATCHSET_STATUS_MERGED = "merged"
PATCHSET_STATUS_SUPERSEDED = "superseded"
PATCHSET_ALLOWED_STATUSES = {
    PATCHSET_STATUS_DRAFT,
    PATCHSET_STATUS_SUBMITTED,
    PATCHSET_STATUS_APPROVED,
    PATCHSET_STATUS_REJECTED,
    PATCHSET_STATUS_STALE,
    PATCHSET_STATUS_MERGED,
    PATCHSET_STATUS_SUPERSEDED,
}
PATCHSET_QUEUE_QUEUED = "queued"
PATCHSET_QUEUE_PROCESSING = "processing"
PATCHSET_QUEUE_MERGED = "merged"
PATCHSET_QUEUE_STALE = "stale"
PATCHSET_QUEUE_FAILED = "failed"
PATCHSET_ALLOWED_QUEUE_STATUSES = {
    "",
    PATCHSET_QUEUE_QUEUED,
    PATCHSET_QUEUE_PROCESSING,
    PATCHSET_QUEUE_MERGED,
    PATCHSET_QUEUE_STALE,
    PATCHSET_QUEUE_FAILED,
}
DEFAULT_TASK_PRIORITY = 2
MIN_TASK_PRIORITY = 0
MAX_TASK_PRIORITY = 3
DEPENDENCY_STATE_COMPLETED = "completed"
DEPENDENCY_STATE_APPROVED = "approved"
ALLOWED_DEPENDENCY_STATES = {
    DEPENDENCY_STATE_COMPLETED,
    DEPENDENCY_STATE_APPROVED,
}
ROLE_ADMIN = "admin"
ROLE_USER = "user"
FEEDBACK_REQUEST_STATUS_TODO = "todo"
FEEDBACK_REQUEST_STATUS_IN_PROGRESS = "in_progress"
FEEDBACK_REQUEST_STATUS_COMPLETED = "completed"
FEEDBACK_REQUEST_STATUS_REJECTED = "rejected"
ALLOWED_FEEDBACK_REQUEST_STATUSES = {
    FEEDBACK_REQUEST_STATUS_TODO,
    FEEDBACK_REQUEST_STATUS_IN_PROGRESS,
    FEEDBACK_REQUEST_STATUS_COMPLETED,
    FEEDBACK_REQUEST_STATUS_REJECTED,
}
UNSET = object()
SESSION_TTL_DAYS = 30
LOGIN_MAX_ATTEMPTS = max(1, int(str(os.getenv("LOGIN_MAX_ATTEMPTS", "5")).strip() or "5"))
LOGIN_LOCK_BASE_SECS = max(1, int(str(os.getenv("LOGIN_LOCK_BASE_SECS", "60")).strip() or "60"))
LOGIN_LOCK_MAX_SECS = max(
    LOGIN_LOCK_BASE_SECS,
    int(str(os.getenv("LOGIN_LOCK_MAX_SECS", "86400")).strip() or "86400"),
)


class LeaseConflictError(RuntimeError):
    """Raised when task lease fence validation fails."""


class DependencyValidationError(ValueError):
    """Raised when task dependency payload is invalid."""


class DependencyCycleError(RuntimeError):
    """Raised when task dependency change would introduce a cycle."""


DEVELOPER_PROMPT_DEFAULT = (
    "你是一名专业软件工程师，负责实现以下任务。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. **所有成果必须写入文件**，不要只在终端打印输出\n"
    "   - 代码任务 → 创建对应语言的源文件（.py / .ts / .go 等）\n"
    "   - 文档/方案任务 → 创建 `.md` 文件，把完整内容写入\n"
    "   - 目标是形成可审查的交付物；若本轮无需新增文件，需在交接中写明依据\n\n"
    "2. **质量标准**\n"
    "   - 代码需有适当注释，边界情况需处理\n"
    "   - 文档需完整、结构清晰\n\n"
    "3. **完成定义（必须自检）**\n"
    "   - 任务描述中的“交付物”“验收标准”“关键约束”同样是本轮实现的完成定义\n"
    "   - 提交前逐项核对；缺任何一项都不能算完成\n"
    "   - TODO 步骤只是执行路径，最终以交付物和验收标准是否满足为准\n\n"
    "4. **分支与交接约束**\n"
    "   - 在当前工作分支完成实现并提交，不要自行合并 main\n"
    "   - 提交后由 reviewer/manager 继续流程，不要跳过审查与合并环节\n"
    "   - 不要伪造“已合并/已发布”结论\n\n"
    "5. 直接开始实现，不需要解释计划"
)

REVIEWER_PROMPT_DEFAULT = (
    "你是资深代码/文档审查工程师，负责审查以下变更。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "## 变更内容\n\n"
    "```\n"
    "{diff}\n"
    "```\n\n"
    "## 审查职责\n\n"
    "- 任务描述中的“交付物”“验收标准”“关键约束”同样是你的独立核查清单\n"
    "- 只有所有验收项都有代码、测试、文档或行为证据时，才能 approve\n"
    "- TODO 步骤只用于理解实现路径，不能替代验收标准\n"
    "- request_changes 时，feedback 必须指出未满足的验收项、对应文件或行为以及修复方向\n\n"
    "## 审查要点\n\n"
    "- 是否完整实现了需求描述中的所有要求\n"
    "- 代码/内容是否正确，有无明显错误或遗漏\n"
    "- 代码质量、可读性、边界情况处理\n"
    "- 文件结构是否合理\n\n"
    "## 输出格式\n\n"
    "审查完毕后，在回复最后一行只输出一个 JSON 对象（不要代码块、不要额外文字）：\n"
    '- decision 只能是 "approve" 或 "request_changes"\n'
    '- decision="approve" 时必须提供 comment 字段\n'
    '- decision="request_changes" 时必须提供 feedback 字段'
)

MANAGER_PROMPT_DEFAULT = (
    "你是发布合并管理者，负责把经审查通过的交付版本合并到 main。\n\n"
    "任务标题：{task_title}\n"
    "请优先按 patchset(base..head) 做 deterministic squash merge；只有缺少 patchset 时才回退到已审查的 commit_hash。"
    "不要直接合并分支 HEAD，遇到冲突时停止自动流程并回退开发修复。"
)

LEADER_PROMPT_DEFAULT = (
    "你是项目主管，负责先完善任务需求，再评估是否需要分解与分派执行。请处理以下任务：\n\n"
    "## 任务标题\n{task_title}\n\n"
    "## 任务描述\n{task_description}\n\n"
    "## 可用 Agent 类型\n{agent_list}\n\n"
    "## 评估标准\n"
    "- **简单任务**：可以由单个 agent 独立完成，工作量在 1-2 小时内\n"
    "- **复杂任务**：涉及多个独立功能模块，或需要不同专业技能协作\n"
    "- **信息不足任务**：先在 refined_description 中补齐结构化需求，并标记待确认项\n\n"
    "## 子任务质量门槛（必须满足）\n"
    "1. 子任务必须可独立验收，禁止空泛措辞。\n"
    "2. 每个子任务必须包含：title/objective/todo_steps/deliverables/acceptance_criteria/agent。\n"
    "3. deliverables 要写清文件、接口、页面或脚本等可交付物。\n"
    "4. acceptance_criteria 至少 2 条，必须可验证。\n\n"
    "## 输出格式（严格 JSON，不要任何其他文字）\n\n"
    "如果是简单任务：\n"
    '{"action": "simple", "reason": "一句话说明为何不需要分解", "assignee": "执行该任务的 agent key（如 art_designer）"}\n\n'
    "如果是复杂任务：\n"
    '{"action": "decompose", "subtasks": [\n'
    '  {"title":"子任务标题","objective":"子任务目标","todo_steps":["步骤1","步骤2"],"deliverables":["交付物1"],"acceptance_criteria":["验收1","验收2"],"agent":"developer"}\n'
    "]}"
)

PRODUCT_MANAGER_PROMPT_DEFAULT = (
    "你是一名资深产品经理，负责以下任务的市场调研、需求分析和产品方案设计。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. 产出可执行、可评审的产品文档（市场洞察、用户画像、需求清单、PRD、原型说明等）\n"
    "2. 所有结论需写明依据、假设与边界，不可只给口号式结论\n"
    "3. 所有成果必须写入文件（`.md` / `.csv` / `.json` 等），不要只在终端输出\n"
    "4. 在当前工作分支完成并提交，不要自行合并 main\n"
    "5. 提交后由 reviewer/manager 继续流程，不要跳过审查与合并\n\n"
    "直接开始执行，不需要解释计划。"
)

FINANCE_OFFICER_PROMPT_DEFAULT = (
    "你是一名财务官，负责以下任务的财务审计分析、财务测算与汇报材料输出。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. 输出结构化财务分析（收支、成本、利润、现金流、预算偏差、风险点）\n"
    "2. 明确数据来源、计算口径和关键假设，无法确认的信息要标注待补充\n"
    "3. 所有成果必须写入文件（如 `.md` / `.csv` / `.xlsx` 模板说明）\n"
    "4. 在当前工作分支提交，提交后交由 reviewer/manager 流转\n"
    "5. 不得伪造已审计通过或已对外披露结论\n\n"
    "直接开始执行，不需要解释计划。"
)

LEGAL_COUNSEL_PROMPT_DEFAULT = (
    "你是一名企业法务顾问，负责以下任务的合同审阅、行政文案审阅与法律风险意见。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. 输出条款级审阅意见：风险等级、问题说明、修改建议、替代条款示例\n"
    "2. 对行政/商务文案做合规与法律风险检查，给出可执行修订建议\n"
    "3. 所有成果必须写入文件，确保可追溯、可评审\n"
    "4. 在当前工作分支完成并提交，不要自行合并 main\n"
    "5. 明确哪些结论基于现有信息推断，避免做无依据断言\n\n"
    "直接开始执行，不需要解释计划。"
)

BUSINESS_MANAGER_PROMPT_DEFAULT = (
    "你是一名商务经理，负责以下任务的商务策略、合作方案、报价与推进计划。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. 输出可落地的商务方案：目标、路径、资源需求、里程碑、风险与备选策略\n"
    "2. 如涉及报价/谈判，需写清假设、区间、让步边界和决策条件\n"
    "3. 所有成果必须写入文件，便于审查与复盘\n"
    "4. 在当前工作分支提交，并交由 reviewer/manager 后续处理\n"
    "5. 不得伪造客户确认、签约或回款等结果\n\n"
    "直接开始执行，不需要解释计划。"
)

BID_WRITER_PROMPT_DEFAULT = (
    "你是一名标书制作员，负责以下任务的投标响应材料编写与一致性检查。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. 输出完整、可提交的标书材料结构（技术响应、商务响应、实施计划、资质清单等）\n"
    "2. 对招标要求逐条做响应矩阵，标注已满足/部分满足/待补充\n"
    "3. 所有成果必须写入文件，避免仅终端文本\n"
    "4. 在当前工作分支完成并提交，按流程交接 reviewer/manager\n"
    "5. 不得虚构资质、业绩、证书或承诺内容\n\n"
    "直接开始执行，不需要解释计划。"
)

RISK_COMPLIANCE_PROMPT_DEFAULT = (
    "你是一名风控/合规专员，负责以下任务的风险识别、合规审查与整改建议。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. 输出风险清单（风险描述、触发条件、影响评估、概率、优先级）\n"
    "2. 输出合规建议（适用规则、控制措施、监控指标、整改计划）\n"
    "3. 明确哪些判断基于推断，哪些有明确依据\n"
    "4. 所有成果必须写入文件，在当前分支完成并提交\n"
    "5. 提交后交由 reviewer/manager 继续流程，不要跳过审查\n\n"
    "直接开始执行，不需要解释计划。"
)

ADMIN_SPECIALIST_PROMPT_DEFAULT = (
    "你是一名行政专员，负责以下任务的通用文书与内部行政材料输出。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. 输出结构完整的行政文书（通知、请示、会议纪要、制度草案、流程说明等）\n"
    "2. 术语、格式、日期、责任人和执行时间必须清晰，避免模糊表述\n"
    "3. 所有成果必须写入文件（`.md` / `.docx` 模板说明 / `.csv` 等）\n"
    "4. 在当前工作分支完成并提交，不要自行合并 main\n"
    "5. 如信息缺失，请标注“待确认”并列出最小补充清单\n\n"
    "直接开始执行，不需要解释计划。"
)

MARKETING_SPECIALIST_PROMPT_DEFAULT = (
    "你是一名市场专员，负责以下任务的市场调研、活动策划与推广文案输出。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. 输出可落地的市场方案（目标客群、渠道策略、预算拆分、里程碑、复盘指标）\n"
    "2. 给出关键假设、数据依据和风险提示，不可只写口号\n"
    "3. 涉及宣传内容时，需给出多版本文案与适用场景\n"
    "4. 所有成果必须写入文件并提交到当前工作分支\n"
    "5. 提交后交由 reviewer/manager 流转，不要跳过审查\n\n"
    "直接开始执行，不需要解释计划。"
)

ART_DESIGNER_PROMPT_DEFAULT = (
    "你是一名美术设计师，负责以下任务的视觉创意与设计交付（图片、海报、宣传物料、宣传页等）。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. 输出完整视觉方案：设计目标、受众、风格关键词、主视觉构图与配色规范\n"
    "2. 至少提供 2 套可对比方案，并说明各自适用场景与取舍理由\n"
    "3. 尽量产出可直接落地的设计文件（如 `.svg` / `.html` / `.css` / `.md` 资产说明）\n"
    "4. 若任务涉及海报/宣传图，请明确尺寸、比例、文案层级与导出建议\n"
    "5. 所有成果必须写入文件并提交到当前工作分支；提交后交由 reviewer/manager 流转\n\n"
    "直接开始执行，不需要解释计划。"
)

HR_SPECIALIST_PROMPT_DEFAULT = (
    "你是一名人力资源专员，负责以下任务的招聘、培训、绩效与组织制度文档输出。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. 输出结构化 HR 材料（JD、面试评估表、培训计划、绩效模板、制度草案）\n"
    "2. 明确岗位职责、评估标准、流程节点与责任角色\n"
    "3. 所有成果必须写入文件，在当前工作分支提交\n"
    "4. 涉及政策或法规内容时，需标注依据与待法务确认项\n"
    "5. 提交后按 reviewer/manager 流程继续推进\n\n"
    "直接开始执行，不需要解释计划。"
)

OPERATIONS_SPECIALIST_PROMPT_DEFAULT = (
    "你是一名运营专员，负责以下任务的运营执行方案、流程优化与数据复盘文档。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. 输出可执行运营方案（目标、动作、排期、资源、人效指标、复盘机制）\n"
    "2. 拆解关键流程，给出依赖关系、风险点与应急预案\n"
    "3. 所有成果必须写入文件并在当前工作分支提交\n"
    "4. 涉及数据结论时要说明口径、来源和统计周期\n"
    "5. 提交后交由 reviewer/manager 按流程处理\n\n"
    "直接开始执行，不需要解释计划。"
)

CUSTOMER_SERVICE_SPECIALIST_PROMPT_DEFAULT = (
    "你是一名客服专员，负责以下任务的服务话术、工单规范与客户问题闭环方案。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. 输出标准化客服资料（FAQ、SOP、升级路径、话术模板、质检清单）\n"
    "2. 按问题类型定义响应 SLA、升级条件和跟进节点\n"
    "3. 所有成果必须写入文件并提交到当前工作分支\n"
    "4. 对潜在投诉或舆情风险给出预警和处理建议\n"
    "5. 提交后交由 reviewer/manager 继续流转\n\n"
    "直接开始执行，不需要解释计划。"
)

PROCUREMENT_SPECIALIST_PROMPT_DEFAULT = (
    "你是一名采购专员，负责以下任务的询报价、供应商评估与采购合规材料输出。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. 输出采购执行材料（需求清单、比选矩阵、询报价记录、采购建议）\n"
    "2. 明确供应商评价维度（价格、交付、质量、售后、合规）\n"
    "3. 所有成果必须写入文件，在当前工作分支完成并提交\n"
    "4. 标注关键风险、合同关注点与待法务确认项\n"
    "5. 提交后按 reviewer/manager 流程继续推进\n\n"
    "直接开始执行，不需要解释计划。"
)

BUILTIN_PROMPTS = dict(prompt_registry.BUILTIN_PROMPTS)


def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS projects (
            id         TEXT PRIMARY KEY,
            name       TEXT NOT NULL,
            path       TEXT NOT NULL UNIQUE,
            created_by_user_id TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS users (
            id            TEXT PRIMARY KEY,
            username      TEXT NOT NULL UNIQUE,
            password_hash TEXT,
            role          TEXT NOT NULL DEFAULT 'user',
            max_projects  INTEGER,
            max_tasks     INTEGER,
            failed_login_attempts INTEGER NOT NULL DEFAULT 0,
            lock_until    TEXT,
            last_failed_login_at TEXT,
            created_by    TEXT,
            created_at    TEXT NOT NULL,
            onboarding_completed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS sessions (
            id         TEXT PRIMARY KEY,
            user_id    TEXT NOT NULL,
            token_hash TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS feedback_requests (
            id                     TEXT PRIMARY KEY,
            project_id             TEXT REFERENCES projects(id) ON DELETE SET NULL,
            submitter_user_id      TEXT NOT NULL,
            title                  TEXT NOT NULL,
            description            TEXT NOT NULL DEFAULT '',
            normalized_title       TEXT NOT NULL DEFAULT '',
            normalized_description TEXT NOT NULL DEFAULT '',
            status                 TEXT NOT NULL DEFAULT 'todo',
            ai_decision            TEXT NOT NULL DEFAULT '',
            ai_reason              TEXT NOT NULL DEFAULT '',
            admin_feedback         TEXT NOT NULL DEFAULT '',
            updated_by_user_id     TEXT,
            created_at             TEXT NOT NULL,
            updated_at             TEXT NOT NULL,
            reviewed_at            TEXT,
            FOREIGN KEY (submitter_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (updated_by_user_id) REFERENCES users(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS tasks (
            id              TEXT PRIMARY KEY,
            project_id      TEXT REFERENCES projects(id),
            title           TEXT NOT NULL,
            description     TEXT NOT NULL DEFAULT '',
            priority        INTEGER NOT NULL DEFAULT 2,
            status          TEXT NOT NULL DEFAULT 'todo',
            assignee        TEXT,
            claim_run_id    TEXT,
            lease_token     TEXT,
            lease_expires_at TEXT,
            execution_phase TEXT NOT NULL DEFAULT 'contract_ready',
            retry_strategy  TEXT NOT NULL DEFAULT 'default_implement',
            failure_fingerprint TEXT NOT NULL DEFAULT '',
            same_fingerprint_streak INTEGER NOT NULL DEFAULT 0,
            cooldown_until  TEXT,
            review_enabled  INTEGER NOT NULL DEFAULT 1,
            review_feedback TEXT,
            review_feedback_history TEXT NOT NULL DEFAULT '[]',
            commit_hash     TEXT,
            current_patchset_id TEXT,
            current_patchset_status TEXT NOT NULL DEFAULT '',
            merged_patchset_id TEXT,
            current_contract_id TEXT,
            latest_evidence_id TEXT,
            latest_attempt_id TEXT,
            allowed_surface_json TEXT NOT NULL DEFAULT '{}',
            archived        INTEGER NOT NULL DEFAULT 0,
            cancel_reason   TEXT NOT NULL DEFAULT '',
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS task_dependencies (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id            TEXT NOT NULL,
            depends_on_task_id TEXT NOT NULL,
            required_state     TEXT NOT NULL DEFAULT 'approved',
            created_by         TEXT,
            created_at         TEXT NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
            FOREIGN KEY (depends_on_task_id) REFERENCES tasks(id) ON DELETE CASCADE,
            UNIQUE (task_id, depends_on_task_id),
            CHECK (task_id != depends_on_task_id)
        );

        CREATE TABLE IF NOT EXISTS logs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id    TEXT NOT NULL,
            agent      TEXT NOT NULL,
            message    TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks(id)
        );

        CREATE TABLE IF NOT EXISTS agent_outputs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            agent      TEXT NOT NULL,
            project_id TEXT,
            task_id    TEXT,
            run_id     TEXT,
            line       TEXT NOT NULL,
            kind       TEXT NOT NULL DEFAULT 'line',
            event      TEXT NOT NULL DEFAULT 'line',
            exit_code  INTEGER,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS task_handoffs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id      TEXT NOT NULL,
            stage        TEXT NOT NULL,
            from_agent   TEXT NOT NULL,
            to_agent     TEXT,
            status_from  TEXT,
            status_to    TEXT,
            title        TEXT NOT NULL DEFAULT '',
            summary      TEXT NOT NULL DEFAULT '',
            commit_hash  TEXT,
            conclusion   TEXT,
            payload      TEXT NOT NULL DEFAULT '{}',
            artifact_path TEXT,
            created_at   TEXT NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks(id)
        );

        CREATE TABLE IF NOT EXISTS task_patchsets (
            id              TEXT PRIMARY KEY,
            task_id         TEXT NOT NULL,
            source_branch   TEXT NOT NULL DEFAULT '',
            base_sha        TEXT NOT NULL DEFAULT '',
            head_sha        TEXT NOT NULL DEFAULT '',
            commit_count    INTEGER NOT NULL DEFAULT 0,
            commit_list     TEXT NOT NULL DEFAULT '[]',
            changed_files   TEXT NOT NULL DEFAULT '[]',
            artifact_manifest TEXT NOT NULL DEFAULT '{}',
            diff_stat       TEXT NOT NULL DEFAULT '',
            status          TEXT NOT NULL DEFAULT 'draft',
            worktree_clean  INTEGER NOT NULL DEFAULT 1,
            merge_strategy  TEXT NOT NULL DEFAULT '',
            summary         TEXT NOT NULL DEFAULT '',
            artifact_path   TEXT,
            created_by_agent TEXT NOT NULL DEFAULT '',
            queue_status    TEXT NOT NULL DEFAULT '',
            queue_reason    TEXT NOT NULL DEFAULT '',
            queued_at       TEXT,
            queue_started_at TEXT,
            queue_finished_at TEXT,
            approved_at     TEXT,
            merged_at       TEXT,
            reviewed_main_sha TEXT NOT NULL DEFAULT '',
            queue_main_sha  TEXT NOT NULL DEFAULT '',
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
            UNIQUE (task_id, head_sha)
        );

        CREATE TABLE IF NOT EXISTS agent_types (
            id             TEXT PRIMARY KEY,
            key            TEXT NOT NULL UNIQUE,
            name           TEXT NOT NULL,
            description    TEXT NOT NULL DEFAULT '',
            prompt         TEXT NOT NULL DEFAULT '',
            poll_statuses  TEXT NOT NULL DEFAULT '["todo"]',
            next_status    TEXT NOT NULL DEFAULT 'in_review',
            working_status TEXT NOT NULL DEFAULT 'in_progress',
            runtime_profile TEXT NOT NULL DEFAULT '',
            cli            TEXT NOT NULL DEFAULT 'codex',
            is_builtin     INTEGER NOT NULL DEFAULT 0,
            created_at     TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS task_contracts (
            id                  TEXT PRIMARY KEY,
            task_id             TEXT NOT NULL,
            version             INTEGER NOT NULL DEFAULT 1,
            source_hash         TEXT NOT NULL DEFAULT '',
            goal                TEXT NOT NULL DEFAULT '',
            scope_json          TEXT NOT NULL DEFAULT '[]',
            non_scope_json      TEXT NOT NULL DEFAULT '[]',
            constraints_json    TEXT NOT NULL DEFAULT '[]',
            deliverables_json   TEXT NOT NULL DEFAULT '[]',
            acceptance_json     TEXT NOT NULL DEFAULT '[]',
            assumptions_json    TEXT NOT NULL DEFAULT '[]',
            evidence_required_json TEXT NOT NULL DEFAULT '[]',
            allowed_surface_json TEXT NOT NULL DEFAULT '{}',
            created_by          TEXT NOT NULL DEFAULT 'system',
            created_at          TEXT NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
            UNIQUE (task_id, version)
        );

        CREATE TABLE IF NOT EXISTS task_issues (
            id              TEXT PRIMARY KEY,
            task_id         TEXT NOT NULL,
            issue_key       TEXT NOT NULL,
            source          TEXT NOT NULL DEFAULT 'system',
            stage           TEXT NOT NULL DEFAULT '',
            acceptance_item TEXT NOT NULL DEFAULT '',
            severity        TEXT NOT NULL DEFAULT 'medium',
            category        TEXT NOT NULL DEFAULT 'other',
            summary         TEXT NOT NULL DEFAULT '',
            reproducer      TEXT NOT NULL DEFAULT '',
            evidence_gap    TEXT NOT NULL DEFAULT '',
            scope           TEXT NOT NULL DEFAULT '',
            fix_hint        TEXT NOT NULL DEFAULT '',
            status          TEXT NOT NULL DEFAULT 'open',
            resolution      TEXT NOT NULL DEFAULT '',
            attempt_id      TEXT,
            first_seen_at   TEXT NOT NULL,
            last_seen_at    TEXT NOT NULL,
            resolved_at     TEXT,
            metadata_json   TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
            UNIQUE (task_id, issue_key)
        );

        CREATE TABLE IF NOT EXISTS task_attempts (
            id                  TEXT PRIMARY KEY,
            task_id             TEXT NOT NULL,
            stage               TEXT NOT NULL DEFAULT '',
            outcome             TEXT NOT NULL DEFAULT '',
            execution_phase     TEXT NOT NULL DEFAULT '',
            retry_strategy      TEXT NOT NULL DEFAULT '',
            failure_fingerprint TEXT NOT NULL DEFAULT '',
            same_fingerprint_streak INTEGER NOT NULL DEFAULT 0,
            summary             TEXT NOT NULL DEFAULT '',
            artifact_path       TEXT,
            metadata_json       TEXT NOT NULL DEFAULT '{}',
            created_by          TEXT NOT NULL DEFAULT 'system',
            created_at          TEXT NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS task_evidence (
            id              TEXT PRIMARY KEY,
            task_id         TEXT NOT NULL,
            stage           TEXT NOT NULL DEFAULT '',
            attempt_id      TEXT,
            summary         TEXT NOT NULL DEFAULT '',
            evidence_json   TEXT NOT NULL DEFAULT '{}',
            artifact_path   TEXT,
            created_by      TEXT NOT NULL DEFAULT 'system',
            created_at      TEXT NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
        );

    """)

    # Migrations for existing DBs: ensure all runtime-required columns exist.
    _ensure_columns(conn, "projects", [
        ("name", "TEXT NOT NULL DEFAULT ''"),
        ("path", "TEXT NOT NULL DEFAULT ''"),
        ("created_by_user_id", "TEXT"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "users", [
        ("username", "TEXT NOT NULL DEFAULT ''"),
        ("password_hash", "TEXT"),
        ("role", "TEXT NOT NULL DEFAULT 'user'"),
        ("max_projects", "INTEGER"),
        ("max_tasks", "INTEGER"),
        ("failed_login_attempts", "INTEGER NOT NULL DEFAULT 0"),
        ("lock_until", "TEXT"),
        ("last_failed_login_at", "TEXT"),
        ("created_by", "TEXT"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ("onboarding_completed_at", "TEXT"),
    ])
    _ensure_columns(conn, "sessions", [
        ("user_id", "TEXT NOT NULL DEFAULT ''"),
        ("token_hash", "TEXT NOT NULL DEFAULT ''"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ("expires_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "feedback_requests", [
        ("project_id", "TEXT"),
        ("submitter_user_id", "TEXT NOT NULL DEFAULT ''"),
        ("title", "TEXT NOT NULL DEFAULT ''"),
        ("description", "TEXT NOT NULL DEFAULT ''"),
        ("normalized_title", "TEXT NOT NULL DEFAULT ''"),
        ("normalized_description", "TEXT NOT NULL DEFAULT ''"),
        ("status", "TEXT NOT NULL DEFAULT 'todo'"),
        ("ai_decision", "TEXT NOT NULL DEFAULT ''"),
        ("ai_reason", "TEXT NOT NULL DEFAULT ''"),
        ("admin_feedback", "TEXT NOT NULL DEFAULT ''"),
        ("updated_by_user_id", "TEXT"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ("updated_at", "TEXT NOT NULL DEFAULT ''"),
        ("reviewed_at", "TEXT"),
    ])
    _ensure_columns(conn, "tasks", [
        ("project_id", "TEXT"),
        ("title", "TEXT NOT NULL DEFAULT ''"),
        ("description", "TEXT NOT NULL DEFAULT ''"),
        ("priority", "INTEGER NOT NULL DEFAULT 2"),
        ("status", "TEXT NOT NULL DEFAULT 'todo'"),
        ("assignee", "TEXT"),
        ("claim_run_id", "TEXT"),
        ("lease_token", "TEXT"),
        ("lease_expires_at", "TEXT"),
        ("execution_phase", "TEXT NOT NULL DEFAULT 'contract_ready'"),
        ("retry_strategy", "TEXT NOT NULL DEFAULT 'default_implement'"),
        ("failure_fingerprint", "TEXT NOT NULL DEFAULT ''"),
        ("same_fingerprint_streak", "INTEGER NOT NULL DEFAULT 0"),
        ("cooldown_until", "TEXT"),
        ("review_enabled", "INTEGER NOT NULL DEFAULT 1"),
        ("review_feedback", "TEXT"),
        ("review_feedback_history", "TEXT NOT NULL DEFAULT '[]'"),
        ("commit_hash", "TEXT"),
        ("current_patchset_id", "TEXT"),
        ("current_patchset_status", "TEXT NOT NULL DEFAULT ''"),
        ("merged_patchset_id", "TEXT"),
        ("current_contract_id", "TEXT"),
        ("latest_evidence_id", "TEXT"),
        ("latest_attempt_id", "TEXT"),
        ("allowed_surface_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("archived", "INTEGER NOT NULL DEFAULT 0"),
        ("cancel_reason", "TEXT NOT NULL DEFAULT ''"),
        ("parent_task_id", "TEXT"),
        ("subtask_order", "INTEGER NOT NULL DEFAULT 0"),
        ("assigned_agent", "TEXT"),
        ("dev_agent", "TEXT"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ("updated_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "task_dependencies", [
        ("task_id", "TEXT NOT NULL DEFAULT ''"),
        ("depends_on_task_id", "TEXT NOT NULL DEFAULT ''"),
        ("required_state", "TEXT NOT NULL DEFAULT 'approved'"),
        ("created_by", "TEXT"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "logs", [
        ("task_id", "TEXT"),
        ("agent", "TEXT"),
        ("message", "TEXT"),
        ("created_at", "TEXT"),
    ])
    _ensure_columns(conn, "agent_outputs", [
        ("agent", "TEXT"),
        ("project_id", "TEXT"),
        ("task_id", "TEXT"),
        ("run_id", "TEXT"),
        ("line", "TEXT"),
        ("kind", "TEXT NOT NULL DEFAULT 'line'"),
        ("event", "TEXT NOT NULL DEFAULT 'line'"),
        ("exit_code", "INTEGER"),
        ("created_at", "TEXT"),
    ])
    _ensure_columns(conn, "task_handoffs", [
        ("task_id", "TEXT"),
        ("stage", "TEXT NOT NULL DEFAULT ''"),
        ("from_agent", "TEXT NOT NULL DEFAULT ''"),
        ("to_agent", "TEXT"),
        ("status_from", "TEXT"),
        ("status_to", "TEXT"),
        ("title", "TEXT NOT NULL DEFAULT ''"),
        ("summary", "TEXT NOT NULL DEFAULT ''"),
        ("commit_hash", "TEXT"),
        ("conclusion", "TEXT"),
        ("payload", "TEXT NOT NULL DEFAULT '{}'"),
        ("artifact_path", "TEXT"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "task_patchsets", [
        ("task_id", "TEXT NOT NULL DEFAULT ''"),
        ("source_branch", "TEXT NOT NULL DEFAULT ''"),
        ("base_sha", "TEXT NOT NULL DEFAULT ''"),
        ("head_sha", "TEXT NOT NULL DEFAULT ''"),
        ("commit_count", "INTEGER NOT NULL DEFAULT 0"),
        ("commit_list", "TEXT NOT NULL DEFAULT '[]'"),
        ("changed_files", "TEXT NOT NULL DEFAULT '[]'"),
        ("artifact_manifest", "TEXT NOT NULL DEFAULT '{}'"),
        ("diff_stat", "TEXT NOT NULL DEFAULT ''"),
        ("status", "TEXT NOT NULL DEFAULT 'draft'"),
        ("worktree_clean", "INTEGER NOT NULL DEFAULT 1"),
        ("merge_strategy", "TEXT NOT NULL DEFAULT ''"),
        ("summary", "TEXT NOT NULL DEFAULT ''"),
        ("artifact_path", "TEXT"),
        ("created_by_agent", "TEXT NOT NULL DEFAULT ''"),
        ("queue_status", "TEXT NOT NULL DEFAULT ''"),
        ("queue_reason", "TEXT NOT NULL DEFAULT ''"),
        ("queued_at", "TEXT"),
        ("queue_started_at", "TEXT"),
        ("queue_finished_at", "TEXT"),
        ("approved_at", "TEXT"),
        ("merged_at", "TEXT"),
        ("reviewed_main_sha", "TEXT NOT NULL DEFAULT ''"),
        ("queue_main_sha", "TEXT NOT NULL DEFAULT ''"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ("updated_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "agent_types", [
        ("key", "TEXT"),
        ("name", "TEXT"),
        ("description", "TEXT NOT NULL DEFAULT ''"),
        ("prompt", "TEXT NOT NULL DEFAULT ''"),
        ("poll_statuses", "TEXT NOT NULL DEFAULT '[\"todo\"]'"),
        ("next_status", "TEXT NOT NULL DEFAULT 'in_review'"),
        ("working_status", "TEXT NOT NULL DEFAULT 'in_progress'"),
        ("runtime_profile", "TEXT NOT NULL DEFAULT ''"),
        ("cli", "TEXT NOT NULL DEFAULT 'codex'"),
        ("is_builtin", "INTEGER NOT NULL DEFAULT 0"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "task_contracts", [
        ("task_id", "TEXT NOT NULL DEFAULT ''"),
        ("version", "INTEGER NOT NULL DEFAULT 1"),
        ("source_hash", "TEXT NOT NULL DEFAULT ''"),
        ("goal", "TEXT NOT NULL DEFAULT ''"),
        ("scope_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("non_scope_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("constraints_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("deliverables_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("acceptance_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("assumptions_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("evidence_required_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("allowed_surface_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("created_by", "TEXT NOT NULL DEFAULT 'system'"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "task_issues", [
        ("task_id", "TEXT NOT NULL DEFAULT ''"),
        ("issue_key", "TEXT NOT NULL DEFAULT ''"),
        ("source", "TEXT NOT NULL DEFAULT 'system'"),
        ("stage", "TEXT NOT NULL DEFAULT ''"),
        ("acceptance_item", "TEXT NOT NULL DEFAULT ''"),
        ("severity", "TEXT NOT NULL DEFAULT 'medium'"),
        ("category", "TEXT NOT NULL DEFAULT 'other'"),
        ("summary", "TEXT NOT NULL DEFAULT ''"),
        ("reproducer", "TEXT NOT NULL DEFAULT ''"),
        ("evidence_gap", "TEXT NOT NULL DEFAULT ''"),
        ("scope", "TEXT NOT NULL DEFAULT ''"),
        ("fix_hint", "TEXT NOT NULL DEFAULT ''"),
        ("status", "TEXT NOT NULL DEFAULT 'open'"),
        ("resolution", "TEXT NOT NULL DEFAULT ''"),
        ("attempt_id", "TEXT"),
        ("first_seen_at", "TEXT NOT NULL DEFAULT ''"),
        ("last_seen_at", "TEXT NOT NULL DEFAULT ''"),
        ("resolved_at", "TEXT"),
        ("metadata_json", "TEXT NOT NULL DEFAULT '{}'"),
    ])
    _ensure_columns(conn, "task_attempts", [
        ("task_id", "TEXT NOT NULL DEFAULT ''"),
        ("stage", "TEXT NOT NULL DEFAULT ''"),
        ("outcome", "TEXT NOT NULL DEFAULT ''"),
        ("execution_phase", "TEXT NOT NULL DEFAULT ''"),
        ("retry_strategy", "TEXT NOT NULL DEFAULT ''"),
        ("failure_fingerprint", "TEXT NOT NULL DEFAULT ''"),
        ("same_fingerprint_streak", "INTEGER NOT NULL DEFAULT 0"),
        ("summary", "TEXT NOT NULL DEFAULT ''"),
        ("artifact_path", "TEXT"),
        ("metadata_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("created_by", "TEXT NOT NULL DEFAULT 'system'"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "task_evidence", [
        ("task_id", "TEXT NOT NULL DEFAULT ''"),
        ("stage", "TEXT NOT NULL DEFAULT ''"),
        ("attempt_id", "TEXT"),
        ("summary", "TEXT NOT NULL DEFAULT ''"),
        ("evidence_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("artifact_path", "TEXT"),
        ("created_by", "TEXT NOT NULL DEFAULT 'system'"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
    ])

    # Build indexes after column backfill to keep old DBs migration-safe.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_feedback_requests_submitter_created "
        "ON feedback_requests(submitter_user_id, created_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_feedback_requests_status_updated "
        "ON feedback_requests(status, updated_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_feedback_requests_project "
        "ON feedback_requests(project_id)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_outputs_agent_id ON agent_outputs(agent, id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_outputs_project_id ON agent_outputs(project_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_agent_outputs_created_at ON agent_outputs(created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_project_priority_updated ON tasks(project_id, priority, updated_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_claim_cooldown ON tasks(status, archived, cooldown_until, updated_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_dependencies_task ON task_dependencies(task_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_dependencies_depends_on ON task_dependencies(depends_on_task_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_contracts_task_version ON task_contracts(task_id, version DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_issues_task_status ON task_issues(task_id, status, last_seen_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_attempts_task_created ON task_attempts(task_id, created_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_evidence_task_created ON task_evidence(task_id, created_at DESC)")

    _seed_admin_user(conn)
    _cleanup_expired_sessions(conn)
    _seed_builtin_agents(conn)
    _cleanup_orphan_agent_outputs(conn)
    _backfill_task_contracts(conn)
    _recover_reviewer_stuck_tasks(conn)
    _recover_invalid_todo_assignments(conn)
    _backfill_subtask_order(conn)
    _normalize_task_priority(conn)
    _normalize_task_dependency_required_state(conn)
    _backfill_cancel_reasons_from_logs(conn)
    conn.commit()
    conn.close()


def _ensure_columns(conn, table: str, columns: list[tuple[str, str]]):
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for col, defn in columns:
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")


def _seed_admin_user(conn):
    now = _now()
    row = conn.execute("SELECT id FROM users WHERE username='admin'").fetchone()
    if not row:
        conn.execute(
            """
            INSERT INTO users (id, username, password_hash, role, created_by, created_at)
            VALUES (?, 'admin', NULL, ?, NULL, ?)
            """,
            (str(uuid.uuid4()), ROLE_ADMIN, now),
        )
        return
    conn.execute(
        """
        UPDATE users
           SET role=?
         WHERE username='admin'
           AND COALESCE(TRIM(role), '') != ?
        """,
        (ROLE_ADMIN, ROLE_ADMIN),
    )


def _cleanup_expired_sessions(conn):
    conn.execute("DELETE FROM sessions WHERE expires_at <= ?", (_now(),))


def _cleanup_orphan_agent_outputs(conn):
    """
    Remove terminal rows belonging to deleted/non-existent agent types.
    """
    conn.execute(
        """
        DELETE FROM agent_outputs
         WHERE agent NOT IN (SELECT key FROM agent_types)
        """
    )


def _seed_builtin_agents(conn):
    """Insert built-in agent records if they don't exist yet."""
    builtins = [
        {
            "key": "developer",
            "name": "开发者",
            "description": "实现任务需求，在 agent/<agent> 工作分支提交并交接审查",
            "prompt": BUILTIN_PROMPTS["developer"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
            "runtime_profile": "developer",
        },
        {
            "key": "reviewer",
            "name": "审查者",
            "description": "审查代码变更，决定通过或要求修改",
            "prompt": BUILTIN_PROMPTS["reviewer"],
            "poll_statuses": '["in_review"]',
            "next_status": "approved",
            "working_status": "reviewing",
        },
        {
            "key": "manager",
            "name": "合并管理者",
            "description": "优先基于 patchset 做 deterministic squash merge；缺少 patchset 时回退到 commit 路径",
            "prompt": BUILTIN_PROMPTS["manager"],
            "poll_statuses": '["approved"]',
            "next_status": "pending_acceptance",
            "working_status": "merging",
        },
        {
            "key": "leader",
            "name": "主管",
            "description": "负责完善任务需求并统筹分派：简单任务直接推进执行，复杂任务分解为可验收子任务",
            "poll_statuses": '["triage","decompose"]',
            "next_status": "decomposed",
            "working_status": "triaging",
            "prompt": BUILTIN_PROMPTS["leader"],
        },
        {
            "key": "product_manager",
            "name": "产品经理",
            "description": "负责市场调研、需求分析、产品方案与设计文档输出",
            "prompt": BUILTIN_PROMPTS["product_manager"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "finance_officer",
            "name": "财务官",
            "description": "负责财务审计分析、财务测算与财务汇报文档",
            "prompt": BUILTIN_PROMPTS["finance_officer"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "legal_counsel",
            "name": "法务顾问",
            "description": "负责合同审阅、行政文案审阅与法律风险意见输出",
            "prompt": BUILTIN_PROMPTS["legal_counsel"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "business_manager",
            "name": "商务经理",
            "description": "负责商务策略、合作方案、报价与推进计划",
            "prompt": BUILTIN_PROMPTS["business_manager"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "bid_writer",
            "name": "标书制作员",
            "description": "负责投标响应材料编写、响应矩阵与一致性检查",
            "prompt": BUILTIN_PROMPTS["bid_writer"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "risk_compliance_officer",
            "name": "风控合规专员",
            "description": "负责风险识别、合规审查、控制措施与整改建议",
            "prompt": BUILTIN_PROMPTS["risk_compliance_officer"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "admin_specialist",
            "name": "行政专员",
            "description": "负责通用行政文书、通知通告、会议纪要与制度流程文档",
            "prompt": BUILTIN_PROMPTS["admin_specialist"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "marketing_specialist",
            "name": "市场专员",
            "description": "负责市场调研、品牌传播、活动策划与推广复盘",
            "prompt": BUILTIN_PROMPTS["marketing_specialist"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "art_designer",
            "name": "美术设计师",
            "description": "负责图片、海报、宣传物料与视觉风格设计产出",
            "prompt": BUILTIN_PROMPTS["art_designer"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "hr_specialist",
            "name": "人力资源专员",
            "description": "负责人力招聘、面试评估、培训方案与组织制度文档",
            "prompt": BUILTIN_PROMPTS["hr_specialist"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "operations_specialist",
            "name": "运营专员",
            "description": "负责运营策略、流程执行、数据复盘与改进方案",
            "prompt": BUILTIN_PROMPTS["operations_specialist"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "customer_service_specialist",
            "name": "客服专员",
            "description": "负责客服话术、工单处理规范与客户反馈闭环",
            "prompt": BUILTIN_PROMPTS["customer_service_specialist"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "procurement_specialist",
            "name": "采购专员",
            "description": "负责采购比选、询报价、供应商管理与采购合规文档",
            "prompt": BUILTIN_PROMPTS["procurement_specialist"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
    ]
    now = _now()
    for b in builtins:
        exists = conn.execute("SELECT id FROM agent_types WHERE key=?", (b["key"],)).fetchone()
        if not exists:
            conn.execute(
                """INSERT INTO agent_types
                   (id,key,name,description,prompt,poll_statuses,next_status,working_status,runtime_profile,cli,is_builtin,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,1,?)""",
                (str(uuid.uuid4()), b["key"], b["name"], b["description"],
                 b.get("prompt", ""),
                 b["poll_statuses"], b["next_status"], b["working_status"], b.get("runtime_profile", ""), "codex", now),
            )
    # Keep built-in agents aligned with current default CLI.
    conn.execute(
        """
        UPDATE agent_types
           SET cli='codex'
         WHERE is_builtin=1
           AND LOWER(TRIM(COALESCE(cli, ''))) IN ('', 'claude')
        """
    )
    # Migrate existing leader record to new triage-aware config
    conn.execute(
        """UPDATE agent_types
           SET poll_statuses='["triage","decompose"]', working_status='triaging'
           WHERE key='leader' AND (poll_statuses='["decompose"]' OR working_status='decomposing')"""
    )
    # Migrate legacy leader naming/description to avoid role misunderstanding.
    conn.execute(
        """UPDATE agent_types
           SET name='主管'
           WHERE key='leader' AND is_builtin=1
             AND (TRIM(COALESCE(name, ''))='' OR name='分解专家' OR LOWER(name)='leader')"""
    )
    conn.execute(
        """UPDATE agent_types
           SET description=?
           WHERE key='leader' AND is_builtin=1
             AND (
                TRIM(COALESCE(description, ''))=''
                OR INSTR(description, '复杂度') > 0
                OR INSTR(description, '分解为子任务') > 0
             )""",
        ("负责完善任务需求并统筹分派：简单任务直接推进执行，复杂任务分解为可验收子任务",),
    )
    # Migrate legacy rows where built-in prompt was empty.
    for key, prompt in BUILTIN_PROMPTS.items():
        conn.execute(
            """UPDATE agent_types
               SET prompt=?
               WHERE key=? AND is_builtin=1 AND TRIM(COALESCE(prompt, ''))=''""",
            (prompt, key),
        )
    # Migrate legacy reviewer prompt that embedded example JSON bodies; those
    # examples can be echoed by CLI and misparsed as real decisions.
    conn.execute(
        """UPDATE agent_types
           SET prompt=?
           WHERE key='reviewer' AND is_builtin=1
             AND INSTR(prompt, '条列说明需要修改的具体内容') > 0""",
        (BUILTIN_PROMPTS["reviewer"],),
    )
    # Migrate legacy leader prompt with weak/free-form subtask spec.
    conn.execute(
        """UPDATE agent_types
           SET prompt=?
           WHERE key='leader' AND is_builtin=1
             AND INSTR(prompt, '输出格式') > 0
             AND INSTR(prompt, '子任务质量门槛') = 0
             AND (INSTR(prompt, 'acceptance_criteria') = 0 OR INSTR(prompt, 'todo_steps') = 0)""",
        (BUILTIN_PROMPTS["leader"],),
    )
    # Migrate legacy leader prompt title to主管语义（仅在仍使用旧默认措辞时覆盖）。
    conn.execute(
        """UPDATE agent_types
           SET prompt=?
           WHERE key='leader' AND is_builtin=1
             AND INSTR(prompt, '项目评估与分解专家') > 0""",
        (BUILTIN_PROMPTS["leader"],),
    )
    # Migrate leader prompt that lacks simple-task assignee field.
    conn.execute(
        """UPDATE agent_types
           SET prompt=?
           WHERE key='leader' AND is_builtin=1
             AND INSTR(prompt, '"action": "simple"') > 0
             AND INSTR(prompt, '"assignee"') = 0""",
        (BUILTIN_PROMPTS["leader"],),
    )
    # Migrate outdated built-in developer prompt that lacked branch/handoff constraints.
    conn.execute(
        """UPDATE agent_types
           SET prompt=?
           WHERE key='developer' AND is_builtin=1
             AND INSTR(prompt, '所有成果必须写入文件') > 0
             AND INSTR(prompt, '分支与交接约束') = 0""",
        (BUILTIN_PROMPTS["developer"],),
    )
    # Migrate built-in developer prompt to include completion-definition guidance.
    conn.execute(
        """UPDATE agent_types
           SET prompt=?
           WHERE key='developer' AND is_builtin=1
             AND INSTR(prompt, '你是一名专业软件工程师，负责实现以下任务。') > 0
             AND INSTR(prompt, '完成定义（必须自检）') = 0
             AND INSTR(prompt, '分支与交接约束') > 0""",
        (BUILTIN_PROMPTS["developer"],),
    )
    # Migrate strict built-in developer line that required creating at least one file.
    conn.execute(
        """UPDATE agent_types
           SET prompt=REPLACE(
                prompt,
                '   - 至少创建一个文件，否则任务无法通过审查',
                '   - 目标是形成可审查的交付物；若本轮无需新增文件，需在交接中写明依据'
           )
           WHERE key='developer' AND is_builtin=1
             AND INSTR(prompt, '至少创建一个文件，否则任务无法通过审查') > 0""",
    )
    # Migrate built-in manager prompt from commit-only wording to patchset-first merge guidance.
    conn.execute(
        """UPDATE agent_types
           SET prompt=?
           WHERE key='manager' AND is_builtin=1
             AND (
                (INSTR(prompt, '合并到主分支') > 0 AND INSTR(prompt, 'commit_hash') = 0)
                OR (INSTR(prompt, '目标 commit') > 0 AND INSTR(prompt, 'patchset') = 0)
                OR (INSTR(prompt, '只合并已审查的 commit_hash') > 0 AND INSTR(prompt, 'patchset') = 0)
             )""",
        (BUILTIN_PROMPTS["manager"],),
    )
    # Migrate built-in reviewer prompt to spell out independent acceptance responsibility.
    conn.execute(
        """UPDATE agent_types
           SET prompt=?
           WHERE key='reviewer' AND is_builtin=1
             AND INSTR(prompt, '你是资深代码/文档审查工程师，负责审查以下变更。') > 0
             AND INSTR(prompt, '任务描述中的“交付物”“验收标准”“关键约束”同样是你的独立核查清单') = 0
             AND INSTR(prompt, '## 审查要点') > 0""",
        (BUILTIN_PROMPTS["reviewer"],),
    )
    # Migrate outdated built-in descriptions.
    conn.execute(
        """UPDATE agent_types
           SET description=?
           WHERE key='developer' AND is_builtin=1
             AND (INSTR(description, 'dev 分支') > 0 OR INSTR(description, '提交到 dev') > 0)""",
        ("实现任务需求，在 agent/<agent> 工作分支提交并交接审查",),
    )
    conn.execute(
        """UPDATE agent_types
           SET runtime_profile='developer'
           WHERE key='developer' AND is_builtin=1
             AND LOWER(TRIM(COALESCE(runtime_profile, ''))) != 'developer'""",
    )
    conn.execute(
        """UPDATE agent_types
           SET description=?
           WHERE key='manager' AND is_builtin=1
             AND INSTR(description, '合并到主分支') > 0""",
        ("优先基于 patchset 做 deterministic squash merge；缺少 patchset 时回退到 commit 路径",),
    )


def _recover_reviewer_stuck_tasks(conn):
    """Repair historical reviewer-system-error tasks stuck in needs_changes."""
    conn.execute(
        """
        UPDATE tasks
           SET status='blocked',
               assigned_agent='reviewer',
               assignee=NULL,
               updated_at=?
         WHERE status='needs_changes'
           AND assigned_agent='reviewer'
           AND (
                review_feedback LIKE '[系统错误]%'
                OR review_feedback LIKE '[系统错误][review_retry=%'
           )
        """,
        (_now(),),
    )


def _parse_poll_statuses(raw) -> list[str]:
    try:
        data = json.loads(raw or "[]")
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    return [str(x) for x in data]


def _working_statuses(conn) -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT working_status FROM agent_types WHERE TRIM(COALESCE(working_status, '')) != ''"
    ).fetchall()
    out: set[str] = set()
    for row in rows:
        ws = str(row["working_status"] or "").strip()
        if ws:
            out.add(ws)
    return out


def _lease_deadline_iso(ttl_seconds: int) -> str:
    ttl = max(30, int(ttl_seconds))
    return (datetime.utcnow() + timedelta(seconds=ttl)).isoformat()


def _assert_task_fence_in_conn(
    conn,
    task_id: str,
    expected_run_id: str | None = None,
    expected_lease_token: str | None = None,
    strict_if_active: bool = False,
) -> dict | None:
    row = conn.execute(
        "SELECT id, assignee, claim_run_id, lease_token FROM tasks WHERE id=?",
        (task_id,),
    ).fetchone()
    if not row:
        return None

    task = dict(row)
    assignee = str(task.get("assignee") or "").strip()
    active_token = str(task.get("lease_token") or "").strip()
    if not assignee or not active_token:
        return task

    run_id = str(expected_run_id or "").strip()
    token = str(expected_lease_token or "").strip()
    if not run_id or not token:
        if strict_if_active:
            raise LeaseConflictError("任务存在活动租约，缺少 run_id/lease_token")
        return task

    current_run_id = str(task.get("claim_run_id") or "").strip()
    if run_id != current_run_id or token != active_token:
        raise LeaseConflictError("租约已失效或被其他运行接管")
    return task


def validate_task_lease(
    task_id: str,
    expected_run_id: str | None = None,
    expected_lease_token: str | None = None,
    strict_if_active: bool = False,
) -> tuple[bool, str]:
    conn = get_conn()
    try:
        row = _assert_task_fence_in_conn(
            conn,
            task_id=task_id,
            expected_run_id=expected_run_id,
            expected_lease_token=expected_lease_token,
            strict_if_active=strict_if_active,
        )
        if not row:
            return False, "task_not_found"
        return True, "ok"
    except LeaseConflictError as e:
        return False, str(e)
    finally:
        conn.close()


def _todo_pollers(conn) -> set[str]:
    rows = conn.execute("SELECT key, poll_statuses FROM agent_types").fetchall()
    out: set[str] = set()
    for row in rows:
        key = str(row["key"] or "").strip()
        if not key:
            continue
        if "todo" in _parse_poll_statuses(row["poll_statuses"]):
            out.add(key)
    return out


def _recover_invalid_todo_assignments(conn):
    """
    Fix historical rows where todo tasks were assigned to agents
    that do not poll todo (e.g. leader/reviewer/manager), which blocks claiming.
    """
    todo_pollers = _todo_pollers(conn)
    if not todo_pollers:
        return
    rows = conn.execute(
        """
        SELECT id, assigned_agent, dev_agent
          FROM tasks
         WHERE status='todo'
           AND archived=0
           AND assigned_agent IS NOT NULL
           AND TRIM(assigned_agent) != ''
        """
    ).fetchall()
    if not rows:
        return
    now = _now()
    for row in rows:
        assigned = str(row["assigned_agent"] or "").strip()
        if assigned in todo_pollers:
            continue
        dev_agent = str(row["dev_agent"] or "").strip()
        fallback = dev_agent if dev_agent in todo_pollers else None
        conn.execute(
            "UPDATE tasks SET assigned_agent=?, updated_at=? WHERE id=?",
            (fallback, now, row["id"]),
        )


def _backfill_subtask_order(conn):
    """
    Ensure all subtasks under the same parent have deterministic 1..N order.
    Existing explicit order is preferred; missing/legacy order falls back to created_at.
    """
    parents = conn.execute(
        """
        SELECT DISTINCT parent_task_id
          FROM tasks
         WHERE parent_task_id IS NOT NULL
           AND TRIM(parent_task_id) != ''
        """
    ).fetchall()
    for p in parents:
        parent_id = str(p["parent_task_id"] or "").strip()
        if not parent_id:
            continue
        rows = conn.execute(
            """
            SELECT id, subtask_order, created_at
              FROM tasks
             WHERE parent_task_id=?
             ORDER BY
               CASE WHEN COALESCE(subtask_order, 0) > 0 THEN 0 ELSE 1 END ASC,
               COALESCE(subtask_order, 0) ASC,
               created_at ASC,
               id ASC
            """,
            (parent_id,),
        ).fetchall()
        for idx, row in enumerate(rows, 1):
            current = int(row["subtask_order"] or 0)
            if current == idx:
                continue
            conn.execute("UPDATE tasks SET subtask_order=? WHERE id=?", (idx, row["id"]))


def _normalize_task_priority(conn):
    conn.execute(
        """
        UPDATE tasks
           SET priority=?
         WHERE priority IS NULL
            OR priority < ?
            OR priority > ?
        """,
        (DEFAULT_TASK_PRIORITY, MIN_TASK_PRIORITY, MAX_TASK_PRIORITY),
    )


def _normalize_task_dependency_required_state(conn):
    conn.execute(
        """
        UPDATE task_dependencies
           SET required_state=?
         WHERE TRIM(COALESCE(required_state, '')) NOT IN (?, ?)
        """,
        (
            DEPENDENCY_STATE_COMPLETED,
            DEPENDENCY_STATE_COMPLETED,
            DEPENDENCY_STATE_APPROVED,
        ),
    )


def _now():
    return datetime.utcnow().isoformat()


def _utcnow() -> datetime:
    return datetime.utcnow()


def _normalize_priority_value(priority, default: int = DEFAULT_TASK_PRIORITY) -> int:
    try:
        out = int(priority)
    except Exception:
        out = int(default)
    if out < MIN_TASK_PRIORITY:
        return MIN_TASK_PRIORITY
    if out > MAX_TASK_PRIORITY:
        return MAX_TASK_PRIORITY
    return out


def _normalize_dependency_required_state(state: str | None) -> str:
    raw = str(state or "").strip().lower()
    if raw in ALLOWED_DEPENDENCY_STATES:
        return raw
    return DEPENDENCY_STATE_APPROVED


def _dependency_is_satisfied(required_state: str, depends_on_status: str) -> bool:
    req = _normalize_dependency_required_state(required_state)
    dep_status = str(depends_on_status or "").strip().lower()
    if req == DEPENDENCY_STATE_APPROVED:
        return dep_status in {"approved", "pending_acceptance", "completed"}
    return dep_status == "completed"


def _parse_iso_datetime(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    with_errors = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(with_errors)
        if dt.tzinfo is not None:
            offset = dt.utcoffset() or timedelta(0)
            dt = (dt - offset).replace(tzinfo=None)  # normalize to UTC naive
        return dt
    except Exception:
        return None


def _remaining_lock_seconds(lock_until: str | None) -> int:
    lock_dt = _parse_iso_datetime(lock_until)
    if not lock_dt:
        return 0
    remain = int((lock_dt - _utcnow()).total_seconds())
    return remain if remain > 0 else 0


def _compute_lock_seconds(failed_attempts: int) -> int:
    attempts = max(0, int(failed_attempts or 0))
    if attempts < LOGIN_MAX_ATTEMPTS:
        return 0
    level = attempts - LOGIN_MAX_ATTEMPTS
    wait = LOGIN_LOCK_BASE_SECS * (2 ** level)
    return min(LOGIN_LOCK_MAX_SECS, wait)


def _normalize_username(username: str) -> str:
    return str(username or "").strip().lower()


def _normalize_quota_limit(value, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        value = raw
    try:
        out = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} 必须是非负整数或 null")
    if out < 0:
        raise ValueError(f"{field_name} 必须是非负整数或 null")
    return out


def _password_hash(password: str, iterations: int = 260000) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${digest.hex()}"


def _verify_password(password: str, encoded: str | None) -> bool:
    payload = str(encoded or "").strip()
    if not payload:
        return False
    try:
        algo, iter_s, salt_hex, digest_hex = payload.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        iterations = int(iter_s)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(digest_hex)
    except Exception:
        return False
    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(actual, expected)


def _hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _public_user(row) -> dict:
    data = dict(row)
    max_projects = data.get("max_projects")
    max_tasks = data.get("max_tasks")
    return {
        "id": data["id"],
        "username": data["username"],
        "role": data["role"],
        "max_projects": int(max_projects) if max_projects is not None else None,
        "max_tasks": int(max_tasks) if max_tasks is not None else None,
        "created_by": data.get("created_by"),
        "created_at": data.get("created_at"),
        "onboarding_completed_at": data.get("onboarding_completed_at"),
        "password_set": bool(str(data.get("password_hash") or "").strip()),
    }


def admin_password_is_set() -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT password_hash FROM users WHERE username='admin' LIMIT 1"
    ).fetchone()
    conn.close()
    return bool(row and str(row["password_hash"] or "").strip())


def set_admin_initial_password(password: str) -> dict | None:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM users WHERE username='admin' LIMIT 1"
        ).fetchone()
        if not row:
            return None
        if str(row["password_hash"] or "").strip():
            return None
        hashed = _password_hash(password)
        conn.execute(
            "UPDATE users SET password_hash=? WHERE id=?",
            (hashed, row["id"]),
        )
        conn.commit()
        updated = conn.execute("SELECT * FROM users WHERE id=?", (row["id"],)).fetchone()
        return _public_user(updated) if updated else None
    finally:
        conn.close()


def get_user(user_id: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    conn.close()
    return _public_user(row) if row else None


def mark_user_onboarding_completed(user_id: str) -> dict | None:
    conn = get_conn()
    try:
        exists = conn.execute("SELECT id FROM users WHERE id=?", (user_id,)).fetchone()
        if not exists:
            return None
        conn.execute(
            """
            UPDATE users
               SET onboarding_completed_at=COALESCE(onboarding_completed_at, ?)
             WHERE id=?
            """,
            (_now(), user_id),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        return _public_user(row) if row else None
    finally:
        conn.close()


def get_user_by_username(username: str) -> dict | None:
    uname = _normalize_username(username)
    if not uname:
        return None
    conn = get_conn()
    row = conn.execute("SELECT * FROM users WHERE username=?", (uname,)).fetchone()
    conn.close()
    return _public_user(row) if row else None


def authenticate_user(username: str, password: str) -> dict:
    uname = _normalize_username(username)
    if not uname:
        return {"ok": False, "reason": "invalid_credentials"}
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE username=?", (uname,)).fetchone()
        if not row:
            return {"ok": False, "reason": "invalid_credentials"}

        locked_secs = _remaining_lock_seconds(row["lock_until"])
        if locked_secs > 0:
            return {
                "ok": False,
                "reason": "locked",
                "retry_after_secs": locked_secs,
                "failed_attempts": int(row["failed_login_attempts"] or 0),
            }

        if _verify_password(password, row["password_hash"]):
            conn.execute(
                """
                UPDATE users
                   SET failed_login_attempts=0,
                       lock_until=NULL,
                       last_failed_login_at=NULL
                 WHERE id=?
                """,
                (row["id"],),
            )
            conn.commit()
            refreshed = conn.execute("SELECT * FROM users WHERE id=?", (row["id"],)).fetchone()
            return {"ok": True, "user": _public_user(refreshed or row)}

        failed_attempts = int(row["failed_login_attempts"] or 0) + 1
        lock_secs = _compute_lock_seconds(failed_attempts)
        now = _utcnow()
        lock_until = (now + timedelta(seconds=lock_secs)).isoformat() if lock_secs > 0 else None
        conn.execute(
            """
            UPDATE users
               SET failed_login_attempts=?,
                   lock_until=?,
                   last_failed_login_at=?
             WHERE id=?
            """,
            (failed_attempts, lock_until, now.isoformat(), row["id"]),
        )
        conn.commit()
        return {
            "ok": False,
            "reason": "invalid_credentials",
            "retry_after_secs": lock_secs,
            "failed_attempts": failed_attempts,
            "locked": lock_secs > 0,
        }
    finally:
        conn.close()


def change_user_password(user_id: str, current_password: str, new_password: str) -> dict | None:
    current = str(current_password or "")
    new = str(new_password or "")
    if not current:
        raise ValueError("current_password 不能为空")
    if not new:
        raise ValueError("new_password 不能为空")
    if current == new:
        raise ValueError("新密码不能与当前密码相同")

    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        if not row:
            return None
        if not _verify_password(current, row["password_hash"]):
            raise ValueError("当前密码错误")

        conn.execute(
            """
            UPDATE users
               SET password_hash=?,
                   failed_login_attempts=0,
                   lock_until=NULL,
                   last_failed_login_at=NULL
             WHERE id=?
            """,
            (_password_hash(new), user_id),
        )
        conn.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
        conn.commit()
        refreshed = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        return _public_user(refreshed) if refreshed else None
    finally:
        conn.close()


def create_user(
    username: str,
    password: str,
    role: str = ROLE_USER,
    created_by: str | None = None,
    max_projects: int | None = None,
    max_tasks: int | None = None,
) -> dict:
    uname = _normalize_username(username)
    if not uname:
        raise ValueError("username 不能为空")
    if role not in {ROLE_ADMIN, ROLE_USER}:
        raise ValueError("role 不合法")
    normalized_max_projects = _normalize_quota_limit(max_projects, "max_projects")
    normalized_max_tasks = _normalize_quota_limit(max_tasks, "max_tasks")
    now = _now()
    uid = str(uuid.uuid4())
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO users (
                id, username, password_hash, role, max_projects, max_tasks, created_by, created_at
            )
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                uid,
                uname,
                _password_hash(password),
                role,
                normalized_max_projects,
                normalized_max_tasks,
                created_by,
                now,
            ),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
        if not row:
            raise RuntimeError("create_user failed")
        return _public_user(row)
    finally:
        conn.close()


def list_users() -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM users ORDER BY CASE role WHEN 'admin' THEN 0 ELSE 1 END, created_at ASC"
    ).fetchall()
    conn.close()
    return [_public_user(r) for r in rows]


def count_users_by_role(role: str) -> int:
    r = str(role or "").strip().lower()
    if r not in {ROLE_ADMIN, ROLE_USER}:
        return 0
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT COUNT(1) AS cnt FROM users WHERE role=?",
            (r,),
        ).fetchone()
        return int(row["cnt"] or 0) if row else 0
    finally:
        conn.close()


def update_user(
    user_id: str,
    *,
    username: str | None = None,
    password: str | None = None,
    role: str | None = None,
    max_projects=UNSET,
    max_tasks=UNSET,
) -> dict | None:
    fields: list[str] = []
    params: list[object] = []
    revoke_sessions = False

    if username is not None:
        uname = _normalize_username(username)
        if not uname:
            raise ValueError("username 不能为空")
        fields.append("username=?")
        params.append(uname)

    if password is not None:
        pwd = str(password or "")
        if not pwd:
            raise ValueError("password 不能为空")
        fields.append("password_hash=?")
        params.append(_password_hash(pwd))
        fields.append("failed_login_attempts=0")
        fields.append("lock_until=NULL")
        fields.append("last_failed_login_at=NULL")
        revoke_sessions = True

    if role is not None:
        normalized_role = str(role or "").strip().lower()
        if normalized_role not in {ROLE_ADMIN, ROLE_USER}:
            raise ValueError("role 不合法")
        fields.append("role=?")
        params.append(normalized_role)

    if max_projects is not UNSET:
        fields.append("max_projects=?")
        params.append(_normalize_quota_limit(max_projects, "max_projects"))

    if max_tasks is not UNSET:
        fields.append("max_tasks=?")
        params.append(_normalize_quota_limit(max_tasks, "max_tasks"))

    if not fields:
        raise ValueError("至少提供一个可更新字段")

    conn = get_conn()
    try:
        exists = conn.execute("SELECT id FROM users WHERE id=?", (user_id,)).fetchone()
        if not exists:
            return None
        conn.execute(f"UPDATE users SET {', '.join(fields)} WHERE id=?", (*params, user_id))
        if revoke_sessions:
            conn.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
        conn.commit()
        row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        return _public_user(row) if row else None
    finally:
        conn.close()


def delete_user(user_id: str) -> bool:
    conn = get_conn()
    try:
        cur = conn.execute("DELETE FROM users WHERE id=?", (user_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def create_session(user_id: str, ttl_days: int = SESSION_TTL_DAYS) -> dict:
    token = secrets.token_urlsafe(32)
    now = datetime.utcnow()
    expires = now + timedelta(days=max(1, int(ttl_days)))
    row = {
        "id": str(uuid.uuid4()),
        "token": token,
        "token_hash": _hash_session_token(token),
        "user_id": user_id,
        "created_at": now.isoformat(),
        "expires_at": expires.isoformat(),
    }
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO sessions (id, user_id, token_hash, created_at, expires_at)
            VALUES (?,?,?,?,?)
            """,
            (row["id"], row["user_id"], row["token_hash"], row["created_at"], row["expires_at"]),
        )
        conn.commit()
        return {"token": token, "expires_at": row["expires_at"]}
    finally:
        conn.close()


def revoke_session(token: str) -> bool:
    hashed = _hash_session_token(token)
    conn = get_conn()
    cur = conn.execute("DELETE FROM sessions WHERE token_hash=?", (hashed,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def get_session_user(token: str) -> dict | None:
    hashed = _hash_session_token(token)
    conn = get_conn()
    try:
        conn.execute("DELETE FROM sessions WHERE expires_at <= ?", (_now(),))
        row = conn.execute(
            """
            SELECT u.*
              FROM sessions s
              JOIN users u ON u.id = s.user_id
             WHERE s.token_hash=?
               AND s.expires_at > ?
             LIMIT 1
            """,
            (hashed, _now()),
        ).fetchone()
        conn.commit()
        return _public_user(row) if row else None
    finally:
        conn.close()


def _parse_feedback_history(raw) -> list[dict]:
    data = raw
    if isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return []
        try:
            data = json.loads(txt)
        except Exception:
            return []
    if not isinstance(data, list):
        return []

    out: list[dict] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        feedback = str(item.get("feedback") or "").strip()
        if not feedback:
            continue
        resolved_at = str(item.get("resolved_at") or "").strip()
        resolved = bool(item.get("resolved")) or bool(resolved_at)
        out.append(
            {
                "id": str(item.get("id") or "").strip(),
                "created_at": str(item.get("created_at") or "").strip(),
                "source": str(item.get("source") or "system").strip() or "system",
                "status_at": str(item.get("status_at") or "").strip(),
                "stage": str(item.get("stage") or "").strip(),
                "actor": str(item.get("actor") or "").strip(),
                "feedback": feedback[:4000],
                "resolved": resolved,
                "resolved_at": resolved_at,
                "resolved_reason": str(item.get("resolved_reason") or "").strip(),
            }
        )
    return out


def _dump_feedback_history(history: list[dict]) -> str:
    return json.dumps(history, ensure_ascii=False)


def _next_feedback_id(history: list[dict]) -> str:
    max_n = 0
    for item in history:
        fid = str(item.get("id") or "").strip().upper()
        m = re.match(r"^FB(\d+)$", fid)
        if not m:
            continue
        try:
            max_n = max(max_n, int(m.group(1)))
        except Exception:
            continue
    return f"FB{max_n + 1:04d}"


def _resolve_open_feedback(
    history: list[dict],
    resolved_at: str,
    reason: str,
    *,
    source: str = "",
    stage: str = "",
    actor: str = "",
) -> bool:
    changed = False
    for item in history:
        if bool(item.get("resolved")):
            continue
        if source and str(item.get("source") or "").strip() != source:
            continue
        if stage and str(item.get("stage") or "").strip() != stage:
            continue
        if actor and str(item.get("actor") or "").strip() != actor:
            continue
        item["resolved"] = True
        item["resolved_at"] = resolved_at
        item["resolved_reason"] = reason[:80]
        changed = True
    return changed


def _append_feedback_entry(
    history: list[dict],
    feedback: str,
    source: str,
    status_at: str,
    stage: str,
    actor: str,
    created_at: str,
) -> bool:
    text = str(feedback or "").strip()
    if not text:
        return False

    # A new feedback item should only supersede unresolved items from the same
    # review channel/stage. Cross-stage issues (for example manager merge
    # feedback vs reviewer content feedback) must remain open until explicitly
    # resolved by status advancement.
    _resolve_open_feedback(
        history,
        created_at,
        "superseded",
        source=source[:40] or "system",
        stage=stage[:80],
        actor=actor[:80],
    )
    history.append(
        {
            "id": _next_feedback_id(history),
            "created_at": created_at,
            "source": source[:40] or "system",
            "status_at": status_at[:40],
            "stage": stage[:80],
            "actor": actor[:80],
            "feedback": text[:4000],
            "resolved": False,
            "resolved_at": "",
            "resolved_reason": "",
        }
    )
    # Keep payload bounded to avoid unbounded task row growth.
    if len(history) > 120:
        del history[: len(history) - 120]
    return True


def _coerce_json_list(raw) -> list:
    data = raw
    if isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return []
        try:
            data = json.loads(txt)
        except Exception:
            return []
    return data if isinstance(data, list) else []


def _coerce_json_dict(raw) -> dict:
    data = raw
    if isinstance(raw, str):
        txt = raw.strip()
        if not txt:
            return {}
        try:
            data = json.loads(txt)
        except Exception:
            return {}
    return data if isinstance(data, dict) else {}


def _json_dump(value) -> str:
    return json.dumps(value, ensure_ascii=False)


def _normalize_contract_row(row) -> dict | None:
    if not row:
        return None
    data = dict(row)
    return {
        "id": str(data.get("id") or "").strip(),
        "task_id": str(data.get("task_id") or "").strip(),
        "version": int(data.get("version") or 0),
        "source_hash": str(data.get("source_hash") or "").strip(),
        "goal": str(data.get("goal") or "").strip(),
        "scope": _coerce_json_list(data.get("scope_json")),
        "non_scope": _coerce_json_list(data.get("non_scope_json")),
        "constraints": _coerce_json_list(data.get("constraints_json")),
        "deliverables": _coerce_json_list(data.get("deliverables_json")),
        "acceptance": _coerce_json_list(data.get("acceptance_json")),
        "assumptions": _coerce_json_list(data.get("assumptions_json")),
        "evidence_required": _coerce_json_list(data.get("evidence_required_json")),
        "allowed_surface": normalize_allowed_surface(data.get("allowed_surface_json")),
        "created_by": str(data.get("created_by") or "").strip() or "system",
        "created_at": str(data.get("created_at") or "").strip(),
    }


def _task_contract_source_hash(title: str, description: str) -> str:
    seed = f"{str(title or '').strip()}\n{str(description or '').strip()}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


def _latest_task_contract_in_conn(conn, task_id: str):
    return conn.execute(
        """
        SELECT *
          FROM task_contracts
         WHERE task_id=?
         ORDER BY version DESC, created_at DESC
         LIMIT 1
        """,
        (task_id,),
    ).fetchone()


def _sync_task_contract_in_conn(
    conn,
    *,
    task_id: str,
    title: str,
    description: str,
    created_by: str = "system",
) -> dict | None:
    task_id = str(task_id or "").strip()
    if not task_id:
        return None
    normalized_description = str(description or "").strip()
    if not normalized_description:
        return None

    source_hash = _task_contract_source_hash(title, normalized_description)
    latest = _latest_task_contract_in_conn(conn, task_id)
    latest_normalized = _normalize_contract_row(latest)
    if latest_normalized and latest_normalized.get("source_hash") == source_hash:
        contract_row = latest_normalized
    else:
        contract = extract_task_contract_from_description(normalized_description)
        version = int((latest["version"] if latest else 0) or 0) + 1
        cid = str(uuid.uuid4())
        now = _now()
        conn.execute(
            """
            INSERT INTO task_contracts (
                id, task_id, version, source_hash, goal,
                scope_json, non_scope_json, constraints_json,
                deliverables_json, acceptance_json, assumptions_json,
                evidence_required_json, allowed_surface_json,
                created_by, created_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                cid,
                task_id,
                version,
                source_hash,
                str(contract.get("goal") or "").strip(),
                _json_dump(contract.get("scope") or []),
                _json_dump(contract.get("non_scope") or []),
                _json_dump(contract.get("constraints") or []),
                _json_dump(contract.get("deliverables") or []),
                _json_dump(contract.get("acceptance") or []),
                _json_dump(contract.get("assumptions") or []),
                _json_dump(contract.get("evidence_required") or []),
                _json_dump(contract.get("allowed_surface") or {}),
                str(created_by or "system")[:80] or "system",
                now,
            ),
        )
        contract_row = _normalize_contract_row(
            conn.execute("SELECT * FROM task_contracts WHERE id=?", (cid,)).fetchone()
        )

    task_row = conn.execute(
        "SELECT allowed_surface_json FROM tasks WHERE id=?",
        (task_id,),
    ).fetchone()
    existing_allowed_surface = normalize_allowed_surface(
        task_row["allowed_surface_json"] if task_row else {}
    )
    next_allowed_surface = existing_allowed_surface
    if not any(existing_allowed_surface.values()):
        next_allowed_surface = normalize_allowed_surface(contract_row.get("allowed_surface") or {})
    conn.execute(
        """
        UPDATE tasks
           SET current_contract_id=?,
               allowed_surface_json=?,
               updated_at=?
         WHERE id=?
        """,
        (
            contract_row.get("id"),
            _json_dump(next_allowed_surface),
            _now(),
            task_id,
        ),
    )
    return contract_row


def _backfill_task_contracts(conn) -> None:
    rows = conn.execute(
        """
        SELECT id, title, description
          FROM tasks
         WHERE TRIM(COALESCE(description, '')) != ''
        """
    ).fetchall()
    for row in rows:
        try:
            _sync_task_contract_in_conn(
                conn,
                task_id=row["id"],
                title=row["title"],
                description=row["description"],
                created_by="migration",
            )
        except Exception:
            continue


def _normalize_issue_row(row) -> dict | None:
    if not row:
        return None
    data = dict(row)
    resolved_at = str(data.get("resolved_at") or "").strip()
    status = str(data.get("status") or "open").strip().lower() or "open"
    return {
        "id": str(data.get("id") or "").strip(),
        "task_id": str(data.get("task_id") or "").strip(),
        "issue_id": str(data.get("issue_key") or "").strip(),
        "source": str(data.get("source") or "").strip() or "system",
        "stage": str(data.get("stage") or "").strip(),
        "acceptance_item": str(data.get("acceptance_item") or "").strip(),
        "severity": str(data.get("severity") or "medium").strip().lower(),
        "category": str(data.get("category") or "other").strip().lower(),
        "summary": str(data.get("summary") or "").strip(),
        "reproducer": str(data.get("reproducer") or "").strip(),
        "evidence_gap": str(data.get("evidence_gap") or "").strip(),
        "scope": str(data.get("scope") or "").strip(),
        "fix_hint": str(data.get("fix_hint") or "").strip(),
        "status": status,
        "resolution": str(data.get("resolution") or "").strip(),
        "attempt_id": str(data.get("attempt_id") or "").strip(),
        "first_seen_at": str(data.get("first_seen_at") or "").strip(),
        "last_seen_at": str(data.get("last_seen_at") or "").strip(),
        "resolved_at": resolved_at,
        "metadata": _coerce_json_dict(data.get("metadata_json")),
        "resolved": bool(resolved_at) or status not in UNRESOLVED_ISSUE_STATUSES,
    }


def _list_task_issues_in_conn(
    conn,
    task_id: str,
    *,
    include_resolved: bool = True,
) -> list[dict]:
    sql = "SELECT * FROM task_issues WHERE task_id=?"
    params: list[object] = [task_id]
    if not include_resolved:
        sql += " AND (resolved_at IS NULL OR TRIM(COALESCE(resolved_at, ''))='') AND status NOT IN ('resolved', 'wont_fix')"
    sql += " ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, last_seen_at DESC, first_seen_at ASC"
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [item for item in (_normalize_issue_row(row) for row in rows) if item]


def _resolve_task_issues_in_conn(
    conn,
    task_id: str,
    *,
    source: str | None = None,
    stage: str | None = None,
    resolution: str,
    attempt_id: str = "",
) -> int:
    where = [
        "task_id=?",
        "(resolved_at IS NULL OR TRIM(COALESCE(resolved_at, ''))='')",
        "status NOT IN ('resolved', 'wont_fix')",
    ]
    params: list[object] = [task_id]
    if source:
        where.append("source=?")
        params.append(source)
    if stage:
        where.append("stage=?")
        params.append(stage)
    now = _now()
    sql = (
        "UPDATE task_issues SET status='resolved', resolution=?, resolved_at=?, "
        "last_seen_at=?, attempt_id=CASE WHEN ? != '' THEN ? ELSE attempt_id END "
        f"WHERE {' AND '.join(where)}"
    )
    cur = conn.execute(sql, (resolution[:160], now, now, attempt_id, attempt_id, *params))
    return int(cur.rowcount or 0)


def _sync_task_issues_in_conn(
    conn,
    *,
    task_id: str,
    source: str,
    stage: str,
    issues: list[dict],
    attempt_id: str = "",
    resolve_missing: bool = True,
) -> list[dict]:
    normalized = normalize_issue_list(issues)
    existing_rows = conn.execute(
        "SELECT * FROM task_issues WHERE task_id=? AND source=? AND stage=?",
        (task_id, source, stage),
    ).fetchall()
    existing_by_key = {str(row["issue_key"] or "").strip(): row for row in existing_rows}
    now = _now()
    seen_keys: set[str] = set()
    for item in normalized:
        issue_key = str(item.get("issue_id") or "").strip()
        if not issue_key:
            continue
        seen_keys.add(issue_key)
        existing = existing_by_key.get(issue_key)
        status = str(item.get("status") or "open").strip().lower()
        if status not in {"resolved", "wont_fix"}:
            if existing and not str(existing["resolved_at"] or "").strip():
                if status == "new":
                    status = "persisting"
            elif status not in {"new", "persisting"}:
                status = "new"
        resolved_at = now if status in {"resolved", "wont_fix"} else None
        resolution = "reviewer_marked_resolved" if status == "resolved" else ("wont_fix" if status == "wont_fix" else "")
        metadata_json = _json_dump({"raw_status": item.get("status") or status})
        if existing:
            conn.execute(
                """
                UPDATE task_issues
                   SET acceptance_item=?,
                       severity=?,
                       category=?,
                       summary=?,
                       reproducer=?,
                       evidence_gap=?,
                       scope=?,
                       fix_hint=?,
                       status=?,
                       resolution=?,
                       attempt_id=CASE WHEN ? != '' THEN ? ELSE attempt_id END,
                       last_seen_at=?,
                       resolved_at=?,
                       metadata_json=?
                 WHERE id=?
                """,
                (
                    item.get("acceptance_item", ""),
                    item.get("severity", "medium"),
                    item.get("category", "other"),
                    item.get("summary", ""),
                    item.get("reproducer", ""),
                    item.get("evidence_gap", ""),
                    item.get("scope", ""),
                    item.get("fix_hint", ""),
                    status,
                    resolution,
                    attempt_id,
                    attempt_id,
                    now,
                    resolved_at,
                    metadata_json,
                    existing["id"],
                ),
            )
        else:
            issue_row_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO task_issues (
                    id, task_id, issue_key, source, stage,
                    acceptance_item, severity, category, summary,
                    reproducer, evidence_gap, scope, fix_hint,
                    status, resolution, attempt_id,
                    first_seen_at, last_seen_at, resolved_at, metadata_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    issue_row_id,
                    task_id,
                    issue_key,
                    source,
                    stage,
                    item.get("acceptance_item", ""),
                    item.get("severity", "medium"),
                    item.get("category", "other"),
                    item.get("summary", ""),
                    item.get("reproducer", ""),
                    item.get("evidence_gap", ""),
                    item.get("scope", ""),
                    item.get("fix_hint", ""),
                    status,
                    resolution,
                    attempt_id or None,
                    now,
                    now,
                    resolved_at,
                    metadata_json,
                ),
            )
    if resolve_missing:
        for row in existing_rows:
            issue_key = str(row["issue_key"] or "").strip()
            if issue_key in seen_keys:
                continue
            if str(row["resolved_at"] or "").strip():
                continue
            conn.execute(
                """
                UPDATE task_issues
                   SET status='resolved',
                       resolution='absent_from_latest_review',
                       resolved_at=?,
                       last_seen_at=?,
                       attempt_id=CASE WHEN ? != '' THEN ? ELSE attempt_id END
                 WHERE id=?
                """,
                (now, now, attempt_id, attempt_id, row["id"]),
            )
    return _list_task_issues_in_conn(conn, task_id, include_resolved=False)


def _normalize_attempt_row(row) -> dict | None:
    if not row:
        return None
    data = dict(row)
    return {
        "id": str(data.get("id") or "").strip(),
        "task_id": str(data.get("task_id") or "").strip(),
        "stage": str(data.get("stage") or "").strip(),
        "outcome": str(data.get("outcome") or "").strip(),
        "execution_phase": str(data.get("execution_phase") or "").strip(),
        "retry_strategy": str(data.get("retry_strategy") or "").strip(),
        "failure_fingerprint": str(data.get("failure_fingerprint") or "").strip(),
        "same_fingerprint_streak": int(data.get("same_fingerprint_streak") or 0),
        "summary": str(data.get("summary") or "").strip(),
        "artifact_path": str(data.get("artifact_path") or "").strip(),
        "metadata": _coerce_json_dict(data.get("metadata_json")),
        "created_by": str(data.get("created_by") or "").strip() or "system",
        "created_at": str(data.get("created_at") or "").strip(),
    }


def _add_task_attempt_in_conn(
    conn,
    *,
    task_id: str,
    stage: str,
    outcome: str,
    execution_phase: str = "",
    retry_strategy: str = "",
    failure_fingerprint: str = "",
    same_fingerprint_streak: int = 0,
    summary: str = "",
    artifact_path: str | None = None,
    metadata: dict | None = None,
    created_by: str = "system",
) -> dict:
    attempt_id = str(uuid.uuid4())
    now = _now()
    conn.execute(
        """
        INSERT INTO task_attempts (
            id, task_id, stage, outcome, execution_phase, retry_strategy,
            failure_fingerprint, same_fingerprint_streak, summary, artifact_path,
            metadata_json, created_by, created_at
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            attempt_id,
            task_id,
            str(stage or "")[:120],
            str(outcome or "")[:80],
            str(execution_phase or "")[:80],
            str(retry_strategy or "")[:80],
            str(failure_fingerprint or "")[:80],
            max(0, int(same_fingerprint_streak or 0)),
            str(summary or "")[:1200],
            str(artifact_path or "")[:1000] or None,
            _json_dump(metadata or {}),
            str(created_by or "system")[:80] or "system",
            now,
        ),
    )
    conn.execute(
        "UPDATE tasks SET latest_attempt_id=?, updated_at=? WHERE id=?",
        (attempt_id, now, task_id),
    )
    row = conn.execute("SELECT * FROM task_attempts WHERE id=?", (attempt_id,)).fetchone()
    normalized = _normalize_attempt_row(row) or {}
    open_issues = _list_task_issues_in_conn(conn, task_id, include_resolved=False)
    conn.execute(
        """
        UPDATE tasks
           SET retry_strategy=?,
               failure_fingerprint=?,
               same_fingerprint_streak=?,
               cooldown_until=?,
               execution_phase=?
         WHERE id=?
        """,
        (
            retry_strategy or RETRY_STRATEGY_DEFAULT,
            failure_fingerprint,
            max(0, int(same_fingerprint_streak or 0)),
            cooldown_until_for_streak(same_fingerprint_streak),
            execution_phase or ("converge" if open_issues else "explore"),
            task_id,
        ),
    )
    return normalized


def _normalize_evidence_row(row) -> dict | None:
    if not row:
        return None
    data = dict(row)
    return {
        "id": str(data.get("id") or "").strip(),
        "task_id": str(data.get("task_id") or "").strip(),
        "stage": str(data.get("stage") or "").strip(),
        "attempt_id": str(data.get("attempt_id") or "").strip(),
        "summary": str(data.get("summary") or "").strip(),
        "bundle": _coerce_json_dict(data.get("evidence_json")),
        "artifact_path": str(data.get("artifact_path") or "").strip(),
        "created_by": str(data.get("created_by") or "").strip() or "system",
        "created_at": str(data.get("created_at") or "").strip(),
    }


def _add_task_evidence_in_conn(
    conn,
    *,
    task_id: str,
    stage: str,
    summary: str,
    bundle: dict,
    attempt_id: str = "",
    artifact_path: str | None = None,
    created_by: str = "system",
) -> dict:
    evidence_id = str(uuid.uuid4())
    now = _now()
    conn.execute(
        """
        INSERT INTO task_evidence (
            id, task_id, stage, attempt_id, summary,
            evidence_json, artifact_path, created_by, created_at
        )
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            evidence_id,
            task_id,
            str(stage or "")[:120],
            attempt_id or None,
            str(summary or "")[:1200],
            _json_dump(bundle or {}),
            str(artifact_path or "")[:1000] or None,
            str(created_by or "system")[:80] or "system",
            now,
        ),
    )
    conn.execute(
        "UPDATE tasks SET latest_evidence_id=?, updated_at=? WHERE id=?",
        (evidence_id, now, task_id),
    )
    row = conn.execute("SELECT * FROM task_evidence WHERE id=?", (evidence_id,)).fetchone()
    return _normalize_evidence_row(row) or {}


def reset_stuck_tasks():
    """
    On server startup, reset tasks left in transient agent states
    back to the last stable state (in case of a crash/restart).
    Uses the agent_types table so custom agents are also handled.
    """
    conn = get_conn()
    rows = conn.execute(
        "SELECT poll_statuses, working_status FROM agent_types WHERE working_status != ''"
    ).fetchall()
    for row in rows:
        working = row["working_status"]
        poll = json.loads(row["poll_statuses"] or "[]")
        reset_to = poll[0] if poll else "todo"
        conn.execute(
            """
            UPDATE tasks
               SET status=?,
                   assignee=NULL,
                   claim_run_id=NULL,
                   lease_token=NULL,
                   lease_expires_at=NULL
             WHERE status=? AND archived=0
            """,
            (reset_to, working),
        )
    conn.commit()
    conn.close()


# ── Projects ──────────────────────────────────────────────────────────────────

def create_project(name: str, path: str, created_by_user_id: str | None = None) -> dict:
    conn = get_conn()
    pid = str(uuid.uuid4())
    now = _now()
    conn.execute(
        "INSERT INTO projects (id, name, path, created_by_user_id, created_at) VALUES (?,?,?,?,?)",
        (pid, name, path, created_by_user_id, now),
    )
    conn.commit()
    row = conn.execute(
        """
        SELECT p.*, u.username AS created_by_username
          FROM projects p
          LEFT JOIN users u ON u.id = p.created_by_user_id
         WHERE p.id=?
        """,
        (pid,),
    ).fetchone()
    conn.close()
    return dict(row)


def get_project(project_id: str, user_id: str | None = None, is_admin: bool = True) -> dict | None:
    conn = get_conn()
    if user_id and not is_admin:
        row = conn.execute(
            """
            SELECT p.*, u.username AS created_by_username
              FROM projects p
              LEFT JOIN users u ON u.id = p.created_by_user_id
             WHERE p.id=?
               AND p.created_by_user_id=?
            """,
            (project_id, user_id),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT p.*, u.username AS created_by_username
              FROM projects p
              LEFT JOIN users u ON u.id = p.created_by_user_id
             WHERE p.id=?
            """,
            (project_id,),
        ).fetchone()
    conn.close()
    return dict(row) if row else None


def list_projects(user_id: str | None = None, is_admin: bool = True) -> list[dict]:
    conn = get_conn()
    if user_id and not is_admin:
        rows = conn.execute(
            """
            SELECT p.*, u.username AS created_by_username
              FROM projects p
              LEFT JOIN users u ON u.id = p.created_by_user_id
             WHERE p.created_by_user_id=?
             ORDER BY p.created_at DESC
            """,
            (user_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT p.*, u.username AS created_by_username
              FROM projects p
              LEFT JOIN users u ON u.id = p.created_by_user_id
             ORDER BY p.created_at DESC
            """
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def count_projects_by_owner(user_id: str) -> int:
    uid = str(user_id or "").strip()
    if not uid:
        return 0
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT COUNT(1) AS cnt FROM projects WHERE created_by_user_id=?",
            (uid,),
        ).fetchone()
        return int(row["cnt"] or 0) if row else 0
    finally:
        conn.close()


def count_tasks_by_owner(user_id: str) -> int:
    uid = str(user_id or "").strip()
    if not uid:
        return 0
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT COUNT(1) AS cnt
              FROM tasks t
              JOIN projects p ON p.id = t.project_id
             WHERE p.created_by_user_id=?
            """,
            (uid,),
        ).fetchone()
        return int(row["cnt"] or 0) if row else 0
    finally:
        conn.close()


def list_worker_projects(
    include_idle: bool = False,
    user_id: str | None = None,
    is_admin: bool = True,
) -> list[dict]:
    """
    Return projects with lightweight queue stats for worker autoscaling decisions.
    """
    conn = get_conn()
    where = []
    params: list[str] = []
    if user_id and not is_admin:
        where.append("p.created_by_user_id=?")
        params.append(user_id)
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""
        SELECT
            p.*,
            u.username AS created_by_username,
            SUM(
                CASE
                    WHEN COALESCE(t.archived, 0) = 0
                     AND COALESCE(t.status, '') NOT IN ('completed', '{CANCELLED_STATUS}')
                    THEN 1 ELSE 0
                END
            ) AS open_task_count,
            SUM(
                CASE
                    WHEN COALESCE(t.archived, 0) = 0
                     AND COALESCE(t.status, '') IN (
                         'triage', 'decompose', 'todo', 'needs_changes', 'in_review', 'approved', 'blocked'
                     )
                    THEN 1 ELSE 0
                END
            ) AS pending_task_count,
            MIN(
                CASE
                    WHEN COALESCE(t.archived, 0) = 0
                     AND COALESCE(t.status, '') NOT IN ('completed', '{CANCELLED_STATUS}')
                    THEN t.updated_at
                    ELSE NULL
                END
            ) AS oldest_open_task_updated_at
          FROM projects p
          LEFT JOIN users u ON u.id = p.created_by_user_id
          LEFT JOIN tasks t ON t.project_id = p.id
          {where_clause}
         GROUP BY p.id
         ORDER BY
            CASE WHEN oldest_open_task_updated_at IS NULL THEN 1 ELSE 0 END ASC,
            oldest_open_task_updated_at ASC,
            p.created_at ASC
        """,
        tuple(params),
    ).fetchall()
    conn.close()
    out: list[dict] = []
    for row in rows:
        item = dict(row)
        item["open_task_count"] = int(item.get("open_task_count") or 0)
        item["pending_task_count"] = int(item.get("pending_task_count") or 0)
        if not include_idle and item["open_task_count"] <= 0:
            continue
        out.append(item)
    return out


def user_can_access_project(project_id: str, user_id: str | None, is_admin: bool) -> bool:
    if is_admin:
        return bool(get_project(project_id))
    if not user_id:
        return False
    conn = get_conn()
    row = conn.execute(
        "SELECT id FROM projects WHERE id=? AND created_by_user_id=? LIMIT 1",
        (project_id, user_id),
    ).fetchone()
    conn.close()
    return bool(row)


def delete_project(project_id: str) -> bool:
    """Delete a project and all its tasks/logs. Returns True if deleted."""
    conn = get_conn()
    # Delete handoffs for all tasks in this project
    conn.execute(
        "DELETE FROM task_handoffs WHERE task_id IN (SELECT id FROM tasks WHERE project_id=?)",
        (project_id,),
    )
    # Delete logs for all tasks in this project
    conn.execute(
        "DELETE FROM logs WHERE task_id IN (SELECT id FROM tasks WHERE project_id=?)",
        (project_id,),
    )
    # Delete all persisted agent terminal output tied to this project/tasks.
    conn.execute(
        "DELETE FROM agent_outputs WHERE project_id=? OR task_id IN (SELECT id FROM tasks WHERE project_id=?)",
        (project_id, project_id),
    )
    # Delete all tasks in this project
    conn.execute("DELETE FROM tasks WHERE project_id=?", (project_id,))
    # Delete the project itself
    cur = conn.execute("DELETE FROM projects WHERE id=?", (project_id,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def project_has_claimed_tasks(project_id: str) -> bool:
    """
    Return True if project has non-archived tasks currently claimed by an agent.
    This is used to avoid deleting projects while agents are still processing tasks.
    """
    conn = get_conn()
    row = conn.execute(
        """
        SELECT 1
          FROM tasks
         WHERE project_id=?
           AND archived=0
           AND assignee IS NOT NULL
         LIMIT 1
        """,
        (project_id,),
    ).fetchone()
    conn.close()
    return bool(row)


def _normalize_feedback_request_status(
    status: str | None,
    *,
    default: str = FEEDBACK_REQUEST_STATUS_TODO,
) -> str:
    raw = str(status or "").strip().lower()
    if raw in ALLOWED_FEEDBACK_REQUEST_STATUSES:
        return raw
    return default


def _join_feedback_requests(base_sql: str) -> str:
    return f"""
        SELECT
            fr.*,
            p.name AS project_name,
            p.path AS project_path,
            submitter.username AS submitter_username,
            updater.username AS updated_by_username
        FROM ({base_sql}) fr
        LEFT JOIN projects p ON p.id = fr.project_id
        LEFT JOIN users submitter ON submitter.id = fr.submitter_user_id
        LEFT JOIN users updater ON updater.id = fr.updated_by_user_id
    """


def _public_feedback_request(row) -> dict:
    data = dict(row)
    return {
        "id": data["id"],
        "project_id": data.get("project_id"),
        "project_name": data.get("project_name"),
        "project_path": data.get("project_path"),
        "submitter_user_id": data.get("submitter_user_id"),
        "submitter_username": data.get("submitter_username"),
        "title": data.get("title") or "",
        "description": data.get("description") or "",
        "normalized_title": data.get("normalized_title") or "",
        "normalized_description": data.get("normalized_description") or "",
        "status": _normalize_feedback_request_status(data.get("status")),
        "ai_decision": str(data.get("ai_decision") or "").strip().lower(),
        "ai_reason": data.get("ai_reason") or "",
        "admin_feedback": data.get("admin_feedback") or "",
        "updated_by_user_id": data.get("updated_by_user_id"),
        "updated_by_username": data.get("updated_by_username"),
        "created_at": data.get("created_at"),
        "updated_at": data.get("updated_at"),
        "reviewed_at": data.get("reviewed_at"),
    }


def create_feedback_request(
    *,
    submitter_user_id: str,
    title: str,
    description: str,
    normalized_title: str = "",
    normalized_description: str = "",
    project_id: str | None = None,
    status: str = FEEDBACK_REQUEST_STATUS_TODO,
    ai_decision: str = "",
    ai_reason: str = "",
    updated_by_user_id: str | None = None,
) -> dict:
    conn = get_conn()
    rid = str(uuid.uuid4())
    now = _now()
    normalized_status = _normalize_feedback_request_status(status)
    normalized_ai_decision = str(ai_decision or "").strip().lower()
    if not normalized_ai_decision:
        normalized_ai_decision = (
            "reject" if normalized_status == FEEDBACK_REQUEST_STATUS_REJECTED else "approve"
        )
    updater = str(updated_by_user_id or submitter_user_id or "").strip() or None
    try:
        conn.execute(
            """
            INSERT INTO feedback_requests (
                id,
                project_id,
                submitter_user_id,
                title,
                description,
                normalized_title,
                normalized_description,
                status,
                ai_decision,
                ai_reason,
                admin_feedback,
                updated_by_user_id,
                created_at,
                updated_at,
                reviewed_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                rid,
                project_id,
                submitter_user_id,
                title,
                description,
                normalized_title,
                normalized_description,
                normalized_status,
                normalized_ai_decision,
                ai_reason,
                "",
                updater,
                now,
                now,
                now,
            ),
        )
        row = conn.execute(
            _join_feedback_requests("SELECT * FROM feedback_requests WHERE id=?"),
            (rid,),
        ).fetchone()
        conn.commit()
        return _public_feedback_request(row) if row else None
    finally:
        conn.close()


def get_feedback_request(
    feedback_id: str,
    user_id: str | None = None,
    is_admin: bool = True,
) -> dict | None:
    conn = get_conn()
    try:
        if user_id and not is_admin:
            row = conn.execute(
                _join_feedback_requests(
                    """
                    SELECT fr.*
                      FROM feedback_requests fr
                     WHERE fr.id=?
                       AND fr.submitter_user_id=?
                    """
                ),
                (feedback_id, user_id),
            ).fetchone()
        else:
            row = conn.execute(
                _join_feedback_requests("SELECT * FROM feedback_requests WHERE id=?"),
                (feedback_id,),
            ).fetchone()
        return _public_feedback_request(row) if row else None
    finally:
        conn.close()


def list_feedback_requests(
    project_id: str | None = None,
    user_id: str | None = None,
    is_admin: bool = True,
    status: str | None = None,
) -> list[dict]:
    conn = get_conn()
    try:
        where: list[str] = []
        params: list[str] = []
        if project_id:
            where.append("fr.project_id=?")
            params.append(project_id)
        normalized_status = str(status or "").strip().lower()
        if normalized_status:
            if normalized_status not in ALLOWED_FEEDBACK_REQUEST_STATUSES:
                return []
            where.append("fr.status=?")
            params.append(normalized_status)
        if user_id and not is_admin:
            where.append("fr.submitter_user_id=?")
            params.append(user_id)
        where_clause = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            _join_feedback_requests(
                f"""
                SELECT fr.*
                  FROM feedback_requests fr
                  {where_clause}
                 ORDER BY fr.updated_at DESC, fr.created_at DESC, fr.id DESC
                """
            ),
            tuple(params),
        ).fetchall()
        return [_public_feedback_request(row) for row in rows]
    finally:
        conn.close()


def update_feedback_request(
    feedback_id: str,
    *,
    status=UNSET,
    admin_feedback=UNSET,
    updated_by_user_id: str | None = None,
) -> dict | None:
    conn = get_conn()
    try:
        current = conn.execute(
            "SELECT id FROM feedback_requests WHERE id=?",
            (feedback_id,),
        ).fetchone()
        if not current:
            return None

        fields: dict[str, object] = {}
        if status is not UNSET:
            normalized_status = str(status or "").strip().lower()
            if normalized_status not in ALLOWED_FEEDBACK_REQUEST_STATUSES:
                raise ValueError("feedback request status 不合法")
            fields["status"] = normalized_status
        if admin_feedback is not UNSET:
            fields["admin_feedback"] = str(admin_feedback or "").strip()
        if updated_by_user_id is not None:
            fields["updated_by_user_id"] = str(updated_by_user_id or "").strip() or None
        if not fields:
            row = conn.execute(
                _join_feedback_requests("SELECT * FROM feedback_requests WHERE id=?"),
                (feedback_id,),
            ).fetchone()
            return _public_feedback_request(row) if row else None

        fields["updated_at"] = _now()
        set_clause = ", ".join(f"{key}=?" for key in fields)
        conn.execute(
            f"UPDATE feedback_requests SET {set_clause} WHERE id=?",
            (*fields.values(), feedback_id),
        )
        row = conn.execute(
            _join_feedback_requests("SELECT * FROM feedback_requests WHERE id=?"),
            (feedback_id,),
        ).fetchone()
        conn.commit()
        return _public_feedback_request(row) if row else None
    finally:
        conn.close()


# ── Tasks ─────────────────────────────────────────────────────────────────────

def _join_project(base_sql: str) -> str:
    """Wrap a task query to also return project.path as project_path."""
    return f"""
        SELECT
            t.*,
            p.path as project_path,
            p.name as project_name,
            p.created_by_user_id as project_owner_user_id,
            u.username as project_owner_username
        FROM ({base_sql}) t
        LEFT JOIN projects p ON t.project_id = p.id
        LEFT JOIN users u ON u.id = p.created_by_user_id
    """


def _normalize_dependency_payload(dependencies) -> list[dict]:
    if dependencies is None:
        return []
    if not isinstance(dependencies, list):
        raise DependencyValidationError("dependencies 必须是数组")
    out: list[dict] = []
    seen: set[str] = set()
    for idx, item in enumerate(dependencies, 1):
        if not isinstance(item, dict):
            raise DependencyValidationError(f"dependencies[{idx}] 必须是对象")
        dep_id = str(item.get("depends_on_task_id") or "").strip()
        if not dep_id:
            raise DependencyValidationError(f"dependencies[{idx}] 缺少 depends_on_task_id")
        raw_state = str(item.get("required_state") or DEPENDENCY_STATE_APPROVED).strip().lower()
        if raw_state not in ALLOWED_DEPENDENCY_STATES:
            allowed = ", ".join(sorted(ALLOWED_DEPENDENCY_STATES))
            raise DependencyValidationError(
                f"dependencies[{idx}].required_state 不合法，仅支持: {allowed}"
            )
        if dep_id in seen:
            continue
        seen.add(dep_id)
        out.append(
            {
                "depends_on_task_id": dep_id,
                "required_state": raw_state,
            }
        )
    return out


def _dependency_reachable_in_conn(conn, from_task_id: str, target_task_id: str) -> bool:
    row = conn.execute(
        """
        WITH RECURSIVE dep_chain(task_id) AS (
            SELECT ?
            UNION
            SELECT td.depends_on_task_id
              FROM task_dependencies td
              JOIN dep_chain dc ON td.task_id = dc.task_id
        )
        SELECT 1
          FROM dep_chain
         WHERE task_id=?
         LIMIT 1
        """,
        (from_task_id, target_task_id),
    ).fetchone()
    return bool(row)


def _list_task_dependencies_in_conn(conn, task_id: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            td.id,
            td.task_id,
            td.depends_on_task_id,
            td.required_state,
            td.created_by,
            td.created_at,
            dep.title AS depends_on_title,
            dep.status AS depends_on_status,
            dep.priority AS depends_on_priority
          FROM task_dependencies td
          JOIN tasks dep ON dep.id = td.depends_on_task_id
         WHERE td.task_id=?
         ORDER BY dep.priority ASC, dep.updated_at ASC, dep.created_at ASC, dep.id ASC
        """,
        (task_id,),
    ).fetchall()
    out: list[dict] = []
    for row in rows:
        dep = dict(row)
        dep["required_state"] = _normalize_dependency_required_state(dep.get("required_state"))
        dep["satisfied"] = _dependency_is_satisfied(
            dep["required_state"],
            str(dep.get("depends_on_status") or ""),
        )
        out.append(dep)
    return out


def _replace_task_dependencies_in_conn(
    conn,
    task_id: str,
    dependencies,
    created_by: str | None = None,
) -> list[dict] | None:
    task_row = conn.execute(
        "SELECT id, project_id FROM tasks WHERE id=?",
        (task_id,),
    ).fetchone()
    if not task_row:
        return None

    project_id = str(task_row["project_id"] or "").strip()
    normalized = _normalize_dependency_payload(dependencies)

    for dep in normalized:
        dep_id = dep["depends_on_task_id"]
        if dep_id == task_id:
            raise DependencyValidationError("任务不能依赖自身")
        dep_row = conn.execute(
            "SELECT id, project_id FROM tasks WHERE id=?",
            (dep_id,),
        ).fetchone()
        if not dep_row:
            raise DependencyValidationError(f"依赖任务不存在: {dep_id}")
        dep_project_id = str(dep_row["project_id"] or "").strip()
        if dep_project_id != project_id:
            raise DependencyValidationError("依赖任务必须与当前任务同项目")
        if _dependency_reachable_in_conn(conn, dep_id, task_id):
            raise DependencyCycleError("检测到循环依赖，无法保存")

    conn.execute("DELETE FROM task_dependencies WHERE task_id=?", (task_id,))
    now = _now()
    created_by_value = str(created_by or "").strip() or None
    for dep in normalized:
        conn.execute(
            """
            INSERT INTO task_dependencies
                (task_id, depends_on_task_id, required_state, created_by, created_at)
            VALUES (?,?,?,?,?)
            """,
            (
                task_id,
                dep["depends_on_task_id"],
                dep["required_state"],
                created_by_value,
                now,
            ),
        )
    return _list_task_dependencies_in_conn(conn, task_id)


def _attach_dependency_state_to_tasks_in_conn(conn, task_rows: list[dict]) -> None:
    if not task_rows:
        return
    task_ids = [str(t.get("id") or "").strip() for t in task_rows if str(t.get("id") or "").strip()]
    if not task_ids:
        return
    placeholders = ",".join("?" for _ in task_ids)
    dep_rows = conn.execute(
        f"""
        SELECT
            td.task_id,
            td.depends_on_task_id,
            td.required_state,
            dep.status AS depends_on_status
          FROM task_dependencies td
          JOIN tasks dep ON dep.id = td.depends_on_task_id
         WHERE td.task_id IN ({placeholders})
        """,
        task_ids,
    ).fetchall()
    summary: dict[str, dict] = {
        tid: {
            "dependency_count": 0,
            "blocking_dependency_ids": [],
        }
        for tid in task_ids
    }
    for row in dep_rows:
        tid = str(row["task_id"] or "").strip()
        if not tid:
            continue
        info = summary.setdefault(
            tid,
            {"dependency_count": 0, "blocking_dependency_ids": []},
        )
        info["dependency_count"] += 1
        required_state = _normalize_dependency_required_state(row["required_state"])
        if not _dependency_is_satisfied(required_state, str(row["depends_on_status"] or "")):
            dep_id = str(row["depends_on_task_id"] or "").strip()
            if dep_id and dep_id not in info["blocking_dependency_ids"]:
                info["blocking_dependency_ids"].append(dep_id)

    for task in task_rows:
        tid = str(task.get("id") or "").strip()
        info = summary.get(tid, {"dependency_count": 0, "blocking_dependency_ids": []})
        blocking = list(info.get("blocking_dependency_ids") or [])
        status = str(task.get("status") or "").strip().lower()
        ready = True
        if status == "todo":
            ready = len(blocking) == 0
        task["dependency_count"] = int(info.get("dependency_count") or 0)
        task["blocking_dependency_ids"] = blocking
        task["blocking_dependency_count"] = len(blocking)
        task["ready"] = ready


def _extract_cancel_reason_from_log_message(message: str) -> str:
    text = str(message or "").strip()
    if not text:
        return ""
    for marker in ("父任务取消原因：", "原因："):
        idx = text.rfind(marker)
        if idx < 0:
            continue
        reason = text[idx + len(marker):].strip()
        if not reason:
            continue
        line = reason.splitlines()[0].strip()
        return (line or reason)[:2000]
    return ""


def _fill_cancel_reasons_from_logs_in_conn(
    conn,
    task_rows: list[dict],
    *,
    persist: bool = False,
) -> None:
    if not task_rows:
        return
    pending_ids: list[str] = []
    for task in task_rows:
        status = str(task.get("status") or "").strip().lower()
        cancel_reason = str(task.get("cancel_reason") or "").strip()
        tid = str(task.get("id") or "").strip()
        if tid and status == CANCELLED_STATUS and not cancel_reason:
            pending_ids.append(tid)
    if not pending_ids:
        return

    placeholders = ",".join("?" for _ in pending_ids)
    log_rows = conn.execute(
        f"""
        SELECT task_id, message, id
          FROM logs
         WHERE task_id IN ({placeholders})
           AND agent='system'
           AND (
                message LIKE '任务已取消并归档，不再执行。%'
                OR message LIKE '因父任务被取消，任务已取消并归档，不再执行。%'
           )
         ORDER BY id DESC
        """,
        pending_ids,
    ).fetchall()
    reason_map: dict[str, str] = {}
    for row in log_rows:
        tid = str(row["task_id"] or "").strip()
        if not tid or tid in reason_map:
            continue
        reason = _extract_cancel_reason_from_log_message(str(row["message"] or ""))
        if reason:
            reason_map[tid] = reason

    if not reason_map:
        return

    for task in task_rows:
        tid = str(task.get("id") or "").strip()
        reason = reason_map.get(tid, "")
        if reason and not str(task.get("cancel_reason") or "").strip():
            task["cancel_reason"] = reason

    if persist:
        conn.executemany(
            "UPDATE tasks SET cancel_reason=? WHERE id=? AND COALESCE(cancel_reason, '')=''",
            [(reason, tid) for tid, reason in reason_map.items()],
        )


def _attach_autonomy_metadata_to_tasks_in_conn(conn, task_rows: list[dict]) -> None:
    task_ids = [str(item.get("id") or "").strip() for item in (task_rows or []) if str(item.get("id") or "").strip()]
    if not task_ids:
        return

    placeholders = ",".join("?" for _ in task_ids)
    issue_rows = conn.execute(
        f"""
        SELECT task_id, COUNT(1) AS cnt
          FROM task_issues
         WHERE task_id IN ({placeholders})
           AND (resolved_at IS NULL OR TRIM(COALESCE(resolved_at, ''))='')
           AND status NOT IN ('resolved', 'wont_fix')
         GROUP BY task_id
        """,
        tuple(task_ids),
    ).fetchall()
    open_issue_counts = {str(row["task_id"]): int(row["cnt"] or 0) for row in issue_rows}

    full_rows = [item for item in task_rows if "description" in item]
    latest_contract_rows = conn.execute(
        f"""
        SELECT c.*
          FROM task_contracts c
          JOIN (
            SELECT task_id, MAX(version) AS max_version
              FROM task_contracts
             WHERE task_id IN ({placeholders})
             GROUP BY task_id
          ) latest
            ON latest.task_id = c.task_id
           AND latest.max_version = c.version
        """,
        tuple(task_ids),
    ).fetchall()
    latest_contract_by_task = {
        str(row["task_id"]): normalized
        for row in latest_contract_rows
        if (normalized := _normalize_contract_row(row))
    }

    latest_evidence_rows = conn.execute(
        f"""
        SELECT e.*
          FROM task_evidence e
          JOIN (
            SELECT task_id, MAX(created_at) AS created_at
              FROM task_evidence
             WHERE task_id IN ({placeholders})
             GROUP BY task_id
          ) latest
            ON latest.task_id = e.task_id
           AND latest.created_at = e.created_at
        """,
        tuple(task_ids),
    ).fetchall()
    latest_evidence_by_task = {
        str(row["task_id"]): normalized
        for row in latest_evidence_rows
        if (normalized := _normalize_evidence_row(row))
    }

    latest_attempt_rows = conn.execute(
        f"""
        SELECT a.*
          FROM task_attempts a
          JOIN (
            SELECT task_id, MAX(created_at) AS created_at
              FROM task_attempts
             WHERE task_id IN ({placeholders})
             GROUP BY task_id
          ) latest
            ON latest.task_id = a.task_id
           AND latest.created_at = a.created_at
        """,
        tuple(task_ids),
    ).fetchall()
    latest_attempt_by_task = {
        str(row["task_id"]): normalized
        for row in latest_attempt_rows
        if (normalized := _normalize_attempt_row(row))
    }

    open_issue_rows = conn.execute(
        f"""
        SELECT *
          FROM task_issues
         WHERE task_id IN ({placeholders})
           AND (resolved_at IS NULL OR TRIM(COALESCE(resolved_at, ''))='')
           AND status NOT IN ('resolved', 'wont_fix')
         ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                  last_seen_at DESC,
                  first_seen_at ASC
        """,
        tuple(task_ids),
    ).fetchall()
    open_issues_by_task: dict[str, list[dict]] = {}
    for row in open_issue_rows:
        issue = _normalize_issue_row(row)
        if not issue:
            continue
        open_issues_by_task.setdefault(str(issue["task_id"]), []).append(issue)

    for item in task_rows:
        task_id = str(item.get("id") or "").strip()
        if not task_id:
            continue
        item["open_issue_count"] = open_issue_counts.get(task_id, 0)
        if "allowed_surface_json" in item:
            item["allowed_surface"] = normalize_allowed_surface(item.get("allowed_surface_json"))
        if "description" not in item:
            continue
        item["current_contract"] = latest_contract_by_task.get(task_id)
        item["open_issues"] = open_issues_by_task.get(task_id, [])[:12]
        item["latest_evidence"] = latest_evidence_by_task.get(task_id)
        item["latest_attempt"] = latest_attempt_by_task.get(task_id)


def _enrich_task_rows_in_conn(
    conn,
    task_rows: list[dict],
    *,
    persist_cancel_reason: bool = False,
) -> None:
    _fill_cancel_reasons_from_logs_in_conn(
        conn,
        task_rows,
        persist=persist_cancel_reason,
    )
    _attach_dependency_state_to_tasks_in_conn(conn, task_rows)
    _attach_autonomy_metadata_to_tasks_in_conn(conn, task_rows)


def _backfill_cancel_reasons_from_logs(conn) -> None:
    rows = conn.execute(
        "SELECT id, status, cancel_reason FROM tasks WHERE status=? AND COALESCE(cancel_reason, '')=''",
        (CANCELLED_STATUS,),
    ).fetchall()
    if not rows:
        return
    _fill_cancel_reasons_from_logs_in_conn(
        conn,
        [dict(row) for row in rows],
        persist=True,
    )


def create_task(title: str, description: str, project_id: str | None = None,
                parent_task_id: str | None = None,
                assigned_agent: str | None = None,
                dev_agent: str | None = None,
                status: str = "triage",
                subtask_order: int | None = None,
                review_enabled: bool = True,
                priority: int | None = None,
                dependencies: list[dict] | None = None,
                created_by: str | None = None) -> dict:
    conn = get_conn()
    tid = str(uuid.uuid4())
    now = _now()
    normalized_status = str(status or "").strip() or "triage"
    normalized_assigned = str(assigned_agent or "").strip() or None
    normalized_dev_agent = str(dev_agent or "").strip() or None
    normalized_subtask_order = int(subtask_order or 0)
    if normalized_subtask_order < 0:
        normalized_subtask_order = 0
    normalized_review_enabled = 1 if bool(review_enabled) else 0
    normalized_priority = _normalize_priority_value(priority, default=DEFAULT_TASK_PRIORITY)
    try:
        conn.execute("BEGIN IMMEDIATE")
        if normalized_status == "todo":
            todo_pollers = _todo_pollers(conn)
            if normalized_assigned and normalized_assigned not in todo_pollers:
                normalized_assigned = (
                    normalized_dev_agent if normalized_dev_agent in todo_pollers else None
                )
        conn.execute(
            """INSERT INTO tasks
               (id, project_id, title, description, priority, status,
                parent_task_id, subtask_order, assigned_agent, dev_agent, review_enabled, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                tid,
                project_id,
                title,
                description,
                normalized_priority,
                normalized_status,
                parent_task_id,
                normalized_subtask_order,
                normalized_assigned,
                normalized_dev_agent,
                normalized_review_enabled,
                now,
                now,
            ),
        )
        if dependencies is not None:
            _replace_task_dependencies_in_conn(
                conn,
                task_id=tid,
                dependencies=dependencies,
                created_by=created_by,
            )
        _sync_task_contract_in_conn(
            conn,
            task_id=tid,
            title=title,
            description=description,
            created_by=str(created_by or "system")[:80] or "system",
        )
        row = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"), (tid,)
        ).fetchone()
        task = dict(row) if row else None
        if not task:
            conn.rollback()
            raise RuntimeError("create_task failed")
        _enrich_task_rows_in_conn(conn, [task])
        conn.commit()
        return task
    finally:
        conn.close()


def list_subtasks(parent_task_id: str, user_id: str | None = None, is_admin: bool = True) -> list[dict]:
    conn = get_conn()
    if user_id and not is_admin:
        rows = conn.execute(
            """
            SELECT
                t.*,
                p.path as project_path,
                p.name as project_name,
                p.created_by_user_id as project_owner_user_id,
                u.username as project_owner_username
              FROM tasks t
              LEFT JOIN projects p ON t.project_id = p.id
              LEFT JOIN users u ON u.id = p.created_by_user_id
             WHERE t.parent_task_id=?
               AND p.created_by_user_id=?
             ORDER BY
               CASE WHEN COALESCE(t.subtask_order, 0) > 0 THEN 0 ELSE 1 END ASC,
               COALESCE(t.subtask_order, 0) ASC,
               t.created_at ASC,
               t.id ASC
            """,
            (parent_task_id, user_id),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT
                t.*,
                p.path as project_path,
                p.name as project_name,
                p.created_by_user_id as project_owner_user_id,
                u.username as project_owner_username
              FROM tasks t
              LEFT JOIN projects p ON t.project_id = p.id
              LEFT JOIN users u ON u.id = p.created_by_user_id
             WHERE t.parent_task_id=?
             ORDER BY
               CASE WHEN COALESCE(t.subtask_order, 0) > 0 THEN 0 ELSE 1 END ASC,
               COALESCE(t.subtask_order, 0) ASC,
               t.created_at ASC,
               t.id ASC
            """,
            (parent_task_id,),
        ).fetchall()
    out = [dict(r) for r in rows]
    _enrich_task_rows_in_conn(conn, out, persist_cancel_reason=True)
    conn.close()
    return out


def check_parent_completion(parent_task_id: str) -> bool:
    """
    If all subtasks of parent_task_id are 'completed', auto-complete the parent.
    Returns True if parent was just completed.
    """
    conn = get_conn()
    subtasks = conn.execute(
        "SELECT status FROM tasks WHERE parent_task_id=?", (parent_task_id,)
    ).fetchall()
    if not subtasks:
        conn.close()
        return False
    all_done = all(s["status"] == "completed" for s in subtasks)
    if all_done:
        conn.execute(
            "UPDATE tasks SET status='completed', updated_at=? WHERE id=? AND status='decomposed'",
            (_now(), parent_task_id),
        )
        conn.commit()
    conn.close()
    return all_done


def get_task(task_id: str, user_id: str | None = None, is_admin: bool = True) -> dict | None:
    conn = get_conn()
    if user_id and not is_admin:
        row = conn.execute(
            _join_project(
                """
                SELECT t.*
                  FROM tasks t
                  JOIN projects p ON p.id = t.project_id
                 WHERE t.id=?
                   AND p.created_by_user_id=?
                """
            ),
            (task_id, user_id),
        ).fetchone()
    else:
        row = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
        ).fetchone()
    task = dict(row) if row else None
    if task:
        _enrich_task_rows_in_conn(conn, [task], persist_cancel_reason=True)
    conn.close()
    return task


def list_tasks(
    project_id: str | None = None,
    user_id: str | None = None,
    is_admin: bool = True,
    compact: bool = False,
) -> list[dict]:
    conn = get_conn()
    select_cols = "t.*"
    if compact:
        # Lightweight shape for board/init payloads; keep only card-level fields.
        select_cols = (
            "t.id, t.title, t.status, t.assignee, t.commit_hash, t.project_id, "
            "t.parent_task_id, t.subtask_order, t.assigned_agent, t.dev_agent, "
            "t.review_enabled, SUBSTR(COALESCE(t.review_feedback, ''), 1, 240) AS review_feedback, "
            "t.cancel_reason, t.created_at, t.updated_at, t.archived, t.priority"
        )
    if user_id and not is_admin:
        if project_id:
            rows = conn.execute(
                _join_project(
                    """
                    SELECT {select_cols}
                      FROM tasks t
                      JOIN projects p ON p.id = t.project_id
                     WHERE t.project_id=?
                       AND p.created_by_user_id=?
                     ORDER BY t.created_at DESC
                    """.format(select_cols=select_cols)
                ),
                (project_id, user_id),
            ).fetchall()
        else:
            rows = conn.execute(
                _join_project(
                    """
                    SELECT {select_cols}
                      FROM tasks t
                      JOIN projects p ON p.id = t.project_id
                     WHERE p.created_by_user_id=?
                     ORDER BY t.created_at DESC
                    """.format(select_cols=select_cols)
                ),
                (user_id,),
            ).fetchall()
    else:
        if project_id:
            rows = conn.execute(
                _join_project(
                    f"SELECT {select_cols} FROM tasks t WHERE t.project_id=? ORDER BY t.created_at DESC"
                ),
                (project_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                _join_project(f"SELECT {select_cols} FROM tasks t ORDER BY t.created_at DESC")
            ).fetchall()
    out = [dict(r) for r in rows]
    _enrich_task_rows_in_conn(conn, out, persist_cancel_reason=True)
    conn.close()
    return out


def list_task_dependencies(task_id: str) -> list[dict] | None:
    conn = get_conn()
    try:
        row = conn.execute("SELECT id FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not row:
            return None
        return _list_task_dependencies_in_conn(conn, task_id)
    finally:
        conn.close()


def list_task_dependents(task_id: str) -> list[dict] | None:
    conn = get_conn()
    try:
        row = conn.execute("SELECT id FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not row:
            return None
        rows = conn.execute(
            _join_project(
                """
                SELECT t.*
                  FROM tasks t
                  JOIN task_dependencies td ON td.task_id = t.id
                 WHERE td.depends_on_task_id=?
                 ORDER BY t.priority ASC, t.updated_at ASC, t.created_at ASC, t.id ASC
                """
            ),
            (task_id,),
        ).fetchall()
        out = [dict(r) for r in rows]
        _enrich_task_rows_in_conn(conn, out, persist_cancel_reason=True)
        return out
    finally:
        conn.close()


def replace_task_dependencies(
    task_id: str,
    dependencies,
    created_by: str | None = None,
) -> list[dict] | None:
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        out = _replace_task_dependencies_in_conn(
            conn,
            task_id=task_id,
            dependencies=dependencies,
            created_by=created_by,
        )
        if out is None:
            conn.rollback()
            return None
        conn.commit()
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_terminal_tasks_for_workspace_cleanup(limit: int = 200) -> list[dict]:
    """
    Return recently updated terminal tasks that may need workspace cleanup.
    Includes joined project fields so caller can resolve project_path safely.
    """
    conn = get_conn()
    rows = conn.execute(
        _join_project(
            f"""
            SELECT *
              FROM tasks
             WHERE status IN ('completed', '{CANCELLED_STATUS}')
             ORDER BY updated_at DESC
             LIMIT ?
            """
        ),
        (max(1, int(limit)),),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _update_task_in_conn(conn, task_id: str, **fields) -> dict | None:
    fields = dict(fields or {})
    # Control/meta fields used by API/agents but not persisted directly.
    feedback_source = str(fields.pop("feedback_source", "") or "").strip() or "system"
    feedback_stage = str(fields.pop("feedback_stage", "") or "").strip()
    feedback_actor = str(fields.pop("feedback_actor", "") or "").strip()
    fields.pop("create_handoff", None)

    current = conn.execute(
        """
        SELECT
            title,
            description,
            status,
            assignee,
            assigned_agent,
            dev_agent,
            claim_run_id,
            lease_token,
            lease_expires_at,
            execution_phase,
            retry_strategy,
            failure_fingerprint,
            same_fingerprint_streak,
            cooldown_until,
            current_contract_id,
            allowed_surface_json,
            review_feedback,
            review_feedback_history
        FROM tasks
        WHERE id=?
        """,
        (task_id,),
    ).fetchone()
    if not current:
        return None
    # Canceled tasks are immutable by normal updates so late agent writes
    # cannot resurrect them.
    if current["status"] == CANCELLED_STATUS:
        row = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
        ).fetchone()
        return dict(row) if row else None
    if not fields:
        row = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
        ).fetchone()
        task = dict(row) if row else None
        if task:
            _enrich_task_rows_in_conn(conn, [task], persist_cancel_reason=True)
        return task

    if "priority" in fields:
        fields["priority"] = _normalize_priority_value(fields.get("priority"), default=DEFAULT_TASK_PRIORITY)
    if "same_fingerprint_streak" in fields:
        try:
            fields["same_fingerprint_streak"] = max(0, int(fields.get("same_fingerprint_streak") or 0))
        except Exception:
            fields["same_fingerprint_streak"] = 0
    if "allowed_surface_json" in fields:
        fields["allowed_surface_json"] = _json_dump(normalize_allowed_surface(fields.get("allowed_surface_json")))
    for key in ("execution_phase", "retry_strategy", "failure_fingerprint"):
        if key in fields:
            fields[key] = str(fields.get(key) or "").strip()
    if "cooldown_until" in fields:
        cooldown_until = str(fields.get("cooldown_until") or "").strip()
        fields["cooldown_until"] = cooldown_until or None

    target_status = str(fields.get("status") or current["status"] or "").strip()
    now = _now()
    needs_contract_sync = (
        "description" in fields
        or not str(current["current_contract_id"] or "").strip()
    )

    # ── Feedback history maintenance ────────────────────────────────────────
    history = _parse_feedback_history(current["review_feedback_history"])
    history_changed = False

    if "review_feedback" in fields:
        new_feedback = str(fields.get("review_feedback") or "").strip()
        old_feedback = str(current["review_feedback"] or "").strip()
        if (
            new_feedback
            and new_feedback != old_feedback
            and target_status in ACTIONABLE_FEEDBACK_STATUSES
        ):
            history_changed = _append_feedback_entry(
                history,
                feedback=new_feedback,
                source=feedback_source,
                status_at=target_status,
                stage=feedback_stage,
                actor=feedback_actor,
                created_at=now,
            ) or history_changed

    if target_status in FEEDBACK_RESOLVE_STATUSES:
        history_changed = _resolve_open_feedback(
            history,
            resolved_at=now,
            reason=f"status:{target_status}",
        ) or history_changed

    if history_changed:
        fields["review_feedback_history"] = _dump_feedback_history(history)

    if target_status == "todo":
        todo_pollers = _todo_pollers(conn)
        # Validate current/effective assignment for todo claimability.
        if "assigned_agent" in fields:
            effective_assigned = str(fields.get("assigned_agent") or "").strip()
        else:
            effective_assigned = str(current["assigned_agent"] or "").strip()
        if effective_assigned and effective_assigned not in todo_pollers:
            fallback_dev = str(fields.get("dev_agent") or current["dev_agent"] or "").strip()
            fields["assigned_agent"] = fallback_dev if fallback_dev in todo_pollers else None

    working_statuses = _working_statuses(conn)
    if "status" in fields and target_status not in working_statuses:
        fields["claim_run_id"] = None
        fields["lease_token"] = None
        fields["lease_expires_at"] = None
    if "assignee" in fields:
        next_assignee = str(fields.get("assignee") or "").strip()
        current_assignee = str(current["assignee"] or "").strip()
        if not next_assignee or next_assignee != current_assignee:
            fields["claim_run_id"] = None
            fields["lease_token"] = None
            fields["lease_expires_at"] = None

    fields["updated_at"] = now
    set_clause = ", ".join(f"{k}=?" for k in fields)
    values = list(fields.values()) + [task_id]
    conn.execute(f"UPDATE tasks SET {set_clause} WHERE id=?", values)
    if needs_contract_sync:
        _sync_task_contract_in_conn(
            conn,
            task_id=task_id,
            title=str(fields.get("title") if "title" in fields else current["title"] or ""),
            description=str(fields.get("description") if "description" in fields else current["description"] or ""),
            created_by=feedback_actor or feedback_source or "system",
        )
    row = conn.execute(
        _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
    ).fetchone()
    task = dict(row) if row else None
    if task:
        _enrich_task_rows_in_conn(conn, [task], persist_cancel_reason=True)
    return task


def update_task(task_id: str, **fields) -> dict | None:
    conn = get_conn()
    try:
        row = _update_task_in_conn(conn, task_id, **fields)
        conn.commit()
        return row
    finally:
        conn.close()


def get_tasks_by_status(
    status: str,
    project_id: str | None = None,
    user_id: str | None = None,
    is_admin: bool = True,
) -> list[dict]:
    conn = get_conn()
    if user_id and not is_admin:
        if project_id:
            rows = conn.execute(
                _join_project(
                    """
                    SELECT t.*
                      FROM tasks t
                      JOIN projects p ON p.id = t.project_id
                     WHERE t.status=?
                       AND t.project_id=?
                       AND p.created_by_user_id=?
                     ORDER BY t.priority ASC, t.updated_at ASC
                    """
                ),
                (status, project_id, user_id),
            ).fetchall()
        else:
            rows = conn.execute(
                _join_project(
                    """
                    SELECT t.*
                      FROM tasks t
                      JOIN projects p ON p.id = t.project_id
                     WHERE t.status=?
                       AND p.created_by_user_id=?
                     ORDER BY t.priority ASC, t.updated_at ASC
                    """
                ),
                (status, user_id),
            ).fetchall()
    else:
        if project_id:
            rows = conn.execute(
                _join_project("SELECT * FROM tasks WHERE status=? AND project_id=? ORDER BY priority ASC, updated_at ASC"),
                (status, project_id),
            ).fetchall()
        else:
            rows = conn.execute(
                _join_project("SELECT * FROM tasks WHERE status=? ORDER BY priority ASC, updated_at ASC"),
                (status,),
            ).fetchall()
    out = [dict(r) for r in rows]
    _enrich_task_rows_in_conn(conn, out, persist_cancel_reason=True)
    conn.close()
    return out


def cancel_task(
    task_id: str,
    include_subtasks: bool = True,
    reason: str | None = None,
) -> list[dict] | None:
    """
    Cancel a task (and optionally all descendants), archive it, and make it non-runnable.
    Returns updated rows with joined project fields in deterministic order, or None if task missing.
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        root = conn.execute(
            "SELECT id, cancel_reason FROM tasks WHERE id=?",
            (task_id,),
        ).fetchone()
        if not root:
            conn.rollback()
            return None

        if include_subtasks:
            rows = conn.execute(
                """
                WITH RECURSIVE task_tree(id) AS (
                    SELECT id FROM tasks WHERE id=?
                    UNION ALL
                    SELECT t.id
                    FROM tasks t
                    JOIN task_tree tt ON t.parent_task_id = tt.id
                )
                SELECT id FROM task_tree
                """,
                (task_id,),
            ).fetchall()
            task_ids = [r["id"] for r in rows]
        else:
            task_ids = [task_id]

        now = _now()
        requested_reason = str(reason or "").strip()[:2000]
        existing_root_reason = str(root["cancel_reason"] or "").strip()
        root_cancel_reason = requested_reason or existing_root_reason or "手动取消"
        child_cancel_reason = f"父任务取消：{root_cancel_reason}" if root_cancel_reason else "父任务已取消"
        placeholders = ",".join("?" for _ in task_ids)
        conn.execute(
            f"""
            UPDATE tasks
               SET status=?,
                   archived=1,
                   assignee=NULL,
                   claim_run_id=NULL,
                   lease_token=NULL,
                   lease_expires_at=NULL,
                   updated_at=?
             WHERE id IN ({placeholders})
            """,
            [CANCELLED_STATUS, now, *task_ids],
        )
        conn.execute(
            "UPDATE tasks SET cancel_reason=? WHERE id=?",
            (root_cancel_reason, task_id),
        )
        child_ids = [tid for tid in task_ids if tid != task_id]
        if child_ids:
            child_placeholders = ",".join("?" for _ in child_ids)
            conn.execute(
                f"UPDATE tasks SET cancel_reason=? WHERE id IN ({child_placeholders})",
                [child_cancel_reason, *child_ids],
            )

        conn.commit()

        # Root first, then descendants (recursive query order).
        ordered = []
        root_row = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
        ).fetchone()
        if root_row:
            ordered.append(dict(root_row))
        for tid in task_ids:
            if tid == task_id:
                continue
            row = conn.execute(
                _join_project("SELECT * FROM tasks WHERE id=?"), (tid,)
            ).fetchone()
            if row:
                ordered.append(dict(row))
        return ordered
    finally:
        conn.close()


def delete_task_permanently(task_id: str, include_subtasks: bool = True) -> list[str] | None:
    """
    Hard-delete a task (and optionally descendants) plus related runtime records.
    Returns deleted task IDs (root first) or None if task missing.
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        root = conn.execute("SELECT id FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not root:
            conn.rollback()
            return None

        if include_subtasks:
            rows = conn.execute(
                """
                WITH RECURSIVE task_tree(id) AS (
                    SELECT id FROM tasks WHERE id=?
                    UNION ALL
                    SELECT t.id
                      FROM tasks t
                      JOIN task_tree tt ON t.parent_task_id = tt.id
                )
                SELECT id FROM task_tree
                """,
                (task_id,),
            ).fetchall()
            task_ids = [str(r["id"]) for r in rows if str(r["id"] or "").strip()]
        else:
            task_ids = [task_id]

        if not task_ids:
            conn.rollback()
            return None

        placeholders = ",".join("?" for _ in task_ids)
        conn.execute(f"DELETE FROM logs WHERE task_id IN ({placeholders})", task_ids)
        conn.execute(f"DELETE FROM task_handoffs WHERE task_id IN ({placeholders})", task_ids)
        conn.execute(f"DELETE FROM agent_outputs WHERE task_id IN ({placeholders})", task_ids)
        conn.execute(f"DELETE FROM tasks WHERE id IN ({placeholders})", task_ids)
        conn.commit()
        return task_ids
    finally:
        conn.close()


def claim_task(
    status: str,
    working_status: str,
    agent: str,
    agent_key: str,
    respect_assignment: bool = True,
    lease_ttl_secs: int = 180,
    project_id: str | None = None,
    user_id: str | None = None,
    is_admin: bool = True,
    per_project_max_workers: int = 0,
    per_agent_type_max_workers: int = 0,
) -> dict | None:
    """
    Atomically claim the next task in `status` and move it to `working_status`.
    Returns the claimed task row (with joined project fields) or None.
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        now = _now()
        where = [
            "t.status=?",
            "t.archived=0",
            "(TRIM(COALESCE(t.cooldown_until, ''))='' OR t.cooldown_until <= ?)",
        ]
        params: list[str] = [status, now]
        normalized_agent_key = str(agent_key or "").strip().lower()

        if project_id:
            where.append("t.project_id=?")
            params.append(project_id)

        if user_id and not is_admin:
            where.append(
                "EXISTS (SELECT 1 FROM projects p WHERE p.id=t.project_id AND p.created_by_user_id=?)"
            )
            params.append(user_id)

        if project_id and int(per_project_max_workers or 0) > 0:
            active_in_project = conn.execute(
                """
                SELECT COUNT(1) AS n
                  FROM tasks
                 WHERE project_id=?
                   AND archived=0
                   AND TRIM(COALESCE(lease_token, '')) != ''
                   AND status NOT IN ('completed', ?)
                """,
                (project_id, CANCELLED_STATUS),
            ).fetchone()
            if int(active_in_project["n"] or 0) >= int(per_project_max_workers):
                conn.rollback()
                return None

        if int(per_agent_type_max_workers or 0) > 0 and normalized_agent_key:
            active_for_agent = conn.execute(
                """
                SELECT COUNT(1) AS n
                  FROM tasks
                 WHERE archived=0
                   AND status=?
                   AND TRIM(COALESCE(lease_token, '')) != ''
                   AND (
                       LOWER(COALESCE(assignee, ''))=?
                       OR LOWER(COALESCE(assignee, '')) LIKE ?
                   )
                """,
                (working_status, normalized_agent_key, f"{normalized_agent_key}__%"),
            ).fetchone()
            if int(active_for_agent["n"] or 0) >= int(per_agent_type_max_workers):
                conn.rollback()
                return None

        if respect_assignment:
            where.append("(t.assigned_agent IS NULL OR t.assigned_agent=?)")
            params.append(normalized_agent_key or agent_key)

        if status == "todo":
            where.append(
                f"""
                (
                    t.parent_task_id IS NULL
                    OR NOT EXISTS (
                        SELECT 1
                          FROM tasks prev
                         WHERE prev.parent_task_id = t.parent_task_id
                           AND prev.id != t.id
                           AND COALESCE(prev.archived, 0) = 0
                           AND prev.status NOT IN ('approved', 'pending_acceptance', 'completed', '{CANCELLED_STATUS}')
                           AND (
                                 (
                                   COALESCE(t.subtask_order, 0) > 0
                                   AND (
                                       (COALESCE(prev.subtask_order, 0) > 0 AND prev.subtask_order < t.subtask_order)
                                       OR COALESCE(prev.subtask_order, 0) <= 0
                                   )
                                 )
                                 OR
                                 (
                                   COALESCE(t.subtask_order, 0) <= 0
                                   AND (
                                       prev.created_at < t.created_at
                                       OR (prev.created_at = t.created_at AND prev.id < t.id)
                                   )
                                 )
                           )
                    )
                )
                """
            )
            where.append(
                """
                NOT EXISTS (
                    SELECT 1
                      FROM task_dependencies td
                      JOIN tasks dep ON dep.id = td.depends_on_task_id
                     WHERE td.task_id = t.id
                       AND (
                           (
                               COALESCE(td.required_state, 'approved') = 'completed'
                               AND dep.status != 'completed'
                           )
                           OR
                           (
                               COALESCE(td.required_state, 'approved') = 'approved'
                               AND dep.status NOT IN ('approved', 'pending_acceptance', 'completed')
                           )
                       )
                )
                """
            )

        row = conn.execute(
            f"SELECT t.id FROM tasks t WHERE {' AND '.join(where)} ORDER BY t.priority ASC, t.updated_at ASC LIMIT 1",
            tuple(params),
        ).fetchone()
        if not row:
            conn.rollback()
            return None

        run_id = str(uuid.uuid4())
        lease_token = str(uuid.uuid4())
        lease_expires_at = _lease_deadline_iso(lease_ttl_secs)
        cur = conn.execute(
            """
            UPDATE tasks
               SET status=?,
                   assignee=?,
                   claim_run_id=?,
                   lease_token=?,
                   lease_expires_at=?,
                   updated_at=?
             WHERE id=? AND status=? AND archived=0
            """,
            (
                working_status,
                agent,
                run_id,
                lease_token,
                lease_expires_at,
                now,
                row["id"],
                status,
            ),
        )
        if cur.rowcount != 1:
            conn.rollback()
            return None

        conn.commit()
        claimed = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"), (row["id"],)
        ).fetchone()
        task = dict(claimed) if claimed else None
        if task:
            _enrich_task_rows_in_conn(conn, [task], persist_cancel_reason=True)
        return task
    finally:
        conn.close()


def recover_stale_tasks_for_agent(agent_key: str) -> list[dict]:
    """
    Recover tasks left in an agent's working state when the agent appears stale.
    Returns changed rows as:
      {"task": <task_row>, "from_status": "...", "to_status": "..."}.
    """
    key = str(agent_key or "").strip().lower()
    if not key:
        return []
    conn = get_conn()
    changed: list[dict] = []
    try:
        rows = conn.execute(
            "SELECT poll_statuses, working_status FROM agent_types WHERE key=? AND working_status != ''",
            (key,),
        ).fetchall()
        if not rows:
            return []
        now = _now()
        for row in rows:
            working = str(row["working_status"] or "").strip()
            if not working:
                continue
            poll = _parse_poll_statuses(row["poll_statuses"])
            reset_to = poll[0] if poll else "todo"
            task_rows = conn.execute(
                "SELECT id FROM tasks WHERE status=? AND assignee=? AND archived=0",
                (working, key),
            ).fetchall()
            for t in task_rows:
                conn.execute(
                    """
                    UPDATE tasks
                       SET status=?,
                           assignee=NULL,
                           claim_run_id=NULL,
                           lease_token=NULL,
                           lease_expires_at=NULL,
                           updated_at=?
                     WHERE id=?
                    """,
                    (reset_to, now, t["id"]),
                )
                updated = conn.execute(
                    _join_project("SELECT * FROM tasks WHERE id=?"), (t["id"],)
                ).fetchone()
                if updated:
                    changed.append(
                        {
                            "task": dict(updated),
                            "from_status": working,
                            "to_status": reset_to,
                        }
                    )
        if changed:
            conn.commit()
        else:
            conn.rollback()
    finally:
        conn.close()
    return changed


def renew_task_lease(
    task_id: str,
    run_id: str,
    lease_token: str,
    lease_ttl_secs: int = 180,
) -> dict | None:
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _assert_task_fence_in_conn(
            conn,
            task_id=task_id,
            expected_run_id=run_id,
            expected_lease_token=lease_token,
            strict_if_active=True,
        )
        now = _now()
        lease_expires_at = _lease_deadline_iso(lease_ttl_secs)
        cur = conn.execute(
            """
            UPDATE tasks
               SET lease_expires_at=?,
                   updated_at=?
             WHERE id=?
               AND claim_run_id=?
               AND lease_token=?
               AND assignee IS NOT NULL
               AND archived=0
            """,
            (lease_expires_at, now, task_id, run_id, lease_token),
        )
        if cur.rowcount != 1:
            conn.rollback()
            return None
        conn.commit()
        row = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"),
            (task_id,),
        ).fetchone()
        return dict(row) if row else None
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def recover_expired_task_leases(
    grace_secs: int = 0,
    exclude_task_ids: set[str] | None = None,
) -> list[dict]:
    """
    Recover tasks whose lease has expired.
    Returns changed rows as:
      {"task": <task_row>, "from_status": "...", "to_status": "...",
       "agent_key": "...", "expired_secs": <int>}
    """
    conn = get_conn()
    changed: list[dict] = []
    excluded = {str(x or "").strip() for x in (exclude_task_ids or set()) if str(x or "").strip()}
    try:
        conn.execute("BEGIN IMMEDIATE")
        now_dt = datetime.utcnow()
        now = now_dt.isoformat()
        cutoff = (now_dt - timedelta(seconds=max(0, int(grace_secs)))).isoformat()
        rows = conn.execute(
            """
            SELECT id, status, assignee, lease_expires_at
              FROM tasks
             WHERE archived=0
               AND assignee IS NOT NULL
               AND TRIM(COALESCE(lease_token, '')) != ''
               AND TRIM(COALESCE(lease_expires_at, '')) != ''
               AND lease_expires_at <= ?
            """,
            (cutoff,),
        ).fetchall()
        for row in rows:
            task_id = str(row["id"] or "").strip()
            if not task_id:
                continue
            if task_id in excluded:
                continue
            from_status = str(row["status"] or "").strip()
            agent_key = str(row["assignee"] or "").strip().lower()
            cfg = conn.execute(
                "SELECT poll_statuses, working_status FROM agent_types WHERE key=?",
                (agent_key,),
            ).fetchone()
            if cfg:
                working = str(cfg["working_status"] or "").strip()
                poll = _parse_poll_statuses(cfg["poll_statuses"])
                reset_to = poll[0] if poll else "todo"
            else:
                working = ""
                reset_to = "todo"
            to_status = reset_to if (working and from_status == working) else from_status
            conn.execute(
                """
                UPDATE tasks
                   SET status=?,
                       assignee=NULL,
                       claim_run_id=NULL,
                       lease_token=NULL,
                       lease_expires_at=NULL,
                       updated_at=?
                 WHERE id=?
                   AND archived=0
                """,
                (to_status, now, task_id),
            )
            updated = conn.execute(
                _join_project("SELECT * FROM tasks WHERE id=?"),
                (task_id,),
            ).fetchone()
            if not updated:
                continue
            expired_secs = 0
            lease_expires_at = str(row["lease_expires_at"] or "").strip()
            if lease_expires_at:
                try:
                    exp_dt = datetime.fromisoformat(lease_expires_at.rstrip("Z"))
                    expired_secs = max(0, int((now_dt - exp_dt).total_seconds()))
                except Exception:
                    expired_secs = 0
            changed.append(
                {
                    "task": dict(updated),
                    "from_status": from_status,
                    "to_status": to_status,
                    "agent_key": agent_key,
                    "expired_secs": expired_secs,
                }
            )
        if changed:
            conn.commit()
        else:
            conn.rollback()
    finally:
        conn.close()
    return changed


def add_agent_output(
    agent: str,
    line: str,
    project_id: str | None = None,
    task_id: str | None = None,
    run_id: str | None = None,
    kind: str | None = None,
    event: str | None = None,
    exit_code: int | None = None,
    keep_last: int = 1000,
) -> dict:
    conn = get_conn()
    now = _now()
    output_kind = str(kind or "line").strip().lower() or "line"
    output_event = str(event or "line").strip().lower() or "line"
    output_exit_code = None if exit_code is None else int(exit_code)
    cur = conn.execute(
        """
        INSERT INTO agent_outputs (agent, project_id, task_id, run_id, line, kind, event, exit_code, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(agent or "").strip().lower(),
            str(project_id or "").strip() or None,
            str(task_id or "").strip() or None,
            str(run_id or "").strip() or None,
            str(line or ""),
            output_kind,
            output_event,
            output_exit_code,
            now,
        ),
    )
    row_id = int(cur.lastrowid or 0)
    keep = max(1, int(keep_last or 1000))
    conn.execute(
        """
        DELETE FROM agent_outputs
         WHERE agent=?
           AND id NOT IN (
               SELECT id
                 FROM agent_outputs
                WHERE agent=?
                ORDER BY id DESC
                LIMIT ?
           )
        """,
        (str(agent or "").strip().lower(), str(agent or "").strip().lower(), keep),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM agent_outputs WHERE id=?", (row_id,)).fetchone()
    conn.close()
    return dict(row) if row else {
        "id": row_id,
        "agent": str(agent or "").strip().lower(),
        "project_id": str(project_id or "").strip() or None,
        "task_id": str(task_id or "").strip() or None,
        "run_id": str(run_id or "").strip() or None,
        "line": str(line or ""),
        "kind": output_kind,
        "event": output_event,
        "exit_code": output_exit_code,
        "created_at": now,
    }


def list_agent_output_agents() -> list[str]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT DISTINCT agent FROM agent_outputs WHERE TRIM(COALESCE(agent, '')) != ''"
    ).fetchall()
    conn.close()
    return [str(r["agent"] or "").strip().lower() for r in rows if str(r["agent"] or "").strip()]


def get_agent_output_lines(agent: str, limit: int = 1000) -> list[str]:
    return [str(e.get("line") or "") for e in get_agent_output_entries(agent, limit=limit)]


def get_agent_output_entries(agent: str, limit: int = 1000) -> list[dict]:
    key = str(agent or "").strip().lower()
    if not key:
        return []
    size = max(1, int(limit or 1000))
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT line, project_id, task_id, run_id, kind, event, exit_code, created_at
          FROM agent_outputs
         WHERE agent=?
         ORDER BY id DESC
         LIMIT ?
        """,
        (key, size),
    ).fetchall()
    conn.close()
    # DB query is DESC for performance; reverse to natural chronology.
    entries: list[dict] = []
    for r in reversed(rows):
        entries.append(
            {
                "line": str(r["line"] or ""),
                "project_id": str(r["project_id"] or "").strip() or None,
                "task_id": str(r["task_id"] or "").strip() or None,
                "run_id": str(r["run_id"] or "").strip() or None,
                "kind": str(r["kind"] or "line").strip().lower() or "line",
                "event": str(r["event"] or "line").strip().lower() or "line",
                "exit_code": int(r["exit_code"]) if r["exit_code"] is not None else None,
                "created_at": str(r["created_at"] or ""),
            }
        )
    return entries


def _add_log_in_conn(conn, task_id: str, agent: str, message: str) -> dict:
    now = _now()
    cur = conn.execute(
        "INSERT INTO logs (task_id, agent, message, created_at) VALUES (?,?,?,?)",
        (task_id, agent, message, now),
    )
    log = {"id": cur.lastrowid, "task_id": task_id, "agent": agent,
           "message": message, "created_at": now}
    return log


def add_log(task_id: str, agent: str, message: str) -> dict:
    conn = get_conn()
    try:
        log = _add_log_in_conn(conn, task_id, agent, message)
        conn.commit()
        return log
    finally:
        conn.close()


def _add_handoff_in_conn(
    conn,
    task_id: str,
    stage: str,
    from_agent: str,
    to_agent: str | None = None,
    status_from: str | None = None,
    status_to: str | None = None,
    title: str = "",
    summary: str = "",
    commit_hash: str | None = None,
    conclusion: str | None = None,
    payload: dict | None = None,
    artifact_path: str | None = None,
) -> dict:
    now = _now()
    payload_text = json.dumps(payload or {}, ensure_ascii=False)
    cur = conn.execute(
        """
        INSERT INTO task_handoffs
        (task_id, stage, from_agent, to_agent, status_from, status_to, title, summary, commit_hash, conclusion, payload, artifact_path, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            task_id,
            stage,
            from_agent,
            to_agent,
            status_from,
            status_to,
            title,
            summary,
            commit_hash,
            conclusion,
            payload_text,
            artifact_path,
            now,
        ),
    )
    row = conn.execute(
        "SELECT * FROM task_handoffs WHERE id=?",
        (cur.lastrowid,),
    ).fetchone()
    return dict(row) if row else {
        "id": cur.lastrowid,
        "task_id": task_id,
        "stage": stage,
        "from_agent": from_agent,
        "to_agent": to_agent,
        "status_from": status_from,
        "status_to": status_to,
        "title": title,
        "summary": summary,
        "commit_hash": commit_hash,
        "conclusion": conclusion,
        "payload": payload_text,
        "artifact_path": artifact_path,
        "created_at": now,
    }


def add_handoff(
    task_id: str,
    stage: str,
    from_agent: str,
    to_agent: str | None = None,
    status_from: str | None = None,
    status_to: str | None = None,
    title: str = "",
    summary: str = "",
    commit_hash: str | None = None,
    conclusion: str | None = None,
    payload: dict | None = None,
    artifact_path: str | None = None,
) -> dict:
    conn = get_conn()
    try:
        row = _add_handoff_in_conn(
            conn,
            task_id=task_id,
            stage=stage,
            from_agent=from_agent,
            to_agent=to_agent,
            status_from=status_from,
            status_to=status_to,
            title=title,
            summary=summary,
            commit_hash=commit_hash,
            conclusion=conclusion,
            payload=payload,
            artifact_path=artifact_path,
        )
        conn.commit()
        return row
    finally:
        conn.close()


def _normalize_patchset_status(value: str | None, *, default: str = PATCHSET_STATUS_DRAFT) -> str:
    text = str(value or "").strip().lower()
    if text in PATCHSET_ALLOWED_STATUSES:
        return text
    return default


def _normalize_patchset_queue_status(value: str | None, *, default: str = "") -> str:
    text = str(value or "").strip().lower()
    if text in PATCHSET_ALLOWED_QUEUE_STATUSES:
        return text
    return default


def _infer_patchset_queue_status(status: str, *, default: str = "") -> str:
    normalized = _normalize_patchset_status(status, default=PATCHSET_STATUS_DRAFT)
    if normalized == PATCHSET_STATUS_APPROVED:
        return PATCHSET_QUEUE_QUEUED
    if normalized == PATCHSET_STATUS_MERGED:
        return PATCHSET_QUEUE_MERGED
    if normalized == PATCHSET_STATUS_STALE:
        return PATCHSET_QUEUE_STALE
    if normalized in {
        PATCHSET_STATUS_DRAFT,
        PATCHSET_STATUS_SUBMITTED,
        PATCHSET_STATUS_REJECTED,
        PATCHSET_STATUS_SUPERSEDED,
    }:
        return ""
    return default


def _coerce_patchset_commit_list(raw) -> list[dict]:
    items = raw
    if isinstance(raw, str):
        try:
            items = json.loads(raw)
        except Exception:
            items = []
    if not isinstance(items, list):
        return []
    out: list[dict] = []
    for item in items:
        if isinstance(item, dict):
            commit_hash = str(
                item.get("hash")
                or item.get("commit_hash")
                or item.get("head_sha")
                or ""
            ).strip()
            if not commit_hash:
                continue
            short = str(item.get("short") or commit_hash[:12]).strip()[:24]
            subject = str(item.get("subject") or "").strip()[:240]
            out.append({"hash": commit_hash[:120], "short": short, "subject": subject})
        else:
            commit_hash = str(item or "").strip()
            if commit_hash:
                out.append({"hash": commit_hash[:120], "short": commit_hash[:12], "subject": ""})
        if len(out) >= 128:
            break
    return out


def _coerce_patchset_changed_files(raw) -> list[dict]:
    items = raw
    if isinstance(raw, str):
        try:
            items = json.loads(raw)
        except Exception:
            items = []
    if not isinstance(items, list):
        return []
    out: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or item.get("new_path") or "").strip()
        if not path:
            continue
        normalized = {
            "status": str(item.get("status") or "M").strip()[:16] or "M",
            "path": path[:500],
        }
        old_path = str(item.get("old_path") or "").strip()
        if old_path:
            normalized["old_path"] = old_path[:500]
        out.append(normalized)
        if len(out) >= 256:
            break
    return out


def _sanitize_patchset_manifest_value(value, *, depth: int = 0):
    if depth >= 4:
        return "..."
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:500]
    if isinstance(value, list):
        return [_sanitize_patchset_manifest_value(item, depth=depth + 1) for item in value[:32]]
    if isinstance(value, dict):
        out: dict[str, object] = {}
        for key, item in list(value.items())[:40]:
            out[str(key)[:120]] = _sanitize_patchset_manifest_value(item, depth=depth + 1)
        return out
    return str(value)[:500]


def _coerce_patchset_artifact_manifest(raw) -> dict:
    data = raw
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except Exception:
            data = {}
    if not isinstance(data, dict):
        return {}
    return _sanitize_patchset_manifest_value(data)


def _deserialize_patchset_row(row) -> dict | None:
    if not row:
        return None
    out = dict(row)
    out["commit_list"] = _coerce_patchset_commit_list(out.get("commit_list"))
    out["changed_files"] = _coerce_patchset_changed_files(out.get("changed_files"))
    out["artifact_manifest"] = _coerce_patchset_artifact_manifest(out.get("artifact_manifest"))
    raw_clean = out.get("worktree_clean")
    if isinstance(raw_clean, str):
        out["worktree_clean"] = raw_clean.strip().lower() not in {"0", "false", "off", "no"}
    else:
        out["worktree_clean"] = bool(raw_clean)
    try:
        out["commit_count"] = int(out.get("commit_count") or 0)
    except Exception:
        out["commit_count"] = 0
    return out


def _patchset_identity(task_id: str, base_sha: str, head_sha: str) -> str:
    digest = hashlib.sha1(f"{task_id}|{base_sha}|{head_sha}".encode("utf-8")).hexdigest()
    return f"ps_{digest[:24]}"


def _save_task_patchset_in_conn(
    conn,
    task_id: str,
    patchset: dict | None,
    *,
    update_task_refs: bool = True,
) -> dict | None:
    payload = dict(patchset or {})
    task_id = str(task_id or payload.get("task_id") or "").strip()
    if not task_id:
        return None

    base_sha = str(payload.get("base_sha") or "").strip()[:120]
    head_sha = str(
        payload.get("head_sha")
        or payload.get("commit_hash")
        or ""
    ).strip()[:120]
    patchset_id = str(payload.get("id") or "").strip()[:80]
    if not patchset_id:
        if not head_sha:
            return None
        patchset_id = _patchset_identity(task_id, base_sha, head_sha)

    existing_row = conn.execute(
        "SELECT * FROM task_patchsets WHERE id=?",
        (patchset_id,),
    ).fetchone()
    existing = dict(existing_row) if existing_row else {}

    commit_list = _coerce_patchset_commit_list(payload.get("commit_list"))
    changed_files = _coerce_patchset_changed_files(
        payload.get("changed_files") if "changed_files" in payload else existing.get("changed_files")
    )
    try:
        commit_count = int(payload.get("commit_count"))
    except Exception:
        commit_count = 0
    if commit_count <= 0:
        commit_count = len(commit_list)
    if commit_count <= 0 and head_sha:
        commit_count = 1

    status = _normalize_patchset_status(payload.get("status"))
    source_branch = str(payload.get("source_branch") or "").strip()[:255]
    diff_stat = str(payload.get("diff_stat") or "").strip()[:4000]
    merge_strategy = str(payload.get("merge_strategy") or "").strip()[:80]
    summary = str(payload.get("summary") or payload.get("conclusion") or "").strip()[:1000]
    artifact_path = str(payload.get("artifact_path") or "").strip()[:1000] or None
    artifact_manifest = _coerce_patchset_artifact_manifest(
        payload.get("artifact_manifest") if "artifact_manifest" in payload else existing.get("artifact_manifest")
    )
    created_by_agent = str(payload.get("created_by_agent") or "").strip()[:80]
    worktree_clean = payload.get("worktree_clean")
    if isinstance(worktree_clean, str):
        worktree_clean = worktree_clean.strip().lower() not in {"0", "false", "off", "no"}
    worktree_clean_flag = 1 if worktree_clean is not False else 0
    queue_status = _normalize_patchset_queue_status(
        payload.get("queue_status"),
        default=str(existing.get("queue_status") or "").strip().lower(),
    )
    if not queue_status:
        queue_status = _infer_patchset_queue_status(
            status,
            default=str(existing.get("queue_status") or "").strip().lower(),
        )
    queue_reason = str(payload.get("queue_reason") or existing.get("queue_reason") or "").strip()[:240]
    reviewed_main_sha = str(payload.get("reviewed_main_sha") or existing.get("reviewed_main_sha") or "").strip()[:120]
    queue_main_sha = str(payload.get("queue_main_sha") or existing.get("queue_main_sha") or "").strip()[:120]

    now = _now()
    approved_at = str(payload.get("approved_at") or existing.get("approved_at") or "").strip()
    if status == PATCHSET_STATUS_APPROVED and not approved_at:
        approved_at = now
    queued_at = str(payload.get("queued_at") or existing.get("queued_at") or "").strip()
    if queue_status == PATCHSET_QUEUE_QUEUED and not queued_at:
        queued_at = approved_at or now
    queue_started_at = str(payload.get("queue_started_at") or existing.get("queue_started_at") or "").strip()
    if queue_status == PATCHSET_QUEUE_PROCESSING and not queue_started_at:
        queue_started_at = now
    queue_finished_at = str(payload.get("queue_finished_at") or existing.get("queue_finished_at") or "").strip()
    if queue_status in {PATCHSET_QUEUE_MERGED, PATCHSET_QUEUE_STALE, PATCHSET_QUEUE_FAILED} and not queue_finished_at:
        queue_finished_at = now
    merged_at = str(payload.get("merged_at") or existing.get("merged_at") or "").strip()
    if status == PATCHSET_STATUS_MERGED and not merged_at:
        merged_at = now

    if existing:
        conn.execute(
            """
            UPDATE task_patchsets
               SET task_id=?,
                   source_branch=?,
                   base_sha=?,
                   head_sha=?,
                   commit_count=?,
                   commit_list=?,
                   changed_files=?,
                   artifact_manifest=?,
                   diff_stat=?,
                   status=?,
                   worktree_clean=?,
                   merge_strategy=?,
                   summary=?,
                   artifact_path=?,
                   created_by_agent=?,
                   queue_status=?,
                   queue_reason=?,
                   queued_at=?,
                   queue_started_at=?,
                   queue_finished_at=?,
                   approved_at=?,
                   merged_at=?,
                   reviewed_main_sha=?,
                   queue_main_sha=?,
                   updated_at=?
             WHERE id=?
            """,
            (
                task_id,
                source_branch,
                base_sha,
                head_sha,
                max(0, int(commit_count)),
                json.dumps(commit_list, ensure_ascii=False),
                json.dumps(changed_files, ensure_ascii=False),
                json.dumps(artifact_manifest, ensure_ascii=False),
                diff_stat,
                status,
                worktree_clean_flag,
                merge_strategy,
                summary,
                artifact_path,
                created_by_agent,
                queue_status,
                queue_reason,
                queued_at or None,
                queue_started_at or None,
                queue_finished_at or None,
                approved_at or None,
                merged_at or None,
                reviewed_main_sha,
                queue_main_sha,
                now,
                patchset_id,
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO task_patchsets
            (id, task_id, source_branch, base_sha, head_sha, commit_count, commit_list, changed_files, artifact_manifest, diff_stat,
             status, worktree_clean, merge_strategy, summary, artifact_path, created_by_agent,
             queue_status, queue_reason, queued_at, queue_started_at, queue_finished_at, approved_at,
             merged_at, reviewed_main_sha, queue_main_sha, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                patchset_id,
                task_id,
                source_branch,
                base_sha,
                head_sha,
                max(0, int(commit_count)),
                json.dumps(commit_list, ensure_ascii=False),
                json.dumps(changed_files, ensure_ascii=False),
                json.dumps(artifact_manifest, ensure_ascii=False),
                diff_stat,
                status,
                worktree_clean_flag,
                merge_strategy,
                summary,
                artifact_path,
                created_by_agent,
                queue_status,
                queue_reason,
                queued_at or None,
                queue_started_at or None,
                queue_finished_at or None,
                approved_at or None,
                merged_at or None,
                reviewed_main_sha,
                queue_main_sha,
                now,
                now,
            ),
        )

    if update_task_refs:
        if status in {PATCHSET_STATUS_SUBMITTED, PATCHSET_STATUS_APPROVED}:
            conn.execute(
                """
                UPDATE task_patchsets
                   SET status=?,
                       queue_status='',
                       queue_reason='superseded_by_newer_patchset',
                       queue_finished_at=COALESCE(queue_finished_at, ?),
                       updated_at=?
                 WHERE task_id=?
                   AND id<>?
                   AND status NOT IN (?, ?, ?)
                """,
                (
                    PATCHSET_STATUS_SUPERSEDED,
                    now,
                    now,
                    task_id,
                    patchset_id,
                    PATCHSET_STATUS_MERGED,
                    PATCHSET_STATUS_SUPERSEDED,
                    status,
                ),
            )
        if status == PATCHSET_STATUS_MERGED:
            conn.execute(
                """
                UPDATE tasks
                   SET current_patchset_id=?,
                       current_patchset_status=?,
                       merged_patchset_id=?,
                       updated_at=?
                 WHERE id=?
                """,
                (patchset_id, status, patchset_id, now, task_id),
            )
        else:
            conn.execute(
                """
                UPDATE tasks
                   SET current_patchset_id=?,
                       current_patchset_status=?,
                       updated_at=?
                 WHERE id=?
                """,
                (patchset_id, status, now, task_id),
            )

    row = conn.execute(
        "SELECT * FROM task_patchsets WHERE id=?",
        (patchset_id,),
    ).fetchone()
    return _deserialize_patchset_row(row)


def save_task_patchset(task_id: str, patchset: dict | None, *, update_task_refs: bool = True) -> dict | None:
    conn = get_conn()
    try:
        row = _save_task_patchset_in_conn(
            conn,
            task_id=task_id,
            patchset=patchset,
            update_task_refs=update_task_refs,
        )
        conn.commit()
        return row
    finally:
        conn.close()


def get_task_patchset(patchset_id: str) -> dict | None:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM task_patchsets WHERE id=?",
            (patchset_id,),
        ).fetchone()
        return _deserialize_patchset_row(row)
    finally:
        conn.close()


def list_task_patchsets(task_id: str) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM task_patchsets WHERE task_id=? ORDER BY created_at ASC, id ASC",
            (task_id,),
        ).fetchall()
        return [_deserialize_patchset_row(r) for r in rows if r]
    finally:
        conn.close()


def get_patchset_metrics(project_id: str | None = None) -> dict:
    conn = get_conn()
    try:
        patchset_params: list[object] = []
        patchset_where = ""
        if project_id:
            patchset_where = "WHERE t.project_id=?"
            patchset_params.append(project_id)
        patchset_rows = conn.execute(
            f"""
            SELECT ps.*, t.project_id, t.status AS task_status, t.updated_at AS task_updated_at
              FROM task_patchsets ps
              JOIN tasks t ON t.id = ps.task_id
             {patchset_where}
            ORDER BY ps.created_at ASC, ps.id ASC
            """,
            tuple(patchset_params),
        ).fetchall()

        handoff_params: list[object] = []
        handoff_where = ""
        if project_id:
            handoff_where = "WHERE t.project_id=?"
            handoff_params.append(project_id)
        handoff_rows = conn.execute(
            f"""
            SELECT h.task_id, h.stage, h.status_to, h.created_at
              FROM task_handoffs h
              JOIN tasks t ON t.id = h.task_id
             {handoff_where}
            """,
            tuple(handoff_params),
        ).fetchall()

        patchsets = [dict(row) for row in patchset_rows]
        handoffs = [dict(row) for row in handoff_rows]
        all_time = _compute_patchset_metrics_window(patchsets, handoffs, window_hours=None)
        recent_24h = _compute_patchset_metrics_window(patchsets, handoffs, window_hours=24)
        return {
            "project_id": project_id,
            **all_time,
            "windows": {
                "all_time": all_time,
                "last_24h": recent_24h,
            },
        }
    finally:
        conn.close()


def _compute_patchset_metrics_window(
    patchsets: list[dict],
    handoffs: list[dict],
    *,
    window_hours: int | None,
) -> dict:
    since_dt = None
    if window_hours:
        since_dt = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=max(1, int(window_hours)))

    def _in_window(ts_value, *, allow_empty: bool = False) -> bool:
        if since_dt is None:
            return True
        parsed = _parse_iso_datetime(ts_value)
        if not parsed:
            return allow_empty
        return parsed >= since_dt

    visible_patchsets = [item for item in patchsets if _in_window(item.get("created_at"))]
    visible_handoffs = [item for item in handoffs if _in_window(item.get("created_at"))]
    total_patchsets = len(visible_patchsets)
    active_patchsets = [
        item for item in visible_patchsets
        if str(item.get("status") or "").strip().lower() not in {PATCHSET_STATUS_DRAFT, PATCHSET_STATUS_SUPERSEDED}
    ]
    queued = sum(1 for item in visible_patchsets if str(item.get("queue_status") or "").strip().lower() == PATCHSET_QUEUE_QUEUED)
    processing = sum(1 for item in visible_patchsets if str(item.get("queue_status") or "").strip().lower() == PATCHSET_QUEUE_PROCESSING)
    stale = sum(1 for item in visible_patchsets if str(item.get("status") or "").strip().lower() == PATCHSET_STATUS_STALE)
    merged = sum(1 for item in visible_patchsets if str(item.get("status") or "").strip().lower() == PATCHSET_STATUS_MERGED)
    review_pass = sum(1 for item in visible_handoffs if str(item.get("stage") or "").strip() == "review_to_manager")
    review_return = sum(1 for item in visible_handoffs if str(item.get("stage") or "").strip() == "review_to_dev")
    manager_pass = sum(1 for item in visible_handoffs if str(item.get("stage") or "").strip() == "merge_to_acceptance")
    manager_return = sum(1 for item in visible_handoffs if str(item.get("stage") or "").strip() == "merge_to_dev")
    preflight_failed = sum(
        1 for item in visible_handoffs
        if str(item.get("stage") or "").strip() in {"dev_dirty_patchset", "dev_commit_required", "dev_no_progress"}
        or str(item.get("stage") or "").strip().endswith("_dirty_patchset")
    )

    review_cycles_by_task: dict[str, int] = {}
    patchsets_by_task: dict[str, int] = {}
    acceptance_durations: list[float] = []
    queue_latencies: list[float] = []
    stale_reason_counts: dict[str, int] = {}
    first_patchset_by_task: dict[str, datetime] = {}
    for item in patchsets:
        task_key = str(item.get("task_id") or "").strip()
        if not task_key:
            continue
        created_at = _parse_iso_datetime(item.get("created_at"))
        if created_at and task_key not in first_patchset_by_task:
            first_patchset_by_task[task_key] = created_at
    for item in active_patchsets:
        task_key = str(item.get("task_id") or "").strip()
        if not task_key:
            continue
        patchsets_by_task[task_key] = patchsets_by_task.get(task_key, 0) + 1
    for item in visible_handoffs:
        task_key = str(item.get("task_id") or "").strip()
        if not task_key:
            continue
        if str(item.get("stage") or "").strip() in {"review_to_manager", "review_to_dev"}:
            review_cycles_by_task[task_key] = review_cycles_by_task.get(task_key, 0) + 1

    accepted_tasks: set[str] = set()
    for item in patchsets:
        task_key = str(item.get("task_id") or "").strip()
        if not task_key or task_key in accepted_tasks:
            continue
        task_status = str(item.get("task_status") or "").strip().lower()
        finished = _parse_iso_datetime(item.get("task_updated_at"))
        if task_status not in {"pending_acceptance", "completed"} or not finished:
            continue
        if since_dt is not None and finished < since_dt:
            continue
        started = first_patchset_by_task.get(task_key)
        if started and finished >= started:
            acceptance_durations.append((finished - started).total_seconds())
            accepted_tasks.add(task_key)

    for item in patchsets:
        final_ts = _parse_iso_datetime(item.get("merged_at") or item.get("queue_finished_at"))
        if since_dt is not None and (not final_ts or final_ts < since_dt):
            continue
        queued_at = _parse_iso_datetime(item.get("queued_at"))
        merged_at = _parse_iso_datetime(item.get("merged_at"))
        if queued_at and merged_at and merged_at >= queued_at:
            queue_latencies.append((merged_at - queued_at).total_seconds())
        reason = str(item.get("queue_reason") or "").strip()
        if str(item.get("status") or "").strip().lower() == PATCHSET_STATUS_STALE and reason:
            stale_reason_counts[reason] = stale_reason_counts.get(reason, 0) + 1

    avg_review_cycles = (
        sum(review_cycles_by_task.values()) / len(review_cycles_by_task)
        if review_cycles_by_task else 0.0
    )
    avg_patchsets_per_task = (
        sum(patchsets_by_task.values()) / len(patchsets_by_task)
        if patchsets_by_task else 0.0
    )
    avg_submit_to_acceptance_secs = (
        sum(acceptance_durations) / len(acceptance_durations)
        if acceptance_durations else 0.0
    )
    avg_queue_latency_secs = (
        sum(queue_latencies) / len(queue_latencies)
        if queue_latencies else 0.0
    )
    return {
        "window": "last_24h" if window_hours else "all_time",
        "window_hours": int(window_hours or 0),
        "total_patchsets": total_patchsets,
        "active_patchsets": len(active_patchsets),
        "queued_patchsets": queued,
        "processing_patchsets": processing,
        "merged_patchsets": merged,
        "stale_patchsets": stale,
        "stale_patchset_rate": (stale / len(active_patchsets)) if active_patchsets else 0.0,
        "review_return_rate": (review_return / (review_pass + review_return)) if (review_pass + review_return) else 0.0,
        "manager_return_rate": (manager_return / (manager_pass + manager_return)) if (manager_pass + manager_return) else 0.0,
        "preflight_fail_rate": (preflight_failed / total_patchsets) if total_patchsets else 0.0,
        "review_cycles_per_task": avg_review_cycles,
        "patchsets_per_task": avg_patchsets_per_task,
        "avg_submit_to_acceptance_secs": avg_submit_to_acceptance_secs,
        "avg_queue_latency_secs": avg_queue_latency_secs,
        "stale_reason_counts": stale_reason_counts,
        "review_pass_count": review_pass,
        "review_return_count": review_return,
        "manager_pass_count": manager_pass,
        "manager_return_count": manager_return,
        "preflight_fail_count": preflight_failed,
    }


def transition_task(
    task_id: str,
    fields: dict | None = None,
    handoff: dict | None = None,
    log: dict | None = None,
    patchset: dict | None = None,
    expected_run_id: str | None = None,
    expected_lease_token: str | None = None,
) -> dict | None:
    """
    Atomically apply task update + optional handoff + optional log in one tx.
    Returns {"task": ..., "handoff": ..., "log": ...} or None when task missing.
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        guard_row = _assert_task_fence_in_conn(
            conn,
            task_id=task_id,
            expected_run_id=expected_run_id,
            expected_lease_token=expected_lease_token,
            strict_if_active=True,
        )
        if not guard_row:
            conn.rollback()
            return None
        update_fields = dict(fields or {})
        task = _update_task_in_conn(conn, task_id, **update_fields)
        if not task:
            conn.rollback()
            return None

        is_cancelled = (
            str(task.get("status") or "").strip().lower() == CANCELLED_STATUS
            or int(task.get("archived") or 0) == 1
        )
        created_handoff = None
        created_log = None
        created_patchset = None
        created_attempt = None
        created_evidence = None
        handoff_payload = handoff.get("payload") if isinstance((handoff or {}).get("payload"), dict) else {}
        if not is_cancelled and handoff:
            created_handoff = _add_handoff_in_conn(
                conn,
                task_id=task_id,
                stage=str(handoff.get("stage") or "").strip(),
                from_agent=str(handoff.get("from_agent") or "").strip(),
                to_agent=handoff.get("to_agent"),
                status_from=handoff.get("status_from"),
                status_to=handoff.get("status_to"),
                title=str(handoff.get("title") or ""),
                summary=str(handoff.get("summary") or ""),
                commit_hash=handoff.get("commit_hash"),
                conclusion=handoff.get("conclusion"),
                payload=handoff_payload,
                artifact_path=handoff.get("artifact_path"),
            )
        if not is_cancelled and patchset:
            created_patchset = _save_task_patchset_in_conn(
                conn,
                task_id=task_id,
                patchset=patchset,
                update_task_refs=True,
            )
            if created_patchset:
                row = conn.execute(_join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)).fetchone()
                task = dict(row) if row else task
                if task:
                    _enrich_task_rows_in_conn(conn, [task], persist_cancel_reason=True)
        if not is_cancelled and handoff_payload:
            attempt_payload = handoff_payload.get("attempt")
            if isinstance(attempt_payload, dict):
                failure_fingerprint = str(
                    attempt_payload.get("failure_fingerprint")
                    or attempt_payload.get("fingerprint")
                    or ""
                ).strip()
                if not failure_fingerprint:
                    failure_fingerprint = compute_failure_fingerprint(
                        stage=str(attempt_payload.get("stage") or handoff.get("stage") or "").strip(),
                        summary=str(attempt_payload.get("summary") or handoff.get("summary") or "").strip(),
                        output=str(attempt_payload.get("output") or ""),
                        extra=str(attempt_payload.get("conclusion") or handoff.get("conclusion") or ""),
                    )
                created_attempt = _add_task_attempt_in_conn(
                    conn,
                    task_id=task_id,
                    stage=str(attempt_payload.get("stage") or handoff.get("stage") or "").strip(),
                    outcome=str(attempt_payload.get("outcome") or "").strip(),
                    execution_phase=str(
                        attempt_payload.get("execution_phase")
                        or task.get("execution_phase")
                        or ""
                    ).strip(),
                    retry_strategy=str(
                        attempt_payload.get("retry_strategy")
                        or task.get("retry_strategy")
                        or RETRY_STRATEGY_DEFAULT
                    ).strip(),
                    failure_fingerprint=failure_fingerprint,
                    same_fingerprint_streak=int(
                        attempt_payload.get("same_fingerprint_streak")
                        or task.get("same_fingerprint_streak")
                        or 0
                    ),
                    summary=str(attempt_payload.get("summary") or handoff.get("summary") or "").strip(),
                    artifact_path=str(
                        attempt_payload.get("artifact_path")
                        or handoff.get("artifact_path")
                        or ""
                    ).strip()
                    or None,
                    metadata=attempt_payload.get("metadata")
                    if isinstance(attempt_payload.get("metadata"), dict)
                    else {},
                    created_by=str(handoff.get("from_agent") or "system").strip() or "system",
                )
            evidence_bundle = handoff_payload.get("evidence_bundle")
            if isinstance(evidence_bundle, dict):
                created_evidence = _add_task_evidence_in_conn(
                    conn,
                    task_id=task_id,
                    stage=str(handoff.get("stage") or "").strip(),
                    summary=str(
                        handoff_payload.get("evidence_summary")
                        or handoff.get("summary")
                        or ""
                    ).strip(),
                    bundle=evidence_bundle,
                    attempt_id=str((created_attempt or {}).get("id") or handoff_payload.get("attempt_id") or "").strip(),
                    artifact_path=str(
                        handoff_payload.get("evidence_artifact_path")
                        or handoff.get("artifact_path")
                        or ""
                    ).strip()
                    or None,
                    created_by=str(handoff.get("from_agent") or "system").strip() or "system",
                )
            issues_payload = handoff_payload.get("issues")
            if isinstance(issues_payload, list):
                _sync_task_issues_in_conn(
                    conn,
                    task_id=task_id,
                    source=str(handoff.get("from_agent") or "system").strip() or "system",
                    stage=str(handoff.get("stage") or "").strip(),
                    issues=issues_payload,
                    attempt_id=str((created_attempt or {}).get("id") or ""),
                    resolve_missing=bool(handoff_payload.get("resolve_missing_issues", True)),
                )
            resolve_sources_raw = handoff_payload.get("resolve_issue_sources")
            if handoff_payload.get("resolve_open_issues"):
                resolve_sources = (
                    [str(handoff.get("from_agent") or "").strip()]
                    if not isinstance(resolve_sources_raw, list)
                    else [str(item or "").strip() for item in resolve_sources_raw if str(item or "").strip()]
                )
                resolution = str(handoff_payload.get("issue_resolution_reason") or "status_advanced").strip()
                if resolve_sources:
                    for source in resolve_sources:
                        _resolve_task_issues_in_conn(
                            conn,
                            task_id,
                            source=source,
                            resolution=resolution,
                            attempt_id=str((created_attempt or {}).get("id") or ""),
                        )
                else:
                    _resolve_task_issues_in_conn(
                        conn,
                        task_id,
                        resolution=resolution,
                        attempt_id=str((created_attempt or {}).get("id") or ""),
                    )
        if not is_cancelled and log:
            created_log = _add_log_in_conn(
                conn,
                task_id=task_id,
                agent=str(log.get("agent") or "system").strip() or "system",
                message=str(log.get("message") or ""),
            )
        row = conn.execute(_join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)).fetchone()
        task = dict(row) if row else task
        if task:
            _enrich_task_rows_in_conn(conn, [task], persist_cancel_reason=True)
        conn.commit()
        return {
            "task": task,
            "handoff": created_handoff,
            "log": created_log,
            "patchset": created_patchset,
            "attempt": created_attempt,
            "evidence": created_evidence,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_logs(task_id: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM logs WHERE task_id=? ORDER BY created_at ASC", (task_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_handoffs(task_id: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM task_handoffs WHERE task_id=? ORDER BY created_at ASC, id ASC",
        (task_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_task_contract(task_id: str) -> dict | None:
    conn = get_conn()
    try:
        row = _latest_task_contract_in_conn(conn, task_id)
        return _normalize_contract_row(row)
    finally:
        conn.close()


def list_task_contracts(task_id: str) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM task_contracts WHERE task_id=? ORDER BY version DESC, created_at DESC",
            (task_id,),
        ).fetchall()
        return [item for item in (_normalize_contract_row(row) for row in rows) if item]
    finally:
        conn.close()


def list_task_issues(task_id: str, *, include_resolved: bool = True) -> list[dict]:
    conn = get_conn()
    try:
        return _list_task_issues_in_conn(conn, task_id, include_resolved=include_resolved)
    finally:
        conn.close()


def list_task_attempts(task_id: str, *, limit: int = 20) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM task_attempts WHERE task_id=? ORDER BY created_at DESC LIMIT ?",
            (task_id, max(1, int(limit or 20))),
        ).fetchall()
        return [item for item in (_normalize_attempt_row(row) for row in rows) if item]
    finally:
        conn.close()


def list_task_evidence(task_id: str, *, limit: int = 20) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM task_evidence WHERE task_id=? ORDER BY created_at DESC LIMIT ?",
            (task_id, max(1, int(limit or 20))),
        ).fetchall()
        return [item for item in (_normalize_evidence_row(row) for row in rows) if item]
    finally:
        conn.close()


def clear_task_agent_refs_for_deleted_agent(agent_key: str, working_status: str | None = None) -> list[dict]:
    """
    When a custom agent type is deleted, clear task-level references to that agent
    so the board no longer shows stale assignment/ownership.
    """
    key = str(agent_key or "").strip().lower()
    if not key:
        return []
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            _join_project(
                """
                SELECT *
                  FROM tasks
                 WHERE assigned_agent=?
                    OR dev_agent=?
                    OR assignee=?
                """
            ),
            (key, key, key),
        ).fetchall()
        task_ids = [str(r["id"] or "").strip() for r in rows if str(r["id"] or "").strip()]
        if not task_ids:
            conn.rollback()
            return []

        now = _now()
        conn.execute(
            "UPDATE tasks SET assigned_agent=NULL, updated_at=? WHERE assigned_agent=?",
            (now, key),
        )
        conn.execute(
            "UPDATE tasks SET dev_agent=NULL, updated_at=? WHERE dev_agent=?",
            (now, key),
        )
        if working_status:
            conn.execute(
                """
                UPDATE tasks
                   SET status='todo',
                       assignee=NULL,
                       claim_run_id=NULL,
                       lease_token=NULL,
                       lease_expires_at=NULL,
                       updated_at=?
                 WHERE assignee=?
                   AND status=?
                """,
                (now, key, str(working_status or "").strip()),
            )
        conn.execute(
            """
            UPDATE tasks
               SET assignee=NULL,
                   claim_run_id=NULL,
                   lease_token=NULL,
                   lease_expires_at=NULL,
                   updated_at=?
             WHERE assignee=?
            """,
            (now, key),
        )
        placeholders = ",".join("?" for _ in task_ids)
        updated_rows = conn.execute(
            _join_project(f"SELECT * FROM tasks WHERE id IN ({placeholders})"),
            task_ids,
        ).fetchall()
        conn.commit()
        return [dict(r) for r in updated_rows]
    finally:
        conn.close()


def delete_agent_outputs_for_agent(agent_key: str) -> int:
    key = str(agent_key or "").strip().lower()
    if not key:
        return 0
    conn = get_conn()
    cur = conn.execute("DELETE FROM agent_outputs WHERE agent=?", (key,))
    conn.commit()
    conn.close()
    return int(cur.rowcount or 0)


# ── Agent Types ────────────────────────────────────────────────────────────────

def list_agent_types() -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM agent_types ORDER BY is_builtin DESC, created_at ASC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_agent_type(key: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM agent_types WHERE key=?", (key,)).fetchone()
    conn.close()
    return dict(row) if row else None


def create_agent_type(key: str, name: str, description: str, prompt: str,
                      poll_statuses: list, next_status: str,
                      working_status: str, cli: str,
                      runtime_profile: str = "") -> dict:
    conn = get_conn()
    aid = str(uuid.uuid4())
    now = _now()
    conn.execute(
        """INSERT INTO agent_types
           (id,key,name,description,prompt,poll_statuses,next_status,working_status,runtime_profile,cli,is_builtin,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,0,?)""",
        (aid, key, name, description, prompt,
         json.dumps(poll_statuses), next_status, working_status, runtime_profile, cli, now),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM agent_types WHERE id=?", (aid,)).fetchone()
    conn.close()
    return dict(row)


def update_agent_type(key: str, **fields) -> dict | None:
    if "poll_statuses" in fields and isinstance(fields["poll_statuses"], list):
        fields["poll_statuses"] = json.dumps(fields["poll_statuses"])
    conn = get_conn()
    set_clause = ", ".join(f"{k}=?" for k in fields)
    values = list(fields.values()) + [key]
    conn.execute(f"UPDATE agent_types SET {set_clause} WHERE key=?", values)
    conn.commit()
    row = conn.execute("SELECT * FROM agent_types WHERE key=?", (key,)).fetchone()
    conn.close()
    return dict(row) if row else None


def delete_agent_type(key: str) -> bool:
    conn = get_conn()
    cur = conn.execute("DELETE FROM agent_types WHERE key=? AND is_builtin=0", (key,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0

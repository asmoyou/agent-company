import hashlib
import json
import re
from datetime import datetime, timedelta

TASK_SECTION_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(.+?)\s*$")
TASK_LIST_PREFIX_RE = re.compile(r"^\s*(?:[-*+]|(?:\d+\.))\s+(?:\[[ xX]\]\s*)?")
TASK_SECTION_ALIAS_MAP = {
    "goal": ("任务目标", "子任务目标", "目标", "需求目标", "objective"),
    "parent_refs": ("关联父需求编号", "父需求编号", "需求编号", "parent refs"),
    "scope": ("实施范围", "实现范围", "范围", "scope"),
    "non_scope": ("非范围", "不做范围", "范围外", "non scope"),
    "constraints": ("关键约束", "约束", "constraints"),
    "todo_steps": ("todo 步骤", "todo", "步骤", "执行步骤"),
    "deliverables": ("交付物", "deliverables"),
    "acceptance": ("验收标准", "acceptance criteria", "acceptance"),
    "assumptions": ("假设", "assumptions", "待确认"),
    "evidence_required": ("证据要求", "evidence required", "evidence"),
    "allowed_surface": ("允许交付面", "allowed surface"),
}

ISSUE_ALLOWED_STATUSES = {"open", "new", "persisting", "resolved", "wont_fix"}
ISSUE_ALLOWED_SEVERITIES = {"critical", "high", "medium", "low"}
ISSUE_ALLOWED_CATEGORIES = {
    "correctness",
    "coverage",
    "scope",
    "evidence",
    "packaging",
    "docs",
    "ux",
    "performance",
    "security",
    "other",
}
UNRESOLVED_ISSUE_STATUSES = {"open", "new", "persisting"}
RETRY_STRATEGY_DEFAULT = "default_implement"
RETRY_STRATEGY_ORDER = [
    RETRY_STRATEGY_DEFAULT,
    "repro_first",
    "test_first",
    "surface_freeze",
    "package_audit",
    "critic_pass",
    "alternate_model",
]
EVIDENCE_PATH_RE = re.compile(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)+|[A-Za-z0-9_.-]+\.(?:py|js|ts|sh|md|html|css)")
BEHAVIORAL_TASK_HINT_RE = re.compile(r"(网页|页面|浏览器|前端|游戏|交互|按钮|界面|动画|移动|点击|重开|重新开始|显示|可玩)", re.IGNORECASE)
TEST_LIKE_PATH_RE = re.compile(r"(^|/)(tests?|specs?|e2e|playwright|cypress)(/|$)|(^|/)(smoke|spec|test)[-_]", re.IGNORECASE)
DEPENDENCY_FILE_RE = re.compile(r"(^|/)(package\.json|pnpm-lock\.yaml|yarn\.lock|package-lock\.json|requirements[^/]*\.txt|pyproject\.toml|poetry\.lock|Pipfile|Pipfile\.lock)$", re.IGNORECASE)
BACKEND_ROOT_RE = re.compile(r"^(server|backend|api|db|migrations|services?)(/|$)", re.IGNORECASE)


def _clip_text(text: str, *, limit: int = 400) -> str:
    raw = str(text or "").strip()
    if len(raw) <= limit:
        return raw
    return raw[: max(0, limit - 1)].rstrip() + "…"


def _normalize_section_name(raw: str) -> str:
    text = re.sub(r"[\s:：_-]+", "", str(raw or "").strip().lower())
    for key, aliases in TASK_SECTION_ALIAS_MAP.items():
        for alias in aliases:
            alias_key = re.sub(r"[\s:：_-]+", "", str(alias).strip().lower())
            if text == alias_key:
                return key
    return ""


def parse_task_description_sections(description: str) -> dict[str, str]:
    if not str(description or "").strip():
        return {}
    sections: dict[str, str] = {}
    current_key = ""
    buf: list[str] = []
    for raw_line in str(description or "").splitlines():
        match = TASK_SECTION_HEADING_RE.match(raw_line)
        if match:
            if current_key:
                body = "\n".join(buf).strip()
                if body:
                    prev = sections.get(current_key)
                    sections[current_key] = f"{prev}\n{body}".strip() if prev else body
            current_key = _normalize_section_name(match.group(1))
            buf = []
            continue
        if current_key:
            buf.append(raw_line)
    if current_key:
        body = "\n".join(buf).strip()
        if body:
            prev = sections.get(current_key)
            sections[current_key] = f"{prev}\n{body}".strip() if prev else body
    return sections


def section_items(
    body: str,
    *,
    max_items: int = 8,
    item_limit: int = 240,
) -> list[str]:
    items: list[str] = []
    current = ""
    for raw_line in str(body or "").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("```"):
            continue
        is_item = bool(TASK_LIST_PREFIX_RE.match(stripped))
        content = TASK_LIST_PREFIX_RE.sub("", stripped).strip() if is_item else stripped
        if not content:
            continue
        if is_item:
            if current:
                items.append(_clip_text(current, limit=item_limit))
                if len(items) >= max_items:
                    return items
            current = content
            continue
        if current:
            current = f"{current} {content}".strip()
        else:
            current = content
    if current and len(items) < max_items:
        items.append(_clip_text(current, limit=item_limit))
    return items


def infer_allowed_surface(contract: dict | None) -> dict[str, list[str]]:
    files: list[str] = []
    roots: list[str] = []
    docs: list[str] = []
    cli_paths: list[str] = []
    for item in (contract or {}).get("deliverables", []) or []:
        text = str(item or "")
        candidates = [str(match or "").strip() for match in EVIDENCE_PATH_RE.findall(text)]
        if not candidates:
            stripped = re.split(r"[：:]", text, maxsplit=1)[0].strip("`'\" ")
            if stripped and re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", stripped):
                candidates = [stripped]
        for raw_path in candidates:
            path = raw_path.strip().lstrip("./")
            if not path:
                continue
            if path not in files:
                files.append(path[:500])
            root = path.split("/", 1)[0]
            if root and root not in roots:
                roots.append(root[:160])
            lowered = path.lower()
            if lowered.endswith(".md") and path not in docs:
                docs.append(path[:500])
            if (
                "/" not in path
                or root in {"bin", "scripts"}
                or lowered.endswith((".py", ".sh", ".ts", ".js"))
            ) and path not in cli_paths:
                cli_paths.append(path[:500])
    return {
        "roots": roots[:32],
        "files": files[:128],
        "docs": docs[:64],
        "cli_paths": cli_paths[:64],
    }


def extract_task_contract_from_description(
    description: str,
    *,
    existing_allowed_surface: dict | None = None,
) -> dict[str, object]:
    sections = parse_task_description_sections(description)
    if not sections:
        return {}
    goal_items = section_items(sections.get("goal", ""), max_items=3, item_limit=320)
    contract = {
        "goal": _clip_text(" ".join(goal_items), limit=320) if goal_items else "",
        "parent_refs": section_items(sections.get("parent_refs", ""), max_items=8),
        "scope": section_items(sections.get("scope", ""), max_items=8),
        "non_scope": section_items(sections.get("non_scope", ""), max_items=8),
        "constraints": section_items(sections.get("constraints", ""), max_items=8),
        "todo_steps": section_items(sections.get("todo_steps", ""), max_items=12),
        "deliverables": section_items(sections.get("deliverables", ""), max_items=12),
        "acceptance": section_items(sections.get("acceptance", ""), max_items=12),
        "assumptions": section_items(sections.get("assumptions", ""), max_items=8),
        "evidence_required": section_items(sections.get("evidence_required", ""), max_items=8),
    }
    allowed_surface = existing_allowed_surface or infer_allowed_surface(contract)
    contract["allowed_surface"] = normalize_allowed_surface(allowed_surface)
    return contract


def normalize_allowed_surface(raw) -> dict[str, list[str]]:
    data = raw
    if isinstance(raw, str):
        txt = raw.strip()
        if txt:
            try:
                data = json.loads(txt)
            except Exception:
                data = {}
        else:
            data = {}
    if not isinstance(data, dict):
        data = {}
    out: dict[str, list[str]] = {}
    for key in ("roots", "files", "docs", "cli_paths"):
        items = data.get(key)
        if isinstance(items, str):
            try:
                items = json.loads(items)
            except Exception:
                items = [items]
        if not isinstance(items, list):
            items = []
        cleaned: list[str] = []
        seen: set[str] = set()
        for item in items:
            text = str(item or "").strip().lstrip("./")
            if not text or text in seen:
                continue
            seen.add(text)
            cleaned.append(text[:500])
        out[key] = cleaned
    return out


def detect_surface_from_changed_files(changed_files: list[dict] | list[str] | None) -> dict[str, list[str]]:
    files: list[str] = []
    roots: list[str] = []
    docs: list[str] = []
    cli_paths: list[str] = []
    for item in changed_files or []:
        if isinstance(item, dict):
            path = str(item.get("path") or item.get("new_path") or "").strip()
        else:
            path = str(item or "").strip()
        path = path.lstrip("./")
        if not path:
            continue
        if path not in files:
            files.append(path[:500])
        root = path.split("/", 1)[0]
        if root and root not in roots:
            roots.append(root[:160])
        lowered = path.lower()
        if lowered.endswith(".md") and path not in docs:
            docs.append(path[:500])
        if (
            "/" not in path
            or root in {"bin", "scripts"}
            or lowered.endswith((".py", ".sh", ".ts", ".js"))
        ) and path not in cli_paths:
            cli_paths.append(path[:500])
    return normalize_allowed_surface(
        {
            "roots": roots,
            "files": files,
            "docs": docs,
            "cli_paths": cli_paths,
        }
    )


def find_surface_violations(allowed_surface, current_surface) -> list[str]:
    allowed = normalize_allowed_surface(allowed_surface)
    current = normalize_allowed_surface(current_surface)
    violations: list[str] = []
    allowed_roots = set(allowed.get("roots") or [])
    for root in current.get("roots") or []:
        if allowed_roots and root not in allowed_roots:
            violations.append(f"新增交付根目录: {root}")
    allowed_files = set(allowed.get("files") or [])
    for path in current.get("files") or []:
        if allowed_files and path not in allowed_files:
            root = path.split("/", 1)[0]
            if not allowed_roots or root not in allowed_roots:
                violations.append(f"新增未授权交付文件: {path}")
    allowed_cli = set(allowed.get("cli_paths") or [])
    for path in current.get("cli_paths") or []:
        if allowed_cli and path not in allowed_cli:
            violations.append(f"新增命令/脚本入口: {path}")
    return violations[:32]


def _changed_paths(changed_files: list[dict] | list[str] | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in changed_files or []:
        if isinstance(item, dict):
            path = str(item.get("path") or item.get("new_path") or "").strip()
        else:
            path = str(item or "").strip()
        path = path.lstrip("./")
        if not path or path in seen:
            continue
        seen.add(path)
        out.append(path[:500])
    return out


def _artifact_manifest_paths(artifact_manifest) -> list[str]:
    if not isinstance(artifact_manifest, dict):
        return []
    out: list[str] = []
    path = str(artifact_manifest.get("path") or "").strip().lstrip("./")
    if path:
        out.append(path[:500])
    for key in ("files", "artifacts", "paths"):
        items = artifact_manifest.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                raw = str(item.get("path") or "").strip()
            else:
                raw = str(item or "").strip()
            raw = raw.lstrip("./")
            if raw:
                out.append(raw[:500])
    # preserve order, dedupe
    uniq: list[str] = []
    seen: set[str] = set()
    for item in out:
        if item in seen:
            continue
        seen.add(item)
        uniq.append(item)
    return uniq[:64]


def looks_like_behavioral_evidence_path(path: str) -> bool:
    lowered = str(path or "").strip().lower()
    if not lowered:
        return False
    if TEST_LIKE_PATH_RE.search(lowered):
        return True
    return lowered.endswith((".spec.js", ".spec.ts", ".test.js", ".test.ts"))


def acceptance_requires_test_evidence(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    explicit_test_tokens = (
        "pytest",
        "unit",
        "integration",
        "e2e",
        "playwright",
        "cypress",
        "smoke-test",
        "smoke test",
        "测试脚本",
        "自动化",
        "断言",
        "assert",
        "`node ",
    )
    if any(token in lowered for token in explicit_test_tokens):
        return True
    if "测试" in lowered or "test" in lowered:
        observational_tokens = ("观察", "可见", "视觉", "玩家", "手动", "冒烟", "显示", "提示")
        if any(token in lowered for token in observational_tokens):
            return False
        return True
    return False


def _looks_like_behavioral_contract(contract: dict | None) -> bool:
    parts: list[str] = []
    for key in ("goal",):
        parts.append(str((contract or {}).get(key) or ""))
    for key in ("scope", "deliverables", "acceptance", "constraints"):
        parts.extend([str(item or "") for item in ((contract or {}).get(key) or [])])
    text = " ".join(parts)
    return bool(BEHAVIORAL_TASK_HINT_RE.search(text))


def _extract_evidence_candidate_paths(text: str) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for raw in EVIDENCE_PATH_RE.findall(str(text or "")):
        path = str(raw or "").strip().lstrip("./")
        if not path or path in seen:
            continue
        seen.add(path)
        found.append(path[:500])
    return found[:16]


def evaluate_contract_evidence(
    contract: dict | None,
    *,
    changed_files: list[dict] | list[str] | None,
    current_surface: dict | None = None,
    allowed_surface: dict | None = None,
    artifact_manifest: dict | None = None,
) -> dict[str, object]:
    contract = contract or {}
    changed_paths = _changed_paths(changed_files)
    changed_paths_lower = [path.lower() for path in changed_paths]
    artifact_paths = _artifact_manifest_paths(artifact_manifest)
    artifact_paths_lower = [path.lower() for path in artifact_paths]
    evidence_checks: list[dict] = []
    missing_evidence_required: list[dict] = []
    assumption_conflicts: list[str] = []
    issues: list[dict] = []

    current = normalize_allowed_surface(current_surface or detect_surface_from_changed_files(changed_files))
    allowed = normalize_allowed_surface(allowed_surface or {})
    surface_violations = find_surface_violations(allowed, current)

    for item in contract.get("evidence_required", []) or []:
        text = str(item or "").strip()
        if not text:
            continue
        lowered = text.lower()
        matched_paths: list[str] = []
        for candidate in _extract_evidence_candidate_paths(text):
            candidate_lower = candidate.lower()
            if any(
                candidate_lower == path
                or path.endswith(candidate_lower)
                or candidate_lower.endswith(path)
                for path in changed_paths_lower + artifact_paths_lower
            ):
                matched_paths.append(candidate)
        if not matched_paths:
            if any(token in lowered for token in ("pytest", "unit", "integration", "e2e", "playwright", "cypress", "smoke", "test")):
                matched_paths = [path for path in changed_paths if looks_like_behavioral_evidence_path(path)]
            elif any(token in lowered for token in ("readme", "文档", "说明", ".md")):
                matched_paths = [path for path in changed_paths if path.lower().endswith(".md")]
            elif any(
                token in lowered
                for token in ("测试", "断言", "桩", "stub", "mock", "seed", "固定序列", "随机")
            ):
                matched_paths = [path for path in changed_paths if looks_like_behavioral_evidence_path(path)]
        status = "provided" if matched_paths else "missing"
        check = {
            "item": text,
            "status": status,
            "matched_paths": matched_paths[:8],
        }
        evidence_checks.append(check)
        if status == "missing":
            missing_evidence_required.append(check)
            issues.append(
                {
                    "issue_id": f"evidence-required-{len(missing_evidence_required)}",
                    "acceptance_item": "证据要求",
                    "severity": "high",
                    "category": "evidence",
                    "summary": f"预检未发现与证据要求对应的验证资产：{text}",
                    "reproducer": "检查 patchset changed_files / artifact_manifest 是否包含对应测试、冒烟脚本或验证文件",
                    "evidence_gap": text,
                    "scope": ", ".join(_extract_evidence_candidate_paths(text)[:4]),
                    "fix_hint": "补齐对应的测试、冒烟脚本或文档证据，并确保其进入 patchset",
                    "status": "new",
                }
            )

    if not evidence_checks and _looks_like_behavioral_contract(contract):
        behavior_paths = [path for path in changed_paths if looks_like_behavioral_evidence_path(path)]
        if not behavior_paths:
            summary = "交互/行为型任务缺少本地验证脚本或测试证据"
            missing_evidence_required.append(
                {
                    "item": "至少一个覆盖关键交互路径的本地验证命令、测试或冒烟脚本",
                    "status": "missing",
                    "matched_paths": [],
                }
            )
            issues.append(
                {
                    "issue_id": "behavioral-evidence-missing",
                    "acceptance_item": "证据要求",
                    "severity": "high",
                    "category": "evidence",
                    "summary": summary,
                    "reproducer": "交互/前端/网页任务的 patchset 中未发现 smoke/test/spec/e2e/playwright/cypress 类验证文件",
                    "evidence_gap": "缺少关键交互路径的本地验证证据",
                    "scope": "tests",
                    "fix_hint": "补充至少一个覆盖开始、关键交互和失败恢复路径的本地验证脚本或测试",
                    "status": "new",
                }
            )

    backend_paths = [path for path in changed_paths if BACKEND_ROOT_RE.search(path)]
    dependency_paths = [path for path in changed_paths if DEPENDENCY_FILE_RE.search(path)]
    for item in contract.get("assumptions", []) or []:
        text = str(item or "").strip()
        lowered = text.lower()
        if not text:
            continue
        if surface_violations and any(token in lowered for token in ("不新增", "不引入", "不增加", "保持现有", "沿用现有")):
            assumption_conflicts.append(f"假设与当前交付面冲突：{text}")
        elif backend_paths and any(token in text for token in ("单页前端", "浏览器可直接运行", "纯前端", "网页")):
            assumption_conflicts.append(f"假设与后端改动冲突：{text}")
        elif dependency_paths and any(token in text for token in ("不新增依赖", "不引入外部依赖", "不增加依赖")):
            assumption_conflicts.append(f"假设与依赖变更冲突：{text}")
    for conflict in assumption_conflicts[:16]:
        issues.append(
            {
                "issue_id": f"assumption-conflict-{hashlib.sha1(conflict.encode('utf-8')).hexdigest()[:10]}",
                "acceptance_item": "假设",
                "severity": "high",
                "category": "scope",
                "summary": conflict,
                "reproducer": "对比 assumptions 与当前交付面/变更文件",
                "evidence_gap": conflict,
                "scope": conflict,
                "fix_hint": "收敛交付面或调整需求合同中的 assumptions 后再送审",
                "status": "new",
            }
        )

    return {
        "evidence_checks": evidence_checks,
        "missing_evidence_required": missing_evidence_required,
        "assumption_conflicts": assumption_conflicts[:16],
        "issues": issues[:32],
    }


def evidence_bundle_has_blockers(bundle: dict | None) -> bool:
    data = bundle or {}
    hard_blockers = data.get("hard_blockers")
    if isinstance(hard_blockers, list):
        return bool(hard_blockers)
    return bool(
        (data.get("assumption_conflicts") or [])
        or (data.get("surface_violations") or [])
    )


def summarize_evidence_blockers(bundle: dict | None, *, limit: int = 4) -> list[str]:
    data = bundle or {}
    lines: list[str] = []
    hard_blockers = data.get("hard_blockers")
    if isinstance(hard_blockers, list):
        for item in hard_blockers[: max(1, limit)]:
            if not isinstance(item, dict):
                continue
            summary = str(item.get("summary") or "").strip()
            if summary:
                lines.append(summary)
        if lines:
            return lines[: max(1, limit)]
    for item in (data.get("missing_acceptance_checks") or [])[:limit]:
        label = str(item.get("item") or "").strip()
        if label:
            lines.append(f"缺少验收证据：{label}")
    for item in (data.get("missing_evidence_required") or [])[:limit]:
        label = str(item.get("item") or "").strip()
        if label:
            lines.append(f"缺少要求证据：{label}")
    for item in (data.get("assumption_conflicts") or [])[:limit]:
        text = str(item or "").strip()
        if text:
            lines.append(text)
    for item in (data.get("surface_violations") or [])[:limit]:
        text = str(item or "").strip()
        if text:
            lines.append(text)
    return lines[: max(1, limit)]


def normalize_issue_payload(issue: dict, *, default_status: str = "open") -> dict | None:
    if not isinstance(issue, dict):
        return None
    summary = _clip_text(issue.get("summary") or issue.get("title") or issue.get("feedback") or "", limit=400)
    acceptance_item = _clip_text(issue.get("acceptance_item") or "", limit=240)
    if not summary and not acceptance_item:
        return None
    severity = str(issue.get("severity") or "medium").strip().lower()
    if severity not in ISSUE_ALLOWED_SEVERITIES:
        severity = "medium"
    category = str(issue.get("category") or "other").strip().lower()
    if category not in ISSUE_ALLOWED_CATEGORIES:
        category = "other"
    status = str(issue.get("status") or default_status).strip().lower()
    if status not in ISSUE_ALLOWED_STATUSES:
        status = default_status
    issue_id = str(issue.get("issue_id") or issue.get("id") or "").strip()
    if not issue_id:
        digest_source = "|".join(
            [
                acceptance_item,
                summary,
                _clip_text(issue.get("scope") or "", limit=160),
                category,
            ]
        )
        issue_id = f"ISS-{hashlib.sha1(digest_source.encode('utf-8')).hexdigest()[:10]}"
    return {
        "issue_id": issue_id[:40],
        "acceptance_item": acceptance_item,
        "severity": severity,
        "category": category,
        "summary": summary,
        "reproducer": _clip_text(issue.get("reproducer") or "", limit=500),
        "evidence_gap": _clip_text(issue.get("evidence_gap") or "", limit=500),
        "scope": _clip_text(issue.get("scope") or "", limit=320),
        "fix_hint": _clip_text(issue.get("fix_hint") or "", limit=500),
        "status": status,
    }


def normalize_issue_list(raw_issues, *, default_status: str = "open") -> list[dict]:
    data = raw_issues
    if isinstance(raw_issues, str):
        txt = raw_issues.strip()
        if txt:
            try:
                data = json.loads(txt)
            except Exception:
                data = []
        else:
            data = []
    if not isinstance(data, list):
        return []
    out: list[dict] = []
    seen: set[str] = set()
    for item in data:
        normalized = normalize_issue_payload(item, default_status=default_status)
        if not normalized:
            continue
        key = normalized["issue_id"]
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
        if len(out) >= 64:
            break
    return out


def summarize_issue_list(issues: list[dict], *, limit: int = 6) -> str:
    lines: list[str] = []
    for item in issues[: max(1, limit)]:
        status = str(item.get("status") or "open")
        severity = str(item.get("severity") or "medium")
        summary = str(item.get("summary") or "").strip()
        acceptance_item = str(item.get("acceptance_item") or "").strip()
        head = f"[{status}/{severity}] {summary}"
        if acceptance_item:
            head += f"（验收项: {acceptance_item}）"
        lines.append(f"- {head}")
    return "\n".join(lines)


def build_feedback_from_issues(issues: list[dict], fallback_feedback: str) -> str:
    lines: list[str] = []
    plain = str(fallback_feedback or "").strip()
    if plain:
        lines.append(plain)
    if issues:
        if lines:
            lines.append("")
        lines.append("结构化问题账本：")
        lines.append(summarize_issue_list(issues, limit=8))
    return "\n".join(line for line in lines if line is not None).strip()[:4000]


def summarize_output_for_fingerprint(text: str) -> str:
    raw = re.sub(r"\s+", " ", str(text or "").strip())
    if not raw:
        return ""
    raw = re.sub(r"[0-9a-f]{12,40}", "<sha>", raw, flags=re.IGNORECASE)
    raw = re.sub(r"/[^ ]+", "<path>", raw)
    return _clip_text(raw, limit=280)


def compute_failure_fingerprint(
    *,
    stage: str,
    summary: str,
    output: str = "",
    extra: str = "",
) -> str:
    basis = "|".join(
        [
            str(stage or "").strip().lower(),
            summarize_output_for_fingerprint(summary),
            summarize_output_for_fingerprint(output),
            summarize_output_for_fingerprint(extra),
        ]
    )
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]


def next_retry_strategy(
    *,
    current_strategy: str,
    failure_stage: str,
    same_fingerprint_streak: int,
    open_issue_count: int,
    has_surface_violation: bool = False,
    has_evidence_gap: bool = False,
) -> str:
    if has_surface_violation:
        return "surface_freeze"
    if has_evidence_gap:
        return "test_first" if open_issue_count > 0 else "repro_first"
    if same_fingerprint_streak <= 1:
        return current_strategy or RETRY_STRATEGY_DEFAULT
    if current_strategy == RETRY_STRATEGY_DEFAULT:
        return "repro_first"
    if current_strategy == "repro_first":
        return "test_first"
    if current_strategy == "test_first":
        return "package_audit" if "review" in str(failure_stage or "") else "critic_pass"
    if current_strategy == "package_audit":
        return "critic_pass"
    if current_strategy == "critic_pass":
        return "alternate_model"
    return current_strategy or RETRY_STRATEGY_DEFAULT


def cooldown_until_for_streak(same_fingerprint_streak: int) -> str:
    streak = max(0, int(same_fingerprint_streak or 0))
    if streak <= 1:
        return ""
    cooldown_seconds = min(300, 15 * streak)
    return (datetime.utcnow() + timedelta(seconds=cooldown_seconds)).isoformat()


def count_open_issues(issues: list[dict]) -> int:
    return sum(1 for item in issues if str(item.get("status") or "").strip().lower() in UNRESOLVED_ISSUE_STATUSES)

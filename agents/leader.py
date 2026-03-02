import asyncio
import json
import os
import re
from pathlib import Path

from base import BaseAgent, get_project_dirs

TRIAGE_PROMPT_DEFAULT = (
    "你是一个专业的项目评估与分解专家。请评估以下任务是否需要分解：\n\n"
    "## 任务标题\n{task_title}\n\n"
    "## 任务描述\n{task_description}\n\n"
    "## 可用 Agent 类型\n{agent_list}\n\n"
    "## 评估标准\n"
    "- **简单任务**：可以由单个 agent 独立完成，工作量在 1-2 小时内\n"
    "- **复杂任务**：涉及多个独立功能模块，或需要不同专业技能协作\n\n"
    "## 输出格式（严格 JSON，不要任何其他文字）\n\n"
    "如果是简单任务：\n"
    '{"action": "simple", "reason": "一句话说明为何不需要分解"}\n\n'
    "如果是复杂任务：\n"
    '{"action": "decompose", "subtasks": [\n'
    '  {"title": "子任务标题", "description": "详细描述和验收标准", "agent": "developer"}\n'
    "]}"
)

FORCE_DECOMPOSE_PROMPT = (
    "你是一个专业的项目分解专家。请将以下任务分解为 2-5 个可执行的子任务：\n\n"
    "## 任务标题\n{task_title}\n\n"
    "## 任务描述\n{task_description}\n\n"
    "## 可用 Agent 类型\n{agent_list}\n\n"
    "只输出 JSON 数组，不要任何其他文字：\n"
    "[\n"
    '  {"title": "子任务标题", "description": "详细描述和验收标准", "agent": "developer"}\n'
    "]"
)


class LeaderAgent(BaseAgent):
    name = "leader"
    poll_statuses = ["triage", "decompose"]
    cli_name = os.getenv("DEVELOPER_CLI", "claude")
    working_status = "triaging"

    def respect_assignment_for(self, status: str) -> bool:
        return False

    async def _get_prompt_template(self) -> str:
        try:
            r = await self.http.get("/agent-types/leader")
            r.raise_for_status()
            p = r.json().get("prompt", "")
            return p if p else TRIAGE_PROMPT_DEFAULT
        except Exception:
            return TRIAGE_PROMPT_DEFAULT

    async def _get_agent_list(self) -> str:
        try:
            r = await self.http.get("/agent-types")
            r.raise_for_status()
            types = r.json()
            lines = [
                f"- {t['key']}: {t['name']} — {t.get('description', '')}"
                for t in types if t["key"] not in ("leader", "manager")
            ]
            return "\n".join(lines) if lines else "- developer: 开发者"
        except Exception:
            return "- developer: 开发者"

    def _parse_triage(self, output: str) -> dict | None:
        """Parse {"action": "simple"|"decompose", ...} from CLI output."""
        for m in re.findall(r"```(?:json)?\s*(\{[\s\S]+?\})\s*```", output):
            try:
                d = json.loads(m)
                if "action" in d:
                    return d
            except json.JSONDecodeError:
                pass
        for m in re.findall(r"\{[^{}]*\"action\"[^{}]*\}", output, re.DOTALL):
            try:
                d = json.loads(m)
                if "action" in d:
                    return d
            except json.JSONDecodeError:
                pass
        # Bigger nested object with subtasks
        try:
            start = output.rfind('{"action"')
            if start == -1:
                start = output.rfind("{'action'")
            if start != -1:
                chunk = output[start:]
                # find balanced brace
                depth, end = 0, -1
                for i, c in enumerate(chunk):
                    if c == '{':
                        depth += 1
                    elif c == '}':
                        depth -= 1
                        if depth == 0:
                            end = i + 1
                            break
                if end > 0:
                    d = json.loads(chunk[:end])
                    if "action" in d:
                        return d
        except Exception:
            pass
        return None

    def _parse_subtasks_array(self, output: str) -> list[dict]:
        for m in re.findall(r"```(?:json)?\s*(\[[\s\S]+?\])\s*```", output):
            try:
                d = json.loads(m)
                if isinstance(d, list) and d:
                    return d
            except json.JSONDecodeError:
                pass
        for m in re.findall(r"\[\s*\{[\s\S]*?\}\s*\]", output):
            try:
                d = json.loads(m)
                if isinstance(d, list) and d:
                    return d
            except json.JSONDecodeError:
                pass
        return []

    async def _create_subtasks(self, task: dict, subtasks: list[dict]) -> int:
        created = 0
        for i, st in enumerate(subtasks, 1):
            title = str(st.get("title") or f"子任务 {i}")[:200]
            desc  = str(st.get("description") or "")
            agent = str(st.get("agent") or "developer")
            try:
                r = await self.http.post("/tasks", json={
                    "title": title,
                    "description": desc,
                    "project_id": task.get("project_id"),
                    "parent_task_id": task["id"],
                    "assigned_agent": agent,
                    "status": "todo",   # subtasks skip triage
                })
                r.raise_for_status()
                created += 1
                await self.add_log(task["id"], f"  ✓ [{i}] {title} (→ {agent})")
                self._post_output_bg(f"  子任务 {i}: {title}")
            except Exception as e:
                await self.add_log(task["id"], f"  ✗ [{i}] 创建失败: {e}")
        return created

    async def process_task(self, task: dict):
        original_status = task.get("_claimed_from_status", task.get("status"))  # "triage" or "decompose"
        task_id = task["id"]

        if original_status == "decompose":
            # User explicitly requested decomposition — skip evaluation
            await self.add_log(task_id, "Leader 执行强制分解（用户指定）")
            await self._force_decompose(task)
        else:
            # Auto triage — evaluate complexity first
            await self.add_log(task_id, "Leader 评估任务复杂度...")
            await self._auto_triage(task)

    async def _auto_triage(self, task: dict):
        task_id = task["id"]

        # Idempotency: if subtasks already exist (crash recovery), just mark decomposed
        r = await self.http.get(f"/tasks/{task_id}/subtasks")
        if r.status_code == 200 and r.json():
            await self.add_log(task_id, "已有子任务，标记为 decomposed（崩溃恢复）")
            await self.update_task(task_id, status="decomposed", assignee=None)
            return

        agent_list  = await self._get_agent_list()
        prompt_tpl  = await self._get_prompt_template()
        try:
            prompt = prompt_tpl.format(
                task_title=task["title"],
                task_description=task["description"] or "(无额外描述)",
                agent_list=agent_list,
            )
        except KeyError:
            prompt = prompt_tpl

        proj_root, _ = get_project_dirs(task)
        run_dir = proj_root if proj_root.exists() else Path.cwd()

        _, output = await self.run_cli(prompt, cwd=run_dir, task_id=task_id)
        await self.add_log(task_id, f"评估输出:\n{output[:600]}")

        decision = self._parse_triage(output)

        if decision and decision.get("action") == "decompose":
            subtasks = decision.get("subtasks") or []
            if subtasks:
                await self.add_log(task_id, f"判断为复杂任务，分解为 {len(subtasks)} 个子任务")
                n = await self._create_subtasks(task, subtasks)
                await self.update_task(task_id, status="decomposed", assignee=None)
                await self.add_log(task_id, f"✅ 分解完成，共创建 {n} 个子任务")
                self._post_output_bg(f"✓ 已分解为 {n} 个子任务")
                return

        # Simple task (or parse failed) — push to todo
        reason = decision.get("reason", "判定为简单任务") if decision else "无法解析评估结果，按简单任务处理"
        await self.add_log(task_id, f"判断为简单任务：{reason}")
        await self.update_task(task_id, status="todo", assignee=None)
        self._post_output_bg(f"✓ 简单任务，推进至 todo")

    async def _force_decompose(self, task: dict):
        task_id = task["id"]
        agent_list = await self._get_agent_list()
        try:
            prompt = FORCE_DECOMPOSE_PROMPT.format(
                task_title=task["title"],
                task_description=task["description"] or "(无额外描述)",
                agent_list=agent_list,
            )
        except KeyError:
            prompt = FORCE_DECOMPOSE_PROMPT

        proj_root, _ = get_project_dirs(task)
        run_dir = proj_root if proj_root.exists() else Path.cwd()

        _, output = await self.run_cli(prompt, cwd=run_dir, task_id=task_id)
        await self.add_log(task_id, f"分解输出:\n{output[:600]}")

        subtasks = self._parse_subtasks_array(output)
        if not subtasks:
            await self.add_log(task_id, "⚠ 无法解析子任务，回退为 todo")
            await self.update_task(task_id, status="todo", assignee=None)
            return

        n = await self._create_subtasks(task, subtasks)
        await self.update_task(task_id, status="decomposed", assignee=None)
        await self.add_log(task_id, f"✅ 强制分解完成，共创建 {n} 个子任务")
        self._post_output_bg(f"✓ 强制分解完成: {n} 个子任务")


if __name__ == "__main__":
    asyncio.run(LeaderAgent().run())

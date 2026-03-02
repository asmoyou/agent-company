import asyncio
import json
import os
import re
from pathlib import Path

from base import BaseAgent, get_project_dirs


class LeaderAgent(BaseAgent):
    """
    Decomposes a task (in 'decompose' status) into subtasks by calling the CLI
    with a decomposition prompt, then creates the subtasks via the API.
    """
    name = "leader"
    poll_statuses = ["decompose"]
    cli_name = os.getenv("DEVELOPER_CLI", "claude")

    async def _get_config(self) -> dict:
        try:
            r = await self.http.get("/agent-types/leader")
            r.raise_for_status()
            return r.json()
        except Exception:
            return {}

    async def _get_agent_list(self) -> str:
        try:
            r = await self.http.get("/agent-types")
            r.raise_for_status()
            types = r.json()
            lines = [
                f"- {t['key']}: {t['name']} — {t.get('description','')}"
                for t in types if t["key"] not in ("leader", "manager")
            ]
            return "\n".join(lines) if lines else "- developer: 开发者"
        except Exception:
            return "- developer: 开发者"

    def _parse_subtasks(self, output: str) -> list[dict]:
        # Code block first
        for m in re.findall(r"```(?:json)?\s*(\[[\s\S]+?\])\s*```", output):
            try:
                data = json.loads(m)
                if isinstance(data, list) and data:
                    return data
            except json.JSONDecodeError:
                pass
        # Bare JSON array
        for m in re.findall(r"\[\s*\{[\s\S]*?\}\s*\]", output):
            try:
                data = json.loads(m)
                if isinstance(data, list) and data:
                    return data
            except json.JSONDecodeError:
                pass
        return []

    async def process_task(self, task: dict):
        task_id = task["id"]
        await self.update_task(task_id, status="decomposing", assignee=self.name)
        await self.add_log(task_id, "Leader 开始分解任务")

        config = await self._get_config()
        prompt_template = config.get("prompt") or (
            "你是一个专业的项目分解专家。请将以下任务分解为可执行的子任务。\n\n"
            "## 任务标题\n{task_title}\n\n"
            "## 任务描述\n{task_description}\n\n"
            "## 可用 Agent 类型\n{agent_list}\n\n"
            "## 分解要求\n"
            "1. 分解为 2-5 个具体、独立、可验收的子任务\n"
            "2. 每个子任务有明确的完成标准\n"
            "3. 为每个子任务指定最合适的 agent（使用列表中的 key）\n"
            "4. 子任务按合理的执行顺序排列\n\n"
            "## 输出格式\n"
            "只输出 JSON 数组，不要任何其他文字：\n"
            '[\n  {"title": "标题", "description": "详细描述", "agent": "developer"}\n]'
        )

        agent_list = await self._get_agent_list()
        try:
            prompt = prompt_template.format(
                task_title=task["title"],
                task_description=task["description"] or "(无额外描述)",
                agent_list=agent_list,
            )
        except KeyError:
            prompt = prompt_template

        # Run CLI in project root (no file writing)
        proj_root, _ = get_project_dirs(task)
        run_dir = proj_root if proj_root.exists() else Path.cwd()

        _, output = await self.run_cli(prompt, cwd=run_dir, task_id=task_id)
        await self.add_log(task_id, f"CLI 输出:\n{output[:800]}")

        subtasks = self._parse_subtasks(output)
        if not subtasks:
            await self.add_log(task_id, "⚠ 未能解析出子任务 JSON，回退为 todo")
            await self.update_task(task_id, status="todo")
            return

        await self.add_log(task_id, f"解析出 {len(subtasks)} 个子任务，开始创建...")
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
                    "parent_task_id": task_id,
                    "assigned_agent": agent,
                })
                r.raise_for_status()
                created += 1
                await self.add_log(task_id, f"  ✓ [{i}] {title} (agent: {agent})")
                self._post_output_bg(f"  子任务 {i}: {title}")
            except Exception as e:
                await self.add_log(task_id, f"  ✗ [{i}] 创建失败: {e}")

        await self.update_task(task_id, status="decomposed")
        await self.add_log(task_id, f"✅ 分解完成，共创建 {created}/{len(subtasks)} 个子任务")
        self._post_output_bg(f"✓ 分解完成: {created} 个子任务")


if __name__ == "__main__":
    asyncio.run(LeaderAgent().run())

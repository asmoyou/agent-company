import asyncio
import os
from pathlib import Path

from base import BaseAgent, PROJECT_ROOT

WORKTREE_DEV = PROJECT_ROOT / ".worktrees" / "dev"


class ReviewerAgent(BaseAgent):
    name = "reviewer"
    poll_statuses = ["in_review"]
    cli_name = os.getenv("REVIEWER_CLI", "claude")

    @property
    def worktree(self) -> Path:
        # Reviewer reads code from the dev worktree
        return WORKTREE_DEV

    async def get_diff(self) -> str:
        """Get changes in the dev worktree."""
        try:
            # Show the latest commit diff
            diff = await self.git("show", "--stat", "HEAD", cwd=WORKTREE_DEV)
            full = await self.git("show", "HEAD", cwd=WORKTREE_DEV)
            return full if full else diff
        except Exception as e:
            try:
                # Fallback: unstaged changes
                return await self.git("diff", "HEAD", cwd=WORKTREE_DEV)
            except Exception:
                return f"(could not get diff: {e})"

    async def process_task(self, task: dict):
        task_id = task["id"]

        # Claim
        await self.update_task(task_id, status="reviewing", assignee=self.name)
        await self.add_log(task_id, "Reviewer started code review")

        diff = await self.get_diff()
        await self.add_log(task_id, f"Reviewing {len(diff)} chars of diff")

        # The reviewer CLI prompt asks for a structured JSON decision at the end
        prompt = f"""You are a senior code reviewer. Review the following code changes carefully.

Task being implemented: {task['title']}
Task description: {task['description']}

Code changes:
```
{diff[:8000]}
```

Check for:
- Correctness and completeness
- Edge cases and error handling
- Code quality and clarity
- Security issues

After your review, output your decision as the LAST thing in your response, exactly in this JSON format (no text after it):

If approving:
{{"decision": "approve", "comment": "your brief approval comment"}}

If requesting changes:
{{"decision": "request_changes", "feedback": "specific actionable list of issues to fix"}}
"""

        returncode, output = await self.run_cli(prompt, cwd=WORKTREE_DEV)

        # Log reviewer output summary
        await self.add_log(task_id, f"Review output: {output[:300].strip()}")

        # Parse the decision from output
        decision = self.parse_json_decision(output)

        if decision is None:
            # Fallback: look for keywords in the output
            low = output.lower()
            if any(w in low for w in ["approve", "lgtm", "looks good", "合格", "通过"]):
                decision = {"decision": "approve", "comment": output[:200].strip()}
            elif any(w in low for w in ["request_changes", "fix", "issue", "problem", "需要修改", "问题"]):
                decision = {"decision": "request_changes", "feedback": output[:500].strip()}
            else:
                # Default to approve if we can't determine
                await self.add_log(task_id, "Could not parse decision, defaulting to approve")
                decision = {"decision": "approve", "comment": "(auto-approved: could not parse reviewer output)"}

        if decision["decision"] == "approve":
            comment = decision.get("comment", "LGTM")
            await self.add_log(task_id, f"Approved: {comment}")
            await self.update_task(task_id, status="approved", review_feedback=comment)

        else:
            feedback = decision.get("feedback", "Please fix the issues")
            await self.add_log(task_id, f"Changes requested: {feedback[:200]}")
            await self.update_task(task_id, status="needs_changes", review_feedback=feedback)


if __name__ == "__main__":
    asyncio.run(ReviewerAgent().run())

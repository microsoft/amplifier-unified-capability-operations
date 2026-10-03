"""Offline test provider; execution still uses actual Core/Foundation/loop-live."""
import asyncio
from amplifier_core.models import ProviderInfo
from amplifier_core.message_models import ChatResponse, TextBlock, Usage


class Provider:
    name = "operations-fixture"

    def get_info(self):
        return ProviderInfo(id=self.name, display_name="Offline operations fixture",
                            defaults={"model": "fixture-model", "max_tokens": 4096},
                            capabilities=["tools"])

    def parse_tool_calls(self, response):
        return response.tool_calls or []

    async def list_models(self):
        return [{"id": "fixture-model"}]

    async def close(self):
        pass

    async def complete(self, request, **kwargs):
        prompt = next((row.content for row in reversed(request.messages) if row.role == "user"), "")
        if isinstance(prompt, list):
            prompt = " ".join(getattr(block, "text", "") for block in prompt)
        if "slow qualification" in str(prompt):
            await asyncio.sleep(60)
        return ChatResponse(content=[TextBlock(text="Offline operations reply: " + str(prompt))],
                            finish_reason="stop", usage=Usage(input_tokens=8, output_tokens=5, total_tokens=13))


async def mount(coordinator, config=None):
    await coordinator.mount("providers", Provider(), name="operations-fixture")

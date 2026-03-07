"""LLM interface using Responses API."""

from __future__ import annotations

import base64
import logging
from typing import Literal, TypedDict

from openai import AsyncOpenAI

from objectdb_business import blob_storage
from objectdb_business.agent import Agent

logger = logging.getLogger(__name__)


class ChatMessage(TypedDict):
    """Model for chat messages."""

    role: Literal["system", "user", "assistant"]
    content: str


class AIError(Exception):
    """Raised when LLM does not act as expected."""


class Chat:
    """LLM chat interface based on the new Responses API."""

    def __init__(self, agent: Agent, openai_api_key: str) -> None:
        self._client = AsyncOpenAI(api_key=openai_api_key)
        self.conversation: list[ChatMessage] = []
        self.agent = agent

    async def ask(self, question: str) -> str:
        """Ask a question in a persistent chat."""
        question_item: ChatMessage = {"role": "user", "content": question}
        self.conversation.append(question_item)
        response = await self._client.responses.create(
            model="gpt-5-mini",
            input=self.conversation,  # type: ignore
            instructions=str(self.agent),
            reasoning={"effort": "minimal"},
        )
        self.conversation.append(response.output)
        logger.info("AI conversation: %s", self.conversation)
        return response.output_text

    async def edit_image(
        self, image_handler: blob_storage.ImageHandler, input_image_data: bytes, prompt: str | None = None
    ) -> str:
        """Generate image based on current converation, existing image and optional prompt."""
        context = [*self.conversation, {"role": "user", "content": prompt}] if prompt else []
        context += [  # type: ignore
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Generate image considering reference picture, "
                        "all context from developer instructions and ongoing conversation.",
                    },
                    {
                        "type": "input_image",
                        "image_url": f"data:image/jpeg;base64,{base64.b64encode(input_image_data).decode('utf-8')}",
                    },
                ],
            }
        ]
        response = await self._client.responses.create(
            model="gpt-5-mini",
            instructions=str(self.agent),
            input=context,  # type: ignore
            tools=[{"type": "image_generation", "output_format": "jpeg"}],
            tool_choice={"type": "image_generation"},
        )
        image_generation_calls = [output for output in response.output if output.type == "image_generation_call"]
        output_image_data = [output.result for output in image_generation_calls]

        if output_image_data and output_image_data[0]:
            output_image_base64 = output_image_data[0]
            output_image = blob_storage.Image.from_bytes(base64.b64decode(output_image_base64))
            image_handler.upload(output_image)
            return output_image.identifier

        raise AIError(f"No image created: {response.output}.")

    async def edit_video(
        self,
        video_handler: blob_storage.VideoHandler,
        input_image: blob_storage.Image | None = None,
        prompt: str | None = None,
        seconds: int = 4,
        size: str = "720x1280",
    ) -> str:
        """Generate video based on current converation, existing image and optional prompt."""
        convo_text = "\n".join(str(m) for m in self.conversation) if self.conversation else ""
        prompt_parts = [str(self.agent)]
        if convo_text:
            prompt_parts.append("Conversation:")
            prompt_parts.append(convo_text)
        if prompt:
            prompt_parts.append("User prompt:")
            prompt_parts.append(prompt)
        prompt_text = "\n\n".join(prompt_parts)

        input_ref = (
            ("input.jpeg", input_image.resize(*map(int, size.split("x"))).data, "image/jpeg") if input_image else None
        )

        logger.info("Generating video with prompt: %s", prompt_text)
        video = await self._client.videos.create_and_poll(
            prompt=prompt_text, input_reference=input_ref, model="sora-2", seconds=seconds, size=size
        )

        if getattr(video, "status", None) != "completed":
            raise AIError(f"Video generation failed or did not complete: {video}")

        content_resp = await self._client.videos.download_content(video.id)
        video_bytes = getattr(content_resp, "content", None)
        if video_bytes is None:
            # fallback to read() for APIResponse-like objects
            if hasattr(content_resp, "read"):
                video_bytes = content_resp.read()
            else:
                raise AIError("Could not read video content from response")

        stored_video = blob_storage.Video.from_bytes(video_bytes)
        video_handler.upload(stored_video)
        return stored_video.identifier

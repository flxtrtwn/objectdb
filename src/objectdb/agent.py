"""Agent profile."""

from __future__ import annotations

from pathlib import Path

import pydantic
import yaml
from objectdb.models import Customer, Product


class Agent(pydantic.BaseModel):
    """Agent with optional customer customization."""

    role: str
    personality: list[str]
    content: list[str]
    customer_profile: Customer | None = None
    product_profile: Product | None = None
    feedback: list[str] | None = None

    def __str__(self) -> str:
        """Agent personality and rules."""
        agent_base = (
            f"{self.role} "
            "You have the following personality traits: "
            + ", ".join(self.personality)
            + ". "
            + "You must follow these rules: \n"
            + "- "
            + "\n - ".join(self.content)
            + ". "
        )
        agent_representation = agent_base
        if self.customer_profile:
            agent_representation += str(self.customer_profile)
        if self.product_profile:
            agent_representation += str(self.product_profile)
        if self.feedback:
            considered_feedback = (
                "You already solved this task before. "
                "For the previous result, the customer provided the following feedback: \n"
                "- " + "\n - ".join(self.feedback) + ". "
            )
            agent_representation += considered_feedback
        return agent_representation

    @classmethod
    def from_yaml(cls, path: Path) -> Agent:
        """Instantiate Agent from yaml."""
        return cls.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))  # type: ignore

    def assign(self, customer_profile: Customer) -> None:
        """Assign customer-specific behavior."""
        self.customer_profile = customer_profile

    def select(self, product_profile: Product) -> None:
        """Select product to focus on."""
        self.product_profile = product_profile

    def consider(self, feedback: list[str]) -> None:
        """Consider customer feedback for request."""
        self.feedback = feedback

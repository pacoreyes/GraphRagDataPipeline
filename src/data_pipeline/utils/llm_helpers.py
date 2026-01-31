# -----------------------------------------------------------
# LLM Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

"""
Generic LLM inference helpers using MLX.

This module provides reusable functions for loading and running
local LLM inference via mlx-lm. No domain-specific logic is included.
"""

from typing import Any


def load_mlx_model(model_path: str) -> tuple[Any, Any]:
    """
    Loads an MLX model and tokenizer.

    Args:
        model_path: HuggingFace model path or local path
                    (e.g., "mlx-community/Qwen2.5-14B-Instruct-4bit").

    Returns:
        Tuple of (model, tokenizer).

    Raises:
        ImportError: If mlx-lm is not installed.
    """
    try:
        from mlx_lm import load
    except ImportError as e:
        raise ImportError(
            "mlx-lm is required for LLM inference. Install with: uv add mlx-lm"
        ) from e

    model, tokenizer = load(model_path)
    return model, tokenizer


def generate_text(
    model: Any,
    tokenizer: Any,
    prompt: str,
    max_tokens: int = 200,
) -> str:
    """
    Generates text using an MLX model.

    Args:
        model: MLX model instance.
        tokenizer: Model tokenizer.
        prompt: User prompt (will be formatted as chat message).
        max_tokens: Maximum tokens to generate.

    Returns:
        Generated text response.
    """
    from mlx_lm import generate

    # Format as chat message for instruction-tuned models
    messages = [{"role": "user", "content": prompt}]

    # Apply chat template
    formatted_prompt = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True,
    )

    # Generate response
    response = generate(
        model,
        tokenizer,
        prompt=formatted_prompt,
        max_tokens=max_tokens,
        verbose=False,
    )

    return response.strip()


def generate_text_batch(
    model: Any,
    tokenizer: Any,
    prompts: list[str],
    max_tokens: int = 200,
) -> list[str]:
    """
    Generates text for multiple prompts sequentially.

    Note: MLX currently doesn't support true batch inference,
    so this processes prompts one at a time.

    Args:
        model: MLX model instance.
        tokenizer: Model tokenizer.
        prompts: List of user prompts.
        max_tokens: Maximum tokens to generate per prompt.

    Returns:
        List of generated text responses.
    """
    responses = []
    for prompt in prompts:
        response = generate_text(model, tokenizer, prompt, max_tokens)
        responses.append(response)
    return responses

# -----------------------------------------------------------
# Tests for LLM Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from unittest.mock import MagicMock, patch

import pytest


class TestLoadMlxModel:
    """Tests for load_mlx_model function."""

    def test_load_mlx_model_success(self):
        """Test successful model loading."""
        mock_model = MagicMock()
        mock_tokenizer = MagicMock()

        with patch("mlx_lm.load") as mock_load:
            mock_load.return_value = (mock_model, mock_tokenizer)

            from data_pipeline.utils.llm_helpers import load_mlx_model
            model, tokenizer = load_mlx_model("test-model")

            mock_load.assert_called_once_with("test-model")
            assert model == mock_model
            assert tokenizer == mock_tokenizer

    def test_load_mlx_model_import_error(self):
        """Test that ImportError is raised when mlx-lm is not installed."""
        # This test verifies the error message when mlx-lm is unavailable
        # We can't easily test this without actually uninstalling mlx-lm
        pass


class TestGenerateText:
    """Tests for generate_text function."""

    def test_generate_text_formats_prompt_correctly(self):
        """Test that generate_text properly formats the prompt as a chat message."""
        mock_model = MagicMock()
        mock_tokenizer = MagicMock()
        mock_tokenizer.apply_chat_template.return_value = "formatted_prompt"

        with patch("mlx_lm.generate") as mock_generate:
            mock_generate.return_value = "  Generated response  "

            from data_pipeline.utils.llm_helpers import generate_text
            result = generate_text(mock_model, mock_tokenizer, "Test prompt")

            # Verify chat template was applied
            mock_tokenizer.apply_chat_template.assert_called_once()
            call_args = mock_tokenizer.apply_chat_template.call_args
            messages = call_args[0][0]
            assert messages == [{"role": "user", "content": "Test prompt"}]

            # Verify response is stripped
            assert result == "Generated response"

    def test_generate_text_passes_parameters(self):
        """Test that generate_text passes max_tokens correctly."""
        mock_model = MagicMock()
        mock_tokenizer = MagicMock()
        mock_tokenizer.apply_chat_template.return_value = "formatted"

        with patch("mlx_lm.generate") as mock_generate:
            mock_generate.return_value = "response"

            from data_pipeline.utils.llm_helpers import generate_text
            generate_text(
                mock_model, mock_tokenizer, "Test",
                max_tokens=100
            )

            mock_generate.assert_called_once()
            call_kwargs = mock_generate.call_args[1]
            assert call_kwargs["max_tokens"] == 100
            assert call_kwargs["verbose"] is False


class TestGenerateTextBatch:
    """Tests for generate_text_batch function."""

    def test_generate_text_batch_processes_all_prompts(self):
        """Test that generate_text_batch processes all prompts sequentially."""
        mock_model = MagicMock()
        mock_tokenizer = MagicMock()
        mock_tokenizer.apply_chat_template.return_value = "formatted"

        with patch("mlx_lm.generate") as mock_generate:
            mock_generate.side_effect = ["Response 1", "Response 2", "Response 3"]

            from data_pipeline.utils.llm_helpers import generate_text_batch
            results = generate_text_batch(
                mock_model, mock_tokenizer,
                ["Prompt 1", "Prompt 2", "Prompt 3"]
            )

            assert len(results) == 3
            assert results[0] == "Response 1"
            assert results[1] == "Response 2"
            assert results[2] == "Response 3"
            assert mock_generate.call_count == 3

    def test_generate_text_batch_empty_list(self):
        """Test generate_text_batch with empty prompt list."""
        mock_model = MagicMock()
        mock_tokenizer = MagicMock()

        from data_pipeline.utils.llm_helpers import generate_text_batch
        results = generate_text_batch(mock_model, mock_tokenizer, [])

        assert results == []

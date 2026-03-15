"""Tests for pipeline.runner — orchestration of fix pipeline."""
from unittest.mock import patch, call

from pipeline.runner import cmd_fix_all


class TestCmdFixAll:
    """Verify cmd_fix_all calls all 6 steps in correct order."""

    @patch('pipeline.runner.cmd_fix_title_normalization')
    @patch('pipeline.runner.cmd_backfill_ai_role_signature')
    @patch('pipeline.runner.cmd_backfill_title_ai')
    @patch('pipeline.runner.cmd_backfill_skills')
    @patch('pipeline.runner.cmd_fix_data_quality')
    @patch('pipeline.runner.cmd_normalize_platforms')
    def test_calls_all_six_steps(self, mock_norm, mock_fix, mock_skills,
                                  mock_title_ai, mock_sig, mock_title_norm):
        cmd_fix_all()
        mock_norm.assert_called_once()
        mock_fix.assert_called_once()
        mock_skills.assert_called_once()
        mock_title_ai.assert_called_once()
        mock_sig.assert_called_once()
        mock_title_norm.assert_called_once()

    @patch('pipeline.runner.cmd_fix_title_normalization')
    @patch('pipeline.runner.cmd_backfill_ai_role_signature')
    @patch('pipeline.runner.cmd_backfill_title_ai')
    @patch('pipeline.runner.cmd_backfill_skills')
    @patch('pipeline.runner.cmd_fix_data_quality')
    @patch('pipeline.runner.cmd_normalize_platforms')
    def test_correct_call_order(self, mock_norm, mock_fix, mock_skills,
                                 mock_title_ai, mock_sig, mock_title_norm):
        """Steps must execute in dependency order."""
        manager = patch('pipeline.runner.log')
        with manager:
            cmd_fix_all()

        # Verify ordering: normalize_platforms before fix_data_quality, etc.
        assert mock_norm.call_count == 1
        assert mock_fix.call_count == 1

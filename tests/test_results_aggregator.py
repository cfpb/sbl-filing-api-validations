import os
import shutil

from unittest.mock import patch, MagicMock

from pytest_mock import MockerFixture

from sqlalchemy.orm import scoped_session
from sbl_validation_processor.results_aggregator import aggregate_validation_results
from sbl_filing_api.entities.models.dao import SubmissionState, SubmissionDAO


class TestResultsAggregator:

    def test_results_aggregation(self, mocker: MockerFixture, tmp_path):

        results = {
            "total_records": 300003,
            "syntax_errors": {
                "single_field_count": 0,
                "multi_field_count": 0,
                "register_count": 0,
                "total_count": 0,
            },
            "logic_errors": {
                "single_field_count": 2500025,
                "multi_field_count": 9100091,
                "register_count": 300003,
                "total_count": 11900119,
            },
            "logic_warnings": {
                "single_field_count": 2700027,
                "multi_field_count": 300003,
                "register_count": 0,
                "total_count": 3000030,
            },
        }

        mock_submission = SubmissionDAO()
        mock_submission.state = SubmissionState.VALIDATION_IN_PROGRESS
        mock_submission.validation_results = None

        mock_db_session = MagicMock(spec=scoped_session)
        mock_query = mock_db_session.query.return_value
        mock_query.where.return_value.one.return_value = mock_submission

        mock_get_db_session = MagicMock()
        mock_get_db_session.__enter__.return_value = mock_db_session
        mock_get_db_session.__exit__.return_value = None

        shutil.copytree(
            "tests/test_files/1_res", tmp_path / "2025/123456789TESTBANK01/1_res"
        )
        with patch(
            "sbl_validation_processor.results_aggregator.get_db_session",
            return_value=mock_get_db_session,
        ):
            aggregate_validation_results(
                bucket=str(tmp_path),
                key="2025/123456789TESTBANK01/1_res/",
                results=results,
            )
            assert os.path.isfile(tmp_path / "2025/123456789TESTBANK01/1_report.csv")
            assert mock_submission.state == SubmissionState.VALIDATION_WITH_ERRORS
            assert (
                mock_submission.validation_results["logic_errors"]["total_count"]
                == 11900119
            )
            assert (
                mock_submission.validation_results["logic_warnings"]["total_count"]
                == 3000030
            )

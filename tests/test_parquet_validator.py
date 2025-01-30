import os
import shutil

from pytest_mock import MockerFixture

from sbl_validation_processor.parquet_validator import validate_parquets


class TestValidateParquets:

    def test_validate_parquets(self, mocker: MockerFixture, tmp_path):
        shutil.copytree(
            "tests/test_files/1_pqs", tmp_path / "123456789TESTBANK01/1_pqs"
        )
        results = validate_parquets(
            bucket=str(tmp_path), key="123456789TESTBANK01/1_pqs/"
        )
        res_parquet_files = os.listdir(tmp_path / "123456789TESTBANK01/1_res")
        assert set(res_parquet_files) == set(
            [
                "00001.parquet",
                "00002.parquet",
            ]
        )
        assert results == {
            "statusCode": 200,
            "body": '"done validating!"',
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": str(tmp_path)},
                        "object": {"key": "123456789TESTBANK01/1_res/"},
                    },
                    "results": {
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
                    },
                }
            ],
        }

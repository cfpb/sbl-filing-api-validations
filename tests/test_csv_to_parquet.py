import os
import shutil

from pytest_mock import MockerFixture

from sbl_validation_processor.csv_to_parquet import split_csv_into_parquet


class TestCsvToParquet:

    def test_csv_to_parquet(self, mocker: MockerFixture, tmp_path):
        test_dir = tmp_path / "test_files"
        test_dir.mkdir()
        shutil.copyfile("tests/test_files/1.csv", test_dir / "1.csv")
        results = split_csv_into_parquet(bucket=str(tmp_path), key="test_files/1.csv")
        parquet_files = os.listdir(test_dir / "1_pqs")
        assert set(parquet_files) == set(
            [
                "00001.parquet",
                "00002.parquet",
                "00003.parquet",
                "00004.parquet",
                "00005.parquet",
                "00006.parquet",
                "00007.parquet",
            ]
        )
        assert results == {
            "statusCode": 200,
            "body": '"done converting!"',
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": str(tmp_path)},
                        "object": {"key": "test_files/1_pqs/"},
                    }
                }
            ],
        }

import io
import pytest
from unittest.mock import patch, MagicMock

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.configuration import ParquetFormatConfiguration
from dlt.common.data_writers.writers import ParquetDataWriter


def test_parquet_writer_dictionary_encoding_config() -> None:
    # 1. Test with supports_dictionary_encoding = True (default)
    caps = DestinationCapabilitiesContext.generic_capabilities("parquet")
    caps.parquet_format = ParquetFormatConfiguration(supports_dictionary_encoding=True)

    with io.BytesIO() as f:
        with patch("dlt.common.libs.pyarrow.pyarrow.parquet.ParquetWriter") as mock_writer:
            # We need to mock columns_to_arrow as well since write_header calls it
            with patch("dlt.common.libs.pyarrow.columns_to_arrow", return_value=MagicMock()):
                writer = ParquetDataWriter(f, caps=caps)
                writer.write_header({})  # Empty schema
                mock_writer.assert_called_once()
                args, kwargs = mock_writer.call_args
                assert kwargs["use_dictionary"] is True

    # 2. Test with supports_dictionary_encoding = False
    caps = DestinationCapabilitiesContext.generic_capabilities("parquet")
    caps.parquet_format = ParquetFormatConfiguration(supports_dictionary_encoding=False)

    with io.BytesIO() as f:
        with patch("dlt.common.libs.pyarrow.pyarrow.parquet.ParquetWriter") as mock_writer:
            with patch("dlt.common.libs.pyarrow.columns_to_arrow", return_value=MagicMock()):
                writer = ParquetDataWriter(f, caps=caps)
                writer.write_header({})
                mock_writer.assert_called_once()
                args, kwargs = mock_writer.call_args
                assert kwargs["use_dictionary"] is False

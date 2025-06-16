import sw_minio_service


def test_import() -> None:
    """Test that the package can be imported without errors."""
    assert isinstance(sw_minio_service.__name__, str)

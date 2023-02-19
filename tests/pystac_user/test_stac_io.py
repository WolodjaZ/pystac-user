import asyncio
import json
import tempfile
from pathlib import Path
from typing import Dict, Optional

import aiohttp
import pytest

from pystac_user.stac_io import AsyncStacIO, DefaultStacIO

TEST_DATA = Path(__file__).parent / "data"
STAC_URLS = {
    "PLANETARY-COMPUTER": "https://planetarycomputer.microsoft.com/api/stac/v1",
    "EARTH-SEARCH": "https://earth-search.aws.element84.com/v0",
    "MLHUB": "https://api.radiant.earth/mlhub/v1",
    "forest-observatory": "https://storage.googleapis.com/cfo-public/vegetation/collection.json",  # noqa
    "nasa_iserv": "https://nasa-iserv.s3-us-west-2.amazonaws.com/catalog/catalog.json",
}


@pytest.fixture
def static_deafult_stac():
    # Initialize the ReadStac object with sample headers and params
    headers = {"User-Agent": "Mozilla/5.0"}
    params = {"key": "value"}
    read_stac = DefaultStacIO(headers=headers, params=params)
    return read_stac


@pytest.mark.asyncio
@pytest.fixture
def static_async_stac() -> AsyncStacIO:
    # Initialize the ReadStac object with sample headers and params
    headers = {"User-Agent": "Mozilla/5.0"}
    params = {"key": "value"}
    read_stac = AsyncStacIO(headers=headers, params=params)
    return read_stac


@pytest.mark.asyncio
async def test_static_async_read_text_from_href_local(
    static_async_stac: AsyncStacIO,
) -> None:
    # Test example
    stac_example = TEST_DATA / "test-case"
    # Read a local file
    result = await static_async_stac.read_href(str(stac_example / "catalog.json"))
    assert json.loads(result) == json.loads(
        (stac_example / "catalog.json").read_text("utf-8")
    )

    # Read multiple local files
    tasks = [
        asyncio.create_task(static_async_stac.read_text_from_href(str(file.resolve())))
        for file in stac_example.glob("*.json")
    ]
    results = await asyncio.gather(*tasks)
    assert len(results) == len(list(stac_example.glob("*.json")))

    # Test wrong path raises error
    wrong_path = "wrong_path.json"
    with pytest.raises(FileNotFoundError) as excinfo:
        await static_async_stac.read_href(wrong_path)

    assert f"File {wrong_path} not found." in str(excinfo.value)


@pytest.mark.asyncio
@pytest.mark.vcr
@pytest.mark.parametrize(("headers", "params"), [(None, None), ({"": ""}, {"": ""})])
async def test_static_async_read_text_from_href_url(
    static_async_stac: AsyncStacIO,
    headers: Optional[Dict[str, str]],
    params: Optional[Dict[str, str]],
) -> None:
    # Test example
    stac_static = [STAC_URLS["forest-observatory"], STAC_URLS["nasa_iserv"]]
    # Read a url
    result = await static_async_stac.read_href(
        stac_static[0], headers=headers, params=params
    )
    result = json.loads(result)
    assert result is not None
    assert "stac_version" in result
    # TODO check headers and params

    # Read multiple urls
    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.create_task(
                static_async_stac.read_text_from_href(
                    url, session=session, headers=headers, params=params
                )
            )
            for url in stac_static
        ]
        results = await asyncio.gather(*tasks)
        assert len(results) == len(stac_static)

    # Test wrong url raises error
    wrong_url = "https://wrong.url.com"
    with pytest.raises(Exception) as excinfo:
        await static_async_stac.read_href(wrong_url, headers=headers, params=params)

    assert f"Could not read uri {wrong_url}" in str(excinfo.value)


@pytest.mark.asyncio
async def test_fail_static_async_read_text_from_href(
    static_async_stac: AsyncStacIO,
) -> None:
    # Test wrong uri raises error
    wrong_uri = "ipsum://wrong_uri.json"
    with pytest.raises(ValueError) as excinfo:
        await static_async_stac.read_text_from_href(wrong_uri)

    assert f"Invalid scheme: ipsum for href: {wrong_uri}" in str(excinfo.value)


@pytest.mark.asyncio
@pytest.mark.vcr
async def test_static_async_read_text(static_async_stac: AsyncStacIO) -> None:
    # Local example
    stac_example = TEST_DATA / "test-case" / "catalog.json"
    result = await static_async_stac.read_text(str(stac_example.resolve()))
    assert json.loads(result) == json.loads(stac_example.read_text("utf-8"))
    # Url example
    result = await static_async_stac.read_text(STAC_URLS["forest-observatory"])
    result = json.loads(result)
    assert result is not None
    assert "stac_version" in result


@pytest.mark.asyncio
def test_static_async_write_text_to_href(static_async_stac: AsyncStacIO) -> None:
    # Test example
    stac_example = TEST_DATA / "test-case"

    # Temp directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        # Write to temp directory
        with asyncio.TaskGroup() as tg:
            for file in stac_example.glob("*.json"):
                tg.create_task(
                    static_async_stac.write_text_to_href(
                        str((temp_dir / file.name).resolve()), file.read_text("utf-8")
                    )
                )

        # Test files written
        assert len(list(temp_dir.glob("*.json"))) == len(
            list(stac_example.glob("*.json"))
        )

        # Test files written correctly
        for file in stac_example.glob("*.json"):
            assert (temp_dir / file.name).read_text() == file.read_text("utf-8")


@pytest.mark.asyncio
def test_fail_static_async_write_text_to_href(static_async_stac: AsyncStacIO) -> None:
    # Fail with url
    url_example = STAC_URLS["forest-observatory"]
    with pytest.raises(NotImplementedError) as excinfo:
        asyncio.run(static_async_stac.write_text_to_href(url_example, "test"))

    assert "Writing to remote files is not implemented"

    # Fail with wrong schema
    uri_example = "ipsum://wrong_uri.json"
    with pytest.raises(ValueError) as excinfo:
        asyncio.run(static_async_stac.write_text_to_href(uri_example, "test"))

    assert f"Invalid scheme: ipsum for href: {uri_example}" in str(excinfo.value)


@pytest.mark.asyncio
def test_static_async_write_text(static_async_stac: AsyncStacIO) -> None:
    # Test example
    stac_example = TEST_DATA / "test-case"

    # Temp directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        # Write to temp directory
        with asyncio.TaskGroup() as tg:
            for file in stac_example.glob("*.json"):
                tg.create_task(
                    static_async_stac.write_text(
                        str((temp_dir / file.name).resolve()), file.read_text("utf-8")
                    )
                )

        # Test files written
        assert len(list(temp_dir.glob("*.json"))) == len(
            list(stac_example.glob("*.json"))
        )

        # Test files written correctly
        for file in stac_example.glob("*.json"):
            assert (temp_dir / file.name).read_text() == file.read_text("utf-8")


@pytest.mark.asyncio
@pytest.mark.vcr
async def test_static_async_read_json(static_async_stac: AsyncStacIO) -> None:
    # Local example
    stac_example = TEST_DATA / "test-case" / "catalog.json"
    result = await static_async_stac.read_json(str(stac_example.resolve()))
    assert isinstance(result, dict)
    assert result == json.loads(stac_example.read_text("utf-8"))
    # Url example
    result = await static_async_stac.read_json(STAC_URLS["forest-observatory"])
    assert isinstance(result, dict)
    assert result is not None
    assert "stac_version" in result


@pytest.mark.asyncio
@pytest.mark.vcr
async def test_static_async_read_stac_object(static_async_stac: AsyncStacIO) -> None:
    pass


@pytest.mark.asyncio
async def test_static_async_save_json(static_async_stac: AsyncStacIO) -> None:
    # Test example
    stac_example = TEST_DATA / "test-case"

    # Temp directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        # Write to temp directory
        with asyncio.TaskGroup() as tg:
            for file in stac_example.glob("*.json"):
                tg.create_task(
                    static_async_stac.save_json(
                        str((temp_dir / file.name).resolve()),
                        json.loads(file.read_text("utf-8")),
                    )
                )

        # Test files written
        assert len(list(temp_dir.glob("*.json"))) == len(
            list(stac_example.glob("*.json"))
        )

        # Test files written correctly
        for file in stac_example.glob("*.json"):
            assert (temp_dir / file.name).read_text() == file.read_text("utf-8")

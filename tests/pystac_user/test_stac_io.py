import asyncio
import json
import tempfile
from pathlib import Path
from typing import Dict, Optional

import aiohttp
import pystac
import pytest
import pytest_asyncio
from pystac_client.conformance import ConformanceClasses
from pystac_client.exceptions import APIError

from pystac_user.stac_io import AsyncStacIO, DefaultStacIO, StacApiIO

TEST_DIR = Path(__file__).parent.parent
DATA_DIR = TEST_DIR / "data"
VCR_DIR = TEST_DIR / "cassettes"
STAC_URLS = {
    "PLANETARY-COMPUTER": "https://planetarycomputer.microsoft.com/api/stac/v1",
    "EARTH-SEARCH": "https://earth-search.aws.element84.com/v0",
    "MLHUB": "https://api.radiant.earth/mlhub/v1",
    "forest-observatory": "https://storage.googleapis.com/cfo-public/vegetation/collection.json",  # noqa
    "nasa_iserv": "https://nasa-iserv.s3-us-west-2.amazonaws.com/catalog/catalog.json",
}


@pytest.fixture(scope="module")
def vcr_config():
    return {"cassette_library_dir": str(VCR_DIR)}


@pytest.fixture
def static_default_stac() -> DefaultStacIO:
    # Initialize the ReadStac object with sample headers and params
    read_stac = DefaultStacIO(headers=None, params=None)
    return read_stac


@pytest_asyncio.fixture
def static_async_stac() -> AsyncStacIO:
    # Initialize the ReadStac object with sample headers and params
    read_stac = AsyncStacIO(headers=None, params=None)
    return read_stac


@pytest.fixture
def apistac() -> StacApiIO:
    # Initialize the ReadStac object with sample headers and params
    read_stac = StacApiIO(headers=None, parameters=None)
    return read_stac


class TestStaticIO:
    def test_read_text_from_href_local(
        self,
        static_default_stac: DefaultStacIO,
    ) -> None:
        # Test example
        stac_example = DATA_DIR / "test-case"
        # Read a local file
        result = static_default_stac.read_text_from_href(
            str(stac_example / "catalog.json")
        )
        assert json.loads(result) == json.loads(
            (stac_example / "catalog.json").read_text("utf-8")
        )

        # Read multiple local files
        results = [
            static_default_stac.read_text_from_href(str(file.resolve()))
            for file in stac_example.glob("**/*.json")
        ]
        assert len(results) == len(list(stac_example.glob("**/*.json")))

        # Test wrong path raises error
        wrong_path = "wrong_path.json"
        with pytest.raises(FileNotFoundError) as excinfo:
            static_default_stac.read_text_from_href(wrong_path)

        assert f"File {wrong_path} not found." in str(excinfo.value)

    @pytest.mark.vcr()
    @pytest.mark.parametrize(
        ("headers", "params"), [(None, None), ({"User-Agent": "Mozilla/5.0"}, None)]
    )
    def test_read_text_from_href_url(
        self,
        static_default_stac: DefaultStacIO,
        headers: Optional[Dict[str, str]],
        params: Optional[Dict[str, str]],
    ) -> None:
        # Get stac_io
        # Test example
        stac_static = [STAC_URLS["forest-observatory"], STAC_URLS["nasa_iserv"]]
        # Read a url
        result = static_default_stac.read_text_from_href(
            stac_static[0], headers=headers, params=params
        )
        result = json.loads(result)
        assert result is not None
        assert "stac_version" in result
        # TODO check headers and params

        # Read multiple urls
        results = [
            static_default_stac.read_text_from_href(url, headers=headers, params=params)
            for url in stac_static
        ]
        assert len(results) == len(stac_static)

    @pytest.mark.vcr()
    def test_fail_read_text_from_href_url(
        self,
        static_default_stac: DefaultStacIO,
    ) -> None:
        # Test wrong url raises error
        wrong_url = "https://wrong.url.com"
        with pytest.raises(Exception) as excinfo:
            static_default_stac.read_text_from_href(wrong_url)

        assert f"Could not read uri {wrong_url}" in str(excinfo.value)

    def test_fail_read_text_from_href(
        self,
        static_default_stac: DefaultStacIO,
    ) -> None:
        # Test wrong uri raises error
        wrong_uri = "ipsum://wrong_uri.json"
        with pytest.raises(ValueError) as excinfo:
            static_default_stac.read_text_from_href(wrong_uri)

        assert f"Invalid scheme: ipsum for href: {wrong_uri}" in str(excinfo.value)

    def test_read_text(self, static_default_stac: DefaultStacIO) -> None:
        # Local example
        stac_example = DATA_DIR / "test-case" / "catalog.json"
        result = static_default_stac.read_text(str(stac_example.resolve()))
        assert json.loads(result) == json.loads(stac_example.read_text("utf-8"))
        # Url example
        result = static_default_stac.read_text(STAC_URLS["forest-observatory"])
        result = json.loads(result)
        assert result is not None
        assert "stac_version" in result

    def test_write_text_to_href(self, static_default_stac: DefaultStacIO) -> None:
        # Test example
        stac_example = DATA_DIR / "test-case"

        # Temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            stac_files = list(stac_example.glob("**/*.json"))
            # Write to temp directory
            for file in stac_files:
                static_default_stac.write_text_to_href(
                    str(temp_dir / f"{file.parents[0].name}_{file.name}"),
                    file.read_text("utf-8"),
                )

            # Test files written
            assert len(list(temp_dir.glob("**/*.json"))) == len(stac_files)

            # Test files written correctly
            for file in stac_files:
                assert (
                    temp_dir / f"{file.parents[0].name}_{file.name}"
                ).read_text() == file.read_text("utf-8")

    def test_fail_write_text_to_href(self, static_default_stac: DefaultStacIO) -> None:
        # Fail with url
        url_example = STAC_URLS["forest-observatory"]
        with pytest.raises(NotImplementedError) as excinfo:
            static_default_stac.write_text_to_href(url_example, "test")

        assert "Writing to remote files is not implemented" in str(excinfo.value)

        # Fail with wrong schema
        uri_example = "ipsum://wrong_uri.json"
        with pytest.raises(ValueError) as excinfo:
            static_default_stac.write_text_to_href(uri_example, "test")

        assert f"Invalid scheme: ipsum for href: {uri_example}" in str(excinfo.value)

    def test_write_text(self, static_default_stac: DefaultStacIO) -> None:
        # Test example
        stac_example = DATA_DIR / "test-case"

        # Temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            stac_files = list(stac_example.glob("**/*.json"))
            # Write to temp directory
            for file in stac_files:
                static_default_stac.write_text(
                    str(temp_dir / f"{file.parents[0].name}_{file.name}"),
                    file.read_text("utf-8"),
                )

            # Test files written
            assert len(list(temp_dir.glob("**/*.json"))) == len(stac_files)

            # Test files written correctly
            for file in stac_files:
                assert (
                    temp_dir / f"{file.parents[0].name}_{file.name}"
                ).read_text() == file.read_text("utf-8")

    @pytest.mark.vcr
    async def test_read_json(self, static_default_stac: DefaultStacIO) -> None:
        # Local example
        stac_example = DATA_DIR / "test-case" / "catalog.json"
        result = static_default_stac.read_json(str(stac_example.resolve()))
        assert result is not None
        assert isinstance(result, dict)
        assert result == json.loads(stac_example.read_text("utf-8"))
        # Url example
        result = static_default_stac.read_json(STAC_URLS["forest-observatory"])
        assert result is not None
        assert isinstance(result, dict)
        assert "stac_version" in result

    @pytest.mark.vcr
    def test_read_stac_object(self, static_default_stac: DefaultStacIO) -> None:
        # Local example
        stac_example = DATA_DIR / "test-case" / "catalog.json"
        result = static_default_stac.read_stac_object(str(stac_example.resolve()))
        assert result is not None
        assert isinstance(result, pystac.STACObject)
        assert result.STAC_OBJECT_TYPE == pystac.STACObjectType.CATALOG
        # Test root catalog
        stac_example_collection = iter(
            (DATA_DIR / "test-case").glob("**/collection.json")
        ).__next__()
        result_collection = static_default_stac.read_stac_object(
            str(stac_example_collection.resolve()),
            root=result,
        )
        assert result_collection.STAC_OBJECT_TYPE == pystac.STACObjectType.COLLECTION
        assert result_collection.get_root() == result
        # Url example
        result = static_default_stac.read_stac_object(STAC_URLS["forest-observatory"])
        assert result is not None
        assert isinstance(result, pystac.STACObject)


class TestStaticAsyncIO:
    @pytest.mark.asyncio
    async def test_read_text_from_href_local(
        self,
        static_async_stac: AsyncStacIO,
    ) -> None:
        # Test example
        stac_example = DATA_DIR / "test-case"
        # Read a local file
        result = await static_async_stac.read_text_from_href(
            str(stac_example / "catalog.json")
        )
        assert json.loads(result) == json.loads(
            (stac_example / "catalog.json").read_text("utf-8")
        )

        # Read multiple local files
        tasks = [
            asyncio.create_task(
                static_async_stac.read_text_from_href(str(file.resolve()))
            )
            for file in stac_example.glob("**/*.json")
        ]
        results = await asyncio.gather(*tasks)
        assert len(results) == len(list(stac_example.glob("**/*.json")))

        # Test wrong path raises error
        wrong_path = "wrong_path.json"
        with pytest.raises(FileNotFoundError) as excinfo:
            await static_async_stac.read_text_from_href(wrong_path)

        assert f"File {wrong_path} not found." in str(excinfo.value)

    @pytest.mark.asyncio
    @pytest.mark.vcr
    @pytest.mark.parametrize(
        ("headers", "params"), [(None, None), ({"User-Agent": "Mozilla/5.0"}, None)]
    )
    async def test_static_text_from_href_url(
        self,
        static_async_stac: AsyncStacIO,
        headers: Optional[Dict[str, str]],
        params: Optional[Dict[str, str]],
    ) -> None:
        # Test example
        stac_static = [STAC_URLS["forest-observatory"], STAC_URLS["nasa_iserv"]]
        # Read a url
        result = await static_async_stac.read_text_from_href(
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
            await static_async_stac.read_text_from_href(
                wrong_url, headers=headers, params=params
            )

        assert f"Could not read uri {wrong_url}" in str(excinfo.value)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_fail_read_text_from_href_url(
        self,
        static_async_stac: AsyncStacIO,
    ) -> None:
        # Test wrong url raises error
        wrong_url = "https://wrong.url.com"
        with pytest.raises(Exception) as excinfo:
            await static_async_stac.read_text_from_href(wrong_url)

        assert f"Could not read uri {wrong_url}" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_fail_read_text_from_href(
        self, static_async_stac: AsyncStacIO
    ) -> None:
        # Test wrong uri raises error
        wrong_uri = "ipsum://wrong_uri.json"
        with pytest.raises(ValueError) as excinfo:
            await static_async_stac.read_text_from_href(wrong_uri)

        assert f"Invalid scheme: ipsum for href: {wrong_uri}" in str(excinfo.value)

    @pytest.mark.asyncio
    @pytest.mark.vcr
    async def test_read_text(self, static_async_stac: AsyncStacIO) -> None:
        # Local example
        stac_example = DATA_DIR / "test-case" / "catalog.json"
        result = await static_async_stac.read_text(str(stac_example.resolve()))
        assert json.loads(result) == json.loads(stac_example.read_text("utf-8"))
        # Url example
        result = await static_async_stac.read_text(STAC_URLS["forest-observatory"])
        result = json.loads(result)
        assert result is not None
        assert "stac_version" in result

    @pytest.mark.asyncio
    async def test_write_text_to_href(self, static_async_stac: AsyncStacIO) -> None:
        # Test example
        stac_example = DATA_DIR / "test-case"

        # Temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            stac_files = list(stac_example.glob("**/*.json"))
            # Write to temp directory
            tasks = [
                asyncio.create_task(
                    static_async_stac.write_text_to_href(
                        str(temp_dir / f"{file.parents[0].name}_{file.name}"),
                        file.read_text("utf-8"),
                    )
                )
                for file in stac_files
            ]
            await asyncio.gather(*tasks)
            # Test files written
            assert len(list(temp_dir.glob("**/*.json"))) == len(stac_files)

            # Test files written correctly
            for file in stac_files:
                assert (
                    temp_dir / f"{file.parents[0].name}_{file.name}"
                ).read_text() == file.read_text("utf-8")

    @pytest.mark.asyncio
    async def test_fail_write_text_to_href(
        self, static_async_stac: AsyncStacIO
    ) -> None:
        # Fail with url
        url_example = STAC_URLS["forest-observatory"]
        with pytest.raises(NotImplementedError) as excinfo:
            await static_async_stac.write_text_to_href(url_example, "test")

        assert "Writing to remote files is not implemented" in str(excinfo.value)

        # Fail with wrong schema
        uri_example = "ipsum://wrong_uri.json"
        with pytest.raises(ValueError) as excinfo:
            await static_async_stac.write_text_to_href(uri_example, "test")

        assert f"Invalid scheme: ipsum for href: {uri_example}" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_write_text(self, static_async_stac: AsyncStacIO) -> None:
        # Test example
        stac_example = DATA_DIR / "test-case"

        # Temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            stac_files = list(stac_example.glob("**/*.json"))
            # Write to temp directory
            tasks = [
                asyncio.create_task(
                    static_async_stac.write_text(
                        str(temp_dir / f"{file.parents[0].name}_{file.name}"),
                        file.read_text("utf-8"),
                    )
                )
                for file in stac_files
            ]
            await asyncio.gather(*tasks)
            # Test files written
            assert len(list(temp_dir.glob("**/*.json"))) == len(stac_files)

            # Test files written correctly
            for file in stac_files:
                assert (
                    temp_dir / f"{file.parents[0].name}_{file.name}"
                ).read_text() == file.read_text("utf-8")

    @pytest.mark.asyncio
    @pytest.mark.vcr
    async def test_read_json(self, static_async_stac: AsyncStacIO) -> None:
        # Local example
        stac_example = DATA_DIR / "test-case" / "catalog.json"
        result = await static_async_stac.read_json(str(stac_example.resolve()))
        assert result is not None
        assert isinstance(result, dict)
        assert result == json.loads(stac_example.read_text("utf-8"))
        # Url example
        result = await static_async_stac.read_json(STAC_URLS["forest-observatory"])
        assert result is not None
        assert isinstance(result, dict)
        assert "stac_version" in result

    @pytest.mark.asyncio
    @pytest.mark.vcr
    async def test_read_stac_object(self, static_async_stac: AsyncStacIO) -> None:
        # Local example
        stac_example = DATA_DIR / "test-case" / "catalog.json"
        result = await static_async_stac.read_stac_object(str(stac_example.resolve()))
        assert result is not None
        assert isinstance(result, pystac.STACObject)
        assert result.STAC_OBJECT_TYPE == pystac.STACObjectType.CATALOG
        # Test root catalog
        stac_example_collection = iter(
            (DATA_DIR / "test-case").glob("**/collection.json")
        ).__next__()
        result_collection = await static_async_stac.read_stac_object(
            str(stac_example_collection.resolve()),
            root=result,
        )

        assert result_collection.STAC_OBJECT_TYPE == pystac.STACObjectType.COLLECTION
        assert result_collection.get_root() == result
        # Url example
        result = await static_async_stac.read_stac_object(
            STAC_URLS["forest-observatory"]
        )
        assert result is not None
        assert isinstance(result, pystac.STACObject)

    @pytest.mark.asyncio
    async def test_save_json(self, static_async_stac: AsyncStacIO) -> None:
        # Test example
        stac_example = DATA_DIR / "test-case"

        # Temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            stac_files = list(stac_example.glob("**/*.json"))
            # Write to temp directory
            tasks = [
                asyncio.create_task(
                    static_async_stac.save_json(
                        str(temp_dir / f"{file.parents[0].name}_{file.name}"),
                        json.loads(file.read_text("utf-8")),
                    )
                )
                for file in stac_files
            ]
            await asyncio.gather(*tasks)
            # Test files written
            assert len(list(temp_dir.glob("**/*.json"))) == len(stac_files)

            # Test files written correctly
            for file in stac_files:
                assert json.loads(
                    (temp_dir / f"{file.parents[0].name}_{file.name}").read_text()
                ) == json.loads(file.read_text("utf-8"))


class TestStacApiIO:
    @pytest.mark.vcr
    def test_stac_request_input(self, apistac: StacApiIO) -> None:
        response = apistac.read_text(STAC_URLS["PLANETARY-COMPUTER"])
        assert isinstance(response, str)

    @pytest.mark.vcr
    def test_stac_str_input(self, apistac: StacApiIO) -> None:
        response = apistac.read_text(STAC_URLS["PLANETARY-COMPUTER"])
        assert isinstance(response, str)

    @pytest.mark.vcr
    def test_stac_http_error(self, apistac: StacApiIO) -> None:
        # Attempt to access an authenticated endpoint
        with pytest.raises(APIError) as excinfo:
            apistac.read_text(f"{STAC_URLS['MLHUB']}/search")
        assert isinstance(excinfo.value, APIError)

    @pytest.mark.vcr
    def test_stac_pages(self, apistac: StacApiIO) -> None:  # type: ignore
        # TODO: Add a test
        pass

    def test_stac_conforms_to(self, apistac: StacApiIO) -> None:
        # Fail if no conformance classes are set
        apistac.set_conformance([])
        assert not apistac.conforms_to(ConformanceClasses.CORE)
        # Pass if conformance classes are set
        apistac.set_conformance(["https://api.stacspec.org/v1.0.0-beta.1/core"])
        # Check that this does not raise an exception
        assert apistac.conforms_to(ConformanceClasses.CORE)

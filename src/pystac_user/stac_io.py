from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from aiohttp import ClientResponseError, ClientSession
from pystac import Catalog, StacIO, STACObject, link

# For Stac API IO we will use pystac_client implementation for now
from pystac_client.stac_api_io import StacApiIO
from requests import HTTPError, Session

__all__ = ["DefaultStacIO", "StacApiIO", "AsyncStacIO"]


class DefaultStacIO(StacIO):
    """Reimplemented version of `pystac.DefaultStacIO` with use of `pathlib.Path`
    instead of `os.path` and with `requests` instead of `urllib.request`.

    Args:
        headers (Optional[Dict[str, str]], optional):
            A dictionary of additional headers to use in all requests.
            Defaults to None.
        params (Optional[Dict[str, str]], optional):
            A dictionary of additional parameters to use in all requests.
            Defaults to None.
    """

    def __init__(
        self,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
    ) -> None:
        self.headers = headers or {}
        self.params = params or {}

    def read_text(self, source: link.HREF, *args: Any, **kwargs: Any) -> str:
        """
        A concrete implementation of the StacIO.read_text method.
        Converts `source` argument to a string (if it is not already) and
        delegates to `read_text_from_href`. for opening and reading the file.

        Args:
            source (link.HREF): The source to read from.
            args: Additional arguments to pass to `read_text_from_href`.
            kwargs: Additional arguments to pass to `read_text_from_href`.
                For example: `headers`, `params`, `session`. `headers` and `params`
                are used only if `source` is a remote file for the request.
                The type of `session` is `requests.Session`.

        Returns:
            str: The text read from the source.
        """
        # Convert PathLike to str
        # In pystac implementation they use `str(os.fspath(href)),`
        # however I prefer to use `pathlib.Path`
        # TODO: check if it works
        href = str(Path(source))
        # Get content from href
        href_content: str = self.read_text_from_href(
            href=href, args=args, kwargs=kwargs
        )
        return href_content

    def read_text_from_href(self, href: str, *_: Any, **kwargs: Any) -> str:
        """
        Reads file as a UTF-8 string.

        If `href` is a local file, it is read using the `open` function.
        Else, if `href` is a remote file, it is read using an `aiohttp` request.

        Raises:
            ValueError: Invalid scheme for href.

        Args:
            href : The URI of the file to open.
            kwargs: Additional arguments that may contain:
                `headers`, `params`, `session`. `headers` and `params`
                are used only if `source` is a remote file for the request.
                The type of `session` is `requests.Session`.

        Returns:
            str: The text read from the file.
        """
        # Check if href is a url or a local file
        if urlparse(href).scheme == "":
            # Read local file
            # Open file
            with open(href, encoding="utf-8") as f:
                href_content = f.read()
        elif urlparse(href).scheme in ["http", "https"]:
            # Read remote file
            # Update headers and params
            headers = kwargs.get("headers", None)
            params = kwargs.get("params", None)
            if headers is not None:
                headers = self.headers
            if params is None:
                params = self.params

            def make_request(s: Session) -> str:
                """Makes a request to `href` using `s` session."""
                # Read response
                r = s.get(href, headers=headers, params=params)
                href_content = r.text
                # Check status code
                try:
                    r.raise_for_status()
                except HTTPError as e:
                    raise Exception(f"Could not read uri {href}") from e
                return href_content

            session = kwargs.get("session", None)
            # Make request
            if session is None:
                # Create a new session
                with Session() as s:
                    href_content = make_request(s)
            else:
                # Use provided session
                href_content = make_request(session)
        else:
            raise ValueError(
                f"Invalid scheme: {urlparse(href).scheme} for href: {href}"
            )

        return href_content

    def write_text(self, dest: link.HREF, txt: str, *_: Any, **__: Any) -> None:
        """
        A concrete implementation of the StacIO.write_text method.
        Converts `dest` argument to a string (if it is not already) and
        delegates to `write_text_to_href`. For opening and writing the file.

        Args:
            dest (link.HREF): The destination to write to.
            txt (str): The text to write to the destination.
        """
        # Convert PathLike to str
        href = str(Path(dest))
        # Write content to href
        self.write_text_to_href(href=href, txt=txt)

    def write_text_to_href(self, href: str, txt: str) -> None:
        """
        Writes a UTF-8 string to a file.
        This method can only write to local files.
        Writing to remote files is not implemented and raises Exception.

        Raises:
            NotImplementedError: Writing to remote files is not implemented.
            ValueError: Invalid scheme for href.

        Args:
            href (str): The URI of the file to open.
            txt (str): The text to write to the destination.
        """
        # Check if href is a url or a local file
        if urlparse(href).scheme == "":
            # Write local file
            # Get directory name
            dirname = Path(href).parent
            # Create directory if it does not exist
            if not dirname.exists() and not dirname.is_dir():
                dirname.mkdir(parents=True)
            # Open file
            with open(href, "w", encoding="utf-8") as f:
                f.write(txt)
        elif urlparse(href).scheme in ["http", "https"]:
            # Write remote file
            raise NotImplementedError("Writing to remote files is not implemented")
        else:
            raise ValueError(
                f"Invalid scheme: {urlparse(href).scheme} for href: {href}"
            )

    def read_json(self, source: link.HREF, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Reads a dict from the given source.

        Args:
            source (link.HREF): The source to read from.
            *args : Additional positional arguments to be passed to
                :meth:`StacIO.read_text`.
            **kwargs : Additional keyword arguments to be passed to
                :meth:`StacIO.read_text`. May contain `headers`, `params`, `session`.
                `headers` and `params` are used only if `source` is a remote file
                for the request. The type of `session` is `requests.Session`.
        Returns:
            Dict[str, Any]: A dict representation of the JSON contained in the file
            at the given source.
        """
        # Read text from source
        txt = self.read_text(source=source, args=args, kwargs=kwargs)
        # Load json
        json_dict = self.json_loads(txt)
        return json_dict

    def read_stac_object(
        self,
        source: link.HREF,
        root: Optional[Catalog] = None,
        *args: Any,
        **kwargs: Any,
    ) -> STACObject:
        """
        Reads a STAC object from the given source.

        Args:
            source (link.HREF): The source to read from.
            root (Optional[Catalog], optional): Optional root of the catalog
                for this object. If provided, the root's resolved object cache can
                be used to search for previously resolved instances of the STAC object.
            *args : Additional positional arguments to be passed to
                :meth:`StacIO.read_text`.
            **kwargs : Additional keyword arguments to be passed to
                :meth:`StacIO.read_text`. May contain `headers`, `params`, `session`.
                `headers` and `params` are used only if `source` is a remote file
                for the request. The type of `session` is `requests.Session`.
        Returns:
            STACObject: The deserialized STACObject from the serialized JSON
            contained in the file at the given uri.
        """
        # Read json from source
        d = self.read_json(source=source, args=args, kwargs=kwargs)
        # Create STAC object
        stac_object = self.stac_object_from_dict(
            d,
            href=source,
            root=root,
            preserve_dict=True,  # TODO check if preserve_dict is needed
        )
        return stac_object


class AsyncStacIO(StacIO):
    """AsyncStacIO is a StacIO implementation that uses asyncio to make requests.
    It's similar to `DefaultStacIO` but with use of `asyncio` and `aiohttp`.

    Args:
        headers (Optional[Dict[str, str]], optional):
            A dictionary of additional headers to use in all requests.
            Defaults to None.
        params (Optional[Dict[str, str]], optional):
            A dictionary of additional parameters to use in all requests.
            Defaults to None.
    """

    def __init__(
        self,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
    ) -> None:
        self.headers = headers or {}
        self.params = params or {}

    async def read_text(  # type: ignore[override]
        self, source: link.HREF, *args: Any, **kwargs: Any
    ) -> str:
        """
        A concrete implementation of the StacIO.read_text method that uses asyncio
        to make requests. Converts `source` argument to a string (if it is not already)
        and delegates to `read_text_from_href`. for opening and reading the file.

        Args:
            source (link.HREF): The source to read from.
            args: Additional arguments to pass to `read_text_from_href`.
            kwargs: Additional arguments to pass to `read_text_from_href`.
                For example: `headers`, `params`, `session`. `headers` and `params`
                are used only if `source` is a remote file for the request.
                The type of `session` is `aiohttp.ClientSession`.
        Returns:
            str: The text read from the source.
        """
        # Convert PathLike to str
        href = str(Path(source))
        # Get content from href
        href_content: str = await self.read_text_from_href(
            href=href, args=args, kwargs=kwargs
        )
        return href_content

    async def read_text_from_href(  # type: ignore[override]
        self, href: str, *_: Any, **kwargs: Any
    ) -> str:
        """
        Reads asynchronously file as a UTF-8 string.

        If `href` is a local file, it is read using the `open` function.
        Else, if `href` is a remote file, it is read using an `aiohttp` request.

        Raises:
            ValueError: Invalid scheme for href.

        Args:
            href : The URI of the file to open.
            kwargs: Additional arguments to pass to `read_text_from_href`.
                For example: `headers`, `params`, `session`. `headers` and `params`
                are used only if `source` is a remote file for the request.
                The type of `session` is `aiohttp.ClientSession`.

        Returns:
            str: The text read from the file.
        """
        # Check if href is a url or a local file
        if urlparse(href).scheme == "":
            # Read local file
            # Open file
            with open(href, encoding="utf-8") as f:
                href_content = f.read()
        elif urlparse(href).scheme in ["http", "https"]:
            # Read remote file
            # Update headers and params
            headers = kwargs.get("headers", None)
            params = kwargs.get("params", None)

            if headers is None:
                headers = self.headers
            if params is None:
                params = self.params

            async def make_request(s: ClientSession) -> str:
                """Makes a request to `href` using `s` session."""
                async with s.get(href, headers=headers, params=params) as response:
                    # Read response
                    href_content = await response.text(encoding="utf-8")
                    # Check if response is valid. If http status code is not 200,
                    # raise an exception
                    try:
                        response.raise_for_status()
                    except ClientResponseError as e:
                        raise Exception(f"Could not read uri {href}") from e
                return href_content

            session = kwargs.get("session", None)
            # Make request
            if session is None:
                # Create session
                async with ClientSession() as s:
                    href_content = await make_request(s)
            else:
                # Use provided session
                href_content = await make_request(session)
        else:
            raise ValueError(
                f"Invalid scheme: {urlparse(href).scheme} for href: {href}"
            )

        return href_content

    async def write_text(  # type: ignore[override]
        self, dest: link.HREF, txt: str, *_: Any, **__: Any
    ) -> None:
        """
        A concrete implementation of the StacIO.write_text method that uses asyncio
        to make requests. Converts `dest` argument to a string (if it is not already)
        and delegates to `write_text_to_href`. For opening and writing the file.

        Args:
            dest (link.HREF): The destination to write to.
            txt (str): The text to write to the destination.
        """
        # Convert PathLike to str
        href = str(Path(dest))
        # Write content to href
        await self.write_text_to_href(href=href, txt=txt)

    async def write_text_to_href(  # type: ignore[override]
        self, href: str, txt: str
    ) -> None:
        """
        Writes asynchronously a UTF-8 string to a file.
        This method can only write to local files.
        Writing to remote files is not implemented and raises Exception.

        Raises:
            NotImplementedError: Writing to remote files is not implemented.
            ValueError: Invalid scheme for href.

        Args:
            href (str): The URI of the file to open.
            txt (str): The text to write to the destination.
        """
        # Check if href is a url or a local file
        if urlparse(href).scheme == "":
            # Write local file
            # Get directory name
            dirname = Path(href).parent
            # Create directory if it does not exist
            if not dirname.exists() and not dirname.is_dir():
                dirname.mkdir(parents=True)
            # Open file
            with open(href, "w", encoding="utf-8") as f:
                f.write(txt)
        elif urlparse(href).scheme in ["http", "https"]:
            # Write remote file
            raise NotImplementedError("Writing to remote files is not implemented")
        else:
            raise ValueError(
                f"Invalid scheme: {urlparse(href).scheme} for href: {href}"
            )

    async def read_json(  # type: ignore[override]
        self,
        source: link.HREF,
        *args: Any,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Reads asynchronously a dict from the given source.

        Args:
            source (link.HREF): The source to read from.
            *args : Additional positional arguments to be passed to
                :meth:`StacIO.read_text`.
            **kwargs : Additional keyword arguments to be passed to
                :meth:`StacIO.read_text`. May contain `headers`, `params`, `session`.
                `headers` and `params` are used only if `source` is a remote file
                for the request. The type of `session` is `aiohttp.ClientSession`.
        Returns:
            Dict[str, Any]: A dict representation of the JSON contained
            in the file at the given source. M
        """
        # Read text from source
        txt = await self.read_text(source=source, args=args, kwargs=kwargs)
        # Load json
        json_dict = self.json_loads(txt)
        return json_dict

    async def read_stac_object(  # type: ignore[override]
        self,
        source: link.HREF,
        root: Optional[Catalog] = None,
        *args: Any,
        **kwargs: Any,
    ) -> STACObject:
        """
        Reads asynchronously a STAC object from the given source.

        Args:
            source (link.HREF): The source to read from.
            root (Optional[Catalog], optional): Optional root of the catalog
                for this object. If provided, the root's resolved object cache can
                be used to search for previously resolved instances of the STAC object.
            *args : Additional positional arguments to be passed to
                :meth:`StacIO.read_text`.
            **kwargs : Additional keyword arguments to be passed to
                :meth:`StacIO.read_text`. May contain `headers`, `params`, `session`.
                `headers` and `params` are used only if `source` is a remote file
                for the request. The type of `session` is `aiohttp.ClientSession`.
        Returns:
            STACObject: The deserialized STACObject from the serialized JSON
            contained in the file at the given uri.
        """
        # Read json from source
        d = await self.read_json(source=source, args=args, kwargs=kwargs)
        # Create STAC object
        stac_object = self.stac_object_from_dict(
            d,
            href=source,
            root=root,
            preserve_dict=True,  # TODO check if preserve_dict is needed
        )
        return stac_object

    async def save_json(  # type: ignore[override]
        self,
        dest: link.HREF,
        json_dict: Dict[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """_summary_

        Args:
            dest (link.HREF): The destination to write to.
            json_dict (Dict[str, Any]): The JSON dict to write.
            *args : Additional positional arguments to be passed to
                :meth:`StacIO.read_text`.
            **kwargs : Additional keyword arguments to be passed to
                :meth:`StacIO.read_text`.
        """
        # Dump json to text
        txt = self.json_dumps(json_dict, args=args, kwargs=kwargs)
        # Write text to dest
        await self.write_text(dest=dest, txt=txt)

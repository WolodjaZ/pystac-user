import os
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import aiofiles
from aiohttp import ClientSession
from pystac import Catalog, StacIO, STACObject, link
from requests import Session

__all__ = ["DefaultStacIO", "AsyncStacIO"]


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
        self.headers = headers or None
        self.params = params or None

    def read_text(self, source: link.HREF, *args: Any, **kwargs: Any) -> str:
        """
        A concrete implementation of the StacIO.read_text method.
        Converts `source` argument to a string (if it is not already) and
        delegates to `read_text_from_href`. for opening and reading the file.

        Args:
            source (link.HREF): The source to read from.
            args: Additional arguments to pass to `read_text_from_href`.
            kwargs: Additional arguments to pass to `read_text_from_href`.
                For example: `headers`, `session`. `headers` is used only if
                `source` is a remote file for the request. The type of `session`
                is `requests.Session`.

        Returns:
            str: The text read from the source.
        """
        # Convert PathLike to str
        href = os.fspath(source)
        # Get content from href
        href_content: str = self.read_text_from_href(
            href=href, args=args, kwargs=kwargs
        )
        return href_content

    def _request(
        self,
        session: Session,
        method: str,
        href: str,
        headers: Optional[Dict[str, str]],
        params: Optional[Dict[str, str]],
    ) -> str:
        """Makes a request to `href` using `s` session.

        Args:
            session (Session): `request.Session` to use.
            method (str): HTTP method to use. Either "GET" or "POST".
            href (str): The URL of the file to open.
            headers (Optional[Dict[str,str]]):
                A dictionary of additional headers to use in the request.
            params (Optional[Dict[str,str]]):
                A dictionary of additional parameters to use in the request.
                For `GET` requests, the parameters are encoded into the URL.
                For `POST` requests, the parameters are encoded into the body.
        """
        # Send response
        if method == "GET":
            response = session.get(href, headers=headers, params=params)
        else:
            response = session.post(href, headers=headers, json=params)

        # Set encoding
        response.encoding = "utf-8"
        # Read response
        href_content = response.text
        # Raise error if status code is not 200
        response.raise_for_status()
        return href_content

    def read_text_from_href(self, href: str, *_: Any, **kwargs: Any) -> str:
        """
        Reads file as a UTF-8 string.

        If `href` is a local file, it is read using the `open` function.
        Else, if `href` is a remote file, it is read using an `aiohttp` request.

        Raises:
            ValueError: Path incorrect or file not found: `href`.

        Args:
            href : The URI of the file to open.
            kwargs: Additional arguments to pass to `read_text_from_href`.
                For example: `headers`, `session`. `headers` is used only if
                `source` is a remote file for the request. The type of `session`
                is `requests.Session`.

        Returns:
            str: The text read from the file.
        """
        # Check if href is a url or a local file
        if not urlparse(href).scheme in ["http", "https"]:
            # Read local file
            # Open file
            if Path(href).is_file():
                with open(href, encoding="utf-8") as f:
                    href_content = f.read()
            else:
                raise ValueError(f"Path incorrect or file not found: {href}.")
        else:
            # Read remote file
            # Update headers and params
            headers = kwargs.get("headers", None)
            params = kwargs.get("params", None)
            if headers is None:
                headers = self.headers
            if params is None:
                params = self.headers

            # Get method
            method = kwargs.get("method", "GET")
            if method not in ["GET", "POST"]:
                raise ValueError(
                    f"Invalid method: {method} we only support GET and POST"
                )

            session = kwargs.get("session", None)
            # Make request
            if session is None:
                # Create a new session
                with Session() as s:
                    try:
                        href_content = self._request(s, method, href, headers, params)
                    except Exception as e:
                        raise Exception(f"Could not read uri {href}") from e
            else:
                # Use provided session
                href_content = self._request(session, method, href, headers, params)

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
        href = os.fspath(dest)
        # Write content to href
        self.write_text_to_href(href=href, txt=txt)

    def write_text_to_href(self, href: str, txt: str) -> None:
        """
        Writes a UTF-8 string to a file.
        This method can only write to local files.
        Writing to remote files is not implemented and raises Exception.

        Raises:
            NotImplementedError: Writing to remote files is not implemented.

        Args:
            href (str): The URI of the file to open.
            txt (str): The text to write to the destination.
        """
        # Check if href is a url or a local file
        if not urlparse(href).scheme in ["http", "https"]:
            # Write local file
            # Get directory name
            dirname = Path(href).parent
            # Create directory if it does not exist
            if not dirname.exists() and not dirname.is_dir():
                dirname.mkdir(parents=True)
            # Open file
            with open(href, "w", encoding="utf-8") as file:
                file.write(txt)
        else:
            # Write remote file
            raise NotImplementedError("Writing to remote files is not implemented")

    def read_json(self, source: link.HREF, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Reads a dict from the given source.

        Args:
            source (link.HREF): The source to read from.
            *args : Additional positional arguments to be passed to
                :meth:`StacIO.read_text`.
            **kwargs : Additional keyword arguments to be passed to
                :meth:`StacIO.read_text`. May contain `headers`, `session`.
                `headers` is used only if `source` is a remote file
                for the request. The type of `session` is `requests.Session`.
        Returns:
            Dict[str, Any]: A dict representation of the JSON contained in the file
            at the given source.
        """
        # Read text from source
        text = self.read_text(source=source, args=args, kwargs=kwargs)
        # Load json
        json_text = self.json_loads(text)
        return json_text

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
                :meth:`StacIO.read_text`. May contain `headers`, `session`.
                `headers` is used only if `source` is a remote file
                for the request. The type of `session` is `requests.Session`.
        Returns:
            STACObject: The deserialized STACObject from the serialized JSON
            contained in the file at the given uri.
        """
        # Read json from source
        json_text = self.read_json(source=source, args=args, kwargs=kwargs)
        # Create STAC object
        stac_object = self.stac_object_from_dict(
            json_text,
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
            A dictionary of additional query parameters to use in all requests.
            Defaults to None.
    """

    def __init__(
        self,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
    ) -> None:
        self.headers = headers or None
        self.params = params or None

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
                For example: `headers`, `session`. `headers` is used only if `source`
                is a remote file for the request. The type of `session` is
                `aiohttp.ClientSession`.
        Returns:
            str: The text read from the source.
        """
        # Convert PathLike to str
        href = os.fspath(source)
        # Get content from href
        href_content: str = await self.read_text_from_href(
            href=href, args=args, kwargs=kwargs
        )
        return href_content

    async def _request(
        self,
        session: ClientSession,
        method: str,
        href: str,
        headers: Optional[Dict[str, str]],
        params: Optional[Dict[str, str]],
    ) -> str:
        """Makes a request to `href` using `s` session.

        Args:
            session (ClientSession): `aiohttp.ClientSession` to use for the request.
            method (str): HTTP method to use. Either "GET" or "POST".
            href (str): The URL of the file to open.
            headers (Optional[Dict[str,str]]):
                A dictionary of additional headers to use in the request.
            params (Optional[Dict[str,str]]):
                A dictionary of additional parameters to use in the request.
                For `GET` requests, the parameters are encoded into the URL.
                For `POST` requests, the parameters are encoded into the body.
        """
        # Send response
        if method == "GET":
            async with session.get(href, headers=headers, params=params) as response:
                # Read response
                href_content = await response.text(encoding="utf-8")
                # Raise error if status code is not 200
                response.raise_for_status()
        else:
            async with session.post(href, headers=headers, json=params) as response:
                # Read response
                href_content = await response.text(encoding="utf-8")
                # Raise error if status code is not 200
                response.raise_for_status()

        return href_content

    async def read_text_from_href(  # type: ignore[override]
        self, href: str, *_: Any, **kwargs: Any
    ) -> str:
        """
        Reads asynchronously file as a UTF-8 string.

        If `href` is a local file, it is read using the `open` function.
        Else, if `href` is a remote file, it is read using an `aiohttp` request.

        Raises:
            ValueError: Path incorrect or file not found: `href`.

        Args:
            href : The URI of the file to open.
            kwargs: Additional arguments to pass to `read_text_from_href`.
                For example: `headers`, `session`. `headers` is used only if
                `source` is a remote file for the request.
                The type of `session` is `aiohttp.ClientSession`.

        Returns:
            str: The text read from the file.
        """
        # Check if href is a url or a local file
        if not urlparse(href).scheme in ["http", "https"]:
            # Read local file
            # Open file
            if Path(href).is_file():
                async with aiofiles.open(href, encoding="utf-8") as f:
                    href_content = await f.read()
            else:
                raise ValueError(f"Path incorrect or file not found: {href}.")
        else:
            # Read remote file
            # Update headers and params
            headers = kwargs.get("headers", None)
            params = kwargs.get("params", None)
            if headers is None:
                headers = self.headers
            if params is None:
                params = self.params

            # Get method
            method = kwargs.get("method", "GET")
            if method not in ["GET", "POST"]:
                raise ValueError(
                    f"Invalid method: {method} we only support GET and POST"
                )

            session = kwargs.get("session", None)
            # Make request
            if session is None:
                # Create session
                async with ClientSession() as s:
                    try:
                        href_content = await self._request(
                            s, method, href, headers, params
                        )
                    except Exception as e:
                        raise Exception(f"Could not read uri {href}") from e
            else:
                # Use provided session
                href_content = await self._request(
                    session, method, href, headers, params
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
        href = os.fspath(dest)
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

        Args:
            href (str): The URI of the file to open.
            txt (str): The text to write to the destination.
        """
        # Check if href is a url or a local file
        if not urlparse(href).scheme in ["http", "https"]:
            # Write local file
            # Get directory name
            dirname = Path(href).parent
            # Create directory if it does not exist
            if not dirname.exists() and not dirname.is_dir():
                dirname.mkdir(parents=True)
            # Open file
            async with aiofiles.open(href, "w", encoding="utf-8") as file:
                await file.write(txt)
        else:
            # Write remote file
            raise NotImplementedError("Writing to remote files is not implemented")

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
                :meth:`StacIO.read_text`. May contain `headers`, `session`.
                `headers` is used only if `source` is a remote file
                for the request. The type of `session` is `aiohttp.ClientSession`.
        Returns:
            Dict[str, Any]: A dict representation of the JSON contained
            in the file at the given source. M
        """
        # Read text from source
        text = await self.read_text(source=source, args=args, kwargs=kwargs)
        # Load json
        json_text = self.json_loads(text)
        return json_text

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
                :meth:`StacIO.read_text`. May contain `headers`, `session`.
                `headers` is used only if `source` is a remote file
                for the request. The type of `session` is `aiohttp.ClientSession`.
        Returns:
            STACObject: The deserialized STACObject from the serialized JSON
            contained in the file at the given uri.
        """
        # Read json from source
        json_text = await self.read_json(source=source, args=args, kwargs=kwargs)
        # Create STAC object
        stac_object = self.stac_object_from_dict(
            json_text,
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
        txt = self.json_dumps(json_dict, *args, **kwargs)
        # Write text to dest
        await self.write_text(dest=dest, txt=txt)

"""MinIO MCP server implementation using FastMCP.

This module exposes the MinIO Model Context Protocol (MCP) server with
both stdio and HTTP/SSE transports powered by the `fastmcp` package.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import logging
import os
from typing import Any
from urllib.parse import unquote

from dotenv import load_dotenv
from fastmcp import FastMCP
from fastmcp.exceptions import NotFoundError
from fastmcp.resources.resource import Resource as FastMCPResource
from fastmcp.server.server import ReadResourceContents
from pydantic import AnyUrl, Field, PrivateAttr

from resources.minio_resource import MinioResource, is_text_file


# Load environment variables before we create any clients.
load_dotenv()


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("mcp_minio_server")


DEFAULT_MAX_BUCKETS = int(os.getenv("MINIO_MAX_BUCKETS", "5"))
DEFAULT_MAX_KEYS = int(os.getenv("MINIO_MAX_KEYS", "1000"))
DEFAULT_TRANSPORT = os.getenv("MCP_TRANSPORT", "stdio")
DEFAULT_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
DEFAULT_PORT = int(os.getenv("SERVER_PORT", "8000"))

SSE_PATH = os.getenv("SERVER_SSE_PATH")
MESSAGE_PATH = os.getenv("SERVER_MESSAGE_PATH")


class MinioObjectResource(FastMCPResource):
    """FastMCP resource representing a single MinIO object."""

    bucket_name: str = Field(description="Name of the bucket that stores the object")
    object_name: str = Field(description="Key of the object inside the bucket")
    is_text: bool = Field(default=False, description="Whether the object should be treated as text")

    _minio: MinioResource = PrivateAttr()

    def __init__(self, *, minio_resource: MinioResource, **data: Any) -> None:
        super().__init__(**data)
        self._minio = minio_resource

    async def fetch(self) -> tuple[str | bytes, str]:
        """Fetch the object content and its MIME type from MinIO."""

        try:
            response = await self._minio.get_object(self.bucket_name, self.object_name)
        except Exception as error:  # pragma: no cover - defensive guard
            logger.error(
                "Error fetching object %s from bucket %s: %s",
                self.object_name,
                self.bucket_name,
                error,
                exc_info=True,
            )
            raise NotFoundError(
                f"Unable to read object {self.object_name} from bucket {self.bucket_name}"
            ) from error

        data = response.get("Body")
        if data is None:
            raise NotFoundError(
                f"No data returned for object {self.object_name} in bucket {self.bucket_name}"
            )

        content_type = response.get("ContentType")
        if not content_type:
            content_type = "text/plain" if self.is_text else "application/octet-stream"

        if self.is_text:
            content = base64.b64encode(data).decode("utf-8")
        else:
            content = data

        return content, content_type

    async def read(self) -> str | bytes:
        content, _ = await self.fetch()
        return content


class MinioFastMCP(FastMCP):
    """FastMCP server that exposes MinIO buckets, objects and tools."""

    def __init__(
        self,
        *,
        minio_resource: MinioResource,
        max_keys: int = DEFAULT_MAX_KEYS,
        bucket_concurrency: int = 3,
        **kwargs: Any,
    ) -> None:
        super().__init__(name="minio_service", version="0.1.0", **kwargs)
        self._minio_resource = minio_resource
        self._max_keys = max_keys
        self._bucket_concurrency = bucket_concurrency
        self._resource_cache: dict[str, MinioObjectResource] = {}

    async def _list_resources(self) -> list[FastMCPResource]:
        base_resources = await super()._list_resources()

        self._resource_cache.clear()
        resources: list[FastMCPResource] = []

        try:
            buckets = await self._minio_resource.list_buckets()
        except Exception as error:
            logger.error("Error listing MinIO buckets: %s", error, exc_info=True)
            raise

        semaphore = asyncio.Semaphore(self._bucket_concurrency)

        async def process_bucket(bucket: dict[str, Any]) -> None:
            bucket_name = bucket["Name"]
            async with semaphore:
                try:
                    objects = await self._minio_resource.list_objects(
                        bucket_name,
                        max_keys=self._max_keys,
                    )
                except Exception as exc:  # pragma: no cover - network failure guard
                    logger.error(
                        "Error listing objects in bucket %s: %s",
                        bucket_name,
                        exc,
                        exc_info=True,
                    )
                    return

                for obj in objects:
                    object_key = obj.get("Key")
                    if not object_key or object_key.endswith("/"):
                        continue

                    is_text = is_text_file(object_key)
                    resource = MinioObjectResource(
                        uri=f"minio://{bucket_name}/{object_key}",
                        name=object_key,
                        mime_type="text/plain" if is_text else "application/octet-stream",
                        bucket_name=bucket_name,
                        object_name=object_key,
                        is_text=is_text,
                        minio_resource=self._minio_resource,
                    )
                    self._resource_cache[resource.key] = resource
                    resources.append(resource)

        await asyncio.gather(*(process_bucket(bucket) for bucket in buckets))

        return base_resources + resources

    async def _read_resource(self, uri: AnyUrl | str) -> list[ReadResourceContents]:
        try:
            return await super()._read_resource(uri)
        except NotFoundError as original_error:
            uri_str = str(uri)

            resource = self._resource_cache.get(uri_str)
            if resource is None:
                if not uri_str.startswith("minio://"):
                    raise original_error

                try:
                    bucket_name, object_name = self._parse_minio_uri(uri_str)
                except ValueError as parse_error:
                    raise original_error from parse_error

                is_text = is_text_file(object_name)
                resource = MinioObjectResource(
                    uri=uri_str,
                    name=object_name,
                    mime_type="text/plain" if is_text else "application/octet-stream",
                    bucket_name=bucket_name,
                    object_name=object_name,
                    is_text=is_text,
                    minio_resource=self._minio_resource,
                )

            content, mime_type = await resource.fetch()
            return [ReadResourceContents(content=content, mime_type=mime_type)]

    @staticmethod
    def _parse_minio_uri(uri: str) -> tuple[str, str]:
        if not uri.startswith("minio://"):
            raise ValueError(f"Unsupported URI scheme: {uri}")

        path = unquote(uri[len("minio://") :])
        if "/" not in path:
            raise ValueError(f"Invalid MinIO URI: {uri}")

        bucket_name, object_name = path.split("/", 1)
        if not bucket_name or not object_name:
            raise ValueError(f"Invalid MinIO URI: {uri}")

        return bucket_name, object_name


minio_resource = MinioResource(max_buckets=DEFAULT_MAX_BUCKETS)

server_kwargs: dict[str, Any] = {}
if SSE_PATH:
    server_kwargs["sse_path"] = SSE_PATH
if MESSAGE_PATH:
    server_kwargs["message_path"] = MESSAGE_PATH

server = MinioFastMCP(minio_resource=minio_resource, **server_kwargs)


@server.tool(name="ListBuckets", description="Return available MinIO buckets")
async def list_buckets_tool(start_after: str | None = None, max_buckets: int | None = None) -> list[dict[str, Any]]:
    logger.debug("Listing buckets start_after=%s max_buckets=%s", start_after, max_buckets)
    buckets = await minio_resource.list_buckets(start_after)
    if max_buckets is not None:
        buckets = buckets[:max_buckets]
    return buckets


@server.tool(
    name="ListObjects",
    description="List objects stored in a MinIO bucket",
)
async def list_objects_tool(
    bucket_name: str,
    prefix: str = "",
    max_keys: int = DEFAULT_MAX_KEYS,
) -> list[dict[str, Any]]:
    logger.debug(
        "Listing objects for bucket=%s prefix=%s max_keys=%s",
        bucket_name,
        prefix,
        max_keys,
    )
    return await minio_resource.list_objects(bucket_name, prefix=prefix, max_keys=max_keys)


@server.tool(name="GetObject", description="Fetch an object's metadata and data from MinIO")
async def get_object_tool(bucket_name: str, object_name: str) -> dict[str, Any]:
    logger.debug("Fetching object bucket=%s key=%s", bucket_name, object_name)
    response = await minio_resource.get_object(bucket_name, object_name)
    body = response.get("Body")
    if isinstance(body, bytes):
        response = {**response, "Body": base64.b64encode(body).decode("utf-8")}
    return response


@server.tool(name="PutObject", description="Upload a local file to a MinIO bucket")
async def put_object_tool(bucket_name: str, object_name: str, file_path: str) -> dict[str, Any]:
    logger.debug(
        "Uploading object bucket=%s key=%s from path=%s",
        bucket_name,
        object_name,
        file_path,
    )
    return await minio_resource.put_object(bucket_name, object_name, file_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the MinIO MCP server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "http", "streamable-http"],
        default=DEFAULT_TRANSPORT,
        help="Transport protocol to use",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help="Host to bind for HTTP/SSE transports",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help="Port to bind for HTTP/SSE transports",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    transport = args.transport

    run_kwargs: dict[str, Any] = {}
    if transport in {"sse", "http", "streamable-http"}:
        run_kwargs.update(host=args.host, port=args.port)

    server.run(transport=transport, **run_kwargs)


if __name__ == "__main__":
    main()

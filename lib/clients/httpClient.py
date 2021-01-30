from .errorHandler import UnauthorizedException, ForbiddenException, ApiException, ConflictException, \
    ValidationException, InternalException, NotFoundException, TooManyRequestsException
from typing_extensions import TypedDict
from typing import Optional
from ..metaApi.models import ExceptionMessage
import json
import asyncio
import sys
import httpx
from httpx import HTTPError, Response


if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class RequestOptions(TypedDict):
    """Options for HttpClient requests."""
    method: Optional[str]
    url: str
    headers: Optional[dict]
    params: Optional[dict]
    body: Optional[dict]
    files: Optional[dict]


class HttpClient:
    """HTTP client library based on requests module."""
    def __init__(self, timeout: float = 60):
        """Inits HttpClient class instance.

        Args:
            timeout: Request timeout in seconds.
        """
        self._timeout = timeout

    async def request(self, options: RequestOptions) -> Response:
        """Performs a request. Response errors are returned as ApiError or subclasses.

        Args:
            options: Request options.

        Returns:
            A request response.
        """
        response = await self._make_request(options)
        try:
            response.raise_for_status()
            if response.content:
                try:
                    response = response.json()
                except Exception as err:
                    print('Error parsing json', err)
        except HTTPError as err:
            self._convert_error(err)
        return response

    async def _make_request(self, options: RequestOptions) -> Response:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            method = options['method'] if ('method' in options) else 'GET'
            url = options['url']
            params = options['params'] if 'params' in options else None
            files = options['files'] if 'files' in options else None
            headers = options['headers'] if 'headers' in options else None
            body = options['body'] if 'body' in options else None
            req = client.build_request(method, url, params=params, files=files, headers=headers, json=body)
            response = await client.send(req)
            return response

    def _convert_error(self, err: HTTPError):
        try:
            response: ExceptionMessage or TypedDict = json.loads(err.response.text)
        except Exception:
            response = {}
        err_message = response['message'] if 'message' in response else err.response.reason_phrase
        status = err.response.status_code
        if status == 400:
            details = response['details'] if 'details' in response else []
            raise ValidationException(err_message, details)
        elif status == 401:
            raise UnauthorizedException(err_message)
        elif status == 403:
            raise ForbiddenException(err_message)
        elif status == 404:
            raise NotFoundException(err_message)
        elif status == 409:
            raise ConflictException(err_message)
        elif status == 429:
            raise TooManyRequestsException(err_message)
        elif status == 500:
            raise InternalException(err_message)
        else:
            raise ApiException(err_message, status)

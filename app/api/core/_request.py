from datetime import datetime
from abc import ABC
from typing import Optional, Union, Dict, Any, Type, TypeVar
import httpx
import asyncio
from pydantic import BaseModel
from app.settings.log import logger

T = TypeVar("T", bound=BaseModel)


class ApiRequest(ABC):
    """
    Abstract base class for API interactions with robust session management
    """

    def __init__(
        self,
        host: str,
    ) -> None:
        """
        Initialize API client
        """
        self.host = host.rstrip("/")
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=5.0, read=10.0),
            verify=False,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
        )

    def _get_headers(self, access: Optional[str] = None) -> Dict[str, str]:
        """
        Generate authentication headers
        """
        if access:
            headers = {"Content-Type": "application/json"}
            headers["Authorization"] = f"Bearer {access}"
        else:
            headers = None
        return headers

    async def _request(
        self,
        method: str,
        endpoint: str,
        access: Optional[str] = None,
        data: Optional[Union[BaseModel, Dict[str, Any]]] = None,
        params: Optional[Dict[str, Any]] = None,
        response_model: Optional[Type[T]] = None,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
    ) -> Union[httpx.Response, T, bool]:
        """
        Generic request method with retry mechanism and exponential backoff
        """
        headers = self._get_headers(access)
        clean_data = self._clean_payload(data)
        clean_params = self._clean_payload(params)
        full_url = f"{self.host}/{endpoint.lstrip('/')}"
        
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                # Add small delay between requests to avoid overwhelming server
                if attempt > 0:
                    delay = backoff_factor * (1.5 ** (attempt - 1))  # Gentler backoff
                    await asyncio.sleep(delay)
                    logger.info(f"Retrying request (attempt {attempt + 1}/{max_retries + 1}) after {delay:.1f}s delay")
                
                response = await self._client.request(
                    method,
                    full_url,
                    headers=headers,
                    data=clean_data if not access else None,
                    json=clean_data if access else None,
                    params=clean_params,
                )
                response.raise_for_status()

                if not response.content:
                    if response.status_code in [200, 201, 204]:
                        return True
                    return False

                if response_model:
                    return response_model(**response.json())

                jsonres = response.json()
                return jsonres if jsonres != {} else True

            except httpx.HTTPStatusError as e:
                last_exception = e
                # Retry on server errors (5xx) and rate limiting (429)
                if e.response.status_code in [429, 500, 502, 503, 504] and attempt < max_retries:
                    logger.warning(f"HTTP error {e.response.status_code}, retrying... (attempt {attempt + 1}/{max_retries + 1})")
                    continue
                else:
                    logger.error(f"HTTP error occurred: {str(e)}")
                    return False
            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
                last_exception = e
                if attempt < max_retries:
                    logger.warning(f"Network error, retrying... (attempt {attempt + 1}/{max_retries + 1}): {str(e)}")
                    continue
                else:
                    logger.error(f"Network error after {max_retries + 1} attempts: {str(e)}")
                    return False
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                return False
        
        logger.error(f"Request failed after {max_retries + 1} attempts. Last error: {str(last_exception)}")
        return False

    def _clean_payload(
        self, payload: Optional[Union[BaseModel, Dict[str, Any]]]
    ) -> Optional[Dict[str, Any]]:
        if payload is None:
            return None

        if isinstance(payload, BaseModel):
            data = payload.model_dump()
        else:
            data = payload

        def clean_nones_and_convert_datetime(obj: Any) -> Any:
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, dict):
                return {
                    key: clean_nones_and_convert_datetime(value)
                    for key, value in obj.items()
                    if value is not None
                }
            elif isinstance(obj, list):
                return [
                    clean_nones_and_convert_datetime(item)
                    for item in obj
                    if item is not None
                ]
            return obj

        return clean_nones_and_convert_datetime(data)

    async def close(self) -> None:
        """
        Close the HTTP client session
        """
        await self._client.aclose()

    async def get(
        self,
        endpoint: str,
        access: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        response_model: Optional[Type[T]] = None,
    ) -> Union[httpx.Response, T]:
        """
        Perform a GET request
        """
        return await self._request(
            "GET", endpoint, params=params, response_model=response_model, access=access
        )

    async def post(
        self,
        endpoint: str,
        access: Optional[str] = None,
        data: Optional[Union[BaseModel, Dict[str, Any]]] = None,
        params: Optional[Dict[str, Any]] = None,
        response_model: Optional[Type[T]] = None,
    ) -> Union[httpx.Response, T]:
        """
        Perform a POST request
        """
        return await self._request(
            "POST",
            endpoint,
            data=data,
            params=params,
            response_model=response_model,
            access=access,
        )

    async def put(
        self,
        endpoint: str,
        access: Optional[str] = None,
        data: Optional[Union[BaseModel, Dict[str, Any]]] = None,
        params: Optional[Dict[str, Any]] = None,
        response_model: Optional[Type[T]] = None,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
    ) -> Union[httpx.Response, T]:
        """
        Perform a PUT request with retry mechanism
        """
        return await self._request(
            "PUT",
            endpoint,
            data=data,
            response_model=response_model,
            params=params,
            access=access,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )

    async def delete(
        self,
        endpoint: str,
        access: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        response_model: Optional[Type[T]] = None,
    ) -> Union[httpx.Response, T]:
        """
        Perform a DELETE request
        """
        return await self._request(
            "DELETE",
            endpoint,
            params=params,
            response_model=response_model,
            access=access,
        )

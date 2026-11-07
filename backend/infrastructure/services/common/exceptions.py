"""
Enhanced exception handling with proper error types and structured logging.

This module provides comprehensive exception handling for async functions,
with improved error recovery and better type safety.
"""
import logging
from http import HTTPStatus
from typing import Callable, Any, Union, Optional, TypeVar, Type
from uuid import UUID
from functools import wraps

import httpx
from fastapi import HTTPException
from pydantic import BaseModel

from settings import logger

T = TypeVar('T')
ResponseModelType = TypeVar('ResponseModelType', bound=BaseModel)


class AppBaseError(Exception):
    """Base exception for application-specific errors."""
    pass


class ResourceNotFound(HTTPException):
    """Exception raised when a requested resource is not found."""

    def __init__(
        self,
        resource_id: Union[UUID, int, str],
        resource_name: str = "Resource"
    ) -> None:
        super().__init__(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"{resource_name} with ID '{resource_id}' was not found"
        )
        self.resource_id = resource_id
        self.resource_name = resource_name


class ResourceConflict(HTTPException):
    """Exception raised when a resource already exists."""

    def __init__(
        self,
        resource_name: str = "Resource",
        detail: Optional[str] = None
    ) -> None:
        super().__init__(
            status_code=HTTPStatus.CONFLICT,
            detail=detail or f"{resource_name} already exists"
        )
        self.resource_name = resource_name


class BadRequest(HTTPException):
    """Exception raised for malformed requests."""

    def __init__(
        self,
        detail: str = "Bad request",
        source: Optional[str] = None,
        relation: Optional[str] = None
    ) -> None:
        if source and relation:
            detail = f"Provided {source} id or {relation} id not found in database"

        super().__init__(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=detail
        )


class ServiceUnavailableError(HTTPException):
    """Exception raised when a service is temporarily unavailable."""

    def __init__(self, service_name: str = "Service") -> None:
        super().__init__(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail=f"{service_name} is unavailable. Please try again later."
        )


class AuthIsUnavailableError(ServiceUnavailableError):
    """Exception raised when authentication service is unavailable."""

    def __init__(self) -> None:
        super().__init__(service_name="Authorization service")


class ElasticsearchRepositoryError(AppBaseError):
    """Exception raised for Elasticsearch repository errors."""
    pass


class NetworkError(AppBaseError):
    """Exception raised for network-related errors."""

    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.original_error = original_error


class ValidationError(AppBaseError):
    """Exception raised for data validation errors."""

    def __init__(self, message: str, field: Optional[str] = None):
        super().__init__(message)
        self.field = field


async def generic_exception_handler(
    async_func: Optional[Callable[..., Any]] = None,
    sync_func: Optional[Callable[..., Any]] = None,
    response_model: Optional[Type[ResponseModelType]] = None,
    args: tuple = (),
    kwargs: Optional[dict] = None,
    raise_on_none: bool = True,
) -> Any:
    """
    Universal exception handler for async and sync functions.

    This function wraps business logic with comprehensive error handling,
    logging, and optional response model validation.

    Args:
        async_func: Async function to execute
        sync_func: Sync function to execute
        response_model: Pydantic model for response validation
        args: Positional arguments for the function
        kwargs: Keyword arguments for the function
        raise_on_none: Whether to raise HTTPException when result is None

    Returns:
        Function result, optionally validated against response_model

    Raises:
        HTTPException: For HTTP-related errors
        Various specific exceptions based on error type

    Example:
        result = await generic_exception_handler(
            async_func=repo.get_token,
            response_model=TokenSchema,
            args=(token_id,),
            raise_on_none=True
        )
    """
    if kwargs is None:
        kwargs = {}

    if async_func is None and sync_func is None:
        raise ValueError("Either async_func or sync_func must be provided")

    try:
        # Execute the function
        if async_func:
            result = await async_func(*args, **kwargs)
        else:
            result = sync_func(*args, **kwargs)

        # Handle None results
        if result is None:
            if raise_on_none:
                raise ResourceNotFound(
                    resource_id="unknown",
                    resource_name="Object"
                )
            return None

        # Validate and transform response if model provided
        if response_model:
            if isinstance(result, list):
                return [
                    response_model.model_validate(obj, from_attributes=True)
                    for obj in result
                ]
            return response_model.model_validate(result, from_attributes=True)

        return result

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise

    except httpx.HTTPStatusError as exc:
        logger.error(
            f"HTTP status error: {exc.response.status_code}",
            extra={
                "url": str(exc.request.url),
                "status_code": exc.response.status_code,
                "response": exc.response.text[:200] if exc.response else None
            },
            exc_info=True
        )
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"External service error: {exc.response.status_code}"
        )

    except httpx.RequestError as exc:
        logger.error(
            f"Network request error: {exc}",
            extra={
                "url": str(exc.request.url) if exc.request else None,
                "error_type": type(exc).__name__
            },
            exc_info=True
        )
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail="Network error or connection timeout"
        )

    except ValidationError as exc:
        logger.error(
            f"Validation error: {exc}",
            extra={"field": exc.field} if exc.field else {},
            exc_info=True
        )
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            detail=str(exc)
        )

    except Exception as exc:
        logger.error(
            f"Unexpected error: {exc}",
            extra={
                "function": async_func.__name__ if async_func else sync_func.__name__,
                "error_type": type(exc).__name__
            },
            exc_info=True
        )
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred"
        )


def handle_exceptions(
    response_model: Optional[Type[ResponseModelType]] = None,
    raise_on_none: bool = True
):
    """
    Decorator for automatic exception handling in async functions.

    Args:
        response_model: Optional Pydantic model for response validation
        raise_on_none: Whether to raise exception on None result

    Returns:
        Decorated function with exception handling

    Example:
        @handle_exceptions(response_model=TokenSchema)
        async def get_token(token_id: UUID):
            return await repo.get(token_id)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            return await generic_exception_handler(
                async_func=func,
                args=args,
                kwargs=kwargs,
                response_model=response_model,
                raise_on_none=raise_on_none
            )
        return wrapper
    return decorator

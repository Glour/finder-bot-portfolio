from fastapi import APIRouter, Depends

from infrastructure.database.requests import RequestsRepo, get_database_repo
from infrastructure.services.common.exceptions import generic_exception_handler

router = APIRouter()


@router.get(
    "/",
)
async def tokens_info(
        repo: RequestsRepo = Depends(get_database_repo),
):
    return await generic_exception_handler(
        async_func=repo.tokens.get_all,
    )

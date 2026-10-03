"""Documents router: CRUD, pagination, and text search."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.dependencies import CurrentUser
from nexus.api.ratelimit import RateLimitDep
from nexus.db import get_session
from nexus.models import Document
from nexus.schemas import DocumentCreate, DocumentList, DocumentRead

router = APIRouter(prefix="/docs", tags=["documents"], dependencies=[RateLimitDep])


@router.post("", response_model=DocumentRead, status_code=status.HTTP_201_CREATED)
async def create_document(
    payload: DocumentCreate,
    current_user: CurrentUser,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Document:
    existing = await session.scalar(
        select(Document).where(Document.url == payload.url)
    )
    if existing is not None:
        existing.title = payload.title
        existing.content_markdown = payload.content_markdown
        existing.text_len = payload.text_len
        existing.word_count = payload.word_count
        existing.reading_time_mins = payload.reading_time_mins
        existing.summary = payload.summary
        await session.flush()
        return existing

    doc = Document(
        url=payload.url,
        host=payload.host,
        title=payload.title,
        content_markdown=payload.content_markdown,
        text_len=payload.text_len,
        word_count=payload.word_count,
        reading_time_mins=payload.reading_time_mins,
        summary=payload.summary,
        status_code=payload.status_code,
        owner_id=current_user.id,
    )
    session.add(doc)
    await session.flush()
    await session.refresh(doc)
    return doc


@router.get("", response_model=DocumentList)
async def list_documents(
    session: Annotated[AsyncSession, Depends(get_session)],
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    host: str | None = Query(None),
    q: str | None = Query(None),
) -> DocumentList:
    stmt = select(Document)
    count_stmt = select(func.count()).select_from(Document)

    if host:
        stmt = stmt.where(Document.host == host)
        count_stmt = count_stmt.where(Document.host == host)

    if q:
        term = f"%{q}%"
        filter_cond = or_(
            Document.title.ilike(term),
            Document.content_markdown.ilike(term),
            Document.url.ilike(term),
        )
        stmt = stmt.where(filter_cond)
        count_stmt = count_stmt.where(filter_cond)

    total = (await session.scalar(count_stmt)) or 0
    items = (
        await session.scalars(
            stmt.order_by(Document.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).all()

    return DocumentList(
        items=list(items),
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/search", response_model=list[DocumentRead])
async def search_documents(
    session: Annotated[AsyncSession, Depends(get_session)],
    q: Annotated[str, Query(min_length=1)],
) -> list[Document]:
    term = f"%{q}%"
    stmt = (
        select(Document)
        .where(
            or_(
                Document.title.ilike(term),
                Document.content_markdown.ilike(term),
                Document.url.ilike(term),
            )
        )
        .limit(20)
    )
    results = (await session.scalars(stmt)).all()
    return list(results)


@router.get("/{doc_id}", response_model=DocumentRead)
async def get_document(
    doc_id: int,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Document:
    doc = await session.get(Document, doc_id)
    if doc is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Document not found")
    return doc


@router.delete("/{doc_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_document(
    doc_id: int,
    current_user: CurrentUser,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> None:
    doc = await session.get(Document, doc_id)
    if doc is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Document not found")
    if doc.owner_id != current_user.id and current_user.role != "admin":
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Permission denied")

    await session.delete(doc)

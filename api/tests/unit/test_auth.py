from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import AsyncClient

from icarus.services.auth_service import create_access_token, hash_password


def _mock_record(data: dict[str, object]) -> MagicMock:
    record = MagicMock()
    record.__getitem__ = lambda self, key: data[key]
    record.__contains__ = lambda self, key: key in data
    record.keys.return_value = list(data.keys())
    return record


def _fake_result(records: list[MagicMock]) -> AsyncMock:
    result = AsyncMock()

    async def _iter(self: object) -> object:  # noqa: ANN001
        for r in records:
            yield r

    result.__aiter__ = _iter
    result.single = AsyncMock(return_value=records[0] if records else None)
    return result


def _setup_mock_session(driver: MagicMock, records: list[MagicMock]) -> AsyncMock:
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=_fake_result(records))
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    return mock_session


@pytest.mark.anyio
async def test_register_success(client: AsyncClient) -> None:
    record = _mock_record({
        "id": "user-uuid",
        "email": "test@example.com",
        "created_at": "2026-01-01T00:00:00Z",
    })

    from icarus.main import app

    _setup_mock_session(app.state.neo4j_driver, [record])

    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "test@example.com", "password": "password123"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["email"] == "test@example.com"
    assert "id" in data


@pytest.mark.anyio
async def test_register_bad_invite(client: AsyncClient) -> None:
    from icarus.config import settings

    original = settings.invite_code
    try:
        settings.invite_code = "secret-code"
        response = await client.post(
            "/api/v1/auth/register",
            json={"email": "test@example.com", "password": "password123", "invite_code": "wrong"},
        )
        assert response.status_code == 403
    finally:
        settings.invite_code = original


@pytest.mark.anyio
async def test_login_success(client: AsyncClient) -> None:
    hashed = hash_password("password123")
    record = _mock_record({
        "id": "user-uuid",
        "email": "test@example.com",
        "password_hash": hashed,
        "created_at": "2026-01-01T00:00:00Z",
    })

    from icarus.main import app

    _setup_mock_session(app.state.neo4j_driver, [record])

    response = await client.post(
        "/api/v1/auth/login",
        data={"username": "test@example.com", "password": "password123"},
    )
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


@pytest.mark.anyio
async def test_login_bad_password(client: AsyncClient) -> None:
    hashed = hash_password("password123")
    record = _mock_record({
        "id": "user-uuid",
        "email": "test@example.com",
        "password_hash": hashed,
        "created_at": "2026-01-01T00:00:00Z",
    })

    from icarus.main import app

    _setup_mock_session(app.state.neo4j_driver, [record])

    response = await client.post(
        "/api/v1/auth/login",
        data={"username": "test@example.com", "password": "wrongpassword"},
    )
    assert response.status_code == 401


@pytest.mark.anyio
async def test_me_authenticated(client: AsyncClient) -> None:
    token = create_access_token("user-uuid")
    user_record = _mock_record({
        "id": "user-uuid",
        "email": "test@example.com",
        "created_at": "2026-01-01T00:00:00Z",
    })

    from icarus.main import app

    _setup_mock_session(app.state.neo4j_driver, [user_record])

    response = await client.get(
        "/api/v1/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "user-uuid"
    assert data["email"] == "test@example.com"


@pytest.mark.anyio
async def test_me_no_token(client: AsyncClient) -> None:
    response = await client.get("/api/v1/auth/me")
    assert response.status_code == 401


@pytest.mark.anyio
async def test_me_invalid_token(client: AsyncClient) -> None:
    response = await client.get(
        "/api/v1/auth/me",
        headers={"Authorization": "Bearer invalid-token"},
    )
    assert response.status_code == 401


@pytest.mark.anyio
async def test_register_duplicate_email(client: AsyncClient) -> None:
    from icarus.main import app

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(side_effect=Exception("Constraint violation"))
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    with pytest.raises(Exception, match="Constraint violation"):
        await client.post(
            "/api/v1/auth/register",
            json={"email": "duplicate@example.com", "password": "password123"},
        )

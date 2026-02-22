from slowapi import Limiter
from slowapi.util import get_remote_address
from starlette.requests import Request

from icarus.services.auth_service import decode_access_token


def _get_rate_limit_key(request: Request) -> str:
    """Extract user_id from JWT for rate limiting, fallback to IP."""
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:]
        user_id = decode_access_token(token)
        if user_id:
            return f"user:{user_id}"
    return get_remote_address(request)


limiter = Limiter(key_func=_get_rate_limit_key)

"""Separate customer serving API with audience- and identity-bound authorization."""
import hmac
import time

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, StrictBool
import psycopg2
from starlette.middleware.cors import CORSMiddleware

from .auth import GatewaySettings, Tokens, identifier
from .contracts import EVENT_NAMES
from .gateway import RateLimit
from .profiles import ProfileStore

BEARER = HTTPBearer(auto_error=False)


class Attributes(BaseModel):
    model_config = ConfigDict(extra='forbid')
    preferred_language: str | None = Field(default=None, pattern=r'^[a-z]{2,3}(-[A-Za-z0-9]{2,8})*$', max_length=35)
    marketing_consent: StrictBool | None = None


class Onboarding(Attributes):
    preferred_language: str = Field(pattern=r'^[a-z]{2,3}(-[A-Za-z0-9]{2,8})*$', max_length=35)


class Session(BaseModel):
    model_config = ConfigDict(extra='forbid')
    source_id: str
    user_id: str
    session_id: str


def profile_response(row, stale_seconds=300):
    processed = row.pop('processed_at', None)
    served = row['served_at']
    received = row.get('latest_received_at')
    age = max(0, (served - processed).total_seconds()) if processed else None
    row['event_counts'] = {name: (row.get('event_counts') or {}).get(name, 0) for name in EVENT_NAMES}
    row['completed_orders'] = row.get('completed_orders') or 0
    row['revision'] = row.get('revision') or 0
    row['marketing_eligible'] = row['onboarded_at'] is not None and row['marketing_consent'] is True
    row['freshness'] = dict(processed_at=processed, summary_age_seconds=age,
        receipt_to_processing_seconds=max(0, (processed - received).total_seconds()) if processed and received else None,
        status='never_processed' if processed is None else ('stale' if age > stale_seconds else 'recent'),
        stale_after_seconds=stale_seconds, scope='customer_summary_age_not_consumer_lag')
    return row


def create_app(settings=None, store=None, clock=time.time):
    settings = settings or GatewaySettings.from_environment()
    store = store or ProfileStore()
    tokens = Tokens(settings, clock, audience='journeypulse-customers')
    limiter = RateLimit(settings.requests_per_minute)
    app = FastAPI(title='JourneyPulse customer API', version='0.3.0')
    app.add_middleware(CORSMiddleware,
        allow_origins=sorted({o for s in settings.sources for o in s.origins}),
        allow_methods=['GET', 'POST', 'PATCH'], allow_headers=['Authorization', 'Content-Type'])

    @app.middleware('http')
    async def no_store(request, call_next):
        response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        return response

    def rate(source_id):
        if not limiter.allow(source_id):
            raise HTTPException(429, 'rate_limited', headers={'Retry-After': '1'})

    def authenticate(request: Request, credentials=Depends(BEARER)):
        try:
            if credentials is None or credentials.scheme.lower() != 'bearer':
                raise ValueError()
            claims = tokens.verify(credentials.credentials)
        except ValueError:
            raise HTTPException(401, 'invalid_customer_token') from None
        origin = request.headers.get('origin')
        if origin is not None and origin not in settings.source(claims['source_id']).origins:
            raise HTTPException(403, 'origin_not_allowed')
        rate(claims['source_id'])
        return claims

    def authorize(user_id, claims):
        if not identifier(user_id) or user_id != claims['user_id']:
            raise HTTPException(403, 'identity_mismatch')

    def database(action, *args, **kwargs):
        try:
            return action(*args, **kwargs)
        except psycopg2.Error:
            raise HTTPException(503, 'database_unavailable', headers={'Retry-After': '1'}) from None

    def read(user_id):
        row = database(store.get, user_id)
        if row is None:
            raise HTTPException(404, 'customer_not_found')
        return profile_response(row)

    def attributes(body):
        values = body.model_dump(exclude_unset=True)
        if not values or any(value is None for value in values.values()):
            raise HTTPException(422, 'non_null_attributes_required')
        return values

    @app.get('/health/live')
    def live():
        return {'status': 'alive'}

    @app.get('/v1/health/ready')
    def ready():
        if not database(store.ready):
            raise HTTPException(503, 'migration_missing')
        return {'status': 'ready'}

    @app.post('/v1/sessions')
    def session(body: Session, request: Request):
        if request.headers.get('origin') is not None:
            raise HTTPException(403, 'server_only_session_issuance')
        source = settings.source(body.source_id)
        key = request.headers.get('x-source-key', '')
        if source is None or not hmac.compare_digest(key.encode(), source.server_key.encode()):
            raise HTTPException(401, 'invalid_source_key')
        if not all(identifier(v) for v in (body.user_id, body.session_id)):
            raise HTTPException(422, 'invalid_identity')
        rate(source.source_id)
        token, expiry = tokens.issue(source.source_id, body.user_id, body.session_id)
        return dict(access_token=token, expires_at=expiry, expires_in=settings.token_ttl_seconds,
                    user_id=body.user_id, session_id=body.session_id, source_id=source.source_id)

    @app.post('/v1/customers')
    def onboard(body: Onboarding, claims=Depends(authenticate)):
        database(store.update, claims['user_id'], attributes(body), onboarding=True)
        return read(claims['user_id'])

    @app.get('/v1/customers/{user_id}')
    def get(user_id: str, claims=Depends(authenticate)):
        authorize(user_id, claims)
        return read(user_id)

    @app.patch('/v1/customers/{user_id}')
    def update(user_id: str, body: Attributes, claims=Depends(authenticate)):
        authorize(user_id, claims)
        if not database(store.update, user_id, attributes(body)):
            raise HTTPException(404, 'customer_not_found')
        return read(user_id)

    return app

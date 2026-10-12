"""JourneyPulse authenticated, bounded event ingestion API."""
import asyncio
import base64
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import hmac
import json
import time
from uuid import uuid4

from jsonschema import Draft202012Validator

from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.responses import JSONResponse, Response
from fastapi.security import APIKeyHeader, HTTPBearer
from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.middleware.cors import CORSMiddleware

from .auth import GatewaySettings, Tokens, identifier
from .config import EVENT_TOPIC, QUARANTINE_TOPIC
from .contracts import EVENT_NAMES, FORMAT_CHECKER, decode_json, event_schema, load_schema, validation_errors
from .publisher import KafkaPublisher, Record

BEARER = HTTPBearer(auto_error=False)
SOURCE_KEY = APIKeyHeader(name='X-Source-Key', auto_error=False)


class RateLimit:
    """Fixed source-key cardinality; budgets are local to this gateway process."""
    def __init__(self, limit, clock=time.monotonic):
        self.limit, self.clock, self.history = limit, clock, {}

    def allow(self, source_id):
        now = self.clock()
        history = self.history.setdefault(source_id, deque())
        while history and history[0] <= now - 60:
            history.popleft()
        if len(history) >= self.limit:
            return False
        history.append(now)
        return True


def create_app(settings=None, publisher=None, clock=time.time):
    settings = settings or GatewaySettings.from_environment()
    publisher = publisher or KafkaPublisher()
    tokens = Tokens(settings, clock)
    validator = Draft202012Validator(load_schema(), format_checker=FORMAT_CHECKER)
    limiter = RateLimit(settings.requests_per_minute)
    inflight = asyncio.Semaphore(settings.max_inflight_requests)
    registry = CollectorRegistry()
    deliveries = Counter('journeypulse_gateway_events', 'Per-event ingestion outcomes',
                         ['status'], registry=registry)
    rejections = Counter('journeypulse_gateway_rejections', 'Bounded rejection categories',
                         ['reason'], registry=registry)
    kafka_failures = Counter('journeypulse_gateway_kafka_failures', 'Unacknowledged publish attempts',
                            ['channel'], registry=registry)
    latency = Histogram('journeypulse_gateway_request_seconds', 'Gateway endpoint response latency',
                        ['route', 'status'], registry=registry)

    @asynccontextmanager
    async def lifespan(app):
        yield
        await asyncio.to_thread(publisher.close)

    app = FastAPI(title='JourneyPulse event gateway', version='0.2.0', lifespan=lifespan)
    app.state.publisher = publisher
    app.state.registry = registry
    app.add_middleware(CORSMiddleware,
        allow_origins=sorted({origin for s in settings.sources for origin in s.origins}),
        allow_methods=['POST', 'GET'], allow_headers=['Authorization', 'Content-Type'],
        allow_credentials=False, expose_headers=['Retry-After'])

    @app.middleware('http')
    async def observe(request, call_next):
        start = time.monotonic()
        response = await call_next(request)
        route = request.scope.get('route')
        name = getattr(route, 'path', 'unmatched')
        latency.labels(name, str(response.status_code)).observe(time.monotonic() - start)
        if request.url.path.startswith('/v1/'):
            response.headers['Cache-Control'] = 'no-store'
        return response

    def failure(status, code, retryable=False, **extra):
        return HTTPException(status, detail=dict(code=code, retryable=retryable, **extra))

    def rate_limit(source_id):
        if not limiter.allow(source_id):
            raise HTTPException(429, detail={'code': 'rate_limited', 'retryable': True},
                                headers={'Retry-After': '1'})

    def authenticate(request, credentials):
        if credentials is None or credentials.scheme.lower() != 'bearer':
            raise failure(401, 'invalid_session_token')
        try:
            claims = tokens.verify(credentials.credentials)
        except ValueError:
            raise failure(401, 'invalid_session_token') from None
        origin = request.headers.get('origin')
        if origin is not None and origin not in settings.source(claims['source_id']).origins:
            raise failure(403, 'origin_not_allowed')
        return claims

    async def body(request):
        if request.headers.get('content-type', '').split(';', 1)[0].strip().lower() != 'application/json':
            raise failure(415, 'json_content_type_required')
        if request.headers.get('content-encoding', 'identity').lower() != 'identity':
            raise failure(415, 'content_encoding_not_supported')
        length = request.headers.get('content-length')
        if length:
            try:
                if int(length) < 0:
                    raise ValueError()
                if int(length) > settings.max_body_bytes:
                    raise failure(413, 'body_too_large')
            except ValueError:
                raise failure(400, 'invalid_content_length') from None
        chunks, size = [], 0
        async def read():
            nonlocal size
            async for chunk in request.stream():
                size += len(chunk)
                if size > settings.max_body_bytes:
                    raise failure(413, 'body_too_large')
                chunks.append(chunk)
            return b''.join(chunks)
        try:
            return await asyncio.wait_for(read(), timeout=settings.request_timeout_seconds)
        except asyncio.TimeoutError:
            raise failure(408, 'request_body_timeout', retryable=True) from None

    async def publish(records, timeout):
        if timeout <= 0:
            return [None] * len(records)
        try:
            return await asyncio.wait_for(asyncio.to_thread(
                publisher.publish_many, records, min(timeout, settings.publish_timeout_seconds)),
                timeout=timeout)
        except Exception:
            return [None] * len(records)

    def quarantine(raw, claims, request_id, received_at, index, event_id, errors, reason):
        return Record(QUARANTINE_TOPIC, claims['user_id'],
            {'transport_version': 1, 'request_id': request_id,
             'source_id': claims['source_id'], 'user_id': claims['user_id'],
             'session_id': claims['session_id'], 'received_at': received_at,
             'batch_index': index, 'event_id': event_id, 'reason': reason,
             'errors': errors, 'raw_request_base64': base64.b64encode(raw).decode('ascii')})

    @app.get('/health/live')
    def live():
        return {'status': 'alive'}

    @app.get('/v1/health/ready')
    async def ready():
        try:
            ok = await asyncio.wait_for(asyncio.to_thread(publisher.ready), timeout=3)
        except asyncio.TimeoutError:
            ok = False
        return JSONResponse({'status': 'ready' if ok else 'unavailable'}, status_code=200 if ok else 503)

    @app.get('/metrics', include_in_schema=False)
    def metrics():
        return Response(generate_latest(registry), headers={'Content-Type': CONTENT_TYPE_LATEST})

    session_schema = {'type': 'object', 'additionalProperties': False,
        'required': ['source_id', 'user_id', 'session_id'],
        'properties': {key: {'type': 'string'} for key in ('source_id', 'user_id', 'session_id')}}
    @app.post('/v1/sessions', openapi_extra={'requestBody': {'required': True,
        'content': {'application/json': {'schema': session_schema}}}})
    async def session(request: Request, source_key: str | None = Security(SOURCE_KEY)):
        # Issuance is server-to-server. Browser code obtains a token from its own backend.
        if request.headers.get('origin') is not None:
            raise failure(403, 'session_issuance_server_only')
        raw = await body(request)
        try:
            data = decode_json(raw.decode('utf-8'))
        except (ValueError, UnicodeError, RecursionError):
            raise failure(400, 'malformed_json') from None
        if (not isinstance(data, dict) or set(data) != {'source_id', 'user_id', 'session_id'}
                or not all(identifier(value) for value in data.values())):
            raise failure(422, 'invalid_session_request')
        source = settings.source(data['source_id'])
        if source is None or source_key is None or not hmac.compare_digest(source.server_key.encode('utf-8'), source_key.encode('utf-8')):
            raise failure(401, 'invalid_source_credentials')
        rate_limit(source.source_id)
        token, expires_at = tokens.issue(**data)
        return {'access_token': token, 'token_type': 'bearer', 'expires_at': expires_at,
                'expires_in': settings.token_ttl_seconds, **data}

    @app.get('/v1/contracts')
    def contracts(request: Request, credentials=Depends(BEARER)):
        authenticate(request, credentials)
        return {'schema_version': 1, 'contracts': {name: event_schema(name) for name in EVENT_NAMES}}

    batch_schema = {'type': 'object', 'additionalProperties': False, 'required': ['events'],
                    'properties': {'events': {'type': 'array', 'minItems': 1,
                        'maxItems': settings.max_batch_size, 'items': {'type': 'object'}}}}
    @app.post('/v1/events', openapi_extra={'requestBody': {'required': True,
        'content': {'application/json': {'schema': batch_schema}}}})
    async def events(request: Request, credentials=Depends(BEARER)):
        claims = authenticate(request, credentials)
        rate_limit(claims['source_id'])
        if inflight.locked():
            raise HTTPException(429, detail={'code': 'gateway_busy', 'retryable': True},
                                headers={'Retry-After': '1'})
        async with inflight:
            deadline = time.monotonic() + settings.request_timeout_seconds
            raw = await body(request)
            request_id = str(uuid4())
            received_at = datetime.fromtimestamp(clock(), timezone.utc).isoformat().replace('+00:00', 'Z')
            invalid_reason, data = None, None
            try:
                data = decode_json(raw.decode('utf-8'))
            except (ValueError, UnicodeError, RecursionError):
                invalid_reason = 'malformed_json'
            if invalid_reason is None and (not isinstance(data, dict) or set(data) != {'events'}
                    or not isinstance(data['events'], list) or not 1 <= len(data['events']) <= settings.max_batch_size):
                invalid_reason = 'invalid_batch'
            if invalid_reason:
                rejections.labels(invalid_reason).inc()
                errors = [{'path': '$', 'rule': invalid_reason,
                           'message': 'Expected a strict JSON object with 1..%d events.' % settings.max_batch_size}]
                record = quarantine(raw, claims, request_id, received_at, None, None, errors, invalid_reason)
                ack = (await publish([record], deadline - time.monotonic()))[0]
                if ack is None:
                    kafka_failures.labels('quarantine').inc()
                return JSONResponse({'detail': {'code': invalid_reason if ack else 'quarantine_unavailable',
                    'retryable': ack is None, 'request_id': request_id, 'errors': errors,
                    'quarantine_persisted': ack is not None}},
                    status_code=(400 if invalid_reason == 'malformed_json' else 422) if ack else 503)

            results, records = [], []
            for index, event in enumerate(data['events']):
                errors = [{**error, 'path': error['path'][:200], 'message': error['message'][:256]}
                          for error in validation_errors(event, validator)[:16]]
                reason = 'schema'
                event_id = event.get('event_id') if isinstance(event, dict) and identifier(event.get('event_id')) else None
                if not errors:
                    if event['user_id'] != claims['user_id'] or event['session_id'] != claims['session_id']:
                        reason = 'identity'
                        errors = [{'path': '$', 'rule': reason, 'message': 'Event identity does not match the session token.'}]
                    else:
                        try:
                            occurred = datetime.fromisoformat(event['occurred_at'].upper().replace('Z', '+00:00'))
                            if occurred.timestamp() > clock() + settings.future_skew_seconds:
                                reason = 'future_timestamp'
                                errors = [{'path': '$.occurred_at', 'rule': reason,
                                           'message': 'Event exceeds the allowed future clock skew.'}]
                        except (ValueError, OverflowError, OSError):
                            reason = 'timestamp_range'
                            errors = [{'path': '$.occurred_at', 'rule': reason, 'message': 'Unsupported event timestamp range.'}]
                result = {'index': index, 'event_id': event_id, 'status': 'rejected' if errors else 'retryable',
                          'errors': errors}
                if errors:
                    rejections.labels(reason).inc()
                    record = quarantine(raw, claims, request_id, received_at, index, event_id, errors, reason)
                else:
                    record = Record(EVENT_TOPIC, claims['user_id'],
                        {'transport_version': 1, 'request_id': request_id,
                         'source_id': claims['source_id'], 'received_at': received_at, 'event': event})
                results.append(result)
                records.append(record)

            acknowledgements = await publish(records, deadline - time.monotonic())
            for result, record, ack in zip(results, records, acknowledgements):
                if record.topic == QUARANTINE_TOPIC:
                    result['quarantine_persisted'] = ack is not None
                elif ack is not None:
                    result['status'] = 'accepted'
                    result['delivery'] = ack
                if ack is None:
                    kafka_failures.labels('quarantine' if record.topic == QUARANTINE_TOPIC else 'events').inc()
                    if result['status'] == 'retryable':
                        result['errors'] = [{'path': '$', 'rule': 'kafka_unavailable',
                                            'message': 'Delivery is unacknowledged; retry with the same event ID.'}]
                deliveries.labels(result['status']).inc()
            counts = {status: sum(r['status'] == status for r in results)
                      for status in ('accepted', 'rejected', 'retryable')}
            status = 200 if counts['accepted'] == len(results) else (
                503 if counts['retryable'] == len(results) else 207)
            return JSONResponse({'request_id': request_id, 'results': results, 'counts': counts},
                                status_code=status, headers={'Retry-After': '1'} if counts['retryable'] else {})
    return app

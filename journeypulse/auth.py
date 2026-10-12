"""Source registrations and short-lived, identity-bound HMAC session tokens."""
import base64
from dataclasses import dataclass, field
import hashlib
import hmac
import json
import os
import re
import time
from urllib.parse import urlsplit

IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,99}')
DEFAULT_SOURCE_KEY = 'journeypulse-local-source-key'
DEFAULT_SIGNING_KEY = 'journeypulse-local-signing-key-change-this'


def identifier(value):
    return isinstance(value, str) and IDENTIFIER.fullmatch(value) is not None


@dataclass(frozen=True)
class Source:
    source_id: str
    server_key: str = field(repr=False)
    origins: tuple[str, ...] = ()


@dataclass(frozen=True)
class GatewaySettings:
    sources: tuple[Source, ...]
    signing_key: str = field(repr=False)
    token_ttl_seconds: int = 900
    max_batch_size: int = 50
    max_body_bytes: int = 262144
    future_skew_seconds: int = 60
    request_timeout_seconds: float = 10
    publish_timeout_seconds: float = 5
    requests_per_minute: int = 120
    max_inflight_requests: int = 16

    def __post_init__(self):
        if not self.sources or len({s.source_id for s in self.sources}) != len(self.sources):
            raise ValueError('Source registrations must be nonempty and unique')
        if len(self.signing_key.encode()) < 32:
            raise ValueError('Session signing key must have at least 32 bytes')
        for source in self.sources:
            if not identifier(source.source_id) or len(source.server_key.encode()) < 16:
                raise ValueError('Invalid source registration or server key')
            for origin in source.origins:
                parts = urlsplit(origin)
                if (parts.scheme not in ('http', 'https') or not parts.netloc
                        or parts.path or parts.query or parts.fragment or parts.username
                        or parts.password or '*' in origin):
                    raise ValueError('Source origins must be explicit HTTP(S) origins')
        limits = (self.token_ttl_seconds, self.max_batch_size, self.max_body_bytes,
                  self.requests_per_minute, self.max_inflight_requests)
        if any(type(value) is not int or value <= 0 for value in limits):
            raise ValueError('Gateway limits must be positive integers')
        if not 0 <= self.future_skew_seconds <= 300:
            raise ValueError('Future clock skew must be in 0..300 seconds')
        if (not 0 < self.publish_timeout_seconds <= self.request_timeout_seconds <= 30
                or self.token_ttl_seconds > 3600 or self.max_batch_size > 100
                or self.max_body_bytes > 262144):
            raise ValueError('Gateway limits exceed supported bounds')

    @classmethod
    def from_environment(cls):
        registrations = os.getenv('JOURNEYPULSE_SOURCES_JSON')
        if registrations:
            raw = json.loads(registrations)
            if not isinstance(raw, list):
                raise ValueError('JOURNEYPULSE_SOURCES_JSON must be a registration array')
            sources = tuple(Source(item['source_id'], item['server_key'],
                                   tuple(item.get('origins', []))) for item in raw)
        else:
            sources = (Source('demo-app', os.getenv('JOURNEYPULSE_SOURCE_KEY', DEFAULT_SOURCE_KEY),
                              ('http://localhost:5173', 'http://127.0.0.1:5173')),)
        return cls(sources, os.getenv('JOURNEYPULSE_SIGNING_KEY', DEFAULT_SIGNING_KEY))

    def source(self, source_id):
        return next((s for s in self.sources if s.source_id == source_id), None)


def _encode(raw):
    return base64.urlsafe_b64encode(raw).rstrip(b'=').decode('ascii')


class Tokens:
    def __init__(self, settings, clock=time.time, audience='journeypulse-events'):
        self.settings = settings
        self.clock = clock
        self.audience = audience

    def issue(self, source_id, user_id, session_id):
        now = int(self.clock())
        claims = dict(v=1, aud=self.audience, source_id=source_id,
                      user_id=user_id, session_id=session_id,
                      iat=now, exp=now + self.settings.token_ttl_seconds)
        payload = _encode(json.dumps(claims, separators=(',', ':'), sort_keys=True).encode())
        signature = _encode(hmac.new(self.settings.signing_key.encode(),
                                     payload.encode(), hashlib.sha256).digest())
        return payload + '.' + signature, claims['exp']

    def verify(self, token):
        try:
            if not isinstance(token, str) or len(token) > 2048:
                raise ValueError()
            payload, signature = token.split('.')
            expected = _encode(hmac.new(self.settings.signing_key.encode(),
                                        payload.encode(), hashlib.sha256).digest())
            if not hmac.compare_digest(signature, expected):
                raise ValueError()
            claims = json.loads(base64.urlsafe_b64decode(payload + '=' * (-len(payload) % 4)))
            now = self.clock()
            if (not isinstance(claims, dict) or claims.get('v') != 1
                    or claims.get('aud') != self.audience
                    or type(claims.get('exp')) is not int or type(claims.get('iat')) is not int
                    or not claims['iat'] <= now < claims['exp']
                    or claims['exp'] - claims['iat'] > self.settings.token_ttl_seconds
                    or not all(identifier(claims.get(key)) for key in ('source_id', 'user_id', 'session_id'))
                    or self.settings.source(claims['source_id']) is None):
                raise ValueError()
            return claims
        except (ValueError, TypeError, KeyError, UnicodeError):
            raise ValueError('Invalid or expired session token') from None

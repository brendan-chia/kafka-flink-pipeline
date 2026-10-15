"""Versioned behavior rules and PostgreSQL-clock evaluation, independent of consent."""
from dataclasses import dataclass
from datetime import timedelta
import hashlib
import json
import os

from psycopg2.extras import Json

SEGMENTS = ('active_browser', 'abandoned_cart', 'repeat_customer')


@dataclass(frozen=True)
class SegmentSettings:
    window_seconds: int = 600
    abandon_seconds: int = 600
    poll_seconds: int = 5
    stale_seconds: int = 30
    batch_size: int = 100

    def __post_init__(self):
        if any(type(v) is not int or v < 1 for v in self.__dict__.values()):
            raise ValueError('Segment settings must be positive integers')
        if self.stale_seconds < self.poll_seconds * 2:
            raise ValueError('Segment stale threshold must be at least twice the poll interval')

    @classmethod
    def from_environment(cls):
        return cls(**{name: int(os.getenv('JOURNEYPULSE_SEGMENT_' + name.upper(), str(default)))
                      for name, default in cls().__dict__.items()})

    @property
    def version(self):
        if (self.window_seconds, self.abandon_seconds) == (600, 600):
            return '1'
        digest = hashlib.sha256(json.dumps(self.definitions, sort_keys=True).encode()).hexdigest()[:16]
        return '1-' + digest

    @property
    def definitions(self):
        return {
            'active_browser': dict(minimum_events=5, window_seconds=self.window_seconds,
                interval='(now-window,now]', event='restaurant_view', clock='postgres_utc'),
            'abandoned_cart': dict(delay_seconds=self.abandon_seconds, event='add_to_cart',
                deadline='maximum_occurrence_plus_delay', completion='durable_matching_cart_order',
                checkout='does_not_resolve_or_postpone', clock='postgres_utc'),
            'repeat_customer': dict(minimum_distinct_orders=3, identity='customer_order', retention='durable'),
        }


def register_rules(cursor, settings, create=False):
    for name, definition in settings.definitions.items():
        if create:
            cursor.execute('''INSERT INTO journeypulse.segment_definitions
                (segment,rule_version,definition) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING''',
                (name, settings.version, Json(definition)))
        cursor.execute('''SELECT definition FROM journeypulse.segment_definitions
            WHERE segment=%s AND rule_version=%s''', (name, settings.version))
        row = cursor.fetchone()
        if row is None or row[0] != definition:
            raise ValueError('Missing or changed immutable segment rule: ' + name + ' ' + settings.version)


def evaluate(now, views, carts, orders, settings):
    """Pure rule evaluation; callers serialize customer state before reading inputs."""
    lower = now - timedelta(seconds=settings.window_seconds)
    current = sorted(t for t in views if lower < t <= now)
    abandoned = sum(1 for cart in carts if cart['completed_at'] is None and
                    cart['latest_add_at'] is not None and
                    cart['latest_add_at'] + timedelta(seconds=settings.abandon_seconds) <= now)
    deadlines = [now + timedelta(seconds=settings.poll_seconds)]
    deadlines += [t + timedelta(seconds=settings.window_seconds) for t in current]
    deadlines += [t for t in views if t > now]
    deadlines += [c['latest_add_at'] + timedelta(seconds=settings.abandon_seconds) for c in carts
                  if c['completed_at'] is None and c['latest_add_at'] is not None and
                  c['latest_add_at'] + timedelta(seconds=settings.abandon_seconds) > now]
    # Fifth newest view determines when this membership must expire without new input.
    valid_until = current[-5] + timedelta(seconds=settings.window_seconds) if len(current) >= 5 else None
    return {
        'active_browser': dict(member=len(current) >= 5, reason='restaurant_views_in_window',
                               evidence=dict(count=len(current), required=5), valid_until=valid_until),
        'abandoned_cart': dict(member=abandoned > 0, reason='uncompleted_carts_past_deadline',
                               evidence=dict(count=abandoned), valid_until=None),
        'repeat_customer': dict(member=orders >= 3, reason='distinct_completed_orders',
                                evidence=dict(count=orders, required=3), valid_until=None),
    }, min(deadlines)


def record_behavior(cursor, user, event, occurred):
    name, props = event['event_name'], event['properties']
    if name == 'restaurant_view':
        cursor.execute('''INSERT INTO journeypulse.segment_views(user_id,event_id,occurred_at)
            VALUES (%s,%s,%s) ON CONFLICT DO NOTHING''', (user, event['event_id'], occurred))
    elif name in ('add_to_cart', 'checkout_started'):
        field = 'latest_add_at' if name == 'add_to_cart' else 'latest_checkout_at'
        cursor.execute(f'''INSERT INTO journeypulse.carts(user_id,cart_id,{field}) VALUES (%s,%s,%s)
            ON CONFLICT (user_id,cart_id) DO UPDATE SET
            {field}=GREATEST(journeypulse.carts.{field},EXCLUDED.{field})''',
            (user, props['cart_id'], occurred))
    elif name == 'order_completed':
        # An existing order retains its first cart facts, just as in Phase 3.
        cursor.execute('''INSERT INTO journeypulse.carts(user_id,cart_id,completed_at)
            SELECT user_id,cart_id,occurred_at FROM journeypulse.completed_orders
            WHERE user_id=%s AND order_id=%s
            ON CONFLICT (user_id,cart_id) DO UPDATE SET
            completed_at=LEAST(journeypulse.carts.completed_at,EXCLUDED.completed_at)''',
            (user, props['order_id']))


def evaluate_customer(cursor, user, settings):
    # Database clock is authoritative across processor, scheduler and serving hosts.
    cursor.execute('SELECT clock_timestamp()')
    now = cursor.fetchone()[0]
    cursor.execute('''DELETE FROM journeypulse.segment_views
        WHERE user_id=%s AND occurred_at <= %s''',
        (user, now - timedelta(seconds=settings.window_seconds)))
    cursor.execute('SELECT occurred_at FROM journeypulse.segment_views WHERE user_id=%s', (user,))
    views = [r[0] for r in cursor.fetchall()]
    cursor.execute('SELECT latest_add_at,completed_at FROM journeypulse.carts WHERE user_id=%s', (user,))
    carts = [dict(latest_add_at=r[0], completed_at=r[1]) for r in cursor.fetchall()]
    cursor.execute('SELECT completed_orders FROM journeypulse.behavioral_summaries WHERE user_id=%s', (user,))
    results, due = evaluate(now, views, carts, cursor.fetchone()[0], settings)
    for name, result in results.items():
        cursor.execute('''INSERT INTO journeypulse.segment_memberships
            (user_id,segment,rule_version,member,reason,evidence,valid_until,evaluated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (user_id,segment,rule_version) DO UPDATE SET
            member=EXCLUDED.member,reason=EXCLUDED.reason,evidence=EXCLUDED.evidence,
            valid_until=EXCLUDED.valid_until,evaluated_at=EXCLUDED.evaluated_at''',
            (user, name, settings.version, result['member'], result['reason'],
             Json(result['evidence']), result['valid_until'], now))
    cursor.execute('''INSERT INTO journeypulse.segment_evaluations
        (user_id,rule_version,evaluated_at,next_evaluation_at) VALUES (%s,%s,%s,%s)
        ON CONFLICT (user_id) DO UPDATE SET rule_version=EXCLUDED.rule_version,
        evaluated_at=EXCLUDED.evaluated_at,next_evaluation_at=EXCLUDED.next_evaluation_at''',
        (user, settings.version, now, due))
    return results


def targeting_response(row, memberships, settings):
    now, evaluated = row['served_at'], row.get('evaluated_at')
    age = max(0, (now - evaluated).total_seconds()) if evaluated else None
    complete = {m['segment'] for m in memberships} == set(SEGMENTS)
    status = ('unevaluated' if evaluated is None or row.get('rule_version') != settings.version or not complete
              else 'stale' if age > settings.stale_seconds else 'current')
    consent = row['marketing_consent'] is True and row['onboarded_at'] is not None
    by_name = {m['segment']: m for m in memberships}
    results = {}
    for name in SEGMENTS:
        m = by_name.get(name, {})
        member = m.get('member') if status != 'unevaluated' else None
        expired = m.get('valid_until') is not None and m['valid_until'] <= now
        if expired and status != 'unevaluated':
            member = False
        reasons = []
        if not consent:
            reasons.append('marketing_consent_required')
        if status != 'current':
            reasons.append('behavior_' + status)
        if member is not True:
            reasons.append('behavior_not_matched' if member is False else 'behavior_unevaluated')
        if expired:
            reasons.append('active_browser_window_expired')
        if not reasons:
            reasons.append(m['reason'])
        results[name] = dict(member=member, eligible=consent and status == 'current' and member is True,
            reasons=reasons, behavior_reason='active_browser_window_expired' if expired else m.get('reason'),
            evidence=m.get('evidence'),
            rule_version=settings.version, evaluated_at=m.get('evaluated_at'), valid_until=m.get('valid_until'))
    return dict(user_id=row['user_id'], eligible=any(r['eligible'] for r in results.values()),
        marketing_consent=row['marketing_consent'], consent_updated_at=row.get('consent_updated_at'),
        consent_status='granted' if consent else 'missing_or_revoked', segments=results,
        rule_version=settings.version, evaluated_at=evaluated, served_at=now,
        freshness=dict(status=status, evaluation_age_seconds=age,
            stale_after_seconds=settings.stale_seconds, next_evaluation_at=row.get('next_evaluation_at'),
            scope='segment_evaluation_age_not_consumer_lag',
            behavior_processed_at=row.get('processed_at')))

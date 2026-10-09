"""Deterministic evidence service; no model-generated queries or database mutations."""
from contextlib import contextmanager
import os
from uuid import uuid4

from psycopg2.extras import RealDictCursor
from datalens import evidence
from datalens.models import Catalogue, DependencyResult, Investigation, PaymentFreshness, QualityPage


class UnknownMetric(LookupError):
    pass


def api_connection_params():
    params = evidence.connection_params()
    for key, env in (('user','DATALENS_POSTGRES_USER'), ('password','DATALENS_POSTGRES_PASS')):
        if env in os.environ:
            params[key] = os.environ[env]
    return params


class EvidenceService:
    def __init__(self, params=None):
        self.params = api_connection_params() if params is None else params

    @contextmanager
    def snapshot(self):
        conn = evidence.connect(self.params, readonly=True)
        try:
            yield conn
        finally:
            conn.close()

    def catalogue(self, conn):
        result = {}
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for table in ('datasets','metrics','dependencies'):
                # Names are internal constants, never request input.
                cur.execute('SELECT * FROM datalens.' + table + ' ORDER BY 1 LIMIT 1001')
                rows = [dict(row) for row in cur.fetchall()]
                if len(rows) > 1000:
                    raise ValueError('Catalogue exceeds the supported 1000 rows per collection')
                result[table] = rows
        return Catalogue.model_validate(result)

    def quality(self, conn, start, end, limit, dataset=None, check_name=None, datasets=None):
        # Range observations overlap; point-in-time freshness lies in [start,end).
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute('''SELECT * FROM datalens.quality_results
                WHERE ((range_start < %s AND range_end > %s)
                    OR (range_start = range_end AND range_start >= %s AND range_start < %s))
                  AND (%s::text IS NULL OR dataset = %s)
                  AND (%s::text IS NULL OR check_name = %s)
                  AND (%s::text[] IS NULL OR dataset = ANY(%s))
                ORDER BY observed_at DESC, result_id LIMIT %s''',
                (end,start,start,end,dataset,dataset,check_name,check_name,datasets,datasets,limit+1))
            rows = [dict(row) for row in cur.fetchall()]
        return QualityPage(results=rows[:limit], truncated=len(rows)>limit, limit=limit)

    def freshness(self, conn):
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(evidence.FRESHNESS_SQL)
            return PaymentFreshness.model_validate(dict(cur.fetchone()))

    def compare(self, conn, request):
        return evidence.compare_revenue(self.params, request.from_utc, request.until_utc,
            request.currency, request.window_seconds, connection=conn)

    @staticmethod
    def dependencies(catalogue, node, direction):
        known = {d.name for d in catalogue.datasets} | {'metric:' + m.name for m in catalogue.metrics}
        known |= {e.upstream for e in catalogue.dependencies} | {e.downstream for e in catalogue.dependencies}
        if node not in known:
            raise UnknownMetric('Unknown dependency node')
        reached, pending, edges = set(), [node], {}
        while pending:
            current = pending.pop()
            if current in reached:
                continue
            reached.add(current)
            for edge in catalogue.dependencies:
                origin, target = ((edge.downstream, edge.upstream) if direction == 'upstream'
                                  else (edge.upstream, edge.downstream))
                if origin == current:
                    edges[(edge.upstream,edge.downstream)] = edge
                    pending.append(target)
        return DependencyResult(node=node,direction=direction,
            edges=[edges[key] for key in sorted(edges)])

    def investigate(self, request):
        start, end = evidence.validate_range(request.from_utc, request.until_utc,
                                            request.currency, request.window_seconds)
        with self.snapshot() as conn:
            catalogue = self.catalogue(conn)
            definition = next((m for m in catalogue.metrics if m.name == request.metric), None)
            if definition is None or definition.source_dataset != 'payment_revenue_windows':
                raise UnknownMetric('Metric definition is missing from the catalogue')
            dependencies = self.dependencies(catalogue,'metric:' + request.metric,'upstream')
            comparison = self.compare(conn,request)
            freshness = self.freshness(conn)
            quality = self.quality(conn,start,end,request.quality_limit,
                datasets=['user-events','processed_events','payment_revenue_windows'])
        # Summarize verified discrepancies only. Do not infer causes from correlated observations.
        findings, actions = [], []
        status = {'match':'no_discrepancy_found','mismatch':'discrepancy','no_data':'insufficient_evidence'}[comparison['status']]
        counts = {}
        for index, row in enumerate(comparison['rows']):
            counts.setdefault(row['finding'],[]).append('comparison.rows.' + str(index))
        for code, refs in sorted(counts.items()):
            if code == 'match':
                continue
            findings.append(dict(code=code,severity='warning',
                message=str(len(refs)) + ' window(s) have finding: ' + code + '.',evidence_refs=refs))
        if status == 'no_discrepancy_found':
            findings.append(dict(code='audit_agreement',severity='info',
                message='Stored windows agree with the current valid audit; source completeness remains unverified.',
                evidence_refs=['comparison']))
        if status == 'insufficient_evidence':
            findings.append(dict(code='no_data',severity='warning',
                message='Neither payment audit groups nor stored revenue groups were found in the requested scope.',
                evidence_refs=['comparison']))
        if quality.truncated:
            findings.append(dict(code='quality_sample_truncated',severity='info',
                message='Historical quality evidence exceeds the requested limit; only the latest overlapping observations are included.',
                evidence_refs=['historical_quality']))
        if not quality.results:
            findings.append(dict(code='quality_history_absent',severity='info',
                message='No historical quality observations overlap the requested range.',
                evidence_refs=['historical_quality']))
        uncertainty = [
            'Audit agreement does not prove all source payments arrived.',
            'Historical quality snapshots overlap and may be partial; this response does not establish continuous coverage.',
            'Current payment freshness is measured now, not at the historical incident time.',
            'No causal diagnosis or business-performance comparison is established in this phase.'
        ]
        if 'incompatible_window_configuration' in counts:
            actions.append('Verify the actual pipeline window configuration before comparing or repairing aggregates.')
        if status == 'discrepancy':
            actions.append('Inspect source, validation, job and late-arrival evidence before selecting a manual reconciliation procedure.')
        elif status == 'insufficient_evidence':
            actions.append('Verify the time range, currency and source delivery evidence.')
        else:
            actions.append('Confirm source completeness before treating the dashboard as trustworthy.')
        if not quality.results or quality.truncated:
            actions.append('Inspect the quality endpoint with narrower ranges; enable phase-one evidence persistence if history is absent.')
        return Investigation(investigation_id=uuid4(), observed_at=comparison['observed_at'],
            status=status, request=request, metric_definition=definition, dependencies=dependencies,
            comparison=comparison,current_payment_freshness=freshness,historical_quality=quality,
            findings=findings,uncertainty=uncertainty,recommended_actions=actions)

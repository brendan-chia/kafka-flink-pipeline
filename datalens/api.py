"""Read-only HTTP evidence API. Launch with scripts/datalens_api.py."""
import logging
import os
from pathlib import Path
from secrets import compare_digest
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import psycopg2
from pydantic import ValidationError

from datalens.assistant_models import AssistantRequest, AssistantReport, RetrievalPage
from datalens.llm import ModelConfig
from datalens.retrieval import RunbookStore
from datalens.workflow import InvestigationWorkflow, BusyInvestigation
from datalens import evidence
from datalens.investigation import EvidenceService, UnknownMetric
from datalens.models import (Catalogue, DependencyResult, Investigation, InvestigationRequest,
                             PaymentFreshness, QualityPage, RevenueComparison, RevenueRequest)

logger = logging.getLogger(__name__)


def get_service():
    return EvidenceService()


def require_api_key(x_datalens_key: Annotated[str | None, Header()] = None):
    expected = os.getenv('DATALENS_API_KEY')
    if expected and (x_datalens_key is None or not compare_digest(x_datalens_key.encode(),expected.encode())):
        raise HTTPException(status_code=401, detail='Invalid or missing API key')


def get_workflow(service: Annotated[EvidenceService, Depends(get_service)]):
    return InvestigationWorkflow(service)


def create_app():
    app = FastAPI(title='DataLens evidence API', version='0.3.0',
        description='Read-only evidence and bounded OpenAI-assisted investigations. Repairs stay manual.')
    router = APIRouter(prefix='/v1', dependencies=[Depends(require_api_key)])

    @app.exception_handler(psycopg2.Error)
    async def database_error(request: Request, exc):
        logger.warning('DataLens database operation failed (%s)', type(exc).__name__)
        return JSONResponse(status_code=503, content={'detail':'Evidence database unavailable, timed out, or missing required schema. Check configuration and migration 004.'})

    @app.exception_handler(UnknownMetric)
    async def unknown_metric(request: Request, exc):
        return JSONResponse(status_code=404,content={'detail':str(exc)})

    @app.exception_handler(ValidationError)
    async def invalid_evidence(request: Request, exc):
        return JSONResponse(status_code=503,content={'detail':'Stored evidence does not match the expected schema.'})

    @app.exception_handler(ValueError)
    async def invalid_diagnostic(request: Request, exc):
        return JSONResponse(status_code=422,content={'detail':str(exc)})

    @app.get('/health/live')
    def live():
        return {'status':'ok'}

    @router.get('/health/ready')
    def ready(service: Annotated[EvidenceService, Depends(get_service)]):
        with service.snapshot() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT name FROM datalens.metrics LIMIT 0')
                cur.execute('SELECT name FROM datalens.datasets LIMIT 0')
                cur.execute('SELECT upstream, downstream FROM datalens.dependencies LIMIT 0')
                cur.execute('SELECT result_id FROM datalens.quality_results LIMIT 0')
                cur.execute('SELECT event_timestamp_ms FROM processed_events LIMIT 0')
                cur.execute('SELECT revenue FROM payment_revenue_windows LIMIT 0')
        return {'status':'ready'}

    @router.get('/catalogue', response_model=Catalogue)
    def catalogue(service: Annotated[EvidenceService, Depends(get_service)]):
        with service.snapshot() as conn:
            return service.catalogue(conn)

    @router.get('/dependencies', response_model=DependencyResult)
    def dependencies(service: Annotated[EvidenceService, Depends(get_service)],
                     node: str = Query(min_length=1,max_length=200),
                     direction: Literal['upstream','downstream'] = 'upstream'):
        with service.snapshot() as conn:
            return service.dependencies(service.catalogue(conn),node,direction)

    @router.get('/quality', response_model=QualityPage)
    def quality(service: Annotated[EvidenceService, Depends(get_service)],
                from_utc: str = Query(max_length=40), until_utc: str = Query(max_length=40),
                limit: int = Query(default=100,ge=1,le=1000),
                dataset: str | None = Query(default=None,max_length=200),
                check_name: str | None = Query(default=None,max_length=100)):
        start,end = evidence.validate_range(from_utc,until_utc,'MYR',1)
        with service.snapshot() as conn:
            return service.quality(conn,start,end,limit,dataset,check_name)

    @router.get('/payments/freshness', response_model=PaymentFreshness)
    def freshness(service: Annotated[EvidenceService, Depends(get_service)]):
        with service.snapshot() as conn:
            return service.freshness(conn)

    @router.post('/revenue/compare', response_model=RevenueComparison)
    def compare(body: RevenueRequest, service: Annotated[EvidenceService, Depends(get_service)]):
        with service.snapshot() as conn:
            return service.compare(conn,body)

    @router.post('/investigations', response_model=Investigation)
    def investigate(body: InvestigationRequest, service: Annotated[EvidenceService, Depends(get_service)]):
        return service.investigate(body)

    @router.get('/assistant/config')
    def assistant_config():
        config = ModelConfig.from_env()
        return {'provider': 'openai', 'configured': config.configured,
                'model': config.model if config.configured else None,
                'max_model_calls': 1, 'max_concurrent_investigations': 4}

    @router.get('/runbooks', response_model=RetrievalPage)
    def runbooks(q: str = Query(min_length=1, max_length=1000), limit: int = Query(default=4, ge=1, le=6)):
        return RunbookStore().search(q, limit)

    @router.post('/assistant/investigations', response_model=AssistantReport)
    def assistant(body: AssistantRequest, workflow: Annotated[InvestigationWorkflow, Depends(get_workflow)]):
        try:
            return workflow.run(body)
        except BusyInvestigation:
            raise HTTPException(status_code=429, detail='Investigation capacity reached. Try again shortly.',
                                headers={'Retry-After': '5'}) from None

    static = Path(__file__).resolve().parent / 'static'
    app.mount('/assets', StaticFiles(directory=static), name='assets')

    @app.get('/', include_in_schema=False)
    def interface():
        return FileResponse(static / 'index.html')

    @app.middleware('http')
    async def browser_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['Cache-Control'] = 'no-store'
        if request.url.path == '/' or request.url.path.startswith('/assets/'):
            response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"
        return response

    app.include_router(router)
    return app


app = create_app()

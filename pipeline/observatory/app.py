"""Read-only metrics UI. No Delta sessions or DB mutation endpoints."""
from contextlib import asynccontextmanager
import hmac
from pathlib import Path
import threading
from fastapi import FastAPI, Header, HTTPException, Query, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from common import settings
from observatory.sampler import Sampler, date
from observatory.attention import attention_records, ATTENTION_STATES
from observatory.recent import recent_records

STATIC = Path(__file__).with_name('static')


def create_app(sampler=None, background=True):
    sampler = sampler or Sampler()
    cfg = sampler.cfg
    if cfg.observatory_bind not in ('127.0.0.1', 'localhost', '::1') and not cfg.observatory_key:
        raise RuntimeError('OBSERVATORY_KEY is required for a non-loopback bind')
    stop = threading.Event()

    @asynccontextmanager
    async def lifespan(app):
        def run():
            sampler.load_history()
            while not stop.is_set():
                try:
                    sampler.sample()
                    sampler.last_failure = None
                except Exception as e:
                    sampler.last_failure = type(e).__name__
                stop.wait(max(5, cfg.observatory_interval_s))
        thread = None
        if background:
            thread = threading.Thread(target=run, daemon=True, name='ops-sampler')
            thread.start()
        yield
        stop.set()
        if thread:
            thread.join(timeout=2)

    app = FastAPI(title='NODEVERSE Observatory', lifespan=lifespan, docs_url=None, redoc_url=None)
    app.state.sampler = sampler
    app.mount('/static', StaticFiles(directory=STATIC), name='static')

    @app.middleware('http')
    async def headers(request, call_next):
        response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'"
        return response

    def authorize(key):
        if cfg.observatory_key and not hmac.compare_digest((key or '').encode(), cfg.observatory_key.encode()):
            raise HTTPException(401, 'Observatory key required')

    @app.get('/')
    def index():
        return FileResponse(STATIC/'index.html')

    @app.get('/health')
    def health():
        return {'state': 'running', 'sampling': sampler.snapshot is not None,
                'last_sampler_error': sampler.last_failure}

    @app.get('/api/config')
    def config():
        return {'name': cfg.observatory_name, 'auth_required': bool(cfg.observatory_key),
                'interval_s': max(5, cfg.observatory_interval_s)}

    @app.get('/api/snapshot')
    def snapshot(x_observatory_key: str = Header(default='')):
        authorize(x_observatory_key)
        if sampler.snapshot is None:
            raise HTTPException(503, 'Collecting the first sample')
        return dict(sampler.snapshot, sampler_error=sampler.last_failure,
                    snapshot_age_s=max(0, (sampler.clock()-date(sampler.snapshot['at'])).total_seconds()))

    @app.get('/api/history')
    def history(minutes: int = Query(default=15, ge=1, le=360), x_observatory_key: str = Header(default='')):
        authorize(x_observatory_key)
        return {'points': sampler.history(minutes), 'minutes': minutes}

    @app.get('/api/attention')
    def attention(state: str = Query(...), x_observatory_key: str = Header(default='')):
        authorize(x_observatory_key)
        if state not in ATTENTION_STATES:
            raise HTTPException(422, 'Unsupported attention state')
        try:
            return attention_records(sampler._database(), state)
        except Exception:
            raise HTTPException(503, 'Attention records unavailable')

    @app.get('/api/recent')
    def recent(x_observatory_key: str = Header(default='')):
        authorize(x_observatory_key)
        try:
            return recent_records(sampler._database())
        except Exception:
            raise HTTPException(503, 'Recent records unavailable')

    return app


app = create_app()

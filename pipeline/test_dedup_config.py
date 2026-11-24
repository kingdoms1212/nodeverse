import importlib.util
from dataclasses import replace
from pathlib import Path
import pytest
from common.settings import Settings


@pytest.mark.parametrize('hours',[48,168])
def test_dedup_driver_and_audit_same_units(monkeypatch,hours):
    monkeypatch.setenv('DEDUP_TTL_HOURS',str(hours))
    spec=importlib.util.spec_from_file_location('audit_ttl_probe',Path('spark/audit_state.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    assert Settings().dedup_ttl_ms == module.DEDUP_TTL_MS == hours*3600*1000


def test_invalid_ttl_rejected(monkeypatch):
    with pytest.raises(ValueError):_ = replace(Settings(),dedup_ttl_hours=0).dedup_ttl_ms
    monkeypatch.setenv('DEDUP_TTL_HOURS','0')
    spec=importlib.util.spec_from_file_location('audit_ttl_invalid',Path('spark/audit_state.py'))
    with pytest.raises(ValueError):spec.loader.exec_module(importlib.util.module_from_spec(spec))

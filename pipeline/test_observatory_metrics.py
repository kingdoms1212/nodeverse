import json
from datetime import datetime,timedelta,timezone
from observatory.metrics import DeltaCommits,KafkaLag,latency_summary,read_latency

NOW=datetime(2026,11,23,tzinfo=timezone.utc)

def test_delta_reader_incremental_metadata_and_no_fake_scan_bytes(tmp_path):
 root=tmp_path/'silver/learning_events/_delta_log';root.mkdir(parents=True)
 commit={'commitInfo':{'operation':'MERGE','timestamp':int(NOW.timestamp()*1000),'operationParameters':{'predicate':'event_id=x'},'operationMetrics':{'executionTimeMs':'300','scanTimeMs':'0','numTargetBytesAdded':'999'}}}
 (root/'00000000000000000000.json').write_text(json.dumps(commit))
 probe=DeltaCommits(tmp_path);a=probe.sample();assert a['execution_ms']==300 and a['scan_ms']==0
 assert a['scan_bytes'] is None and a['date_predicate'] is False
 assert probe.sample()['metadata_files_checked']==0
 commit['commitInfo']['operationMetrics']['executionTimeMs']='400'
 (root/'00000000000000000001.json').write_text(json.dumps(commit));assert probe.sample()['execution_ms']==400

def test_latency_scope_and_unique_events_and_clock_anomalies():
 rows=[{'_id':str(i),'completed_at':NOW,'queued_at':NOW-timedelta(milliseconds=i*100),'event':{'received_at':(NOW-timedelta(milliseconds=i*200)).isoformat()},'result':{'scope':'runtime_archive'}} for i in range(1,6)]
 rows+=[rows[0],{'_id':'clock','completed_at':NOW,'queued_at':NOW+timedelta(seconds=1)}]
 d=latency_summary(rows,NOW)
 assert d['groups']['runtime_archive']['queue_ms']=={'n':5,'p50':300,'p95':500,'max':500}
 assert d['groups']['runtime_archive']['received_ms']['p95']==1000
 assert d['clock_anomalies']==1

def test_latency_requires_index_before_any_data_read():
 class NoIndex:
  remote_event_outbox=None
  def __init__(self):self.remote_event_outbox=self
  def index_information(self):return {}
  def find(self,*a):raise AssertionError('must not scan without index')
 assert read_latency(NoIndex(),NOW)['state']=='index_required'

def report(offsets):return {'queries':[{'name':'bronze','progress':{'batchId':12,'sources':[{'endOffset':offsets}]}}]}

def test_kafka_lag_reads_offsets_without_commits():
 r=report(json.dumps({'nv.learning.v2':{'0':10,'1':20}}))
 result=KafkaLag('unused',fetch=lambda offsets:{('nv.learning.v2',0):13,('nv.learning.v2',1):25}).sample(r)
 assert result['pending']==8 and result['partitions']==2
 assert result['source_batch_id']==12

def test_kafka_reset_and_missing_are_unknown_not_zero():
 assert KafkaLag('unused',fetch=lambda x:{('nv.learning.v2',0):5}).sample(report({'nv.learning.v2':{'0':10}}))['pending'] is None
 assert KafkaLag('unused').sample({})['state']=='no_checkpoint_offsets'

def test_latency_does_not_mix_scopes_or_count_unknown_timestamps():
 rows=[{'_id':'a','completed_at':NOW,'queued_at':NOW-timedelta(seconds=2),'result':{'scope':'dashboard_records'},'event':{'received_at':'bad-date'}},{'_id':'b','completed_at':NOW,'queued_at':NOW-timedelta(seconds=10),'result':{'scope':'runtime_archive'}}]
 result=latency_summary(rows,NOW)['groups'];assert result['dashboard_records']['queue_ms']['p95']==2000
 assert result['runtime_archive']['queue_ms']['p95']==10000
 assert result['dashboard_records']['received_ms']['n']==0

def test_runtime_manifest_is_invalid_after_application_changes(tmp_path,monkeypatch):
 from observatory.metrics import runtime_config
 path=tmp_path/'runtime.json';path.write_text(json.dumps({'application_id':'before','effective_dedup_hours':168,'verification':'process_env_and_verified_code_default'}))
 class Response:
  def __enter__(self):return self
  def __exit__(self,*a):pass
  def read(self):return b'[{"id":"after"}]'
 monkeypatch.setattr('observatory.metrics.urllib.request.urlopen',lambda *a,**k:Response())
 assert runtime_config(str(path),'http://unused')['state']=='app_changed'

def test_delta_file_write_bytes_are_not_reported_as_scan_bytes(tmp_path):
 root=tmp_path/'silver/learning_events/_delta_log';root.mkdir(parents=True)
 c={'commitInfo':{'timestamp':int(NOW.timestamp()*1000),'operation':'MERGE','operationParameters':{'predicate':'event_id=x AND event_date BETWEEN a AND b'},'operationMetrics':{'numTargetBytesAdded':'12345','numTargetFilesAdded':'5'}}}
 (root/'00000000000000000000.json').write_text(json.dumps(c))
 d=DeltaCommits(tmp_path).sample();assert d['scan_bytes'] is None and d['scan_files'] is None and d['date_predicate'] is True

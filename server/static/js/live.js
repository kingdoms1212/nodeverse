/* /staff/live/ — polls the admin live APIs every 3 seconds. */
(function () {
  'use strict';
  var config = JSON.parse(document.getElementById('live-config').textContent);
  var POLL_MS = 3000;
  var status = document.getElementById('live-status');
  var statusText = document.getElementById('live-status-text');
  var AREA_LABELS = { structures: '자료구조', algorithms: '알고리즘', sql: 'SQL', data: '데이터 탐색', unmapped: '미분류' };

  function $(id) { return document.getElementById(id); }
  function esc(value) {
    return String(value == null ? '' : value).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function pct(value) { return value == null ? '—' : Math.round(value * 1000) / 10 + '%'; }
  function time(iso) {
    if (!iso) return '—';
    var d = new Date(iso);
    return isNaN(d) ? esc(iso) : d.toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' });
  }
  function get(url) {
    return fetch(url, { credentials: 'same-origin', headers: { Accept: 'application/json' } }).then(function (r) {
      if (r.status === 401 || r.status === 403) { window.location.href = '/staff/login/'; throw new Error('auth'); }
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    });
  }
  function bars(el, items) {
    var max = Math.max.apply(null, items.map(function (i) { return i.value; }).concat([1]));
    el.innerHTML = items.length ? items.map(function (i) {
      return '<div class="bar-row"><span>' + esc(i.label) + '</span><div class="bar-track"><i style="width:' +
        Math.round(100 * i.value / max) + '%"></i></div><strong>' + esc(i.text != null ? i.text : i.value) + '</strong></div>';
    }).join('') : '<p class="empty">데이터 없음</p>';
  }
  function sparkline(points) {
    var w = 120, h = 26;
    var valid = points.filter(function (p) { return p != null; });
    if (!valid.length) return '<span class="muted">—</span>';
    var step = points.length > 1 ? w / (points.length - 1) : w;
    var d = '', pen = false;
    points.forEach(function (p, i) {
      if (p == null) { pen = false; return; }
      d += (pen ? 'L' : 'M') + (i * step).toFixed(1) + ',' + (h - p * (h - 2) - 1).toFixed(1);
      pen = true;
    });
    return '<svg class="spark" viewBox="0 0 ' + w + ' ' + h + '" width="' + w + '" height="' + h + '" aria-hidden="true">' +
      '<path d="' + d + '" fill="none" stroke="currentColor" stroke-width="1.6"/></svg>';
  }
  function setStatus(summary, error) {
    status.classList.toggle('stale', !!(error || (summary && summary.stale)));
    if (error) { statusText.textContent = '불러오기 실패: ' + error.message + ' (다시 시도 중)'; return; }
    if (summary.stale) {
      statusText.textContent = '수집 지연 — 마지막 갱신 ' + (summary.updated_at ? time(summary.updated_at) + ' (' + Math.round(summary.age_seconds) + '초 전)' : '기록 없음');
    } else {
      statusText.textContent = '실시간 수집 정상 · ' + time(summary.updated_at) + ' 갱신';
    }
  }

  var SINK_LABELS = { stored: '적재 완료', quarantined: '격리', pending: '대기', processing: '처리 중',
    retrying: '재시도', waiting_context: '조건 대기', parked: '보관(재시도 멈춤)', blocked: '데이터 오류' };
  var REASON_LABELS = { member_mapping_required: '회원 매핑 없음', retry_limit: '재시도 한도 초과',
    other_database_writer_active: '다른 적재 도구 실행 중' };

  function renderSink(sink) {
    var states = $('sink-states');
    if (!states) return;
    if (!sink || !sink.states) { states.textContent = '원격 적재 작업자 상태 기록 없음'; return; }
    var order = ['stored', 'quarantined', 'pending', 'processing', 'retrying', 'waiting_context', 'parked', 'blocked'];
    states.textContent = order.filter(function (k) { return sink.states[k]; })
      .map(function (k) { return SINK_LABELS[k] + ' ' + sink.states[k].toLocaleString(); }).join(' · ') || '적재 기록 없음';
    var parts = [];
    ['parked', 'blocked'].forEach(function (state) {
      var grouped = {};  // sequence_gap:expected_3 / _7 ... are one reason for the operator
      ((sink.reasons && sink.reasons[state]) || []).forEach(function (r) {
        var why = REASON_LABELS[r.reason] || (String(r.reason || '').indexOf('sequence_gap') === 0 ? '앞 이벤트 누락' : r.reason);
        grouped[why] = (grouped[why] || 0) + r.n;
      });
      Object.keys(grouped).sort(function (a, b) { return grouped[b] - grouped[a]; }).slice(0, 3).forEach(function (why) {
        parts.push(SINK_LABELS[state] + ': ' + why + ' ' + grouped[why].toLocaleString() + '건');
      });
    });
    var reasons = $('sink-reasons');
    reasons.hidden = !parts.length;
    reasons.textContent = parts.join(' · ');
    $('sink-updated').textContent = sink.updated_at ? time(sink.updated_at) + ' 기준' : '—';
  }

  var REJECT_LABELS = { invalid_json: 'JSON 형식 오류', unknown_event_type: '알 수 없는 이벤트 종류',
    schema_error: '필드 검증 실패', too_old: '6일 넘은 이벤트', id_conflict: '같은 ID 다른 내용',
    invalid_schema: '필수 필드 누락', unknown: '사유 없음' };

  function renderRejects(rej) {  // PL-8
    var counts = $('reject-counts');
    if (!counts) return;
    if (!rej) { counts.textContent = '거절·격리 집계 기록 없음'; return; }
    counts.textContent = '수집 거절(DLQ) 60분 ' + rej.dlq.last_60m.toLocaleString() + '건 · 24시간 ' + rej.dlq.last_24h.toLocaleString() +
      '건  |  Silver 격리 60분 ' + rej.quarantine.last_60m.toLocaleString() + '건 · 24시간 ' + rej.quarantine.last_24h.toLocaleString() + '건';
    var parts = [];
    [['거절', rej.dlq.reasons_60m], ['격리', rej.quarantine.reasons_60m]].forEach(function (pair) {
      (pair[1] || []).slice(0, 3).forEach(function (r) {
        parts.push(pair[0] + ': ' + (REJECT_LABELS[r.reason] || r.reason) + ' ' + r.n.toLocaleString() + '건');
      });
    });
    var reasons = $('reject-reasons');
    reasons.hidden = !parts.length;
    reasons.textContent = parts.join(' · ');
    var recent = rej.dlq.recent || [];
    $('reject-recent-wrap').hidden = !recent.length;
    $('reject-recent').innerHTML = recent.map(function (r) {
      return '<tr><td>' + time(r.at) + '</td><td>' + esc(REJECT_LABELS[r.reason] || r.reason) + '</td><td>' + esc(r.endpoint) +
        '</td><td>' + esc(r.field) + '</td><td>' + esc(r.message) + '</td></tr>';
    }).join('');
    $('reject-updated').textContent = rej.updated_at ? time(rej.updated_at) + ' 기준 · 1분마다' : '—';
  }

  // Trend arrows (like a stock ticker): compare each KPI with its value about one minute ago.
  // Shown only for a clear change: at least TREND_MIN_ABS and TREND_MIN_RATIO of the earlier value.
  var TREND_WINDOW_MS = 60000, TREND_MIN_ABS = 3, TREND_MIN_RATIO = 0.2;
  var history = {};

  function setKpi(key, value, noteBase) {
    $('kpi-' + key).textContent = Number(value).toLocaleString();
    var now = Date.now();
    var h = history[key] || (history[key] = []);
    h.push({ t: now, v: value });
    while (h.length > 2 && now - h[1].t >= TREND_WINDOW_MS) h.shift();  // keep the newest sample older than 1 min
    var trend = $('kpi-' + key + '-trend');
    var note = $('kpi-' + key + '-note');
    var base = h[0];
    if (!trend) return;
    if (now - base.t < TREND_WINDOW_MS * 0.9) {  // not enough history yet (page just opened)
      trend.textContent = ''; trend.className = 'trend';
      if (note && noteBase) note.textContent = noteBase;
      return;
    }
    var diff = value - base.v;
    var big = Math.abs(diff) >= TREND_MIN_ABS && Math.abs(diff) >= Math.abs(base.v) * TREND_MIN_RATIO;
    trend.className = 'trend' + (big ? (diff > 0 ? ' up' : ' down') : '');
    trend.textContent = big ? (diff > 0 ? '▲ ' : '▼ ') + Math.abs(diff).toLocaleString() : '';
    trend.title = big ? '1분 전 ' + base.v.toLocaleString() + ' → 지금 ' + value.toLocaleString() : '';
    if (note && noteBase) {
      var pct = base.v ? ' (' + (diff > 0 ? '+' : '') + Math.round(diff / base.v * 100) + '%)' : '';
      note.textContent = big ? '1분 전보다 ' + (diff > 0 ? '급증' : '급감') + pct : noteBase;
    }
  }

  function renderLive(summary, games, alerts) {
    renderSink(summary.sink);
    renderRejects(summary.rejects);
    var late = summary.late_last_1m || 0;  // counted in the total, shown apart (7th round)
    setKpi('events', summary.events_last_1m, '게임에서 들어온 학습 행동 수' + (late ? ' · 늦게 도착 ' + late.toLocaleString() + '건 포함' : ''));
    setKpi('sessions', summary.active_sessions_5m, '5분 안에 행동이 있었던 플레이');
    setKpi('alerts', alerts.alerts.length);
    var minutes = [];
    var now = Date.now();
    for (var m = games.minutes; m >= 1; m--) minutes.push(Math.floor((now - m * 60000) / 60000));
    var byGame = {};
    games.rows.forEach(function (row) {
      var g = byGame[row.minigame_id] || (byGame[row.minigame_id] = { attempts: 0, successes: 0, slots: {} });
      g.attempts += row.attempts || 0;
      g.successes += row.successes || 0;
      g.slots[Math.floor(new Date(row.window_start).getTime() / 60000)] = row.attempts ? row.successes / row.attempts : null;
    });
    var total = 0;
    var rows = games.games.map(function (game) {
      var g = byGame[game.minigame_id] || { attempts: 0, successes: 0, slots: {} };
      total += g.attempts;
      return { game: game, g: g };
    }).sort(function (a, b) { return b.g.attempts - a.g.attempts; });
    setKpi('attempts', total, '24개 미니게임 합계');
    $('game-rows').innerHTML = rows.map(function (r) {
      var rate = r.g.attempts ? r.g.successes / r.g.attempts : null;
      return '<tr><td><strong>' + esc(r.game.minigame_id) + '</strong><small>' + esc(r.game.title) + '</small></td><td>' +
        esc(AREA_LABELS[r.game.area]) + '</td><td class="spark-cell">' +
        sparkline(minutes.map(function (k) { return r.g.slots[k] != null ? r.g.slots[k] : null; })) +
        '</td><td class="numeric">' + r.g.attempts + '</td><td class="numeric' + (rate != null && rate < 0.5 ? ' low' : '') + '">' + pct(rate) + '</td></tr>';
    }).join('');
    var note = $('mismatch-note');
    note.hidden = !games.category_mismatches.length;
    note.textContent = '게임이 보낸 category가 서버 카탈로그 영역과 다릅니다 (게임 파트에 알려 주세요): ' + games.category_mismatches.join(', ');
    bars($('area-bars'), games.areas.map(function (a) { return { label: a.label, value: a.attempts }; }));
    $('alert-rows').innerHTML = alerts.alerts.length ? alerts.alerts.map(function (a) {
      return '<tr><td>' + time(a.window_start) + '</td><td><strong>' + esc(a.minigame_id) + '</strong><small>' + esc(a.title || '카탈로그에 없음') +
        '</small></td><td>' + esc(a.area_label) + '</td><td class="numeric">' + esc(a.attempts) + '</td><td class="numeric">' + esc(a.failures) +
        '</td><td class="numeric low">' + pct(a.failure_rate) + '</td><td class="numeric">' + pct(a.baseline_rate) + '</td></tr>';
    }).join('') : '<tr><td colspan="7" class="empty">최근 60분 동안 실패 급증 경고가 없습니다.</td></tr>';
  }

  function renderUpdates(data) {
    var a = data.active;
    $('kpi-build').textContent = a.build_version ? pct(a.build_share) : '—';
    $('kpi-build-label').textContent = a.build_version ? 'EXE ' + a.build_version : '활성 릴리스 없음';
    $('kpi-content').textContent = pct(a.content_share);
    $('kpi-content-label').textContent = '팩 ' + a.content_version;
    $('kpi-below').textContent = a.min_build ? a.installs_below_min_build : '—';
    $('kpi-response').textContent = pct(data.prompt_response.rate);
    $('kpi-response-label').textContent = data.prompt_response.accepted + ' / ' + data.prompt_response.prompted + ' · 최근 ' + data.minutes + '분';
    var warn = $('pack-warning');
    warn.hidden = !data.pack_warning.review;
    warn.innerHTML = '⚠ 최근 15분 팩 다운로드 ' + data.pack_warning.downloads_15m + '건 중 실패율 ' + pct(data.pack_warning.failure_rate_15m) +
      ' — <a href="' + esc(config.releasesUrl) + '">팩 비활성화 검토</a> (자동으로 끄지 않습니다)';
    $('update-rows').innerHTML = data.windows.length ? data.windows.slice().reverse().map(function (w) {
      return '<tr><td>' + time(w.window_start) + '</td><td>' + esc(w.target_version) + '</td><td class="numeric">' + esc(w.downloads_started) +
        '</td><td class="numeric">' + esc(w.downloads_succeeded) + '</td><td class="numeric">' + esc(w.downloads_failed) + '</td><td class="numeric">' +
        esc(w.mounted) + '</td><td class="numeric' + (w.failure_rate >= 0.2 ? ' low' : '') + '">' + pct(w.failure_rate) + '</td><td class="numeric">' +
        (w.p50_duration_ms != null ? (w.p50_duration_ms / 1000).toFixed(1) + '초' : '—') + '</td></tr>';
    }).join('') : '<tr><td colspan="8" class="empty">최근 ' + data.minutes + '분 동안 업데이트 이벤트가 없습니다.</td></tr>';
    bars($('reason-bars'), data.failure_reasons.map(function (r) { return { label: r.reason, value: r.count }; }));
    function versionBars(kind, active) {
      return data.versions.filter(function (v) { return v.kind === kind; })
        .sort(function (x, y) { return y.installs - x.installs; })
        .map(function (v) { return { label: v.version + (v.version === active ? ' (현재)' : ''), value: v.installs, text: v.installs + ' · ' + pct(v.share) }; });
    }
    bars($('build-bars'), versionBars('build', a.build_version));
    bars($('content-bars'), versionBars('content', a.content_version));
  }

  function tick() {
    var requests = config.tab === 'updates'
      ? [get('/api/admin/live/summary'), get('/api/admin/live/updates?minutes=60')]
      : [get('/api/admin/live/summary'), get('/api/admin/live/minigames?minutes=30'), get('/api/admin/live/alerts?minutes=60')];
    Promise.all(requests).then(function (r) {
      setStatus(r[0]);
      if (config.tab === 'updates') renderUpdates(r[1]); else renderLive(r[0], r[1], r[2]);
    }).catch(function (e) { if (e.message !== 'auth') setStatus(null, e); })
      .then(function () { setTimeout(tick, document.hidden ? POLL_MS * 5 : POLL_MS); });
  }
  tick();
})();

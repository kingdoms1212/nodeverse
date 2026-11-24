# -*- coding: utf-8 -*-
"""Slide-ready figures (1920x1080 SVG) comparing the pipeline's first structure, its current structure and the
optimizations of rounds 1-7.  python docs/presentation/pipeline_comparison.py  -> docs/presentation/fig-*.svg
Render PNGs with any headless browser (the PDF build renders them with Edge)."""
from html import escape
from pathlib import Path

OUT = Path(__file__).parent
W, H = 1920, 1080
FONT = "'Malgun Gothic','Apple SD Gothic Neo',sans-serif"
INK, MUTED = "#1f2430", "#5b6472"
RED, GREEN, BLUE = "#c0392b", "#2f8a57", "#3b78c2"
LAYER = {  # fill, stroke, title
    "game": ("#fdf3e3", "#d08a2c", "#7a4a0c"), "ingest": ("#e8f1fb", "#3b78c2", "#1d4d86"),
    "kafka": ("#eef0fb", "#5560b8", "#2d357a"), "spark": ("#eaf6ef", "#2f8a57", "#1d5a38"),
    "store": ("#f3eefb", "#7d57b8", "#4c2f7a"), "writer": ("#fbeef0", "#b8506a", "#7a2238"),
    "server": ("#e9f6f6", "#2b8a8a", "#145a5a"), "ops": ("#f4f5f7", "#7b8494", "#3b4250"),
}


def svg(body, title, subtitle):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" font-family="{FONT}">'
            '<defs>' + "".join(
                f'<marker id="m{k}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
                f'<path d="M0,0 L10,5 L0,10 z" fill="{c}"/></marker>' for k, c in (("d", "#2f6f4f"), ("r", RED), ("b", BLUE))) +
            f'</defs><rect width="{W}" height="{H}" fill="#ffffff"/>'
            f'<text x="60" y="78" font-size="44" font-weight="800" fill="{INK}">{escape(title)}</text>'
            f'<text x="60" y="122" font-size="22" fill="{MUTED}">{escape(subtitle)}</text>' + body + "</svg>")


def text(x, y, s, size=20, color=INK, weight=400, anchor="start"):
    return (f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{color}" text-anchor="{anchor}" '
            f'xml:space="preserve">{escape(s)}</text>')


def box(x, y, w, h, title, lines, kind, size=19):
    fill, stroke, tcol = LAYER[kind]
    o = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="{fill}" stroke="{stroke}" stroke-width="2.5"/>',
         text(x + 18, y + 38, title, 24, tcol, 700)]
    ly = y + 74
    for line in lines:
        color = INK
        if line.startswith("! "):
            color, line = RED, "⚠ " + line[2:]
        elif line.startswith("+ "):
            color, line = GREEN, "✔ " + line[2:]
        o.append(text(x + 20, ly, line, size, color, 600 if color != INK else 400))
        ly += size + 11
    return "".join(o)


def arrow(pts, kind="d", label="", lx=None, ly=None):
    col = {"d": "#2f6f4f", "r": RED, "b": BLUE}[kind]
    d = "M" + " L".join(f"{x},{y}" for x, y in pts)
    o = f'<path d="{d}" fill="none" stroke="{col}" stroke-width="3.5" marker-end="url(#m{kind})"/>'
    if label:
        lx = lx if lx is not None else (pts[0][0] + pts[1][0]) / 2
        ly = ly if ly is not None else (pts[0][1] + pts[1][1]) / 2 - 10
        o += (f'<text x="{lx}" y="{ly}" font-size="18" font-weight="700" fill="{col}" text-anchor="middle" '
              f'paint-order="stroke" stroke="#fff" stroke-width="5">{escape(label)}</text>')
    return o


def flow(stage):
    """Same layout for both structures; problem lines (!) on the first, changes (+) on the current one."""
    first = stage == "first"
    g = box(60, 170, 340, 330, "게임 (Godot)", [
        "학습·업데이트·런처 이벤트", "디스크 큐 → HTTP 전송",
        *(["! 4xx 하나가 큐 전체를 막음", "! 401 영구 거부, 6일 초과 전송", "! UUIDv4 (시간순 아님)"] if first else
          ["+ 응답별 재시도·만료", "+ UUIDv7 (시간순 ID)", "+ 원자적 큐 쓰기, 계정별 세션"])], "game")
    c = box(450, 170, 360, 330, "수집기 (FastAPI)", [
        "검증 → Kafka → 202 / 422+DLQ",
        *(["! Kafka 장애 시 요청당 12초 대기", "! 폴링 스레드 예외 무방비", "! 상태 조회 Mongo 장애 = 500"] if first else
          ["+ 대기 5초, 폴링 스레드 보호", "+ /health/ready = 수집 준비", "+ /health/deps 브로커별 진단"])], "ingest")
    k = box(860, 170, 420, 330, "Kafka 3대 (KRaft)", [
        "토픽 5개 · 복제 3 · 최소 ISR 2", "보관 7일",
        *(["! 헬스체크 = 10초마다 JVM CLI", "  (브로커당 CPU 최대 150%)", "! topic-init도 JVM CLI"] if first else
          ["+ 생존 nc + Python 프로토콜 검사", "+ topic-init AdminClient·재시도", "+ deps-ready 기동 관문"])], "kafka")
    s = box(60, 550, 1220, 290, "Spark + Delta (단일 앱)", [
        *(["Bronze append → Silver 중복 제거 + MERGE → Gold 1분·5분 창 → MongoDB rt_*",
           "! 한 파일 484줄, 쿼리 하나 실패 = 앱 전체 정지 · rt_* 쓰기 재시도 없음",
           "! Silver MERGE 조건 event_id 하나 → 매 배치 전체 스캔 · outbox 이벤트당 Mongo 2회",
           "! 자동 압축이 매 커밋 파티션 전체 재작성 (실데이터 30MB → 디스크 4.9GB)",
           "! 늦게 도착한 기록이 실시간 경고·버전 보급률을 왜곡"] if first else
          ["Bronze append → Silver 중복 제거 + MERGE → Gold 1분·5분 창 → MongoDB rt_*",
           "+ 모듈 분리, 실패한 쿼리만 재시작, sink 재시도, Gold 게시 실패 창 재게시",
           "+ MERGE: 날짜 7일 + v7 ID 범위 + ID 생성일별 분리 · outbox 500건 bulk",
           "+ 자동 압축 끔 → 300배치마다 작은 파일만 + VACUUM (별도 스레드)",
           "+ 늦게 도착 건수 표시, 경고는 정시 이벤트만, 거절·격리(DLQ) 집계"])], "spark", 20)
    r = box(60, 880, 1220, 170, "원격 작업자 → MySQL·대시보드 MongoDB", [
        *(["! 1건씩, 매 건 MySQL 새 연결 + GET_LOCK → 초당 17~24건 (유입의 1/8)",
           "! 대기·재시도 기한 없음, blocked 안 보임, outbox 무한 증가"] if first else
          ["+ 묶음·세션 병렬·풀 + 왕복 26→10회·순번 역전 해결 → 초당 219건 (유입 따라감)",
           "+ 기한 후 parked, 상태 카드, 완료 행 30일 TTL, 상태 집계는 인덱스만"])], "writer", 20)
    sv = box(1330, 170, 530, 670, "관리자 서버 (Django)", [
        "실시간 현황 (rt_*, 3초)", "전체 현황·게임 분석·플레이어",
        "", *(["! 적재 상태·거절 이벤트를", "  볼 화면 없음", "! /staff/ 참조 누락 시 500"] if first else
              ["+ 1분 전 대비 ▲▼ 추세", "+ 분석 화면 적재 상태 카드", "+ 거절·격리 이벤트 카드",
               "+ 늦게 도착 N건 표시", "+ 실시간 = 도착 시각,", "   분석 = 플레이 시각 (명시)",
               "+ 참조 누락에도 500 없음"])], "server")
    m = box(1330, 880, 530, 170, "운영 도구", [
        *(["! 수동 확인뿐"] if first else
          ["+ requeue · dlq_peek · health", "+ dummy_data · probe_latency"])], "ops")
    a = (arrow([(400, 330), (450, 330)]) + arrow([(810, 330), (860, 330)]) +
         arrow([(1070, 500), (1070, 550)]) + arrow([(670, 840), (670, 880)]) +
         arrow([(1280, 640), (1330, 640)], label="rt_*", lx=1305, ly=626) +
         arrow([(1280, 960), (1330, 960)], "d"))
    return g + c + k + s + r + sv + m + a


def fig_first():
    return svg(flow("first"), "① 처음 구조 (공용 git 정리 시점)",
               "2026-11-20 · NVG remote-sink 브랜치 · 빨간 글씨 = 실측·코드로 확인한 문제 지점")


def fig_current():
    return svg(flow("current"), "② 현재 구조 (1~8차 보완 후)",
               "2026-11-21 · 1~8차 · 초록 글씨 = 바뀐 부분 · 처리 경로(Kafka→Bronze→Silver→Gold→rt_*→원격 적재)는 그대로")


ROUNDS = [
    ("1차", "구조·처리량", ["Spark 모듈 분리, 쿼리별 재시작", "sink 재시도, MERGE 7일+v7 범위", "원격 적재 묶음·세션 병렬·연결 풀"],
     "원격 적재 17~24 → 60~90건/초"),
    ("2차", "멈춘 이벤트 (PL-9)", ["대기·재시도 기한 → parked", "자동 재개, requeue 도구", "적재 상태 카드, 30일 TTL"],
     "쉬는 중 무한 재시도 CPU 143% 제거"),
    ("3차", "CPU 경량화 + PL-8", ["헬스체크 JVM·mongosh 제거", "rt_summary 5분 필터, 버전 변경 시만 쓰기", "상태 집계 인덱스만, DLQ 카드"],
     "Kafka p95 120→6%, Mongo 39→5%"),
    ("4차", "신뢰성 P1 + 헬스체크", ["선점 토큰, 순번 복구, 무한 대기 격리", "재처리 예산, archive 재검증", "Gold 재게시, 프로토콜 헬스체크"],
     "Mongo 130초 정지에도 재시작 0"),
    ("5차", "게임·P2 시험", ["디스크 큐 원자적 쓰기·손상 보관", "계정 전환 시 새 세션", "역순·중복·조회·ZORDER 시험"],
     "6,000건·역순 30세션 섞임·누락 0"),
    ("6차", "데이터 증가 대비", ["자동 압축 → 주기 압축", "VACUUM 6시간 (별도 스레드)", "event_id ZORDER 비교 (현행 유지)"],
     "Silver MERGE 5.35 → 1.55초"),
    ("7차", "늦게 도착한 데이터", ["늦게 도착 건수 표시", "경고는 정시 이벤트만", "MERGE ID 생성일별 분리, 버전 보급률"],
     "사흘 전 602건 경고에서만 제외"),
    ("8차", "원격 적재 처리량", ["DB 왕복 26 → 10회 (캐시·삽입 우선)", "순번 역전 선점 해결", "중복 제거 TTL 축소 시험"],
     "34.7 → 219건/초"),
]


def fig_rounds():
    o, x0, w, gap = [], 50, 216, 12
    colors = ["#2f8a57", "#3b78c2", "#2f8a57", "#3b78c2", "#b8506a", "#c47a12", "#6b4fb8", "#2b8a8a"]
    for i, (no, name, items, result) in enumerate(ROUNDS):
        x = x0 + i * (w + gap)
        col = colors[i]
        o.append(f'<rect x="{x}" y="200" width="{w}" height="760" rx="16" fill="#fafbfc" stroke="{col}" stroke-width="3"/>')
        o.append(f'<rect x="{x}" y="200" width="{w}" height="96" rx="16" fill="{col}"/>'
                 f'<rect x="{x}" y="270" width="{w}" height="26" fill="{col}"/>')
        o.append(text(x + w / 2, 248, no, 36, "#fff", 800, "middle"))
        o.append(text(x + w / 2, 284, name, 19, "#fff", 700, "middle"))
        ly = 350
        for it in items:
            for part in wrap(it, 11):
                o.append(text(x + 18, ly, part, 19, INK))
                ly += 30
            ly += 14
        o.append(f'<line x1="{x + 16}" y1="760" x2="{x + w - 16}" y2="760" stroke="#d5dae1" stroke-width="2"/>')
        o.append(text(x + 18, 800, "실측", 17, MUTED, 700))
        ly = 836
        for part in wrap(result, 10):
            o.append(text(x + 18, ly, part, 21, col, 800))
            ly += 32
        if i < len(ROUNDS) - 1:
            o.append(arrow([(x + w + 1, 580), (x + w + gap - 1, 580)]))
    return svg("".join(o), "③ 보완 차수별 변경점 (1~8차)", "각 차수의 핵심 변경과 그 차수에서 직접 잰 대표 결과")


def wrap(s, n):
    out, line = [], ""
    for word in s.split(" "):
        if len((line + " " + word).strip()) > n and line:
            out.append(line)
            line = word
        else:
            line = (line + " " + word).strip()
    if line:
        out.append(line)
    return out


METRICS = [  # label, before, after, unit, higher_is_better, note
    ("원격 적재 처리량 (작업자 순수 처리)", 34.7, 219, "건/초", True, "9,000건 쌓아 두고 비우기 · 처음 구조는 17~24"),
    ("초당 200건 유입 시 큐 대기 → 완료 평균", 48.8, 2.9, "초", False, "12,000건 부하"),
    ("Silver MERGE 단계 (6,000건 부하, 중앙값)", 5.35, 1.55, "초", False, ""),
    ("Kafka 브로커 유휴 CPU p95", 120, 6, "%", False, "81~123 → 5~6"),
    ("MongoDB 유휴 CPU p95", 39, 5, "%", False, ""),
    ("시험 1회 Silver 디스크 증가", 420, 2, "MB", False, "이전 사본 누적"),
    ("원격 적재 상태 집계 문서 읽기 (10초마다)", 134580, 0, "건", False, "COLLSCAN → 인덱스"),
    ("rt_summary 집계 (첫 실행)", 5.8, 2.3, "초", False, "21만 행 → 최근 5분"),
]


def fig_metrics():
    o, top, row = [], 190, 118
    o.append(text(820, top - 18, "보완 전", 20, "#9aa3ae", 700))
    o.append(f'<rect x="790" y="{top - 34}" width="22" height="20" fill="#c9ced6"/>')
    o.append(text(970, top - 18, "현재", 20, GREEN, 700))
    o.append(f'<rect x="940" y="{top - 34}" width="22" height="20" fill="{GREEN}"/>')
    for i, (label, before, after, unit, higher, note) in enumerate(METRICS):
        y = top + i * row
        o.append(text(60, y + 34, label, 23, INK, 700))
        if note:
            o.append(text(60, y + 66, note, 18, MUTED))
        mx = max(before, after) or 1
        bw = lambda v: max(4, 800 * v / mx)  # noqa: E731
        o.append(f'<rect x="780" y="{y + 8}" width="{bw(before)}" height="34" rx="6" fill="#c9ced6"/>')
        o.append(text(790 + bw(before), y + 34, f"{fmt(before)} {unit}", 20, MUTED, 700))
        o.append(f'<rect x="780" y="{y + 50}" width="{bw(after)}" height="34" rx="6" fill="{GREEN}"/>')
        o.append(text(790 + bw(after), y + 76, f"{fmt(after)} {unit}", 20, GREEN, 800))
        change = (after / before if higher else (before - after) / before) if before else 0
        tag = f"{change:.1f}배" if higher else (f"−{change * 100:.1f}%" if 0.99 < change < 1 else f"−{change * 100:.0f}%")
        o.append(f'<rect x="1790" y="{y + 22}" width="90" height="44" rx="10" fill="#eef7f1" stroke="{GREEN}"/>')
        o.append(text(1835, y + 52, tag, 22, GREEN, 800, "middle"))
    return svg("".join(o), "④ 최적화 전후 성능 비교 (로컬 PC 실측)",
               "같은 PC·같은 부하 조건에서 잰 값 · 막대 길이는 지표마다 따로 맞춤")


def fmt(v):
    return f"{v:,.0f}" if v >= 100 or float(v).is_integer() else f"{v:g}"


SCENARIOS = [
    ("MongoDB 30~130초 정지", "rt_* 쓰기 실패 → 쿼리 실패 → 앱 전체 재시작", "수집 202 유지, 실패한 Gold 창 자동 재게시, 재시작 0"),
    ("Spark 쿼리 하나 실패", "Bronze 수집까지 전체 정지", "그 쿼리만 체크포인트에서 재시작"),
    ("Kafka 브로커 1대 정지·동결", "포트·JVM CLI로만 확인", "해당 브로커만 3초 안 이상 표시, 수집 준비 유지"),
    ("같은 이벤트 재전송", "Silver 중복 제거만 (단계별 검증 기록 없음)", "Bronze 이력 보존 · Silver 1행 · Gold 값 불변 확인"),
    ("며칠 늦게 도착한 기록", "경고·버전 보급률 왜곡, MERGE 범위 확대", "총계 포함·따로 표시, 경고 제외, MERGE 분리"),
    ("원격 적재 대기·재시도", "기한 없이 무한 재시도 (CPU 143%)", "기한 후 parked, 자동 재개, 재처리 예산"),
    ("유입이 초당 200건으로 몰릴 때", "초당 17~24건 (8차 직전에도 57건, 대기 48.8초)", "초당 193~219건으로 따라감, 평균 대기 2.9초"),
    ("운영자 park ↔ 작업자 선점", "처리돼 버림", "선점 토큰: park가 이김, 늦은 결과 무시"),
    ("원격 DB 교체·복원", "원본 없이 완료 처리 가능", "시도마다 원본 확인·재기록"),
]


def fig_scenarios():
    o = [f'<rect x="60" y="170" width="1800" height="60" rx="10" fill="#1f4d39"/>',
         text(90, 210, "상황", 24, "#fff", 700), text(560, 210, "처음 구조", 24, "#fff", 700),
         text(1180, 210, "현재 구조", 24, "#fff", 700)]
    for i, (sc, before, after) in enumerate(SCENARIOS):
        y = 240 + i * 90
        fill = "#ffffff" if i % 2 else "#f6f8f9"
        o.append(f'<rect x="60" y="{y}" width="1800" height="84" rx="8" fill="{fill}" stroke="#e1e5ea"/>')
        o.append(text(90, y + 50, sc, 23, INK, 700))
        o.append(text(560, y + 50, "⚠ " + before, 21, RED))
        o.append(text(1180, y + 50, "✔ " + after, 21, GREEN, 600))
    return svg("".join(o), "⑤ 장애·예외 상황 대응 비교", "로컬 스택에서 직접 재현해 확인한 결과")


def chip(x, y, label, kind):
    fill, stroke = {"ok": ("#eaf6ef", GREEN), "bad": ("#fdecea", RED), "wait": ("#fff4e0", "#c47a12"),
                    "plain": ("#f4f5f7", "#7b8494")}[kind]
    return (f'<rect x="{x}" y="{y}" width="120" height="54" rx="10" fill="{fill}" stroke="{stroke}" stroke-width="2.5"/>'
            + text(x + 60, y + 35, label, 22, INK, 700, "middle"))


def fig_sequence():
    o = []
    # left: before
    o.append(f'<rect x="50" y="160" width="890" height="880" rx="16" fill="#fff7f6" stroke="{RED}" stroke-width="2.5"/>')
    o.append(text(80, 205, "이전: 500건 묶음마다 다른 적재 시각", 28, RED, 800))
    o.append(text(80, 260, "Silver 한 배치(1초)를 500건씩 끊어 outbox에 넣음", 21, INK))
    o.append(text(80, 330, "묶음 1 · 12:00:00.425", 21, MUTED, 700))
    o.append(chip(330, 300, "A #1", "plain") + chip(470, 300, "A #3", "bad") + chip(610, 300, "B #1", "plain"))
    o.append(text(80, 420, "묶음 2 · 12:00:00.447", 21, MUTED, 700))
    o.append(chip(330, 390, "A #2", "bad") + chip(470, 390, "A #4", "plain") + chip(610, 390, "B #2", "plain"))
    o.append(text(80, 510, "작업자는 '적재 시각이 빠른 순'으로 200건 선점", 21, INK))
    o.append(text(80, 550, "→ A #3은 들어가고 A #2는 다음 선점으로 밀림", 21, RED, 700))
    o.append(chip(80, 590, "A #3", "wait"))
    o.append(text(220, 625, "앞 순번(#2)이 없음 → 순번 대기, 5초 뒤 재시도", 21, INK))
    o.append(chip(80, 670, "A #4", "wait"))
    o.append(text(220, 705, "같은 묶음의 뒤 순번도 5초 뒤로 되돌림", 21, INK))
    o.append(text(80, 790, "#2가 저장돼도 #3 하나만 깨움, #4 이후는 계속 대기", 21, INK))
    o.append(f'<rect x="80" y="840" width="830" height="160" rx="12" fill="#fdecea"/>')
    o.append(text(110, 890, "9,000건 비우기: 44%가 한 번 이상 재시도", 26, RED, 800))
    o.append(text(110, 940, "처리 속도 53건/초 (왕복만 줄였을 때)", 24, RED, 700))
    # right: after
    o.append(f'<rect x="980" y="160" width="890" height="880" rx="16" fill="#f3faf5" stroke="{GREEN}" stroke-width="2.5"/>')
    o.append(text(1010, 205, "이후: 한 배치는 같은 적재 시각", 28, GREEN, 800))
    o.append(text(1010, 260, "묶음은 나눠도 시각은 배치 시작 시각 하나로 통일", 21, INK))
    o.append(text(1010, 330, "모든 묶음 · 12:00:00.425", 21, MUTED, 700))
    o.append(chip(1260, 300, "A #1", "ok") + chip(1400, 300, "B #1", "ok") + chip(1540, 300, "A #2", "ok"))
    o.append(chip(1260, 390, "B #2", "ok") + chip(1400, 390, "A #3", "ok") + chip(1540, 390, "A #4", "ok"))
    o.append(text(1010, 510, "시각이 같으면 '순번이 작은 것부터' 정렬", 21, INK))
    o.append(text(1010, 550, "→ 같은 세션의 앞 순번이 항상 먼저 선점됨", 21, GREEN, 700))
    o.append(text(1010, 625, "+ 앞 순번이 저장되면 그 세션의 뒤 대기 행을", 21, INK))
    o.append(text(1030, 660, "모두 함께 깨움 (예전: 바로 다음 1건만)", 21, INK))
    o.append(text(1010, 720, "+ 순번 대기 재시도는 1초부터 (예전 5초)", 21, INK))
    o.append(text(1010, 790, "세션 안 순서 규칙·충돌 검사는 그대로", 21, INK))
    o.append(f'<rect x="1010" y="840" width="830" height="160" rx="12" fill="#e3f3e8"/>')
    o.append(text(1040, 890, "9,000건 비우기: 219건/초 (이전 코드 34.7)", 26, GREEN, 800))
    o.append(text(1040, 940, "초당 200건 유입에서 평균 대기 48.8초 → 2.9초", 24, GREEN, 700))
    o.append(arrow([(940, 600), (980, 600)]))
    return svg("".join(o), "⑥ 원격 적재가 느렸던 진짜 원인: 순번 역전 선점",
               "8차 보완 · 세션 안 이벤트는 순번대로만 적재해야 함 (2번보다 3번이 먼저 들어가면 기록이 꼬임)")


FIGS = {"fig-1-first": fig_first, "fig-2-current": fig_current, "fig-3-rounds": fig_rounds,
        "fig-4-metrics": fig_metrics, "fig-5-scenarios": fig_scenarios,
        "fig-6-sequence": fig_sequence}

if __name__ == "__main__":
    for name, fn in FIGS.items():
        (OUT / f"{name}.svg").write_text(fn(), encoding="utf-8")
        print("wrote", name)

# -*- coding: utf-8 -*-
# ============================================================
# Painel de RH — (multi-meses, replicável por marca)
# Lê índice (Google Sheets) com URL/MÊS/ATIVO e carrega BASE GERAL
# ============================================================

import os
import io
import re
import json
import unicodedata
import calendar
from datetime import datetime, date
from typing import Optional, Tuple

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt

import gspread
from google.oauth2 import service_account as gcreds
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ------------------ CONFIG BÁSICA ------------------
st.set_page_config(page_title="Painel de RH", layout="wide")
st.title("Painel de RH")

st.markdown(
    """
<style>
.card-wrap{display:flex;gap:14px;flex-wrap:wrap;margin:10px 0 6px;}
.card{background:#f7f7f9;border-radius:12px;box-shadow:0 1px 4px rgba(0,0,0,.06);padding:14px 16px;min-width:210px;flex:1;text-align:center}
.card h4{margin:0 0 6px;font-size:13px;color:#7a1f1f;font-weight:800}
.card h2{margin:0;font-size:26px;font-weight:900;color:#222}
.card .sub{margin-top:8px;display:inline-block;padding:6px 10px;border-radius:8px;font-size:12px;font-weight:800}
.sub.ok{background:#e8f5ec;color:#197a31;border:1px solid #cce9d4}
.sub.bad{background:#fdeaea;color:#a31616;border:1px solid #f2cccc}
.sub.neu{background:#f1f1f4;color:#444;border:1px solid #e4e4e8}
.section{font-size:18px;font-weight:900;margin:20px 0 8px}
.small{color:#666;font-size:12px}
</style>
""",
    unsafe_allow_html=True,
)

fast_mode = st.toggle("Modo rápido (pular gráficos/tabelas pesadas)", value=False)


# ------------------ SECRETS / CREDENCIAIS ------------------
def _get_clients():
    """
    Espera no secrets.toml:
    rh_index_sheet_id = "..."
    [gcp_service_account]
    ... json da service account ...
    """
    idx_id = st.secrets.get("rh_index_sheet_id", "").strip()
    if not idx_id:
        st.error("Faltou `rh_index_sheet_id` no secrets.toml.")
        st.stop()

    try:
        sa_block = st.secrets["gcp_service_account"]
    except Exception:
        st.error("Não encontrei [gcp_service_account] no secrets.toml.")
        st.stop()

    if "json_path" in sa_block:
        path = sa_block["json_path"]
        if not os.path.isabs(path):
            path = os.path.join(os.path.dirname(__file__), path)
        try:
            with open(path, "r", encoding="utf-8") as f:
                info = json.load(f)
        except Exception as e:
            st.error(f"Não consegui abrir o JSON da service account: {path}")
            with st.expander("Detalhes"):
                st.exception(e)
            st.stop()
    else:
        info = dict(sa_block)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    creds = gcreds.Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)

    return idx_id, gc, drive, info.get("client_email", "")


RH_INDEX_ID, client, DRIVE, SA_EMAIL = _get_clients()


# ------------------ HELPERS ------------------
ID_RE = re.compile(r"/d/([a-zA-Z0-9-_]+)")
def _sheet_id(s: str) -> Optional[str]:
    s = (s or "").strip()
    m = ID_RE.search(s)
    if m:
        return m.group(1)
    return s if re.fullmatch(r"[A-Za-z0-9-_]{20,}", s) else None

def _strip_accents(s: str) -> str:
    if s is None:
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(ch))

def _upper(x):
    return str(x).upper().strip() if pd.notna(x) else ""

def _yes(v) -> bool:
    return str(v).strip().upper() in {"S", "SIM", "Y", "YES", "TRUE", "1"}

def parse_date_any(x):
    if pd.isna(x) or x == "":
        return pd.NaT
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        try:
            return (pd.to_datetime("1899-12-30") + pd.to_timedelta(int(x), unit="D")).date()
        except Exception:
            pass
    s = str(x).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        return pd.to_datetime(s).date()
    except Exception:
        return pd.NaT

def business_days_count(dini: date, dfim: date) -> int:
    if not (isinstance(dini, date) and isinstance(dfim, date) and dini <= dfim):
        return 0
    return len(pd.bdate_range(dini, dfim))

def _find_col(cols, *names) -> Optional[str]:
    norm = {re.sub(r"\W+", "", _strip_accents(c).upper()): c for c in cols}
    for nm in names:
        key = re.sub(r"\W+", "", _strip_accents(nm).upper())
        if key in norm:
            return norm[key]
    return None

def parse_month_to_ym(m: str) -> Optional[str]:
    """
    Aceita: "12/2025", "12-2025", "2025-12", "2025/12", "2025-12-01"
    Retorna: "2025-12"
    """
    if m is None:
        return None
    s = str(m).strip()
    if not s:
        return None

    # MM/YYYY
    mm_yyyy = re.match(r"^(\d{1,2})\s*[\/\-]\s*(\d{4})$", s)
    if mm_yyyy:
        mm = int(mm_yyyy.group(1))
        yy = int(mm_yyyy.group(2))
        if 1 <= mm <= 12:
            return f"{yy:04d}-{mm:02d}"

    # YYYY-MM (ou YYYY/MM)
    yyyy_mm = re.match(r"^(\d{4})\s*[\/\-]\s*(\d{1,2})$", s)
    if yyyy_mm:
        yy = int(yyyy_mm.group(1))
        mm = int(yyyy_mm.group(2))
        if 1 <= mm <= 12:
            return f"{yy:04d}-{mm:02d}"

    # tenta datetime
    try:
        dt = pd.to_datetime(s, errors="raise")
        return f"{int(dt.year):04d}-{int(dt.month):02d}"
    except Exception:
        return None


# ------------------ DRIVE HELPERS (cache) ------------------
@st.cache_data(ttl=300, show_spinner=False)
def _drive_get_file_metadata(file_id: str) -> dict:
    return DRIVE.files().get(fileId=file_id, fields="id,name,mimeType").execute()

@st.cache_data(ttl=300, show_spinner=False)
def _drive_download_bytes(file_id: str) -> bytes:
    req = DRIVE.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


# ------------------ LEITURA ÍNDICE (cache) ------------------
@st.cache_data(ttl=300, show_spinner=False)
def read_index(sheet_id: str, tab: Optional[str] = None) -> pd.DataFrame:
    sh = client.open_by_key(sheet_id)

    if tab is None:
        tab = sh.worksheets()[0].title

    ws = sh.worksheet(tab)
    rows = ws.get_all_records()
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["URL", "MÊS", "ATIVO"])

    df.columns = [str(c).strip().upper() for c in df.columns]
    for need in ["URL", "MÊS", "ATIVO"]:
        if need not in df.columns:
            df[need] = ""

    return df


# ------------------ LEITURA BASE RH DO MÊS (cache) ------------------
@st.cache_data(ttl=300, show_spinner=False)
def read_rh_month(file_id: str, ym_from_index: str) -> Tuple[pd.DataFrame, str]:
    """
    Lê a aba BASE GERAL de um arquivo mensal (Google Sheet ou XLSX no Drive).
    O mês (YM) é FORÇADO pelo índice (ym_from_index), evitando NaT.
    """
    meta = _drive_get_file_metadata(file_id)
    title = meta.get("name", file_id)
    mime = meta.get("mimeType", "")

    if mime == "application/vnd.google-apps.spreadsheet":
        sh = client.open_by_key(file_id)
        try:
            ws = sh.worksheet("BASE GERAL")
        except Exception as e:
            raise RuntimeError(f"O arquivo '{title}' não possui aba 'BASE GERAL'.") from e
        df = pd.DataFrame(ws.get_all_records())
        if df.empty:
            out = pd.DataFrame()
            out["YM"] = []
            out["SRC_FILE"] = []
            return out, title
        df.columns = [str(c).strip() for c in df.columns]
    else:
        content = _drive_download_bytes(file_id)
        try:
            df = pd.read_excel(io.BytesIO(content), sheet_name="BASE GERAL", engine="openpyxl")
        except ValueError as e:
            raise RuntimeError(f"O arquivo '{title}' não possui aba 'BASE GERAL'.") from e
        df.columns = [str(c).strip() for c in df.columns]

    cols = list(df.columns)

    c_cidade  = _find_col(cols, "CIDADE", "UNIDADE")
    c_nome    = _find_col(cols, "NOME DO COLABORADOR", "COLABORADOR", "NOME")
    c_cpf     = _find_col(cols, "CPF")
    c_nasc    = _find_col(cols, "DATA DE NASCIMENTO", "NASCIMENTO")
    c_funcao  = _find_col(cols, "FUNÇÃO", "FUNCAO", "CARGO")
    c_adm     = _find_col(cols, "DATA DE ADMISSÃO", "DATA DE ADMISSAO", "ADMISSAO")
    c_dem     = _find_col(cols, "DATA DE DEMISSÃO", "DATA DE DEMISSAO", "DEMISSAO")
    c_motivo  = _find_col(cols, "MOTIVO DEMISSÃO", "MOTIVO DEMISSAO", "MOTIVO DA DEMISSÃO", "MOTIVO DA DEMISSAO")
    c_status  = _find_col(cols, "STATUS")
    c_du      = _find_col(cols, "DIAS ÚTEIS MÊS", "DIAS UTEIS MES", "DIAS_UTEIS_MES")
    c_faltas  = _find_col(cols, "TOTAL DE FALTAS", "FALTAS")
    c_superv  = _find_col(cols, "SUPERVISOR")

    out = pd.DataFrame()
    out["CIDADE"]  = df[c_cidade] if c_cidade else ""
    out["NOME"]    = df[c_nome] if c_nome else ""
    out["CPF"]     = df[c_cpf] if c_cpf else ""
    out["NASCIMENTO"] = df[c_nasc] if c_nasc else ""
    out["FUNCAO"]  = df[c_funcao] if c_funcao else ""
    out["ADMISSAO"] = df[c_adm] if c_adm else ""
    out["DEMISSAO"] = df[c_dem] if c_dem else ""
    out["MOTIVO_DEMISSAO"] = df[c_motivo] if c_motivo else ""
    out["STATUS"]  = df[c_status] if c_status else ""
    out["DIAS_UTEIS_MES"] = df[c_du] if c_du else np.nan
    out["FALTAS_MES"] = df[c_faltas] if c_faltas else 0
    out["SUPERVISOR"] = df[c_superv] if c_superv else ""

    out["CIDADE"] = out["CIDADE"].astype(str).map(_upper)
    out["NOME"] = out["NOME"].astype(str).str.strip()
    out["FUNCAO"] = out["FUNCAO"].astype(str).map(_upper)
    out["STATUS"] = out["STATUS"].astype(str).map(_upper)
    out["SUPERVISOR"] = out["SUPERVISOR"].astype(str).map(_upper)

    out["ADMISSAO"] = out["ADMISSAO"].apply(parse_date_any)
    out["DEMISSAO"] = out["DEMISSAO"].apply(parse_date_any)

    out["DIAS_UTEIS_MES"] = pd.to_numeric(out["DIAS_UTEIS_MES"], errors="coerce")
    out["FALTAS_MES"] = pd.to_numeric(out["FALTAS_MES"], errors="coerce").fillna(0).astype(int)

    out = out[out["NOME"].astype(str).str.strip() != ""].copy()

    # >>> MÊS FORÇADO PELO ÍNDICE <<<
    out["YM"] = ym_from_index
    out["SRC_FILE"] = title

    return out, title


# ------------------ CARREGA ÍNDICE ------------------
idx = read_index(RH_INDEX_ID).copy()
idx["URL"] = idx["URL"].astype(str)
idx["MÊS"] = idx["MÊS"].astype(str).str.strip()
idx["ATIVO"] = idx["ATIVO"].astype(str)

idx = idx[idx["ATIVO"].map(_yes)].copy()
if idx.empty:
    st.error("Seu índice não tem linhas ATIVAS (ATIVO = S).")
    st.stop()

# normaliza YM a partir da coluna MÊS do índice
idx["YM"] = idx["MÊS"].apply(parse_month_to_ym)
idx = idx[~idx["YM"].isna()].copy()
if idx.empty:
    st.error("A coluna MÊS do índice não está em um formato reconhecido (ex: 12/2025).")
    st.stop()

# ------------------ CARREGA BASES DOS MESES ------------------
ok_msgs, err_msgs = [], []
all_months = []

for _, r in idx.iterrows():
    fid = _sheet_id(r.get("URL", ""))
    ym = r.get("YM", "")
    if not fid or not ym:
        continue
    try:
        d, ttl = read_rh_month(fid, ym_from_index=ym)
        if not d.empty:
            all_months.append(d)
        ok_msgs.append(f"{ym} — {ttl} ({len(d)} linhas)")
    except Exception as e:
        err_msgs.append((fid, ym, e))

if not all_months:
    st.error("Não consegui ler nenhuma BASE GERAL dos meses (verifique links e permissões).")
    with st.expander("Erros"):
        for fid, ym, e in err_msgs:
            st.write(f"{ym} — {fid}")
            st.exception(e)
    st.stop()

df = pd.concat(all_months, ignore_index=True)

# meses disponíveis (agora SEM NaT)
ym_all = sorted(df["YM"].dropna().unique().tolist())
if not ym_all:
    st.error("Não encontrei meses válidos para o filtro.")
    st.stop()

# ------------------ FILTRO MÊS ------------------
label_map = {f"{m[5:]}/{m[:4]}": m for m in ym_all}  # MM/YYYY -> YYYY-MM
sel_label = st.selectbox("Mês de referência", options=list(label_map.keys()), index=len(ym_all) - 1)
ym_sel = label_map[sel_label]

ref_year, ref_month = int(ym_sel[:4]), int(ym_sel[5:7])

df_m = df[df["YM"] == ym_sel].copy()

# ------------------ PERÍODO NO MÊS ------------------
month_start = date(ref_year, ref_month, 1)
last_day = calendar.monthrange(ref_year, ref_month)[1]
month_end = date(ref_year, ref_month, last_day)

c1, c2 = st.columns([1.2, 2.8])
with c1:
    drange = st.date_input(
        "Período (dentro do mês)",
        value=(month_start, month_end),
        min_value=month_start,
        max_value=month_end,
        format="DD/MM/YYYY",
    )
start_d, end_d = (drange if isinstance(drange, tuple) and len(drange) == 2 else (month_start, month_end))

# ------------------ FILTROS CATEGÓRICOS ------------------
cidades = sorted(df_m["CIDADE"].dropna().unique().tolist())
funcoes = sorted(df_m["FUNCAO"].dropna().unique().tolist())
status_opts = sorted(df_m["STATUS"].dropna().unique().tolist())
superv_opts = sorted(df_m["SUPERVISOR"].dropna().unique().tolist())

with c2:
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        f_cidade = st.multiselect("Cidade", cidades, default=cidades)
    with f2:
        f_funcao = st.multiselect("Função", funcoes, default=funcoes)
    with f3:
        f_status = st.multiselect("Status", status_opts, default=status_opts)
    with f4:
        f_superv = st.multiselect("Supervisor", superv_opts)

view = df_m.copy()
if f_cidade:
    view = view[view["CIDADE"].isin([_upper(x) for x in f_cidade])]
if f_funcao:
    view = view[view["FUNCAO"].isin([_upper(x) for x in f_funcao])]
if f_status:
    view = view[view["STATUS"].isin([_upper(x) for x in f_status])]
if f_superv:
    view = view[view["SUPERVISOR"].isin([_upper(x) for x in f_superv])]

if view.empty:
    st.info("Sem dados no recorte selecionado.")
    st.stop()

# ------------------ CÁLCULOS ------------------
def is_active_asof(row, asof: date) -> bool:
    adm = row["ADMISSAO"]
    dem = row["DEMISSAO"]
    if not isinstance(adm, date):
        return False
    if adm > asof:
        return False
    if isinstance(dem, date) and dem <= asof:
        return False
    return True

def count_active_asof(df_in: pd.DataFrame, asof: date) -> int:
    if df_in.empty:
        return 0
    mask = df_in.apply(lambda r: is_active_asof(r, asof), axis=1)
    return int(mask.sum())

hc_start = count_active_asof(view, start_d)
hc_end = count_active_asof(view, end_d)
hc_avg = (hc_start + hc_end) / 2 if (hc_start + hc_end) > 0 else 0

adm_period = view[view["ADMISSAO"].apply(lambda d: isinstance(d, date) and start_d <= d <= end_d)]
dem_period = view[view["DEMISSAO"].apply(lambda d: isinstance(d, date) and start_d <= d <= end_d)]
n_adm = int(len(adm_period))
n_dem = int(len(dem_period))

turnover = np.nan
if hc_avg > 0:
    turnover = (((n_adm + n_dem) / 2) / hc_avg) * 100

du_mes = pd.to_numeric(view["DIAS_UTEIS_MES"], errors="coerce").dropna()
dias_uteis_mes = int(du_mes.mode().iloc[0]) if len(du_mes) else business_days_count(month_start, month_end)

faltas_total_mes = int(pd.to_numeric(view["FALTAS_MES"], errors="coerce").fillna(0).sum())

abs_rate = np.nan
den_abs = hc_end * dias_uteis_mes
if den_abs > 0:
    abs_rate = (faltas_total_mes / den_abs) * 100

pend_cadastro = int((view["ADMISSAO"].isna() | (view["FUNCAO"].astype(str).str.strip() == "")).sum())

def fmt_pct(x):
    return "—" if pd.isna(x) else f"{x:.1f}%".replace(".", ",")

cards_html = f"""
<div class="card-wrap">
  <div class='card'>
    <h4>Headcount (fim do período)</h4>
    <h2>{hc_end:,}</h2>
    <span class='sub neu'>início: {hc_start:,} | médio: {hc_avg:.1f}</span>
  </div>
  <div class='card'>
    <h4>Admissões (período)</h4>
    <h2>{n_adm:,}</h2>
  </div>
  <div class='card'>
    <h4>Demissões (período)</h4>
    <h2>{n_dem:,}</h2>
  </div>
  <div class='card'>
    <h4>Turnover (período)</h4>
    <h2>{fmt_pct(turnover)}</h2>
    <span class='sub neu'>((adm+dem)/2)/HC médio</span>
  </div>
  <div class='card'>
    <h4>Faltas (mês)</h4>
    <h2>{faltas_total_mes:,}</h2>
    <span class='sub neu'>absenteísmo: {fmt_pct(abs_rate)}</span>
  </div>
  <div class='card'>
    <h4>Pendências de cadastro</h4>
    <h2>{pend_cadastro:,}</h2>
  </div>
</div>
"""
st.markdown(cards_html.replace(",", "."), unsafe_allow_html=True)

# ------------------ GRÁFICOS ------------------
def bar_with_labels(df_plot, x_col, y_col, height=320, x_title="", y_title="QTD"):
    base = alt.Chart(df_plot).encode(
        x=alt.X(f"{x_col}:N", sort="-y", title=x_title, axis=alt.Axis(labelAngle=0, labelLimit=220)),
        y=alt.Y(f"{y_col}:Q", title=y_title),
        tooltip=[x_col, y_col],
    )
    bars = base.mark_bar()
    labels = base.mark_text(dy=-6).encode(text=alt.Text(f"{y_col}:Q", format=".0f"))
    return (bars + labels).properties(height=height)

g1, g2 = st.columns(2)

with g1:
    st.markdown("<div class='section'>Headcount por função (fim do período)</div>", unsafe_allow_html=True)
    tmp = view.copy()
    tmp["ATIVO_ASOF"] = tmp.apply(lambda r: 1 if is_active_asof(r, end_d) else 0, axis=1)
    by_func = tmp.groupby("FUNCAO")["ATIVO_ASOF"].sum().reset_index(name="QTD").sort_values("QTD", ascending=False)
    if len(by_func):
        st.altair_chart(bar_with_labels(by_func, "FUNCAO", "QTD", height=340, x_title="FUNÇÃO"), use_container_width=True)
    else:
        st.info("Sem dados.")

with g2:
    st.markdown("<div class='section'>Admissões e demissões por função (período)</div>", unsafe_allow_html=True)
    a = adm_period.groupby("FUNCAO")["NOME"].size().reset_index(name="ADM")
    d = dem_period.groupby("FUNCAO")["NOME"].size().reset_index(name="DEM")
    m = a.merge(d, on="FUNCAO", how="outer").fillna(0)
    m["ADM"] = m["ADM"].astype(int)
    m["DEM"] = m["DEM"].astype(int)
    if len(m):
        m_long = m.melt(id_vars=["FUNCAO"], value_vars=["ADM", "DEM"], var_name="TIPO", value_name="QTD")
        chart = alt.Chart(m_long).mark_bar().encode(
            x=alt.X("FUNCAO:N", sort="-y", axis=alt.Axis(labelAngle=0, labelLimit=220), title="FUNÇÃO"),
            y=alt.Y("QTD:Q", title="QTD"),
            color=alt.Color("TIPO:N", legend=alt.Legend(title="")),
            tooltip=["FUNCAO", "TIPO", "QTD"],
        ).properties(height=340)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Sem admissões/demissões no período.")

if not fast_mode:
    st.markdown("<div class='section'>Top faltas por colaborador (mês)</div>", unsafe_allow_html=True)
    top_faltas = view.copy()
    top_faltas["FALTAS_MES"] = pd.to_numeric(top_faltas["FALTAS_MES"], errors="coerce").fillna(0).astype(int)
    top_faltas = top_faltas.sort_values("FALTAS_MES", ascending=False).head(15)
    if len(top_faltas):
        chart = alt.Chart(top_faltas).mark_bar().encode(
            x=alt.X("NOME:N", sort="-y", axis=alt.Axis(labelAngle=0, labelLimit=260), title="COLABORADOR"),
            y=alt.Y("FALTAS_MES:Q", title="FALTAS"),
            tooltip=["NOME", "CIDADE", "FUNCAO", "FALTAS_MES"],
        ).properties(height=340)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Sem faltas.")

# ------------------ TABELA + EXPORTAÇÃO ------------------
st.markdown("<div class='section'>Base (recorte atual)</div>", unsafe_allow_html=True)

cols_show = [
    "CIDADE", "NOME", "CPF", "FUNCAO", "ADMISSAO", "DEMISSAO",
    "MOTIVO_DEMISSAO", "STATUS", "FALTAS_MES", "DIAS_UTEIS_MES", "SUPERVISOR"
]
for c in cols_show:
    if c not in view.columns:
        view[c] = ""

df_show = view[cols_show].copy()
st.dataframe(df_show, use_container_width=True, hide_index=True)

try:
    import openpyxl  # noqa
    xbuf = io.BytesIO()
    with pd.ExcelWriter(xbuf, engine="openpyxl") as writer:
        df_show.to_excel(writer, index=False, sheet_name="BASE_RECORTE")
        resumo = pd.DataFrame(
            {
                "Métrica": ["Headcount fim", "Headcount início", "Headcount médio", "Admissões", "Demissões", "Turnover %", "Faltas", "Absenteísmo %"],
                "Valor": [
                    hc_end, hc_start, round(hc_avg, 1), n_adm, n_dem,
                    None if pd.isna(turnover) else round(float(turnover), 1),
                    faltas_total_mes,
                    None if pd.isna(abs_rate) else round(float(abs_rate), 2),
                ],
            }
        )
        resumo.to_excel(writer, index=False, sheet_name="RESUMO")
    xbuf.seek(0)

    st.download_button(
        "Baixar Excel (recorte + resumo)",
        data=xbuf,
        file_name=f"rh_recorte_{ym_sel}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
except Exception:
    st.caption("<span class='small'>Exportação Excel indisponível no ambiente.</span>", unsafe_allow_html=True)

with st.expander("Diagnóstico", expanded=False):
    st.write("Service account:", SA_EMAIL)
    st.write("Meses carregados:", ym_all)
    if err_msgs:
        st.write("Falhas ao carregar alguns arquivos:")
        for fid, ym, e in err_msgs:
            st.write(f"{ym} — {fid}")
            st.exception(e)

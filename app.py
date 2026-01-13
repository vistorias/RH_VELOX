import re
from datetime import date
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Painel RH - VELOX", layout="wide")

# Pode ser ID ou URL do índice
INDEX_SHEET_REF = "1N5D_ARAgpXMNsHKZZJBc1JGom5JUCG84cxiDxE3QcLo"

GERENTE_POR_CIDADE = {
    "IMPERATRIZ": "Jorge Alexandre Bezerra da Costa",
    "ESTREITO": "Jorge Alexandre Bezerra da Costa",
    "SÃO LUIS": "Moisés Santos do Nascimento",
    "SAO LUIS": "Moisés Santos do Nascimento",
    "PEDREIRAS": "Moisés Santos do Nascimento",
    "GRAJAÚ": "Moisés Santos do Nascimento",
    "GRAJAU": "Moisés Santos do Nascimento",
}

# Abas reais (conforme sua base)
TAB_BASE_GERAL = "BASE GERAL"
TAB_BASE_PRESENCA = "BASE PRESENÇA"
TAB_TREINAMENTOS = "TREINAMENTOS"
TAB_ABS_TURNOVER = "ABSENTEISMO E TURNOVER"

INDEX_TAB_NAME = "Página1"  # sua planilha índice

# =========================
# HELPERS
# =========================
def extract_sheet_id(url_or_id: str) -> str:
    if not isinstance(url_or_id, str):
        return ""
    s = url_or_id.strip()
    if s.startswith("http"):
        m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", s)
        return m.group(1) if m else ""
    return s  # já é um ID

def norm_text(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = s.replace("\u00a0", " ")
    return s

def norm_name(x):
    s = norm_text(x).upper()
    s = re.sub(r"\s+", " ", s)
    return s

def norm_city(x):
    s = norm_text(x).upper()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("SAO LUIS", "SÃO LUIS")
    s = s.replace("GRAJAU", "GRAJAÚ")
    return s

def parse_date_safe(x):
    if pd.isna(x) or str(x).strip() == "":
        return pd.NaT
    return pd.to_datetime(x, dayfirst=True, errors="coerce")

def month_start_end(mes_ref: str):
    m, y = mes_ref.split("/")
    y = int(y)
    m = int(m)
    start = date(y, m, 1)
    end = (start + relativedelta(months=1)) - relativedelta(days=1)
    return start, end

def kpi_card(col, title, value, subtitle=None):
    with col:
        st.markdown(
            f"""
            <div style="border:1px solid #e6e6e6;border-radius:14px;padding:14px;">
              <div style="font-size:12px;color:#666;margin-bottom:4px;">{title}</div>
              <div style="font-size:26px;font-weight:700;line-height:1.1;">{value}</div>
              <div style="font-size:12px;color:#666;margin-top:6px;">{subtitle or ""}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

def make_unique_columns(cols):
    """Garante nomes únicos: se repetir, vira 'COL', 'COL__2', 'COL__3'..."""
    seen = {}
    out = []
    for c in cols:
        c0 = norm_text(c)
        if c0 == "":
            c0 = "COL"
        if c0 not in seen:
            seen[c0] = 1
            out.append(c0)
        else:
            seen[c0] += 1
            out.append(f"{c0}__{seen[c0]}")
    return out

def collapse_duplicate_columns(df: pd.DataFrame, base_name: str) -> pd.Series:
    """
    Se houver colunas base_name, base_name__2, base_name__3...,
    retorna uma Series com o primeiro valor não vazio por linha.
    """
    candidates = [c for c in df.columns if c == base_name or c.startswith(base_name + "__")]
    if not candidates:
        return pd.Series([np.nan] * len(df), index=df.index)

    tmp = df[candidates].copy()

    # transforma tudo em string "limpa" para identificar vazio
    for c in candidates:
        tmp[c] = tmp[c].apply(norm_text)

    # pega primeiro não vazio
    out = tmp[candidates[0]].copy()
    for c in candidates[1:]:
        mask = (out == "") | out.isna()
        out.loc[mask] = tmp.loc[mask, c]
    return out

def ensure_numeric_series(s):
    return pd.to_numeric(s, errors="coerce").fillna(0)

def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    return gspread.authorize(creds)

def open_sheet(gc, url_or_id: str):
    ref = str(url_or_id).strip()
    if ref.startswith("http"):
        return gc.open_by_url(ref)
    return gc.open_by_key(ref)

@st.cache_data(ttl=600, show_spinner=False)
def read_worksheet_as_df(sheet_ref: str, tab_name: str) -> pd.DataFrame:
    gc = get_gspread_client()
    sh = open_sheet(gc, sheet_ref)
    ws = sh.worksheet(tab_name)
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return pd.DataFrame()

    header = values[0]
    data = values[1:]
    df = pd.DataFrame(data, columns=header)

    # normaliza e garante colunas únicas
    df.columns = make_unique_columns(df.columns)
    return df

@st.cache_data(ttl=600, show_spinner=False)
def read_index_df(index_ref: str) -> pd.DataFrame:
    df = read_worksheet_as_df(index_ref, INDEX_TAB_NAME)
    if df.empty:
        return df

    # tenta detectar colunas (no seu print: URL | MÊS | ATIVO)
    cols_upper = {c.upper(): c for c in df.columns}

    url_col = cols_upper.get("URL_BASE") or cols_upper.get("URL")
    mes_col = cols_upper.get("MES_REF") or cols_upper.get("MÊS") or cols_upper.get("MES")
    ativo_col = cols_upper.get("ATIVO")

    if not url_col or not mes_col or not ativo_col:
        return df  # para você visualizar no app se necessário

    out = df[[url_col, mes_col, ativo_col]].copy()
    out.columns = ["URL_BASE", "MES_REF", "ATIVO"]

    out["URL_BASE"] = out["URL_BASE"].apply(norm_text)
    out["MES_REF"] = out["MES_REF"].apply(norm_text)
    out["ATIVO"] = out["ATIVO"].apply(lambda x: norm_text(x).upper())

    out["SHEET_ID"] = out["URL_BASE"].apply(extract_sheet_id)
    return out

def build_base_geral(df_bg: pd.DataFrame) -> pd.DataFrame:
    if df_bg.empty:
        return df_bg

    # renomeia por heurística
    colmap = {}
    for c in df_bg.columns:
        cu = c.upper()

        if cu in ["NOME", "COLABORADOR", "FUNCIONARIO"]:
            colmap[c] = "NOME"
        elif cu in ["CIDADE", "UNIDADE"]:
            colmap[c] = "CIDADE"
        elif cu in ["FUNÇÃO", "FUNCAO", "CARGO"]:
            colmap[c] = "FUNCAO"
        elif cu in ["SUPERVISOR", "LIDER", "LÍDER"]:
            colmap[c] = "SUPERVISOR"
        elif cu.strip() == "STATUS":
            colmap[c] = "STATUS"
        elif "DT NASC" in cu or "NASC" in cu:
            colmap[c] = "DT_NASCIMENTO"
        elif "DT ADMIS" in cu or "ADMISS" in cu:
            colmap[c] = "DT_ADMISSAO"
        elif "DT DEMIS" in cu or "DEMISS" in cu:
            colmap[c] = "DT_DEMISSAO"
        elif "MOTIVO" in cu:
            colmap[c] = "MOTIVO_DEMISSAO"
        elif "DIAS ÚTEIS" in cu or "DIAS UTEIS" in cu:
            colmap[c] = "DIAS_UTEIS"
        elif cu.strip() == "E-MAIL" or cu.strip() == "EMAIL":
            colmap[c] = "EMAIL"

    df = df_bg.rename(columns=colmap).copy()

    # ✅ consolida duplicadas que viraram DT_DEMISSAO__2 etc.
    for base_col in ["DT_ADMISSAO", "DT_DEMISSAO", "DT_NASCIMENTO", "STATUS", "CIDADE", "NOME", "FUNCAO", "SUPERVISOR", "MOTIVO_DEMISSAO", "DIAS_UTEIS", "EMAIL"]:
        if base_col in df.columns or any(c.startswith(base_col + "__") for c in df.columns):
            df[base_col] = collapse_duplicate_columns(df, base_col)
            # remove colunas duplicadas (mantém só a base_col)
            drop_cols = [c for c in df.columns if c.startswith(base_col + "__")]
            df = df.drop(columns=drop_cols, errors="ignore")

    # normalizações
    df["NOME_NORM"] = df["NOME"].apply(norm_name) if "NOME" in df.columns else ""
    df["CIDADE"] = df["CIDADE"].apply(norm_city) if "CIDADE" in df.columns else ""

    if "FUNCAO" in df.columns:
        df["FUNCAO"] = df["FUNCAO"].apply(lambda x: norm_text(x).upper())
        df["FUNCAO"] = df["FUNCAO"].str.replace("VISTORIADORA", "VISTORIADOR", regex=False)

    if "STATUS" in df.columns:
        df["STATUS"] = df["STATUS"].apply(lambda x: norm_text(x).upper())

    df["GERENTE_RESPONSAVEL"] = df["CIDADE"].map(GERENTE_POR_CIDADE).fillna("Não mapeado")

    # datas
    for col in ["DT_ADMISSAO", "DT_DEMISSAO", "DT_NASCIMENTO"]:
        if col in df.columns:
            df[col] = df[col].apply(parse_date_safe)

    return df

def build_presenca(df_pres: pd.DataFrame) -> pd.DataFrame:
    if df_pres.empty:
        return df_pres

    # detectar coluna nome
    name_col = None
    for c in df_pres.columns:
        if c.upper() in ["NOME", "COLABORADOR", "FUNCIONARIO"]:
            name_col = c
            break
    if not name_col:
        name_col = df_pres.columns[0]

    df = df_pres.copy()
    df["NOME_NORM"] = df[name_col].apply(norm_name)

    # colunas dia
    day_cols = []
    for c in df.columns:
        cu = c.strip()
        if cu.isdigit() and 1 <= int(cu) <= 31:
            day_cols.append(c)
        elif re.fullmatch(r"\d{2}", cu) and 1 <= int(cu) <= 31:
            day_cols.append(c)

    total_col = None
    for c in df.columns:
        if c.upper().strip() == "TOTAL":
            total_col = c
            break

    if total_col:
        df["FALTAS_TOTAL"] = ensure_numeric_series(df[total_col])
    else:
        if day_cols:
            faltas = np.zeros(len(df))
            for c in day_cols:
                faltas += ensure_numeric_series(df[c]).to_numpy()
            df["FALTAS_TOTAL"] = faltas
        else:
            df["FALTAS_TOTAL"] = 0

    return df[["NOME_NORM", "FALTAS_TOTAL"]]

def build_treinamentos(df_tr: pd.DataFrame) -> pd.DataFrame:
    if df_tr.empty:
        return df_tr

    name_col = None
    pres_col = None

    for c in df_tr.columns:
        cu = c.upper()
        if cu in ["NOME", "COLABORADOR", "FUNCIONARIO"]:
            name_col = c
        if "PRESEN" in cu:
            pres_col = c

    if not name_col:
        name_col = df_tr.columns[0]

    df = df_tr.copy()
    df["NOME_NORM"] = df[name_col].apply(norm_name)

    if pres_col:
        df["PRESENCA"] = df[pres_col].apply(lambda x: norm_text(x).upper())
    else:
        df["PRESENCA"] = ""

    df["PRESENCA_OK"] = df["PRESENCA"].isin(["SIM", "S", "OK", "PRESENTE"])
    return df[["NOME_NORM", "PRESENCA_OK"]]

def compute_metrics(df_base: pd.DataFrame, df_pres: pd.DataFrame, df_tr: pd.DataFrame, mes_ref: str):
    headcount = len(df_base)

    if "STATUS" in df_base.columns:
        status_norm = df_base["STATUS"].astype(str).str.upper().str.strip()
        ativos = int((status_norm == "ATIVO").sum())
    else:
        ativos = headcount
    desligados_total = headcount - ativos

    start, end = month_start_end(mes_ref)

    entradas = 0
    saidas = 0

    if "DT_ADMISSAO" in df_base.columns:
        adm = pd.to_datetime(df_base["DT_ADMISSAO"], errors="coerce", dayfirst=True)
        entradas = int(((adm.dt.date >= start) & (adm.dt.date <= end)).sum())

    if "DT_DEMISSAO" in df_base.columns:
        dem = pd.to_datetime(df_base["DT_DEMISSAO"], errors="coerce", dayfirst=True)
        saidas = int(((dem.dt.date >= start) & (dem.dt.date <= end)).sum())

    turnover = (((entradas + saidas) / 2) / headcount * 100) if headcount else 0

    faltas_total = float(df_pres["FALTAS_TOTAL"].sum()) if (df_pres is not None and not df_pres.empty) else 0.0

    dias_uteis = 22
    if "DIAS_UTEIS" in df_base.columns:
        du = pd.to_numeric(df_base["DIAS_UTEIS"], errors="coerce").dropna()
        if len(du) > 0:
            dias_uteis = int(float(du.iloc[0]))

    abs_pct = (faltas_total / (headcount * dias_uteis) * 100) if headcount and dias_uteis else 0

    if df_tr is not None and not df_tr.empty and "PRESENCA_OK" in df_tr.columns:
        pres_ok = int(df_tr["PRESENCA_OK"].sum())
        treino_pct = pres_ok / headcount * 100 if headcount else 0
    else:
        pres_ok = 0
        treino_pct = 0

    return {
        "headcount": headcount,
        "ativos": ativos,
        "desligados_total": desligados_total,
        "entradas": entradas,
        "saidas": saidas,
        "turnover_pct": turnover,
        "faltas_total": faltas_total,
        "dias_uteis": dias_uteis,
        "abs_pct": abs_pct,
        "treino_presencas": pres_ok,
        "treino_pct": treino_pct,
        "periodo_inicio": start,
        "periodo_fim": end,
    }

def add_age_tenure(df: pd.DataFrame, ref_date: date):
    out = df.copy()

    if "DT_NASCIMENTO" in out.columns:
        dn = pd.to_datetime(out["DT_NASCIMENTO"], errors="coerce", dayfirst=True)
        out["IDADE"] = (pd.to_datetime(ref_date) - dn).dt.days / 365.25
    else:
        out["IDADE"] = np.nan

    if "DT_ADMISSAO" in out.columns:
        da = pd.to_datetime(out["DT_ADMISSAO"], errors="coerce", dayfirst=True)
        out["TEMPO_CASA_ANOS"] = (pd.to_datetime(ref_date) - da).dt.days / 365.25
    else:
        out["TEMPO_CASA_ANOS"] = np.nan

    def faixa_idade(x):
        if pd.isna(x):
            return "Sem dado"
        if x < 18:
            return "<18 (inconsistente)"
        if x <= 24:
            return "18–24"
        if x <= 29:
            return "25–29"
        if x <= 34:
            return "30–34"
        if x <= 39:
            return "35–39"
        return "40+"

    def faixa_tempo(x):
        if pd.isna(x):
            return "Sem dado"
        if x < 0:
            return "Inconsistente"
        if x < 0.25:
            return "<3 meses"
        if x < 0.5:
            return "3–6 meses"
        if x < 1:
            return "6–12 meses"
        if x < 2:
            return "1–2 anos"
        if x < 5:
            return "2–5 anos"
        return "5+ anos"

    out["FAIXA_IDADE"] = out["IDADE"].apply(faixa_idade)
    out["FAIXA_TEMPO_CASA"] = out["TEMPO_CASA_ANOS"].apply(faixa_tempo)
    return out

def quality_alerts(df_base: pd.DataFrame, df_tr: pd.DataFrame):
    alerts = []
    if "FAIXA_IDADE" in df_base.columns:
        bad_age = df_base[df_base["FAIXA_IDADE"] == "<18 (inconsistente)"]
        if len(bad_age) > 0:
            alerts.append(f"Idade inconsistente: {len(bad_age)} registro(s) com idade < 18.")
    if df_tr is not None and not df_tr.empty:
        base_names = set(df_base["NOME_NORM"].tolist()) if "NOME_NORM" in df_base.columns else set()
        tr_names = set(df_tr["NOME_NORM"].tolist()) if "NOME_NORM" in df_tr.columns else set()
        extra = tr_names - base_names
        if len(extra) > 0:
            alerts.append(f"Treinamento com nomes fora da base: {len(extra)} (ex.: {sorted(list(extra))[:3]}).")
    return alerts

# =========================
# UI
# =========================
st.title("Painel de RH - VELOX Vistorias")

with st.sidebar:
    st.subheader("Controle")
    st.caption("O painel lê a planilha ÍNDICE e puxa a base do mês selecionado.")
    st.divider()

idx = read_index_df(INDEX_SHEET_REF)

if idx.empty or not set(["URL_BASE", "MES_REF", "ATIVO"]).issubset(set(idx.columns)):
    st.error("A aba do índice não está no formato esperado. Precisa de colunas: URL, MÊS, ATIVO.")
    st.dataframe(idx)
    st.stop()

ativos_idx = idx[idx["ATIVO"] == "S"].copy()
if ativos_idx.empty:
    st.error("Não há meses com ATIVO = S no índice.")
    st.dataframe(idx)
    st.stop()

mes_opts = ativos_idx["MES_REF"].tolist()
mes_sel = st.sidebar.selectbox("Mês de referência", mes_opts, index=len(mes_opts) - 1)

row = ativos_idx[ativos_idx["MES_REF"] == mes_sel].iloc[0]
base_ref = row["URL_BASE"]  # pode ser URL

# debug útil (pode remover depois)
with st.sidebar:
    st.caption("Valor vindo do índice:")
    st.write(base_ref)
    st.caption("ID extraído:")
    st.write(extract_sheet_id(base_ref))

with st.spinner("Carregando base do mês..."):
    df_bg_raw = read_worksheet_as_df(base_ref, TAB_BASE_GERAL)
    df_pr_raw = read_worksheet_as_df(base_ref, TAB_BASE_PRESENCA)
    df_tr_raw = read_worksheet_as_df(base_ref, TAB_TREINAMENTOS)

df_base = build_base_geral(df_bg_raw)
df_pres = build_presenca(df_pr_raw)
df_tr = build_treinamentos(df_tr_raw)

df = df_base.merge(df_pres, on="NOME_NORM", how="left")
df["FALTAS_TOTAL"] = df["FALTAS_TOTAL"].fillna(0)
df = df.merge(df_tr, on="NOME_NORM", how="left")
df["PRESENCA_OK"] = df["PRESENCA_OK"].fillna(False)

start, end = month_start_end(mes_sel)
df = add_age_tenure(df, end)

with st.sidebar:
    st.subheader("Filtros")
    cidades = sorted([c for c in df.get("CIDADE", pd.Series()).dropna().unique().tolist() if str(c).strip() != ""])
    funcoes = sorted([f for f in df.get("FUNCAO", pd.Series()).dropna().unique().tolist() if str(f).strip() != ""])
    gerentes = sorted([g for g in df.get("GERENTE_RESPONSAVEL", pd.Series()).dropna().unique().tolist() if str(g).strip() != ""])

    f_cidade = st.multiselect("Cidade", cidades, default=cidades)
    f_funcao = st.multiselect("Função", funcoes, default=funcoes)
    f_gerente = st.multiselect("Gerente", gerentes, default=gerentes)
    f_status = st.multiselect("Status", ["ATIVO", "DESLIGADO"], default=["ATIVO", "DESLIGADO"])

df_f = df[
    df["CIDADE"].isin(f_cidade)
    & df["FUNCAO"].isin(f_funcao)
    & df["GERENTE_RESPONSAVEL"].isin(f_gerente)
    & df["STATUS"].isin(f_status)
].copy()

metrics = compute_metrics(
    df_f,
    df_f[["NOME_NORM", "FALTAS_TOTAL"]] if "FALTAS_TOTAL" in df_f.columns else pd.DataFrame(),
    df_f[["NOME_NORM", "PRESENCA_OK"]] if "PRESENCA_OK" in df_f.columns else pd.DataFrame(),
    mes_sel,
)

st.subheader(f"Visão Geral | {mes_sel}")

c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(8)
kpi_card(c1, "Headcount", f"{metrics['headcount']}", f"Período {metrics['periodo_inicio']} a {metrics['periodo_fim']}")
kpi_card(c2, "Ativos", f"{metrics['ativos']}", None)
kpi_card(c3, "Desligados", f"{metrics['desligados_total']}", None)
kpi_card(c4, "Entradas", f"{metrics['entradas']}", None)
kpi_card(c5, "Saídas", f"{metrics['saidas']}", None)
kpi_card(c6, "Turnover", f"{metrics['turnover_pct']:.2f}%", "((entradas+saídas)/2)/headcount")
kpi_card(c7, "Absenteísmo", f"{metrics['abs_pct']:.2f}%", f"Faltas: {int(metrics['faltas_total'])} | Dias úteis: {metrics['dias_uteis']}")
kpi_card(c8, "Treinamento", f"{metrics['treino_pct']:.2f}%", f"Presenças: {metrics['treino_presencas']}")

alerts = quality_alerts(df_f, df_tr)
if alerts:
    st.warning("Alertas de qualidade de dados:\n\n- " + "\n- ".join(alerts))

st.divider()

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Composição do Time",
    "Perfil (Idade e Tempo de Casa)",
    "Absenteísmo",
    "Turnover",
    "Treinamentos",
    "Gerencial (Jorge x Moisés)"
])

with tab1:
    st.markdown("### Distribuição")
    colA, colB = st.columns(2)

    city_counts = df_f.groupby("CIDADE", dropna=False).size().reset_index(name="HEADCOUNT")
    fig_city = px.bar(city_counts.sort_values("HEADCOUNT", ascending=False), x="CIDADE", y="HEADCOUNT")
    colA.plotly_chart(fig_city, use_container_width=True)

    func_counts = df_f.groupby("FUNCAO", dropna=False).size().reset_index(name="HEADCOUNT")
    fig_func = px.bar(func_counts.sort_values("HEADCOUNT", ascending=True), x="HEADCOUNT", y="FUNCAO", orientation="h")
    colB.plotly_chart(fig_func, use_container_width=True)

    st.markdown("### Ativos x Desligados por Cidade")
    status_city = df_f.groupby(["CIDADE", "STATUS"]).size().reset_index(name="QTD")
    fig_sc = px.bar(status_city, x="CIDADE", y="QTD", color="STATUS", barmode="stack")
    st.plotly_chart(fig_sc, use_container_width=True)

    st.markdown("### Base filtrada (tabela)")
    st.dataframe(df_f, use_container_width=True, height=380)

with tab2:
    st.markdown("### Idade")
    idade_counts = df_f.groupby("FAIXA_IDADE").size().reset_index(name="QTD")
    ordem_idade = ["<18 (inconsistente)", "18–24", "25–29", "30–34", "35–39", "40+", "Sem dado"]
    idade_counts["ORDEM"] = idade_counts["FAIXA_IDADE"].apply(lambda x: ordem_idade.index(x) if x in ordem_idade else 999)
    idade_counts = idade_counts.sort_values("ORDEM")
    fig_idade = px.bar(idade_counts, x="FAIXA_IDADE", y="QTD")
    st.plotly_chart(fig_idade, use_container_width=True)

    st.markdown("### Tempo de casa")
    tc_counts = df_f.groupby("FAIXA_TEMPO_CASA").size().reset_index(name="QTD")
    ordem_tc = ["Inconsistente", "<3 meses", "3–6 meses", "6–12 meses", "1–2 anos", "2–5 anos", "5+ anos", "Sem dado"]
    tc_counts["ORDEM"] = tc_counts["FAIXA_TEMPO_CASA"].apply(lambda x: ordem_tc.index(x) if x in ordem_tc else 999)
    tc_counts = tc_counts.sort_values("ORDEM")
    fig_tc = px.bar(tc_counts, x="FAIXA_TEMPO_CASA", y="QTD")
    st.plotly_chart(fig_tc, use_container_width=True)

    st.markdown("### Médias")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Idade média (anos)", f"{df_f['IDADE'].dropna().mean():.1f}" if df_f["IDADE"].notna().any() else "Sem dado")
    with col2:
        st.metric("Tempo médio de casa (anos)", f"{df_f['TEMPO_CASA_ANOS'].dropna().mean():.2f}" if df_f["TEMPO_CASA_ANOS"].notna().any() else "Sem dado")

with tab3:
    st.markdown("### Absenteísmo por cidade")
    abs_city = df_f.groupby("CIDADE")["FALTAS_TOTAL"].sum().reset_index()
    abs_city = abs_city.sort_values("FALTAS_TOTAL", ascending=False)
    fig_abs_city = px.bar(abs_city, x="CIDADE", y="FALTAS_TOTAL")
    st.plotly_chart(fig_abs_city, use_container_width=True)

    st.markdown("### Absenteísmo por gerente")
    abs_ger = df_f.groupby("GERENTE_RESPONSAVEL")["FALTAS_TOTAL"].sum().reset_index().sort_values("FALTAS_TOTAL", ascending=False)
    fig_abs_ger = px.bar(abs_ger, x="GERENTE_RESPONSAVEL", y="FALTAS_TOTAL")
    st.plotly_chart(fig_abs_ger, use_container_width=True)

    st.markdown("### Top faltas (colaborador)")
    top_abs = df_f.groupby("NOME_NORM")["FALTAS_TOTAL"].sum().reset_index().sort_values("FALTAS_TOTAL", ascending=False)
    top_abs = top_abs[top_abs["FALTAS_TOTAL"] > 0].head(15)
    if len(top_abs) == 0:
        st.info("Sem faltas no recorte atual.")
    else:
        fig_top = px.bar(top_abs, x="NOME_NORM", y="FALTAS_TOTAL")
        st.plotly_chart(fig_top, use_container_width=True)

with tab4:
    st.markdown("### Desligamentos no mês (detalhe)")
    if "DT_DEMISSAO" in df_f.columns:
        dem = pd.to_datetime(df_f["DT_DEMISSAO"], errors="coerce", dayfirst=True)
        deslig_mes = df_f[(dem.dt.date >= metrics["periodo_inicio"]) & (dem.dt.date <= metrics["periodo_fim"])].copy()
        deslig_mes["DT_DEMISSAO"] = dem
    else:
        deslig_mes = pd.DataFrame()

    if deslig_mes.empty:
        st.info("Sem desligamentos no mês no recorte atual.")
    else:
        cols_show = [c for c in ["CIDADE", "GERENTE_RESPONSAVEL", "FUNCAO", "STATUS", "DT_DEMISSAO", "MOTIVO_DEMISSAO", "NOME_NORM"] if c in deslig_mes.columns]
        st.dataframe(deslig_mes[cols_show].sort_values("DT_DEMISSAO"), use_container_width=True, height=320)

        if "MOTIVO_DEMISSAO" in deslig_mes.columns:
            m = deslig_mes.groupby("MOTIVO_DEMISSAO").size().reset_index(name="QTD").sort_values("QTD", ascending=False)
            fig_m = px.bar(m, x="MOTIVO_DEMISSAO", y="QTD")
            st.plotly_chart(fig_m, use_container_width=True)

    st.markdown("### Admissões no mês (detalhe)")
    if "DT_ADMISSAO" in df_f.columns:
        adm = pd.to_datetime(df_f["DT_ADMISSAO"], errors="coerce", dayfirst=True)
        adm_mes = df_f[(adm.dt.date >= metrics["periodo_inicio"]) & (adm.dt.date <= metrics["periodo_fim"])].copy()
        adm_mes["DT_ADMISSAO"] = adm
    else:
        adm_mes = pd.DataFrame()

    if adm_mes.empty:
        st.info("Sem admissões no mês no recorte atual.")
    else:
        cols_show = [c for c in ["CIDADE", "GERENTE_RESPONSAVEL", "FUNCAO", "DT_ADMISSAO", "NOME_NORM"] if c in adm_mes.columns]
        st.dataframe(adm_mes[cols_show].sort_values("DT_ADMISSAO"), use_container_width=True, height=320)

with tab5:
    st.markdown("### Participação no treinamento")
    tr_sum = df_f.groupby("CIDADE")["PRESENCA_OK"].mean().reset_index()
    tr_sum["PCT"] = tr_sum["PRESENCA_OK"] * 100
    fig_tr_city = px.bar(tr_sum.sort_values("PCT", ascending=False), x="CIDADE", y="PCT")
    st.plotly_chart(fig_tr_city, use_container_width=True)

    tr_ger = df_f.groupby("GERENTE_RESPONSAVEL")["PRESENCA_OK"].mean().reset_index()
    tr_ger["PCT"] = tr_ger["PRESENCA_OK"] * 100
    fig_tr_ger = px.bar(tr_ger.sort_values("PCT", ascending=False), x="GERENTE_RESPONSAVEL", y="PCT")
    st.plotly_chart(fig_tr_ger, use_container_width=True)

    st.markdown("### Lista de não participantes")
    nao = df_f[df_f["PRESENCA_OK"] == False].copy()
    if nao.empty:
        st.info("Sem não participantes no recorte atual.")
    else:
        cols_show = [c for c in ["CIDADE", "GERENTE_RESPONSAVEL", "FUNCAO", "STATUS", "NOME_NORM"] if c in nao.columns]

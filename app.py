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

INDEX_SHEET_URL = "https://docs.google.com/spreadsheets/d/1N5D_ARAgpXMNsHKZZJBc1JGom5JUCG84cxiDxE3QcLo/edit?gid=0#gid=0"

# Abas (conforme suas bases)
TAB_BASE_GERAL = "BASE GERAL"
TAB_BASE_PRESENCA = "BASE PRESENÇA"
TAB_TREINAMENTOS = "TREINAMENTOS"
TAB_ABS_TURNOVER = "ABSENTEISMO E TURNOVER"  # mantido

GERENTE_POR_CIDADE = {
    "IMPERATRIZ": "Jorge Alexandre Bezerra da Costa",
    "ESTREITO": "Jorge Alexandre Bezerra da Costa",
    "SÃO LUIS": "Moisés Santos do Nascimento",
    "SAO LUIS": "Moisés Santos do Nascimento",
    "PEDREIRAS": "Moisés Santos do Nascimento",
    "GRAJAÚ": "Moisés Santos do Nascimento",
    "GRAJAU": "Moisés Santos do Nascimento",
}


# =========================
# HELPERS
# =========================
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
    if x is None:
        return pd.NaT
    sx = str(x).strip()
    if sx == "" or sx.lower() in ["nan", "none", "-"]:
        return pd.NaT
    try:
        return pd.to_datetime(sx, dayfirst=True, errors="coerce")
    except Exception:
        return pd.NaT


def month_start_end(mes_ref: str):
    m, y = mes_ref.split("/")
    y = int(y)
    m = int(m)
    start = date(y, m, 1)
    end = (start + relativedelta(months=1)) - relativedelta(days=1)
    return start, end


def extract_sheet_id(value: str) -> str:
    if not isinstance(value, str):
        return ""
    s = value.strip()

    if re.fullmatch(r"[a-zA-Z0-9-_]{20,}", s):
        return s

    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", s)
    return m.group(1) if m else ""


def make_unique_columns(cols):
    """
    ✅ Garante nomes únicos, evita colunas vazias e duplicadas
    """
    out = []
    seen = {}
    for c in cols:
        c = norm_text(c)
        if c == "":
            c = "COL"
        key = c
        if key in seen:
            seen[key] += 1
            key = f"{c}_{seen[c]}"
        else:
            seen[key] = 0
        out.append(key)
    return out


def first_series(df: pd.DataFrame, colname: str) -> pd.Series:
    """
    ✅ Se a coluna vier duplicada, df[colname] vira DataFrame.
    Aqui forçamos pegar a primeira coluna.
    """
    obj = df[colname]
    if isinstance(obj, pd.DataFrame):
        return obj.iloc[:, 0]
    return obj


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


@st.cache_resource
def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    return gspread.authorize(creds)


@st.cache_data(ttl=600, show_spinner=False)
def read_worksheet_as_df(sheet_id_or_url: str, tab_name: str) -> pd.DataFrame:
    gc = get_gspread_client()

    raw = norm_text(sheet_id_or_url)
    sheet_id = extract_sheet_id(raw)

    if not sheet_id:
        raise ValueError(f"Não foi possível extrair o ID da planilha a partir de: {raw}")

    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return pd.DataFrame()

    header = values[0]
    data = values[1:]
    df = pd.DataFrame(data, columns=header)

    # ✅ normaliza cabeçalhos e evita duplicados
    df.columns = make_unique_columns(df.columns)

    # remove linhas completamente vazias
    df = df.replace("", np.nan).dropna(how="all").fillna("")
    return df


@st.cache_data(ttl=600, show_spinner=False)
def read_index_df(index_sheet_url: str) -> pd.DataFrame:
    df = read_worksheet_as_df(index_sheet_url, "Página1")

    cols = {c.upper(): c for c in df.columns}
    url_col = cols.get("URL_BASE") or cols.get("URL")
    mes_col = cols.get("MES_REF") or cols.get("MÊS") or cols.get("MES")
    ativo_col = cols.get("ATIVO")

    if not url_col or not mes_col or not ativo_col:
        return df

    out = df[[url_col, mes_col, ativo_col]].copy()
    out.columns = ["URL_BASE", "MES_REF", "ATIVO"]
    out["MES_REF"] = out["MES_REF"].apply(norm_text)
    out["ATIVO"] = out["ATIVO"].apply(lambda x: norm_text(x).upper())
    return out


def ensure_numeric_series(s):
    return pd.to_numeric(s, errors="coerce").fillna(0)


def build_base_geral(df_bg: pd.DataFrame) -> pd.DataFrame:
    if df_bg.empty:
        return df_bg

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
        elif cu == "STATUS":
            colmap[c] = "STATUS"
        elif "ADMISS" in cu:
            colmap[c] = "DT_ADMISSAO"
        elif "DEMISS" in cu:
            colmap[c] = "DT_DEMISSAO"
        elif "NASC" in cu:
            colmap[c] = "DT_NASCIMENTO"
        elif "MOTIVO" in cu:
            colmap[c] = "MOTIVO_DEMISSAO"
        elif "DIAS ÚTEIS" in cu or "DIAS UTEIS" in cu:
            colmap[c] = "DIAS_UTEIS"

    df = df_bg.rename(columns=colmap).copy()

    if "NOME" in df.columns:
        df["NOME_NORM"] = first_series(df, "NOME").apply(norm_name)
    else:
        df["NOME_NORM"] = ""

    if "CIDADE" in df.columns:
        df["CIDADE"] = first_series(df, "CIDADE").apply(norm_city)
    else:
        df["CIDADE"] = ""

    if "FUNCAO" in df.columns:
        df["FUNCAO"] = first_series(df, "FUNCAO").apply(lambda x: norm_text(x).upper())
    else:
        df["FUNCAO"] = ""

    if "STATUS" in df.columns:
        df["STATUS"] = first_series(df, "STATUS").apply(lambda x: norm_text(x).upper())
    else:
        df["STATUS"] = "ATIVO"

    df["GERENTE_RESPONSAVEL"] = df["CIDADE"].map(GERENTE_POR_CIDADE).fillna("Não mapeado")

    # ✅ Datas (robusto contra colunas duplicadas)
    for col in ["DT_ADMISSAO", "DT_DEMISSAO", "DT_NASCIMENTO"]:
        if col in df.columns:
            s = first_series(df, col)
            df[col] = s.apply(parse_date_safe)

    return df


def build_presenca(df_pres: pd.DataFrame) -> pd.DataFrame:
    if df_pres.empty:
        return df_pres

    name_col = None
    for c in df_pres.columns:
        if c.upper() in ["NOME", "COLABORADOR", "FUNCIONARIO"]:
            name_col = c
            break
    if not name_col:
        name_col = df_pres.columns[0]

    df = df_pres.copy()
    df["NOME_NORM"] = first_series(df, name_col).apply(norm_name)

    day_cols = []
    for c in df.columns:
        cu = str(c).strip()
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
        df["FALTAS_TOTAL"] = ensure_numeric_series(first_series(df, total_col))
    else:
        if day_cols:
            faltas = np.zeros(len(df))
            for c in day_cols:
                faltas += ensure_numeric_series(first_series(df, c)).to_numpy()
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
    df["NOME_NORM"] = first_series(df, name_col).apply(norm_name)

    if pres_col:
        df["PRESENCA"] = first_series(df, pres_col).apply(lambda x: norm_text(x).upper())
    else:
        df["PRESENCA"] = ""

    df["PRESENCA_OK"] = df["PRESENCA"].isin(["SIM", "S", "OK", "PRESENTE"])
    return df[["NOME_NORM", "PRESENCA_OK"]]


def compute_metrics(df_base: pd.DataFrame, df_pres: pd.DataFrame, df_tr: pd.DataFrame, mes_ref: str):
    headcount = len(df_base)

    # status
    if "STATUS" in df_base.columns:
        status_norm = df_base["STATUS"].astype(str).str.upper().str.strip()
        ativos = int((status_norm == "ATIVO").sum())
    else:
        ativos = headcount
    desligados_total = headcount - ativos

    # período do mês
    start, end = month_start_end(mes_ref)

    # ✅ força datetime aqui (mesmo que já tenha vindo certo)
    entradas = 0
    saidas = 0

    if "DT_ADMISSAO" in df_base.columns:
        adm = pd.to_datetime(df_base["DT_ADMISSAO"], errors="coerce", dayfirst=True)
        entradas = int(((adm.dt.date >= start) & (adm.dt.date <= end)).sum())

    if "DT_DEMISSAO" in df_base.columns:
        dem = pd.to_datetime(df_base["DT_DEMISSAO"], errors="coerce", dayfirst=True)
        saidas = int(((dem.dt.date >= start) & (dem.dt.date <= end)).sum())

    # turnover
    turnover = (((entradas + saidas) / 2) / headcount * 100) if headcount else 0

    # faltas
    faltas_total = float(df_pres["FALTAS_TOTAL"].sum()) if (df_pres is not None and not df_pres.empty) else 0.0

    # dias úteis
    dias_uteis = 22
    if "DIAS_UTEIS" in df_base.columns:
        du = pd.to_numeric(df_base["DIAS_UTEIS"], errors="coerce").dropna()
        if len(du) > 0:
            dias_uteis = int(du.iloc[0])

    abs_pct = (faltas_total / (headcount * dias_uteis) * 100) if headcount and dias_uteis else 0

    # treinamento
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
        out["IDADE"] = (pd.to_datetime(ref_date) - out["DT_NASCIMENTO"]).dt.days / 365.25
    else:
        out["IDADE"] = np.nan

    if "DT_ADMISSAO" in out.columns:
        out["TEMPO_CASA_ANOS"] = (pd.to_datetime(ref_date) - out["DT_ADMISSAO"]).dt.days / 365.25
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
    if not df_tr.empty:
        base_names = set(df_base["NOME_NORM"].tolist())
        tr_names = set(df_tr["NOME_NORM"].tolist())
        extra = tr_names - base_names
        if len(extra) > 0:
            alerts.append(f"Treinamento com nomes fora da base: {len(extra)} (ex.: {sorted(list(extra))[:3]}).")
    return alerts


# =========================
# UI
# =========================
st.title("Painel de RH - VELOX Vistorias")

idx = read_index_df(INDEX_SHEET_URL)

if idx.empty or not set(["URL_BASE", "MES_REF", "ATIVO"]).issubset(set(idx.columns)):
    st.error("A aba do índice precisa de colunas: URL (ou URL_BASE), MÊS (ou MES_REF), ATIVO.")
    st.dataframe(idx)
    st.stop()

ativos = idx[idx["ATIVO"] == "S"].copy()
if ativos.empty:
    st.error("Não há meses ATIVO = S no índice.")
    st.dataframe(idx)
    st.stop()

with st.sidebar:
    st.subheader("Controle")
    st.caption("O painel lê o índice e puxa a base do mês selecionado.")
    st.divider()

mes_opts = ativos["MES_REF"].tolist()
mes_sel = st.sidebar.selectbox("Mês de referência", mes_opts, index=len(mes_opts) - 1)

row = ativos[ativos["MES_REF"] == mes_sel].iloc[0]
base_url = norm_text(row["URL_BASE"])

st.sidebar.caption("Valor vindo do índice:")
st.sidebar.write(base_url)
st.sidebar.caption("ID extraído:")
st.sidebar.write(extract_sheet_id(base_url))

with st.spinner("Carregando base do mês..."):
    df_bg_raw = read_worksheet_as_df(base_url, TAB_BASE_GERAL)
    df_pr_raw = read_worksheet_as_df(base_url, TAB_BASE_PRESENCA)
    df_tr_raw = read_worksheet_as_df(base_url, TAB_TREINAMENTOS)

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
    cidades = sorted([c for c in df["CIDADE"].unique().tolist() if c])
    funcoes = sorted([f for f in df["FUNCAO"].unique().tolist() if f])
    gerentes = sorted([g for g in df["GERENTE_RESPONSAVEL"].unique().tolist() if g])

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

metrics = compute_metrics(df_f, df_f[["NOME_NORM", "FALTAS_TOTAL"]], df_f[["NOME_NORM", "PRESENCA_OK"]], mes_sel)

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
    "Gerencial (Jorge x Moisés)",
])

with tab1:
    st.markdown("### Distribuição")
    colA, colB = st.columns(2)

    city_counts = df_f.groupby("CIDADE", dropna=False).size().reset_index(name="HEADCOUNT")
    colA.plotly_chart(px.bar(city_counts.sort_values("HEADCOUNT", ascending=False), x="CIDADE", y="HEADCOUNT"), use_container_width=True)

    func_counts = df_f.groupby("FUNCAO", dropna=False).size().reset_index(name="HEADCOUNT")
    colB.plotly_chart(px.bar(func_counts.sort_values("HEADCOUNT", ascending=True), x="HEADCOUNT", y="FUNCAO", orientation="h"), use_container_width=True)

    st.markdown("### Ativos x Desligados por Cidade")
    status_city = df_f.groupby(["CIDADE", "STATUS"]).size().reset_index(name="QTD")
    st.plotly_chart(px.bar(status_city, x="CIDADE", y="QTD", color="STATUS", barmode="stack"), use_container_width=True)

    st.markdown("### Base filtrada (tabela)")
    st.dataframe(df_f, use_container_width=True, height=380)

with tab2:
    st.markdown("### Idade")
    idade_counts = df_f.groupby("FAIXA_IDADE").size().reset_index(name="QTD")
    ordem_idade = ["<18 (inconsistente)", "18–24", "25–29", "30–34", "35–39", "40+", "Sem dado"]
    idade_counts["ORDEM"] = idade_counts["FAIXA_IDADE"].apply(lambda x: ordem_idade.index(x) if x in ordem_idade else 999)
    idade_counts = idade_counts.sort_values("ORDEM")
    st.plotly_chart(px.bar(idade_counts, x="FAIXA_IDADE", y="QTD"), use_container_width=True)

    st.markdown("### Tempo de casa")
    tc_counts = df_f.groupby("FAIXA_TEMPO_CASA").size().reset_index(name="QTD")
    ordem_tc = ["Inconsistente", "<3 meses", "3–6 meses", "6–12 meses", "1–2 anos", "2–5 anos", "5+ anos", "Sem dado"]
    tc_counts["ORDEM"] = tc_counts["FAIXA_TEMPO_CASA"].apply(lambda x: ordem_tc.index(x) if x in ordem_tc else 999)
    tc_counts = tc_counts.sort_values("ORDEM")
    st.plotly_chart(px.bar(tc_counts, x="FAIXA_TEMPO_CASA", y="QTD"), use_container_width=True)

    cA, cB = st.columns(2)
    with cA:
        st.metric("Idade média (anos)", f"{df_f['IDADE'].dropna().mean():.1f}" if df_f["IDADE"].notna().any() else "Sem dado")
    with cB:
        st.metric("Tempo médio de casa (anos)", f"{df_f['TEMPO_CASA_ANOS'].dropna().mean():.2f}" if df_f["TEMPO_CASA_ANOS"].notna().any() else "Sem dado")

with tab3:
    st.markdown("### Absenteísmo por cidade")
    abs_city = df_f.groupby("CIDADE")["FALTAS_TOTAL"].sum().reset_index().sort_values("FALTAS_TOTAL", ascending=False)
    st.plotly_chart(px.bar(abs_city, x="CIDADE", y="FALTAS_TOTAL"), use_container_width=True)

    st.markdown("### Absenteísmo por gerente")
    abs_ger = df_f.groupby("GERENTE_RESPONSAVEL")["FALTAS_TOTAL"].sum().reset_index().sort_values("FALTAS_TOTAL", ascending=False)
    st.plotly_chart(px.bar(abs_ger, x="GERENTE_RESPONSAVEL", y="FALTAS_TOTAL"), use_container_width=True)

    st.markdown("### Top faltas (colaborador)")
    top_abs = df_f.groupby("NOME_NORM")["FALTAS_TOTAL"].sum().reset_index().sort_values("FALTAS_TOTAL", ascending=False)
    top_abs = top_abs[top_abs["FALTAS_TOTAL"] > 0].head(15)
    if top_abs.empty:
        st.info("Sem faltas no recorte atual.")
    else:
        st.plotly_chart(px.bar(top_abs, x="NOME_NORM", y="FALTAS_TOTAL"), use_container_width=True)

with tab4:
    st.markdown("### Desligamentos no mês (detalhe)")
    if "DT_DEMISSAO" in df_f.columns:
        deslig_mes = df_f[
            (df_f["DT_DEMISSAO"].dt.date >= metrics["periodo_inicio"]) &
            (df_f["DT_DEMISSAO"].dt.date <= metrics["periodo_fim"])
        ].copy()
    else:
        deslig_mes = pd.DataFrame()

    if deslig_mes.empty:
        st.info("Sem desligamentos no mês no recorte atual.")
    else:
        cols_show = [c for c in ["CIDADE", "GERENTE_RESPONSAVEL", "FUNCAO", "STATUS", "DT_DEMISSAO", "MOTIVO_DEMISSAO", "NOME_NORM"] if c in deslig_mes.columns]
        st.dataframe(deslig_mes[cols_show].sort_values("DT_DEMISSAO"), use_container_width=True, height=320)

        if "MOTIVO_DEMISSAO" in deslig_mes.columns:
            m = deslig_mes.groupby("MOTIVO_DEMISSAO").size().reset_index(name="QTD").sort_values("QTD", ascending=False)
            st.plotly_chart(px.bar(m, x="MOTIVO_DEMISSAO", y="QTD"), use_container_width=True)

    st.markdown("### Entradas no mês (detalhe)")
    if "DT_ADMISSAO" in df_f.columns:
        adm_mes = df_f[
            (df_f["DT_ADMISSAO"].dt.date >= metrics["periodo_inicio"]) &
            (df_f["DT_ADMISSAO"].dt.date <= metrics["periodo_fim"])
        ].copy()
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
    st.plotly_chart(px.bar(tr_sum.sort_values("PCT", ascending=False), x="CIDADE", y="PCT"), use_container_width=True)

    tr_ger = df_f.groupby("GERENTE_RESPONSAVEL")["PRESENCA_OK"].mean().reset_index()
    tr_ger["PCT"] = tr_ger["PRESENCA_OK"] * 100
    st.plotly_chart(px.bar(tr_ger.sort_values("PCT", ascending=False), x="GERENTE_RESPONSAVEL", y="PCT"), use_container_width=True)

    st.markdown("### Lista de não participantes")
    nao = df_f[df_f["PRESENCA_OK"] == False].copy()
    if nao.empty:
        st.info("Sem não participantes no recorte atual.")
    else:
        cols_show = [c for c in ["CIDADE", "GERENTE_RESPONSAVEL", "FUNCAO", "STATUS", "NOME_NORM"] if c in nao.columns]
        st.dataframe(nao[cols_show], use_container_width=True, height=320)

with tab6:
    st.markdown("### Comparativo por gerente")
    agg = df_f.groupby("GERENTE_RESPONSAVEL").agg(
        HEADCOUNT=("NOME_NORM", "count"),
        ATIVOS=("STATUS", lambda s: (s == "ATIVO").sum()),
        FALTAS=("FALTAS_TOTAL", "sum"),
        TREINO_PCT=("PRESENCA_OK", "mean"),
        IDADE_MEDIA=("IDADE", "mean"),
        TEMPO_CASA_MEDIA=("TEMPO_CASA_ANOS", "mean"),
    ).reset_index()

    agg["TREINO_PCT"] = agg["TREINO_PCT"] * 100
    agg["ABS_PCT_APROX"] = np.where(
        (agg["HEADCOUNT"] > 0) & (metrics["dias_uteis"] > 0),
        (agg["FALTAS"] / (agg["HEADCOUNT"] * metrics["dias_uteis"]) * 100),
        0
    )

    st.dataframe(agg.sort_values("HEADCOUNT", ascending=False), use_container_width=True, height=280)
    st.plotly_chart(px.bar(agg.sort_values("ABS_PCT_APROX", ascending=False), x="GERENTE_RESPONSAVEL", y="ABS_PCT_APROX"), use_container_width=True)
    st.plotly_chart(px.bar(agg.sort_values("TEMPO_CASA_MEDIA", ascending=False), x="GERENTE_RESPONSAVEL", y="TEMPO_CASA_MEDIA"), use_container_width=True)

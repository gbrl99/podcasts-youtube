# %% [markdown]
# ## **Importação bibliotecas**

# %%
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', 200)
import re
from datetime import datetime
import pytz

# %% [markdown]
# ## **Funções**

# %%
def filtrar_episodios_podcast(df: pd.DataFrame, coluna: str) -> pd.DataFrame:    
    # Padrões desejados
    padrao_podcast = (
        r"(PODPAH\s*(-\s*)?#\s*\d+)|"
        r"(FLOW\s*(-\s*)?#\s*\d+)|"
        r"(PODCAST\s*(-\s*)?#\s*\d+)|"
        r"(LTDA\.?\s*(-\s*)?#\s*\d+)|"
        r"(VERÃO\s*(-\s*)?#\s*\d+)"
    )
    padrao_excecao = r"RODRIGO CONSTANTINO"
    
    # Padrões indesejados
    padrao_indesejado = r"(EXTRA\s*FLOW)"
    
    # Selecionar episódios desejados
    df = df[df[coluna].apply(
        lambda titulo: (
            not pd.isna(titulo) and (
                bool(re.search(padrao_podcast, str(titulo).strip().upper())) or
                bool(re.search(padrao_excecao, str(titulo).strip().upper()))
            )
        )
    )].copy()
    
    # Remover episódios indesejados
    df = df[df[coluna].apply(
        lambda titulo: (
            pd.isna(titulo) or
            not bool(re.search(padrao_indesejado, str(titulo).strip().upper()))
        )
    )].copy()
    
    return df

def atualizar_canal_por_titulo(df: pd.DataFrame, col_titulo: str, col_canal: str) -> pd.DataFrame:
    # Padrões desejados
    padroes_flow_1 = (
        r"FLOW\s+PODCAST\s*#\s*\d+",
        r"RODRIGO CONSTANTINO"
    )
    padroes_flow_2 = (
        r"FLOW\s*#\s*\d+",
    )
    
    def definir_canal(row):
        titulo = str(row[col_titulo]).strip().upper()
        
        if any(re.search(p, titulo) for p in padroes_flow_1):
            return "Flow 1.0"
        elif any(re.search(p, titulo) for p in padroes_flow_2):
            return "Flow 2.0"
        else:
            return row[col_canal]
    
    df[col_canal] = df.apply(definir_canal, axis=1)
    
    return df

def limpar_titulo_podcast(df: pd.DataFrame, coluna: str) -> pd.DataFrame:
    # Regra 1: Transformar tudo em maiúsculo
    df[coluna] = df[coluna].astype(str).str.upper()
    
    # Regra 2: Remover tudo a partir do emoji 🤝
    df[coluna] = df[coluna].str.split("🤝").str[0]
    
    # Regra 3: Aplicar strip()
    df[coluna] = df[coluna].str.strip()
    
    return df


def extrair_nome_convidado(df: pd.DataFrame, coluna_titulo: str) -> pd.DataFrame:
    padrao_remover = (
        r"\s*[-–]\s*PODPAH\s*(-\s*)?#\s*\d+\s*$|"
        r"\s*[-–]\s*INTELIGÊNCIA\s*LTDA\.?\s*(PODCAST)?\s*#\s*\d+\s*$|"
        r"\s*[-–]\s*FLOW\s*(-\s*)?#\s*\d+\s*$|"
        r"\s*PODPAH\s*(-\s*)?#\s*\d+\s*$|"
        r"\s*FLOW\s*(-\s*)?#\s*\d+\s*$|"
        r"\s*INTELIGÊNCIA\s*LTDA\.?\s*#\s*\d+\s*$|"
        r"\s*DE VERÃO\s*#\s*\d+\s*$"
    )
    
    df["PODCAST_CONVIDADO"] = (
        df[coluna_titulo]
        .astype(str)
        .str.strip()
        .str.replace(padrao_remover, "", flags=re.IGNORECASE, regex=True)
        .str.strip()
    )
    
    return df

def extrair_numero_episodio(titulo):
    if pd.isna(titulo):
        return None

    titulo = str(titulo).strip()

    # Exceção explícita
    if titulo.upper().startswith("RODRIGO CONSTANTINO"):
        return 486

    # Padrões robustos para todos os formatos conhecidos
    padroes = [
        r'#\s*(\d{1,4})',  # "#123" ou "# 123"
        r'Flow\s+(Podcast\s+)?#\s*(\d{1,4})',
        r'Podpah\s*[-–]?\s*#\s*(\d{1,4})',
        r'Podpah\s+de\s+Verão\s+#\s*(\d{1,4})',
        r'Inteligência\s+Ltda\.?\s+(Podcast\s+)?#\s*(\d{1,4})'
    ]

    for padrao in padroes:
        match = re.search(padrao, titulo, flags=re.IGNORECASE)
        if match:
            # Usa o último grupo numérico capturado
            numeros = [g for g in match.groups() if g and g.isdigit()]
            if numeros:
                return int(numeros[-1])

    return None


def limpar_valor_numerico(valor):
    try:
        return int(str(valor).replace("'", "").replace(".", "").strip())
    except:
        return 0

def converter_para_horario_brasil(data_iso):
    if pd.isna(data_iso):
        return None

    try:
        data_utc = pd.to_datetime(data_iso, utc=True)
#        data_utc = pd.to_datetime.strptime(data_iso, "%Y-%m-%dT%H:%M:%SZ")
        fuso_brasil = pytz.timezone("America/Sao_Paulo")
        data_brasil = data_utc.replace(tzinfo=pytz.utc).astimezone(fuso_brasil)
        return data_brasil
    except Exception as e:
        print(f"Erro ao converter data: {data_iso} -> {e}")
        return None

def extrair_horario_publicacao(data_brasil):
    if data_brasil:
        return data_brasil.strftime("%H:%M")
    return None

def obter_dia_da_semana(data_brasil):
    if data_brasil:
        dias = ['segunda-feira', 'terça-feira', 'quarta-feira', 'quinta-feira', 'sexta-feira', 'sábado', 'domingo']
        return dias[data_brasil.weekday()]
    return None

def classificar_momento_do_dia(data_brasil):
    if data_brasil:
        hora = data_brasil.hour
        if 6 <= hora < 12:
            return "DIA"
        elif 12 <= hora < 18:
            return "TARDE"
        elif 18 <= hora < 24:
            return "NOITE"
        else:
            return "MADRUGADA"
    return None

def calcular_dias_desde_publicacao(data_brasil):
    if data_brasil:
        hoje = datetime.now().replace(tzinfo=None)
        return (hoje - data_brasil).days
    return None

def extrair_numero_episodio(df: pd.DataFrame, col_titulo: str) -> pd.DataFrame:
    # Regex para capturar o número após o #
    padrao_numero = r"#\s*(\d+)"
    
    # Extrair número após hashtag
    df["PODCAST_EPISODIO"] = (
        df[col_titulo]
        .astype(str)
        .str.upper()
        .apply(lambda x: re.search(padrao_numero, x).group(1) if re.search(padrao_numero, x) else None)
    )   
    
    # Tratar exceção do RODRIGO CONSTANTINO
    df.loc[
        df[col_titulo].astype(str).str.upper().str.strip() == "RODRIGO CONSTANTINO",
        "PODCAST_EPISODIO"
    ] = "486"
    
    return df

def identificar_episodios_faltantes(df: pd.DataFrame, col_canal: str, col_ep: str) -> pd.DataFrame:
    # Converte para número, ignora episódios que não são numéricos
    df = df.copy()
    df[col_ep] = pd.to_numeric(df[col_ep], errors="coerce")
    
    # Resultado final
    faltantes = []

    # Agrupa por canal
    for canal, grupo in df.groupby(col_canal):
        episodios_presentes = grupo[col_ep].dropna().astype(int).sort_values()
        
        if episodios_presentes.empty:
            continue
        
        min_ep = episodios_presentes.min()
        max_ep = episodios_presentes.max()
        
        todos = set(range(min_ep, max_ep + 1))
        existentes = set(episodios_presentes.tolist())
        ausentes = sorted(todos - existentes)
        
        for ep in ausentes:
            faltantes.append({
                col_canal: canal,
                "EPISODIO_FALTANTE": ep
            })
    
    return pd.DataFrame(faltantes)


# %% [markdown]
# ## **Leitura**

# %%
df_podcasts = pd.read_excel("../../dados/tabelas/01_EPS_COMPLETOS_RAW.xlsx")
df_podcasts.head(2)

# %% [markdown]
# ## **Transformação**

# %%
df = df_podcasts.copy()
df = filtrar_episodios_podcast(df, 'VIDEO_TITULO')  
df = atualizar_canal_por_titulo(df, 'VIDEO_TITULO', 'CANAL')
df = limpar_titulo_podcast(df, 'VIDEO_TITULO')
df = extrair_nome_convidado(df, 'VIDEO_TITULO')
df = extrair_numero_episodio(df, 'VIDEO_TITULO')
df = df.drop("STATUS_EXTRAÇÃO", axis=1)

# Limpar valores numéricos
df['VISUALIZACOES'] = df['VISUALIZACOES'].apply(limpar_valor_numerico)
df['CURTIDAS'] = df['CURTIDAS'].apply(limpar_valor_numerico)
df['COMENTARIOS'] = df['COMENTARIOS'].apply(limpar_valor_numerico)

# Calcular métricas
df['CURTIDAS_POR_VISUALIZACOES'] = df.apply(lambda row: row['CURTIDAS'] / row['VISUALIZACOES'] if row['VISUALIZACOES'] > 0 else 0, axis=1)
df['COMENTARIOS_POR_VISUALIZACOES'] = df.apply(lambda row: row['COMENTARIOS'] / row['VISUALIZACOES'] if row['VISUALIZACOES'] > 0 else 0, axis=1)

# Converter para datetime no fuso horário do Brasil
df['DATA_PUBLICACAO_BR'] = df['DATA_PUBLICACAO'].apply(converter_para_horario_brasil)
df['DATA_PUBLICACAO_BR'] = df['DATA_PUBLICACAO_BR'].dt.tz_localize(None)
df['MES_ANO_PUBLICACAO'] = df['DATA_PUBLICACAO_BR'].dt.strftime('%m/%Y')
df['ANO_PUBLICACAO'] = df['DATA_PUBLICACAO_BR'].dt.strftime('%Y')
df['MES_PUBLICACAO'] = df['DATA_PUBLICACAO_BR'].dt.strftime('%m')
df['HORARIO_PUBLICACAO'] = df['DATA_PUBLICACAO_BR'].apply(extrair_horario_publicacao)
df['DIA_DA_SEMANA'] = df['DATA_PUBLICACAO_BR'].apply(obter_dia_da_semana)
df['MOMENTO_DIA'] = df['DATA_PUBLICACAO_BR'].apply(classificar_momento_do_dia)
df['DIAS_DESDE_PUBLICACAO'] = df['DATA_PUBLICACAO_BR'].apply(calcular_dias_desde_publicacao)

# Ideias
# QTD_CORTES -> Trazer quantidade de cortes daquele vídeo
# QTD_VISUALIZACOES_CORTES ->
# QTD_CURTIDAS_CORTES -> 
# QTD_COMENTARIOS_CORTES ->
# DESEMPENHO_VIDEO -> 0 ~ 100K = BAIXO, 100K~500K = MEDIO_BAIXO, 500K~1M = MEDIO_ALTO, 1M+ = ALTO ou classificar por quartis
# CATEGORIA_DURACAO_VIDEO -> Classificar por quartis (0 - 1h30 min curto / 1h30-3h medio / 3h+ longo)
df.head()

# %%
df_faltantes = identificar_episodios_faltantes(df, 'CANAL', 'PODCAST_EPISODIO')
df_faltantes

# %% [markdown]
# ## **SALVAR TABELA**

# %%
df.to_excel("../../dados/tabelas/02_EPS_COMPLETOS_CLEAN.xlsx", index=False)
df.head()

# %%




import os
import re
import pandas as pd
import time
from datetime import datetime
from googleapiclient.discovery import build
from dotenv import load_dotenv
import isodate


# ===================== CONFIGURAÇÕES =====================

load_dotenv("../../.env")
API_KEY = os.getenv('YOUTUBE_API_KEY')

youtube = build('youtube', 'v3', developerKey=API_KEY)

CANAIS = {
    'Flow Podcast': {
        'id': 'UC4ncvgh5hFr5O83MH7-jRJg',
        'padroes_titulo': [
            r'Flow\s+(Podcast\s+)?#\s?\d{1,4}', # Cobre Flow #123 e Flow Podcast #123
            r'^RODRIGO CONSTANTINO$' # Episódio 486
        ]
    },
    'Podpah Podcast': {
        'id': 'UCj9R9rOhl81fhnKxBpwJ-yw',
        'padroes_titulo': [
            r'Podpah\s*[-–]?\s*#\s?\d{1,4}',               # cobre: "Podpah - #495" e "Podpah #123"
            r'Podpah\s+de\s+Verão\s+#\s?\d{1,4}'           # cobre: "Podpah de Verão # 405"
        ]
    },
    'Inteligência Ltda.': {
        'id': 'UCWZoPPW7u2I4gZfhJBZ6NqQ',
        'padroes_titulo': [
            r'Inteligência\s+Ltda\.?\s+(?:Podcast\s+)?#\s?\d{1,4}',  # cobre com/sem ponto final e espaço
        ]
    }
}


dt_execucao_script = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
dados_videos = []

# ===================== FUNÇÕES AUXILIARES =====================

def converter_duracao_para_segundos(duracao_iso):
    try:
        duracao = isodate.parse_duration(duracao_iso)
        return int(duracao.total_seconds())
    except Exception:
        return None

def obter_uploads_playlist_id(channel_id):
    resposta = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    ).execute()
    return resposta['items'][0]['contentDetails']['relatedPlaylists']['uploads']

def coletar_todos_video_ids(playlist_id):
    video_ids = []
    proxima_pagina = None

    while True:
        resposta = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=proxima_pagina
        ).execute()

        ids = [item['contentDetails']['videoId'] for item in resposta['items']]
        video_ids.extend(ids)

        proxima_pagina = resposta.get('nextPageToken')
        if not proxima_pagina:
            break

    return video_ids

def obter_categorias():
    categorias = {}
    resposta = youtube.videoCategories().list(
        part='snippet',
        regionCode='BR'
    ).execute()

    for item in resposta['items']:
        categorias[item['id']] = item['snippet']['title']
    
    return categorias

def titulo_valido(titulo, padroes):
    for padrao in padroes:
        if re.search(padrao, titulo, re.IGNORECASE):
            return True
    return False

def processar_videos_em_lote(video_ids, nome_canal, padroes_titulo, categorias):
    for i in range(0, len(video_ids), 50):
        lote_ids = video_ids[i:i+50]
        resposta = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=','.join(lote_ids)
        ).execute()

        for item in resposta['items']:
            titulo = item['snippet']['title']

            if not titulo_valido(titulo, padroes_titulo):
                continue

            print(f'Extraindo vídeo: {titulo}')

            video_id = item['id']
            descricao = item['snippet'].get('description', '')
            data_publicacao = item['snippet'].get('publishedAt', '')
            duracao_iso = item['contentDetails'].get('duration', '')
            duracao = converter_duracao_para_segundos(duracao_iso)
            visualizacoes = item['statistics'].get('viewCount', '0')
            curtidas = item['statistics'].get('likeCount', '0')
            comentarios = item['statistics'].get('commentCount', '0')
            categoria_id = item['snippet'].get('categoryId', '')
            descricao_categoria = categorias.get(categoria_id, 'Desconhecida')

            dados_videos.append({
                'CANAL': nome_canal,
                'VIDEO_TITULO': titulo,
                'ID_VIDEO': video_id,
                'DURACAO': duracao,
                'DESCRICAO': descricao,
                'DATA_PUBLICACAO': data_publicacao,
                'VISUALIZACOES': visualizacoes,
                'CURTIDAS': curtidas,
                'COMENTARIOS': comentarios,
                'CATEGORIA_ID': categoria_id,
                'DESCRICAO_CATEGORIA': descricao_categoria,
                'DT_EXECUCAO_SCRIPT': dt_execucao_script
                })

# ===================== FUNÇÃO PRINCIPAL =====================

def extrair_videos():
    categorias = obter_categorias()
    print(f'Categorias obtidas: {len(categorias)} categorias.')

    for nome_canal, info in CANAIS.items():
        print(f'\nExtraindo vídeos do canal: {nome_canal}')
        playlist_id = obter_uploads_playlist_id(info['id'])
        video_ids = coletar_todos_video_ids(playlist_id)
        print(f'Total de vídeos encontrados: {len(video_ids)}')

        padroes_titulo = info.get('padroes_titulo', [])
        processar_videos_em_lote(video_ids, nome_canal, padroes_titulo, categorias)

# ===================== EXECUÇÃO =====================

inicio = time.time()

extrair_videos()

df = pd.DataFrame(dados_videos)
dir_dados = '../../dados/tabelas'
nome_arquivo = '01_EPS_COMPLETOS_RAW.xlsx'
df.to_excel(dir_dados + '/' + nome_arquivo, index=False)

fim = time.time()
duracao_total = fim - inicio

print(f'\nExtração concluída! Arquivo salvo como: {nome_arquivo}')
print(f'Tempo total de execução: {duracao_total:.2f} segundos')

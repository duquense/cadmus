# Função para obter dados semanais do youtube charts

import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from pandas.io.json import json_normalize

# Formatação de data e inicialização dos arquivos de saída.
# A função faz um loop de cada semana, pega as 100 tracks mais tocadas e concatena no arquivo de saída.
# Ao final do loop o arquivo de saída é salvo em um .csv para ser consumido no Tableau.

def get_youtube_charts_data(start_date='20190104', end_date='20230602'):
    date_format = '%Y%m%d'
    current_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)
    file_artistas = []
    file_tracks = []

    while current_date <= end_date:
        # Formatação de datas de início e fim da semana, a serem inseridas na URL
        formatted_date = current_date.strftime(date_format)
        week_date = (current_date + timedelta(days=6)).strftime(date_format)
        
        #Parametros da requisição. Os valores de data em 'query' variam conforme a semana selecionada.
        params = {
            "context": {
                "client": {
                    "clientName": "WEB_MUSIC_ANALYTICS",
                    "clientVersion": "0.2",
                    "hl": "pt",
                    "gl": "BR",
                    "experimentIds": [],
                    "experimentsToken": "",
                    "theme": "MUSIC"
                },
                "capabilities": {},
                "request": {
                    "internalExperimentFlags": []
                }
            },
            "browseId": "FEmusic_analytics_charts_home",
            "query": f"chart_params_type=WEEK&perspective=CHART&flags=viral_video_chart&selected_chart=TRACKS&chart_params_id=weekly%3A{formatted_date}%3A{week_date}%3Abr"
        }
        
        # Exceução da requisição com os parâmetros acima e obtenção do json 
        req = requests.post(api_youtube, json=params)
        dados = req.json()

        # O json de saída é aninhado em vários níveis, é necessário 'descer' pelas keys para fazer a normalização apenas da parte de Tracks ('trackviews')
        tracks = json_normalize(dados["contents"]["sectionListRenderer"]["contents"],
                         sep='_',
                         record_path=['musicAnalyticsSectionRenderer','content','trackTypes','trackViews']) 

        # Criação de coluna de data para indicar qual a semana de entrada do registro
        tracks = tracks.assign(data_inicio=current_date)

        # A mesma lógica se aplica para obter os dados de nomes dos artistas, que estão em um nível abaixo no json
        artistas = json_normalize(dados['contents']['sectionListRenderer']['contents'],['musicAnalyticsSectionRenderer','content','trackTypes','trackViews','artists'],
                         sep='_',
                         meta=[['musicAnalyticsSectionRenderer','content','trackTypes','trackViews','id']])

        artistas = artistas.assign(data_inicio=current_date)

        # Final da extração da semana atual, soma-se 7 dias, armazena-se os dados extraídos e avança para a próxima semana no loop
        current_date += timedelta(days=7)

        file_tracks.append(tracks)
        file_artistas.append(artistas)
    
    # Ao final do loop, é feita a concatenação dos dados armazenados de cada semana em dataframes únicos 
    df_artistas = pd.concat(file_artistas)
    df_tracks = pd.concat(file_tracks)

    # Os dataframes são salvos em formato .csv com indicação do início e fim da extração
    end_date = end_date.strftime(date_format) 
    
    artistas_filename = f'artistas_data_{start_date}_{end_date}.csv'
    tracks_filename = f'tracks_data_{start_date}_{end_date}.csv'

    df_artistas.to_csv(artistas_filename)        
    df_tracks.to_csv(tracks_filename)     
    
    return df_artistas, df_tracks   

# Chamada da função. Como não foram passados parâmetros, a função assume os valores default de start_date='20190104' e end_date='20230602'
tracks_data = get_youtube_charts_data() 
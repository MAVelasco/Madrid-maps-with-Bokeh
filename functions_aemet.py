def consulta_horaria_aemet(apikey, localidad, API):
    import http.client
    import urllib.request, json 
    from pandas.io.json import json_normalize
    import pandas as pd
    import numpy as np
    
    # configuraciones previas
    periodo = "horaria"  

    # consulta AEMET
    conn = http.client.HTTPSConnection("opendata.aemet.es")
    headers = {'cache-control': "no-cache"}
    request = "https://opendata.aemet.es/opendata/api/" + API + "/" + periodo + "/" + localidad + "/" + "?api_key=" + apikey
    conn.request("GET", request, headers=headers)
    res = conn.getresponse()
    aux = res.read()

    data = aux.decode("ANSI")
    #print(data)

    json_data = json.loads(data)
    URL_datos = json_data['datos']
    URL_metadatos = json_data['metadatos']

    # Decodifica el JSON
    if (json_data['estado']==200):
        #print(URL_datos)
        data = urllib.request.urlopen(URL_datos)#.read()
        json_url = data.read()
        L = len(json_url)
        json_aemet = json_url[2:L-1].decode('ANSI')
        #print(json_aemet)
        message = json.loads(json_aemet)
    
        precipitacion = message['prediccion']
        dia = json_normalize(precipitacion['dia'])
        hoy = dia.iloc[0]
        manhana= dia.iloc[1]
        #print(hoy)
        
        # 4 periodos de 6h: 01-07, 07-13, 13-19, 19-01
        hoy_prob_precipitacion = json_normalize(hoy['probPrecipitacion'])        
        hoy_prob_tormenta = json_normalize(hoy['probTormenta'])
        hoy_prob_nieve = json_normalize(hoy['probNieve'])
        hoy_prob_precipitacion.columns = ['periodo','probPrecip']
        hoy_prob_tormenta.columns = ['periodo','probTormen']
        hoy_prob_nieve.columns = ['periodo','probNieve']
        resumen_hoy_6h = pd.merge(hoy_prob_precipitacion,hoy_prob_tormenta)
        resumen_hoy_6h = pd.merge(resumen_hoy_6h,hoy_prob_nieve)
        #print(resumen_hoy_6h)
 
        # 16 periodos horarios 07-23
        hoy_estado_cielo = json_normalize(hoy['estadoCielo'])        
        hoy_precipitacion = json_normalize(hoy['precipitacion'])     
        hoy_humedad_relativa = json_normalize(hoy['humedadRelativa'])  
        hoy_temperatura = json_normalize(hoy['temperatura'])  
        hoy_nieve = json_normalize(hoy['nieve'])  
        hoy_sensacion_termica = json_normalize(hoy['sensTermica'])  
        hoy_viento_racha_max = json_normalize(hoy['vientoAndRachaMax']) 
        hoy_racha_max = hoy_viento_racha_max[['periodo','direccion','velocidad']]
        hoy_racha_max = hoy_racha_max[0::2]
        hoy_viento = hoy_viento_racha_max[['periodo','value']]
        hoy_viento = hoy_viento[1::2]
        hoy_nieve.columns = ['periodo','nieve']
        hoy_temperatura.columns = ['periodo','temperatura']
        hoy_sensacion_termica.columns = ['periodo','sensacion_termica']
        hoy_humedad_relativa.columns = ['periodo','humedad_relativa']
        hoy_precipitacion.columns = ['periodo','precipitacion']
        hoy_estado_cielo.columns = ['descripcion','periodo','estado_cielo']
        hoy_racha_max.columns = ['periodo','direccion','velocidad']
        hoy_viento.columns = ['periodo','viento']

        resumen_hoy_1h = pd.merge(hoy_precipitacion,hoy_nieve)
        resumen_hoy_1h = pd.merge(resumen_hoy_1h,hoy_humedad_relativa)
        resumen_hoy_1h = pd.merge(resumen_hoy_1h,hoy_estado_cielo)
        resumen_hoy_1h = pd.merge(resumen_hoy_1h,hoy_sensacion_termica)
        resumen_hoy_1h = pd.merge(resumen_hoy_1h,hoy_temperatura)
        #resumen_hoy_1h = pd.merge(resumen_hoy_1h,hoy_racha_max)
        resumen_hoy_1h = pd.merge(resumen_hoy_1h,hoy_viento)
        #print(resumen_hoy_1h)
            
    return resumen_hoy_1h, resumen_hoy_6h

def consulta_diaria_aemet(apikey, localidad, query_semana, API):
    import http.client
    import urllib.request, json 
    from pandas.io.json import json_normalize
    import pandas as pd
    import numpy as np
    
    # configuraciones previas
    periodo = "diaria"

    # consulta AEMET
    conn = http.client.HTTPSConnection("opendata.aemet.es")
    headers = {'cache-control': "no-cache"}
    request = "https://opendata.aemet.es/opendata/api/" + API + "/" + periodo + "/" + localidad + "/" + "?api_key=" + apikey
    conn.request("GET", request, headers=headers)
    res = conn.getresponse()
    aux = res.read()

    data = aux.decode("utf-8")
    #print(data)

    json_data = json.loads(data)
    URL_datos = json_data['datos']
    URL_metadatos = json_data['metadatos']

    # Decodifica el JSON
    if (json_data['estado']==200):
        #print(URL_datos)
        data = urllib.request.urlopen(URL_datos)#.read()
        json_url = data.read()
        L = len(json_url)
        json_aemet = json_url[2:L-1].decode('ANSI')
        #print(json_aemet)
        message = json.loads(json_aemet)
    
        precipitacion = message['prediccion']
        dia = json_normalize(precipitacion['dia'])
        out = dia[query_semana]
        #print(out)
                    
    return out
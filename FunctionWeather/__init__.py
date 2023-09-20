import datetime
import logging
import json 
import datetime
import requests
from azure.storage.blob import BlobServiceClient

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
        
    # Variables necesarias 
    url = 'https://opendata.aemet.es/opendata/api/observacion/convencional/todas'
    headers = {'accept': 'application/json',"api_key":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhYW1hcnF1ZXpAc3RlbWRvLmlvIiwianRpIjoiNjYwNTIyNjQtMzA0Yi00MTc5LWEwZDgtMzYwYzRmMDVjZjEwIiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE2OTUwMzUxOTcsInVzZXJJZCI6IjY2MDUyMjY0LTMwNGItNDE3OS1hMGQ4LTM2MGM0ZjA1Y2YxMCIsInJvbGUiOiIifQ.efyv_q7tMIjbaxlub4etvdF5qCW-HSdCIPHBTRSWRRA"}

    #Conseguimos el primer archivo en el que luego vamos a buscar la url que queremos
    req = requests.get(url, headers=headers)

    # Si quitas el # puedes ver si estas consiguiendo bien el primer archivo
    # print(req.text)

    # Con esto guardamos lo anterior en formato json
    objeto_json = json.loads(req.text)

    # Buscamos en el json el apartado de datos que contiene la url que queremos
    nueva_url= objeto_json["datos"]

    # Si quitas el # puedes comprobar si estas cogiendo la url bien
    # print(nueva_url)

    #Aqui hacemos una llamada y obtenemos el json que queremos
    response = requests.get(nueva_url,headers=headers)

    # Si quitas el # puedes ver si lo estas recibiendo todo correctamente
    # print(response.text)

    #Aqui indico la ruta del archivo donde voy a escribir todo el contenido
    nombre_archivo = r"C:\Users\aoioan\Desktop\Pruebas\Prueba.json"

    # Aqui escribimos en el archivo indicado antes
    #with open(nombre_archivo, "w") as archivo:
        # Escribe el contenido de la variable en el archivo y ademas borramos por si hubiera algo
        #archivo.truncate(0)
        #archivo.write(str(response.text))

    #print(response.text)
    # URL de la firma de acceso compartido (SAS)
    url_sas = "https://stemdoasaoi.blob.core.windows.net/weatherdata?sp=racwdl&st=2023-09-20T08:06:05Z&se=2023-09-20T16:06:05Z&sv=2022-11-02&sr=c&sig=%2B0EiQn29GRW0Zb4cDjnVKxPMlSDvZvbP0ymaqSZshYU%3D"

    # Nombre del archivo local que deseas cargar
    nombre_archivo_local = response.text

    # Obtener la fecha y hora actual
    fecha_hora_actual = datetime.datetime.now()

    # Formatear la fecha y hora como un timestamp en el formato deseado (por ejemplo, yyyyMMdd_HHmmss)
    timestamp = fecha_hora_actual.strftime("%Y%m%d_%H%M%S")

    # Nombre del archivo en el contenedor de Azure Blob Storage
    nombre_archivo_blob = f"tiempo_{timestamp}.json"

    # Conecta con el servicio de Blob Storage utilizando la URL de SAS
    blob_service_client = BlobServiceClient(account_url=url_sas)

    # Obtiene una referencia al contenedor donde deseas cargar el archivo
    container_client = blob_service_client.get_container_client("weatherdata")

    # Carga el archivo al contenedor
    # with open(nombre_archivo_local, "rb") as archivo:
    blob_client = container_client.get_blob_client(nombre_archivo_blob)
    blob_client.upload_blob(json.dumps(json.loads(response.text),indent=2))

    print(f"Se ha cargado el archivo {nombre_archivo_blob} al contenedor de Blob Storage.")

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

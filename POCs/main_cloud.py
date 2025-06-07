import os
from io import BytesIO

import pandas as pd

from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import DocumentAnalysisFeature
from azure.storage.blob import BlobServiceClient


# Configuración de la "Azure storage account"
STORAGE_CONNECTION_STRING = os.environ["STORAGE_CONN_STR"]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]

# Configuración de "Azure document intelligence"
DOC_INTELLIGENCE_ENDPOINT = os.environ["DOCUMENT_INTELLIGENCE_URL"]
DOC_INTELLIGENCE_KEY = os.environ["DOCUMENT_INTELLIGENCE_KEY"]

PDF_FILENAME = "psd-data.pdf"


def to_pandas(t_result):
    """Convierte una table en un DataFrame de Pandas"""
    df = pd.DataFrame()
    
    for c in t_result.cells:
        df.at[c.row_index, c.column_index] = c.content
    
    df.columns = df.iloc[0]
    df = df[1:]

    return df
    

if __name__ == "__main__":

    storage_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)

    # Descarga el document de la cuenta de almacenamiento
    blob_data = BytesIO()
    container_client = storage_client.get_container_client(CONTAINER_NAME)
    container_client.download_blob(PDF_FILENAME).readinto(blob_data)
    blob_data.seek(0)

    # El client para interactuar con "document intelligence services"
    doc_client = DocumentIntelligenceClient(
        DOC_INTELLIGENCE_ENDPOINT,
        AzureKeyCredential(DOC_INTELLIGENCE_KEY)
    )

    # Analiza el documento descargado
    poller = doc_client.begin_analyze_document(
        "prebuilt-layout",
        body=blob_data,
        features=[DocumentAnalysisFeature.KEY_VALUE_PAIRS]
    )
    result = poller.result()

    # Chequea si el resultado del análisis encontró tablas
    if not result.tables:
        print("No existen table en el documento analizado")
        exit()

    # Transforma todas las tablas en una lista de DataFrames de Pandas
    tables = [to_pandas(t) for t in result.tables]

    # Imprime las primeras dos tablas para validar su funcionamiento
    print(tables[0].to_string(index=False))
    print("")
    print(tables[1].to_string(index=False))
    print("")
    print("Chau-chis!")


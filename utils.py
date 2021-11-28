from zipfile import ZipFile
import os 
from pyspark.sql import SparkSession, SQLContext

def spark_configs(memory, executor_memory='4G', max_cores='2'):
    """
    Método para definir configurações do cluster spark local.
    O único parâmetro necessário é o de memory. Preencha-o 
    com um valor menor do que a memória de seu computador.
    Os valores devem ser preenchidos em formato de texto.
    """
    spark_session = SparkSession.builder \
        .master("local") \
        .appName("leitor-dados-rf-empresas") \
        .config("spark.driver.memory",memory) \
        .config('spark.executor.memory', executor_memory) \
        .config("spark.cores.max", max_cores) \
        .getOrCreate()
    sqlContext = SQLContext(spark_session)
    spark_context = spark_session.sparkContext
    return spark_session, sqlContext, spark_context

def descompactando (caminho_do_arquivo, tipo_arquivo, destino):
    """
    Função para descompactar todos os arquivos dentro de
    um mesmo diretório.
    *caminho_do_arquivo:* caminho completo do diretório que 
    contém diversos arquivos compactados.
    *tipo_arquivo:* extensão utilizada, como .zip.
    *destino:* caminho completo da pasta de destino dos
    arquivos a serem descompactados.
    """
    files = os.listdir(caminho_do_arquivo)
    for file in files:
        if file.endswith(tipo_arquivo):
            file_path = caminho_do_arquivo + file
            with ZipFile(file_path) as zip_file:
                zip_file.extract(member=file.strip('.zip'), path=destino)
            print('arquivo descompactado: ', file)
        else:
            print('tipo de arquivo ainda não suportado')
    return 'arquivos descompactados com sucesso'

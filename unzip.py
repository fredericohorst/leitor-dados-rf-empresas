from zipfile import ZipFile
import os 

def descompactando (caminho_do_arquivo, tipo_arquivo, caminho_descompactado):
    files = os.listdir(caminho_do_arquivo)
    for file in files:
        if file.endswith(tipo_arquivo):
            file_path = caminho_do_arquivo + file
            with ZipFile(file_path) as zip_file:
                zip_file.extract(member=file.strip('.zip'), path=caminho_descompactado)
            print('arquivo descompactado: ', file)
    return 'arquivos descompactados com sucesso'



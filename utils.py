from zipfile import ZipFile
import os 
import wget

def descompactando (caminho_do_arquivo, tipo_arquivo, destino):
    files = os.listdir(caminho_do_arquivo)
    for file in files:
        if file.endswith(tipo_arquivo):
            file_path = caminho_do_arquivo + file
            with ZipFile(file_path) as zip_file:
                zip_file.extract(member=file.strip('.zip'), path=destino)
            print('arquivo descompactado: ', file)
    return 'arquivos descompactados com sucesso'


def downloader (arquivos_urls, prefixo, destino):
    for file in arquivos_urls:
        f = open(prefixo + file, 'r')
        urls = f.read().split()
        f.close()
        for url in urls:
            filename = wget.download(url,out=destino)
    return 'download concluído com sucesso'
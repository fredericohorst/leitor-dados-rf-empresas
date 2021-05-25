
import os 
import utils

# buscando as variáveis
path = os.getcwd()
zipped = path + '/arquivos-zip/'
print(zipped)
unzipped = path + '/arquivos-unzip/'

# arquivos contendo as urls de download
lista_urls = ['outros.txt','empresas.txt', 'estabelecimentos.txt','socios.txt']

def start():
    # iniciando o downloader:
    utils.downloader(arquivos_urls=lista_urls, prefixo=zipped,destino=zipped)
    # iniciando o descompactador:
    utils.descompactando(caminho_do_arquivo=zipped, tipo_arquivo='.zip',destino=unzipped)


if __name__ == "__main__":
    start()
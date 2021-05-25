
import os 

import unzip

# comecar com o downloader
# bash command 
# bash downloader.sh

# descompactando os arquivos
path = os.getcwd()
zipped = path + '/arquivos-zip/'
print(zipped)
unzipped = path + '/arquivos-unzip/'
print(unzipped)

unzip.descompactando(caminho_do_arquivo=zipped,tipo_arquivo='.zip',caminho_descompactado=unzipped)

# leitor dos dados 
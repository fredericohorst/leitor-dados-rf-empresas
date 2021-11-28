
working_dir=$HOME/leitor-dados-rf-empresas
dir=$HOME/leitor-dados-rf-empresas/arquivos-zip

if [ -d "$dir" ]; 
then
  cd arquivos-zip
  wget -r --cut-dirs=2 -A "*.zip"  http://200.152.38.155/CNPJ/
else
  mkdir arquivos-zip
  cd arquivos-zip
  wget -r --cut-dirs=2 -A "*.zip"  http://200.152.38.155/CNPJ/
fi

echo "Download concluído com sucesso! Arquivos disponíveis no diretório /arquivos-zip."
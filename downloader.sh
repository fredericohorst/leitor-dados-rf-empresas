

# mkdir arquivos-zip
cd arquivos-zip
# mkdir empresas
# mkdir estabelecimentos
# mkdir outros
# mkdir socios

bash download-empresas.sh
bash download-outros.sh
bash download-socios.sh
bash download-estabelecimentos.sh

echo "Download concluído com sucesso! Arquivos disponíveis no diretório /arquivos-zip."
PROJETO: MONITORAMENTO DE PREÇOS DE CRIPTOMOEDAS

Descrição:
Este projeto permite que o cliente visualize preços de criptomoedas em tempo real utilizando uma API pública da AwesomeAPI. A identidade visual utilizada neste projeto, incluindo a logo da empresa, é completamente fictícia e não representa nenhuma entidade real. O objetivo inicial é criar uma solução simples e funcional, que sirva como base para futuras melhorias. Em versões futuras, planeja-se implementar o projeto com PySpark e utilizar tecnologias como containers na nuvem para escalabilidade e melhor desempenho.

Tecnologias Utilizadas:
- Python
- Pandas
- API AwesomeAPI
- MySQL
- Windows Schedule
- PowerBI
- Figma
- Docker (planejado para futuras versões)
- PySpark (planejado para futuras versões)
- Google Cloud Platform (GCP) (planejado para futuras versões)

Estrutura do Projeto:
.
- ├── __pycache__/
- ├── projeto_powerbi/
- ├── rascunho/
- ├── venv_cadastra/
- ├── .env
- ├── .gitignore
- ├── anotações/
- ├── constants.py
- ├── LICENSE
- ├── main.py
- ├── projeto_v1.ipynb
- ├── projeto.py
- ├── README.md
- ├── requirements.txt
- ├── run.bat

Requisitos:
- Python 3.11+
- PowerBI
- MySQL Workbench 8.0

Backup MySQL:
- C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump.exe" -u root -p db_cadastra > backup.sql

Passos:

1. Importar Backup Dataset MySQL
- download mysql workbanch https://dev.mysql.com/downloads/workbench/
- No menu superior: Server > Data Import
- Na aba aberta: Selecione "Import From Disk" e selecione o arquivo backup.sql que se encontra em projeto_cadastra
- Em "Default Schema to be Imported to", selecione a database que as tabelas serão importadas
- Clique em "Start Import"
- Conector para o PowerBI: https://dev.mysql.com/downloads/connector/net/

2. Clonar repositório
- git clone https://github.com/hdneves/projeto_cadastra.git

3. Executar venv
- venv_cadastra\Scripts\activate
- python -m pip install -r requirements.txt
- Executar arquivo .bat para iniciar o script

Observações:
Para garantir uma escalabidade e uma atualização por hora, diária, semanal ou mensal, não é muito recomendado utilizar o agendador nativo do windows (Windows Schedule). Entretanto, foi utilizado como forma
de realizar disparos esse método. Para futuras implementações do projeto, vale estudar a viabilidade de implementar um orquestrador de dados - assim como o Airflow - e o software docker para futuros scripts e disparos contínuos desses scripts utilizando CRON. Para projetos mais escaláveis, soluções como um servidor próprio de automação ou ambientes na nuvem são uma opção.

Prints de como configurar o arquivo .bat na pasta "config agendador"




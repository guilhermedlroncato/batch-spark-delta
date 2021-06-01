# Projeto

Construção de um pipeline de dados através de streaming utilizando spark 3.0 e arquitetura Delta Lake tudo sendo executando na Databricks.

## Descrição

O projeto possui as seguintes etapas:
- Gerar dados ficticios atraves de uma aplicação python e gravar em um bucket S3 na camanda de Landing;
- Rotina em spark 3.0 utilizando streaming para fazer a ingestão dos dados que estão na landing e salvando numa tabela bronze em formato Delta;
- Rotina em spark streaming, que irá pegar os dados da tabela bronze, criar alguns campos calculados e salvar uma tabela silver em formato Delta;
- ROtina em spark streaming que irá pegar os dados na tabela silver, criar algumas transformações/agregações e salvar uma tabela gold em formato Delta.

### Dependencias

* Instalar o pacote python que irá gerar os dados fakes da camada de landing
```
pip install faker
```
### Instalação

* Verificar

### Execução do Pipeline

* Verificar
```
code blocks for commands
```

## Ajuda

Verificar

## Autor

Nome do autor do projet

Guilherme Roncato (guilhermeroncato@gmail.com)

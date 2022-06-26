# EP1 - Desenvolvimento de Sistemas Distribuidos
Esse repositorio se refere ao programa que analisa os dados do Google Borg, sistema distribuido da google,
que controla a execução de tasks do cluster

## Execução
A analise é feita pela dimensão escolhida (dia ou hora)
É apenas necessário ter o trace no mesmo diretório que o programa


```python
python3 spark_analysis --dimension='day'
python3 spark_analysis --dimension='hour'
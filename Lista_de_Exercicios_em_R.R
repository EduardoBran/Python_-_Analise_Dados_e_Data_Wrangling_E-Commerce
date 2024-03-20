####  Big Data Real-Time Analytics com Python e Spark  ####

# Configurando o diretório de trabalho
setwd("~/Desktop/DataScience/CienciaDeDados/2.Big-Data-Real-Time-Analytics-com-Python-e-Spark/4.Analise_Exploratoria_de_Dados")
getwd()



## Importando Pacotes
library(readxl)         # carregar arquivos
library(dplyr)          # manipula dados
library(tidyr)          # manipula dados (funcao pivot_longer)
library(ggplot2)        # gera gráficos
library(patchwork)      # unir gráficos




####  Lista de Exercícios  ####

# -> Respondas as questões.
# -> # O dataset contendo registros de corridas de táxi.



## Carregando os dados
df <- data.frame(read.csv2("dados/dataframe.csv", sep = ","))
df$Data <- as.Date(df$Data)
df$Distancia <- as.numeric(df$Distancia)
df$Segundos <- as.numeric(df$Segundos)
df$Minutos <- as.numeric(df$Minutos)
df$Min_Por_Km <- as.numeric(df$Min_Por_Km)
head(df)
str(df)



#### Exercício 1 - Qual o valor máximo da coluna Minutos?





#### Exercício 2 - Qual o valor mínimo de distância acima de 2.0?





#### Exercício 3 - Crie um plot com a frequência acumulada da coluna Distancia.





#### Exercício 4 - Qual o dia da semana no índice de posição zero?





#### Exercício 5 - Qual o dia da semana nos índices nas 5 primeiras posições?





#### Exercício 6 - Extraia todos os dias da semana (em formato texto) e insira em uma nova coluna no dataframe df.





#### Exercício 7 - Crie um gráfico de barras com o total da distância percorrida em cada dia da semana.





#### Exercício 8 - Delete a coluna Tempo do dataframe df.





#### Exercício 9 - Qual o total de corridas de taxi por dia da semana?





#### Exercício 10 - Qual a média para cada uma das colunas por dia da semana?





library(basedosdados)
library(tidyverse)

set_billing_id("dados-caged-386217")

# ANTIGO CAGED

query_caged = 
  "SELECT ano, mes, id_municipio, sigla_uf, cnae_2, SUM(saldo_movimentacao), 
  SUM(CASE WHEN CAST(admitidos_desligados AS INT64) = 1 THEN 1 ELSE 0 END) AS admissoes,
  SUM(CASE WHEN CAST(admitidos_desligados AS INT64) = 2 THEN 1 ELSE 0 END) AS demissoes
  
  FROM `basedosdados.br_me_caged.microdados_antigos`
  
  WHERE sigla_uf = 'SP' AND id_municipio IN 
                        ('3501608',
                         '3503802',
                         '3509502',
                         '3512803',
                         '3515152',
                         '3519055',
                         '3519071',
                         '3520509',
                         '3523404',
                         '3524709',
                         '3531803',
                         '3532009',
                         '3533403',
                         '3536505',
                         '3537107',
                         '3545803',
                         '3548005',
                         '3552403',
                         '3556206',
                         '3556701')

GROUP BY ano, mes, id_municipio, sigla_uf, cnae_2"

download(query = query_caged,
path = "CAGED_dados.csv")

setwd("C://Users//User//Documents//Observatório PUC Camp//Outros//Data")

dados_caged <- read.csv("CAGED_dados.csv") %>% 
rename(saldo_emprego = f0_) 

dados_caged <- dados_caged %>%
  group_by(ano, mes, cnae_2) %>% 
  summarise(saldo_emprego = sum(saldo_emprego), admissoes = sum(admissoes), 
            demissoes = sum(demissoes))

dados_caged <- dados_caged %>%
  mutate(cnae_2 = as.factor(cnae_2)) %>% 
  mutate(cnae_2 = str_sub(paste0(0,cnae_2),-5))

dados_caged$cnae_2 <- paste0(substr(dados_caged$cnae_2, 1, nchar(dados_caged$cnae_2) - 1),
                             "-", substr(dados_caged$cnae_2, nchar(dados_caged$cnae_2), 
                                         nchar(dados_caged$cnae_2)))

# NOVO CAGED
query_novo_caged = 
  "SELECT ano, mes, id_municipio, sigla_uf, cnae_2_subclasse, SUM(saldo_movimentacao), 
  SUM(CASE WHEN saldo_movimentacao = 1 THEN 1 ELSE 0 END) AS admissoes,
  SUM(CASE WHEN saldo_movimentacao = -1 THEN 1 ELSE 0 END) AS demissoes
  
  FROM `basedosdados.br_me_caged.microdados_movimentacao`
  
  WHERE sigla_uf = 'SP' AND id_municipio IN 
                        ('3501608',
                         '3503802',
                         '3509502',
                         '3512803',
                         '3515152',
                         '3519055',
                         '3519071',
                         '3520509',
                         '3523404',
                         '3524709',
                         '3531803',
                         '3532009',
                         '3533403',
                         '3536505',
                         '3537107',
                         '3545803',
                         '3548005',
                         '3552403',
                         '3556206',
                         '3556701')

  GROUP BY ano, mes, id_municipio, sigla_uf, cnae_2_subclasse"

download(query = query_novo_caged,
         path = "NOVO_CAGED_dados.csv")

setwd("C://Users//User//Documents//Observatório PUC Camp//Outros//Data")

dados_novo_caged <- read.csv("NOVO_CAGED_dados.csv")# %>% 
  rename(saldo_emprego = f0_) 

dados_novo_caged <- dados_novo_caged %>%
  group_by(ano, mes, cnae_2_subclasse) %>% 
  summarise(saldo_emprego = sum(saldo_emprego), admissoes = sum(admissoes), 
            demissoes = sum(demissoes))

dados_novo_caged <- dados_novo_caged %>%
  mutate(cnae_2 = as.factor(cnae_2_subclasse)) %>% 
  mutate(cnae_2 = str_sub(paste0(0,cnae_2),-5))

dados_caged$cnae_2 <- paste0(substr(dados_caged$cnae_2, 1, nchar(dados_caged$cnae_2) - 1),
                             "-", substr(dados_caged$cnae_2, nchar(dados_caged$cnae_2), 
                                         nchar(dados_caged$cnae_2)))

# COMEX STATS - EXP

query_comex_exp =
  "SELECT ano, mes, id_municipio, sigla_uf, id_sh4, sum(valor_fob_dolar) 
  FROM `basedosdados.br_me_comex_stat.municipio_exportacao` 
  WHERE ano > 2006 AND sigla_uf = 'SP' AND id_municipio IN 
                        ('3501608',
                         '3503802',
                         '3509502',
                         '3512803',
                         '3515152',
                         '3519055',
                         '3519071',
                         '3520509',
                         '3523404',
                         '3524709',
                         '3531803',
                         '3532009',
                         '3533403',
                         '3536505',
                         '3537107',
                         '3545803',
                         '3548005',
                         '3552403',
                         '3556206',
                         '3556701')
  GROUP BY ano, mes, id_municipio, sigla_uf, id_sh4"

setwd("C://Users//User//Documents//Observatório PUC Camp//Outros//Data")

download(query = query_comex_exp,
         path = "COMEX_dados_exp.csv")

dados_comex_exp <- read.csv("COMEX_dados_exp.csv") %>% 
  rename(valor_exp = f0_) 

dados_comex_exp <- dados_comex_exp %>%
  mutate(id_sh4 = as.factor(id_sh4)) %>% 
  mutate(id_sh4 = str_sub(paste0(0,id_sh4),-4)) %>% 
  group_by(ano, mes, id_sh4) %>% 
  summarise(valor_exp = sum(valor_exp))

# COMEX STATS - IMP

query_comex_imp =
  "SELECT ano, mes, id_municipio, sigla_uf, id_sh4, sum(valor_fob_dolar) 
  FROM `basedosdados.br_me_comex_stat.municipio_importacao` 
  WHERE ano > 2006 AND sigla_uf = 'SP' AND id_municipio IN 
                        ('3501608',
                         '3503802',
                         '3509502',
                         '3512803',
                         '3515152',
                         '3519055',
                         '3519071',
                         '3520509',
                         '3523404',
                         '3524709',
                         '3531803',
                         '3532009',
                         '3533403',
                         '3536505',
                         '3537107',
                         '3545803',
                         '3548005',
                         '3552403',
                         '3556206',
                         '3556701')
  GROUP BY ano, mes, id_municipio, sigla_uf, id_sh4"

download(query = query_comex_imp,
         path = "COMEX_dados_imp.csv")

setwd("C://Users//User//Documents//Observatório PUC Camp//Outros//Data")

dados_comex_imp <- read.csv("COMEX_dados_imp.csv") %>% 
  rename(valor_imp = f0_) 

dados_comex_imp <- dados_comex_imp %>%
  mutate(id_sh4 = as.factor(id_sh4)) %>% 
  mutate(id_sh4 = str_sub(paste0(0,id_sh4),-4)) %>% 
  group_by(ano, mes, id_sh4) %>% 
  summarise(valor_imp = sum(valor_imp))

#######################-------------------------##########################

setwd("C://Users//User//Documents//Observatório PUC Camp//Outros//CAGED -  IBGE (NCM x CNAE)")
tabela_corr <- readxl::read_xlsx("Tabela de correspondencia CNAE x NCM_v5.xlsx")
tabela_corr_ag <- readxl::read_xlsx("Tabela de correspondencia CNAE_ag x NCM_v7.xlsx")

tabela_corr_ag <- tabela_corr_ag %>% 
  rename(cnae_2 = `CNAE 2.0`)

# Join e tradução dos bancos de dados

dados_comex <- dados_comex_exp %>% 
  full_join(dados_comex_imp, join_by(ano, mes, id_sh4)) %>% 
  arrange(ano, mes, id_sh4)

dados_caged_corr <- dados_caged %>% 
  semi_join(tabela_corr_ag, by = "cnae_2") # eliminar cnaes que não tem tradução

dados_comex_ncm <- dados_comex %>% 
  left_join(tabela_corr_ag, join_by(id_sh4 == NCM)) 

dados_comex_exp_ncm <- dados_comex_exp %>% 
  left_join(tabela_corr_ag, join_by(id_sh4 == NCM)) 
  
dados_comex_imp_ncm <- dados_comex_imp %>% 
  left_join(tabela_corr_ag, join_by(id_sh4 == NCM)) 

# Tabela conjunta
dados_comex_caged <- dados_comex_ncm %>% 
  inner_join(dados_caged_corr, join_by(cnae_2, ano, mes)) %>% 
  group_by(ano, mes, id_sh4, valor_exp, valor_imp, NCM_descricao, CNAE_ag, CNAE_ag_descricao) %>% 
  summarise(saldo_emprego = sum(saldo_emprego), admissoes = sum(admissoes), 
            demissoes = sum(demissoes)) %>% 
  relocate(valor_exp, .after = CNAE_ag_descricao) %>% 
  relocate(valor_imp, .after = valor_exp) %>% 
  arrange(ano, mes, id_sh4)

# Tabela para exportações
dados_comex_exp_caged <- dados_comex_exp_ncm %>% 
  inner_join(dados_caged_corr, join_by(cnae_2, ano, mes)) %>% 
  group_by(ano, mes, id_sh4, valor_exp, NCM_descricao, CNAE_ag, CNAE_ag_descricao) %>% 
  summarise(saldo_emprego = sum(saldo_emprego), admissoes = sum(admissoes), 
            demissoes = sum(demissoes)) %>% 
  relocate(valor_exp, .after = CNAE_ag_descricao) %>% 
  arrange(ano, mes, id_sh4)

# Tabela para importações
dados_comex_imp_caged <- dados_comex_imp_ncm %>% 
  inner_join(dados_caged_corr, join_by(cnae_2, ano, mes)) %>% 
  group_by(ano, mes, id_sh4, valor_imp, NCM_descricao, CNAE_ag, CNAE_ag_descricao) %>% 
  summarise(saldo_emprego = sum(saldo_emprego), admissoes = sum(admissoes), 
            demissoes = sum(demissoes)) %>% 
  relocate(valor_imp, .after = CNAE_ag_descricao) %>% 
  arrange(ano, mes, id_sh4)


#library(openxlsx)

#setwd("C://Users//User//Documents//Observatório PUC Camp//Projects//Projects-PUCC")
#write.xlsx(dados_comex_caged, "Tabela antigo CAGEDxCOMEX conjunta.xlsx", rowNames = FALSE)
#write.xlsx(dados_comex_exp_caged, "Tabela antigo CAGEDxCOMEX exp.xlsx", rowNames = FALSE)
#write.xlsx(dados_comex_imp_caged, "Tabela antigo CAGEDxCOMEX imp.xlsx", rowNames = FALSE)


         
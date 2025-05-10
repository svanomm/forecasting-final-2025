library(tidyverse)
library(here)

naive   <- read_csv(here("../output/predictions_naive_202505091536.csv"))
ets     <- read_csv(here("../output/predictions_ets_202505091536.csv"))
arima   <- read_csv(here("../output/predictions_arima_202505091536.csv"))
sarimax <- read_csv(here("../output/predictions_sarimax_202505091536.csv"))
nn      <- read_csv(here("../output/predictions_nn_202505081730.csv"))
gbm     <- read_csv(here("../output/predictions_gbm_202505091956.csv"))

df <- naive |> rename(naive = item_cnt_month)
df <- df |> left_join(ets, by="ID", keep=FALSE) |> 
  rename(ets = item_cnt_month)
df <- df |> left_join(arima, by="ID", keep=FALSE) |> 
  rename(arima = item_cnt_month)
df <- df |> left_join(sarimax, by="ID", keep=FALSE) |> 
  rename(sarimax = item_cnt_month)
df <- df |> left_join(nn, by="ID", keep=FALSE) |> 
  rename(nn = item_cnt_month)
df <- df |> left_join(gbm, by="ID", keep=FALSE) |> 
  rename(gbm = item_cnt_month)

df <- df |> pivot_longer(
  cols = c(naive, ets, arima, sarimax, nn, gbm),
  names_to = "model",
  values_to = "item_cnt_month"
) |> group_by(ID) |> mutate(
  ensemble_all    = mean(item_cnt_month),
  ensemble_median = median(item_cnt_month),
) |> ungroup() |> pivot_wider(
  names_from = model,
  values_from = c(item_cnt_month)
)

df <- df |> mutate(
  ensemble_noml = (naive+ets+arima+sarimax)/4,
  ensemble_ml   = (nn+gbm)/2
)

exp_noml = df |>
  select(ID, ensemble_noml) |>
  rename(item_cnt_month = ensemble_noml)
exp_ml = df |>
  select(ID, ensemble_ml) |>
  rename(item_cnt_month = ensemble_ml)
exp_all = df |>
  select(ID, ensemble_all) |>
  rename(item_cnt_month = ensemble_all)
exp_median = df |>
  select(ID, ensemble_median) |>
  rename(item_cnt_month = ensemble_median)

write_csv(exp_noml  , here("../output/ensemble_noml_202505091957.csv"))
write_csv(exp_ml    , here("../output/ensemble_ml_202505091957.csv"))
write_csv(exp_all   , here("../output/ensemble_all_202505091957.csv"))
write_csv(exp_median, here("../output/ensemble_median_202505091957.csv"))

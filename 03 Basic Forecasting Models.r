library(tidyverse)
library(fpp3)
library(tsibble)
library(here)
library(data.table)

# Load the tables
reg_data <- read_csv(here("../data/reg_data.csv"))

# We need to aggregate the data because it's so large.
reg_data <- reg_data |> as.data.table()

# Add a flag for brand new products, shops, and both
reg_data <- reg_data |>
  mutate(
    brand_new = ifelse(max_qty_ever == 0, 1, 0)
  ) |> group_by(item_category_id) |> mutate(
    new_item_category = ifelse(max(max_qty_ever) == 0, 1, 0)
  ) |> ungroup()

reg_data_long <- reg_data |>
  pivot_longer(
    cols = -c("ID", "shop_id", "item_id", "item_category_id", "ym"),
    names_to = "var", values_to = "val"
  )

reg_data_shop <- reg_data_long |> 
  group_by(shop_id, ym, var) |> 
  summarise(
    mean = mean(val),
  ) |>
  pivot_wider(
    names_from = var,
    values_from = mean
  )

reg_data_prodcat <- reg_data_long |> 
  group_by(item_category_id, ym, var) |> 
  summarise(
    mean = mean(val),
  ) |>
  pivot_wider(
    names_from = var,
    values_from = mean
  )

# Delete reg_data_long to free up memory
rm(reg_data_long)
gc()

# Convert ym to yearmonth
reg_data_shop <- reg_data_shop |>
  mutate(ym = yearmonth(ym))

reg_data_prodcat <- reg_data_prodcat |>
  mutate(ym = yearmonth(ym))

# Declare tsibble
reg_data_shop <- reg_data_shop |> as.data.table() |>
  as_tsibble(key = shop_id, index = ym) |>
  filter(ym > yearmonth("2013 Mar")) # data clipping

reg_data_prodcat <- reg_data_prodcat |> as.data.table() |>
  as_tsibble(key = item_category_id, index = ym) |>
  filter(ym > yearmonth("2013 Mar")) # data clipping

train_shop    <- reg_data_shop    |> filter(ym <  yearmonth("2015 Nov"))
 test_shop    <- reg_data_shop    |> filter(ym == yearmonth("2015 Nov"))
train_prodcat <- reg_data_prodcat |> filter(ym <  yearmonth("2015 Nov"))
 test_prodcat <- reg_data_prodcat |> filter(ym == yearmonth("2015 Nov"))

# Shop models ----
models_shop <- train_shop |>
	model(
		naive = NAIVE(qty),
		ets   = ETS(qty ~ trend("A")  + season("A")),
		arima = ARIMA(qty),
		sarimax = ARIMA(qty ~ 
		  price_mean_complement_prod_1_pclag1 +
	    price_mean_complement_prod_2_pclag1 +
	    price_mean_complement_prod_3_pclag1 +
		  price_mean_substitute_shop_1_pclag1 +
	    price_mean_substitute_shop_2_pclag1 +
	    price_mean_substitute_shop_3_pclag1 +
	    product_qty_pclag1 +
	    qty_substitute_prod_pclag1 +
	    relative_price_lag1 +
	    relative_price3_lag1 +
	    shop_qty_pclag1
		  )
	)

forecasts_shop <- models_shop |>
 forecast(new_data = test_shop) |>
 as.data.frame() |>
 select(shop_id, .model, ym, .mean) |>
 pivot_wider(
   names_from = .model,
   values_from = .mean
 )

test_shop_pred <- test_shop |> as.data.frame() |>
 select(shop_id, ym, qty_lag1) |>
 left_join(
   forecasts_shop,
   by = c("shop_id", "ym"),
   keep = FALSE
 ) |> mutate(
   shop_factor_naive   = naive   / qty_lag1,
   shop_factor_ets     = ets     / qty_lag1,
   shop_factor_arima   = arima   / qty_lag1,
   shop_factor_sarimax = sarimax / qty_lag1
 ) |> mutate(
   shop_factor_naive   = ifelse(is.na(shop_factor_naive), 1, shop_factor_naive),
   shop_factor_ets     = ifelse(is.na(shop_factor_ets), 1, shop_factor_ets),
   shop_factor_arima   = ifelse(is.na(shop_factor_arima), 1, shop_factor_arima),
   shop_factor_sarimax = ifelse(is.na(shop_factor_sarimax), 1, shop_factor_sarimax)
 ) |>  select(-c(qty_lag1, naive, ets, arima, sarimax))
 
 
# Prodcat models ----
models_prodcat <- train_prodcat |>
  model(
    naive = NAIVE(qty),
    ets   = ETS(qty ~ trend("A")  + season("A")),
    arima = ARIMA(qty),
    sarimax = ARIMA(qty ~ 
      price_mean_complement_prod_1_pclag1 +
      price_mean_complement_prod_2_pclag1 +
      price_mean_complement_prod_3_pclag1 +
      price_mean_substitute_shop_1_pclag1 +
      price_mean_substitute_shop_2_pclag1 +
      price_mean_substitute_shop_3_pclag1 +
      product_qty_pclag1 +
      qty_substitute_prod_pclag1 +
      relative_price_lag1 +
      relative_price3_lag1 +
      shop_qty_pclag1
    )
  )

forecasts_prodcat <- models_prodcat |>
  forecast(new_data = test_prodcat) |>
  as.data.frame() |>
  select(item_category_id, .model, ym, .mean) |>
  pivot_wider(
    names_from = .model,
    values_from = .mean
  )

test_prodcat_pred <- test_prodcat |> as.data.frame() |>
  select(item_category_id, ym, qty_lag1) |>
  left_join(
    forecasts_prodcat,
    by = c("item_category_id", "ym"),
    keep = FALSE
  ) |> mutate(
    prodcat_factor_naive   = naive   / qty_lag1,
    prodcat_factor_ets     = ets     / qty_lag1,
    prodcat_factor_arima   = arima   / qty_lag1,
    prodcat_factor_sarimax = sarimax / qty_lag1
  ) |> mutate(
    prodcat_factor_naive   = ifelse(is.na(prodcat_factor_naive), 1, prodcat_factor_naive),
    prodcat_factor_ets     = ifelse(is.na(prodcat_factor_ets), 1, prodcat_factor_ets),
    prodcat_factor_arima   = ifelse(is.na(prodcat_factor_arima), 1, prodcat_factor_arima),
    prodcat_factor_sarimax = ifelse(is.na(prodcat_factor_sarimax), 1, prodcat_factor_sarimax)
  ) |>  select(-c(qty_lag1, naive, ets, arima, sarimax))


test_shop_pred <- test_shop_pred |> as.data.frame() |>
  mutate(ym = as.numeric(ym))
test_prodcat_pred <- test_prodcat_pred |> as.data.frame() |>
  mutate(ym = as.numeric(ym))

# Joining back to full data ----
predictions <- reg_data[reg_data$ym == 550] |>
  left_join(
    test_shop_pred, by = c("shop_id", "ym"), keep = FALSE
  ) |> 
  left_join(
    test_prodcat_pred, by = c("item_category_id", "ym"), keep = FALSE
  ) |> 
  mutate(
    shop_factor_naive   = ifelse(is.na(shop_factor_naive), 1, shop_factor_naive),
    shop_factor_ets     = ifelse(is.na(shop_factor_ets), 1, shop_factor_ets),
    shop_factor_arima   = ifelse(is.na(shop_factor_arima), 1, shop_factor_arima),
    shop_factor_sarimax = ifelse(is.na(shop_factor_sarimax), 1, shop_factor_sarimax),
    prodcat_factor_naive   = ifelse(is.na(prodcat_factor_naive), 1, prodcat_factor_naive),
    prodcat_factor_ets     = ifelse(is.na(prodcat_factor_ets), 1, prodcat_factor_ets),
    prodcat_factor_arima   = ifelse(is.na(prodcat_factor_arima), 1, prodcat_factor_arima),
    prodcat_factor_sarimax = ifelse(is.na(prodcat_factor_sarimax), 1, prodcat_factor_sarimax)
  )


predictions <- predictions |> mutate(
  item_cnt_month_naive   = qty_lag1 * shop_factor_naive * prodcat_factor_naive,
  item_cnt_month_ets     = qty_lag1 * shop_factor_ets * prodcat_factor_ets,
  item_cnt_month_arima   = qty_lag1 * shop_factor_arima * prodcat_factor_arima,
  item_cnt_month_sarimax = qty_lag1 * shop_factor_sarimax * prodcat_factor_sarimax
  ) |> mutate(
    item_cnt_month_naive = ifelse(
      item_cnt_month_naive < 0, 0, ifelse(
        item_cnt_month_naive > 20, 20, item_cnt_month_naive)),
    item_cnt_month_ets = ifelse(
      item_cnt_month_ets < 0, 0, ifelse(
        item_cnt_month_ets > 20, 20, item_cnt_month_ets)),
    item_cnt_month_arima = ifelse(
      item_cnt_month_arima < 0, 0, ifelse(
        item_cnt_month_arima > 20, 20, item_cnt_month_arima)),
    item_cnt_month_sarimax = ifelse(
      item_cnt_month_sarimax < 0, 0, ifelse(
        item_cnt_month_sarimax > 20, 20, item_cnt_month_sarimax))
  ) |>
  select(ID, qty_lag1, item_cnt_month_naive, item_cnt_month_ets,
         item_cnt_month_arima, item_cnt_month_sarimax)



# Calculate RMSE for each model
rmse_shop <- test_shop_pred |>
  mutate(
    se_naive   = (qty - naive)^2,
    se_ets     = (qty - ets)^2,
    se_arima   = (qty - arima)^2,
    se_sarimax = (qty - sarimax)^2
  ) |>
  select(shop_id, ym, qty, qty_lag1, starts_with("se")) |>
  summarise(
    rmse_naive   = sqrt(mean(se_naive)),
    rmse_ets     = sqrt(mean(se_ets)),
    rmse_arima   = sqrt(mean(se_arima)),
    rmse_sarimax = sqrt(mean(se_sarimax))
  )

forecasts_prodcat <- models_prodcat |>
  forecast(new_data = test_prodcat)


forecasts_prodcat <- forecasts_prodcat |>
  mutate(.mean = ifelse(is.na(.mean), 0, .mean))



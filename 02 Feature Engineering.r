library(tidyverse)
library(tsibble)
library(here)
library(data.table) 

# Load the tables
load(here("../data/processed_data.RData"))
data_final <- data_monthly

# Add rows for 2015 Nov for each group
data_final <- as.data.frame(data_final)
nov15 <- data_final[data_final$ym == yearmonth("2015 Oct"),] |> 
  mutate(
    ym = yearmonth("2015 Nov"),
    qty = NA,
    revenue = NA,
    price_mean = NA
  )

data_final <- data_final |> 
  bind_rows(nov15) |> 
  arrange(ID, ym)

# joining own-product's monthly qty/revenue to data
data_final <- data_final |>
  left_join(
    data_product_monthly |>
      select(-c(product_price_mean)),
    by = c("ym", "item_id", "item_category_id"),
    keep = FALSE
  )

# joining own-store's monthly qty/revenue to data
data_final <- data_final |>
  left_join(
    data_store_monthly |>
      select(-c(price_mean)),
    by = c("ym", "shop_id"),
    keep = FALSE
  ) |> rename(
    qty=qty.x,
    revenue=revenue.x,
    shop_qty = qty.y,
    shop_revenue = revenue.y
  )

# own-product category minus own-product (average substitute products)
own_sub <- data_product_monthly |> 
  left_join(
    data_prodcat_monthly, by=c("ym", "item_category_id"),
    keep = FALSE
  ) |> mutate(
    qty_substitute_prod = qty - product_qty,
    revenue_substitute_prod = revenue - product_revenue 
  ) |>
  select(
    ym, item_id, item_category_id, qty_substitute_prod, revenue_substitute_prod
  )

data_final <- data_final |>
  left_join(
    own_sub,
    by = c("ym", "item_id", "item_category_id"),
    keep = FALSE
  )

# Identifying complement product categories ----
data_product_category <- data_prodcat_monthly |>
  mutate(
    diff_qty   = (qty - lag(qty))/lag(qty),
    diff_price = (price_mean - lag(price_mean))/lag(price_mean),
    diff_qty   = ifelse(!is.finite(diff_qty), 0, diff_qty),
    diff_price = ifelse(!is.finite(diff_price), 0, diff_price),
  )

data_category_wide <- data_product_category |>
  select(-c(revenue, price_mean, qty, revenue)) |>
  pivot_wider(
    names_from = item_category_id,
    values_from = c(diff_qty, diff_price),
    names_sep = "_"
  )

corr <- data_category_wide |> 
  as.data.frame() |> 
  select(-c("ym")) |> 
  cor() 

corr <- corr |> 
  as.data.frame() |>
  mutate(id_x = row.names(corr)) |>
  pivot_longer(-id_x, names_to = "id_y", values_to = "cor") |>
  filter(
     grepl("diff_qty", id_x),
     grepl("diff_price", id_y),
     # extract the numbers from the string columns
     gsub("diff_qty_", "", id_x) != gsub("diff_price_", "", id_y)
     ) |>
  arrange(id_x, cor)

# Keep the top 5 most negative-correlated product categories
corr_top_5 <- corr |>
  group_by(id_x) |>
  slice_min(cor, n = 5) |>
  ungroup() |>
  mutate(
    id_x = gsub("diff_qty_", "", id_x),
    id_y = gsub("diff_price_", "", id_y)
  ) |>
  select(id_x, id_y, cor) |>
	filter(!is.na(cor))

# Finally, create xwalk of complement product categories
xwalk_complement_product <- corr_top_5 |>
  group_by(id_x) |>
  summarise(
    complement_prod_1 = as.numeric(id_y[1]),
    complement_prod_2 = as.numeric(id_y[2]),
    complement_prod_3 = as.numeric(id_y[3]),
    complement_prod_4 = as.numeric(id_y[4]),
    complement_prod_5 = as.numeric(id_y[5])
  ) |>
  ungroup() |> mutate(
    id_x = as.numeric(id_x)
  ) |> arrange(id_x) |> rename(item_category_id = id_x)


# Identifying substitute stores ----
data_store <- data_store_monthly |>
  mutate(
  	diff_qty   = (qty - lag(qty))/lag(qty),
  	diff_price = (price_mean - lag(price_mean))/lag(price_mean),
  	diff_qty   = ifelse(!is.finite(diff_qty), 0, diff_qty),
  	diff_price = ifelse(!is.finite(diff_price), 0, diff_price),
  )

data_store_wide <- data_store |>
  select(-c(revenue, price_mean, qty, revenue)) |>
  pivot_wider(
    names_from = shop_id,
    values_from = c(diff_qty, diff_price),
    names_sep = "_"
  )

corr <- data_store_wide |> 
  as.data.frame() |> 
  select(-c("ym")) |> 
  cor() 

corr <- corr |> 
  as.data.frame() |>
  mutate(id_x = row.names(corr)) |>
  pivot_longer(-id_x, names_to = "id_y", values_to = "cor") |>
  filter(
    grepl("diff_qty", id_x),
    grepl("diff_price", id_y),
    # extract the numbers from the string columns
    gsub("diff_qty_", "", id_x) != gsub("diff_price_", "", id_y)
  ) |>
  arrange(id_x, -cor)

# Keep the top 5 most positive-correlated product categories
corr_top_5 <- corr |>
  group_by(id_x) |>
  slice_max(cor, n = 5) |>
  ungroup() |>
  mutate(
    id_x = gsub("diff_qty_", "", id_x),
    id_y = gsub("diff_price_", "", id_y)
  ) |>
  select(id_x, id_y, cor) |>
	filter(!is.na(cor))

# Finally, create xwalk of substitute stores
xwalk_substitute_store <- corr_top_5 |>
  group_by(id_x) |>
  summarise(
    substitute_shop_1 = as.numeric(id_y[1]),
    substitute_shop_2 = as.numeric(id_y[2]),
    substitute_shop_3 = as.numeric(id_y[3]),
    substitute_shop_4 = as.numeric(id_y[4]),
    substitute_shop_5 = as.numeric(id_y[5])
  ) |>
  ungroup() |> mutate(
    id_x = as.numeric(id_x)
  ) |> arrange(id_x) |> rename(shop_id = id_x)



# Assembling the monthly data ----
# Join the xwalks on
data_final <- data_final |>
  left_join(xwalk_complement_product, by = "item_category_id", keep=FALSE) |>
  left_join(xwalk_substitute_store, by = "shop_id", keep=FALSE)

# Now for each variable, we need to join on the avg price from the corresponding monthly aggregated dataset
# Start by reshaping the data long by id
data_long <- data_final |> as.data.table() |>
  select(
    ym, shop_id, item_id, item_category_id,
    starts_with("complement_prod_"), starts_with("substitute_shop_")
  ) |>
  pivot_longer(
    cols = -c(ym, shop_id, item_id, item_category_id),
    names_to = c("var", "rank"),
    names_pattern = "^(complement_prod|substitute_shop)_(\\d+)$"
  ) |>
  pivot_wider(
    names_from = "var",
    values_from = "value"
  )
# Now we can join on the monthly data
data_long <- data_long |>
  left_join(
    data_prodcat_monthly, 
    by = join_by(ym, complement_prod == item_category_id),
    keep = FALSE
  ) |> 
  rename(qty_complement_prod        = qty, 
         revenue_complement_prod    = revenue, 
         price_mean_complement_prod = price_mean)

data_long <- data_long |>
  left_join(
    data_store_monthly, 
    by = join_by(ym, substitute_shop == shop_id),
    keep = FALSE
  ) |> 
  rename(qty_substitute_shop        = qty, 
         revenue_substitute_shop    = revenue, 
         price_mean_substitute_shop = price_mean)

# Now we can reshape the data back to wide format
data_compsub <- data_long |>
  select(-c(revenue_complement_prod, revenue_substitute_shop, 
            complement_prod, substitute_shop)) |>
  pivot_wider(
    names_from = "rank",
    values_from = c(
      qty_complement_prod, price_mean_complement_prod,
      qty_substitute_shop, price_mean_substitute_shop
    )
  )

# Now we can join the data back to the original dataset
data_final <- data_final |>
  select(-c(
    starts_with("complement_prod_"), starts_with("substitute_shop_")
  )) |>
  left_join(data_compsub, by = c("ym", "shop_id", "item_id", "item_category_id"),
            keep=FALSE)

data_final <- data_final |> as.data.table() |> group_by(shop_id, item_id)

# Price discount
data_final <- data_final |>
	mutate(
		price_mean_substitute_prod = revenue_substitute_prod / qty_substitute_prod,
		relative_price = (log(price_mean) / log(price_mean_substitute_prod)) - 1,
		relative_price = ifelse(is.na(relative_price), 0, relative_price),
		relative_price = ifelse(relative_price >  0.5,  0.5, relative_price),
		relative_price = ifelse(relative_price < -0.5, -0.5, relative_price)
	)

# Rolling averages
data_final <- data_final |>
  mutate(
    qty_roll3  = frollmean(qty, n=3 , na.rm=TRUE, fill=0),
    qty_roll6  = frollmean(qty, n=6 , na.rm=TRUE, fill=0),
    qty_roll12 = frollmean(qty, n=12, na.rm=TRUE, fill=0),
    relative_price3 = frollmean(relative_price, n=3 , na.rm=TRUE, fill=0),
  )

# Calculating lagged variables ----
calculate_lags <- function(df, var, lags){
  map_lag <- lags |> map(~partial(dplyr::lag, n = .x))
  result <- df |>
    mutate(across(.cols = {{var}}, .fns = map_lag, .names = "{.col}_lag{lags}"))
  orig_var_name <- as.character(substitute(var))
  lag_col_names <- paste0(orig_var_name, "_lag", lags)
  result <- result |>
    mutate(across(all_of(lag_col_names), ~replace_na(., 0)))
  return(result)
}
calculate_pc_changes_price <- function(df, var, lags){
	pct_change <- function(x, n) {
		pct <- (x / dplyr::lag(x, n = n)) - 1
		# Try to catch mistaken data entries
		ifelse(pct < -0.5 | pct > 0.5, 0, pct)
	}
	map_pct_change <- lags |> map(~partial(pct_change, n = .x))
	result <- df |>
		mutate(across(.cols = {{var}}, .fns = map_pct_change, .names = "{.col}_pclag{lags}"))
	orig_var_name <- as.character(substitute(var))
	lag_col_names <- paste0(orig_var_name, "_pclag", lags)
	result <- result |>
		mutate(across(all_of(lag_col_names), ~replace_na(., 0)))
	return(result)
}
calculate_pc_changes_qty <- function(df, var, lags){
	pct_change <- function(x, n) {
		pct <- (x / lag(x, n = n)) - 1
		# Try to catch outliers
		ifelse(pct < -0.5, -0.5, ifelse(pct > 0.5, 0.5, pct))
	}
	map_pct_change <- lags |> map(~partial(pct_change, n = .x))
	result <- df |>
		mutate(across(.cols = {{var}}, .fns = map_pct_change, .names = "{.col}_pclag{lags}"))
	orig_var_name <- as.character(substitute(var))
	lag_col_names <- paste0(orig_var_name, "_pclag", lags)
	result <- result |>
		mutate(across(all_of(lag_col_names), ~replace_na(., 0)))
	return(result)
}

dfsave <- data_final
data_final <- dfsave

## Own ID lags
data_final <- data_final |> calculate_lags(qty, 1:6)
data_final <- data_final |> calculate_lags(qty_roll3, 1:3)
data_final <- data_final |> calculate_lags(qty_roll6, 1:3)
data_final <- data_final |> calculate_lags(qty_roll12, 1:3)

data_final <- data_final |> calculate_lags(relative_price, 1:6)
data_final <- data_final |> calculate_lags(relative_price3, 1:3)
#data_final <- data_final |> calculate_lags(price_mean, 1:6)
#data_final <- data_final |> calculate_lags(price_median, 1:6)
#data_final <- data_final |> calculate_lags(price_max, 1:6)
#data_final <- data_final |> calculate_lags(price_min, 1:6)
#data_final <- data_final |> calculate_lags(price_sd,  1:6)

#data_final <- data_final |> calculate_lags(product_qty, 1:3)
#data_final <- data_final |> calculate_lags(shop_qty, 1:3)
#data_final <- data_final |> calculate_lags(qty_substitute_prod, 1:3)
#data_final <- data_final |> calculate_lags(qty_complement_prod_1, 1:3)
#data_final <- data_final |> calculate_lags(qty_substitute_shop_1, 1:3)

data_final <- data_final |> calculate_pc_changes_qty(product_qty, 1:3)
#data_final <- data_final |> calculate_pc_changes_qty(product_revenue, 1:3)
#data_final <- data_final |> calculate_lags(product_revenue, 1:3)
data_final <- data_final |> calculate_pc_changes_qty(shop_qty, 1:3)
#data_final <- data_final |> calculate_pc_changes_qty(shop_revenue, 1:3)
#data_final <- data_final |> calculate_lags(shop_revenue, 1:3)

data_final <- data_final |> calculate_pc_changes_qty(qty_substitute_prod, 1:3)
#data_final <- data_final |> calculate_pc_changes_qty(revenue_substitute_prod, 1:3)

#data_final <- data_final |> calculate_pc_changes_qty(qty_complement_prod_1, 1:3)
#data_final <- data_final |> calculate_pc_changes_qty(qty_complement_prod_2, 1:3)
#data_final <- data_final |> calculate_pc_changes_qty(qty_complement_prod_3, 1:3)
#data_final <- data_final |> calculate_pc_changes_qty(qty_complement_prod_4, 1:3)
#data_final <- data_final |> calculate_pc_changes_qty(qty_complement_prod_5, 1:3)
#data_final <- data_final |> calculate_pc_changes_qty(qty_substitute_shop_1, 1:3)
#data_final <- data_final |> calculate_pc_changes_qty(qty_substitute_shop_2, 1:3)
#data_final <- data_final |> calculate_pc_changes_qty(qty_substitute_shop_3, 1:3)
#data_final <- data_final |> calculate_pc_changes_qty(qty_substitute_shop_4, 1:3)
#data_final <- data_final |> calculate_pc_changes_qty(qty_substitute_shop_5, 1:3)
#data_final <- data_final |> calculate_lags(price_mean_complement_prod_1, 1:3)
#data_final <- data_final |> calculate_lags(price_mean_complement_prod_2, 1:3)
#data_final <- data_final |> calculate_lags(price_mean_complement_prod_3, 1:3)
#data_final <- data_final |> calculate_lags(price_mean_complement_prod_4, 1:3)
#data_final <- data_final |> calculate_lags(price_mean_complement_prod_5, 1:3)
#data_final <- data_final |> calculate_lags(price_mean_substitute_shop_1, 1:3)
#data_final <- data_final |> calculate_lags(price_mean_substitute_shop_2, 1:3)
#data_final <- data_final |> calculate_lags(price_mean_substitute_shop_3, 1:3)
#data_final <- data_final |> calculate_lags(price_mean_substitute_shop_4, 1:3)
#data_final <- data_final |> calculate_lags(price_mean_substitute_shop_5, 1:3)

data_final <- data_final |> calculate_pc_changes_price(price_mean_complement_prod_1, 1:3)
data_final <- data_final |> calculate_pc_changes_price(price_mean_complement_prod_2, 1:3)
data_final <- data_final |> calculate_pc_changes_price(price_mean_complement_prod_3, 1:3)
#data_final <- data_final |> calculate_pc_changes_price(price_mean_complement_prod_4, 1:3)
#data_final <- data_final |> calculate_pc_changes_price(price_mean_complement_prod_5, 1:3)
data_final <- data_final |> calculate_pc_changes_price(price_mean_substitute_shop_1, 1:3)
data_final <- data_final |> calculate_pc_changes_price(price_mean_substitute_shop_2, 1:3)
data_final <- data_final |> calculate_pc_changes_price(price_mean_substitute_shop_3, 1:3)
#data_final <- data_final |> calculate_pc_changes_price(price_mean_substitute_shop_4, 1:3)
#data_final <- data_final |> calculate_pc_changes_price(price_mean_substitute_shop_5, 1:3)

# Drop unneeded columns
data_final <- data_final |>
  select(
    c(
      ID, shop_id, item_id, item_category_id, ym, qty,
      contains("lag")
    )
  )

# Add column for store-product's max qty ever
maxes <- data_final |>
	mutate(
		max_qty_ever = max(qty, na.rm = TRUE),
	) |> select(
		shop_id, item_id, max_qty_ever
	) |> distinct()

data_final <- data_final |>
	left_join(maxes, by = c("shop_id", "item_id"), keep=FALSE)

data_final$ym <- as.numeric(data_final$ym)

# For zero records, we only need to save the last month of data. Don't care abt the other records because we're not using them for estimation
#write_csv(data_final, here("../data/reg_data_balanced.csv"), )

# Now we can remove months with 0 sales and add a field for "months since last sale"
#data_final0 <- data_final[data_final$qty > 0 | data_final$ym == 550 | data_final$ym == 516,] |> 
#	mutate(
#		months_since_last_sale = ym - lag(ym),
#		months_since_last_sale = ifelse(is.na(months_since_last_sale), 0, months_since_last_sale),
#	)

#data_final0 <- data_final0[data_final0$qty > 0 | data_final0$ym == 550,]


#write_csv(data_final0, here("../data/reg_data.csv"), )
write_csv(data_final, here("../data/reg_data.csv"), )

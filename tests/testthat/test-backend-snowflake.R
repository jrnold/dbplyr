context("test-backend-odbc.R")

test_that("custom scalar translated correctly", {

  trans <- function(x) {
    translate_sql(!!enquo(x), con = simulate_snowflake())
  }

  expect_equal(trans(as.numeric(x)),            sql("CAST(`x` AS DOUBLE)"))
  expect_equal(trans(as.double(x)),             sql("CAST(`x` AS DOUBLE)"))
  expect_equal(trans(as.integer(x)),            sql("CAST(`x` AS NUMBER(10))"))
  expect_equal(trans(as.character(x)),          sql("CAST(`x` AS VARCHAR)"))
})

test_that("numeric scalar functions translated correctly", {

  trans <- function(x) {
    translate_sql(!!enquo(x), con = simulate_snowflake())
  }

  # Rounding and truncation
  expect_equal(trans(abs(x)),           sql("ABS(`x`)"))
  expect_equal(trans(ceiling(x)),       sql("CEIL(`x`)"))
  expect_equal(trans(floor(x)),         sql("FLOOR(`x`)"))
  expect_equal(trans(x %%  y),          sql("`x` % `y`"))
  expect_equal(trans(round(x)),         sql("ROUND(`x`, 0)"))
  expect_equal(trans(round(x, digits = 1)), sql("ROUND(`x`, 1)"))
  expect_equal(trans(sign(x)),          sql("SIGN(`x`)"))
  expect_equal(trans(trunc(x)),         sql("TRUNC(`x`)"))

  # Exponent and Root
  # ignore cbrt

  # Trig funtions
  expect_equal(trans(cosh(x)),           sql("COSH(`x`)"))
  expect_equal(trans(sinh(x)),           sql("SINH(`x`)"))
  expect_equal(trans(tanh(x)),           sql("TANH(`x`)"))
  expect_equal(trans(acosh(x)),          sql("ACOSH(`x`)"))
  expect_equal(trans(asinh(x)),          sql("ASINH(`x`)"))
  expect_equal(trans(atanh(x)),          sql("ATANH(`x`)"))
  expect_equal(trans(pi),                sql("PI()"))
  expect_equal(trans(cospi(x)),          sql("COS(`x` * PI())"))
  expect_equal(trans(sinpi(x)),          sql("SIN(`x` * PI())"))
  expect_equal(trans(tanpi(x)),          sql("TAN(`x` * PI())"))
  # other
  expect_equal(trans(as.integer64(x)),   sql("CAST(`x` AS NUMBER(38, 0))"))
  expect_equal(trans(as.POSIXct(x)),     sql("CAST(`x` AS TIMESTAMP_NTZ)"))

  # Date stuff
  expect_equal(trans(yday(x)), sql("EXTRACT(dayofyear FROM `x`)"))
  expect_equal(trans(wday(x)), sql("EXTRACT(dayofweekiso FROM `x`)"))
  expect_equal(trans(qday(x)), sql("EXTRACT(dayofyear FROM `x`) - EXTRACT(dayofyear FROM DATE_TRUNC('quarter', `x`)) + 1"))

  # bit functions
  expect_equal(trans(bitwAnd(x, y)), sql("BITAND(`x`, `y`)"))
  expect_equal(trans(bitwOr(x, y)), sql("BITOR(`x`, `y`)"))
  expect_equal(trans(bitwNot(x)), sql("BITNOT(`x`)"))
  expect_equal(trans(bitwXor(x, y)), sql("BITXOR(`x`, `y`)"))
  expect_equal(trans(bitwShiftL(x, y)), sql("BITSHIFTLEFT(`x`, `y`"))
  expect_equal(trans(bitwShiftR(x, y)), sql("BITSHIFTRIGHT(`x`, `y`"))

  # Paste
  expect_equal(trans(paste("foo", 1L, a)), sql("CONCAT_WS(' ', 'foo', 1, `a`)"))
  expect_equal(trans(paste0("foo", 1L, a)), sql("CONCAT_WS('', 'foo', 1, `a`)"))

  # base string functions
  # base R
  expect_equal(trans(nchar(x)), sql("LENGTH(`x`)"))
  expect_equal(trans(tolower(x)), sql("LOWER(`x`)"))
  expect_equal(trans(toupper(x)), sql("UPPER(`x`)"))
  expect_equal(trans(trimws(x)), sql("TRIM(`x`)"))
  expect_equal(trans(substr(x, 1, 2)), sql("SUBSTR(`x`, 1, 2)"))

  expect_equal(trans(paste0("foo", 1L, a)), sql("CONCAT_WS('', 'foo', 1, `a`)"))
  # stringr
  expect_equal(trans(str_length(x)), sql("LENGTH(`x`)"))
  expect_equal(trans(str_to_lower(x)), sql("LOWER(`x`)"))
  expect_equal(trans(str_to_upper(x)), sql("UPPER(`x`)"))
  expect_equal(trans(str_to_title(x)), sql("INITCAP(`x`)"))
  expect_equal(trans(str_trim(x)), sql("TRIM(`x`)"))
  expect_equal(trans(str_c(x)), sql("CONCAT_WS('', `x`)"))

})


test_that("aggregators translated correctly", {

  trans <- function(x) {
    translate_sql(!!enquo(x), window = FALSE, con = simulate_snowflake())
  }

  # any-value not necessary
  expect_equal(trans(mean(x, na.rm=TRUE)),       sql("AVG(`x`)"))
  expect_equal(trans(cor(x, y)), sql("CORR(`x`, `y`)"))
  expect_equal(trans(n()), sql("COUNT(*)"))
  expect_equal(trans(n_distinct(x)), sql("COUNT(DISTINCT `x`)"))
  expect_equal(trans(cov(x, y)),       sql("COVAR_SAMP(`x`, `y`)"))
  expect_equal(trans(max(x, na.rm=TRUE)),               sql("MAX(`x`)"))
  expect_equal(trans(median(x, na.rm=TRUE)),            sql("MEDIAN(`x`)"))
  expect_equal(trans(sd(x, na.rm=TRUE)),         sql("STDDEV_SAMP(`x`)"))
  expect_equal(trans(sum(x, na.rm=TRUE)),        sql("SUM(`x`)"))
  expect_equal(trans(var(x, na.rm=TRUE)),        sql("VAR_SAMP(`x`)"))
  expect_equal(trans(str_flatten(x)),            sql("LISTAGG(`x`, '')"))
  expect_equal(trans(str_flatten(x, collapse = ",")),   sql("LISTAGG(`x`, ',')"))
  expect_equal(trans(var(x, na.rm=TRUE)),        sql("VAR_SAMP(`x`)"))
  expect_equal(trans(quantile(x, 0.25, na.rm=TRUE)),          sql("PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY `x`)"))
  expect_equal(trans(quantile(x, 0.25, type=1, na.rm=TRUE)),  sql("PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY `x`)"))
  expect_equal(trans(all(x, na.rm=TRUE)),       sql("BOOLAND_AGG(`x`)"))
  expect_equal(trans(any(x, na.rm=TRUE)),       sql("BOOLOR_AGG(`x`)"))
  expect_equal(trans(list(x)),       sql("ARRAY_AGG(`x`)"))
})


test_that("window functions translated correctly", {

  trans <- function(x) {
    translate_sql(!!enquo(x), window = TRUE, con = simulate_snowflake())
  }

  expect_equal(trans(mean(x, na.rm = TRUE)),       sql("AVG(`x`) OVER ()"))
  expect_equal(trans(cor(x, y)),                   sql("CORR(`x`, `y`) OVER ()"))
  expect_equal(trans(count()),                     sql("COUNT(*) OVER ()"))
  expect_equal(trans(n()),                         sql("COUNT(*) OVER ()"))
  expect_equal(trans(cov(x, y)),                   sql("COVAR_SAMP(`x`, `y`) OVER ()"))
  expect_equal(trans(str_flatten(x)),              sql("LISTAGG(`x`, ' ') OVER ()"))
  expect_equal(trans(str_flatten(x, collapse=',')),              sql("LISTAGG(`x`, ',') OVER ()"))
  expect_equal(trans(max(x, na.rm=TRUE)),                      sql("MAX(`x`) OVER ()"))
  expect_equal(trans(median(x, na.rm=TRUE)),                   sql("MEDIAN(`x`) OVER ()"))
  expect_equal(trans(min(x, na.rm=TRUE)),                      sql("MIN(`x`) OVER ()"))
  expect_equal(trans(quantile(x, 0.25, na.rm=TRUE)),           sql("PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY `x`) OVER ()"))
  expect_equal(trans(quantile(x, 0.25, type=1, na.rm=TRUE)),   sql("PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY `x`) OVER ()"))
  expect_equal(trans(sd(x, na.rm = TRUE)),         sql("STDDEV_SAMP(`x`) OVER ()"))
  expect_equal(trans(sum(x, na.rm = TcccRUE)),        sql("SUM(`x`) OVER ()"))
  expect_equal(trans(var(x, na.rm=TRUE)),          sql("VAR_SAMP(`x`) OVER ()"))
  expect_equal(trans(all(x, na.rm=TRUE)),          sql("BOOLAND_AGG(`x`) OVER ()"))
  expect_equal(trans(any(x, na.rm=TRUE)),          sql("BOOLOR_AGG(`x`) OVER ()"))
  expect_equal(trans(first(x)),          sql("FIRST_VALUE(`x`) OVER ()"))
  expect_equal(trans(last(x)),           sql("LAST_VALUE(`x`) OVER ()"))
  expect_equal(trans(row_number(x)),     sql("ROW_NUMBER() OVER (ORDER BY `x`)"))
  expect_equal(trans(lead(x)),           sql("LEAD(`x`, 1, NULL) OVER ()"))
  expect_equal(trans(lag(x)),            sql("LAG(`x`, 1, NULL) OVER ()"))
  expect_equal(trans(nth(x, 2)),         sql("NTH_VALUE(`x`, 2) OVER ()"))
  expect_equal(trans(cume_dist(x)),      sql("CUME_DIST() OVER (ORDER BY `x`)"))
  expect_equal(trans(dense_rank(x)),     sql("DENSE_RANK() OVER (ORDER BY `x`)"))
  expect_equal(trans(percent_rank(x)),   sql("PERCENT_RANK() OVER (ORDER BY `x`)"))
  expect_equal(trans(ntile(x, 5)),       sql("NTILE(5) OVER (ORDER BY `x`)"))
  expect_equal(trans(list(x)),           sql("ARRAY_AGG(`x`) OVER ()"))
})

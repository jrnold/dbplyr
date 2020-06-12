#' @include translate-sql-window.R
#' @include translate-sql-helpers.R
#' @include translate-sql-paste.R
#' @include escape.R
NULL

#' @export
db_explain.SnowflakeOdbcConnection <- function(con, sql, format = "tabular", ...) {
  format <- match.arg(format, c("text", "tabular", "json"))

  exsql <- build_sql(
    "EXPLAIN ",
    if (!is.null(format)) sql(paste0("USING ", format, " ")),
    sql,
    con = con
  )
  expl <- dbGetQuery(con, exsql)
  switch (format,
    "text" = paste(expl[[1]], collapse = "\n"),
    "json" = jsonlite::fromJSON(expl[[1]]),
    expl)
}

convert_pattern <-  function(x) {
  opts <- attr(x, "options")
  if (is(x, "boundary")) {
    if (opts$type == "word") {
      list(pattern = "\\b", parameters = "c")
    } else if (opts$type == "character") {
      list(pattern = "", parameters = "c")
    } else if (opts$type == "line_break") {
      list(pattern = "$", parameters = "cm")
    } else {
      sql_not_supported(stringr::str_c("boundary(type = ", opts$type, ")"))
    }
  } else if (is(x, "fixed") || is(x, "coll") || is(x, "regex")) {
    list(pattern = as.character(x),
         parameters = str_c(if (opts[["case_insensitive"]] %||% FALSE) "c" else "i",
                            if (opts[["dotall"]] %||% FALSE) "s" else "",
                            if (opts[["multiline"]] %||% FALSE) "m" else ""))
  }
}

#' @export
sql_translate_env.SnowflakeOdbcConnection <- function(con) {
  sql_variant(
    scalar = base_snowflake_scalar,
    aggregate = base_snowflake_agg,
    window = base_snowflake_win
  )
}

snowflake_round <- function(x, digits = 0L) {
  digits <- as.integer(digits)
  sql_expr(round((!!x), !!digits))
}

snowflake_str_pad <- function(string, width,  side = c("both", "left", "right"), pad = " ") {
  side <- match.arg(side)
  lwidth <- floor(width * 0.5)
  rwidth <- ceiling(width * 0.5)
  switch(side,
         left = sql_expr(LPAD(!!string, !!width, !!pad)),
         right = sql_expr(RPAD(!!string, !!width, !!pad)),
         both = sql_expr(LPAD(RPAD(!!string, !!rwidth, !!pad), !!lwidth,  !!pad)),
  )
}

snowflake_str_trim <- function (string, side = c("both", "left", "right"))
{
  side <- match.arg(side)
  switch(side,
         left = sql_expr(LTRIM(!!string)),
         right = sql_expr(RTRIM(!!string)),
         both = sql_expr(TRIM(!!string)), )
}

snowflake_grepl <- function(pattern, x, ignore.case = FALSE, perl = FALSE, fixed = FALSE, useBytes = FALSE) {
  # https://www.postgresql.org/docs/current/static/functions-matching.html#FUNCTIONS-POSIX-TABLE
  if (any(c(perl, fixed, useBytes))) {
    abort("`perl`, `fixed` and `useBytes` parameters are unsupported")
  }
  parameters <- if (ignore.case) { "i" } else { "c" }
  sql_expr(REGEXP_LIKE((!!x), (!!pattern), (!!parameters)))
}

snowflake_str_locate <- function (string, pattern) {
  patt <- convert_pattern(pattern)
  sql_expr(REGEXP_INSTR((!!x), !!patt$pattern, 1L, 1L, 0L, !!patt$parameters))
}

snowflake_str_match <- function (string, pattern) {
  patt <- convert_pattern(pattern)
  sql_expr(REGEXP_SUBSTR((!!x), !!patt$pattern, 1L, 1L, 0L, !!patt$parameters))
}

snowflake_str_replace <- function(all = FALSE) {
  function(string, pattern, replacement) {
    patt <- convert_pattern(pattern)
    occurence <- if (all) 0L else 1L
    sql_expr(REGEXP_REPLACE((!!x), (!!patt$pattern), (!!replacement), 1L, !!occurence, !!patt$parameters))
  }
}

snowflake_str_remove <- function(all = FALSE) {
  function(string, pattern) {
    patt <- convert_pattern(pattern)
    occurence <- if (all) 0L else 1L
    sql_expr(REGEXP_REPLACE((!!x), (!!patt$pattern), "", 1L, !!occurence, !!patt$parameters))
  }
}

#' @export
#' @rdname sql_variant
#' @format NULL
base_snowflake_scalar <- sql_translator(
  .parent = base_scalar,
  as.numeric = sql_cast("DOUBLE"),
  as.double = sql_cast("DOUBLE"),
  as.integer = sql_cast("NUMBER(10)"),
  as.character = sql_cast("VARCHAR"),
  as.POSIXct    = sql_cast("TIMESTAMP_NTZ"),
  as.integer64  = sql_cast("NUMBER(38, 0)"),

  # arithmetic
  cosh     = sql_prefix("COSH", 1),
  sinh     = sql_prefix("SINH", 1),
  tanh     = sql_prefix("TANH", 1),
  acosh    = sql_prefix("ACOSH", 1),
  asinh    = sql_prefix("ASINH", 1),
  atanh    = sql_prefix("ATANH", 1),
  cospi    = function(x) sql_expr(COS((!!x) * PI())),
  sinpi    = function(x) sql_expr(SIN((!!x) * PI())),
  tanpi    = function(x) sql_expr(TAN((!!x) * PI())),
  round    = snowflake_round,
  trunc    = sql_prefix("TRUNC", 1),

  # dates and times
  yday = function(x) sql_expr(EXTRACT(dayofyear %from% !!x)),
  wday = function(x) sql_expr(EXTRACT(dayofweekiso %from% !!x)),
  qday = function(x) sql_expr(
    EXTRACT(dayofyear %from% !!x) - EXTRACT(dayofyear %from% DATE_TRUNC("quarter", !!x)) + 1L
  ),

  # base string functions
  strtrim = sql_prefix("LEFT", 1),
  trimws = snowflake_str_trim,
  grepl = snowflake_grepl,
  strrep = sql_prefix("REPEAT", 2),

  # stringr str functions
  str_c = sql_paste(""),
  str_conv = sql_not_supported("str_conv()"),
  str_count = function(x, pattern) {
    patt <- convert_pattern(pattern)
    position <- 0L
    sql_expr(REGEXP_COUNT((!!x), !!patt$pattern, !!position, !!patt$parameters))
  },
  str_detect = function(x, pattern) {
    patt <- convert_pattern(pattern)
    position <- 0L
    sql_expr(REGEXP_LIKE((!!x), !!patt$pattern, !!patt$parameters))
  },
  str_dup = sql_prefix("REPEAT", 2),
  str_extract = sql_not_supported("str_extract()"),
  str_extract_all = sql_not_supported("str_extract_all()"), # returns  an array
  str_flatten = sql_not_supported("str_flatten()"),  # Add function
  str_glue = sql_not_supported("str_glue()"), # cannot be implemented
  str_glue_data = sql_not_supported("str_glue_data()"), # cannot be implemented
  str_interp = sql_not_supported("str_interp()"), # cannot be interpreted
  str_locate = snowflake_str_locate,
  str_locate_all = sql_not_supported("str_locate_all()"), # doesn't translate easily
  str_match = snowflake_str_match,
  str_match_all = sql_not_supported("str_match_all()"),
  str_order = sql_not_supported("str_order()"),
  str_pad = snowflake_str_pad,
  str_remove = snowflake_str_replace(FALSE),
  str_remove_all = snowflake_str_replace(TRUE),
  str_replace = snowflake_str_replace(FALSE),
  str_replace_all = snowflake_str_replace(TRUE),
  str_replace_na = function(string, replacement = "NA") {
    sql_expr(IFF((((!!string)) %is% NULL), !!replacement, !!string))
  },
  str_sort = sql_not_supported("str_sort()"), # not relevant
  str_split = function(string, pattern, sql_prefix, n = Inf, simplify = FALSE) {
    if (is.finite(n)) {
      sql_not_supported(str_c("str_split(..., n = ", n, ")"))
    }
    if (simplify) {
      sql_not_supported("str_split(..., simplify = TRUE)")
    }
    sql_expr(SPLIT((!!x), pattern))
  },
  str_split_fixed = sql_not_supported("str_split_fixed()"),
  str_squish = function(string) {
    sql_expr(TRIM(REGEXP_REPLACE(!!string), "\\s+", " "), " ")
  },
  str_subset =   function(string, pattern, negate = FALSE) {
    patt <- convert_pattern(pattern)
    position <- 0L
    if (negate) {
      sql_expr(IFF(REGEXP_LIKE((!!x), !!patt$pattern, !!patt$parameters),
                   (!!x), NULL))
    } else {
      sql_expr(IFF(REGEXP_LIKE((!!x), !!patt$pattern, !!patt$parameters),
                   NULL, (!!x)))
    }
  },
  str_trim = snowflake_str_trim,
  str_trunc = function(string, width, side = c("both", "left", "right"), ellipses="...") {
      side <- match.arg(side)
      switch(side,
             left = sql_expr(CONCAT(LEFT(!!string, !!width - LENGTH(!!ellipses)), ellipses)),
             right = sql_expr(CONCAT(!!ellipses, RIGHT(!!string, !!width - LENGTH(!!ellipses)))),
             both = sql_expr(CONCAT(LEFT(!!string, CEIL((!!width - LENGTH(!!ellipses)) / 2)), !!ellipses,
                                    RIGHT(!!string, FLOOR((!!width - LENGTH(!!ellipses)) / 2))))
      )
  },
  str_which = sql_not_supported("str_which()"), # not relevant
  str_wrap = sql_not_supported("str_wrap()"), # not easily implemented

  xor = sql_prefix("BOOLXOR", 2),

  # random number
  # the value of n is ignored
  rnorm = function(n, mean = 0, sd = 1) {
    if (n == 1) {
      seed <- Random.seed[[1]]
    } else {
      message("For n != 1, `n` is ignored.")
    }
    sql_expr(NORMAL((!!mean), (!!sd), RANDOM()))
  },
  # the value of n is ignored
  runif = function(n, min = 0, max = 1) {
    if (n == 1) {
      seed <- Random.seed[[1]]
      sql_expr(UNIFORM((!!min), (!!max), (!!seed)))
    } else {
      message("For n != 1, `n` is ignored.")
      sql_expr(UNIFORM((!!min), (!!max), RANDOM()))
    }
  }
)


snowflake_quantile <- function(window = FALSE) {
  warned <- FALSE
  force(window)
  function(x, probs = 0.5, na.rm = FALSE, type = 7, ...) {
    warned <<- check_na_rm("quantile", na.rm, warned)
    check_probs(probs)
    if (type == 1) {
      f <- "PERCENTILE_DISC"
    } else {
      f <- "PERCENTILE_CONT"
    }
    sql <- build_sql(sql_call2(f, probs), " WITHIN GROUP (ORDER BY ", x, ")")
    if (window) {
      sql <- win_over(sql, partition = win_current_group(),
                      frame = win_current_frame())
    }
    sql
  }
}


#' @export
#' @rdname sql_variant
#' @format NULL
base_snowflake_agg <- sql_translator(
   .parent = base_agg,
    n = function() sql("COUNT(*)"),
    cor = sql_aggregate_2("CORR"),
    cov = sql_aggregate_2("COVAR_SAMP"),
    sd =  sql_aggregate("STDDEV_SAMP", "sd"),
    var = sql_aggregate("VAR_SAMP", "var"),
    median = sql_aggregate("MEDIAN"),
    str_flatten = function(x, collapse = "") sql_expr(LISTAGG(!!x, !!collapse)),
    # boolean agg
    all = sql_aggregate("BOOLAND_AGG", "all"),
    any = sql_aggregate("BOOLOR_AGG", "any"),
    # bitwise agg - none
    # hash agg
    list = function(x) {
      build_sql(sql("ARRAY_AGG"), list(x))
    },
    quantile = snowflake_quantile(window=FALSE)
)

#' @export
#' @rdname sql_variant
#' @format NULL
base_snowflake_win <- sql_translator(
  .parent = base_win,
  n = function() {
    win_over(sql("COUNT(*)"), partition = win_current_group())
  },
  count = function() {
    win_over(sql("COUNT(*)"), partition = win_current_group())
  },
  cor = win_aggregate_2("CORR"),
  cov = win_aggregate_2("COVAR_SAMP"),
  sd =  win_aggregate("STDDEV_SAMP"),
  var = win_aggregate("VAR_SAMP"),
  median = win_aggregate("MEDIAN"),
  all = win_aggregate("BOOLAND_AGG"),
  any = win_aggregate("BOOLOR_AGG"),
  str_flatten = function(x, collapse = ' ', ...) {
    win_over(
      sql_expr(LISTAGG(!!x, !!collapse)),
      partition = win_current_group(),
      order = win_current_order()
    )
  },
  list = function(x) {
    frame <- win_current_frame()
    win_over(build_sql(sql("ARRAY_AGG"), list(x)),
             partition = win_current_group(),
             order = if (!is.null(frame)) win_current_order(), frame = frame)
  },
  quantile = snowflake_quantile(window=TRUE)
)

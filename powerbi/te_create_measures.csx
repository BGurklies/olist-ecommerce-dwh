// Tabular Editor 2 — Bulk-create all DAX measures for the Olist dashboard.
// Usage: open Power BI Desktop with the model, launch Tabular Editor via External Tools,
//        paste this script into the C# Script tab and press F5, then Ctrl+S.
//
// The multi-line DAX below is indented for readability in this file; Dedent() strips
// the shared leading indentation so the stored measure expressions stay clean.

Func<string, string> Dedent = (raw) => {
    var lines = raw.Replace("\r\n", "\n").Split('\n').ToList();
    while (lines.Count > 0 && lines[0].Trim().Length == 0) lines.RemoveAt(0);
    while (lines.Count > 0 && lines[lines.Count - 1].Trim().Length == 0) lines.RemoveAt(lines.Count - 1);
    if (lines.Count == 0) return "";
    int indent = lines.Where(l => l.Trim().Length > 0)
                      .Select(l => l.Length - l.TrimStart(' ').Length)
                      .Min();
    return string.Join("\n", lines.Select(l => l.Length >= indent ? l.Substring(indent) : l.TrimStart()));
};

foreach(var m in Model.Tables["_Measures"].Measures.ToList()) m.Delete();

var t = Model.Tables["_Measures"];

var defs = new[] {
    // ============================ Revenue & Orders ============================
    new {
        Name   = "Avg Order Value",
        Folder = "Revenue & Orders",
        Format = "#,0.0",
        Dax    = @"DIVIDE([Total Revenue], [Total Orders])"
    },
    new {
        Name   = "Total Orders",
        Folder = "Revenue & Orders",
        Format = "#,0",
        Dax    = @"DISTINCTCOUNT('mart fact_sales'[order_id])"
    },
    new {
        Name   = "Total Revenue",
        Folder = "Revenue & Orders",
        Format = "\"R$ \"#,##0.00",
        Dax    = @"SUM('mart fact_sales'[total_value])"
    },
    new {
        Name   = "Avg Order Value CM Label",
        Folder = "Revenue & Orders\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Order Value]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            VAR _fmt =
                SWITCH(
                    TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _cm_value >= 1000,    FORMAT(_cm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_cm_value, ""#,##0.0"", ""en-US"")
                )
            RETURN
                IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Avg Order Value Formatted",
        Folder = "Revenue & Orders\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _val = [Avg Order Value]
            RETURN
                SWITCH(
                    TRUE(),
                    _val >= 1000000, ""R$ "" & FORMAT(_val / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _val >= 1000,    ""R$ "" & FORMAT(_val / 1000, ""0.0"", ""en-US"") & ""K"",
                                     ""R$ "" & FORMAT(_val, ""#,##0.0"", ""en-US"")
                )
        "
    },
    new {
        Name   = "Avg Order Value MoM Badge",
        Folder = "Revenue & Orders\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Order Value]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Order Value]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm   = CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"", ""en-US"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Order Value PM Label",
        Folder = "Revenue & Orders\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Order Value]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Order Value]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _fmt =
                SWITCH(
                    TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _pm_value >= 1000,    FORMAT(_pm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_pm_value, ""#,##0.0"", ""en-US"")
                )
            RETURN
                IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Total Orders CM Label",
        Folder = "Revenue & Orders\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Orders]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            VAR _fmt =
                SWITCH(
                    TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _cm_value >= 1000,    FORMAT(_cm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_cm_value, ""#,##0"", ""en-US"")
                )
            RETURN
                IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & _fmt)
        "
    },
    new {
        Name   = "Total Orders Formatted",
        Folder = "Revenue & Orders\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _val = [Total Orders]
            RETURN
                SWITCH(
                    TRUE(),
                    _val >= 1000000, FORMAT(_val / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _val >= 1000,    FORMAT(_val / 1000, ""0.0"", ""en-US"") & ""K"",
                                     FORMAT(_val, ""#,##0"", ""en-US"")
                )
        "
    },
    new {
        Name   = "Total Orders MoM Badge",
        Folder = "Revenue & Orders\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Orders]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Orders]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm   = CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"", ""en-US"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Total Orders PM Label",
        Folder = "Revenue & Orders\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Orders]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Orders]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _fmt =
                SWITCH(
                    TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _pm_value >= 1000,    FORMAT(_pm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_pm_value, ""#,##0"", ""en-US"")
                )
            RETURN
                IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & _fmt)
        "
    },
    new {
        Name   = "Total Revenue CM Label",
        Folder = "Revenue & Orders\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Revenue]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE(
                    [Total Revenue],
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _fmt =
                SWITCH(
                    TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _cm_value >= 1000,    FORMAT(_cm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_cm_value, ""#,##0"", ""en-US"")
                )
            RETURN
                IF(
                    ISBLANK(_cm_value),
                    BLANK(),
                    _cm_label & "" · R$ "" & _fmt
                )
        "
    },
    new {
        Name   = "Total Revenue Formatted",
        Folder = "Revenue & Orders\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _val = [Total Revenue]
            RETURN
                SWITCH(
                    TRUE(),
                    _val >= 1000000, ""R$ "" & FORMAT(_val / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _val >= 1000,    ""R$ "" & FORMAT(_val / 1000, ""0.0"", ""en-US"") & ""K"",
                                     ""R$ "" & FORMAT(_val, ""#,##0.0"", ""en-US"")
                )
        "
    },
    new {
        Name   = "Total Revenue MoM Badge",
        Folder = "Revenue & Orders\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Revenue]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Revenue]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm      = CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm      = CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc    = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"", ""en-US"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Total Revenue PM Label",
        Folder = "Revenue & Orders\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Revenue]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Revenue]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE(
                    [Total Revenue],
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            VAR _fmt =
                SWITCH(
                    TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _pm_value >= 1000,    FORMAT(_pm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_pm_value, ""#,##0"", ""en-US"")
                )
            RETURN
                IF(
                    ISBLANK(_pm_value),
                    BLANK(),
                    _pm_label & "" · R$ "" & _fmt
                )
        "
    },
    new {
        Name   = "Avg Order Value Badge BG Color",
        Folder = "Revenue & Orders\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Order Value]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Order Value]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""#EAF8EC"", _cm - _pm < 0, ""#FFDCDC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Avg Order Value Badge Text Color",
        Folder = "Revenue & Orders\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Order Value]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Order Value]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },
    new {
        Name   = "Total Orders Badge BG Color",
        Folder = "Revenue & Orders\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Orders]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Orders]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""#EAF8EC"", _cm - _pm < 0, ""#FFDCDC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Total Orders Badge Text Color",
        Folder = "Revenue & Orders\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Orders]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Orders]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },
    new {
        Name   = "Total Revenue Badge BG Color",
        Folder = "Revenue & Orders\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Revenue]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Revenue]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm      = CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm      = CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""#EAF8EC"", _cm - _pm < 0, ""#FFDCDC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Total Revenue Badge Text Color",
        Folder = "Revenue & Orders\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
            MAXX(
                FILTER(
                    VALUES('mart dim_date'[year_month_key]),
                    CALCULATE([Total Revenue]) > 0
                ),
                'mart dim_date'[year_month_key]
            )
            VAR _pm_ym =
            MAXX(
                FILTER(
                    VALUES('mart dim_date'[year_month_key]),
                    CALCULATE([Total Revenue]) > 0
                        && 'mart dim_date'[year_month_key] < _last_ym
                ),
                'mart dim_date'[year_month_key]
            )
            VAR _cm      = CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm      = CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },
    new {
        Name   = "Axis Max Categories Revenue",
        Folder = "Revenue & Orders\\Titles & Axis",
        Format = "",
        Dax    = @"
            CALCULATE(
                MAXX(
                    VALUES('mart dim_product'[product_category_name_english]),
                    [Total Revenue]
                ),
                ALLSELECTED('mart dim_product'[product_category_name_english])
            ) * 1.2
        "
    },
    new {
        Name   = "Title Revenue by State",
        Folder = "Revenue & Orders\\Titles & Axis",
        Format = "",
        Dax    = @"
            VAR _state   = SELECTEDVALUE('mart dim_customer'[customer_state_name])
            VAR _state_n = COUNTROWS(VALUES('mart dim_customer'[customer_state_name]))
            VAR _all_n   = COUNTROWS(ALL('mart dim_customer'[customer_state_name]))
            RETURN
                SWITCH(TRUE(),
                    _state_n = 1,          _state & "": Revenue"",
                    _state_n < _all_n,     _state_n & "" States: Revenue"",
                                           ""Revenue by State""
                )
        "
    },
    new {
        Name   = "Title Revenue Trend",
        Folder = "Revenue & Orders\\Titles & Axis",
        Format = "",
        Dax    = @"
            VAR _min = MIN('mart dim_date'[year])
            VAR _max = MAX('mart dim_date'[year])
            VAR _count = DISTINCTCOUNT('mart dim_date'[year])
            RETURN
                ""Monthly Revenue Trend ("" &
                IF(_count = 1, _min, _min & ""–"" & _max) &
                "")""
        "
    },
    new {
        Name   = "Title Top Categories Revenue",
        Folder = "Revenue & Orders\\Titles & Axis",
        Format = "",
        Dax    = @"
            VAR _cat   = SELECTEDVALUE('mart dim_product'[product_category_name_english])
            VAR _cat_n = COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))
            RETURN
                SWITCH(TRUE(),
                    _cat_n = 1,  _cat & "": Revenue"",
                    _cat_n < 5,  _cat_n & "" Categories: Revenue"",
                                 ""Top 5 Product Categories by Revenue""
                )
        "
    },

    // ============================ Product & Freight ============================
    new {
        Name   = "Avg Freight Value",
        Folder = "Product & Freight",
        Format = "0.0",
        Dax    = @"AVERAGE('mart fact_sales'[freight_value])"
    },
    new {
        Name   = "Avg Item Price",
        Folder = "Product & Freight",
        Format = "\"R$ \"#,##0.0",
        Dax    = @"AVERAGE('mart fact_sales'[price])"
    },
    new {
        Name   = "Freight Share %",
        Folder = "Product & Freight",
        Format = "0.0%;-0.0%;0.0%",
        Dax    = @"DIVIDE(SUM('mart fact_sales'[freight_value]), SUM('mart fact_sales'[total_value]))"
    },
    new {
        Name   = "Total Items Sold",
        Folder = "Product & Freight",
        Format = "#,0",
        Dax    = @"COUNTROWS('mart fact_sales')"
    },
    new {
        Name   = "Avg Freight Value CM Label",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Freight Value]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value = CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            VAR _fmt =
                SWITCH(TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _cm_value >= 1000,    FORMAT(_cm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_cm_value, ""#,##0.0"", ""en-US"")
                )
            RETURN IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Avg Freight Value Formatted",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _val = [Avg Freight Value]
            RETURN
                SWITCH(
                    TRUE(),
                    _val >= 1000000, ""R$ "" & FORMAT(_val / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _val >= 1000,    ""R$ "" & FORMAT(_val / 1000, ""0.0"", ""en-US"") & ""K"",
                                     ""R$ "" & FORMAT(_val, ""#,##0.0"", ""en-US"")
                )
        "
    },
    new {
        Name   = "Avg Freight Value MoM Badge",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Freight Value]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Freight Value]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm   = CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"", ""en-US"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Freight Value PM Label",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Freight Value]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Freight Value]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value = CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _fmt =
                SWITCH(TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _pm_value >= 1000,    FORMAT(_pm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_pm_value, ""#,##0.0"", ""en-US"")
                )
            RETURN IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Avg Item Price CM Label",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Item Price]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value = CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            VAR _fmt =
                SWITCH(TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _cm_value >= 1000,    FORMAT(_cm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_cm_value, ""#,##0.0"", ""en-US"")
                )
            RETURN IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Avg Item Price Formatted",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _val = [Avg Item Price]
            RETURN
                SWITCH(
                    TRUE(),
                    _val >= 1000000, ""R$ "" & FORMAT(_val / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _val >= 1000,    ""R$ "" & FORMAT(_val / 1000, ""0.0"", ""en-US"") & ""K"",
                                     ""R$ "" & FORMAT(_val, ""#,##0.0"", ""en-US"")
                )
        "
    },
    new {
        Name   = "Avg Item Price MoM Badge",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Item Price]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Item Price]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm   = CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"", ""en-US"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Item Price PM Label",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Item Price]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Item Price]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value = CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _fmt =
                SWITCH(TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _pm_value >= 1000,    FORMAT(_pm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_pm_value, ""#,##0.0"", ""en-US"")
                )
            RETURN IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Freight Share % CM Label",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Freight Share %]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value = CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            RETURN IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & FORMAT(_cm_value, ""0.0%"", ""en-US""))
        "
    },
    new {
        Name   = "Freight Share % MoM Badge",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Freight Share %]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Freight Share %]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = ROUND(CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _last_ym), 3)
            VAR _pm   = ROUND(CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _pm_ym), 3)
            VAR _diff = _cm - _pm
            VAR _perc = FORMAT(ABS(_diff) * 100, ""0.0"", ""en-US"") & "" pp""
            RETURN
                IF(_diff > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Freight Share % PM Label",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Freight Share %]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Freight Share %]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value = CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & FORMAT(_pm_value, ""0.0%"", ""en-US""))
        "
    },
    new {
        Name   = "Total Items Sold CM Label",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Items Sold]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value = CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            VAR _fmt =
                SWITCH(TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _cm_value >= 1000,    FORMAT(_cm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_cm_value, ""#,##0"", ""en-US"")
                )
            RETURN IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & _fmt)
        "
    },
    new {
        Name   = "Total Items Sold Formatted",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _val = [Total Items Sold]
            RETURN
                SWITCH(
                    TRUE(),
                    _val >= 1000000, FORMAT(_val / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _val >= 1000,    FORMAT(_val / 1000, ""0.0"", ""en-US"") & ""K"",
                                     FORMAT(_val, ""#,##0"", ""en-US"")
                )
        "
    },
    new {
        Name   = "Total Items Sold MoM Badge",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Items Sold]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Items Sold]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm   = CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"", ""en-US"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Total Items Sold PM Label",
        Folder = "Product & Freight\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Items Sold]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Items Sold]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value = CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _fmt =
                SWITCH(TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _pm_value >= 1000,    FORMAT(_pm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_pm_value, ""#,##0"", ""en-US"")
                )
            RETURN IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & _fmt)
        "
    },
    new {
        Name   = "Avg Freight Value Badge BG Color",
        Folder = "Product & Freight\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Freight Value]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Freight Value]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""#FFDCDC"", _cm - _pm < 0, ""#EAF8EC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Avg Freight Value Badge Text Color",
        Folder = "Product & Freight\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Freight Value]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Freight Value]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""Red"", _cm - _pm < 0, ""Green"", ""Grey"")
        "
    },
    new {
        Name   = "Avg Item Price Badge BG Color",
        Folder = "Product & Freight\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Item Price]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Item Price]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""#EAF8EC"", _cm - _pm < 0, ""#FFDCDC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Avg Item Price Badge Text Color",
        Folder = "Product & Freight\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Item Price]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Item Price]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },
    new {
        Name   = "Freight Share % Badge BG Color",
        Folder = "Product & Freight\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Freight Share %]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Freight Share %]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""#FFDCDC"", _cm - _pm < 0, ""#EAF8EC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Freight Share % Badge Text Color",
        Folder = "Product & Freight\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Freight Share %]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Freight Share %]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""Red"", _cm - _pm < 0, ""Green"", ""Grey"")
        "
    },
    new {
        Name   = "Total Items Sold Badge BG Color",
        Folder = "Product & Freight\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Items Sold]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Items Sold]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""#EAF8EC"", _cm - _pm < 0, ""#FFDCDC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Total Items Sold Badge Text Color",
        Folder = "Product & Freight\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Items Sold]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Items Sold]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },
    new {
        Name   = "Axis Max Avg Item Price",
        Folder = "Product & Freight\\Titles & Axis",
        Format = "",
        Dax    = @"
            CALCULATE(
                MAXX(
                    VALUES('mart dim_product'[product_category_name_english]),
                    [Avg Item Price]
                ),
                ALLSELECTED('mart dim_product'[product_category_name_english])
            ) * 1.15
        "
    },
    new {
        Name   = "Axis Max Freight Share",
        Folder = "Product & Freight\\Titles & Axis",
        Format = "",
        Dax    = @"
            CALCULATE(
                MAXX(
                    VALUES('mart dim_product'[product_category_name_english]),
                    [Freight Share %]
                ),
                ALLSELECTED('mart dim_product'[product_category_name_english])
            ) * 1.18
        "
    },
    new {
        Name   = "Axis Max Items Sold",
        Folder = "Product & Freight\\Titles & Axis",
        Format = "",
        Dax    = @"
            CALCULATE(
                MAXX(
                    VALUES('mart dim_product'[product_category_name_english]),
                    [Total Items Sold]
                ),
                ALLSELECTED('mart dim_product'[product_category_name_english])
            ) * 1.15
        "
    },
    new {
        Name   = "Title Avg Item Price",
        Folder = "Product & Freight\\Titles & Axis",
        Format = "",
        Dax    = @"
            VAR _cat   = SELECTEDVALUE('mart dim_product'[product_category_name_english])
            VAR _cat_n = COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))
            RETURN
                SWITCH(TRUE(),
                    _cat_n = 1,  _cat & "": Avg Item Price"",
                    _cat_n < 5,  _cat_n & "" Categories: Avg Item Price"",
                                 ""Avg Item Price of Top Revenue Categories""
                )
        "
    },
    new {
        Name   = "Title Category Revenue Trend",
        Folder = "Product & Freight\\Titles & Axis",
        Format = "",
        Dax    = @"
            VAR _min   = MIN('mart dim_date'[year])
            VAR _max   = MAX('mart dim_date'[year])
            VAR _count = DISTINCTCOUNT('mart dim_date'[year])
            VAR _years = IF(_count = 1, _min, _min & ""–"" & _max)
            VAR _cat   = SELECTEDVALUE('mart dim_product'[product_category_name_english])
            VAR _cat_n = COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))
            RETURN
                SWITCH(TRUE(),
                    _cat_n = 1,  _cat & "": Monthly Revenue Trend ("" & _years & "")"",
                    _cat_n < 5,  _cat_n & "" Categories: Monthly Revenue Trend ("" & _years & "")"",
                                 ""Top 5 Categories: Monthly Revenue Trend ("" & _years & "")""
                )
        "
    },
    new {
        Name   = "Title Freight Share",
        Folder = "Product & Freight\\Titles & Axis",
        Format = "",
        Dax    = @"
            VAR _cat   = SELECTEDVALUE('mart dim_product'[product_category_name_english])
            VAR _cat_n = COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))
            RETURN
                SWITCH(TRUE(),
                    _cat_n = 1,  _cat & "": Freight Share"",
                    _cat_n < 5,  _cat_n & "" Categories: Freight Share"",
                                 ""Top 5 Categories by Freight Share""
                )
        "
    },
    new {
        Name   = "Title Items Sold",
        Folder = "Product & Freight\\Titles & Axis",
        Format = "",
        Dax    = @"
            VAR _cat   = SELECTEDVALUE('mart dim_product'[product_category_name_english])
            VAR _cat_n = COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))
            RETURN
                SWITCH(TRUE(),
                    _cat_n = 1,  _cat & "": Items Sold"",
                    _cat_n < 5,  _cat_n & "" Categories: Items Sold"",
                                 ""Top 5 Categories by Items Sold""
                )
        "
    },

    // ============================ Delivery & Operations ============================
    new {
        Name   = "Avg Approval Hours",
        Folder = "Delivery & Operations",
        Format = "0.0",
        Dax    = @"
            AVERAGEX(
                FILTER('mart fact_sales', NOT ISBLANK('mart fact_sales'[purchase_to_approval_hours])),
                'mart fact_sales'[purchase_to_approval_hours]
            )
        "
    },
    new {
        Name   = "Avg Delivery Days",
        Folder = "Delivery & Operations",
        Format = "0.0",
        Dax    = @"
            AVERAGEX(
                FILTER('mart fact_sales', NOT ISBLANK('mart fact_sales'[purchase_to_delivery_days])),
                'mart fact_sales'[purchase_to_delivery_days]
            )
        "
    },
    new {
        Name   = "Avg Last Mile Days",
        Folder = "Delivery & Operations",
        Format = "0.0",
        Dax    = @"
            AVERAGEX(
                FILTER('mart fact_sales', NOT ISBLANK('mart fact_sales'[carrier_to_delivery_days])),
                'mart fact_sales'[carrier_to_delivery_days]
            )
        "
    },
    new {
        Name   = "Avg SLA Variance Days",
        Folder = "Delivery & Operations",
        Format = "+#,##0.0;-#,##0.0;0.0",
        Dax    = @"
            AVERAGEX(
                FILTER('mart fact_sales', NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days])),
                'mart fact_sales'[delivery_vs_estimate_days]
            )
        "
    },
    new {
        Name   = "Avg SLA Variance Display",
        Folder = "Delivery & Operations",
        Format = "",
        Dax    = @"
            VAR _val = [Avg SLA Variance Days]
            RETURN
                IF(_val < 0,
                    FORMAT(_val, ""0.0"", ""en-US"") & "" "" & UNICHAR(9650),
                    FORMAT(_val, ""0.0"", ""en-US"") & "" "" & UNICHAR(9660)
                )
        "
    },
    new {
        Name   = "On-Time Delivery Rate",
        Folder = "Delivery & Operations",
        Format = "0.0%;-0.0%;0.0%",
        Dax    = @"DIVIDE(COUNTROWS(FILTER('mart fact_sales', NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]) && 'mart fact_sales'[delivery_vs_estimate_days] <= 0)), COUNTROWS(FILTER('mart fact_sales', NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]))))"
    },
    new {
        Name   = "Order Status Share",
        Folder = "Delivery & Operations",
        Format = "0.0%;-0.0%;0.0%",
        Dax    = @"
            DIVIDE(
                [Total Orders],
                CALCULATE([Total Orders], ALL('mart dim_order_status'))
            )
        "
    },
    new {
        Name   = "Orders On-Time (0–7d early)",
        Folder = "Delivery & Operations",
        Format = "#,0",
        Dax    = @"
            CALCULATE(
                DISTINCTCOUNT('mart fact_sales'[order_id]),
                FILTER('mart fact_sales',
                    NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]) &&
                    'mart fact_sales'[delivery_vs_estimate_days] >= -7 &&
                    'mart fact_sales'[delivery_vs_estimate_days] <= 0
                )
            )
        "
    },
    new {
        Name   = "Orders Slightly Late (1–7d)",
        Folder = "Delivery & Operations",
        Format = "#,0",
        Dax    = @"
            CALCULATE(
                DISTINCTCOUNT('mart fact_sales'[order_id]),
                FILTER('mart fact_sales',
                    NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]) &&
                    'mart fact_sales'[delivery_vs_estimate_days] > 0 &&
                    'mart fact_sales'[delivery_vs_estimate_days] <= 7
                )
            )
        "
    },
    new {
        Name   = "Orders Very Early (>7d early)",
        Folder = "Delivery & Operations",
        Format = "#,0",
        Dax    = @"
            CALCULATE(
                DISTINCTCOUNT('mart fact_sales'[order_id]),
                FILTER('mart fact_sales',
                    NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]) &&
                    'mart fact_sales'[delivery_vs_estimate_days] < -7
                )
            )
        "
    },
    new {
        Name   = "Orders Very Late (>7d)",
        Folder = "Delivery & Operations",
        Format = "#,0",
        Dax    = @"
            CALCULATE(
                DISTINCTCOUNT('mart fact_sales'[order_id]),
                FILTER('mart fact_sales',
                    NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]) &&
                    'mart fact_sales'[delivery_vs_estimate_days] > 7
                )
            )
        "
    },
    new {
        Name   = "Avg Approval Hours CM Label",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Approval Hours]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value = CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            RETURN IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & FORMAT(_cm_value, ""0.0"", ""en-US""))
        "
    },
    new {
        Name   = "Avg Approval Hours MoM Badge",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Approval Hours]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Approval Hours]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm   = CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"", ""en-US"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Approval Hours PM Label",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Approval Hours]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Approval Hours]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value = CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & FORMAT(_pm_value, ""0.0"", ""en-US""))
        "
    },
    new {
        Name   = "Avg Delivery Days CM Label",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Delivery Days]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value = CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            RETURN IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & FORMAT(_cm_value, ""0.0"", ""en-US""))
        "
    },
    new {
        Name   = "Avg Delivery Days MoM Badge",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Delivery Days]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Delivery Days]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm   = CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"", ""en-US"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Delivery Days PM Label",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Delivery Days]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Delivery Days]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value = CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & FORMAT(_pm_value, ""0.0"", ""en-US""))
        "
    },
    new {
        Name   = "Avg Last Mile Days CM Label",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Last Mile Days]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value = CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            RETURN IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & FORMAT(_cm_value, ""0.0"", ""en-US""))
        "
    },
    new {
        Name   = "Avg Last Mile Days MoM Badge",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Last Mile Days]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Last Mile Days]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm   = CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"", ""en-US"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Last Mile Days PM Label",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Last Mile Days]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Last Mile Days]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value = CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & FORMAT(_pm_value, ""0.0"", ""en-US""))
        "
    },
    new {
        Name   = "Avg SLA Variance Days CM Label",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value = CALCULATE([Avg SLA Variance Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            RETURN IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & FORMAT(_cm_value, ""0.0"", ""en-US""))
        "
    },
    new {
        Name   = "Avg SLA Variance Days MoM Badge",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = CALCULATE([Avg SLA Variance Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm   = CALCULATE([Avg SLA Variance Days], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _diff = _cm - _pm
            VAR _perc = FORMAT(DIVIDE(_diff, ABS(_pm)), ""0.0%;0.0%"", ""en-US"")
            RETURN
                IF(_diff > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg SLA Variance Days PM Label",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value = CALCULATE([Avg SLA Variance Days], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & FORMAT(_pm_value, ""0.0"", ""en-US""))
        "
    },
    new {
        Name   = "On-Time Delivery Rate CM Label",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([On-Time Delivery Rate]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE([On-Time Delivery Rate], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            RETURN
                IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & FORMAT(_cm_value, ""0.0%"", ""en-US""))
        "
    },
    new {
        Name   = "On-Time Delivery Rate MoM Badge",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([On-Time Delivery Rate]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([On-Time Delivery Rate]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = ROUND(CALCULATE([On-Time Delivery Rate], 'mart dim_date'[year_month_key] = _last_ym), 3)
            VAR _pm   = ROUND(CALCULATE([On-Time Delivery Rate], 'mart dim_date'[year_month_key] = _pm_ym), 3)
            VAR _diff = _cm - _pm
            VAR _perc = FORMAT(ABS(_diff) * 100, ""0.0"", ""en-US"") & "" pp""
            RETURN
                IF(_diff > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "On-Time Delivery Rate PM Label",
        Folder = "Delivery & Operations\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([On-Time Delivery Rate]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([On-Time Delivery Rate]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([On-Time Delivery Rate], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & FORMAT(_pm_value, ""0.0%"", ""en-US""))
        "
    },
    new {
        Name   = "Avg Approval Hours Badge BG Color",
        Folder = "Delivery & Operations\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Approval Hours]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Approval Hours]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""#FFDCDC"", _cm - _pm < 0, ""#EAF8EC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Avg Approval Hours Badge Text Color",
        Folder = "Delivery & Operations\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Approval Hours]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Approval Hours]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""Red"", _cm - _pm < 0, ""Green"", ""Grey"")
        "
    },
    new {
        Name   = "Avg Delivery Days Badge BG Color",
        Folder = "Delivery & Operations\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Delivery Days]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Delivery Days]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""#FFDCDC"", _cm - _pm < 0, ""#EAF8EC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Avg Delivery Days Badge Text Color",
        Folder = "Delivery & Operations\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Delivery Days]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Delivery Days]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""Red"", _cm - _pm < 0, ""Green"", ""Grey"")
        "
    },
    new {
        Name   = "Avg Last Mile Days Badge BG Color",
        Folder = "Delivery & Operations\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Last Mile Days]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Last Mile Days]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""#FFDCDC"", _cm - _pm < 0, ""#EAF8EC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Avg Last Mile Days Badge Text Color",
        Folder = "Delivery & Operations\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Last Mile Days]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Last Mile Days]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""Red"", _cm - _pm < 0, ""Green"", ""Grey"")
        "
    },
    new {
        Name   = "Avg SLA Variance Days Badge BG Color",
        Folder = "Delivery & Operations\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg SLA Variance Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg SLA Variance Days], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""#FFDCDC"", _cm - _pm < 0, ""#EAF8EC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Avg SLA Variance Days Badge Text Color",
        Folder = "Delivery & Operations\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg SLA Variance Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg SLA Variance Days], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""Red"", _cm - _pm < 0, ""Green"", ""Grey"")
        "
    },
    new {
        Name   = "On-Time Delivery Rate Badge BG Color",
        Folder = "Delivery & Operations\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([On-Time Delivery Rate]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([On-Time Delivery Rate]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([On-Time Delivery Rate], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([On-Time Delivery Rate], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""#EAF8EC"", _cm - _pm < 0, ""#FFDCDC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "On-Time Delivery Rate Badge Text Color",
        Folder = "Delivery & Operations\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([On-Time Delivery Rate]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([On-Time Delivery Rate]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([On-Time Delivery Rate], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([On-Time Delivery Rate], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },
    new {
        Name   = "Axis Max Avg Delivery Days",
        Folder = "Delivery & Operations\\Titles & Axis",
        Format = "",
        Dax    = @"
            CALCULATE(
                MAXX(VALUES('mart dim_customer'[customer_state_name]), [Avg Delivery Days]),
                ALLSELECTED('mart dim_customer'[customer_state_name])
            ) * 1.15
        "
    },
    new {
        Name   = "Axis Max Orders by Status",
        Folder = "Delivery & Operations\\Titles & Axis",
        Format = "",
        Dax    = @"
            CALCULATE(
                MAXX(VALUES('mart dim_order_status'[status_name]), [Total Orders]),
                ALLSELECTED('mart dim_order_status'[status_name])
            ) * 1.15
        "
    },
    new {
        Name   = "Title Delivery Performance by State",
        Folder = "Delivery & Operations\\Titles & Axis",
        Format = "",
        Dax    = @"
            VAR _state   = SELECTEDVALUE('mart dim_customer'[customer_state_name])
            VAR _state_n = COUNTROWS(VALUES('mart dim_customer'[customer_state_name]))
            RETURN
                SWITCH(TRUE(),
                    _state_n = 1,  _state & "": Delivery Performance"",
                    _state_n < 5,  _state_n & "" States: Delivery Performance"",
                                   ""Delivery Performance: Top 5 States by Volume""
                )
        "
    },
    new {
        Name   = "Title Delivery Trend",
        Folder = "Delivery & Operations\\Titles & Axis",
        Format = "",
        Dax    = @"
            VAR _min   = MIN('mart dim_date'[year])
            VAR _max   = MAX('mart dim_date'[year])
            VAR _count = DISTINCTCOUNT('mart dim_date'[year])
            RETURN
                ""Monthly On-Time Delivery Rate ("" &
                IF(_count = 1, _min, _min & ""–"" & _max) &
                "")""
        "
    },

    // ============================ Customer & Payments ============================
    new {
        Name   = "Avg Credit Card Installments",
        Folder = "Customer & Payments",
        Format = "0.0",
        Dax    = @"
            AVERAGEX(
                FILTER('mart fact_payments',
                    'mart fact_payments'[payment_type_key] = 1
                ),
                'mart fact_payments'[payment_installments]
            )
        "
    },
    new {
        Name   = "Avg Review Score",
        Folder = "Customer & Payments",
        Format = "0.0",
        Dax    = @"
            AVERAGEX(
                FILTER(
                    ADDCOLUMNS(VALUES('mart fact_sales'[order_id]),
                               ""@score"", CALCULATE(MAX('mart fact_sales'[review_score]))),
                    NOT ISBLANK([@score])),
                [@score])
        "
    },
    new {
        Name   = "Credit Card Share %",
        Folder = "Customer & Payments",
        Format = "0.0%;-0.0%;0.0%",
        Dax    = @"
            DIVIDE(
                CALCULATE(
                    SUM('mart fact_payments'[payment_value]),
                    'mart dim_payment_type'[payment_type_name] = ""Credit Card""
                ),
                SUM('mart fact_payments'[payment_value])
            )
        "
    },
    new {
        Name   = "Review Submission Rate",
        Folder = "Customer & Payments",
        Format = "0.0%;-0.0%;0.0%",
        Dax    = @"
            DIVIDE(
                CALCULATE(
                    DISTINCTCOUNT('mart fact_sales'[order_id]),
                    NOT ISBLANK('mart fact_sales'[review_score])
                ),
                [Total Orders]
            )
        "
    },
    new {
        Name   = "Total Transaction Value",
        Folder = "Customer & Payments",
        Format = "\"R$ \"#,##0.00",
        Dax    = @"SUM('mart fact_payments'[payment_value])"
    },
    new {
        Name   = "Unique Customers",
        Folder = "Customer & Payments",
        Format = "#,0",
        Dax    = @"
            CALCULATE(
                DISTINCTCOUNT('mart dim_customer'[customer_unique_id]),
                'mart fact_sales'
            )
        "
    },
    new {
        Name   = "Avg Credit Card Installments CM Label",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Credit Card Installments]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value = CALCULATE([Avg Credit Card Installments], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            RETURN IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & FORMAT(_cm_value, ""0.00"", ""en-US""))
        "
    },
    new {
        Name   = "Avg Credit Card Installments MoM Badge",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Credit Card Installments]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Credit Card Installments]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = CALCULATE([Avg Credit Card Installments], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm   = CALCULATE([Avg Credit Card Installments], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"", ""en-US"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Credit Card Installments PM Label",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Credit Card Installments]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Credit Card Installments]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value = CALCULATE([Avg Credit Card Installments], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & FORMAT(_pm_value, ""0.00"", ""en-US""))
        "
    },
    new {
        Name   = "Avg Review Score CM Label",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg Review Score]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            RETURN
                IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & FORMAT(_cm_value, ""0.00"", ""en-US""))
        "
    },
    new {
        Name   = "Avg Review Score MoM Badge",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg Review Score]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg Review Score]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm   = CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _diff = _cm - _pm
            VAR _perc = FORMAT(ABS(_diff), ""0.00"", ""en-US"") & "" pt""
            RETURN
                IF(_diff > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Review Score PM Label",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg Review Score]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg Review Score]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & FORMAT(_pm_value, ""0.00"", ""en-US""))
        "
    },
    new {
        Name   = "Credit Card Share % CM Label",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Credit Card Share %]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value = CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            RETURN IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & FORMAT(_cm_value, ""0.0%"", ""en-US""))
        "
    },
    new {
        Name   = "Credit Card Share % MoM Badge",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Credit Card Share %]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Credit Card Share %]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = ROUND(CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _last_ym), 3)
            VAR _pm   = ROUND(CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _pm_ym), 3)
            VAR _diff = _cm - _pm
            VAR _perc = FORMAT(ABS(_diff) * 100, ""0.0"", ""en-US"") & "" pp""
            RETURN
                IF(_diff > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Credit Card Share % PM Label",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Credit Card Share %]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Credit Card Share %]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value = CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & FORMAT(_pm_value, ""0.0%"", ""en-US""))
        "
    },
    new {
        Name   = "Review Submission Rate CM Label",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Review Submission Rate]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value = CALCULATE([Review Submission Rate], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            RETURN IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & FORMAT(_cm_value, ""0.0%"", ""en-US""))
        "
    },
    new {
        Name   = "Review Submission Rate MoM Badge",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Review Submission Rate]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Review Submission Rate]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = ROUND(CALCULATE([Review Submission Rate], 'mart dim_date'[year_month_key] = _last_ym), 3)
            VAR _pm   = ROUND(CALCULATE([Review Submission Rate], 'mart dim_date'[year_month_key] = _pm_ym), 3)
            VAR _diff = _cm - _pm
            VAR _perc = FORMAT(ABS(_diff) * 100, ""0.0"", ""en-US"") & "" pp""
            RETURN
                IF(_diff > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Review Submission Rate PM Label",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Review Submission Rate]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Review Submission Rate]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value = CALCULATE([Review Submission Rate], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & FORMAT(_pm_value, ""0.0%"", ""en-US""))
        "
    },
    new {
        Name   = "Unique Customers CM Label",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Unique Customers]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value = CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _last_ym)
            VAR _fmt =
                SWITCH(TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _cm_value >= 1000,    FORMAT(_cm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_cm_value, ""#,##0"", ""en-US"")
                )
            RETURN IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & _fmt)
        "
    },
    new {
        Name   = "Unique Customers Formatted",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _val = [Unique Customers]
            RETURN
                SWITCH(
                    TRUE(),
                    _val >= 1000000, FORMAT(_val / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _val >= 1000,    FORMAT(_val / 1000, ""0.0"", ""en-US"") & ""K"",
                                     FORMAT(_val, ""#,##0"", ""en-US"")
                )
        "
    },
    new {
        Name   = "Unique Customers MoM Badge",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Unique Customers]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Unique Customers]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm   = CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm   = CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"", ""en-US"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Unique Customers PM Label",
        Folder = "Customer & Payments\\Labels & Badges",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Unique Customers]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Unique Customers]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value = CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label = CALCULATE(MAX('mart dim_date'[month_year_short]), 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _fmt =
                SWITCH(TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"", ""en-US"") & ""M"",
                    _pm_value >= 1000,    FORMAT(_pm_value / 1000, ""0.0"", ""en-US"") & ""K"",
                                          FORMAT(_pm_value, ""#,##0"", ""en-US"")
                )
            RETURN IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & _fmt)
        "
    },
    new {
        Name   = "Avg Review Score Badge BG Color",
        Folder = "Customer & Payments\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg Review Score]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg Review Score]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""#EAF8EC"", _cm - _pm < 0, ""#FFDCDC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Avg Review Score Badge Text Color",
        Folder = "Customer & Payments\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg Review Score]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg Review Score]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },
    new {
        Name   = "Credit Card Share % Badge BG Color",
        Folder = "Customer & Payments\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Credit Card Share %]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Credit Card Share %]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""#EAF8EC"", _cm - _pm < 0, ""#FFDCDC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Credit Card Share % Badge Text Color",
        Folder = "Customer & Payments\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Credit Card Share %]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Credit Card Share %]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },
    new {
        Name   = "Review Submission Rate Badge BG Color",
        Folder = "Customer & Payments\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Review Submission Rate]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Review Submission Rate]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Review Submission Rate], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Review Submission Rate], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""#EAF8EC"", _cm - _pm < 0, ""#FFDCDC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Review Submission Rate Badge Text Color",
        Folder = "Customer & Payments\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Review Submission Rate]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Review Submission Rate]))
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Review Submission Rate], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Review Submission Rate], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },
    new {
        Name   = "Unique Customers Badge BG Color",
        Folder = "Customer & Payments\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Unique Customers]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Unique Customers]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""#EAF8EC"", _cm - _pm < 0, ""#FFDCDC"", ""#F2F2F2"")
        "
    },
    new {
        Name   = "Unique Customers Badge Text Color",
        Folder = "Customer & Payments\\Colors",
        Format = "",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Unique Customers]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Unique Customers]) > 0
                            && 'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },
    new {
        Name   = "Axis Max Payment Type",
        Folder = "Customer & Payments\\Titles & Axis",
        Format = "",
        Dax    = @"
            CALCULATE(
                MAXX(VALUES('mart dim_payment_type'[payment_type_name]), [Total Transaction Value]),
                ALLSELECTED('mart dim_payment_type'[payment_type_name])
            ) * 1.175
        "
    },
    new {
        Name   = "Axis Max States by Customers",
        Folder = "Customer & Payments\\Titles & Axis",
        Format = "",
        Dax    = @"
            CALCULATE(
                MAXX(VALUES('mart dim_customer'[customer_state_name]), [Unique Customers]),
                ALLSELECTED('mart dim_customer'[customer_state_name])
            ) * 1.15
        "
    },
    new {
        Name   = "Title Review Score Trend",
        Folder = "Customer & Payments\\Titles & Axis",
        Format = "",
        Dax    = @"
            VAR _min   = MIN('mart dim_date'[year])
            VAR _max   = MAX('mart dim_date'[year])
            VAR _count = DISTINCTCOUNT('mart dim_date'[year])
            RETURN
                ""Monthly Review Score and Submission Rate ("" &
                IF(_count = 1, _min, _min & ""–"" & _max) &
                "")""
        "
    },
    new {
        Name   = "Title States by Customers",
        Folder = "Customer & Payments\\Titles & Axis",
        Format = "",
        Dax    = @"
            VAR _state   = SELECTEDVALUE('mart dim_customer'[customer_state_name])
            VAR _state_n = COUNTROWS(VALUES('mart dim_customer'[customer_state_name]))
            RETURN
                SWITCH(TRUE(),
                    _state_n = 1,  _state & "": Unique Customers"",
                    _state_n < 5,  _state_n & "" States: Unique Customers"",
                                   ""Top 5 States by Unique Customers""
                )
        "
    },

    // ============================ Display ============================
    new {
        Name   = "Last Updated",
        Folder = "Display",
        Format = "",
        Dax    = @"
            VAR _date = MAX('mart fact_sales'[mart_load_ts])
            VAR _month = MONTH(_date)
            VAR _month_name = SWITCH(_month,
                1, ""Jan"", 2, ""Feb"", 3, ""Mar"", 4, ""Apr"",
                5, ""May"", 6, ""Jun"", 7, ""Jul"", 8, ""Aug"",
                9, ""Sep"", 10, ""Oct"", 11, ""Nov"", 12, ""Dec""
            )
            RETURN
                ""Last updated: "" & FORMAT(_date, ""DD"", ""en-US"") & "" "" & _month_name & "" "" & FORMAT(_date, ""YYYY"", ""en-US"")
        "
    },
};

foreach(var d in defs) {
    var m = t.AddMeasure(d.Name, Dedent(d.Dax), d.Folder);
    if (d.Format.Length > 0) m.FormatString = d.Format;
}

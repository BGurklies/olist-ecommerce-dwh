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

    // ── Revenue — Base ────────────────────────────────────────────────────────
    new {
        Name   = "Total Revenue",
        Folder = "Revenue",
        Dax    = @"SUM('mart fact_sales'[total_value])"
    },
    new {
        Name   = "Total Orders",
        Folder = "Revenue",
        Dax    = @"DISTINCTCOUNT('mart fact_sales'[order_id])"
    },
    new {
        Name   = "Avg Order Value",
        Folder = "Revenue",
        Dax    = @"DIVIDE([Total Revenue], [Total Orders])"
    },
    new {
        Name   = "Total Items Sold",
        Folder = "Revenue",
        Dax    = @"COUNTROWS('mart fact_sales')"
    },
    new {
        Name   = "Avg Item Price",
        Folder = "Revenue",
        Dax    = @"AVERAGE('mart fact_sales'[price])"
    },
    new {
        Name   = "Avg Freight Value",
        Folder = "Revenue",
        Dax    = @"AVERAGE('mart fact_sales'[freight_value])"
    },
    new {
        Name   = "Freight Share %",
        Folder = "Revenue",
        Dax    = @"
            DIVIDE(
                SUM('mart fact_sales'[freight_value]),
                SUM('mart fact_sales'[total_value])
            )
        "
    },

    // ── Revenue — Formatted ───────────────────────────────────────────────────
    new {
        Name   = "Total Revenue Formatted",
        Folder = "Revenue",
        Dax    = @"
            VAR _val = [Total Revenue]
            RETURN
                SWITCH(TRUE(),
                    _val >= 1000000, ""R$ "" & FORMAT(_val / 1000000, ""0.0"") & ""M"",
                    _val >= 1000, ""R$ "" & FORMAT(_val / 1000, ""0.0"") & ""K"",
                    ""R$ "" & FORMAT(_val, ""#,##0.0"")
                )
        "
    },
    new {
        Name   = "Total Orders Formatted",
        Folder = "Revenue",
        Dax    = @"
            VAR _val = [Total Orders]
            RETURN
                SWITCH(TRUE(),
                    _val >= 1000000, FORMAT(_val / 1000000, ""0.0"") & ""M"",
                    _val >= 1000, FORMAT(_val / 1000, ""0.0"") & ""K"",
                    FORMAT(_val, ""#,##0"")
                )
        "
    },
    new {
        Name   = "Avg Order Value Formatted",
        Folder = "Revenue",
        Dax    = @"
            VAR _val = [Avg Order Value]
            RETURN
                SWITCH(TRUE(),
                    _val >= 1000000, ""R$ "" & FORMAT(_val / 1000000, ""0.0"") & ""M"",
                    _val >= 1000, ""R$ "" & FORMAT(_val / 1000, ""0.0"") & ""K"",
                    ""R$ "" & FORMAT(_val, ""#,##0.0"")
                )
        "
    },
    new {
        Name   = "Avg Item Price Formatted",
        Folder = "Revenue",
        Dax    = @"
            VAR _val = [Avg Item Price]
            RETURN
                SWITCH(TRUE(),
                    _val >= 1000000, ""R$ "" & FORMAT(_val / 1000000, ""0.0"") & ""M"",
                    _val >= 1000, ""R$ "" & FORMAT(_val / 1000, ""0.0"") & ""K"",
                    ""R$ "" & FORMAT(_val, ""#,##0.0"")
                )
        "
    },
    new {
        Name   = "Avg Freight Value Formatted",
        Folder = "Revenue",
        Dax    = @"
            VAR _val = [Avg Freight Value]
            RETURN
                SWITCH(TRUE(),
                    _val >= 1000000, ""R$ "" & FORMAT(_val / 1000000, ""0.0"") & ""M"",
                    _val >= 1000, ""R$ "" & FORMAT(_val / 1000, ""0.0"") & ""K"",
                    ""R$ "" & FORMAT(_val, ""#,##0.0"")
                )
        "
    },
    new {
        Name   = "Total Items Sold Formatted",
        Folder = "Revenue",
        Dax    = @"
            VAR _val = [Total Items Sold]
            RETURN
                SWITCH(TRUE(),
                    _val >= 1000000, FORMAT(_val / 1000000, ""0.0"") & ""M"",
                    _val >= 1000, FORMAT(_val / 1000, ""0.0"") & ""K"",
                    FORMAT(_val, ""#,##0"")
                )
        "
    },

    // ── Revenue — Total Revenue KPI ───────────────────────────────────────────
    new {
        Name   = "Total Revenue CM Label",
        Folder = "Revenue",
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
                CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"") & ""M"",
                    _cm_value >= 1000, FORMAT(_cm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_cm_value, ""#,##0"")
                )
            RETURN
                IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Total Revenue PM Label",
        Folder = "Revenue",
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
                        CALCULATE([Total Revenue]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"") & ""M"",
                    _pm_value >= 1000, FORMAT(_pm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_pm_value, ""#,##0"")
                )
            RETURN
                IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Total Revenue MoM Badge",
        Folder = "Revenue",
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
                        CALCULATE([Total Revenue]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Total Revenue Badge BG Color",
        Folder = "Revenue",
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
                        CALCULATE([Total Revenue]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#EAF8EC"",
                    _cm - _pm < 0, ""#FFDCDC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Total Revenue Badge Text Color",
        Folder = "Revenue",
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
                        CALCULATE([Total Revenue]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Total Revenue], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },

    // ── Revenue — Total Orders KPI ────────────────────────────────────────────
    new {
        Name   = "Total Orders CM Label",
        Folder = "Revenue",
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
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"") & ""M"",
                    _cm_value >= 1000, FORMAT(_cm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_cm_value, ""#,##0"")
                )
            RETURN
                IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & _fmt)
        "
    },
    new {
        Name   = "Total Orders PM Label",
        Folder = "Revenue",
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
                        CALCULATE([Total Orders]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"") & ""M"",
                    _pm_value >= 1000, FORMAT(_pm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_pm_value, ""#,##0"")
                )
            RETURN
                IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & _fmt)
        "
    },
    new {
        Name   = "Total Orders MoM Badge",
        Folder = "Revenue",
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
                        CALCULATE([Total Orders]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Total Orders Badge BG Color",
        Folder = "Revenue",
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
                        CALCULATE([Total Orders]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#EAF8EC"",
                    _cm - _pm < 0, ""#FFDCDC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Total Orders Badge Text Color",
        Folder = "Revenue",
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
                        CALCULATE([Total Orders]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm = CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Total Orders], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },

    // ── Revenue — Avg Order Value KPI ─────────────────────────────────────────
    new {
        Name   = "Avg Order Value CM Label",
        Folder = "Revenue",
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
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"") & ""M"",
                    _cm_value >= 1000, FORMAT(_cm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_cm_value, ""#,##0.0"")
                )
            RETURN
                IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Avg Order Value PM Label",
        Folder = "Revenue",
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
                        CALCULATE([Avg Order Value]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"") & ""M"",
                    _pm_value >= 1000, FORMAT(_pm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_pm_value, ""#,##0.0"")
                )
            RETURN
                IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Avg Order Value MoM Badge",
        Folder = "Revenue",
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
                        CALCULATE([Avg Order Value]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Order Value Badge BG Color",
        Folder = "Revenue",
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
                        CALCULATE([Avg Order Value]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#EAF8EC"",
                    _cm - _pm < 0, ""#FFDCDC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Avg Order Value Badge Text Color",
        Folder = "Revenue",
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
                        CALCULATE([Avg Order Value]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Order Value], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },

    // ── Revenue — Avg Item Price KPI ──────────────────────────────────────────
    new {
        Name   = "Avg Item Price CM Label",
        Folder = "Revenue",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Item Price]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"") & ""M"",
                    _cm_value >= 1000, FORMAT(_cm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_cm_value, ""#,##0.0"")
                )
            RETURN
                IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Avg Item Price PM Label",
        Folder = "Revenue",
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
                        CALCULATE([Avg Item Price]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"") & ""M"",
                    _pm_value >= 1000, FORMAT(_pm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_pm_value, ""#,##0.0"")
                )
            RETURN
                IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Avg Item Price MoM Badge",
        Folder = "Revenue",
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
                        CALCULATE([Avg Item Price]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Item Price Badge BG Color",
        Folder = "Revenue",
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
                        CALCULATE([Avg Item Price]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#EAF8EC"",
                    _cm - _pm < 0, ""#FFDCDC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Avg Item Price Badge Text Color",
        Folder = "Revenue",
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
                        CALCULATE([Avg Item Price]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Avg Item Price], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },

    // ── Revenue — Avg Freight Value KPI (inverted: higher = worse) ────────────
    new {
        Name   = "Avg Freight Value CM Label",
        Folder = "Revenue",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Freight Value]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"") & ""M"",
                    _cm_value >= 1000, FORMAT(_cm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_cm_value, ""#,##0.0"")
                )
            RETURN
                IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Avg Freight Value PM Label",
        Folder = "Revenue",
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
                        CALCULATE([Avg Freight Value]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"") & ""M"",
                    _pm_value >= 1000, FORMAT(_pm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_pm_value, ""#,##0.0"")
                )
            RETURN
                IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · R$ "" & _fmt)
        "
    },
    new {
        Name   = "Avg Freight Value MoM Badge",
        Folder = "Revenue",
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
                        CALCULATE([Avg Freight Value]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Freight Value Badge BG Color",
        Folder = "Revenue",
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
                        CALCULATE([Avg Freight Value]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#FFDCDC"",
                    _cm - _pm < 0, ""#EAF8EC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Avg Freight Value Badge Text Color",
        Folder = "Revenue",
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
                        CALCULATE([Avg Freight Value]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Freight Value], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Red"", _cm - _pm < 0, ""Green"", ""Grey"")
        "
    },

    // ── Revenue — Total Items Sold KPI ────────────────────────────────────────
    new {
        Name   = "Total Items Sold CM Label",
        Folder = "Revenue",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Total Items Sold]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"") & ""M"",
                    _cm_value >= 1000, FORMAT(_cm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_cm_value, ""#,##0"")
                )
            RETURN
                IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & _fmt)
        "
    },
    new {
        Name   = "Total Items Sold PM Label",
        Folder = "Revenue",
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
                        CALCULATE([Total Items Sold]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"") & ""M"",
                    _pm_value >= 1000, FORMAT(_pm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_pm_value, ""#,##0"")
                )
            RETURN
                IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & _fmt)
        "
    },
    new {
        Name   = "Total Items Sold MoM Badge",
        Folder = "Revenue",
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
                        CALCULATE([Total Items Sold]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Total Items Sold Badge BG Color",
        Folder = "Revenue",
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
                        CALCULATE([Total Items Sold]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#EAF8EC"",
                    _cm - _pm < 0, ""#FFDCDC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Total Items Sold Badge Text Color",
        Folder = "Revenue",
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
                        CALCULATE([Total Items Sold]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Total Items Sold], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },

    // ── Revenue — Freight Share % KPI (inverted: higher = worse) ─────────────
    new {
        Name   = "Freight Share % CM Label",
        Folder = "Revenue",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Freight Share %]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            RETURN
                IF(ISBLANK(_cm_value),
                    BLANK(),
                    _cm_label & "" · "" & FORMAT(_cm_value, ""0.0%"")
                )
        "
    },
    new {
        Name   = "Freight Share % PM Label",
        Folder = "Revenue",
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
                        NOT ISBLANK(CALCULATE([Freight Share %])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            RETURN
                IF(ISBLANK(_pm_value),
                    BLANK(),
                    _pm_label & "" · "" & FORMAT(_pm_value, ""0.0%"")
                )
        "
    },
    new {
        Name   = "Freight Share % MoM Badge",
        Folder = "Revenue",
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
                        NOT ISBLANK(CALCULATE([Freight Share %])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                ROUND(
                    CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _last_ym),
                    3
                )
            VAR _pm =
                ROUND(
                    CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _pm_ym),
                    3
                )
            VAR _diff = _cm - _pm
            VAR _perc = FORMAT(ABS(_diff) * 100, ""0.0"") & "" pp""
            RETURN
                IF(_diff > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Freight Share % Badge BG Color",
        Folder = "Revenue",
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
                        NOT ISBLANK(CALCULATE([Freight Share %])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#FFDCDC"",
                    _cm - _pm < 0, ""#EAF8EC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Freight Share % Badge Text Color",
        Folder = "Revenue",
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
                        NOT ISBLANK(CALCULATE([Freight Share %])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm = CALCULATE([Freight Share %], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Red"", _cm - _pm < 0, ""Green"", ""Grey"")
        "
    },

    // ── Delivery — Base ───────────────────────────────────────────────────────
    new {
        Name   = "On-Time Delivery Rate",
        Folder = "Delivery",
        Dax    = @"
            DIVIDE(
                COUNTROWS(
                    FILTER(
                        'mart fact_sales',
                        NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]) &&
                        'mart fact_sales'[delivery_vs_estimate_days] <= 0
                    )
                ),
                COUNTROWS(
                    FILTER(
                        'mart fact_sales',
                        NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days])
                    )
                )
            )
        "
    },
    new {
        Name   = "Avg Delivery Days",
        Folder = "Delivery",
        Dax    = @"
            AVERAGEX(
                FILTER(
                    'mart fact_sales',
                    NOT ISBLANK('mart fact_sales'[purchase_to_delivery_days])
                ),
                'mart fact_sales'[purchase_to_delivery_days]
            )
        "
    },
    new {
        Name   = "Avg SLA Variance Days",
        Folder = "Delivery",
        Dax    = @"
            AVERAGEX(
                FILTER(
                    'mart fact_sales',
                    NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days])
                ),
                'mart fact_sales'[delivery_vs_estimate_days]
            )
        "
    },
    new {
        Name   = "Avg Approval Hours",
        Folder = "Delivery",
        Dax    = @"
            AVERAGEX(
                FILTER(
                    'mart fact_sales',
                    NOT ISBLANK('mart fact_sales'[purchase_to_approval_hours])
                ),
                'mart fact_sales'[purchase_to_approval_hours]
            )
        "
    },
    new {
        Name   = "Avg Last Mile Days",
        Folder = "Delivery",
        Dax    = @"
            AVERAGEX(
                FILTER(
                    'mart fact_sales',
                    NOT ISBLANK('mart fact_sales'[carrier_to_delivery_days])
                ),
                'mart fact_sales'[carrier_to_delivery_days]
            )
        "
    },
    new {
        Name   = "Order Status Share",
        Folder = "Delivery",
        Dax    = @"DIVIDE([Total Orders], CALCULATE([Total Orders], ALL('mart dim_order_status')))"
    },
    new {
        Name   = "Orders Very Early (>7d early)",
        Folder = "Delivery",
        Dax    = @"
            CALCULATE(
                DISTINCTCOUNT('mart fact_sales'[order_id]),
                FILTER(
                    'mart fact_sales',
                    NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]) &&
                    'mart fact_sales'[delivery_vs_estimate_days] < -7
                )
            )
        "
    },
    new {
        Name   = "Orders On-Time (0–7d early)",
        Folder = "Delivery",
        Dax    = @"
            CALCULATE(
                DISTINCTCOUNT('mart fact_sales'[order_id]),
                FILTER(
                    'mart fact_sales',
                    NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]) &&
                    'mart fact_sales'[delivery_vs_estimate_days] >= -7 &&
                    'mart fact_sales'[delivery_vs_estimate_days] <= 0
                )
            )
        "
    },
    new {
        Name   = "Orders Slightly Late (1–7d)",
        Folder = "Delivery",
        Dax    = @"
            CALCULATE(
                DISTINCTCOUNT('mart fact_sales'[order_id]),
                FILTER(
                    'mart fact_sales',
                    NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]) &&
                    'mart fact_sales'[delivery_vs_estimate_days] > 0 &&
                    'mart fact_sales'[delivery_vs_estimate_days] <= 7
                )
            )
        "
    },
    new {
        Name   = "Orders Very Late (>7d)",
        Folder = "Delivery",
        Dax    = @"
            CALCULATE(
                DISTINCTCOUNT('mart fact_sales'[order_id]),
                FILTER(
                    'mart fact_sales',
                    NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]) &&
                    'mart fact_sales'[delivery_vs_estimate_days] > 7
                )
            )
        "
    },

    // ── Delivery — On-Time Delivery Rate KPI ──────────────────────────────────
    new {
        Name   = "On-Time Delivery Rate CM Label",
        Folder = "Delivery",
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
                CALCULATE(
                    [On-Time Delivery Rate],
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            RETURN
                IF(ISBLANK(_cm_value),
                    BLANK(),
                    _cm_label & "" · "" & FORMAT(_cm_value, ""0.0%"")
                )
        "
    },
    new {
        Name   = "On-Time Delivery Rate PM Label",
        Folder = "Delivery",
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
                        NOT ISBLANK(CALCULATE([On-Time Delivery Rate])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([On-Time Delivery Rate], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            RETURN
                IF(ISBLANK(_pm_value),
                    BLANK(),
                    _pm_label & "" · "" & FORMAT(_pm_value, ""0.0%"")
                )
        "
    },
    new {
        Name   = "On-Time Delivery Rate MoM Badge",
        Folder = "Delivery",
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
                        NOT ISBLANK(CALCULATE([On-Time Delivery Rate])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                ROUND(
                    CALCULATE(
                        [On-Time Delivery Rate],
                        'mart dim_date'[year_month_key] = _last_ym
                    ),
                    3
                )
            VAR _pm =
                ROUND(
                    CALCULATE(
                        [On-Time Delivery Rate],
                        'mart dim_date'[year_month_key] = _pm_ym
                    ),
                    3
                )
            VAR _diff = _cm - _pm
            VAR _perc = FORMAT(ABS(_diff) * 100, ""0.0"") & "" pp""
            RETURN
                IF(_diff > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "On-Time Delivery Rate Badge BG Color",
        Folder = "Delivery",
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
                        NOT ISBLANK(CALCULATE([On-Time Delivery Rate])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE(
                    [On-Time Delivery Rate],
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _pm =
                CALCULATE([On-Time Delivery Rate], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#EAF8EC"",
                    _cm - _pm < 0, ""#FFDCDC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "On-Time Delivery Rate Badge Text Color",
        Folder = "Delivery",
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
                        NOT ISBLANK(CALCULATE([On-Time Delivery Rate])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE(
                    [On-Time Delivery Rate],
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _pm =
                CALCULATE([On-Time Delivery Rate], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },

    // ── Delivery — Avg Delivery Days KPI (inverted) ───────────────────────────
    new {
        Name   = "Avg Delivery Days CM Label",
        Folder = "Delivery",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Delivery Days]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            RETURN
                IF(ISBLANK(_cm_value),
                    BLANK(),
                    _cm_label & "" · "" & FORMAT(_cm_value, ""0.0"")
                )
        "
    },
    new {
        Name   = "Avg Delivery Days PM Label",
        Folder = "Delivery",
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
                        CALCULATE([Avg Delivery Days]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            RETURN
                IF(ISBLANK(_pm_value),
                    BLANK(),
                    _pm_label & "" · "" & FORMAT(_pm_value, ""0.0"")
                )
        "
    },
    new {
        Name   = "Avg Delivery Days MoM Badge",
        Folder = "Delivery",
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
                        CALCULATE([Avg Delivery Days]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Delivery Days Badge BG Color",
        Folder = "Delivery",
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
                        CALCULATE([Avg Delivery Days]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#FFDCDC"",
                    _cm - _pm < 0, ""#EAF8EC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Avg Delivery Days Badge Text Color",
        Folder = "Delivery",
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
                        CALCULATE([Avg Delivery Days]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Delivery Days], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Red"", _cm - _pm < 0, ""Green"", ""Grey"")
        "
    },

    // ── Delivery — Avg SLA Variance Days KPI (inverted, ABS in DIVIDE) ────────
    new {
        Name   = "Avg SLA Variance Days CM Label",
        Folder = "Delivery",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE(
                    [Avg SLA Variance Days],
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            RETURN
                IF(ISBLANK(_cm_value),
                    BLANK(),
                    _cm_label & "" · "" & FORMAT(_cm_value, ""0.0"")
                )
        "
    },
    new {
        Name   = "Avg SLA Variance Days PM Label",
        Folder = "Delivery",
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
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Avg SLA Variance Days], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            RETURN
                IF(ISBLANK(_pm_value),
                    BLANK(),
                    _pm_label & "" · "" & FORMAT(_pm_value, ""0.0"")
                )
        "
    },
    new {
        Name   = "Avg SLA Variance Days MoM Badge",
        Folder = "Delivery",
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
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE(
                    [Avg SLA Variance Days],
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _pm =
                CALCULATE([Avg SLA Variance Days], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, ABS(_pm)), ""0.0%;0.0%"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg SLA Variance Days Badge BG Color",
        Folder = "Delivery",
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
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE(
                    [Avg SLA Variance Days],
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _pm =
                CALCULATE([Avg SLA Variance Days], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#FFDCDC"",
                    _cm - _pm < 0, ""#EAF8EC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Avg SLA Variance Days Badge Text Color",
        Folder = "Delivery",
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
                        NOT ISBLANK(CALCULATE([Avg SLA Variance Days])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE(
                    [Avg SLA Variance Days],
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _pm =
                CALCULATE([Avg SLA Variance Days], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Red"", _cm - _pm < 0, ""Green"", ""Grey"")
        "
    },

    // ── Delivery — Avg Approval Hours KPI (inverted) ──────────────────────────
    new {
        Name   = "Avg Approval Hours CM Label",
        Folder = "Delivery",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Approval Hours]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            RETURN
                IF(ISBLANK(_cm_value),
                    BLANK(),
                    _cm_label & "" · "" & FORMAT(_cm_value, ""0.0"")
                )
        "
    },
    new {
        Name   = "Avg Approval Hours PM Label",
        Folder = "Delivery",
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
                        CALCULATE([Avg Approval Hours]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            RETURN
                IF(ISBLANK(_pm_value),
                    BLANK(),
                    _pm_label & "" · "" & FORMAT(_pm_value, ""0.0"")
                )
        "
    },
    new {
        Name   = "Avg Approval Hours MoM Badge",
        Folder = "Delivery",
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
                        CALCULATE([Avg Approval Hours]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Approval Hours Badge BG Color",
        Folder = "Delivery",
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
                        CALCULATE([Avg Approval Hours]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#FFDCDC"",
                    _cm - _pm < 0, ""#EAF8EC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Avg Approval Hours Badge Text Color",
        Folder = "Delivery",
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
                        CALCULATE([Avg Approval Hours]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Approval Hours], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Red"", _cm - _pm < 0, ""Green"", ""Grey"")
        "
    },

    // ── Delivery — Avg Last Mile Days KPI (inverted) ──────────────────────────
    new {
        Name   = "Avg Last Mile Days CM Label",
        Folder = "Delivery",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Last Mile Days]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            RETURN
                IF(ISBLANK(_cm_value),
                    BLANK(),
                    _cm_label & "" · "" & FORMAT(_cm_value, ""0.0"")
                )
        "
    },
    new {
        Name   = "Avg Last Mile Days PM Label",
        Folder = "Delivery",
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
                        CALCULATE([Avg Last Mile Days]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            RETURN
                IF(ISBLANK(_pm_value),
                    BLANK(),
                    _pm_label & "" · "" & FORMAT(_pm_value, ""0.0"")
                )
        "
    },
    new {
        Name   = "Avg Last Mile Days MoM Badge",
        Folder = "Delivery",
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
                        CALCULATE([Avg Last Mile Days]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Last Mile Days Badge BG Color",
        Folder = "Delivery",
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
                        CALCULATE([Avg Last Mile Days]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#FFDCDC"",
                    _cm - _pm < 0, ""#EAF8EC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Avg Last Mile Days Badge Text Color",
        Folder = "Delivery",
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
                        CALCULATE([Avg Last Mile Days]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Last Mile Days], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Red"", _cm - _pm < 0, ""Green"", ""Grey"")
        "
    },

    // ── Customer — Base ───────────────────────────────────────────────────────
    new {
        Name   = "Avg Review Score",
        Folder = "Customer",
        Dax    = @"
            AVERAGEX(
                FILTER('mart fact_sales', NOT ISBLANK('mart fact_sales'[review_score])),
                'mart fact_sales'[review_score]
            )
        "
    },
    new {
        Name   = "Unique Customers",
        Folder = "Customer",
        Dax    = @"DISTINCTCOUNT('mart fact_sales'[customer_key])"
    },
    new {
        Name   = "Review Submission Rate",
        Folder = "Customer",
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

    // ── Customer — Formatted ──────────────────────────────────────────────────
    new {
        Name   = "Unique Customers Formatted",
        Folder = "Customer",
        Dax    = @"
            VAR _val = [Unique Customers]
            RETURN
                SWITCH(TRUE(),
                    _val >= 1000000, FORMAT(_val / 1000000, ""0.0"") & ""M"",
                    _val >= 1000, FORMAT(_val / 1000, ""0.0"") & ""K"",
                    FORMAT(_val, ""#,##0"")
                )
        "
    },

    // ── Customer — Avg Review Score KPI ──────────────────────────────────────
    new {
        Name   = "Avg Review Score CM Label",
        Folder = "Customer",
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
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            RETURN
                IF(ISBLANK(_cm_value),
                    BLANK(),
                    _cm_label & "" · "" & FORMAT(_cm_value, ""0.00"")
                )
        "
    },
    new {
        Name   = "Avg Review Score PM Label",
        Folder = "Customer",
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
                        NOT ISBLANK(CALCULATE([Avg Review Score])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            RETURN
                IF(ISBLANK(_pm_value),
                    BLANK(),
                    _pm_label & "" · "" & FORMAT(_pm_value, ""0.00"")
                )
        "
    },
    new {
        Name   = "Avg Review Score MoM Badge",
        Folder = "Customer",
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
                        NOT ISBLANK(CALCULATE([Avg Review Score])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _diff = _cm - _pm
            VAR _perc = FORMAT(ABS(_diff), ""0.00"") & "" pt""
            RETURN
                IF(_diff > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Review Score Badge BG Color",
        Folder = "Customer",
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
                        NOT ISBLANK(CALCULATE([Avg Review Score])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#EAF8EC"",
                    _cm - _pm < 0, ""#FFDCDC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Avg Review Score Badge Text Color",
        Folder = "Customer",
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
                        NOT ISBLANK(CALCULATE([Avg Review Score])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Avg Review Score], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },

    // ── Customer — Unique Customers KPI ──────────────────────────────────────
    new {
        Name   = "Unique Customers CM Label",
        Folder = "Customer",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Unique Customers]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _cm_value >= 1000000, FORMAT(_cm_value / 1000000, ""0.0"") & ""M"",
                    _cm_value >= 1000, FORMAT(_cm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_cm_value, ""#,##0"")
                )
            RETURN
                IF(ISBLANK(_cm_value), BLANK(), _cm_label & "" · "" & _fmt)
        "
    },
    new {
        Name   = "Unique Customers PM Label",
        Folder = "Customer",
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
                        CALCULATE([Unique Customers]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            VAR _fmt =
                SWITCH(TRUE(),
                    _pm_value >= 1000000, FORMAT(_pm_value / 1000000, ""0.0"") & ""M"",
                    _pm_value >= 1000, FORMAT(_pm_value / 1000, ""0.0"") & ""K"",
                    FORMAT(_pm_value, ""#,##0"")
                )
            RETURN
                IF(ISBLANK(_pm_value), BLANK(), _pm_label & "" · "" & _fmt)
        "
    },
    new {
        Name   = "Unique Customers MoM Badge",
        Folder = "Customer",
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
                        CALCULATE([Unique Customers]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Unique Customers Badge BG Color",
        Folder = "Customer",
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
                        CALCULATE([Unique Customers]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#EAF8EC"",
                    _cm - _pm < 0, ""#FFDCDC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Unique Customers Badge Text Color",
        Folder = "Customer",
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
                        CALCULATE([Unique Customers]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Unique Customers], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },

    // ── Customer — Review Submission Rate KPI ─────────────────────────────────
    new {
        Name   = "Review Submission Rate CM Label",
        Folder = "Customer",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Review Submission Rate]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE(
                    [Review Submission Rate],
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            RETURN
                IF(ISBLANK(_cm_value),
                    BLANK(),
                    _cm_label & "" · "" & FORMAT(_cm_value, ""0.0%"")
                )
        "
    },
    new {
        Name   = "Review Submission Rate PM Label",
        Folder = "Customer",
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
                        NOT ISBLANK(CALCULATE([Review Submission Rate])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE(
                    [Review Submission Rate],
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            RETURN
                IF(ISBLANK(_pm_value),
                    BLANK(),
                    _pm_label & "" · "" & FORMAT(_pm_value, ""0.0%"")
                )
        "
    },
    new {
        Name   = "Review Submission Rate MoM Badge",
        Folder = "Customer",
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
                        NOT ISBLANK(CALCULATE([Review Submission Rate])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                ROUND(
                    CALCULATE(
                        [Review Submission Rate],
                        'mart dim_date'[year_month_key] = _last_ym
                    ),
                    3
                )
            VAR _pm =
                ROUND(
                    CALCULATE(
                        [Review Submission Rate],
                        'mart dim_date'[year_month_key] = _pm_ym
                    ),
                    3
                )
            VAR _diff = _cm - _pm
            VAR _perc = FORMAT(ABS(_diff) * 100, ""0.0"") & "" pp""
            RETURN
                IF(_diff > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Review Submission Rate Badge BG Color",
        Folder = "Customer",
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
                        NOT ISBLANK(CALCULATE([Review Submission Rate])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE(
                    [Review Submission Rate],
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _pm =
                CALCULATE(
                    [Review Submission Rate],
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#EAF8EC"",
                    _cm - _pm < 0, ""#FFDCDC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Review Submission Rate Badge Text Color",
        Folder = "Customer",
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
                        NOT ISBLANK(CALCULATE([Review Submission Rate])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE(
                    [Review Submission Rate],
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _pm =
                CALCULATE(
                    [Review Submission Rate],
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },

    // ── Payments — Base ───────────────────────────────────────────────────────
    new {
        Name   = "Total Transaction Value",
        Folder = "Payments",
        Dax    = @"SUM('mart fact_payments'[payment_value])"
    },
    new {
        Name   = "Avg Credit Card Installments",
        Folder = "Payments",
        Dax    = @"
            AVERAGEX(
                FILTER('mart fact_payments', 'mart fact_payments'[payment_type_key] = 1),
                'mart fact_payments'[payment_installments]
            )
        "
    },
    new {
        Name   = "Credit Card Share %",
        Folder = "Payments",
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

    // ── Payments — Avg Credit Card Installments KPI (neutral: direction ambiguous) ──
    new {
        Name   = "Avg Credit Card Installments CM Label",
        Folder = "Payments",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        CALCULATE([Avg Credit Card Installments]) > 0
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE(
                    [Avg Credit Card Installments],
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            RETURN
                IF(ISBLANK(_cm_value),
                    BLANK(),
                    _cm_label & "" · "" & FORMAT(_cm_value, ""0.0"")
                )
        "
    },
    new {
        Name   = "Avg Credit Card Installments PM Label",
        Folder = "Payments",
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
                        CALCULATE([Avg Credit Card Installments]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE(
                    [Avg Credit Card Installments],
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            RETURN
                IF(ISBLANK(_pm_value),
                    BLANK(),
                    _pm_label & "" · "" & FORMAT(_pm_value, ""0.0"")
                )
        "
    },
    new {
        Name   = "Avg Credit Card Installments MoM Badge",
        Folder = "Payments",
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
                        CALCULATE([Avg Credit Card Installments]) > 0 &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE(
                    [Avg Credit Card Installments],
                    'mart dim_date'[year_month_key] = _last_ym
                )
            VAR _pm =
                CALCULATE(
                    [Avg Credit Card Installments],
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            VAR _perc = FORMAT(DIVIDE(_cm - _pm, _pm), ""0.0%;0.0%"")
            RETURN
                IF(_cm - _pm > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Avg Credit Card Installments Badge BG Color",
        Folder = "Payments",
        Dax    = @"""#F2F2F2"""
    },
    new {
        Name   = "Avg Credit Card Installments Badge Text Color",
        Folder = "Payments",
        Dax    = @"""Grey"""
    },

    // ── Payments — Credit Card Share % KPI ────────────────────────────────────
    new {
        Name   = "Credit Card Share % CM Label",
        Folder = "Payments",
        Dax    = @"
            VAR _last_ym =
                MAXX(
                    FILTER(
                        VALUES('mart dim_date'[year_month_key]),
                        NOT ISBLANK(CALCULATE([Credit Card Share %]))
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm_value =
                CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _cm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _last_ym
                )
            RETURN
                IF(ISBLANK(_cm_value),
                    BLANK(),
                    _cm_label & "" · "" & FORMAT(_cm_value, ""0.0%"")
                )
        "
    },
    new {
        Name   = "Credit Card Share % PM Label",
        Folder = "Payments",
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
                        NOT ISBLANK(CALCULATE([Credit Card Share %])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _pm_value =
                CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _pm_ym)
            VAR _pm_label =
                CALCULATE(
                    MAX('mart dim_date'[month_year_short]),
                    'mart dim_date'[year_month_key] = _pm_ym
                )
            RETURN
                IF(ISBLANK(_pm_value),
                    BLANK(),
                    _pm_label & "" · "" & FORMAT(_pm_value, ""0.0%"")
                )
        "
    },
    new {
        Name   = "Credit Card Share % MoM Badge",
        Folder = "Payments",
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
                        NOT ISBLANK(CALCULATE([Credit Card Share %])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                ROUND(
                    CALCULATE(
                        [Credit Card Share %],
                        'mart dim_date'[year_month_key] = _last_ym
                    ),
                    3
                )
            VAR _pm =
                ROUND(
                    CALCULATE(
                        [Credit Card Share %],
                        'mart dim_date'[year_month_key] = _pm_ym
                    ),
                    3
                )
            VAR _diff = _cm - _pm
            VAR _perc = FORMAT(ABS(_diff) * 100, ""0.0"") & "" pp""
            RETURN
                IF(_diff > 0, UNICHAR(9650) & "" "" & _perc, UNICHAR(9660) & "" "" & _perc)
        "
    },
    new {
        Name   = "Credit Card Share % Badge BG Color",
        Folder = "Payments",
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
                        NOT ISBLANK(CALCULATE([Credit Card Share %])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(),
                    _cm - _pm > 0, ""#EAF8EC"",
                    _cm - _pm < 0, ""#FFDCDC"",
                    ""#F2F2F2""
                )
        "
    },
    new {
        Name   = "Credit Card Share % Badge Text Color",
        Folder = "Payments",
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
                        NOT ISBLANK(CALCULATE([Credit Card Share %])) &&
                        'mart dim_date'[year_month_key] < _last_ym
                    ),
                    'mart dim_date'[year_month_key]
                )
            VAR _cm =
                CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _last_ym)
            VAR _pm =
                CALCULATE([Credit Card Share %], 'mart dim_date'[year_month_key] = _pm_ym)
            RETURN
                SWITCH(TRUE(), _cm - _pm > 0, ""Green"", _cm - _pm < 0, ""Red"", ""Grey"")
        "
    },

    // ── Display — Page 1 ─────────────────────────────────────────────────────
    new {
        Name   = "Title Revenue Trend",
        Folder = "Display",
        Dax    = @"
            VAR _min = MIN('mart dim_date'[year])
            VAR _max = MAX('mart dim_date'[year])
            VAR _count = DISTINCTCOUNT('mart dim_date'[year])
            RETURN
                ""Monthly Revenue Trend (R$, "" &
                IF(_count = 1, _min, _min & ""–"" & _max) &
                "")""
        "
    },
    new {
        Name   = "Title Top Categories Revenue",
        Folder = "Display",
        Dax    = @"
            VAR _cat = SELECTEDVALUE('mart dim_product'[product_category_name_english])
            VAR _cat_n =
                COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))
            RETURN
                SWITCH(TRUE(),
                    _cat_n = 1, _cat & "" — Revenue (R$)"",
                    _cat_n < 5, _cat_n & "" Categories — Revenue (R$)"",
                    ""Top 5 Product Categories by Revenue (R$)""
                )
        "
    },
    new {
        Name   = "Title Revenue by State",
        Folder = "Display",
        Dax    = @"
            VAR _state = SELECTEDVALUE('mart dim_customer'[customer_state_name])
            VAR _state_n = COUNTROWS(VALUES('mart dim_customer'[customer_state_name]))
            VAR _all_n = COUNTROWS(ALL('mart dim_customer'[customer_state_name]))
            RETURN
                SWITCH(TRUE(),
                    _state_n = 1, _state & "" — Revenue (R$)"",
                    _state_n < _all_n, _state_n & "" States — Revenue (R$)"",
                    ""Revenue by State (R$)""
                )
        "
    },

    // ── Display — Page 2 ─────────────────────────────────────────────────────
    new {
        Name   = "Title Category Revenue Trend",
        Folder = "Display",
        Dax    = @"
            VAR _min = MIN('mart dim_date'[year])
            VAR _max = MAX('mart dim_date'[year])
            VAR _count = DISTINCTCOUNT('mart dim_date'[year])
            VAR _years = IF(_count = 1, _min, _min & ""–"" & _max)
            VAR _cat = SELECTEDVALUE('mart dim_product'[product_category_name_english])
            VAR _cat_n =
                COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))
            RETURN
                SWITCH(TRUE(),
                    _cat_n = 1, _cat & "" — Monthly Revenue Trend (R$, "" & _years & "")"",
                    _cat_n < 5, _cat_n & "" Categories — Monthly Revenue Trend (R$, "" & _years & "")"",
                    ""Top 5 Categories — Monthly Revenue Trend (R$, "" & _years & "")""
                )
        "
    },
    new {
        Name   = "Title Items Sold",
        Folder = "Display",
        Dax    = @"
            VAR _cat = SELECTEDVALUE('mart dim_product'[product_category_name_english])
            VAR _cat_n =
                COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))
            RETURN
                SWITCH(TRUE(),
                    _cat_n = 1, _cat & "" — Items Sold"",
                    _cat_n < 5, _cat_n & "" Categories — Items Sold"",
                    ""Top 5 Categories by Items Sold""
                )
        "
    },
    new {
        Name   = "Title Freight Share",
        Folder = "Display",
        Dax    = @"
            VAR _cat = SELECTEDVALUE('mart dim_product'[product_category_name_english])
            VAR _cat_n =
                COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))
            RETURN
                SWITCH(TRUE(),
                    _cat_n = 1, _cat & "" — Freight Share"",
                    _cat_n < 5, _cat_n & "" Categories — Freight Share"",
                    ""Top 5 Categories by Freight Share""
                )
        "
    },
    new {
        Name   = "Title Avg Item Price",
        Folder = "Display",
        Dax    = @"
            VAR _cat = SELECTEDVALUE('mart dim_product'[product_category_name_english])
            VAR _cat_n =
                COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))
            RETURN
                SWITCH(TRUE(),
                    _cat_n = 1, _cat & "" — Avg Item Price (R$)"",
                    _cat_n < 5, _cat_n & "" Categories — Avg Item Price (R$)"",
                    ""Top 5 Categories by Avg Item Price (R$)""
                )
        "
    },

    // ── Display — Page 3 ─────────────────────────────────────────────────────
    new {
        Name   = "Title Delivery Days by State",
        Folder = "Display",
        Dax    = @"
            VAR _state = SELECTEDVALUE('mart dim_customer'[customer_state_name])
            VAR _state_n = COUNTROWS(VALUES('mart dim_customer'[customer_state_name]))
            RETURN
                SWITCH(TRUE(),
                    _state_n = 1, _state & "" — Avg Delivery Days"",
                    _state_n < 5, _state_n & "" States — Avg Delivery Days"",
                    ""Avg Delivery Days — Top 5 States by Volume""
                )
        "
    },
    new {
        Name   = "Title Delivery Trend",
        Folder = "Display",
        Dax    = @"
            VAR _min = MIN('mart dim_date'[year])
            VAR _max = MAX('mart dim_date'[year])
            VAR _count = DISTINCTCOUNT('mart dim_date'[year])
            RETURN
                ""Monthly Delivery Trend ("" & IF(_count = 1, _min, _min & ""–"" & _max) & "")""
        "
    },
    new {
        Name   = "Avg SLA Variance Display",
        Folder = "Display",
        Dax    = @"
            VAR _val = [Avg SLA Variance Days]
            RETURN
                IF(_val < 0,
                    FORMAT(_val, ""0.0"") & "" "" & UNICHAR(9650),
                    FORMAT(_val, ""0.0"") & "" "" & UNICHAR(9660)
                )
        "
    },

    // ── Display — Page 4 ─────────────────────────────────────────────────────
    new {
        Name   = "Title States by Customers",
        Folder = "Display",
        Dax    = @"
            VAR _state = SELECTEDVALUE('mart dim_customer'[customer_state_name])
            VAR _state_n = COUNTROWS(VALUES('mart dim_customer'[customer_state_name]))
            RETURN
                SWITCH(TRUE(),
                    _state_n = 1, _state & "" — Unique Customers"",
                    _state_n < 5, _state_n & "" States — Unique Customers"",
                    ""Top 5 States by Unique Customers""
                )
        "
    },
    new {
        Name   = "Title Review Score Trend",
        Folder = "Display",
        Dax    = @"
            VAR _min = MIN('mart dim_date'[year])
            VAR _max = MAX('mart dim_date'[year])
            VAR _count = DISTINCTCOUNT('mart dim_date'[year])
            RETURN
                ""Monthly Review Trend ("" & IF(_count = 1, _min, _min & ""–"" & _max) & "")""
        "
    },

    // ── Display — Global ─────────────────────────────────────────────────────
    new {
        Name   = "Last Updated",
        Folder = "Display",
        Dax    = @"
            VAR _date = MAX('mart fact_sales'[mart_load_ts])
            VAR _month = MONTH(_date)
            VAR _month_name =
                SWITCH(
                    _month,
                    1,
                    ""Jan"",
                    2,
                    ""Feb"",
                    3,
                    ""Mar"",
                    4,
                    ""Apr"",
                    5,
                    ""May"",
                    6,
                    ""Jun"",
                    7,
                    ""Jul"",
                    8,
                    ""Aug"",
                    9,
                    ""Sep"",
                    10,
                    ""Oct"",
                    11,
                    ""Nov"",
                    12,
                    ""Dec""
                )
            RETURN
                ""Last updated: "" &
                FORMAT(_date, ""DD"") &
                "" "" &
                _month_name &
                "" "" &
                FORMAT(_date, ""YYYY"")
        "
    },

    // ── Display\AxisMax ───────────────────────────────────────────────────────
    new {
        Name   = "Axis Max Categories Revenue",
        Folder = "Display\\AxisMax",
        Dax    = @"
            CALCULATE(
                MAXX(
                    VALUES('mart dim_product'[product_category_name_english]),
                    [Total Revenue]
                ),
                ALLSELECTED('mart dim_product'[product_category_name_english])
            ) * 1.15
        "
    },
    new {
        Name   = "Axis Max Items Sold",
        Folder = "Display\\AxisMax",
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
        Name   = "Axis Max Freight Share",
        Folder = "Display\\AxisMax",
        Dax    = @"
            CALCULATE(
                MAXX(
                    VALUES('mart dim_product'[product_category_name_english]),
                    [Freight Share %]
                ),
                ALLSELECTED('mart dim_product'[product_category_name_english])
            ) * 1.15
        "
    },
    new {
        Name   = "Axis Max Avg Item Price",
        Folder = "Display\\AxisMax",
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
        Name   = "Axis Max Avg Delivery Days",
        Folder = "Display\\AxisMax",
        Dax    = @"
            CALCULATE(
                MAXX(VALUES('mart dim_customer'[customer_state_name]), [Avg Delivery Days]),
                ALLSELECTED('mart dim_customer'[customer_state_name])
            ) * 1.15
        "
    },
    new {
        Name   = "Axis Max Orders by Status",
        Folder = "Display\\AxisMax",
        Dax    = @"
            CALCULATE(
                MAXX(VALUES('mart dim_order_status'[status_name]), [Total Orders]),
                ALLSELECTED('mart dim_order_status'[status_name])
            ) * 1.15
        "
    },
    new {
        Name   = "Axis Max Payment Type",
        Folder = "Display\\AxisMax",
        Dax    = @"
            CALCULATE(
                MAXX(
                    VALUES('mart dim_payment_type'[payment_type_name]),
                    [Total Transaction Value]
                ),
                ALLSELECTED('mart dim_payment_type'[payment_type_name])
            ) * 1.15
        "
    },
    new {
        Name   = "Axis Max States by Customers",
        Folder = "Display\\AxisMax",
        Dax    = @"
            CALCULATE(
                MAXX(VALUES('mart dim_customer'[customer_state_name]), [Unique Customers]),
                ALLSELECTED('mart dim_customer'[customer_state_name])
            ) * 1.15
        "
    },
};

foreach(var d in defs) {
    t.AddMeasure(d.Name, Dedent(d.Dax), d.Folder);
}

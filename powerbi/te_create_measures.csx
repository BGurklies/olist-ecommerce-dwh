// Tabular Editor 2 — Bulk-create all DAX measures for the Olist dashboard.
// Usage: open Power BI Desktop with the model, launch Tabular Editor via External Tools,
//        paste this script into the C# Script tab and press F5, then Ctrl+S.

foreach(var m in Model.Tables["_Measures"].Measures.ToList()) m.Delete();

var t = Model.Tables["_Measures"];

var defs = new[] {
    // ── Revenue (Page 1 & 2) ────────────────────────────────────────────────
    new { Name = "Total Revenue",            Dax = "SUM('mart fact_sales'[total_value])",                                                                            Folder = "Revenue"  },
    new { Name = "Total Orders",             Dax = "DISTINCTCOUNT('mart fact_sales'[order_id])",                                                                     Folder = "Revenue"  },
    new { Name = "Avg Order Value",          Dax = "DIVIDE([Total Revenue], [Total Orders])",                                                                        Folder = "Revenue"  },
    new { Name = "Total Items Sold",         Dax = "COUNTROWS('mart fact_sales')",                                                                                   Folder = "Revenue"  },
    new { Name = "Avg Item Price",           Dax = "AVERAGE('mart fact_sales'[price])",                                                                              Folder = "Revenue"  },
    new { Name = "Avg Freight Value",        Dax = "AVERAGE('mart fact_sales'[freight_value])",                                                                      Folder = "Revenue"  },
    new { Name = "Freight Share %",          Dax = "DIVIDE(SUM('mart fact_sales'[freight_value]), SUM('mart fact_sales'[total_value]))",                             Folder = "Revenue"  },
    // ── Delivery (Page 1 & 3) ───────────────────────────────────────────────
    new { Name = "On-Time Delivery Rate",    Dax = "DIVIDE(COUNTROWS(FILTER('mart fact_sales', NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]) && 'mart fact_sales'[delivery_vs_estimate_days] <= 0)), COUNTROWS(FILTER('mart fact_sales', NOT ISBLANK('mart fact_sales'[delivery_vs_estimate_days]))))", Folder = "Delivery" },
    // ── Customer (Page 1 & 4) ───────────────────────────────────────────────
    new { Name = "Avg Review Score",         Dax = "AVERAGEX(FILTER('mart fact_sales', NOT ISBLANK('mart fact_sales'[review_score])), 'mart fact_sales'[review_score])", Folder = "Customer" },
    // ── Display — Page 1 ────────────────────────────────────────────────────
    new { Name = "Title Revenue Trend",
          Dax = "VAR _min = MIN('mart dim_date'[year])\nVAR _max = MAX('mart dim_date'[year])\nVAR _count = DISTINCTCOUNT('mart dim_date'[year])\nRETURN\n    \"Monthly Revenue Trend (R$, \" &\n    IF(_count = 1, _min, _min & \"–\" & _max) &\n    \")\"",
          Folder = "Display"  },
    new { Name = "Title Top Categories Revenue",
          Dax = "VAR _cat   = SELECTEDVALUE('mart dim_product'[product_category_name_english])\nVAR _cat_n = COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))\nRETURN\n    SWITCH(TRUE(),\n        _cat_n = 1,  _cat & \" — Revenue (R$)\",\n        _cat_n < 5,  _cat_n & \" Categories — Revenue (R$)\",\n                     \"Top 5 Product Categories by Revenue (R$)\"\n    )",
          Folder = "Display"  },
    new { Name = "Title Revenue by State",
          Dax = "VAR _state   = SELECTEDVALUE('mart dim_customer'[customer_state_name])\nVAR _state_n = COUNTROWS(VALUES('mart dim_customer'[customer_state_name]))\nVAR _all_n   = COUNTROWS(ALL('mart dim_customer'[customer_state_name]))\nRETURN\n    SWITCH(TRUE(),\n        _state_n = 1,          _state & \" — Revenue (R$)\",\n        _state_n < _all_n,     _state_n & \" States — Revenue (R$)\",\n                               \"Revenue by State (R$)\"\n    )",
          Folder = "Display"  },
    // ── Display — Page 2 ────────────────────────────────────────────────────
    new { Name = "Title Category Revenue Trend",
          Dax = "VAR _min   = MIN('mart dim_date'[year])\nVAR _max   = MAX('mart dim_date'[year])\nVAR _count = DISTINCTCOUNT('mart dim_date'[year])\nVAR _years = IF(_count = 1, _min, _min & \"–\" & _max)\nVAR _cat   = SELECTEDVALUE('mart dim_product'[product_category_name_english])\nVAR _cat_n = COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))\nRETURN\n    SWITCH(TRUE(),\n        _cat_n = 1,  _cat & \" — Monthly Revenue Trend (R$, \" & _years & \")\",\n        _cat_n < 5,  _cat_n & \" Categories — Monthly Revenue Trend (R$, \" & _years & \")\",\n                     \"Top 5 Categories — Monthly Revenue Trend (R$, \" & _years & \")\"\n    )",
          Folder = "Display"  },
    new { Name = "Title Items Sold",
          Dax = "VAR _cat   = SELECTEDVALUE('mart dim_product'[product_category_name_english])\nVAR _cat_n = COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))\nRETURN\n    SWITCH(TRUE(),\n        _cat_n = 1,  _cat & \" — Items Sold\",\n        _cat_n < 5,  _cat_n & \" Categories — Items Sold\",\n                     \"Top 5 Categories by Items Sold\"\n    )",
          Folder = "Display"  },
    new { Name = "Title Freight Share",
          Dax = "VAR _cat   = SELECTEDVALUE('mart dim_product'[product_category_name_english])\nVAR _cat_n = COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))\nRETURN\n    SWITCH(TRUE(),\n        _cat_n = 1,  _cat & \" — Freight Share %\",\n        _cat_n < 5,  _cat_n & \" Categories — Freight Share %\",\n                     \"Top 5 Categories by Freight Share %\"\n    )",
          Folder = "Display"  },
    new { Name = "Title Avg Item Price",
          Dax = "VAR _cat   = SELECTEDVALUE('mart dim_product'[product_category_name_english])\nVAR _cat_n = COUNTROWS(VALUES('mart dim_product'[product_category_name_english]))\nRETURN\n    SWITCH(TRUE(),\n        _cat_n = 1,  _cat & \" — Avg Item Price (R$)\",\n        _cat_n < 5,  _cat_n & \" Categories — Avg Item Price (R$)\",\n                     \"Top 5 Categories by Avg Item Price (R$)\"\n    )",
          Folder = "Display"  },
    // ── Display — Global ────────────────────────────────────────────────────
    new { Name = "Last Updated",
          Dax = "VAR _date = MAX('mart fact_sales'[mart_load_ts])\nVAR _month = MONTH(_date)\nVAR _month_name = SWITCH(_month,\n    1, \"Jan\", 2, \"Feb\", 3, \"Mar\", 4, \"Apr\",\n    5, \"May\", 6, \"Jun\", 7, \"Jul\", 8, \"Aug\",\n    9, \"Sep\", 10, \"Oct\", 11, \"Nov\", 12, \"Dec\"\n)\nRETURN\n    \"Last updated: \" & FORMAT(_date, \"DD\") & \" \" & _month_name & \" \" & FORMAT(_date, \"YYYY\")",
          Folder = "Display"  },
};

foreach(var d in defs) {
    t.AddMeasure(d.Name, d.Dax, d.Folder);
}

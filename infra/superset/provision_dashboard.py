import json
import sys
import requests

BASE = "http://localhost:8088"
USER, PW = "admin", "admin"
TRINO_URI = "trino://trino@trino:8080/iceberg"

s = requests.Session()


def login():
    r = s.post(f"{BASE}/api/v1/security/login",
               json={"username": USER, "password": PW, "provider": "db", "refresh": True})
    r.raise_for_status()
    tok = r.json()["access_token"]
    s.headers.update({"Authorization": f"Bearer {tok}"})

    c = s.get(f"{BASE}/api/v1/security/csrf_token/")
    c.raise_for_status()
    s.headers.update({"X-CSRFToken": c.json()["result"], "Referer": BASE})


def find(endpoint, col, val):
    r = s.get(f"{BASE}/api/v1/{endpoint}/", params={"q": json.dumps({"page_size": 100})})
    r.raise_for_status()
    for item in r.json().get("result", []):
        if item.get(col) == val:
            return item["id"]
    return None


def get_database():
    did = find("database", "database_name", "LAMF Trino")
    if did:
        print(f"  db exists id={did}")
        return did
    r = s.post(f"{BASE}/api/v1/database/", json={
        "database_name": "LAMF Trino",
        "sqlalchemy_uri": TRINO_URI,
        "expose_in_sqllab": True,
        "allow_ctas": False, "allow_cvas": False,
    })
    if r.status_code not in (200, 201):
        print("DB create failed:", r.status_code, r.text); sys.exit(1)
    did = r.json()["id"]
    print(f"  db created id={did}")
    return did


def get_dataset(db_id, table):
    dsid = find("dataset", "table_name", table)
    if dsid:
        print(f"  dataset {table} exists id={dsid}")
        return dsid
    r = s.post(f"{BASE}/api/v1/dataset/", json={
        "database": db_id, "schema": "gold", "table_name": table,
    })
    if r.status_code not in (200, 201):
        print(f"dataset {table} failed:", r.status_code, r.text); sys.exit(1)
    dsid = r.json()["id"]
    print(f"  dataset {table} created id={dsid}")
    return dsid


def metric(sql, label):
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label,
            "optionName": "m_" + label.lower().replace(" ", "_"), "aggregate": None}


def build_query_context(dsid, params):
    """Superset 4.x needs a saved query_context for /data/ + dashboard render."""
    viz = params["viz_type"]
    if params.get("query_mode") == "raw":
        cols, mets = params.get("all_columns", []), []
    elif "groupby" in params:
        cols, mets = params.get("groupby", []), params.get("metrics", [])
    else: 
        cols, mets = [], [params["metric"]]
    query = {
        "filters": [], "extras": {"having": "", "where": ""},
        "applied_time_extras": {}, "columns": cols, "metrics": mets,
        "orderby": [], "annotation_layers": [],
        "row_limit": params.get("row_limit", 1000),
        "series_limit": 0, "order_desc": True, "url_params": {},
        "custom_params": {}, "custom_form_data": {},
    }
    return {
        "datasource": {"id": dsid, "type": "table"},
        "force": False, "queries": [query],
        "form_data": {**params, "datasource": f"{dsid}__table"},
        "result_format": "json", "result_type": "full",
    }


def make_chart(name, dsid, viz, params):
    p = {"datasource": f"{dsid}__table", "viz_type": viz, **params}
    body = {"slice_name": name, "viz_type": viz,
            "datasource_id": dsid, "datasource_type": "table",
            "params": json.dumps(p),
            "query_context": json.dumps(build_query_context(dsid, p))}
    r = s.post(f"{BASE}/api/v1/chart/", json=body)
    if r.status_code not in (200, 201):
        print(f"chart '{name}' failed:", r.status_code, r.text); sys.exit(1)
    cid = r.json()["id"]
    print(f"  chart '{name}' id={cid}")
    return cid


def cleanup():
    """Delete any prior dashboard + charts we created, so this is re-runnable."""
    did = find("dashboard", "dashboard_title", "LAMF Portfolio & Risk")
    if did:
        s.delete(f"{BASE}/api/v1/dashboard/{did}")
        print(f"  removed old dashboard id={did}")
    for nm in ["Total Loan Book", "AUM Pledged", "Active Loans", "Open Margin Calls",
               "Customers by Segment", "Exposure by NPA Bucket", "Risk by DPD Bucket"]:
        cid = find("chart", "slice_name", nm)
        while cid:
            s.delete(f"{BASE}/api/v1/chart/{cid}")
            cid = find("chart", "slice_name", nm)


def big_number(name, dsid, sql, label, subheader):
    return make_chart(name, dsid, "big_number_total", {
        "metric": metric(sql, label), "adhoc_filters": [],
        "subheader": subheader, "header_font_size": 0.4, "subheader_font_size": 0.15,
    })


def main():
    login()
    print("logged in")
    cleanup()
    db_id = get_database()

    ds_portfolio = get_dataset(db_id, "mart_portfolio_daily")
    ds_risk = get_dataset(db_id, "mart_risk_daily")
    ds_c360 = get_dataset(db_id, "customer_360")

    charts = []
    charts.append(("kpi1", big_number("Total Loan Book", ds_portfolio,
                   "SUM(loan_book)", "Loan Book", "₹ outstanding (sum)")))
    charts.append(("kpi2", big_number("AUM Pledged", ds_portfolio,
                   "SUM(aum_pledged)", "AUM Pledged", "₹ collateral value")))
    charts.append(("kpi3", big_number("Active Loans", ds_portfolio,
                   "SUM(active_loans)", "Active Loans", "count")))
    charts.append(("kpi4", big_number("Open Margin Calls", ds_risk,
                   "SUM(margin_calls)", "Margin Calls", "loans breaching DP")))

    charts.append(("seg", make_chart("Customers by Segment", ds_c360, "pie", {
        "groupby": ["segment"], "metric": metric("COUNT(*)", "customers"),
        "adhoc_filters": [], "row_limit": 25, "show_legend": True,
    })))
    charts.append(("exposure", make_chart("Exposure by NPA Bucket", ds_risk, "dist_bar", {
        "groupby": ["npa_bucket"], "metrics": [metric("SUM(exposure)", "exposure")],
        "adhoc_filters": [], "row_limit": 50, "show_legend": False,
    })))
    charts.append(("risktbl", make_chart("Risk by DPD Bucket", ds_risk, "table", {
        "query_mode": "raw",
        "all_columns": ["npa_bucket", "loans", "exposure", "margin_calls", "total_shortfall"],
        "adhoc_filters": [], "row_limit": 50, "order_desc": True,
    })))

    cid = {k: v for k, v in charts}

    def chart_node(node_id, parents, chart_id, name, w, h):
        return {"type": "CHART", "id": node_id, "children": [],
                "parents": parents,
                "meta": {"chartId": chart_id, "uuid": None, "sliceName": name,
                         "width": w, "height": h}}

    GP = ["ROOT_ID", "GRID_ID"]
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "parents": ["ROOT_ID"],
                    "children": ["ROW1", "ROW2"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID",
                      "meta": {"text": "LAMF — Portfolio & Risk (live via Trino)"}},
        "ROW1": {"type": "ROW", "id": "ROW1", "parents": GP,
                 "meta": {"background": "BACKGROUND_TRANSPARENT"},
                 "children": ["C_kpi1", "C_kpi2", "C_kpi3", "C_kpi4"]},
        "ROW2": {"type": "ROW", "id": "ROW2", "parents": GP,
                 "meta": {"background": "BACKGROUND_TRANSPARENT"},
                 "children": ["C_seg", "C_exposure", "C_risktbl"]},
        "C_kpi1": chart_node("C_kpi1", GP + ["ROW1"], cid["kpi1"], "Total Loan Book", 3, 50),
        "C_kpi2": chart_node("C_kpi2", GP + ["ROW1"], cid["kpi2"], "AUM Pledged", 3, 50),
        "C_kpi3": chart_node("C_kpi3", GP + ["ROW1"], cid["kpi3"], "Active Loans", 3, 50),
        "C_kpi4": chart_node("C_kpi4", GP + ["ROW1"], cid["kpi4"], "Open Margin Calls", 3, 50),
        "C_seg": chart_node("C_seg", GP + ["ROW2"], cid["seg"], "Customers by Segment", 4, 60),
        "C_exposure": chart_node("C_exposure", GP + ["ROW2"], cid["exposure"], "Exposure by NPA Bucket", 4, 60),
        "C_risktbl": chart_node("C_risktbl", GP + ["ROW2"], cid["risktbl"], "Risk by DPD Bucket", 4, 60),
    }

    body = {"dashboard_title": "LAMF Portfolio & Risk",
            "published": True,
            "position_json": json.dumps(pos)}

    r = s.post(f"{BASE}/api/v1/dashboard/", json=body)
    if r.status_code not in (200, 201):
        print("dashboard create failed:", r.status_code, r.text); sys.exit(1)
    dash = r.json()
    dash_id = dash["id"]
    print(f"dashboard created id={dash_id}")

    for _, chart_id in charts:
        s.put(f"{BASE}/api/v1/chart/{chart_id}", json={"dashboards": [dash_id]})

    slug = dash.get("result", {}).get("slug")
    print(f"DONE dashboard_id={dash_id} url={BASE}/superset/dashboard/{dash_id}/")


if __name__ == "__main__":
    main()

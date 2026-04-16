from flask import Flask, jsonify, render_template, request

from main import STATES, search_ads

app = Flask(__name__)


@app.route("/")
def index():
    states = [{"code": k, "name": v["nome"]} for k, v in STATES.items()]
    return render_template("index.html", states=states)


@app.route("/api/search")
def api_search():
    query = (request.args.get("q") or "").strip()
    vehicle_type = request.args.get("tipo", "ambos")
    state = request.args.get("state", "sp")
    mps = request.args.get("marketplaces", "olx,ml,webmotors")
    city = (request.args.get("city") or "").strip()
    try:
        radius_km = float(request.args.get("radius", 0))
    except ValueError:
        radius_km = 0
    try:
        limit = max(1, min(int(request.args.get("limit", 15)), 40))
    except ValueError:
        limit = 15

    if vehicle_type not in ("carros", "motos", "ambos"):
        vehicle_type = "ambos"
    marketplaces = [m.strip() for m in mps.split(",") if m.strip() in ("olx", "ml", "webmotors")]
    if not marketplaces:
        marketplaces = ["olx", "ml", "webmotors"]

    if not query:
        return jsonify({"query": "", "results": []})

    try:
        results = search_ads(
            query=query,
            vehicle_type=vehicle_type,
            state=state,
            limit=limit,
            marketplaces=marketplaces,
            city=city,
            radius_km=radius_km,
        )
    except Exception as e:
        return jsonify({"query": query, "error": str(e), "results": []}), 500

    return jsonify({"query": query, "tipo": vehicle_type, "count": len(results), "results": results})


@app.route("/api/debug")
def api_debug():
    import traceback
    results = {}
    # Test curl_cffi
    try:
        from curl_cffi import requests as cr
        s = cr.Session(impersonate="chrome120")
        r = s.get("https://httpbin.org/ip", timeout=10)
        results["curl_cffi"] = {"status": r.status_code, "ip": r.json().get("origin", "?")}
    except Exception as e:
        results["curl_cffi"] = {"error": str(e), "trace": traceback.format_exc()[-500:]}
    # Test OLX
    try:
        from main import make_session, fetch
        html = fetch(make_session(), "https://www.olx.com.br/autos-e-pecas/carros-vans-e-utilitarios/estado-sp")
        results["olx"] = {"ok": html is not None, "len": len(html) if html else 0, "blocked": "Attention Required" in (html or "")}
    except Exception as e:
        results["olx"] = {"error": str(e)}
    # Test ML
    try:
        html = fetch(make_session(), "https://carros.mercadolivre.com.br/honda-civic")
        results["ml"] = {"ok": html is not None, "len": len(html) if html else 0}
    except Exception as e:
        results["ml"] = {"error": str(e)}
    # Test WM
    try:
        from main import WM_HEADERS
        html = fetch(make_session(), "https://www.webmotors.com.br/api/search/car?url=https%3A%2F%2Fwww.webmotors.com.br%2Fcarros%2Festoque%3Fmarca1%3DHONDA%26modelo1%3DCIVIC&actualPage=1&displayPerPage=1", headers=WM_HEADERS)
        results["webmotors"] = {"ok": html is not None, "len": len(html) if html else 0}
    except Exception as e:
        results["webmotors"] = {"error": str(e)}
    return jsonify(results)


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5055)

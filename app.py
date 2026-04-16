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


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5055)

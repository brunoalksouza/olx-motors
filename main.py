import argparse
import datetime as dt
import difflib
import json
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from urllib.parse import quote_plus

import pandas as pd
import requests
from curl_cffi import requests as cr
from tqdm import tqdm


FIPE_BASE = "https://parallelum.com.br/fipe/api/v1"
IMPERSONATE = "chrome120"

VEHICLE_CONFIGS = {
    "carros": {
        "olx_path": "autos-e-pecas/carros-vans-e-utilitarios",
        "fipe_type": "carros",
        "ml_path": "carros",
        "wm_tipo": "carros",
    },
    "motos": {
        "olx_path": "autos-e-pecas/motos",
        "fipe_type": "motos",
        "ml_path": "motos",
        "wm_tipo": "motos",
    },
}

STATES = {
    "sp": {"nome": "São Paulo", "ml": "sao-paulo", "wm": "SP - São Paulo"},
    "rj": {"nome": "Rio de Janeiro", "ml": "rio-de-janeiro", "wm": "RJ - Rio de Janeiro"},
    "mg": {"nome": "Minas Gerais", "ml": "minas-gerais", "wm": "MG - Minas Gerais"},
    "pr": {"nome": "Paraná", "ml": "parana", "wm": "PR - Paraná"},
    "rs": {"nome": "Rio Grande do Sul", "ml": "rio-grande-do-sul", "wm": "RS - Rio Grande do Sul"},
    "sc": {"nome": "Santa Catarina", "ml": "santa-catarina", "wm": "SC - Santa Catarina"},
    "ba": {"nome": "Bahia", "ml": "bahia", "wm": "BA - Bahia"},
    "go": {"nome": "Goiás", "ml": "goias", "wm": "GO - Goiás"},
    "pe": {"nome": "Pernambuco", "ml": "pernambuco", "wm": "PE - Pernambuco"},
    "ce": {"nome": "Ceará", "ml": "ceara", "wm": "CE - Ceará"},
    "df": {"nome": "Distrito Federal", "ml": "distrito-federal", "wm": "DF - Distrito Federal"},
    "es": {"nome": "Espírito Santo", "ml": "espirito-santo", "wm": "ES - Espírito Santo"},
    "pa": {"nome": "Pará", "ml": "para", "wm": "PA - Pará"},
    "ma": {"nome": "Maranhão", "ml": "maranhao", "wm": "MA - Maranhão"},
    "ms": {"nome": "Mato Grosso do Sul", "ml": "mato-grosso-do-sul", "wm": "MS - Mato Grosso do Sul"},
    "mt": {"nome": "Mato Grosso", "ml": "mato-grosso", "wm": "MT - Mato Grosso"},
    "am": {"nome": "Amazonas", "ml": "amazonas", "wm": "AM - Amazonas"},
    "pb": {"nome": "Paraíba", "ml": "paraiba", "wm": "PB - Paraíba"},
    "rn": {"nome": "Rio Grande do Norte", "ml": "rio-grande-do-norte", "wm": "RN - Rio Grande do Norte"},
    "al": {"nome": "Alagoas", "ml": "alagoas", "wm": "AL - Alagoas"},
    "pi": {"nome": "Piauí", "ml": "piaui", "wm": "PI - Piauí"},
    "se": {"nome": "Sergipe", "ml": "sergipe", "wm": "SE - Sergipe"},
    "ro": {"nome": "Rondônia", "ml": "rondonia", "wm": "RO - Rondônia"},
    "to": {"nome": "Tocantins", "ml": "tocantins", "wm": "TO - Tocantins"},
    "ac": {"nome": "Acre", "ml": "acre", "wm": "AC - Acre"},
    "ap": {"nome": "Amapá", "ml": "amapa", "wm": "AP - Amapá"},
    "rr": {"nome": "Roraima", "ml": "roraima", "wm": "RR - Roraima"},
}


def make_session() -> cr.Session:
    return cr.Session(impersonate=IMPERSONATE)


# ──────────────────────── Geocoding ────────────────────────

_GEO_CACHE: dict = {}
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "OLXMotors/1.0 (vehicle search tool)"}


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def geocode(city: str, state: str = "") -> Optional[tuple]:
    key = f"{city},{state}".lower().strip()
    if key in _GEO_CACHE:
        return _GEO_CACHE[key]
    q = f"{city}, {state}, Brazil" if state else f"{city}, Brazil"
    try:
        r = requests.get(NOMINATIM_URL, params={"q": q, "format": "json", "limit": 1}, headers=NOMINATIM_HEADERS, timeout=10)
        data = r.json()
        if data:
            coords = (float(data[0]["lat"]), float(data[0]["lon"]))
            _GEO_CACHE[key] = coords
            return coords
    except Exception:
        pass
    _GEO_CACHE[key] = None
    return None


def filter_by_radius(rows: list, city: str, radius_km: float) -> list:
    origin = geocode(city)
    if not origin:
        return rows
    out = []
    for r in rows:
        rc = r.get("Cidade", "")
        rs = r.get("Estado", "")
        if not rc:
            r["Distancia"] = None
            out.append(r)
            continue
        loc = geocode(rc, rs)
        if not loc:
            r["Distancia"] = None
            out.append(r)
            continue
        dist = _haversine(origin[0], origin[1], loc[0], loc[1])
        r["Distancia"] = round(dist)
        if dist <= radius_km:
            out.append(r)
    return out


def fetch(session: cr.Session, url: str, retries: int = 2, **kwargs) -> Optional[str]:
    for attempt in range(retries + 1):
        try:
            r = session.get(url, timeout=30, **kwargs)
            if r.status_code == 200 and "Attention Required" not in r.text:
                return r.text
        except Exception:
            pass
        if attempt < retries:
            time.sleep(2 + attempt * 2)
    return None


def parse_price(value) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value).strip()
    if "," in s:
        s = s.split(",")[0]
    digits = re.sub(r"[^\d]", "", s)
    return int(digits) if digits else None


def _clean_records(records: list) -> list:
    out = []
    for r in records:
        clean = {}
        for k, v in r.items():
            if isinstance(v, float) and math.isnan(v):
                clean[k] = None
            elif hasattr(v, "isoformat"):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        out.append(clean)
    return out


def _compute_rate(price, fipe):
    p = parse_price(price)
    f = parse_price(fipe)
    if p and f and f > p:
        return int(((f - p) / f) * 100)
    return 0


# ──────────────────────── OLX ────────────────────────

OG_IMAGE_RE = re.compile(r'<meta[^>]*property="og:image"[^>]*content="([^"]+)"', re.I)
AD_HREF_RE = re.compile(r'https://[a-z]{2}\.olx\.com\.br/[^"\'\s<>]+-\d{8,}')


def _olx_extract(html: str) -> Optional[dict]:
    m = re.search(r"window\.dataLayer\s*=\s*\[", html)
    if not m:
        return None
    try:
        arr, _ = json.JSONDecoder().raw_decode(html[m.end() - 1:])
    except (ValueError, json.JSONDecodeError):
        return None
    if not arr:
        return None
    page = (arr[0] or {}).get("page", {})
    img_m = OG_IMAGE_RE.search(html)
    return {
        "ad": page.get("adDetail") or {},
        "detail": page.get("detail") or {},
        "image": img_m.group(1) if img_m else "",
    }


def _olx_normalize(payload: dict, vehicle_type: str) -> Optional[dict]:
    ad = payload.get("ad") or {}
    detail = payload.get("detail") or {}
    if not ad:
        return None
    brand = ad.get("brand", "")
    model = ad.get("model", "")
    version = ad.get("version", "")
    model_full = " ".join(x for x in (model, version) if x).strip()
    epoch = detail.get("adDate", 0)
    try:
        date_str = dt.datetime.fromtimestamp(int(epoch)).isoformat() if int(epoch) > 0 else ""
    except (TypeError, ValueError):
        date_str = ""
    return {
        "Titulo": ad.get("subject", ""),
        "Preco": parse_price(ad.get("price") or detail.get("price")),
        "Fabricante": brand,
        "Modelo": model_full,
        "Ano": re.search(r"\d{4}", str(ad.get("regdate", ""))).group(0) if re.search(r"\d{4}", str(ad.get("regdate", ""))) else "",
        "Km Rodado": parse_price(ad.get("mileage")),
        "Cambio": ad.get("gearbox", "") or "",
        "Combustivel": ad.get("fuel") or ad.get("motorcycle_fuel", ""),
        "Cor": ad.get("carcolor") or ad.get("color", ""),
        "Foto": payload.get("image", ""),
        "Cidade": ad.get("municipality", ""),
        "Estado": ad.get("state", ""),
        "Dates": date_str,
        "Categoria": vehicle_type,
        "Marketplace": "OLX",
    }


def _olx_fetch_one(url, vehicle_type):
    session = make_session()
    html = fetch(session, url)
    if not html:
        return None
    payload = _olx_extract(html)
    if not payload:
        return None
    row = _olx_normalize(payload, vehicle_type)
    if row:
        row["Link"] = url
    return row


def search_olx(query: str, vehicle_type: str, state: str, limit: int) -> list:
    cfg = VEHICLE_CONFIGS[vehicle_type]
    session = make_session()
    qs = f"?q={quote_plus(query)}" if query else ""
    url = f"https://www.olx.com.br/{cfg['olx_path']}/estado-{state}{qs}"
    html = fetch(session, url)
    if not html:
        return []
    links = list(dict.fromkeys(AD_HREF_RE.findall(html)))[:limit]
    rows = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(_olx_fetch_one, u, vehicle_type) for u in links]
        for f in futs:
            r = f.result()
            if r and r.get("Preco"):
                rows.append(r)
    return rows


# ──────────────────────── Mercado Livre ────────────────────────

def _ml_parse_initial_state(html: str) -> Optional[dict]:
    marker = '"initialState":'
    idx = html.find(marker)
    if idx < 0:
        return None
    start = idx + len(marker)
    depth = 0
    i = start
    while i < len(html):
        c = html[i]
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(html[start:i + 1])
                except json.JSONDecodeError:
                    return None
        elif c == '"':
            i += 1
            while i < len(html) and html[i] != '"':
                if html[i] == '\\':
                    i += 1
                i += 1
        i += 1
    return None


def _ml_parse_jsonld_graph(html: str) -> list:
    items = []
    for m in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S):
        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError:
            continue
        graph = data.get("@graph") if isinstance(data, dict) else None
        if graph:
            for item in graph:
                if item.get("@type") == "Product":
                    items.append(item)
    return items


ML_SUBDOMAIN = {"carros": "carros", "motos": "motos"}


def search_ml(query: str, vehicle_type: str, state: str, limit: int) -> list:
    sub = ML_SUBDOMAIN.get(vehicle_type, "carros")
    slug = query.lower().replace(" ", "-")
    url = f"https://{sub}.mercadolivre.com.br/{slug}"
    session = make_session()
    html = fetch(session, url)
    if not html:
        return []

    jsonld_items = _ml_parse_jsonld_graph(html)
    jsonld_map = {}
    for it in jsonld_items:
        offers = it.get("offers") or {}
        item_url = offers.get("url", "")
        mlb_m = re.search(r"MLB-?(\d+)", item_url)
        if mlb_m:
            jsonld_map[mlb_m.group(1)] = it

    state_obj = _ml_parse_initial_state(html)
    results = (state_obj or {}).get("results") or []
    rows = []
    for entry in results:
        poly = entry.get("polycard") or {}
        meta = poly.get("metadata") or {}
        if meta.get("is_pad") == "true":
            continue
        mlb_id = (meta.get("id") or "").replace("MLB", "")
        ad_url = meta.get("url", "")
        if not ad_url:
            continue

        title = ""
        price = None
        year = ""
        km = None
        location = ""
        for comp in poly.get("components") or []:
            ctype = comp.get("type", "")
            if ctype == "title":
                title = (comp.get("title") or {}).get("text", "")
            elif ctype == "price":
                cp = (comp.get("price") or {}).get("current_price") or {}
                price = cp.get("value")
            elif ctype == "attributes_list":
                texts = (comp.get("attributes_list") or {}).get("texts") or []
                if texts:
                    ym = re.search(r"\d{4}", texts[0])
                    year = ym.group(0) if ym else ""
                if len(texts) > 1:
                    kmm = re.search(r"([\d.]+)\s*[Kk]m", texts[1])
                    if kmm:
                        km = int(kmm.group(1).replace(".", ""))
            elif ctype == "location":
                location = (comp.get("location") or {}).get("text", "")

        ld = jsonld_map.get(mlb_id, {})
        brand = (ld.get("brand") or {}).get("name", "")
        image = ld.get("image", "")
        if not brand and title:
            brand = title.split()[0] if title.split() else ""

        cidade = ""
        estado = ""
        if location:
            parts = [p.strip() for p in location.split("-")]
            if len(parts) >= 2:
                cidade = parts[0]
                estado = parts[1]
            else:
                cidade = location

        p = parse_price(price)
        if not p:
            continue
        rows.append({
            "Titulo": title,
            "Preco": p,
            "Fabricante": brand,
            "Modelo": title.replace(brand, "").strip() if brand else title,
            "Ano": year,
            "Km Rodado": km,
            "Cambio": "",
            "Combustivel": "",
            "Cor": "",
            "Foto": image,
            "Cidade": cidade,
            "Estado": estado,
            "Dates": "",
            "Categoria": vehicle_type,
            "Marketplace": "Mercado Livre",
            "Link": ad_url if ad_url.startswith("http") else f"https://{ad_url}",
        })
        if len(rows) >= limit:
            break
    return rows


# ──────────────────────── Webmotors ────────────────────────

WM_HEADERS = {
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    ),
    "Sec-Ch-Ua-Mobile": "?1",
    "Sec-Ch-Ua-Platform": '"Android"',
}


def search_webmotors(query: str, vehicle_type: str, state: str, limit: int) -> list:
    cfg = VEHICLE_CONFIGS[vehicle_type]
    state_info = STATES.get(state)
    parts = query.strip().upper().split(None, 1)
    marca = parts[0] if parts else ""
    modelo = parts[1] if len(parts) > 1 else ""
    params = [f"tipoveiculo={cfg['wm_tipo']}"]
    if marca:
        params.append(f"marca1={quote_plus(marca)}")
    if modelo:
        params.append(f"modelo1={quote_plus(modelo)}")
    if state_info:
        params.append(f"estadocidade={quote_plus(state_info['wm'])}")
    inner_url = "https://www.webmotors.com.br/carros/estoque?" + "&".join(params)
    api_url = (
        f"https://www.webmotors.com.br/api/search/car"
        f"?url={quote_plus(inner_url)}&actualPage=1&displayPerPage={limit}"
    )
    session = make_session()
    text = fetch(session, api_url, headers=WM_HEADERS)
    if not text:
        return []
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []
    results = data.get("SearchResults") or []
    rows = []
    for item in results[:limit]:
        spec = item.get("Specification") or {}
        prices = item.get("Prices") or {}
        seller = item.get("Seller") or {}
        media = item.get("Media") or {}
        photos = media.get("Photos") or []

        price = parse_price(prices.get("Price"))
        if not price:
            continue

        fipe_pct = item.get("FipePercent")
        fipe_val = None
        rate_val = 0
        if fipe_pct and fipe_pct > 0:
            fipe_val = int(round(price / fipe_pct * 100))
            rate_val = max(0, 100 - fipe_pct)

        make = (spec.get("Make") or {}).get("Value", "")
        model = (spec.get("Model") or {}).get("Value", "")
        version = (spec.get("Version") or {}).get("Value", "")
        title_parts = [make, model, version]
        title = " ".join(p for p in title_parts if p).strip()

        photo_url = ""
        if photos:
            path = photos[0].get("PhotoPath", "").replace("\\", "/")
            if path:
                photo_url = f"https://image.webmotors.com.br/imagens/prod/ficha{path}" if not path.startswith("http") else path

        uid = item.get("UniqueId", "")
        version_slug = re.sub(r"[^a-z0-9]+", "-", version.lower()).strip("-")
        doors = spec.get("NumberPorts", "4")
        year_model = str(int(spec.get("YearModel", 0))) if spec.get("YearModel") else ""
        condition = "usado" if item.get("ListingType") == "U" else "0km"
        link = (
            f"https://www.webmotors.com.br/comprar/{make.lower()}/{model.lower()}"
            f"/{version_slug}/{doors}-portas/{year_model}/{uid}/{condition}/"
        )

        rows.append({
            "Titulo": title,
            "Preco": price,
            "Fabricante": make,
            "Modelo": f"{model} {version}".strip(),
            "Ano": str(int(spec.get("YearModel", 0))) if spec.get("YearModel") else "",
            "Km Rodado": int(spec["Odometer"]) if spec.get("Odometer") else None,
            "Cambio": spec.get("Transmission", ""),
            "Combustivel": "",
            "Cor": (spec.get("Color") or {}).get("Primary", ""),
            "Foto": photo_url,
            "Cidade": seller.get("City", ""),
            "Estado": seller.get("State", "").split("(")[-1].replace(")", "").strip() if seller.get("State") else "",
            "Dates": "",
            "Categoria": vehicle_type,
            "Marketplace": "Webmotors",
            "Link": link,
            "fipe": fipe_val,
            "rate": rate_val,
        })
    return rows


# ──────────────────────── FIPE Client ────────────────────────

class FipeClient:
    def __init__(self, vehicle_type: str):
        self.type = VEHICLE_CONFIGS[vehicle_type]["fipe_type"]
        self._brands = None
        self._models_cache = {}
        self._years_cache = {}

    def _get(self, path: str):
        url = f"{FIPE_BASE}/{self.type}/{path}"
        for attempt in range(3):
            try:
                r = requests.get(url, timeout=30)
                if r.status_code == 429:
                    time.sleep(30 * (attempt + 1))
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException:
                if attempt == 2:
                    return None
                time.sleep(5)
        return None

    def brands(self):
        if self._brands is None:
            self._brands = self._get("marcas") or []
        return self._brands

    def models(self, brand_code):
        if brand_code not in self._models_cache:
            data = self._get(f"marcas/{brand_code}/modelos") or {}
            self._models_cache[brand_code] = data.get("modelos", []) if isinstance(data, dict) else []
        return self._models_cache[brand_code]

    def years(self, brand_code, model_code):
        key = (brand_code, model_code)
        if key not in self._years_cache:
            self._years_cache[key] = self._get(f"marcas/{brand_code}/modelos/{model_code}/anos") or []
        return self._years_cache[key]

    def price(self, brand_code, model_code, year_code):
        return self._get(f"marcas/{brand_code}/modelos/{model_code}/anos/{year_code}")

    @staticmethod
    def _best_match(items, query, key="nome", id_key="codigo", cutoff=0.55):
        q = (query or "").strip().upper()
        if not q or not items:
            return None
        names = [str(it.get(key, "")).upper() for it in items]
        match = difflib.get_close_matches(q, names, n=1, cutoff=cutoff)
        if not match:
            for i, n in enumerate(names):
                if q in n or n in q:
                    return items[i].get(id_key)
            return None
        idx = names.index(match[0])
        return items[idx].get(id_key)

    def lookup(self, brand, model, year):
        try:
            brand_code = self._best_match(self.brands(), brand)
            if not brand_code:
                return None
            models = self.models(brand_code)
            model_code = self._best_match(models, model)
            if not model_code:
                return None
            years = self.years(brand_code, model_code)
            year_code = self._match_year(years, year)
            if not year_code:
                return None
            return self.price(brand_code, model_code, year_code)
        except Exception:
            return None

    @staticmethod
    def _match_year(years, target):
        try:
            t = int(re.findall(r"\d{4}", str(target))[0])
        except (IndexError, ValueError):
            return None
        gasoline_codes = [y for y in years if str(y.get("codigo", "")).endswith("-1")]
        pool = gasoline_codes or years
        best = None
        best_diff = 1e9
        for y in pool:
            code = str(y.get("codigo", ""))
            m_y = re.match(r"(\d{4})", code)
            if not m_y:
                continue
            diff = abs(int(m_y.group(1)) - t)
            if diff < best_diff:
                best_diff = diff
                best = code
        return best


_FIPE_CLIENTS: dict = {}


def get_fipe_client(vt: str) -> FipeClient:
    if vt not in _FIPE_CLIENTS:
        _FIPE_CLIENTS[vt] = FipeClient(vt)
    return _FIPE_CLIENTS[vt]


def _enrich_fipe(rows: list, vehicle_type: str) -> list:
    needs_fipe = [r for r in rows if r.get("fipe") is None]
    if not needs_fipe:
        return rows
    client = get_fipe_client(vehicle_type)

    def _one(r):
        result = client.lookup(r.get("Fabricante"), r.get("Modelo"), r.get("Ano"))
        if result and "Valor" in result:
            fipe_val = parse_price(result["Valor"])
            r["fipe"] = fipe_val
            r["rate"] = _compute_rate(r.get("Preco"), fipe_val)
        else:
            r["fipe"] = None
            r["rate"] = 0
        return r

    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(_one, needs_fipe))
    return rows


# ──────────────────────── Unified Search ────────────────────────

ALL_MARKETPLACES = ["olx", "ml", "webmotors"]


def search_ads(
    query: str,
    vehicle_type: str = "ambos",
    state: str = "sp",
    limit: int = 15,
    marketplaces: Optional[list] = None,
    city: str = "",
    radius_km: float = 0,
) -> list:
    if not marketplaces:
        marketplaces = ALL_MARKETPLACES
    types = ["carros", "motos"] if vehicle_type == "ambos" else [vehicle_type]
    all_rows = []

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {}
        for vt in types:
            for mp in marketplaces:
                if mp == "olx":
                    futures[pool.submit(search_olx, query, vt, state, limit)] = (mp, vt)
                elif mp == "ml":
                    futures[pool.submit(search_ml, query, vt, state, limit)] = (mp, vt)
                elif mp == "webmotors":
                    futures[pool.submit(search_webmotors, query, vt, state, limit)] = (mp, vt)
        for f in as_completed(futures):
            mp, vt = futures[f]
            try:
                rows = f.result()
            except Exception:
                rows = []
            if rows:
                rows = _enrich_fipe(rows, vt)
                all_rows.extend(rows)

    all_rows = [r for r in all_rows if r.get("Preco")]
    for r in all_rows:
        if not r.get("fipe"):
            r["fipe"] = None
            r["rate"] = 0
    if city and radius_km > 0:
        all_rows = filter_by_radius(all_rows, city, radius_km)
    all_rows.sort(key=lambda x: x.get("rate", 0), reverse=True)
    return _clean_records(all_rows)


# ──────────────────────── CLI ────────────────────────

def run_once(vehicle_types, state, pages, limit, csv_path):
    session = make_session()
    all_df = []
    for vt in vehicle_types:
        cfg = VEHICLE_CONFIGS[vt]
        qs = ""
        url = f"https://www.olx.com.br/{cfg['olx_path']}/estado-{state}{qs}"
        html = fetch(session, url)
        if not html:
            continue
        links = list(dict.fromkeys(AD_HREF_RE.findall(html)))[:limit]
        rows = []
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = [pool.submit(_olx_fetch_one, u, vt) for u in links]
            for f in futs:
                r = f.result()
                if r:
                    rows.append(r)
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df = _enrich_fipe_df(df, vt)
        all_df.append(df)
    if not all_df:
        return
    combined = pd.concat(all_df, ignore_index=True)
    combined = combined.sort_values(by=["rate"], ascending=False).reset_index(drop=True)
    if os.path.exists(csv_path):
        existing = pd.read_csv(csv_path)
        combined = pd.concat([existing, combined], ignore_index=True)
    combined = combined.drop_duplicates(subset=["Link"]).reset_index(drop=True)
    combined.to_csv(csv_path, index=False)
    print(f"Saved {len(combined)} rows to {csv_path}")


def _enrich_fipe_df(df, vt):
    client = get_fipe_client(vt)
    fipe_vals = []
    rates = []
    for _, r in df.iterrows():
        result = client.lookup(r.get("Fabricante"), r.get("Modelo"), r.get("Ano"))
        fv = parse_price(result.get("Valor", "")) if result else None
        fipe_vals.append(fv)
        rates.append(_compute_rate(r.get("Preco"), fv) if fv else 0)
    df["fipe"] = fipe_vals
    df["rate"] = rates
    return df[df["fipe"].notna() & (df["fipe"] > 0)].reset_index(drop=True)


def parse_args():
    parser = argparse.ArgumentParser(description="OLX + ML + Webmotors vehicle search")
    parser.add_argument("--vehicle-type", choices=["carros", "motos", "ambos"], default="ambos")
    parser.add_argument("--state", default="sp")
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--csv", default="data.csv")
    parser.add_argument("--loop", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    types = ["carros", "motos"] if args.vehicle_type == "ambos" else [args.vehicle_type]
    while True:
        run_once(types, args.state, args.pages, args.limit, args.csv)
        if not args.loop:
            break


if __name__ == "__main__":
    main()

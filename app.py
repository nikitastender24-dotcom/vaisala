#  app.py  –  один файл, который делает всё, что вы просили
# ----------------------------------------------------------------------
import os
import time
import json
import socket
import threading
from datetime import datetimefrom threading import Lock

import requests
from flask import Flask, Response, jsonify, send_filefrom flask_cors import CORS

# --------------------------  НАСТРОЙКИ  ---------------------------
SOURCE_URL = "https://tiles.wo-cloud.com/live?channels=lightning-nowcast,lightning-vaisala"
ARCHIVE_FILE = "lightning_archive.json"      # будет лежать рядом со скриптом
MAX_AGE_SECONDS = 7200                       # 2 часа
MAX_POINTS = 80000                           # максимум точек в памяти
DELETE_FILE_INTERVAL = 20 * 60                # 20 минут (в секундах)
# -------------------------------------------------------------------

app = Flask(__name__)
CORS(app)                                    # разрешаем запросы с любого домена

# --------------------------  ГЛОБАЛЬНЫЕ ДАННЫЕ  --------------------------
lightning_archive = {}                       # {(lat, lng): {'time': ..., 'lat': ..., 'lng': ...}}
lock = Lock()

# --------------------------  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ  --------------------------
def load_archive():
    """Читает JSON‑файл в память (если он есть)."""
    global lightning_archive
    if not os.path.exists(ARCHIVE_FILE):
        return
    try:
        with open(ARCHIVE_FILE, "r") as f:
            data = json.load(f)
        lightning_archive = {}
        for key, val in data.items():
            lat, lng = map(float, key.split(","))
            lightning_archive[(lat, lng)] = val
        print(f"✅ Загружено {len(lightning_archive)} точек из {ARCHIVE_FILE}")
    except Exception as e:
        print(f"❌ Ошибка загрузки архива: {e}")

def save_archive():
    """Записывает текущий словарь в файл."""
    try:
        out = {f"{lat},{lng}": v for (lat, lng), v in lightning_archive.items()}
        with open(ARCHIVE_FILE, "w") as f:
            json.dump(out, f)
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")

def delete_archive_file():
    """Удаляет файл архива, если он существует."""
    try:
        if os.path.exists(ARCHIVE_FILE):
            os.remove(ARCHIVE_FILE)
            print("🗑️ Файл архива удалён")
    except Exception as e:
        print(f"❌ Ошибка удаления файла: {e}")

def clean_old():
    """Удаляет точки старше MAX_AGE_SECONDS (2 ч)."""
    with lock:
        now = time.time()
        to_del = [k for k, v in lightning_archive.items()
                  if now - v["time"] > MAX_AGE_SECONDS]
        for k in to_del:
            del lightning_archive[k]
        if to_del:
            print(f"🧹 Удалено {len(to_del)} старых точек")
            save_archive()

def check_limit():
    """Если точек больше MAX_POINTS – удаляем самые старые."""
    global lightning_archive
    deleted = 0
    while len(lightning_archive) > MAX_POINTS:
        oldest_key = None
        oldest_time = float("inf")
        for k, v in lightning_archive.items():
            t = v.get("time", float("inf"))
            if t < oldest_time:
                oldest_time = t
                oldest_key = k
        if oldest_key:
            del lightning_archive[oldest_key]
            deleted += 1
        else:
            break
    if deleted:
        print(f"⚠️ Удалено по лимиту: {deleted} точек")
        save_archive()

def clean_old_and_save():
    """Удобный wrapper, который одновременно очищает и сохраняет."""
    clean_old()

def periodic_file_deletion():
    """Отдельный daemon‑поток: каждые 20 мин удаляем файл и чистим словарь."""
    while True:
        time.sleep(DELETE_FILE_INTERVAL)
        with lock:
            delete_archive_file()
            lightning_archive.clear()
            print("🔄 Автоматическая очистка архива (каждые 20 мин) выполнена")
        # Файл уже удалён, но словарь в памяти пустой – дальше будем собирать заново

# --------------------------  СБОРЩИК МОЛНИЙ  --------------------------
def background_lightning_collector():
    """
    Бесконечный поток, который постоянно получает данные от
    https://tiles.wo-cloud.com и кладёт их в lightning_archive.
    """
    print("=" * 70)
    print("🔥 ФОНОВОЙ СБОРЩИК МОЛНИЙ ЗАПУЩЕН")
    print("=" * 70)

    session = requests.Session()
    session.trust_env = False                # игнорируем системный прокси
    session.proxies = {"http": None, "https": None}

    while True:
        try:
            print("\n📡 Подключаемся к источнику данных...")
            resp = session.get(SOURCE_URL, stream=True, timeout=15)
            if resp.status_code != 200:
                print(f"❌ HTTP {resp.status_code} – повтор через 5 сек")
                time.sleep(5)
                continue

            print("✅ Подключено! Ждём события lightning‑vaisala…")
            buffer = ""

            while True:
                chunk = resp.iter_content(1024)
                if not chunk:
                    break
                buffer += chunk.decode("utf-8", errors="ignore")

                while "\n\n" in buffer:
                    event, buffer = buffer.split("\n\n", 1)
                    if "lightning-vaisala" in event:
                        try:
                            # Берём строку после "data: "
                            data_line = None
                            for line in event.split("\n"):
                                if line.startswith("data: "):
                                    data_line = line[6:]
                                    break
                            if not data_line:
                                continue                            data = json.loads(data_line)

                            if data and "lightning" in data:
                                now_ts = time.time()
                                with lock:
                                    for strike in data["lightning"]:
                                        if isinstance(strike, list) and len(strike) >= 2:
                                            lat, lng = float(strike[0]), float(strike[1])
                                            key = (lat, lng)
                                            # Перезаписываем/добавляем точку
                                            lightning_archive[key] = {
                                                "time": now_ts,
                                                "lat": lat,
                                                "lng": lng,
                                            }
                                    check_limit()
                                # Сохраняем каждые 10 сек (чтобы не переполнять RAM)
                                save_archive()
                        except Exception as e:
                            print(f"⚠️ Ошибка парсинга события: {e}")

                # Периодически проверяем «живость» соединения
                if time.time() % 60 < 1:
                    print(f"💓 Соединение активно | Архив: {len(lightning_archive)} точек")
        except requests.exceptions.ProxyError:
            print("⚠️ Прокси‑ошибка. Возможно, ваш VPS блокирует выход. Ожидаем 10 сек…")
            time.sleep(10)
        except requests.exceptions.SSLError as e:
            print(f"⚠️ SSL‑ошибка: {e}. Ожидаем 10 сек…")
            time.sleep(10)
        except requests.exceptions.ConnectionError:
            print("🔌 Потеря связи. Ожидаем 10 сек…")
            time.sleep(10)
        except Exception as e:
            print(f"❌ Неожиданная ошибка: {e}. Ожидаем 10 сек…")
            time.sleep(10)

# --------------------------  Flask‑роуты  --------------------------
@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_html(path):
    """
    Возвращаем ваш HTML‑клиент (файл index.html) по корневому URL.
    Если хотите отдавать отдельные static‑файлы – просто положите их рядом и
    используйте send_from_directory, но для простоты отправляем всё как строку.
    """
    # HTML‑шаблон – вставляем как тройные кавычки
    html_template = """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no" />
        <title>⚡ Грозы — plus canvas</title>
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
        <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            html, body { width: 100%; height: 100%; overflow: hidden; background: #000; }
            body { font-family: monospace; }
            #map { width: 100vw; height: 100vh; }

            .info-panel {
                position: absolute;
                top: 20px;
                right: 20px;
                background: rgba(0,0,0,0.8);
                padding: 10px 15px;
                border-radius: 8px;
                color: white;
                z-index: 1000;
                pointer-events: none;
                border-left: 3px solid #ff6600;
                font-size: 12px;
            }

            .count { font-size: 24px; font-weight: bold; }

            .legend {
                position: absolute;
                bottom: 20px;
                right: 20px;
                background: rgba(0,0,0,0.7);
                padding: 8px 12px;
                border-radius: 6px;
                font-size: 10px;
                color: white;
                z-index: 1000;
                pointer-events: none;
            }

            .legend span { display: inline-block; width: 20px; text-align: center; }

            button {
                position: absolute;
                bottom: 20px;
                left: 20px;
                background: rgba(0,0,0,0.7);
                border: 1px solid #ff6600;
                color: white;
                padding: 6px 12px;
                border-radius: 6px;
                cursor: pointer;
                z-index: 1000;
                margin-right: 5px;
            }

            button:hover { background: #ff6600; }

            button.danger { border-color: red; }
            button.danger:hover { background: red; }

            .leaflet-lightning-layer { pointer-events: none; }
        </style>
    </head>
    <body>
        <div id="map"></div>

        <div class="info-panel">
            <div>⚡ ГРОЗЫ (все плюсики)</div>
            <div class="count" id="strikeCount">—</div>
            <div id="statusText">Загрузка…</div>
        </div>

        <div class="legend">
            <div><span style="color:#fff">✚</span> 0-10мин</div>
            <div><span style="color:#ff0">✚</span> 10-20</div>
            <div><span style="color:#ffa500">✚</span> 20-30</div>
            <div><span style="color:#ff4500">✚</span> 30-50</div>
            <div><span style="color:#f00">✚</span> 50-60</div>
            <div><span style="color:#8b0000">✚</span> 60-90</div>
            <div><span style="color:#4a0000">✚</span> 90-120</div>
        </div>

        <div class="controls" style="position:absolute; bottom:20px; left:20px; z-index:1000;">
            <button id="refreshBtn">🔄 Обновить</button>
            <button id="clearBtn" class="danger">🗑 Сброс</button>
        </div>

        <script>
            const PROXY_URL = '{{PROXY_URL}}';   // будет подставлено сервером

            const map = L.map('map', {preferCanvas:true, zoomControl:true}).setView([20,0],2);
            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',{
                attribution:'&copy; CartoDB, OSM',
                subdomains:'abcd',
                maxZoom:19
            }).addTo(map);

            const strikeStore = new Map();

            const LightningCanvasLayer = L.Layer.extend({
                initialize:function(){this._canvas=L.DomUtil.create('canvas','leaflet-lightning-layer');
                                    this._ctx=this._canvas.getContext('2d',{alpha:true});this._redrawScheduled=false;}
                onAdd:function(m){this._map=m;m.getPanes().overlayPane.appendChild(this._canvas);
                                 m.on('move zoom resize viewreset zoomend moveend',this._reset,this);this._reset();}
                onRemove:function(m){L.DomUtil.remove(this._canvas);
                                    m.off('move zoom resize viewreset zoomend moveend',this._reset,this);}
                _reset:function(){const s=m.getSize();const tl=m.containerPointToLayerPoint([0,0]);
                                 L.DomUtil.setPosition(this._canvas,tl);
                                 const dpr=window.devicePixelRatio||1;this._canvas.width=s.x*dpr;
                                 this._canvas.height=s.y*dpr;this._canvas.style.width=s.x+'px';
                                 this._canvas.style.height=s.y+'px';
                                 this._ctx.setTransform(1,0,0,1,0,0);this._ctx.scale(dpr,dpr);this.redraw();}
                redraw:function(){if(this._redrawScheduled)return;this._redrawScheduled=true;
                                 requestAnimationFrame(()=>{this._redrawScheduled=false;this._draw();});}
                _draw:function(){const ctx=this._ctx;const mp=this._map;const sz=mp.getSize();ctx.clearRect(0,0,sz.x,sz.y);
                                 const bounds=mp.getBounds().pad(0.3);let plusSize=4;const z=mp.getZoom();
                                 if(z>=8)plusSize=8;else if(z>=6)plusSize=7;else if(z>=4)plusSize=6;else if(z>=2)plusSize=5;
                                 const groups=new Map();for(const s of strikeStore.values()){
                                     if(!bounds.contains([s.lat,s.lng]))continue;
                                     const p=mp.latLngToContainerPoint([s.lat,s.lng]);
                                     if(p.x<-10||p.y<-10||p.x>sz.x+10||p.y>sz.y+10)continue;
                                     if(!groups.has(s.color))groups.set(s.color,[]);
                                     groups.get(s.color).push([Math.round(p.x),Math.round(p.y)]);}
                                 ctx.lineWidth=1;ctx.lineCap='square';
                                 for(const [col,pts] of groups){ctx.strokeStyle=col;ctx.beginPath();const h=Math.floor(plusSize/2);
                                 for(const [x,y] of pts){ctx.moveTo(x-h,y);ctx.lineTo(x+h,y);
                                 ctx.moveTo(x,y-h);ctx.lineTo(x,y+h);}
                                 ctx.stroke();}
            });

            const lightningLayer=new LightningCanvasLayer();lightningLayer.addTo(map);

            async function reloadArchive(){
                try{
                    document.getElementById('statusText').innerText='Обновление…';
                    const resp=await fetch('{{PROXY_URL}}/api/archive');
                    const data=await resp.json();
                    strikeStore.clear();
                    for(const s of data.strikes){
                        const key=`${s.lat.toFixed(4)},${s.lng.toFixed(4)}`;
                        strikeStore.set(key,{lat:s.lat,lng:s.lng,color:s.color,age_min:s.age_min});
                    }
                    document.getElementById('strikeCount').textContent=strikeStore.size;
                    document.getElementById('statusText').textContent=`Обновлено ${new Date().toLocaleTimeString()}`;
                    lightningLayer.redraw();
                }catch(e){console.error(e);document.getElementById('statusText').textContent='Ошибка';}
            }

            function addNewStrikes(arr){
                if(!arr||!arr.length)return;
                let added=0;
                for(const s of arr){
                    if(!Array.isArray(s)||s.length<2)continue;
                    const lat=s[0],lng=s[1];
                    const key=`${lat.toFixed(4)},${lng.toFixed(4)}`;
                    if(!strikeStore.has(key)){
                        strikeStore.set(key,{lat,lng,color:'#ffffff',age_min:0});
                        added++;
                    }
                }
                if(added){
                    document.getElementById('strikeCount').textContent=strikeStore.size;
                    document.getElementById('statusText').textContent=`+${added} новых (всего ${strikeStore.size})`;
                    lightningLayer.redraw();
                    setTimeout(()=>{document.getElementById('statusText').textContent=`${strikeStore.size} разрядов`;},3000);
                }
            }

            function clearAllMarkers(){
                strikeStore.clear();lightningLayer.redraw();
                document.getElementById('strikeCount').textContent='0';
                document.getElementById('statusText').textContent='Сброс выполнен';
            }

            let evtSource=null;
            function connectSSE(){
                if(evtSource)evtSource.close();
                evtSource=new EventSource('{{PROXY_URL}}/lightning-stream');
                evtSource.addEventListener('lightning-vaisala',e=>{
                    try{
                        const data=JSON.parse(e.data);
                        if(data&&data.lightning)addNewStrikes(data.lightning);
                    }catch(e){}
                });
                evtSource.onerror=()=>{console.log('SSE error');if(evtSource)evtSource.close();
                                        setTimeout(connectSSE,5000);};
            }

            document.getElementById('refreshBtn').onclick=reloadArchive;
            document.getElementById('clearBtn').onclick=clearAllMarkers;
            setInterval(reloadArchive,60000);
            window.addEventListener('resize',()=>{map.invalidateSize();lightningLayer._reset();});
            reloadArchive();connectSSE();
        </script>
    </body>
    </html>
    """
    # Подставляем URL, по которому клиент будет обращаться к API
    return html_template.replace("{{PROXY_URL}}", request.host_url.rstrip("/"))

@app.route("/lightning-stream")
def stream():
    """SSE‑поток, который отдает события lightning‑vaisala."""
    def generate():
        try:
            resp = requests.get(SOURCE_URL, stream=True, timeout=30)
            if resp.status_code != 200:
                yield f"event: error\ndata: HTTP {resp.status_code}\n\n"
                return
            buffer = ""
            for chunk in resp.iter_content(1024):
                if not chunk:
                    continue
                buffer += chunk.decode("utf-8", errors="ignore")
                while "\n\n" in buffer:
                    event, buffer = buffer.split("\n\n", 1)
                    if "lightning-vaisala" in event:
                        yield event + "\n\n"
        except Exception as e:
            yield f"event: error\ndata: {e}\n\n"
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache"})

@app.route("/api/archive")
def get_archive():
    """Возвращаем готовый набор точек для карты (цвета уже рассчитаны)."""
    clean_old()
    with lock:
        now = time.time()
        result = []
        for (lat, lng), data in lightning_archive.items():
            age_min = (now - data["time"]) / 60.0
            if age_min > 120:          # слишком старые – игнорируем
                continue
            if age_min <= 10:   color = "#ffffff"
            elif age_min <= 20: color = "#ffff00"
            elif age_min <= 30: color = "#ffa500"
            elif age_min <= 50: color = "#ff4500"
            elif age_min <= 60: color = "#ff0000"
            elif age_min <= 90: color = "#8b0000"
            else:               color = "#4a0000"
            result.append({
                "lat": lat,
                "lng": lng,
                "color": color,
                "age_min": round(age_min, 1)
            })
    return jsonify({"strikes": result, "count": len(result)})

@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200

# --------------------------  ОЧИСТКА ФАЙЛА  --------------------------
def periodic_file_deletion():
    """Фоновый поток, который каждые 20 мин удаляет файл и очищает dict."""
    while True:
        time.sleep(DELETE_FILE_INTERVAL)
        with lock:
            delete_archive_file()
            lightning_archive.clear()
            print("🔄 Автоматическая очистка архива (каждые 20 мин) выполнена")

# --------------------------  ТЕСТОВЫЙ ПОРТ  --------------------------
# Если вы хотите, чтобы сервер слушал *другой* порт (например, 5000), просто
# измените переменную ниже. На обычном локальном запуске 5000 – нормально.
PORT = int(os.getenv("PORT", 5000))

if __name__ == "__main__":
    # 1️⃣ Загружаем уже сохранённый архив
    load_archive()

    # 2️⃣ Запускаем фоновые потоки
    threading.Thread(target=background_lightning_collector, daemon=True).start()
    threading.Thread(target=periodic_file_deletion, daemon=True).start()

    # 3️⃣ Запускаем Flask через WSGI‑сервер (для локального теста просто app.run)
    print("\n" + "=" * 70)
    print(f"⚡  Lightning‑collector запущен на http://0.0.0.0:{PORT}")
    print("✅ Файл архива:", ARCHIVE_FILE)
    print("✅ Авто‑очистка каждую 20‑минуту включена")
    print("=" * 70 + "\n")

    # Если вы на локальном ПК – обычный run подойдет.
    # На PythonAnywhere WSGI‑сервер будет вызывать `app` из `wsgi.py`
    # (см. ниже, если захотите деплоить). Для простоты:
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)

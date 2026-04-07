from flask import Flask, Response, jsonify
from flask_cors import CORS
import requests
import json
import time
import os
from threading import Lock, Thread
from queue import Queue

app = Flask(__name__)

CORS(app, resources={
    r"/*": {
        "origins": "*",
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type"]
    }
})

SOURCE_URL = "https://tiles.wo-cloud.com/live?channels=lightning-nowcast,lightning-vaisala"
ARCHIVE_FILE = "lightning_archive.json"
MAX_AGE_SECONDS = 3600  # 1 час
MAX_POINTS = 80000
DELETE_FILE_INTERVAL = 1200  # 20 минут
SAVE_INTERVAL = 30

lightning_archive = {}
lock = Lock()
collector_running = False

# Очередь для отправки новых меток всем SSE-клиентам
new_strikes_queue = Queue()

def load_archive():
    global lightning_archive
    if not os.path.exists(ARCHIVE_FILE):
        print("📂 Файл архива не найден, начинаем с пустого")
        return
    try:
        with open(ARCHIVE_FILE, 'r') as f:
            data = json.load(f)
        lightning_archive = {}
        for key, value in data.items():
            lat, lng = map(float, key.split(','))
            lightning_archive[(lat, lng)] = value
        print(f"✅ Загружено {len(lightning_archive)} разрядов из файла")
    except Exception as e:
        print(f"❌ Ошибка загрузки: {e}")

def save_archive():
    try:
        with lock:
            out = {f"{lat},{lng}": v for (lat, lng), v in lightning_archive.items()}
        with open(ARCHIVE_FILE, 'w') as f:
            json.dump(out, f)
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")

def delete_archive_file():
    try:
        if os.path.exists(ARCHIVE_FILE):
            os.remove(ARCHIVE_FILE)
            print(f"🗑️ Файл {ARCHIVE_FILE} удален")
    except Exception as e:
        print(f"❌ Ошибка удаления файла: {e}")

def periodic_file_deletion():
    """Полная очистка каждые 20 минут"""
    while True:
        time.sleep(DELETE_FILE_INTERVAL)
        with lock:
            count = len(lightning_archive)
            delete_archive_file()
            lightning_archive.clear()
            print(f"🔄 Полная очистка: удалено {count} разрядов (каждые {DELETE_FILE_INTERVAL // 60} мин)")

def periodic_save():
    """Периодическое сохранение в файл"""
    while True:
        time.sleep(SAVE_INTERVAL)
        save_archive()
        print(f"💾 Автосохранение: {len(lightning_archive)} точек")

def clean_old():
    """Удаляет точки старше 1 часа"""
    with lock:
        now = time.time()
        initial_count = len(lightning_archive)
        to_del = [k for k, v in lightning_archive.items() if now - v['time'] > MAX_AGE_SECONDS]
        for k in to_del:
            del lightning_archive[k]
        if to_del:
            print(f"🧹 Удалено {len(to_del)} старых точек >1ч (было {initial_count}, осталось {len(lightning_archive)})")
        check_limit()

def check_limit():
    """Если точек больше MAX_POINTS — удаляем самые старые"""
    global lightning_archive
    deleted = 0
    while len(lightning_archive) > MAX_POINTS:
        oldest_key = None
        oldest_time = float('inf')
        
        for k, v in lightning_archive.items():
            t = v.get('time', float('inf'))
            if t < oldest_time:
                oldest_time = t
                oldest_key = k
        
        if oldest_key:
            del lightning_archive[oldest_key]
            deleted += 1
        else:
            break
    
    if deleted > 0:
        print(f"⚠️ Удалено по лимиту: {deleted} точек")

def background_lightning_collector():
    """
    ЕДИНСТВЕННЫЙ сборщик — работает постоянно в фоне.
    Собирает данные и отправляет их в очередь для SSE-клиентов.
    """
    global collector_running
    collector_running = True
    
    print("=" * 70)
    print("🔥 ФОНОВЫЙ СБОРЩИК МОЛНИЙ ЗАПУЩЕН")
    print("=" * 70)
    
    reconnect_delay = 5
    
    while collector_running:
        try:
            print(f"\n📡 Подключение к источнику...")
            
            resp = requests.get(SOURCE_URL, stream=True, timeout=60)
            
            if resp.status_code != 200:
                print(f"❌ Ошибка HTTP {resp.status_code}")
                time.sleep(reconnect_delay)
                continue
            
            print("✅ ПОДКЛЮЧЕНО!")
            print(f"📊 Текущий архив: {len(lightning_archive)} разрядов")
            
            buffer = ""
            last_clean = time.time()
            last_heartbeat = time.time()
            
            for chunk in resp.iter_content(chunk_size=1024, decode_unicode=True):
                if not chunk or not collector_running:
                    break
                
                buffer += chunk
                
                now = time.time()
                if now - last_heartbeat > 60:
                    print(f"💓 Активно | Архив: {len(lightning_archive)}")
                    last_heartbeat = now
                
                while '\n\n' in buffer:
                    event, buffer = buffer.split('\n\n', 1)
                    
                    if 'lightning-vaisala' in event:
                        try:
                            lines = event.split('\n')
                            data_line = None
                            
                            for line in lines:
                                if line.startswith('data: '):
                                    data_line = line[6:]
                                    break
                            
                            if not data_line:
                                continue
                            
                            data = json.loads(data_line)
                            
                            if data and 'lightning' in data:
                                strikes = data['lightning']
                                now_ts = time.time()
                                new_strikes = []
                                
                                with lock:
                                    for s in strikes:
                                        if isinstance(s, list) and len(s) >= 2:
                                            lat, lng = float(s[0]), float(s[1])
                                            key = (lat, lng)
                                            
                                            is_new = key not in lightning_archive
                                            
                                            lightning_archive[key] = {
                                                'time': now_ts,
                                                'lat': lat,
                                                'lng': lng
                                            }
                                            
                                            if is_new:
                                                new_strikes.append([lat, lng])
                                    
                                    check_limit()
                                
                                if new_strikes:
                                    print(f"⚡ +{len(new_strikes)} | Всего: {len(lightning_archive)}")
                                    # Отправляем новые метки всем SSE-клиентам
                                    new_strikes_queue.put({'lightning': new_strikes})
                                    
                        except Exception as e:
                            pass
                    
                # Очистка старых каждые 60 секунд
                if now - last_clean > 60:
                    clean_old()
                    last_clean = now
                        
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            time.sleep(reconnect_delay)

def fake_user_keepalive():
    """Keepalive бот"""
    print("🤖 Keepalive бот запущен")
    while True:
        try:
            time.sleep(300)  # 5 минут
            with lock:
                count = len(lightning_archive)
            print(f"🤖 Keepalive: в памяти {count} точек")
        except Exception as e:
            print(f"❌ Ошибка keepalive: {e}")

# ========== РОУТЫ ==========

@app.route('/')
def serve_map():
    """Отдаём HTML-страницу с картой"""
    html = """<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no" />
    <title>⚡ Грозы — Lightning Map</title>
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
            background: rgba(0,0,0,0.7);
            border: 1px solid #ff6600;
            color: white;
            padding: 6px 12px;
            border-radius: 6px;
            cursor: pointer;
            margin-right: 5px;
        }

        button:hover { background: #ff6600; }
        button.danger { border-color: red; }
        button.danger:hover { background: red; }

        .controls {
            position: absolute;
            bottom: 20px;
            left: 20px;
            z-index: 1000;
            display: flex;
            gap: 5px;
        }

        .leaflet-lightning-layer { pointer-events: none; }
    </style>
</head>
<body>
    <div id="map"></div>

    <div class="info-panel">
        <div>⚡ ГРОЗЫ (последний 1 час)</div>
        <div class="count" id="strikeCount">—</div>
        <div id="statusText">Загрузка...</div>
    </div>

    <div class="legend">
        <div><span style="color:#fff">✚</span> 0-10мин</div>
        <div><span style="color:#ff0">✚</span> 10-20</div>
        <div><span style="color:#ffa500">✚</span> 20-30</div>
        <div><span style="color:#ff4500">✚</span> 30-40</div>
        <div><span style="color:#f00">✚</span> 40-50</div>
        <div><span style="color:#8b0000">✚</span> 50-60</div>
    </div>

    <div class="controls">
        <button id="refreshBtn">🔄 Обновить</button>
        <button id="clearBtn" class="danger">🗑 Сброс</button>
    </div>

    <script>
        const PROXY_URL = window.location.origin;

        const map = L.map('map', {
            preferCanvas: true,
            zoomControl: true
        }).setView([55, 37], 5);

        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; CartoDB, OSM',
            subdomains: 'abcd',
            maxZoom: 19
        }).addTo(map);

        const strikeStore = new Map();

        const LightningCanvasLayer = L.Layer.extend({
            initialize: function () {
                this._canvas = L.DomUtil.create('canvas', 'leaflet-lightning-layer');
                this._ctx = this._canvas.getContext('2d', { alpha: true });
                this._redrawScheduled = false;
            },

            onAdd: function (map) {
                this._map = map;
                map.getPanes().overlayPane.appendChild(this._canvas);
                map.on('move zoom resize viewreset zoomend moveend', this._reset, this);
                this._reset();
            },

            onRemove: function (map) {
                L.DomUtil.remove(this._canvas);
                map.off('move zoom resize viewreset zoomend moveend', this._reset, this);
            },

            _reset: function () {
                const size = this._map.getSize();
                const topLeft = this._map.containerPointToLayerPoint([0, 0]);
                L.DomUtil.setPosition(this._canvas, topLeft);

                const dpr = window.devicePixelRatio || 1;
                this._canvas.width = size.x * dpr;
                this._canvas.height = size.y * dpr;
                this._canvas.style.width = size.x + 'px';
                this._canvas.style.height = size.y + 'px';

                this._ctx.setTransform(1, 0, 0, 1, 0, 0);
                this._ctx.scale(dpr, dpr);
                this.redraw();
            },

            redraw: function () {
                if (this._redrawScheduled) return;
                this._redrawScheduled = true;
                requestAnimationFrame(() => {
                    this._redrawScheduled = false;
                    this._draw();
                });
            },

            _draw: function () {
                const ctx = this._ctx;
                const map = this._map;
                const size = map.getSize();
                ctx.clearRect(0, 0, size.x, size.y);

                const bounds = map.getBounds().pad(0.3);
                let plusSize = 4;
                const zoom = map.getZoom();
                if (zoom >= 8) plusSize = 8;
                else if (zoom >= 6) plusSize = 7;
                else if (zoom >= 4) plusSize = 6;
                else if (zoom >= 2) plusSize = 5;

                const groups = new Map();

                for (const s of strikeStore.values()) {
                    if (!bounds.contains([s.lat, s.lng])) continue;
                    const p = map.latLngToContainerPoint([s.lat, s.lng]);
                    if (p.x < -10 || p.y < -10 || p.x > size.x + 10 || p.y > size.y + 10) continue;
                    if (!groups.has(s.color)) groups.set(s.color, []);
                    groups.get(s.color).push([Math.round(p.x), Math.round(p.y)]);
                }

                ctx.lineWidth = 1;
                ctx.lineCap = 'square';

                for (const [color, points] of groups.entries()) {
                    ctx.strokeStyle = color;
                    ctx.beginPath();
                    const half = Math.floor(plusSize / 2);
                    for (const [x, y] of points) {
                        ctx.moveTo(x - half, y);
                        ctx.lineTo(x + half, y);
                        ctx.moveTo(x, y - half);
                        ctx.lineTo(x, y + half);
                    }
                    ctx.stroke();
                }
            }
        });

        const lightningLayer = new LightningCanvasLayer();
        lightningLayer.addTo(map);

        async function reloadArchive() {
            try {
                document.getElementById('statusText').innerText = 'Обновление...';
                const resp = await fetch(PROXY_URL + '/api/archive');
                const data = await resp.json();

                strikeStore.clear();
                for (const s of data.strikes) {
                    const key = `${s.lat.toFixed(4)},${s.lng.toFixed(4)}`;
                    strikeStore.set(key, {
                        lat: s.lat,
                        lng: s.lng,
                        color: s.color,
                        age_min: s.age_min
                    });
                }

                document.getElementById('strikeCount').textContent = strikeStore.size;
                document.getElementById('statusText').textContent = `Обновлено: ${new Date().toLocaleTimeString()}`;
                lightningLayer.redraw();
            } catch (err) {
                console.error('❌ Ошибка:', err);
                document.getElementById('statusText').textContent = '❌ Ошибка';
            }
        }

        function addNewStrikes(newStrikes) {
            if (!newStrikes || !newStrikes.length) return;

            let added = 0;
            for (const s of newStrikes) {
                if (!Array.isArray(s) || s.length < 2) continue;

                const lat = s[0];
                const lng = s[1];
                const key = `${lat.toFixed(4)},${lng.toFixed(4)}`;

                if (!strikeStore.has(key)) {
                    strikeStore.set(key, {
                        lat,
                        lng,
                        color: '#ffffff',
                        age_min: 0
                    });
                    added++;
                }
            }

            if (added) {
                document.getElementById('strikeCount').textContent = strikeStore.size;
                lightningLayer.redraw();
            }
        }

        function clearAllMarkers() {
            strikeStore.clear();
            lightningLayer.redraw();
            document.getElementById('strikeCount').textContent = '0';
            document.getElementById('statusText').textContent = 'Сброс выполнен';
        }

        let eventSource = null;
        function connectSSE() {
            if (eventSource) eventSource.close();
            eventSource = new EventSource(PROXY_URL + '/lightning-stream');

            eventSource.addEventListener('lightning-vaisala', (e) => {
                try {
                    const data = JSON.parse(e.data);
                    if (data && data.lightning) {
                        addNewStrikes(data.lightning);
                    }
                } catch (err) {
                    console.warn('SSE parse error', err);
                }
            });

            eventSource.onerror = () => {
                console.log('SSE error, reconnecting...');
                if (eventSource) eventSource.close();
                setTimeout(connectSSE, 5000);
            };
        }

        document.getElementById('refreshBtn').onclick = reloadArchive;
        document.getElementById('clearBtn').onclick = clearAllMarkers;

        setInterval(reloadArchive, 30000);

        window.addEventListener('resize', () => {
            map.invalidateSize();
            lightningLayer._reset();
        });

        reloadArchive();
        connectSSE();
        
        console.log('🚀 Карта запущена, SSE + периодическое обновление');
    </script>
</body>
</html>"""
    return html

@app.route('/lightning-stream')
def stream():
    """
    SSE-поток для пользователей.
    НЕ подключается к внешнему источнику, а читает из очереди new_strikes_queue,
    которую заполняет фоновый сборщик.
    """
    def generate():
        print("👤 Новый SSE-клиент подключен")
        try:
            while True:
                # Ждём новые данные от фонового сборщика
                data = new_strikes_queue.get(timeout=30)  # Таймаут 30 сек для heartbeat
                
                if data:
                    # Отправляем данные клиенту
                    yield f"event: lightning-vaisala\ndata: {json.dumps(data)}\n\n"
                    
        except Exception as e:
            print(f"👤 SSE-клиент отключен: {e}")
            yield f"event: error\ndata: {str(e)}\n\n"
    
    return Response(generate(), mimetype='text/event-stream',
                    headers={
                        'Cache-Control': 'no-cache',
                        'X-Accel-Buffering': 'no'
                    })

@app.route('/api/archive')
def get_archive():
    with lock:
        now = time.time()
        strikes = []
        for data in lightning_archive.values():
            age_min = (now - data['time']) / 60.0
            
            if age_min > 60:
                continue
                
            if age_min <= 10:
                color = '#ffffff'
            elif age_min <= 20:
                color = '#ffff00'
            elif age_min <= 30:
                color = '#ffa500'
            elif age_min <= 40:
                color = '#ff4500'
            elif age_min <= 50:
                color = '#ff0000'
            else:
                color = '#8b0000'
                
            strikes.append({
                'lat': data['lat'],
                'lng': data['lng'],
                'color': color,
                'age_min': round(age_min, 1)
            })
    
    return jsonify({'strikes': strikes, 'count': len(strikes)})

@app.route('/api/clear', methods=['POST'])
def clear_archive():
    with lock:
        lightning_archive.clear()
        delete_archive_file()
    return jsonify({'status': 'cleared', 'count': 0})

@app.route('/api/stats')
def get_stats():
    with lock:
        total = len(lightning_archive)
        now = time.time()
        age_groups = {
            '0-10min': 0,
            '10-20min': 0,
            '20-30min': 0,
            '30-40min': 0,
            '40-50min': 0,
            '50-60min': 0
        }
        
        for data in lightning_archive.values():
            age_min = (now - data['time']) / 60.0
            if age_min <= 10:
                age_groups['0-10min'] += 1
            elif age_min <= 20:
                age_groups['10-20min'] += 1
            elif age_min <= 30:
                age_groups['20-30min'] += 1
            elif age_min <= 40:
                age_groups['30-40min'] += 1
            elif age_min <= 50:
                age_groups['40-50min'] += 1
            elif age_min <= 60:
                age_groups['50-60min'] += 1
    
    return jsonify({
        'total_strikes': total,
        'age_distribution': age_groups,
        'file_exists': os.path.exists(ARCHIVE_FILE),
        'collector_running': collector_running
    })

@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'archive_size': len(lightning_archive)}), 200

# ========== ИНИЦИАЛИЗАЦИЯ ==========

load_archive()

deletion_thread = Thread(target=periodic_file_deletion, daemon=True)
deletion_thread.start()

save_thread = Thread(target=periodic_save, daemon=True)
save_thread.start()

collector_thread = Thread(target=background_lightning_collector, daemon=True)
collector_thread.start()

keepalive_thread = Thread(target=fake_user_keepalive, daemon=True)
keepalive_thread.start()

print("\n" + "=" * 70)
print("⚡ LIGHTNING ARCHIVE API")
print("📊 Фоновый сборщик: АКТИВЕН (собирает в очередь)")
print("💾 Автосохранение: каждые 30 сек")
print("👥 SSE для пользователей: читает из очереди")
print("🤖 Keepalive: каждые 5 мин")
print("⏱️ Очистка старых: через 1 час")
print("🗑️ Полная очистка: каждые 20 минут")
print("=" * 70 + "\n")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)

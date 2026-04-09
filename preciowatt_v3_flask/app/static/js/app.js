/* ══════════════════════════════════════
   PARTICLES
══════════════════════════════════════ */
(function() {
    const w = document.getElementById('particles');
    for (let i = 0; i < 18; i++) {
        const p = document.createElement('div');
        p.className = 'particle';
        p.style.cssText = `left:${Math.random()*100}%;animation-duration:${8+Math.random()*12}s;animation-delay:${-Math.random()*10}s`;
        w.appendChild(p);
    }
})();

/* ══════════════════════════════════════
   LIVE TICKER — datos reales + predicción RF
══════════════════════════════════════ */
let currentPrice = 290.0;

// Inicializar ticker con último precio real del dataset
(function() {
    const lp = document.getElementById('live-price');
    if (lp) lp.textContent = '290.0';
    const tf = document.getElementById('ticker-fill');
    if (tf) tf.style.width = ((290 - 101) / (2459 - 101) * 100).toFixed(1) + '%';
    const fl = document.getElementById('ticker-fecha-lbl');
    if (fl) fl.textContent = '· 2026-03-24';
    const sh = document.getElementById('shower-cost');
    if (sh) sh.textContent = '$ ' + (290 * 0.275).toFixed(0);
})();

// Cargar predicción RF en el ticker al arrancar
async function cargarTickerPred() {
    try {
        const r = await fetch('/api/v1/prediccion/diaria', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ modo: 'B', dias: 7 })
        });
        const data = await r.json();
        if (data.ok && data.predicciones && data.predicciones.length > 0) {
            const p0 = data.predicciones[0];
            const pv = p0.Precio_Pred;
            const el = document.getElementById('ticker-pred-val');
            if (el) el.textContent = pv.toFixed(1) + ' COP/kWh para ' + p0.Fecha;
            const badge = document.getElementById('ticker-decision-badge');
            if (badge) {
                badge.textContent = p0.Decision === 'rf' ? 'RF activo' : 'Modo Naive';
                badge.style.color = p0.Decision === 'rf' ? 'var(--green)' : 'var(--amber)';
            }
            const delta = pv - currentPrice;
            const ch = document.getElementById('live-change');
            if (ch) {
                ch.className = 'ticker-change ' + (delta > 2 ? 'up' : delta < -2 ? 'down' : 'flat');
                ch.innerHTML = '<span>' + (delta > 2 ? '▲' : delta < -2 ? '▼' : '≈') + '</span><span>Predicción próximo día: ' + pv.toFixed(1) + ' COP/kWh (' + (delta >= 0 ? '+' : '') + delta.toFixed(1) + ' vs último real)</span>';
            }
        }
    } catch (e) {}
}
setTimeout(cargarTickerPred, 1500);

/* FECHA DEFAULT eliminada — el predictor usa modo/días, no fecha/hora */

/* ══════════════════════════════════════
   PREDICTOR — RF Híbrido v8 real vía API
══════════════════════════════════════ */
let predModo = 'B';
let predDias = 14;

function setModo(m) {
    predModo = m;
    document.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('active'));
    const mb = document.getElementById('mode-btn-' + m);
    if (mb) mb.classList.add('active');
    const descs = {
        B: '🔮 <strong>Modo Futuro:</strong> predice desde el día siguiente a la última fecha del dataset usando contexto fresco de SIMEM.',
        A: '📊 <strong>Modo Histórico:</strong> valida el modelo sobre el tramo histórico previo al dataset base, entre 2023-01-01 y 2023-07-30.',
        C: '📅 <strong>Modo Rango:</strong> predice cualquier período — pasado conocido o futuro.'
    };
    const d = document.getElementById('pred-modo-desc');
    if (d) d.innerHTML = descs[m] || '';
    const fd = document.getElementById('field-dias');
    const fr = document.getElementById('field-rango');
    if (fd) fd.style.display = m === 'C' ? 'none' : 'block';
    if (fr) fr.style.display = m === 'C' ? 'block' : 'none';
}

function setDias(d) {
    predDias = d;
    document.querySelectorAll('.dias-btn').forEach(b => b.classList.remove('active'));
    const b = document.getElementById('dias-btn-' + d);
    if (b) b.classList.add('active');
}

async function runPrediction() {
    const errEl = document.getElementById('pred-error');
    if (errEl) { errEl.innerHTML = '';
        errEl.classList.remove('show'); }

    if (predModo === 'C') {
        const fi = document.getElementById('rango-inicio');
        const ff = document.getElementById('rango-fin');
        if (!fi || !ff || !fi.value || !ff.value) {
            if (errEl) { errEl.innerHTML = '<span>⚠ Selecciona fecha inicio y fecha fin.</span>';
                errEl.classList.add('show'); }
            return;
        }
    }

    const btn = document.getElementById('btn-predict');
    if (btn) { btn.textContent = '⏳ Ejecutando...';
        btn.disabled = true; }

    const ph = document.getElementById('result-placeholder');
    const ld = document.getElementById('result-loading');
    const sm = document.getElementById('result-summary');
    const pt = document.getElementById('panel-tabla');
    const pm = document.getElementById('panel-metricas');
    if (ph) ph.style.display = 'none';
    if (ld) ld.style.display = 'block';
    if (sm) sm.style.display = 'none';
    if (pt) pt.style.display = 'none';
    if (pm) pm.style.display = 'none';

    const lb = document.getElementById('loading-bar');
    if (lb) { lb.style.width = '0%';
        setTimeout(() => { lb.style.transition = 'width 25s linear';
            lb.style.width = '90%'; }, 50); }

    const body = { modo: predModo, dias: predDias };
    if (predModo === 'C') {
        const ri = document.getElementById('rango-inicio');
        const rf = document.getElementById('rango-fin');
        if (ri) body.fecha_inicio = ri.value;
        if (rf) body.fecha_fin = rf.value;
    }

    try {
        const r = await fetch('/api/v1/prediccion/diaria', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        const data = await r.json();
        if (lb) { lb.style.transition = 'width .3s';
            lb.style.width = '100%'; }
        setTimeout(() => {
            if (ld) ld.style.display = 'none';
            if (data.ok) { console.log('Predicciones recibidas:', data.predicciones.length, 'días');
                renderResultado(data); } else {
                if (ph) ph.style.display = 'block';
                if (errEl) { errEl.innerHTML = '<span>⚠ ' + (data.error || 'Error desconocido') + '</span>';
                    errEl.classList.add('show'); }
            }
        }, 300);
    } catch (e) {
        if (ld) ld.style.display = 'none';
        if (ph) ph.style.display = 'block';
        if (errEl) { errEl.innerHTML = '<span>⚠ Error de conexión: ' + e.message + '</span>';
            errEl.classList.add('show'); }
    } finally {
        if (btn) { btn.textContent = '⚡ EJECUTAR PREDICCIÓN RF v8';
            btn.disabled = false; }
    }
}

function renderResultado(data) {
    const preds = data.predicciones;
    if (!preds || !preds.length) return;
    const sm = document.getElementById('result-summary');
    if (sm) sm.style.display = 'block';
    const p0 = preds[0];
    const rp = document.getElementById('result-price');
    if (rp) rp.textContent = p0.Precio_Pred.toFixed(1);
    const pt2 = document.getElementById('panel-result-title');
    if (pt2) pt2.textContent = 'RF Híbrido v8 · ' + data.fecha_inicio + ' → ' + data.fecha_fin + ' · ' + data.dias + 'd';
    const delta = p0.Precio_Pred - p0.Precio_Naive;
    const dEl = document.getElementById('result-delta');
    if (dEl) {
        if (delta > 2) { dEl.className = 'result-delta up';
            dEl.innerHTML = '▲ +' + (delta.toFixed(1)) + ' COP vs Naive · el modelo predice alza'; } else if (delta < -2) { dEl.className = 'result-delta down';
            dEl.innerHTML = '▼ ' + (delta.toFixed(1)) + ' COP vs Naive · el modelo predice baja'; } else { dEl.className = 'result-delta flat';
            dEl.innerHTML = '≈ Modelo coincide con Naive (' + (delta >= 0 ? '+' : '') + delta.toFixed(1) + ' COP)'; }
    }
    const total = data.n_rf + data.n_naive;
    const rfPct = total > 0 ? Math.round(data.n_rf / total * 100) : 0;
    const cp = document.getElementById('conf-pct');
    if (cp) cp.textContent = rfPct + '% RF activo';
    const cf = document.getElementById('conf-fill');
    if (cf) { cf.style.width = rfPct + '%';
        cf.style.background = rfPct > 60 ? 'var(--green)' : rfPct > 30 ? 'var(--amber)' : 'var(--col-blue2)'; }
    const dl = document.getElementById('decision-label');
    if (dl) dl.textContent = 'Uso del RF en el horizonte';
    const mr = document.getElementById('mg-rf');
    if (mr) mr.textContent = data.n_rf + 'd';
    const mn = document.getElementById('mg-naive');
    if (mn) mn.textContent = data.n_naive + 'd';
    const maxB = Math.max(...preds.map(p => p.dias_bajando || 0));
    const ma = document.getElementById('mg-alert');
    if (ma) { if (maxB >= 4) { ma.textContent = '⚠ ' + maxB + 'd';
            ma.style.color = 'var(--red)'; } else { ma.textContent = maxB + 'd';
            ma.style.color = 'var(--green)'; } }
    const panTab = document.getElementById('panel-tabla');
    if (panTab) panTab.style.display = 'block';
    // Actualizar título con conteo real de filas
    const ptitle = document.getElementById('panel-tabla').querySelector('.panel-title');
    if (ptitle) ptitle.textContent = 'Predicciones día a día (' + preds.length + ' días)';
    const tbody = document.getElementById('pred-tbody');
    if (tbody) {
        tbody.innerHTML = preds.map(p => {
            const hr = p.Precio_Real != null;
            const err = hr ? Math.abs((p.Precio_Real - p.Precio_Pred) / p.Precio_Real * 100) : null;
            const dc = p.Decision === 'rf' ? 'var(--green)' : p.Decision === 'naive_forzado' ? 'var(--red)' : 'var(--text3)';
            const dt = p.Decision === 'rf' ? 'RF' : p.Decision === 'naive_forzado' ? 'NAI*' : 'NAI';
            return `<tr style="border-bottom:1px solid var(--border)">
        <td style="padding:4px 6px;font-family:var(--mono);font-size:.68rem">${p.Fecha}${p.es_futuro?'<span style="font-size:.6rem;color:var(--accent);margin-left:3px">fut</span>':''}</td>
        <td style="padding:4px 6px;text-align:right;font-family:var(--mono);font-size:.68rem;font-weight:600">${p.Precio_Pred.toFixed(1)}</td>
        <td style="padding:4px 6px;text-align:right;font-family:var(--mono);font-size:.68rem;color:var(--text3)">${p.Precio_Naive.toFixed(1)}</td>
        <td style="padding:4px 6px;text-align:right;font-family:var(--mono);font-size:.68rem;color:var(--text3)">${p.std_14d!=null?p.std_14d.toFixed(1):'—'}</td>
        <td style="padding:4px 6px;text-align:center;font-family:var(--mono);font-size:.68rem;color:${dc};font-weight:700">${dt}</td>
        <td style="padding:4px 6px;text-align:right;font-family:var(--mono);font-size:.68rem;color:var(--accent)">${hr?p.Precio_Real.toFixed(1):'—'}</td>
        <td style="padding:4px 6px;text-align:right;font-family:var(--mono);font-size:.68rem;color:${err!=null?(err<10?'var(--green)':err<20?'var(--amber)':'var(--red)'):'var(--text3)'}">${err!=null?err.toFixed(1)+'%':'—'}</td>
      </tr>`;
        }).join('');
    }
    const panMet = document.getElementById('panel-metricas');
    if (panMet) panMet.style.display = 'block';
    const mc = document.getElementById('metricas-content');
    if (mc) {
        if (data.metricas) {
            const m = data.metricas;
            mc.innerHTML = `<table style="width:100%;border-collapse:collapse;font-family:var(--mono);font-size:.72rem">
        <thead><tr style="color:var(--text3);border-bottom:1px solid var(--border)">
          <th style="padding:5px;text-align:left">Modelo</th><th style="padding:5px;text-align:right">MAPE</th>
          <th style="padding:5px;text-align:right">MAE</th><th style="padding:5px;text-align:right">RMSE</th><th style="padding:5px;text-align:right">R²</th>
        </tr></thead><tbody>
          <tr style="border-bottom:1px solid var(--border)"><td style="padding:5px">Naive</td>
            <td style="padding:5px;text-align:right;color:var(--amber)">${m.naive.mape}%</td>
            <td style="padding:5px;text-align:right">${m.naive.mae}</td><td style="padding:5px;text-align:right">${m.naive.rmse}</td>
            <td style="padding:5px;text-align:right">${m.naive.r2!=null?m.naive.r2:'—'}</td></tr>
          <tr><td style="padding:5px;font-weight:700;color:var(--green)">RF Híbrido v8</td>
            <td style="padding:5px;text-align:right;color:var(--green);font-weight:700">${m.hibrido.mape}%</td>
            <td style="padding:5px;text-align:right;font-weight:700">${m.hibrido.mae}</td><td style="padding:5px;text-align:right;font-weight:700">${m.hibrido.rmse}</td>
            <td style="padding:5px;text-align:right;font-weight:700">${m.hibrido.r2!=null?m.hibrido.r2:'—'}</td></tr>
        </tbody></table>
        <div style="font-size:.68rem;color:var(--text3);margin-top:.5rem;font-family:var(--mono)">
          Ref. test set → Híbrido: ${data.ref_mape_hib}%  Naive: ${data.ref_mape_naive}% · ${m.dias_con_real} días con precio real</div>`;
        } else {
            mc.innerHTML = '<div style="font-family:var(--mono);font-size:.72rem;color:var(--text3);padding:.75rem">Predicciones completamente futuras — métricas disponibles cuando SIMEM publique los precios reales.</div>';
        }
    }
}

/* ══════════════════════════════════════
   HIDRO SIMULATOR
══════════════════════════════════════ */
const cuencas = [
    { name: 'Guatapé (Antioquia)', base: 68 },
    { name: 'Betania (Huila)', base: 72 },
    { name: 'El Quimbo (Huila)', base: 80 },
    { name: 'Salvajina (Cauca)', base: 55 },
    { name: 'Porce II (Ant.)', base: 62 },
];

function renderCuencas(h) {
    document.getElementById('cuencas-list').innerHTML = cuencas.map(c => {
        const lvl = Math.max(5, Math.min(100, c.base + (h - 65) * 0.8));
        const col = lvl < 40 ? 'var(--col-red)' : lvl < 60 ? 'var(--amber)' : 'var(--hydro)';
        return `<div style="display:flex;align-items:center;gap:8px">
      <div style="font-family:var(--mono);font-size:.65rem;color:var(--text2);width:155px;flex-shrink:0">${c.name}</div>
      <div style="flex:1;height:4px;background:var(--border);border-radius:2px;overflow:hidden">
        <div style="width:${lvl}%;height:100%;background:${col};border-radius:2px;transition:width .6s"></div>
      </div>
      <div style="font-family:var(--mono);font-size:.65rem;color:${col};width:35px;text-align:right">${lvl.toFixed(0)}%</div>
    </div>`;
    }).join('');
}

function updateHidroSim() {
    const h = parseInt(document.getElementById('hidro-slider').value);
    const t = parseInt(document.getElementById('thermal-slider').value);
    document.getElementById('hidro-val').textContent = h + '%';
    document.getElementById('thermal-val').textContent = t + '%';
    document.getElementById('res-label').innerHTML = h + '%<small>NIVEL</small>';
    document.getElementById('res-water').style.height = h + '%';
    document.getElementById('src-hydro-pct').textContent = h + '%';
    document.getElementById('src-thermal-pct').textContent = t + '%';
    const solar = 5,
        other = Math.max(0, 100 - h - t - solar);
    document.getElementById('sources-bar').innerHTML = `
    <div class="src-seg" style="width:${h}%;background:var(--hydro)"></div>
    <div class="src-seg" style="width:${t}%;background:var(--thermal)"></div>
    <div class="src-seg" style="width:${solar}%;background:var(--solar)"></div>
    <div class="src-seg" style="width:${other}%;background:#a78bfa"></div>`;
    const price = Math.max(97, Math.min(900, 174 + (h - 65) * (-1.8) + (t - 25) * 1.4));
    document.getElementById('hidro-price').innerHTML = `${price.toFixed(0)} <span style="font-size:.5em;color:var(--text3)">COP/kWh</span>`;
    const pct = ((price - 174) / 174 * 100);
    let arrow = '⚪',
        expl = 'Condiciones normales. Precio dentro del rango habitual.';
    if (h < 30 && t > 55) { arrow = '🔴';
        expl = `⚠️ Crisis severa: embalses críticos + máximo térmico. Precio ${Math.abs(pct).toFixed(0)}% sobre la mediana. Riesgo de racionamiento.`; } else if (h < 35 && t > 50) { arrow = '🔴';
        expl = `⚠️ Crisis hídrica + alto despacho térmico. Precio ${Math.abs(pct).toFixed(0)}% sobre la mediana.`; } else if (h < 45) { arrow = '🟡';
        expl = `Embalses bajos. Más generación térmica (más costosa). Precio +${Math.abs(pct).toFixed(0)}% sobre lo normal.`; } else if (h > 80 && t < 15) { arrow = '🟢';
        expl = `Embalses en niveles óptimos. Generación hídrica abundante y barata. Precio ${Math.abs(pct).toFixed(0)}% bajo la mediana.`; }
    document.getElementById('hidro-arrow').textContent = arrow;
    document.getElementById('hidro-expl').textContent = expl;
    renderCuencas(h);
    saveState();
}
document.getElementById('hidro-slider').addEventListener('input', updateHidroSim);
document.getElementById('thermal-slider').addEventListener('input', updateHidroSim);
renderCuencas(65);

/* ══════════════════════════════════════
   CALCULADORA
══════════════════════════════════════ */
const catalog = [
    { icon: '❄️', name: 'Refrigerador (Haceb/Samsung 300L)', watts: 180, h: 24, maxQ: 1, sel: true, qty: 1 },
    { icon: '📺', name: 'TV LED 43" (Kalley/Samsung)', watts: 85, h: 5, maxQ: 4, sel: true, qty: 2 },
    { icon: '📺', name: 'TV LED 55" (Samsung/LG)', watts: 130, h: 4, maxQ: 3, sel: false, qty: 1 },
    { icon: '💡', name: 'Iluminación LED (12 puntos)', watts: 96, h: 6, maxQ: 1, sel: true, qty: 1 },
    { icon: '📱', name: 'Cargadores smartphone (×4)', watts: 60, h: 4, maxQ: 1, sel: true, qty: 1 },
    { icon: '🌀', name: 'Aire acond. mini-split 12K BTU', watts: 1050, h: 8, maxQ: 3, sel: false, qty: 1 },
    { icon: '💧', name: 'Ducha eléctrica (Lorenzetti/Corona)', watts: 3300, h: 0.5, maxQ: 4, sel: false, qty: 2 },
    { icon: '🧺', name: 'Lavadora (Whirlpool/Challenger 14kg)', watts: 450, h: 1, maxQ: 1, sel: false, qty: 1 },
    { icon: '🍳', name: 'Estufa eléctrica (Indurama 4 ptos)', watts: 5800, h: 1.5, maxQ: 1, sel: false, qty: 1 },
    { icon: '🌡️', name: 'Microondas (Haceb/Samsung)', watts: 1100, h: 0.3, maxQ: 2, sel: false, qty: 1 },
    { icon: '☕', name: 'Cafetera goteo (Kalley/Oster)', watts: 800, h: 0.5, maxQ: 1, sel: false, qty: 1 },
    { icon: '🖥️', name: 'PC de escritorio', watts: 250, h: 6, maxQ: 2, sel: false, qty: 1 },
    { icon: '💻', name: 'Portátil / laptop', watts: 65, h: 6, maxQ: 3, sel: false, qty: 1 },
    { icon: '🎮', name: 'Consola (PS5/Xbox)', watts: 200, h: 3, maxQ: 2, sel: false, qty: 1 },
    { icon: '💨', name: 'Ventilador de techo (Hunter)', watts: 75, h: 8, maxQ: 4, sel: false, qty: 2 },
    { icon: '🫙', name: 'Dispensador agua (frío/calor)', watts: 420, h: 24, maxQ: 2, sel: false, qty: 1 },
    { icon: '🪣', name: 'Bomba de agua (½ HP)', watts: 370, h: 2, maxQ: 1, sel: false, qty: 1 },
    { icon: '🔊', name: 'Equipo de sonido (Kalley/LG)', watts: 95, h: 3, maxQ: 1, sel: false, qty: 1 },
];
const tarifas = { e12: 410, e3: 530, e46: 680, ind: 350 };
let tarifaKey = 'e12';

function hLabel(h) { return h < 1 ? Math.round(h * 60) + 'min' : h + 'h'; }

function setTarifa(k, btn) {
    tarifaKey = k;
    document.querySelectorAll('.tarifa-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('c-tarifa').textContent = tarifas[k] + ' COP/kWh';
    updateCalc();
    saveState();
}

function renderAppliances() {
    document.getElementById('appliance-list').innerHTML = catalog.map((a, i) => `
    <div class="app-card ${a.sel?'sel':''}" id="acard-${i}" role="button" tabindex="0" aria-pressed="${a.sel?'true':'false'}">
      <div class="app-icon" onclick="toggleApp(${i})" style="cursor:pointer">${a.icon}</div>
      <div class="app-info" onclick="toggleApp(${i})" style="cursor:pointer">
        <div class="app-name">${a.name}</div>
        <div class="app-sub">${a.watts}W · ${hLabel(a.h)}/día · ×${a.qty}</div>
      </div>
      <div class="app-controls">
        <div class="stepper">
          <button class="stp-btn" onclick="chQty(${i},-1,event)">−</button>
          <span class="stp-val" style="color:var(--thermal)">×${a.qty}</span>
          <button class="stp-btn" onclick="chQty(${i},1,event)">+</button>
          <span class="stp-lbl">UND</span>
        </div>
        <div class="stepper">
          <button class="stp-btn" onclick="chHours(${i},-0.5,event)">−</button>
          <span class="stp-val">${hLabel(a.h)}</span>
          <button class="stp-btn" onclick="chHours(${i},0.5,event)">+</button>
          <span class="stp-lbl">H/DÍA</span>
        </div>
      </div>
    </div>`).join('');
}

function toggleApp(i) { catalog[i].sel = !catalog[i].sel;
    renderAppliances();
    updateCalc();
    saveState(); }

function chHours(i, d, e) { e.stopPropagation();
    catalog[i].h = Math.max(0.5, Math.min(24, catalog[i].h + d));
    renderAppliances();
    updateCalc();
    saveState(); }

function chQty(i, d, e) { e.stopPropagation();
    catalog[i].qty = Math.max(1, Math.min(catalog[i].maxQ, catalog[i].qty + d));
    renderAppliances();
    updateCalc();
    saveState(); }

function updateCalc() {
    const sel = catalog.filter(a => a.sel);
    const kwhDay = sel.reduce((s, a) => s + (a.watts * a.qty * a.h / 1000), 0);
    const kwhMonth = kwhDay * 30;
    const tarifa = tarifas[tarifaKey];
    const monthly = kwhMonth * tarifa;
    document.getElementById('calc-monthly').textContent = '$ ' + Math.round(monthly).toLocaleString('es-CO');
    document.getElementById('c-kwh-day').textContent = kwhDay.toFixed(2) + ' kWh';
    document.getElementById('c-kwh-month').textContent = kwhMonth.toFixed(1) + ' kWh';
    document.getElementById('c-tarifa').textContent = tarifa + ' COP/kWh';
    document.getElementById('c-bolsa').textContent = '$ ' + Math.round(kwhMonth * currentPrice).toLocaleString('es-CO');
    document.getElementById('c-tax').textContent = '$ ' + Math.round(monthly * 0.08).toLocaleString('es-CO');
    document.getElementById('c-count').textContent = sel.length + ' aparatos';
}
renderAppliances();
updateCalc();

document.addEventListener('click', function(e) {
    const toggleTarget = e.target.closest('[data-toggle-app]');
    if (toggleTarget) {
        const idx = parseInt(toggleTarget.getAttribute('data-toggle-app'), 10);
        if (!Number.isNaN(idx)) toggleApp(idx);
    }
});


/* ══════════════════════════════════════
   QUIZ — 10 preguntas público general
══════════════════════════════════════ */
const quizData = [{
        q: '¿Cuál es la fuente principal de energía eléctrica en Colombia?',
        opts: ['El gas natural', 'El carbón', 'Las centrales hidroeléctricas', 'Los paneles solares'],
        correct: 2,
        exp: 'Colombia genera cerca del 65% de su electricidad en centrales hidroeléctricas, aprovechando sus ríos y embalses. Esto nos hace muy dependientes de la lluvia.',
        fun: '💧 Colombia tiene más de 1.000 ríos. ¡Es uno de los países con más agua dulce del mundo!'
    },
    {
        q: '¿Qué fenómeno climático hace que la energía en Colombia se encarezca?',
        opts: ['La Niña', 'El Niño', 'Los huracanes', 'Las nevadas'],
        correct: 1,
        exp: 'El Fenómeno del Niño reduce las lluvias en Colombia, baja los embalses y obliga a usar más plantas térmicas (gas/carbón), que son más costosas. Esto sube el precio de la luz.',
        fun: '🌡️ En el Niño de 2015-2016, el precio de bolsa llegó a duplicarse en Colombia.'
    },
    {
        q: '¿Qué significa "kWh" en tu recibo de energía?',
        opts: ['Kilowatt por hora de producción', 'Kilovoltio-hora de consumo', 'Kilowatt-hora: unidad de energía consumida', 'Costo por hora de servicio'],
        correct: 2,
        exp: 'Un kilowatt-hora (kWh) es la energía que consume un aparato de 1.000 vatios funcionando durante una hora. Por ejemplo: un bombillo de 100W encendido 10 horas = 1 kWh.',
        fun: '💡 Un bombillo LED consume 5-10 veces menos que uno incandescente para dar la misma luz.'
    },
    {
        q: '¿Cuál de estos electrodomésticos consume MÁS energía en un hogar colombiano?',
        opts: ['El televisor LED', 'La nevera', 'La ducha eléctrica', 'El celular cargando'],
        correct: 2,
        exp: 'La ducha eléctrica (3.000-4.500 W) es uno de los mayores consumidores. Aunque se usa poco tiempo, su potencia es muy alta. La nevera consume más en total por estar encendida 24 horas.',
        fun: '🚿 5 minutos de ducha eléctrica equivalen energéticamente a tener el televisor encendido casi 5 horas.'
    },
    {
        q: '¿Qué es Empresas Públicas de Medellín (EPM) en el contexto energético?',
        opts: ['Solo una empresa de agua', 'Una empresa distribuidora de energía', 'Una de las principales empresas generadoras y distribuidoras de Colombia', 'Una empresa privada extranjera'],
        correct: 2,
        exp: 'EPM es una de las empresas multiutilitarias más grandes de América Latina. Opera centrales hidroeléctricas, distribuye energía en Antioquia y tiene presencia en varios países de la región.',
        fun: '🏗️ EPM opera Hidroituango, la central hidroeléctrica más grande de Colombia con 2.400 MW.'
    },
    {
        q: '¿Qué estrato socioeconómico paga la tarifa de energía MÁS alta en Colombia?',
        opts: ['Estrato 1', 'Estrato 3', 'Estrato 4', 'Estrato 6'],
        correct: 3,
        exp: 'El sistema de tarifas por estratos en Colombia subsidia a los estratos bajos (1, 2 y 3) con cargo a los estratos altos (5 y 6) y el sector industrial. El estrato 6 paga la tarifa más alta.',
        fun: '📊 En Colombia, el 40% de los hogares son estrato 1 o 2 y reciben subsidio en su factura de energía.'
    },
    {
        q: '¿Cuántas horas al día funciona un refrigerador doméstico típico?',
        opts: ['Solo cuando está frío adentro', '8 horas', 'Las 24 horas del día', 'Depende si está lleno o vacío'],
        correct: 2,
        exp: 'El refrigerador funciona las 24 horas del día todos los días del año. El compresor se apaga y prende automáticamente para mantener la temperatura, pero el aparato siempre está "activo".',
        fun: '🧊 Un refrigerador eficiente (A+++) puede consumir hasta un 60% menos que uno de hace 15 años.'
    },
    {
        q: '¿Qué es SIMEM en Colombia?',
        opts: ['Una empresa de paneles solares', 'El Sistema de Información del Mercado de Energía Mayorista', 'Un impuesto a la energía', 'Una central eléctrica en el Caribe'],
        correct: 1,
        exp: 'SIMEM es el Sistema de Información del Mercado de Energía Mayorista, operado por XM. Publica en tiempo real los precios, la demanda y la generación del sistema eléctrico colombiano.',
        fun: '📡 Los datos de SIMEM son públicos y gratuitos. ¡Con ellos entrenamos este modelo!'
    },
    {
        q: '¿En qué horas del día se consume MÁS energía en Colombia?',
        opts: ['Madrugada (1am-5am)', 'Mañana (6am-9am)', 'Tarde-noche (18pm-22pm)', 'Mediodía (12pm-2pm)'],
        correct: 2,
        exp: 'El "pico" de demanda eléctrica en Colombia ocurre entre las 6pm y las 10pm, cuando las familias llegan a casa, encienden televisores, preparan la cena y usan duchas. Esto puede subir el precio.',
        fun: '🌙 Durante la madrugada la energía es más barata porque hay menos demanda y más oferta disponible.'
    },
    {
        q: '¿Cuánto representa el sector eléctrico en la economía colombiana aproximadamente?',
        opts: ['Menos del 1% del PIB', 'Entre el 3% y 4% del PIB', 'El 10% del PIB', 'El 20% del PIB'],
        correct: 1,
        exp: 'El sector eléctrico representa entre el 3% y 4% del PIB colombiano. Genera más de 150.000 empleos directos e indirectos y es fundamental para la competitividad del país.',
        fun: '🇨🇴 Colombia exporta energía eléctrica a Ecuador y es referente en América Latina por su cobertura eléctrica (97% de hogares).'
    },
];

let qCurrent = 0,
    qScore = 0,
    qAnswered = false;

function renderQuizProg() {
    document.getElementById('quiz-prog').innerHTML = quizData.map((_, i) =>
        `<div class="quiz-dot ${i<qCurrent?'done':i===qCurrent?'active':''}"></div>`).join('');
}

function renderQuiz() {
    if (qCurrent >= quizData.length) {
        document.getElementById('quiz-wrap').style.display = 'none';
        const sc = document.getElementById('quiz-score');
        sc.style.display = 'block';
        const pct = Math.round(qScore / quizData.length * 100);
        let emoji = '🎉',
            msg = '';
        if (pct === 100) { emoji = '🏆';
            msg = '¡Perfecto! Sabes todo sobre la energía eléctrica colombiana. ¡Eres un experto!' } else if (pct >= 80) { emoji = '⚡';
            msg = '¡Excelente! Tienes muy buenos conocimientos sobre energía. Colombia te necesita.' } else if (pct >= 60) { emoji = '💡';
            msg = '¡Bien! Sabes bastante. Sigue explorando el simulador para aprender más.' } else if (pct >= 40) { emoji = '📚';
            msg = 'Vas bien, pero hay más por descubrir. ¡Intenta de nuevo!' } else { emoji = '🌱';
            msg = '¡Todo experto empezó desde cero! Explora la app y vuelve a intentarlo.' }
        sc.innerHTML = `<div class="score-emoji">${emoji}</div>
      <div class="score-title">Quiz completado</div>
      <div class="score-val">${qScore}/${quizData.length}</div>
      <div class="score-msg">${msg}</div>
      <button type="button" class="btn-retry" onclick="resetQuiz()" type="button">⚡ Intentar de nuevo</button>`;
        return;
    }
    qAnswered = false;
    const q = quizData[qCurrent];
    renderQuizProg();
    document.getElementById('quiz-wrap').innerHTML = `
    <div class="quiz-card">
      <div class="quiz-num">Pregunta ${qCurrent+1} de ${quizData.length}</div>
      <div class="quiz-q">${q.q}</div>
      <div class="quiz-opts">
        ${q.opts.map((o,i)=>`
          <div class="quiz-opt" onclick="answerQuiz(${i})" id="qopt-${i}">
            <div class="quiz-letter">${String.fromCharCode(65+i)}</div>${o}
          </div>`).join('')}
      </div>
      <div class="quiz-exp" id="quiz-exp">
        ${q.exp}
        ${q.fun?`<div class="quiz-fun">💡 Sabías que... ${q.fun}</div>`:''}
      </div>
      <button type="button" class="quiz-next" id="quiz-next" onclick="nextQ()" type="button">Siguiente pregunta →</button>
    </div>`;
}

function answerQuiz(idx){
  if(qAnswered) return;
  qAnswered=true;
  const q=quizData[qCurrent];
  if(idx===q.correct) qScore++;
  document.querySelectorAll('.quiz-opt').forEach((el,i)=>{
    el.style.pointerEvents='none';
    if(i===q.correct) el.classList.add('correct');
    else if(i===idx) el.classList.add('wrong');
  });
  document.getElementById('quiz-exp').classList.add('show');
  document.getElementById('quiz-next').classList.add('show');
}

function nextQ(){qCurrent++;renderQuiz();}
function resetQuiz(){
  qCurrent=0;qScore=0;qAnswered=false;
  document.getElementById('quiz-score').style.display='none';
  document.getElementById('quiz-wrap').style.display='block';
  renderQuiz();
}
renderQuiz();

/* ══════════════════════════════════════
   FEATURE IMPORTANCE — RF Híbrido v8
══════════════════════════════════════ */
const features=[
  {n:'precio_lag_1d',             v:.1821, c:'var(--col-blue2)'},
  {n:'precio_std_14d',            v:.1043, c:'var(--col-blue2)'},
  {n:'precio_media_7d',           v:.0912, c:'var(--col-blue2)'},
  {n:'precio_lag_1d_log',         v:.0834, c:'var(--col-blue2)'},
  {n:'share_hidraulica',          v:.0721, c:'var(--hydro)'},
  {n:'dias_bajando_consecutivos', v:.0698, c:'var(--red)'},
  {n:'ratio_termica_hidraulica',  v:.0587, c:'var(--thermal)'},
  {n:'precio_cambio_7d',          v:.0512, c:'var(--col-blue2)'},
  {n:'precio_sobre_media_30d',    v:.0489, c:'var(--col-blue2)'},
  {n:'hidraulica_cambio_7d',      v:.0421, c:'var(--hydro)'},
  {n:'termica_media_7d',          v:.0378, c:'var(--thermal)'},
  {n:'presion_termica_14d',       v:.0342, c:'var(--thermal)'},
];
const maxF=features[0].v;
document.getElementById('feature-bars').innerHTML=features.map(f=>`
  <div class="spark-row">
    <div class="spark-name">${f.n}</div>
    <div class="spark-track"><div class="spark-fill" style="width:${(f.v/maxF*100).toFixed(1)}%;background:${f.c}"></div></div>
    <div class="spark-val">${(f.v*100).toFixed(2)}%</div>
  </div>`).join('');

/* ══════════════════════════════════════
   PERSISTENCIA localStorage
══════════════════════════════════════ */
function saveState(){
  try{
    const g=id=>{const e=document.getElementById(id);return e?e.value:null;};
    localStorage.setItem('pw_v8',JSON.stringify({
      modo:    predModo,
      rangoIni:g('rango-inicio'),
      rangoFin:g('rango-fin'),
      hs:      g('hidro-slider'),
      ts:      g('thermal-slider'),
      tk:      tarifaKey,
      apps:    catalog.map(a=>({sel:a.sel,h:a.h,qty:a.qty})),
    }));
  }catch(e){}
}

function loadState(){
  try{
    const raw=localStorage.getItem('pw_v8');
    if(!raw) return;
    const s=JSON.parse(raw);
    const set=(id,val)=>{const el=document.getElementById(id);if(el&&val!=null)el.value=val;};
    // Restaurar modo/días del predictor
    if(s.modo) setModo(s.modo);
    // no restaurar días — siempre usar el default del HTML
    set('rango-inicio',s.rangoIni);
    set('rango-fin',s.rangoFin);
    set('hidro-slider',s.hs);
    set('thermal-slider',s.ts);
    if(s.tk){
      tarifaKey=s.tk;
      document.querySelectorAll('.tarifa-btn').forEach((btn,i)=>{
        const keys=['e12','e3','e46','ind'];
        btn.classList.toggle('active',keys[i]===s.tk);
      });
      const ct=document.getElementById('c-tarifa');
      if(ct) ct.textContent=tarifas[s.tk]+' COP/kWh';
    }
    if(s.apps&&s.apps.length===catalog.length){
      s.apps.forEach((a,i)=>{catalog[i].sel=a.sel;catalog[i].h=a.h;catalog[i].qty=a.qty;});
    }
    updateHidroSim();
    renderAppliances();
    updateCalc();
  }catch(e){}
}
loadState();


/* ══════════════════════════════════════
   THEME + ACCESSIBILITY
══════════════════════════════════════ */
const THEME_KEY='pw_theme';
function applyTheme(theme){
  const isLight=theme==='light';
  document.body.classList.toggle('theme-light',isLight);
  const meta=document.querySelector('meta[name="theme-color"]');
  if(meta) meta.setAttribute('content', isLight ? '#f4f8fc' : '#06090d');
  const btn=document.getElementById('theme-toggle');
  const lbl=document.getElementById('theme-toggle-label');
  if(btn){ btn.setAttribute('aria-pressed', String(isLight)); }
  if(lbl){ lbl.textContent=isLight ? 'Modo oscuro' : 'Modo claro'; }
  try{ localStorage.setItem(THEME_KEY, theme); }catch(e){}
}
function initTheme(){
  let theme='dark';
  try{
    theme=localStorage.getItem(THEME_KEY) || (window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark');
  }catch(e){}
  applyTheme(theme);
  const btn=document.getElementById('theme-toggle');
  if(btn){
    btn.addEventListener('click',()=>{
      applyTheme(document.body.classList.contains('theme-light') ? 'dark' : 'light');
    });
  }
}
initTheme();

document.addEventListener('keydown',function(e){
  const card=e.target.closest('.app-card');
  if(card && (e.key==='Enter' || e.key===' ')){
    e.preventDefault();
    const id=card.id || '';
    const match=id.match(/acard-(\d+)/);
    if(match) toggleApp(parseInt(match[1],10));
  }
});

/* ══════════════════════════════════════
   SCROLL ANIMATIONS
══════════════════════════════════════ */
const obs=new IntersectionObserver(entries=>{
  entries.forEach(e=>{if(e.isIntersecting){e.target.style.animation='fadeUp .6s ease forwards';}});
},{threshold:.08});
document.querySelectorAll('.panel,.metric-card,.ver-card,.chart-wrap,.imp-card,.why-card').forEach(el=>{
  el.style.opacity='0';obs.observe(el);
});

/* ══════════════════════════════════════
   CSS dinámico — botones modo/días predictor
══════════════════════════════════════ */
(function(){
  const s=document.createElement('style');
  s.textContent=`
    .mode-btn{background:var(--bg3);border:1px solid var(--border);color:var(--text2);
      border-radius:var(--r);padding:.75rem .5rem;cursor:pointer;font-family:var(--mono);
      font-size:.72rem;transition:all .2s;text-align:center;line-height:1.4;width:100%}
    .mode-btn:hover{border-color:var(--accent);color:var(--text)}
    .mode-btn.active{background:rgba(251,191,36,.12);border-color:var(--accent);color:var(--accent)}
    .dias-btn{background:var(--bg3);border:1px solid var(--border);color:var(--text2);
      border-radius:3px;padding:.5rem;cursor:pointer;font-family:var(--mono);
      font-size:.72rem;transition:all .2s;width:100%}
    .dias-btn:hover{border-color:var(--accent);color:var(--text)}
    .dias-btn.active{background:rgba(251,191,36,.12);border-color:var(--accent);color:var(--accent)}
    .ticker-change.flat{color:var(--text2)}
  `;
  document.head.appendChild(s);
})();

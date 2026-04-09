(function(){
  if (!window.L || !document.getElementById('climate-map')) return;

  const departments = Array.isArray(window.CLIMATE_DEPARTMENTS) ? window.CLIMATE_DEPARTMENTS : [];
  const selected = window.CLIMATE_SELECTED || null;

  const colorByAlert = (alert) => {
    if (alert === 'Alta') return '#ef4444';
    if (alert === 'Media') return '#f59e0b';
    return '#22c55e';
  };

  const isLight = document.body.classList.contains('theme-light');
  const tileUrl = isLight
    ? 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png'
    : 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png';

  const attr = isLight
    ? '&copy; OpenStreetMap'
    : '&copy; OpenStreetMap &copy; CARTO';

  const map = L.map('climate-map', {zoomControl:true, scrollWheelZoom:true}).setView([4.6, -74.1], 5.3);
  L.tileLayer(tileUrl, { attribution: attr }).addTo(map);

  const bounds = [];
  let selectedMarker = null;

  departments.forEach(dep => {
    if (typeof dep.lat !== 'number' || typeof dep.lon !== 'number') return;
    const marker = L.circleMarker([dep.lat, dep.lon], {
      radius: dep.slug === selected ? 10 : 7,
      color: dep.slug === selected ? '#fbbf24' : colorByAlert(dep.nivel_alerta),
      weight: dep.slug === selected ? 3 : 2,
      fillColor: dep.slug === selected ? '#fbbf24' : colorByAlert(dep.nivel_alerta),
      fillOpacity: dep.slug === selected ? 0.95 : 0.8
    }).addTo(map);

    marker.bindPopup(
      `<div style="min-width:180px"><strong>${dep.nombre}</strong><br>Lluvia: ${dep.lluvia_mm} mm<br>Temperatura: ${dep.temperatura_c} °C<br>Alerta: ${dep.nivel_alerta}<br>Presión: ${dep.presion_precio}</div>`
    );

    marker.on('click', () => {
      const url = new URL(window.location.href);
      url.searchParams.set('departamento', dep.slug);
      window.location.href = url.toString();
    });

    bounds.push([dep.lat, dep.lon]);
    if (dep.slug === selected) selectedMarker = marker;
  });

  if (bounds.length) map.fitBounds(bounds, { padding: [30, 30] });
  if (selectedMarker) {
    selectedMarker.openPopup();
    map.setView(selectedMarker.getLatLng(), 6.2);
  }
})();

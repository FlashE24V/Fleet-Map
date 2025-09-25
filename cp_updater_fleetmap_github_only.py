<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>NYC Fleet EV Map (Live via GitHub CSV)</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <link rel="stylesheet" href="https://unpkg.com/leaflet/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
  <script src="https://unpkg.com/papaparse@5.4.1/papaparse.min.js"></script>
  <style>
    html, body { height: 100%; margin: 0; }
    #map { height: 100vh; width: 100%; }
    .legend, .controls {
      background: white; padding: 8px 10px; border-radius: 8px;
      box-shadow: 0 1px 6px rgba(0,0,0,0.25); font: 14px/1.2 system-ui, -apple-system, Arial, sans-serif;
    }
    .legend .row { display: flex; align-items: center; margin-bottom: 4px; }
    .swatch { width: 12px; height: 12px; border-radius: 50%; margin-right: 8px; border: 1px solid #3333; }
    .controls label { display: block; margin: 3px 0; }

    .bolt-circle {
      width: 26px;
      height: 26px;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      border: 2px solid #222;
      color: #fff;
      font-size: 16px;
      line-height: 1;
      box-shadow: 0 1px 4px rgba(0,0,0,0.25);
      transform: translate(-1px, -1px);
    }

    /* COLORS BY CHARGER TYPE */
    .bolt-l3 { background: #16a34a; }     /* Level 3 → green */
    .bolt-l2 { background: #2563eb; }     /* Level 2 → blue */
    .bolt-solar { background: #facc15; color: #111; } /* Solar → yellow */
    .bolt-public { background: #f97316; } /* Public → orange */

    .leaflet-div-icon { background: transparent; border: none; }
  </style>
</head>
<body>
  <div id="map"></div>
  <script>
    var map = L.map('map').setView([40.7128, -74.0060], 11);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors'
    }).addTo(map);

    // CSV from GitHub Raw (cache-busted)
    var CSV_URL = 'https://raw.githubusercontent.com/FlashE24V/Fleet-Map/main/status_latest_slim.csv?v=' + Date.now();

    function pick(r, keys) {
      for (var i = 0; i < keys.length; i++) {
        var k = keys[i];
        if (r[k] !== undefined && r[k] !== null && r[k] !== '') return r[k];
      }
      return undefined;
    }

    // Normalize to: Available | In Use | Needs Service | Unreachable
    function normalizeStatusToInUse(s) {
      if (!s) return 'Unknown';
      var t = ('' + s).toUpperCase();
      if (t.includes('AVAILABLE')) return 'Available';
      if (t.includes('CHARGING')) return 'In Use';
      if (t.includes('OCCUPIED') || t === 'INUSE') return 'In Use';
      if (t.includes('NEEDS SERVICE') || t.includes('NEED SERVICE')) return 'Needs Service';
      if (t.includes('UNREACHABLE') || t.includes('UNAVAILABLE') || t.includes('DOWN') || t.includes('OFFLINE')) return 'Unreachable';
      return s;
    }

    function chargerTypeFromRow(r) {
      var v = pick(r, ['Charger type (legend)', 'Charger type']);
      if (!v) return 'Level 2';
      var s = ('' + v).toLowerCase();
      if (s.includes('level 3')) return 'Level 3';
      if (s.includes('solar')) return 'Level 2 Solar';
      return 'Level 2';
    }

    function isPublicStation(r) {
      var legend = pick(r, ['Charger type (legend)']);
      if (!legend) return false;
      var t = ('' + legend).toLowerCase();
      return t.indexOf('public stations') !== -1;
    }

    function classForRow(r) {
      if (isPublicStation(r)) return 'bolt-public';
      var ctype = chargerTypeFromRow(r);
      if (ctype === 'Level 3') return 'bolt-l3';
      if (ctype === 'Level 2 Solar') return 'bolt-solar';
      return 'bolt-l2';
    }

    var allRows = [];
    var markers = [];
    var layerGroup = L.layerGroup().addTo(map);

    // Filters (merged charging+occupied → "In Use"; no "Faulted", no "Model")
    var filterState = {
      'Available': true,
      'In Use': true,
      'Needs Service': true,
      'Unreachable': true
    };
    function passesFilter(statusNorm) {
      if (statusNorm in filterState) return !!filterState[statusNorm];
      return true;
    }

    function makePopup(name, statusText, lat, lon, countsHtml) {
      var apple = 'http://maps.apple.com/?daddr=' + lat + ',' + lon;
      var gmaps = 'https://www.google.com/maps/dir/?api=1&destination=' + lat + ',' + lon;
      var popup = '<div style="min-width:220px">'
                + '<strong>' + name + '</strong>'
                + (statusText ? '<br/>Status: ' + statusText : '')
                + (countsHtml ? '<br/>' + countsHtml : '')
                + '<br/><div style="margin-top:6px">'
                + '<a href="' + apple + '" target="_blank" rel="noopener">Directions (Apple)</a> &nbsp;|&nbsp; '
                + '<a href="' + gmaps + '" target="_blank" rel="noopener">Directions (Google)</a>'
                + '</div></div>';
      return popup;
    }

    function boltIconHTML(cls) {
      return '<div class="bolt-circle ' + cls + '">⚡</div>';
    }

    function render() {
      layerGroup.clearLayers();
      markers = [];
      var hasAny = false;

      allRows.forEach(function(r){
        var lat = pick(r, ['Lat','lat','Latitude']);
        var lon = pick(r, ['Long','Longitude','lon','long','Lng','lng']);
        if (typeof lat !== 'number' || typeof lon !== 'number' || isNaN(lat) || isNaN(lon)) return;

        var name  = pick(r, ['stationName','name','Location','Site']) || r.stationID || 'EV Site';

        // Prefer station_label then legacy fields
        var rawStatus = pick(r, ['station_label','LastPortStatus','StationNetworkStatus','status']);
        var statusNorm = normalizeStatusToInUse(rawStatus);
        if (!passesFilter(statusNorm)) return;

        var cls = classForRow(r);

        // Compact counts (one line) — In Use = charging + occupied
        var countsHtml = '';
        var pTot = pick(r, ['ports_total']);
        var pA   = pick(r, ['ports_available']);
        var pC   = pick(r, ['ports_charging']) || 0;
        var pO   = pick(r, ['ports_occupied']) || 0;
        var inUse = (typeof pC === 'number' ? pC : parseInt(pC || 0, 10)) +
                    (typeof pO === 'number' ? pO : parseInt(pO || 0, 10));

        if (pTot !== undefined && pTot !== null) {
          var parts = [];
          if (pA !== undefined) parts.push(pA + ' Available');
          parts.push(inUse + ' In Use'); // show merged number
          countsHtml = '<div style="margin-top:6px"><b>Ports:</b> ' + pTot + ' total'
                     + (parts.length ? ' (' + parts.join(' | ') + ')' : '')
                     + '</div>';
        }

        var icon = L.divIcon({
          className: 'bolt-divicon',
          iconSize: [26, 26],
          html: boltIconHTML(cls)
        });

        var marker = L.marker([lat, lon], { icon: icon })
          .bindPopup(makePopup(name, statusNorm, lat, lon, countsHtml));

        marker.addTo(layerGroup);
        markers.push(marker);
        hasAny = true;
      });

      if (hasAny) {
        var group = L.featureGroup(markers);
        map.fitBounds(group.getBounds().pad(0.15));
      }
    }

    function addLegend() {
      var legend = L.control({position: 'topleft'});
      legend.onAdd = function (map) {
        var div = L.DomUtil.create('div', 'legend');
        div.innerHTML = '<div class="row"><span class="swatch" style="background:#f97316"></span>Public</div>' +
                        '<div class="row"><span class="swatch" style="background:#2563eb"></span>Level 2</div>' +
                        '<div class="row"><span class="swatch" style="background:#facc15"></span>Level 2 Solar</div>' +
                        '<div class="row"><span class="swatch" style="background:#16a34a"></span>Level 3</div>';
        return div;
      };
      legend.addTo(map);

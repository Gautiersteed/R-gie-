#!/usr/bin/env node
/**
 * Live Encoder YouTube — v2
 * ==================================================================
 * Deux moteurs de diffusion, pilotables depuis le téléphone :
 *
 *  1. CAMÉRA   : navigateur -> WebSocket -> FFmpeg -> RTMP
 *  2. CLIPS    : le SERVEUR lit le fichier et le pousse seul en RTMP.
 *                => l'écran du téléphone peut être verrouillé ou éteint,
 *                   le téléphone peut perdre le réseau : la diffusion
 *                   continue côté serveur jusqu'à l'heure de fin.
 *
 * Clips : upload ou lien (yt-dlp), point de début / de fin, boucle,
 * planification (heure de début + durée ou heure de fin).
 * ==================================================================
 */

'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const os = require('os');
const { spawn, spawnSync } = require('child_process');
const { pipeline } = require('stream');

const PORT = Number(process.env.PORT || 3000);
const ROOT = __dirname;
const PUBLIC_DIR = path.join(ROOT, 'public');
const CLIPS_DIR = path.join(ROOT, 'clips');
const DATA_DIR = path.join(ROOT, 'data');
const REC_DIR = path.join(ROOT, 'recordings');
const CACHE_DIR = path.join(ROOT, 'data', 'cache');
const STATE_FILE = path.join(DATA_DIR, 'state.json');
const LIB_FILE = path.join(DATA_DIR, 'clips.json');

for (const d of [CLIPS_DIR, DATA_DIR, REC_DIR, CACHE_DIR]) {
  try { fs.mkdirSync(d, { recursive: true }); } catch {}
}

/* ================================================================== */
/* 0. Outils : FFmpeg / ffprobe                                       */
/* ================================================================== */

let FFMPEG = 'ffmpeg';
let FFPROBE = 'ffprobe';

function binOk(bin, args = ['-version']) {
  const r = spawnSync(bin, args, { stdio: 'ignore' });
  return r.status === 0;
}

function ensureFfmpeg() {
  if (binOk(FFMPEG)) {
    console.log('✓ FFmpeg détecté :', spawnSync(FFMPEG, ['-version']).stdout.toString().split('\n')[0]);
    return;
  }
  console.log('… FFmpeg absent, installation en cours (peut prendre 1–2 min).');
  const tries = [
    ['sudo', ['-n', 'apt-get', 'install', '-y', 'ffmpeg']],
    ['apt-get', ['install', '-y', 'ffmpeg']],
  ];
  for (const [cmd, args] of tries) {
    spawnSync(cmd, args, { stdio: 'ignore' });
    if (binOk(FFMPEG)) { console.log('✓ FFmpeg installé'); return; }
  }
  const local = path.join(ROOT, 'bin', 'ffmpeg');
  if (fs.existsSync(local)) { FFMPEG = local; FFPROBE = path.join(ROOT, 'bin', 'ffprobe'); }
  console.warn('⚠ FFmpeg introuvable : les diffusions ne démarreront pas.');
}

function probe(file) {
  const r = spawnSync(FFPROBE, [
    '-v', 'error', '-print_format', 'json',
    '-show_format', '-show_streams', file,
  ], { maxBuffer: 8 * 1024 * 1024 });
  if (r.status !== 0) throw new Error('Fichier illisible : ' + (r.stderr || '').toString().slice(0, 200));
  const info = JSON.parse(r.stdout.toString());
  const v = (info.streams || []).find((s) => s.codec_type === 'video');
  const a = (info.streams || []).find((s) => s.codec_type === 'audio');
  if (!v) throw new Error('Aucune piste vidéo dans ce fichier.');
  return {
    duration: Number(info.format && info.format.duration) || Number(v.duration) || 0,
    size: Number(info.format && info.format.size) || 0,
    width: v.width, height: v.height,
    videoCodec: v.codec_name,
    audioCodec: a ? a.codec_name : null,
    hasAudio: Boolean(a),
    fps: v.r_frame_rate || null,
  };
}

/* ================================================================== */
/* 1. Bibliothèque de clips (persistée)                               */
/* ================================================================== */

const clips = new Map(); // id -> clip

function loadLibrary() {
  try {
    const raw = JSON.parse(fs.readFileSync(LIB_FILE, 'utf8'));
    for (const c of raw) clips.set(c.id, c);
    console.log(`✓ Bibliothèque : ${clips.size} clip(s)`);
  } catch {}
}

function saveLibrary() {
  try {
    fs.writeFileSync(LIB_FILE, JSON.stringify([...clips.values()], null, 2));
  } catch (e) { console.warn('saveLibrary', e.message); }
}

function addClip(meta) {
  const clip = {
    id: crypto.randomBytes(4).toString('hex') + Date.now().toString(36).slice(-4),
    title: meta.title || 'Clip',
    kind: meta.kind,                 // 'upload' | 'link'
    source: meta.source || null,     // URL d'origine
    file: meta.file || null,         // chemin relatif
    start: meta.start || 0,          // point de début (s)
    end: meta.end || null,           // point de fin (s)
    loop: Boolean(meta.loop),
    duration: meta.duration || 0,
    videoCodec: meta.videoCodec || null,
    audioCodec: meta.audioCodec || null,
    width: meta.width || null,
    height: meta.height || null,
    hasAudio: meta.hasAudio !== false,
    size: meta.size || 0,
    notes: meta.notes || '',
    createdAt: Date.now(),
    absent: false,
  };
  clips.set(clip.id, clip);
  saveLibrary();
  return clip;
}

function purgeCache(clipId) {
  try {
    for (const f of fs.readdirSync(CACHE_DIR)) {
      if (f.startsWith(clipId + '__')) { try { fs.unlinkSync(path.join(CACHE_DIR, f)); } catch {} }
    }
  } catch {}
}

function pruneCache() {
  const maxAge = 7 * 24 * 3600 * 1000;
  let n = 0;
  try {
    for (const f of fs.readdirSync(CACHE_DIR)) {
      const abs = path.join(CACHE_DIR, f);
      try {
        if (Date.now() - fs.statSync(abs).mtimeMs > maxAge) { fs.unlinkSync(abs); n++; }
      } catch {}
    }
  } catch {}
  if (n) console.log(`✓ Cache nettoyé : ${n} clip(s) préparé(s) supprimé(s)`);
}

function clipPath(clip) {
  return clip.file ? path.join(ROOT, clip.file) : null;
}

/* ================================================================== */
/* 2. État de diffusion (persisté pour survivre à un redémarrage)      */
/* ================================================================== */

const live = {
  mode: null,          // 'camera' | 'clip'
  proc: null,
  clipId: null,
  options: null,
  startedAt: 0,
  scheduledEnd: 0,
  stopping: false,
  bytesIn: 0,
  loops: 0,
  streamKey: '',        // en mémoire vive uniquement
  ingest: 'rtmp://a.rtmp.youtube.com/live2',
  lastStats: null,
  stderrBuf: '',
  subscribers: new Set(),  // connexions WebSocket à informer

  // Relance automatique : après la fin d'une diffusion, on attend N secondes
  // puis on relance la même session, en boucle, jusqu'à un arrêt explicite.
  auto: {
    enabled: true,
    delaySec: 15,   // relance quasi immédiate quand la vidéo s'arrête
    timer: null,
    restartAt: 0,      // horodatage de la prochaine relance
    attempts: 0,       // nombre de relances enchaînées
    lastKey: '',       // mémorisé pour relancer à l'identique
    lastIngest: '',
    // Recyclage : coupe volontairement le flux toutes les N minutes pour
    // repartir sur un NOUVEAU direct (YouTube limite l'enregistrement à 12 h
    // et ne comptabilise pas correctement les vues des directs trop longs).
    autoBroadcast: true, // crée un nouveau direct YouTube avant chaque relance
    recycleMin: 660,   // 11 h par défaut ; 0 = désactivé
    recycleTimer: null,
    recycleAt: 0,      // horodatage du prochain recyclage
    recycles: 0,       // nombre de recyclages enchaînés
  },
  recycling: false,    // true pendant une coupure volontaire
  starting: false,     // true pendant une préparation (empêche les doubles départs)
};

/* ------------------------------------------------------------------ */
/* Relance automatique                                                 */
/* ------------------------------------------------------------------ */

function saveAutoSettings() {
  writeJsonFile(SETTINGS_FILE, {
    enabled: live.auto.enabled,
    delaySec: live.auto.delaySec,
    recycleMin: live.auto.recycleMin,
    autoBroadcast: Boolean(live.auto.autoBroadcast),
  });
}

function loadAutoSettings() {
  const d = readJsonFile(SETTINGS_FILE);
  if (!d) return;
  if (d.enabled !== undefined) live.auto.enabled = Boolean(d.enabled);
  if (d.delaySec !== undefined) live.auto.delaySec = Math.max(5, Math.min(3600, Number(d.delaySec) || 15));
  if (d.recycleMin !== undefined) live.auto.recycleMin = Math.max(0, Math.min(1440, Number(d.recycleMin) || 0));
  if (d.autoBroadcast !== undefined) live.auto.autoBroadcast = Boolean(d.autoBroadcast);
  console.log(`✓ Réglages relance : ${live.auto.enabled ? 'activée' : 'désactivée'} · délai ${live.auto.delaySec} s · renouvellement ${live.auto.recycleMin} min`);
}

function autoCancel(reason) {
  if (live.auto.timer) { clearTimeout(live.auto.timer); live.auto.timer = null; }
  const hadPending = Boolean(live.auto.restartAt);
  live.auto.restartAt = 0;
  if (hadPending && reason) logAll('info', 'Relance automatique annulée (' + reason + ').');
  broadcastState();
}

function broadcastState(extra) {
  broadcast({ type: 'state', state: publicState(), ...(extra || {}) });
}

/** Programme la relance de la session qui vient de se terminer. */
function autoSchedule(last) {
  if (!live.auto.enabled || !last || !live.auto.enabled) return false;
  if (live.auto.attempts >= 1000) {
    logAll('error', 'Relance automatique stoppée : 1000 relances enchaînées.');
    autoCancel();
    return false;
  }
  autoCancel();
  const delay = Math.max(1, Number(live.auto.delaySec) || 120);
  live.auto.restartAt = Date.now() + delay * 1000;
  logAll('info', `Nouvelle diffusion programmée dans ${fmtDelay(delay)} (relance automatique n°${live.auto.attempts + 1}).`);

  live.auto.timer = setTimeout(async () => {
    live.auto.timer = null;
    live.auto.restartAt = 0;
    live.auto.attempts++;
    logAll('info', `Relance automatique de la diffusion (n°${live.auto.attempts})…`);

    // Nouveau direct YouTube avant de relancer : c'est ce qui permet
    // d'enchaîner les cycles de 11 h sans aucune intervention.
    let clipOk = true;
    if (last.mode === 'clip') {
      const c = clips.get(last.clipId);
      clipOk = Boolean(c && c.file && fs.existsSync(clipPath(c)));
      if (!clipOk) {
        logAll('error', 'Le fichier du clip est introuvable : aucun nouveau direct créé.');
      }
    }

    if (live.auto.autoBroadcast && ytReady() && clipOk) {
      try {
        const b = await createLiveBroadcast();
        live.auto.lastKey = b.streamKey || live.auto.lastKey;
        live.auto.lastIngest = b.ingest || live.auto.lastIngest;
        logAll('ok', `Nouveau direct YouTube créé (relance n°${live.auto.attempts}) : ${b.watchUrl}`);
      } catch (e) {
        logAll('error', 'Création du nouveau direct impossible : ' + e.message
          + ' — la diffusion repart avec la clé précédente.');
      }
    }

    try {
      if (last.mode === 'clip') {
        await startClip(last.clipId, { ...last.options, streamKey: live.auto.lastKey || live.streamKey, ingest: live.auto.lastIngest || live.ingest }, false);
      } else {
        startCamera({ ...last.options, streamKey: live.auto.lastKey || live.streamKey, ingest: live.auto.lastIngest || live.ingest });
      }
      live.auto.failures = 0;
    } catch (e) {
      live.auto.failures = (live.auto.failures || 0) + 1;
      // Après 3 échecs d'affilée on s'arrête et on le dit clairement :
      // sinon le serveur tourne en boucle, crée des directs inutiles et sature le quota.
      if (live.auto.failures >= 3) {
        live.auto.enabled = false;
        live.auto.restartAt = 0;
        logAll('error', `Relance automatique ARRÊTÉE après 3 échecs : ${e.message}`);
        logAll('error', 'Corrige la cause (place disque, format, autorisation YouTube) '
          + 'puis appuie de nouveau sur « Diffuser ».');
      } else {
        logAll('error', `Relance automatique impossible (échec ${live.auto.failures}/3) : ${e.message}`);
        live.auto.restartAt = Date.now() + delay * 1000;
        live.auto.timer = setTimeout(() => autoSchedule(last), delay * 1000);
      }
    }
    broadcastState();
  }, delay * 1000);

  broadcastState();
  return true;
}

/* ---- Recyclage : nouveau direct toutes les N minutes ----------------- */

/** Programme la coupure volontaire du flux courant. */
function armRecycle() {
  cancelRecycle();
  if (!live.proc || !live.auto.enabled) return;
  if (!(live.auto.recycleMin > 0)) return;

  // Un clip déjà long (ex. 11 h) : inutile de le couper par le recyclage,
  // sa fin naturelle ouvre déjà un nouveau direct.
  if (live.mode === 'clip') {
    const c = live.clipId ? clips.get(live.clipId) : null;
    if (c && c.duration && !c.loop && c.duration >= live.auto.recycleMin * 60 * 0.9) {
      logAll('info', `Recyclage ignoré : le clip dure déjà ${fmtTime(c.duration)}. `
        + 'Sa fin naturelle ouvrira le nouveau direct (bien en dessous des 12 h).');
      broadcastState();
      return;
    }
  }

  const ms = live.auto.recycleMin * 60000;
  live.auto.recycleAt = Date.now() + ms;
  live.auto.recycleTimer = setTimeout(
    () => recycleNow('délai de ' + fmtDelay(live.auto.recycleMin * 60) + ' atteint'),
    Math.min(ms, 2 ** 31 - 1),
  );
  logAll('info', `Recyclage programmé dans ${fmtDelay(live.auto.recycleMin * 60)}`
    + ' : la diffusion sera relancée sur un nouveau direct.');
  broadcastState();
}

function cancelRecycle() {
  if (live.auto.recycleTimer) clearTimeout(live.auto.recycleTimer);
  live.auto.recycleTimer = null;
  live.auto.recycleAt = 0;
}

/** Coupe proprement FFmpeg sans marquer d'arrêt explicite → la relance prend le relais. */
function recycleNow(reason) {
  cancelRecycle();
  if (!live.proc) {
    logAll('warn', 'Recyclage ignoré : aucune diffusion en cours.');
    return false;
  }
  live.auto.recycles = (live.auto.recycles || 0) + 1;
  live.recycling = true;
  logAll('info', `Recyclage de la diffusion (n°${live.auto.recycles}`
    + (reason ? ' · ' + reason : '') + ') : coupure propre, puis nouveau direct.');
  const proc = live.proc;
  try { proc.kill('SIGINT'); } catch {}
  if (live.mode === 'camera') { try { proc.stdin.end(); } catch {} }
  setTimeout(() => { try { if (live.proc === proc) proc.kill('SIGKILL'); } catch {} }, 6000);
  broadcastState();
  return true;
}

function fmtDelay(sec) {
  sec = Math.round(sec);
  if (sec < 60) return sec + ' s';
  const m = Math.floor(sec / 60), r = sec % 60;
  return m + ' min' + (r ? ' ' + r + ' s' : '');
}

function persistState() {
  const snap = live.mode ? {
    mode: live.mode,
    clipId: live.clipId,
    options: live.options,
    startedAt: live.startedAt,
    scheduledEnd: live.scheduledEnd,
    streamKey: live.streamKey,
    ingest: live.ingest,
  } : null;
  try {
    if (snap) {
      fs.writeFileSync(STATE_FILE, JSON.stringify(snap), { mode: 0o600 });
      try { fs.chmodSync(STATE_FILE, 0o600); } catch {}
    } else if (fs.existsSync(STATE_FILE)) fs.unlinkSync(STATE_FILE);
  } catch {}
}

function broadcast(obj) {
  for (const ws of live.subscribers) {
    try { ws.sendJSON(obj); } catch {}
  }
}

const logRing = { items: [], seq: 0 };

function logAll(level, message) {
  const clean = redact(message);
  logRing.items.push({ seq: ++logRing.seq, t: Date.now(), level, message: clean });
  if (logRing.items.length > 300) logRing.items.shift();
  broadcast({ type: 'log', level, message: clean });
}

/* ================================================================== */
/* 3. Construction des arguments FFmpeg                               */
/* ================================================================== */

const PRESETS = {
  '1080p30': { height: 1080, fps: 30, vbr: 4500 },
  '720p30':  { height: 720,  fps: 30, vbr: 2500 },
  '720p60':  { height: 720,  fps: 60, vbr: 3500 },
  '480p30':  { height: 480,  fps: 30, vbr: 1000 },
  '360p30':  { height: 360,  fps: 30, vbr: 600 },
};

/* Formats d'image. 9:16 = vertical « Shorts / feed mobile ».
   Les dimensions sont calculées depuis la classe de qualité (720 / 1080). */
const RATIOS = {
  '16:9': { w: 16, h: 9 },
  '9:16': { w: 9, h: 16 },
  '1:1':  { w: 1, h: 1 },
  '4:5':  { w: 4, h: 5 },
};

function dimsFor(ratio, qualityHeight) {
  const r = RATIOS[ratio] || RATIOS['16:9'];
  const q = qualityHeight >= 1080 ? 1080 : (qualityHeight >= 720 ? 720 : 480);
  const shortSide = q;            // côté de référence = la hauteur 16:9
  let w, h;
  if (r.w >= r.h) {               // paysage (16:9)
    h = shortSide;
    w = Math.round(shortSide * r.w / r.h);
  } else if (r.w === r.h) {       // carré
    w = shortSide; h = shortSide;
  } else {                        // portrait (9:16, 4:5) : la largeur devient la référence
    w = shortSide;                        // 720x1280, 1080x1920, 720x900, 1080x1350
    h = Math.round(w * r.h / r.w);
  }
  const even = (n) => Math.max(2, Math.round(n / 2) * 2);
  return [even(w), even(h)];
}

function redact(text) {
  return String(text).replace(/(\/live2\/|\/stream\/)([^\s"']+)/g, (m, p) => p + '••••••••');
}

function encoderArgs(opts) {
  const preset = PRESETS[opts.preset] || PRESETS['720p30'];
  const fps = clamp(Number(opts.fps) || preset.fps, 5, 60);
  const quality = clamp(Number(opts.height) || preset.height, 144, 1440);
  const ratio = opts.ratio || '16:9';
  const [width, height] = dimsFor(ratio, quality);
  const vbr = clamp(Number(opts.videoBitrate) || preset.vbr, 200, 12000);
  const abr = clamp(Number(opts.audioBitrate) || 128, 64, 320);

  // Crop centré : remplit le cadre au lieu de le déformer (indispensable
  // pour passer du 16:9 caméra au 9:16 vertical sans bandes ni étirement).
  return [
    '-vf', `scale=${width}:${height}:force_original_aspect_ratio=increase:flags=fast_bilinear,`
         + `crop=${width}:${height},fps=${fps},format=yuv420p,setsar=1`,
    '-c:v', 'libx264',
    '-preset', 'veryfast',
    '-tune', 'zerolatency',
    '-profile:v', 'high',
    '-pix_fmt', 'yuv420p',
    '-g', String(fps * 2),
    '-keyint_min', String(fps * 2),
    '-sc_threshold', '0',
    '-b:v', `${vbr}k`,
    '-maxrate', `${vbr}k`,
    '-bufsize', `${vbr * 2}k`,
    '-c:a', 'aac',
    '-b:a', `${abr}k`,
    '-ar', '44100',
    '-ac', '2',
  ].map(String);
}

function clamp(n, a, b) { return Math.max(a, Math.min(b, n)); }

/* ================================================================== */
/* 4. Supervision du processus FFmpeg                                 */
/* ================================================================== */

function attachProcess(proc, opts) {
  live.proc = proc;
  live.recycling = false;
  live.startedAt = Date.now();
  live.bytesIn = 0;
  live.loops = 0;
  live.stopping = false;
  live.lastStats = null;

  proc.stderr.on('data', (d) => {
    live.stderrBuf += d.toString();
    // FFmpeg écrit ses statistiques avec \r : on coupe aussi sur \r pour ne
    // jamais accumuler des heures de progression dans une seule ligne.
    const lines = live.stderrBuf.split(/\r\n|\r|\n/);
    live.stderrBuf = lines.pop() || '';
    if (live.stderrBuf.length > 8192) live.stderrBuf = live.stderrBuf.slice(-4096);
    for (const line of lines) {
      if (!line.trim()) continue;
      if (/frame=\s*\d+/.test(line)) {
        const stat = parseProgress(line);
        if (stat) {
          live.lastStats = stat;
          broadcast({ type: 'stats', ...stat, uptime: uptime() });
        }
      } else if (/Input #0|Output #0|Stream mapping|Opening|Successfully opened|looping/i.test(line)) {
        logAll('info', line.trim());
        // Premier "Output #0" = le flux est réellement ouvert : on démarre
        // le compte à rebours du recyclage depuis ce moment-là.
        if (/Output #0/.test(line) && !live.auto.recycleAt) armRecycle();
      } else if (/error|failed|invalid|unable|refused|not found|No such|401|403|404/i.test(line)) {
        logAll('error', line.trim());
      }
    }
  });

  proc.on('error', (e) => logAll('error', 'FFmpeg : ' + e.message));

  proc.on('exit', (code, signal) => {
    const wasStopping = live.stopping;
    const wasRecycling = live.recycling;
    live.proc = null;
    live.stopping = false;
    live.recycling = false;
    cancelRecycle();

    // Boucle de clip : on relance jusqu'à l'heure de fin
    // Relance uniquement en cas de sortie anormale (jamais après un arrêt demandé)
    const loopMode = opts && opts.loop && live.mode === 'clip'
      && !live.endReached && !wasStopping && !wasRecycling
      && signal !== 'SIGINT' && signal !== 'SIGTERM';
    if (loopMode && code !== 0) {
      live.loops++;
      if (live.loops > 5000) { logAll('error', 'Boucle interrompue (limite atteinte).'); }
      else {
        logAll('info', `Bouclage du clip (tour ${live.loops + 1})…`);
        setTimeout(() => {
          startClip(live.clipId, { ...live.options, streamKey: live.streamKey, ingest: live.ingest }, true)
            .catch((e) => logAll('error', 'Relance impossible : ' + e.message));
        }, 400);
        return;
      }
    }

    if (wasRecycling) {
      logAll('info', 'Flux coupé pour recyclage : la relance automatique va ouvrir un nouveau direct.');
    } else if (!wasStopping) {
      logAll(code === 0 ? 'info' : 'error',
        code === 0 ? 'Diffusion terminée.' : `FFmpeg s'est arrêté (code ${code}${signal ? ' ' + signal : ''}).`);
    } else {
      logAll('info', 'Diffusion arrêtée proprement.');
    }

    const wasMode = live.mode;
    const lastSession = {
      mode: wasMode,
      clipId: live.clipId,
      options: { ...(live.options || {}) },
    };
    live.auto.lastMode = wasMode;
    live.auto.lastClipId = live.clipId;

    live.mode = null;
    live.clipId = null;
    live.scheduledEnd = 0;
    persistState();

    // Fin programmée = consigne explicite : on ne relance pas.
    const explicitEnd = wasStopping || live.endReached;
    let scheduled = false;
    if (!explicitEnd && lastSession.mode) {
      const duree = live.startedAt ? (Date.now() - live.startedAt) / 1000 : 0;
      const mort = !wasRecycling && duree < 20;
      if (mort) {
        live.auto.crashStreak = (live.auto.crashStreak || 0) + 1;
      } else {
        live.auto.crashStreak = 0;
      }
      if (live.auto.crashStreak >= 3) {
        live.auto.enabled = false;
        live.auto.crashStreak = 0;
        live.auto.restartAt = 0;
        saveAutoSettings();
        logAll('error', 'Boucle d\'échecs détectée (3 diffusions mortes en moins de 20 s) : '
          + 'relance automatique ARRÊTÉE.');
        logAll('error', 'Cause probable : fichier du clip déplacé ou supprimé, disque plein, '
          + 'format impossible à produire. Vérifie le journal, corrige, puis relance « Diffuser ».');
      } else {
        scheduled = autoSchedule(lastSession);
      }
    }

    broadcast({
      type: 'state',
      state: publicState(),
      endedMode: wasMode,
      uptime: uptime(),
      autoRestart: scheduled ? live.auto.restartAt : null,
    });
  });
}

function parseProgress(line) {
  const m = line.match(/frame=\s*(\d+).*?fps=\s*([\d.]+).*?(?:size=\s*([\d.]+\s*\w+))?.*?bitrate=\s*([\d.]+\s*\w+\/s)/);
  if (!m) return null;
  return { frame: Number(m[1]), fps: Number(m[2]) || 0, size: (m[3] || '').trim(), bitrate: (m[4] || '').trim() };
}

function uptime() { return live.startedAt ? Math.round((Date.now() - live.startedAt) / 1000) : 0; }

function publicState() {
  const clip = live.clipId ? clips.get(live.clipId) : null;
  return {
    live: Boolean(live.proc),
    mode: live.mode,
    clipId: live.clipId,
    clipTitle: clip ? clip.title : null,
    startedAt: live.startedAt || null,
    scheduledEnd: live.scheduledEnd || null,
    remaining: live.scheduledEnd ? Math.max(0, Math.round((live.scheduledEnd - Date.now()) / 1000)) : null,
    uptime: uptime(),
    loops: live.loops,
    options: live.options || null,
    stats: live.lastStats,
    nextStart: schedule.startAt || null,
    pendingClipId: schedule.clipId || null,
    auto: {
      enabled: live.auto.enabled,
      delaySec: live.auto.delaySec,
      restartAt: live.auto.restartAt || null,
      restartIn: live.auto.restartAt ? Math.max(0, Math.round((live.auto.restartAt - Date.now()) / 1000)) : null,
      attempts: live.auto.attempts,
      autoBroadcast: Boolean(live.auto.autoBroadcast),
      ytReady: ytReady(),
      recycleMin: live.auto.recycleMin,
      recycleAt: live.auto.recycleAt || null,
      recycleIn: live.auto.recycleAt
        ? Math.max(0, Math.round((live.auto.recycleAt - Date.now()) / 1000)) : null,
      recycles: live.auto.recycles || 0,
      recycling: Boolean(live.recycling),
    },
  };
}

/* ================================================================== */
/* 5. Moteur CAMÉRA (navigateur -> serveur)                           */
/* ================================================================== */

function startCamera(opts) {
  if (live.proc) throw new Error('Une diffusion est déjà en cours.');

  const container = opts.container === 'mp4' ? 'mp4' : 'webm';
  const mode = opts.mode === 'file' ? 'file' : 'youtube';
  const key = String(opts.streamKey || '').trim();
  const ingest = String(opts.ingest || 'rtmp://a.rtmp.youtube.com/live2').replace(/\/+$/, '');
  if (mode === 'youtube' && !key) throw new Error('Clé de flux YouTube manquante.');

  const args = ['-hide_banner', '-loglevel', 'info', '-nostdin', '-fflags', '+genpts', '-f', container, '-i', 'pipe:0'];
  args.push(...encoderArgs(opts));

  let fileUrl = null;
  if (mode === 'file') {
    const name = `test-${new Date().toISOString().replace(/[:.]/g, '-')}.mp4`;
    args.push('-movflags', '+faststart', path.join(REC_DIR, name));
    fileUrl = '/recordings/' + name;
  } else {
    args.push('-f', 'flv', '-flvflags', 'no_duration_filesize', `${ingest}/${key}`);
  }

  live.mode = 'camera';
  live.clipId = null;
  live.options = { ...opts, streamKey: undefined, mode, preset: opts.preset };

  // Fin de diffusion programmable (durée en minutes ou heure absolue)
  if (opts.untilTs) live.scheduledEnd = Number(opts.untilTs);
  else if (opts.durationMin) live.scheduledEnd = Date.now() + Number(opts.durationMin) * 60000;
  else live.scheduledEnd = 0;
  armEndTimer();

  const proc = spawn(FFMPEG, args, { stdio: ['pipe', 'ignore', 'pipe'] });
  attachProcess(proc, null);
  persistState();

  live.auto.lastKey = key;
  live.auto.lastIngest = ingest;
  logAll('info', `Caméra → ${mode === 'file' ? 'fichier de test' : redact(`${ingest}/${key}`)}`);
  return { fileUrl, target: mode === 'file' ? 'fichier local' : redact(`${ingest}/${key}`) };
}

/* ================================================================== */
/* 6. Moteur CLIP (serveur autonome)                                  */
/* ================================================================== */
/*
 * Principe : le clip est d'abord "préparé" une fois (extrait début→fin,
 * ré-encodé aux réglages YouTube, keyframes toutes les 2 s) puis mis en
 * cache. La diffusion réelle se fait ensuite en `-c copy` : le serveur
 * ne fait que pousser des octets, donc un CPU quasi nul et aucune
 * dérive de qualité, même sur un petit serveur.
 */

const schedule = { timer: null, startAt: 0, clipId: null, options: null };

function cachePath(clip, opts) {
  const preset = PRESETS[opts.preset] || PRESETS['720p30'];
  const fps = clamp(Number(opts.fps) || preset.fps, 5, 60);
  const height = clamp(Number(opts.height) || preset.height, 144, 1440);
  const vbr = clamp(Number(opts.videoBitrate) || preset.vbr, 200, 12000);
  const s = Number(clip.start) || 0;
  const e = clip.end || clip.duration || 0;
  const [cw, ch] = dimsFor(opts.ratio || '16:9', height);
  return path.join(CACHE_DIR, `${clip.id}__${cw}x${ch}_${Math.round(fps)}_${Math.round(vbr)}k_${s.toFixed(1)}-${e.toFixed(1)}.mp4`);
}

/** Prépare (ou récupère en cache) le clip aux réglages cibles. */
/**
 * Peut-on diffuser le fichier tel quel, sans le ré-encoder ?
 * Indispensable pour les très longues vidéos (11 h) : cela évite un fichier
 * de cache de plusieurs Go et des heures de préparation.
 */
const DIRECT_AUDIO = new Set(['aac', 'mp3']);

/**
 * Le clip a-t-il déjà le rapport d'image demandé ?
 * Tolérance 5 % (une vidéo 312x570 = 0,547 est « assez verticale » pour du 9:16),
 * et on n'agrandit JAMAIS une vidéo : upscaler 11 h pour rien coûte des heures de CPU.
 */
function ratioMatches(clip, ratio, opts = {}) {
  if (!ratio || ratio === 'auto') return true;
  const r = RATIOS[ratio];
  if (!r || !clip.width || !clip.height) return true;
  const src = clip.width / clip.height;
  const want = r.w / r.h;
  if (Math.abs(src - want) / want < 0.05) return true;

  const preset = PRESETS[opts.preset] || PRESETS['720p30'];
  const targetH = clamp(Number(opts.height) || preset.height, 144, 1440);
  if (clip.height < targetH) {
    logAll('info', `Vidéo ${clip.width}×${clip.height} plus petite que le format visé `
      + `(${targetH}px de haut) : elle est diffusée telle quelle, sans agrandissement inutile.`);
    return true;
  }
  return false;
}

/* ---- Alerte e-mail (Gmail SMTP via curl) ---- */
const MAIL_FILE = path.join(DATA_DIR, 'mail.conf');

function mailConf() {
  try {
    const txt = fs.readFileSync(MAIL_FILE, 'utf8');
    const out = {};
    for (const l of txt.split('\n')) {
      const m = l.match(/^\s*([A-Z_]+)=(.*)$/);
      if (m) out[m[1]] = m[2].trim();
    }
    return out;
  } catch { return {}; }
}

function mailReady() {
  const c = mailConf();
  return Boolean(c.GMAIL_USER && c.GMAIL_APP_PASSWORD && c.MAIL_TO);
}

function sendMail(subject, body) {
  const c = mailConf();
  if (!c.GMAIL_USER || !c.GMAIL_APP_PASSWORD || !c.MAIL_TO) {
    return { ok: false, error: 'Alerte e-mail non configurée.' };
  }
  const msg = [
    'From: Encodeur live <' + c.GMAIL_USER + '>',
    'To: ' + c.MAIL_TO,
    'Subject: [Encodeur] ' + String(subject).replace(/[\r\n]/g, ' '),
    'Date: ' + new Date().toUTCString(),
    'MIME-Version: 1.0',
    'Content-Type: text/plain; charset=UTF-8',
    '',
    String(body || ''),
    '',
    '---',
    'Serveur : ' + os.hostname() + ' · ' + new Date().toLocaleString('fr-FR'),
    'Direct  : ' + (live.proc ? 'en cours (' + (live.clipId ? (clips.get(live.clipId) || {}).title : '') + ')' : 'à l\'arrêt'),
    'https://live-gautier.duckdns.org',
  ].join('\r\n');

  const tmp = path.join(DATA_DIR, 'mail-tmp-' + Date.now() + '.txt');
  try { fs.writeFileSync(tmp, msg, { mode: 0o600 }); } catch (e) { return { ok: false, error: e.message }; }
  const r = spawnSync('curl', [
    '-sS', '--url', 'smtps://smtp.gmail.com:465', '--ssl-reqd',
    '--user', c.GMAIL_USER + ':' + c.GMAIL_APP_PASSWORD,
    '--mail-from', c.GMAIL_USER, '--mail-rcpt', c.MAIL_TO,
    '--upload-file', tmp, '--max-time', '30',
  ], { encoding: 'utf8' });
  try { fs.unlinkSync(tmp); } catch {}
  if (r.status === 0) {
    logAll('ok', 'Alerte e-mail envoyée à ' + c.MAIL_TO);
    return { ok: true };
  }
  const brut = (r.stderr || r.stdout || '').trim();
  const connus = {
    6: 'serveur Gmail introuvable (DNS) — le VPS a-t-il accès au réseau ?',
    7: 'connexion au port 465 refusée (pare-feu du VPS ?)',
    35: 'handshake TLS impossible',
    55: 'envoi interrompu par le serveur',
    56: 'connexion interrompue',
    67: 'mot de passe d\'application refusé par Google — vérifie les 16 caractères et la validation en 2 étapes',
    28: 'délai dépassé en contactant Gmail',
  };
  const expl = connus[r.status] || brut || ('curl ' + r.status);
  const motif = brut && !connus[r.status] ? brut.slice(0, 300) : expl;
  logAll('error', 'Alerte e-mail refusée : ' + motif);
  return { ok: false, error: motif };
}

process.on('uncaughtException', (e) => {
  logAll('error', 'Erreur interne : ' + e.message);
  if (mailReady()) {
    try { sendMail('Encodeur : erreur interne', 'Le serveur a rencontré une erreur inattendue :\n' + e.message + '\n\nLe service continue de tourner.'); } catch {}
  }
});

function fmtBytes(b) {
  if (!Number.isFinite(b)) return '∞';
  const u = ['o', 'Ko', 'Mo', 'Go', 'To'];
  let i = 0, n = b;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return n.toFixed(n >= 10 || i === 0 ? 0 : 1) + ' ' + u[i];
}

/** Estimation de la taille du fichier préparé (octets). */
function estimatePrepareBytes(clip, opts) {
  const preset = PRESETS[opts.preset] || PRESETS['720p30'];
  const vbr = clamp(Number(opts.videoBitrate) || preset.vbr, 200, 12000) * 1000;
  const dur = Math.max(1, (clip.end || clip.duration || 1) - (Number(clip.start) || 0));
  return Math.round((vbr + 128000) / 8 * dur * 1.05);
}

function canDirectPlay(clip) {
  if (!clip || !clip.duration) return false;
  // Découpe demandée → il faut réencoder
  const s0 = Number(clip.start) || 0;
  const end = clip.end || clip.duration;
  if (s0 > 0.5 || Math.abs(end - clip.duration) > 1) return false;
  // Le fichier doit être lisible et déjà compatible YouTube
  if (!clip.videoCodec) {
    try {
      const src = clipPath(clip);
      if (!src || !fs.existsSync(src)) return false;
      const info = probe(src);
      clip.videoCodec = info.videoCodec;
      clip.audioCodec = info.audioCodec;
      clip.width = info.width;
      clip.height = info.height;
      clip.duration = clip.duration || info.duration;
      saveLibrary();
    } catch { return false; }
  }
  if (clip.videoCodec !== 'h264') return false;
  if (!clip.hasAudio || !DIRECT_AUDIO.has(clip.audioCodec)) return false;
  if (!clip.width || !clip.height) return false;
  if (clip.width > 1920 || clip.height > 1920) return false;
  return true;
}

function prepareClip(clip, opts, out) {
  return new Promise((resolve, reject) => {
    // Un cache existant n'est réutilisé QUE s'il est lisible en entier.
    // (Avant : un fichier interrompu était pris pour un clip prêt -> « moov atom not found ».)
    if (fs.existsSync(out) && fs.statSync(out).size > 1024) {
      try {
        probe(out);
        return resolve(out);
      } catch {
        try { fs.unlinkSync(out); } catch {}
        logAll('warn', 'Fichier préparé incomplet détecté : il est supprimé et refait proprement.');
      }
    }

    const src = clipPath(clip);
    if (!src || !fs.existsSync(src)) return reject(new Error('Fichier du clip absent du disque.'));

    const startPos = Math.max(0, Number(clip.start) || 0);
    const endPos = clip.end || clip.duration || 0;
    const len = Math.max(0.5, endPos - startPos);

    logAll('info', `Préparation du clip « ${clip.title} » (${fmtTime(startPos)} → ${fmtTime(endPos)}, ${fmtTime(len)})…`);

    const args = ['-hide_banner', '-loglevel', 'error', '-y'];
    if (startPos > 0) args.push('-ss', String(startPos));
    args.push('-i', src);
    if (endPos) args.push('-to', String(endPos));

    if (clip.hasAudio) {
      args.push('-map', '0:v:0', '-map', '0:a:0');
    } else {
      args.push('-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100');
      args.push('-map', '0:v:0', '-map', '1:a:0', '-shortest');
    }
    args.push(...encoderArgs(opts));
    // On écrit dans un fichier temporaire : le cache final n'apparaît que complet.
    const tmp = out.replace(/\.mp4$/i, '') + '.part.mp4';
    try { fs.unlinkSync(tmp); } catch {}
    args.push('-movflags', '+faststart', tmp);

    const t0 = Date.now();
    const proc = spawn(FFMPEG, args, { stdio: ['ignore', 'ignore', 'pipe'] });
    let err = '';
    proc.stderr.on('data', (d) => { err += d.toString(); });
    proc.on('exit', (code) => {
      if (code === 0 && fs.existsSync(tmp)) {
        try {
          probe(tmp);                       // contrôle final avant de publier
          fs.renameSync(tmp, out);
        } catch (e) {
          try { fs.unlinkSync(tmp); } catch {}
          return reject(new Error('Préparation invalide : ' + e.message));
        }
        logAll('ok', `Clip prêt en ${((Date.now() - t0) / 1000).toFixed(1)} s · diffusion à coût CPU nul.`);
        resolve(out);
      } else {
        try { fs.unlinkSync(tmp); } catch {}
        const motif = err.trim().split('\n').pop() || ('code ' + code);
        reject(new Error('Préparation impossible : ' + motif
          + (/No space left/i.test(err) ? ' — disque plein' : '')));
      }
    });
  });
}

/**
 * Démarre la diffusion d'un clip depuis le serveur.
 * @param {string} clipId
 * @param {object} options { preset, fps, height, streamKey, ingest, mode, durationMin, untilTs }
 * @param {boolean} reloop relance interne après sortie du process
 */
async function startClip(clipId, options, reloop = false) {
  if (live.starting) throw new Error('Une préparation est déjà en cours : patiente quelques instants.');
  const clip = clips.get(clipId);
  if (!clip) throw new Error('Clip introuvable.');
  if (!clip.file) throw new Error('Ce clip est un lien : le téléchargement n\'est pas terminé.');

  const src0 = clipPath(clip);
  if (!src0 || !fs.existsSync(src0)) {
    throw new Error(`Fichier du clip introuvable sur le serveur (${clip.file || 'chemin absent'}) : `
      + 'le clip a peut-être été déplacé ou supprimé.');
  }

  const opts = { ...options };
  const key = String(opts.streamKey || live.streamKey || '').trim();
  const ingest = String(opts.ingest || live.ingest || 'rtmp://a.rtmp.youtube.com/live2').replace(/\/+$/, '');
  const mode = opts.mode === 'file' ? 'file' : 'youtube';
  if (mode === 'youtube' && !key) throw new Error('Clé de flux YouTube manquante.');
  if (live.proc) throw new Error('Une diffusion est déjà en cours.');

  // 1) Préparation / cache — ou lecture directe pour les vidéos déjà conformes
  const direct = canDirectPlay(clip) && ratioMatches(clip, opts.ratio, opts);
  if (canDirectPlay(clip) && !direct) {
    logAll('info', `Format demandé ${opts.ratio} alors que la vidéo est en `
      + `${clip.width}×${clip.height} : conversion en une passe (peut être long sur une très longue vidéo).`);
  }
  let prepared;
  if (direct) {
    prepared = clipPath(clip);
    logAll('ok', `Lecture directe de « ${clip.title} » (${clip.videoCodec}/${clip.audioCodec}, `
      + `${clip.width}×${clip.height}) : aucun ré-encodage, aucun fichier temporaire.`);
  } else {
    // Place disque : une conversion de plusieurs heures demande plusieurs Go.
    const besoin = estimatePrepareBytes(clip, opts);
    let libre = Infinity;
    try {
      const st = fs.statfsSync(ROOT);
      libre = Number(st.bavail) * Number(st.bsize);
    } catch {}
    if (libre < besoin * 1.15) {
      throw new Error(`Conversion impossible : ${fmtBytes(besoin)} nécessaires, `
        + `${fmtBytes(libre)} libres. Choisis le format « Automatique » `
        + `ou libère de la place dans /opt/live-encoder/data/cache.`);
    }
    logAll('info', `Conversion nécessaire : environ ${fmtBytes(besoin)} de cache `
      + `pour une vidéo de ${fmtTime(clip.duration)}.`);
    live.starting = true;
    try {
      prepared = await prepareClip(clip, opts, cachePath(clip, opts));
    } finally {
      live.starting = false;
    }
  }

  // 2) Montage du flux à partir du fichier préparé
  const args = ['-hide_banner', '-loglevel', 'info', '-nostdin', '-re'];
  if (clip.loop) args.push('-stream_loop', '-1');
  args.push('-i', prepared, '-c', 'copy');
  if (mode === 'file') {
    const name = `clip-${clipId}-${Date.now()}.mp4`;
    args.push('-movflags', '+faststart', path.join(REC_DIR, name));
  } else {
    args.push('-f', 'flv', '-flvflags', 'no_duration_filesize', `${ingest}/${key}`);
  }

  // 3) État
  live.mode = 'clip';
  live.clipId = clipId;
  live.streamKey = key;
  live.ingest = ingest;
  live.options = {
    preset: opts.preset, fps: opts.fps, height: opts.height,
    durationMin: opts.durationMin || null, mode,
  };
  live.endReached = false;

  if (!reloop || !live.scheduledEnd) {
    if (opts.untilTs) live.scheduledEnd = Number(opts.untilTs);
    else if (opts.durationMin) live.scheduledEnd = Date.now() + Number(opts.durationMin) * 60000;
    else if (!clip.loop) live.scheduledEnd = 0;
    else live.scheduledEnd = 0;
  }

  const proc = spawn(FFMPEG, args, { stdio: ['ignore', 'ignore', 'pipe'] });
  attachProcess(proc, { loop: Boolean(clip.loop) });
  persistState();

  live.auto.lastKey = key;
  live.auto.lastIngest = ingest;

  broadcast({ type: 'state', state: publicState(), note: reloop ? 'reloop' : 'start' });
  logAll('ok', `EN DIRECT · clip « ${clip.title} »`
    + (clip.loop ? ' · boucle' : '')
    + (live.scheduledEnd ? ` · fin prévue ${new Date(live.scheduledEnd).toLocaleTimeString('fr-FR')}` : ''));

  armEndTimer();
  return { ok: true };
}

function fmtTime(s) {
  s = Math.max(0, Math.floor(s || 0));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
  return (h ? h + ':' + String(m).padStart(2, '0') : String(m)) + ':' + String(sec).padStart(2, '0');
}

/* ---- Minuteries : début planifié et fin de diffusion ---------------- */

function armStartTimer(startAt, clipId, options) {
  clearTimeout(schedule.timer);
  schedule.startAt = startAt;
  schedule.clipId = clipId;
  schedule.options = options;

  const delay = Math.max(0, startAt - Date.now());
  if (delay > 2 ** 31 - 1) return;

  schedule.timer = setTimeout(() => {
    logAll('info', 'Heure de début atteinte : lancement automatique.');
    schedule.startAt = 0;
    const cid = clipId, opt = options;
    schedule.clipId = null;
    startClip(cid, opt).catch((e) => logAll('error', 'Démarrage planifié impossible : ' + e.message));
    persistState();
  }, delay);

  broadcast({ type: 'state', state: publicState() });
  logAll('info', `Démarrage planifié le ${new Date(startAt).toLocaleString('fr-FR')}.`);
}

function armEndTimer() {
  clearTimeout(live.endTimer);
  if (!live.scheduledEnd) return;
  const delay = Math.max(0, live.scheduledEnd - Date.now()) + 500;

  live.endTimer = setTimeout(() => {
    logAll('info', 'Heure de fin atteinte : arrêt de la diffusion.');
    live.endReached = true;
    stopLive();
  }, Math.min(delay, 2 ** 31 - 1));
}

function stopLive() {
  // Un arrêt demandé par l'utilisateur met fin à la relance automatique
  // et au recyclage.
  autoCancel();
  cancelRecycle();
  live.auto.attempts = 0;
  live.auto.recycles = 0;
  if (!live.proc) {
    cancelSchedule();
    return false;
  }
  live.stopping = true;
  const proc = live.proc;

  // Arrêt propre : SIGINT laisse FFmpeg vider son tampon vers YouTube
  try { proc.kill('SIGINT'); } catch {}
  if (live.mode === 'camera') { try { proc.stdin.end(); } catch {} }
  setTimeout(() => { try { if (live.proc === proc) proc.kill('SIGKILL'); } catch {} }, 6000);
  return true;
}

function cancelSchedule() {
  clearTimeout(schedule.timer);
  schedule.startAt = 0;
  schedule.clipId = null;
  schedule.options = null;
  broadcast({ type: 'state', state: publicState() });
  logAll('info', 'Planification annulée.');
}

/* ---- Téléchargements (liens) ---------------------------------------- */

let YTDLP = 'yt-dlp';

function ytdlp() {
  return spawnSync(YTDLP, ['--version'], { stdio: 'ignore' }).status === 0;
}

/** Installe yt-dlp si absent : pip, puis binaire statique. */
function ensureYtdlp() {
  if (ytdlp()) { console.log('✓ yt-dlp détecté'); return; }

  console.log('… yt-dlp absent, installation (ajout par lien).');
  const tries = [
    ['python3', ['-m', 'pip', 'install', '--quiet', '--user', 'yt-dlp']],
    ['pip3', ['install', '--quiet', '--user', 'yt-dlp']],
  ];
  for (const [cmd, args] of tries) {
    spawnSync(cmd, args, { stdio: 'ignore' });
    if (ytdlp()) { console.log('✓ yt-dlp installé via ' + cmd); return; }
  }

  // Binaire statique officiel
  try {
    const bin = path.join(ROOT, 'bin', 'yt-dlp');
    fs.mkdirSync(path.dirname(bin), { recursive: true });
    spawnSync('curl', ['-sL', '--max-time', '120', '-o', bin,
      'https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp'], { stdio: 'ignore' });
    spawnSync('chmod', ['+x', bin], { stdio: 'ignore' });
    if (spawnSync(bin, ['--version'], { stdio: 'ignore' }).status === 0) {
      YTDLP = bin;
      console.log('✓ yt-dlp installé (binaire local)');
      return;
    }
  } catch {}

  console.warn('⚠ yt-dlp indisponible : upload de fichiers uniquement.');
}

function downloadLink(url, clipId, onDone) {
  if (!ytdlp()) {
    return onDone(new Error('yt-dlp n\'est pas installé sur le serveur. Installe-le (pip install yt-dlp) ou upload le fichier.'));
  }
  const outTpl = path.join(CLIPS_DIR, `${clipId}.%(ext)s`);
  const args = [
    '-f', 'bv*[height<=1080]+ba/b[height<=1080]/bv*+ba/b',
    '--merge-output-format', 'mp4',
    '--no-playlist',
    '--no-progress', '--newline',
    '--retries', '3',
    // Depuis 2026, YouTube exige un runtime JS pour l'extraction
    '--js-runtimes', 'deno', '--js-runtimes', 'node',
  ];
  // Cookies facultatifs (YouTube bloque parfois les IP de datacenter)
  const cookies = path.join(DATA_DIR, 'cookies.txt');
  if (fs.existsSync(cookies)) args.push('--cookies', cookies);
  args.push('-o', outTpl, url);
  const p = spawn(YTDLP, args, { stdio: ['ignore', 'pipe', 'pipe'] });
  let lastPct = -10;
  let lastError = '';

  const onLine = (buf) => {
    const txt = buf.toString();
    const m = txt.match(/\[download\]\s+([\d.]+)%/);
    if (m) {
      const pct = Math.round(Number(m[1]));
      if (pct >= lastPct + 5) {
        lastPct = pct;
        broadcast({ type: 'download', clipId, progress: pct });
      }
    }
    const e = txt.match(/ERROR:\s*(.+)/);
    if (e) {
      lastError = e[1].slice(0, 300);
      broadcast({ type: 'log', level: 'error', message: 'Téléchargement : ' + lastError });
    }
  };
  p.stdout.on('data', onLine);
  p.stderr.on('data', onLine);

  p.on('exit', (code) => {
    if (code !== 0) {
      return onDone(new Error(lastError || 'Téléchargement impossible (code ' + code + ').'));
    }
    const found = fs.readdirSync(CLIPS_DIR).filter((f) => f.startsWith(clipId + '.'));
    if (!found.length) return onDone(new Error('Aucun fichier téléchargé.'));
    onDone(null, path.join(CLIPS_DIR, found[0]));
  });
}

/* ---- Restauration après redémarrage --------------------------------- */

function restoreState() {
  let snap;
  try { snap = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch { return; }
  if (!snap || snap.mode !== 'clip' || !snap.clipId) return;

  const clip = clips.get(snap.clipId);
  if (!clip) return;

  if (snap.scheduledEnd && snap.scheduledEnd < Date.now()) {
    console.log('• Diffusion expirée pendant l\'arrêt : rien à reprendre.');
    try { fs.unlinkSync(STATE_FILE); } catch {}
    return;
  }
  console.log('↻ Reprise de la diffusion du clip « ' + clip.title + ' » après redémarrage.');
  live.scheduledEnd = snap.scheduledEnd || 0;
  startClip(snap.clipId, {
    ...(snap.options || {}),
    streamKey: snap.streamKey,
    ingest: snap.ingest,
    untilTs: snap.scheduledEnd || null,
  }, true).catch((e) => console.warn('Reprise impossible :', e.message));
}

/* ================================================================== */
/* 7. Mini serveur WebSocket (RFC 6455)                               */
/* ================================================================== */

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

class WSConnection {
  constructor(socket) {
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.fragments = [];
    this.fragOpcode = 0;
    this.alive = true;
    this.handlers = { message: [], close: [] };
    socket.on('data', (d) => this._onData(d));
    socket.on('error', () => this._closed());
    socket.on('close', () => this._closed());
  }
  on(evt, fn) { this.handlers[evt] && this.handlers[evt].push(fn); return this; }
  _emit(evt, ...a) { (this.handlers[evt] || []).forEach((f) => f(...a)); }
  _closed() { if (this.alive) { this.alive = false; this._emit('close'); } }

  _onData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (this._parseFrame()) { /* continue */ }
  }

  _parseFrame() {
    const b = this.buffer;
    if (b.length < 2) return false;
    const fin = (b[0] & 0x80) !== 0;
    const opcode = b[0] & 0x0f;
    const masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f;
    let offset = 2;
    if (len === 126) { if (b.length < 4) return false; len = b.readUInt16BE(2); offset = 4; }
    else if (len === 127) {
      if (b.length < 10) return false;
      len = b.readUInt32BE(2) * 4294967296 + b.readUInt32BE(6); offset = 10;
    }
    if (masked) { if (b.length < offset + 4) return false; offset += 4; }
    if (b.length < offset + len) return false;

    let payload = b.subarray(offset, offset + len);
    if (masked) {
      const mask = b.subarray(offset - 4, offset);
      payload = Buffer.from(payload);
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
    }
    this.buffer = b.subarray(offset + len);

    if (opcode === 0x8) { this.close(); return false; }
    if (opcode === 0x9) { this._frame(0xa, payload); return true; }
    if (opcode === 0xa) return true;

    if (opcode === 0x0) {
      this.fragments.push(payload);
      if (fin) { const full = Buffer.concat(this.fragments); this.fragments = []; this._deliver(this.fragOpcode, full); }
      return true;
    }
    if (!fin) { this.fragOpcode = opcode; this.fragments = [payload]; return true; }
    this._deliver(opcode, payload);
    return true;
  }

  _deliver(opcode, payload) {
    if (opcode === 0x1) this._emit('message', payload.toString('utf8'), false);
    else if (opcode === 0x2) this._emit('message', payload, true);
  }

  _frame(opcode, payload) {
    if (!this.alive) return;
    const len = payload.length;
    let header;
    if (len < 126) { header = Buffer.alloc(2); header[1] = len; }
    else if (len < 65536) { header = Buffer.alloc(4); header[1] = 126; header.writeUInt16BE(len, 2); }
    else {
      header = Buffer.alloc(10); header[1] = 127;
      header.writeUInt32BE(Math.floor(len / 4294967296), 2);
      header.writeUInt32BE(len % 4294967296, 6);
    }
    header[0] = 0x80 | opcode;
    try { this.socket.write(Buffer.concat([header, payload])); } catch { this._closed(); }
  }

  sendJSON(obj) { this._frame(0x1, Buffer.from(JSON.stringify(obj))); }
  close() { this._frame(0x8, Buffer.alloc(0)); try { this.socket.end(); } catch {} this._closed(); }
  pauseRead() { try { this.socket.pause(); } catch {} }
  resumeRead() { try { this.socket.resume(); } catch {} }
}

/* ================================================================== */
/* 8. HTTP : statique + API clips                                     */
/* ================================================================== */

const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml', '.mp4': 'video/mp4', '.webm': 'video/webm', '.png': 'image/png',
};

/* ================================================================== */
/* YouTube : OAuth côté serveur (autorisation unique + jeton durable) */
/* ================================================================== */
/*
 * Principe : l'utilisateur autorise UNE fois dans son navigateur. Le serveur
 * reçoit un jeton de rafraîchissement, le garde sur le disque et crée tout
 * seul un nouveau direct à chaque cycle. Aucune reconnexion horaire.
 */

const OAUTH_FILE = path.join(DATA_DIR, 'oauth.json');
const TOKEN_FILE = path.join(DATA_DIR, 'yt-token.json');
const YT_STREAM_FILE = path.join(DATA_DIR, 'yt-stream.json'); // flux réutilisable (clé stable)
const oauthStates = new Map();

function readJsonFile(f) { try { return JSON.parse(fs.readFileSync(f, 'utf8')); } catch { return null; } }
function writeJsonFile(f, o) {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(f, JSON.stringify(o, null, 2), { mode: 0o600 });
    return true;
  } catch { return false; }
}

function oauthCfg() { return readJsonFile(OAUTH_FILE) || {}; }
function oauthTokens() { return readJsonFile(TOKEN_FILE) || {}; }

function publicBase(req) {
  const fwd = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
  const proto = fwd || (req.socket && req.socket.encrypted ? 'https' : 'http');
  const host = String(req.headers['x-forwarded-host'] || req.headers.host || '').trim();
  return proto + '://' + host;
}

function redirectUri(req) {
  return oauthCfg().redirectUri || (publicBase(req) + '/oauth/callback');
}

function ytReady() {
  const c = oauthCfg();
  return Boolean(c.clientId && c.clientSecret && oauthTokens().refresh_token);
}

/** Échange le code d'autorisation contre des jetons durables. */
async function exchangeCode(code, req) {
  const c = oauthCfg();
  const body = new URLSearchParams({
    code,
    client_id: c.clientId,
    client_secret: c.clientSecret,
    redirect_uri: c.redirectUri || redirectUri(req),
    grant_type: 'authorization_code',
  });
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
  });
  const d = await r.json();
  if (!r.ok || !d.access_token) {
    throw new Error(d.error_description || d.error || 'échange refusé');
  }
  const old = oauthTokens();
  const tok = {
    access_token: d.access_token,
    refresh_token: d.refresh_token || old.refresh_token || '',
    expiry: Date.now() + (Number(d.expires_in) || 3600) * 1000 - 60000,
    scope: d.scope || '',
  };
  writeJsonFile(TOKEN_FILE, tok);
  return tok;
}

/** Jeton d'accès valide, rafraîchi automatiquement si besoin. */
async function ytAccessToken() {
  const t = oauthTokens();
  if (!t.refresh_token) throw new Error('YouTube n’est pas autorisé (aucun jeton).');
  if (t.access_token && Date.now() < (t.expiry || 0)) return t.access_token;

  const c = oauthCfg();
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      refresh_token: t.refresh_token,
      client_id: c.clientId,
      client_secret: c.clientSecret,
      grant_type: 'refresh_token',
    }),
  });
  const d = await r.json();
  if (!r.ok || !d.access_token) {
    throw new Error(d.error_description || d.error || 'rafraîchissement refusé');
  }
  const tok = {
    ...t,
    access_token: d.access_token,
    expiry: Date.now() + (Number(d.expires_in) || 3600) * 1000 - 60000,
  };
  writeJsonFile(TOKEN_FILE, tok);
  return tok.access_token;
}

async function ytApi(pathname, { method = 'GET', query = {}, body = null, retry = 2 } = {}) {
  const token = await ytAccessToken();
  const u = new URL('https://www.googleapis.com/youtube/v3/' + pathname);
  for (const [k, v] of Object.entries(query)) u.searchParams.set(k, v);

  let r;
  for (let essai = 0; ; essai++) {
    try {
      r = await fetch(u, {
        method,
        headers: {
          Authorization: 'Bearer ' + token,
          ...(body ? { 'Content-Type': 'application/json' } : {}),
        },
        ...(body ? { body: JSON.stringify(body) } : {}),
        signal: AbortSignal.timeout(25000),   // plus de blocage silencieux
      });
      break;
    } catch (e) {
      // « fetch failed » : réseau qui hoquette. On réessaie avant d'abandonner.
      if (essai >= retry) {
        throw new Error(`réseau indisponible (${e.message}) — réessaie dans un instant`);
      }
      await new Promise((r2) => setTimeout(r2, 1500 * (essai + 1)));
    }
  }
  const d = await r.json().catch(() => ({}));
  if (!r.ok) {
    const e = d.error || {};
    throw new Error(e.message || ('HTTP ' + r.status));
  }
  return d;
}

/**
 * Crée un nouveau direct + un flux réutilisable, lie les deux,
 * et renvoie la clé à utiliser. C'est ce qui permet d'enchaîner
 * les cycles de 11 h sans intervention.
 */
let broadcastLock = null;                 // une création à la fois
let recentBroadcast = null;               // pour ne pas créer 2 directs en 3 secondes

async function createLiveBroadcast(title) {
  if (broadcastLock) return broadcastLock;          // appel concurrent : on attend le même
  if (recentBroadcast && Date.now() - recentBroadcast.at < 3 * 60000) {
    logAll('info', `Direct récent réutilisé (${Math.round((Date.now() - recentBroadcast.at) / 1000)} s) : `
      + recentBroadcast.watchUrl);
    return recentBroadcast;
  }
  broadcastLock = createLiveBroadcastNow(title);
  try {
    const b = await broadcastLock;
    recentBroadcast = { ...b, at: Date.now() };
    return b;
  } finally {
    broadcastLock = null;
  }
}

async function createLiveBroadcastNow(title) {
  const nom = String(title || ('Diffusion ' + new Date().toLocaleString('fr-FR'))).slice(0, 90);
  const memo = readJsonFile(YT_STREAM_FILE) || {};

  // 1) Le direct (c'est lui qui « réarme » YouTube ; la clé, elle, peut rester la même).
  const bc = await ytApi('liveBroadcasts', {
    method: 'POST',
    query: { part: 'snippet,status,contentDetails' },
    body: {
      snippet: { title: nom, scheduledStartTime: new Date(Date.now() + 60000).toISOString() },
      status: { privacyStatus: 'public', selfDeclaredMadeForKids: false },
      contentDetails: {
        enableAutoStart: true,
        enableAutoStop: true,
        enableDvr: true,
        recordFromStart: true,
        enableEmbed: true,
        latencyPreference: 'normal',
        monitorStream: { enableMonitorStream: false },
      },
    },
  });

  // 2) Le flux : on réutilise celui déjà créé s'il existe encore
  //    (moins d'objets dans le compte, et la clé ne change pas).
  let st = null;
  if (memo.streamId) {
    try {
      const got = await ytApi('liveStreams', {
        query: { part: 'id,cdn,status', id: memo.streamId },
      });
      const it = (got.items || [])[0];
      if (it && it.cdn && it.cdn.ingestionInfo && it.cdn.ingestionInfo.streamName) st = it;
    } catch { st = null; }
  }
  if (!st) {
    st = await ytApi('liveStreams', {
      method: 'POST',
      query: { part: 'snippet,cdn,contentDetails' },
      body: {
        snippet: { title: nom + ' — flux' },
        cdn: { frameRate: 'variable', resolution: 'variable', ingestionType: 'rtmp' },
        contentDetails: { isReusable: true },
      },
    });
  }

  // 3) On lie le direct au flux : sans ça, YouTube n'attend pas le flux.
  await ytApi('liveBroadcasts/bind', {
    method: 'POST',
    query: { part: 'id,contentDetails', id: bc.id, streamId: st.id },
  });

  const info = (st.cdn && st.cdn.ingestionInfo) || {};
  const out = {
    broadcastId: bc.id,
    streamId: st.id,
    streamKey: info.streamName || '',
    ingest: info.ingestionAddress || 'rtmp://a.rtmp.youtube.com/live2',
    watchUrl: 'https://youtube.com/watch?v=' + bc.id,
    title: nom,
  };
  writeJsonFile(YT_STREAM_FILE, { streamId: out.streamId, streamKey: out.streamKey, ingest: out.ingest });
  return out;
}

/* ================================================================== */
/* Authentification (mot de passe + session par cookie)               */
/* ================================================================== */

const AUTH_FILE = path.join(DATA_DIR, 'auth.json');
const SESSION_FILE = path.join(DATA_DIR, 'sessions.json');
const SETTINGS_FILE = path.join(DATA_DIR, 'settings.json');
const COOKIE = 'enc_session';

const auth = {
  salt: '',
  hash: '',            // scrypt du mot de passe
  source: 'none',      // 'env' | 'fichier' | 'genere'
  sessions: new Map(), // sid -> { exp, ip }
  fails: new Map(),    // ip -> { n, until }
  ttlMs: 7 * 24 * 3600 * 1000,
  maxFails: 6,
  blocageMs: 10 * 60 * 1000,
};

function scryptHash(pw, salt) {
  return crypto.scryptSync(String(pw), salt, 64).toString('hex');
}

function initAuth() {
  const envPw = String(process.env.ENCODER_PASSWORD || '').trim();
  try {
    const raw = JSON.parse(fs.readFileSync(AUTH_FILE, 'utf8'));
    if (raw && raw.salt && raw.hash) { auth.salt = raw.salt; auth.hash = raw.hash; auth.source = 'fichier'; }
  } catch {}

  if (envPw) {
    auth.salt = auth.salt || crypto.randomBytes(16).toString('hex');
    auth.hash = scryptHash(envPw, auth.salt);
    auth.source = 'env';
    console.log('✓ Authentification : mot de passe fourni par ENCODER_PASSWORD');
    return;
  }

  if (auth.source === 'fichier') {
    console.log('✓ Authentification active (mot de passe déjà enregistré)');
    return;
  }

  // Premier démarrage : on génère un mot de passe lisible et on l'enregistre
  const pw = String(crypto.randomInt(100000, 999999));
  setPassword(pw, 'genere');
  console.log('');
  console.log('┌──────────────────────────────────────────────┐');
  console.log('│  MOT DE PASSE CRÉÉ AUTOMATIQUEMENT           │');
  console.log('│                                              │');
  console.log(`│      ${pw}                                 │`);
  console.log('│                                              │');
  console.log('│  Note-le : il protège ta page et ton direct. │');
  console.log('│  Modifiable ensuite depuis l\'onglet Réglages. │');
  console.log('└──────────────────────────────────────────────┘');
  console.log('');
}

function setPassword(pw, source = 'fichier') {
  auth.salt = crypto.randomBytes(16).toString('hex');
  auth.hash = scryptHash(pw, auth.salt);
  auth.source = source;
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(AUTH_FILE, JSON.stringify({ salt: auth.salt, hash: auth.hash }), { mode: 0o600 });
  } catch (e) { console.warn('auth : écriture impossible', e.message); }
}

function checkPassword(pw) {
  if (!auth.hash) return false;
  const test = Buffer.from(scryptHash(pw, auth.salt), 'hex');
  const ref = Buffer.from(auth.hash, 'hex');
  return test.length === ref.length && crypto.timingSafeEqual(test, ref);
}

function parseCookies(req) {
  const out = {};
  const raw = req.headers.cookie || '';
  for (const part of raw.split(';')) {
    const i = part.indexOf('=');
    if (i < 0) continue;
    out[part.slice(0, i).trim()] = decodeURIComponent(part.slice(i + 1).trim());
  }
  return out;
}

function clientIp(req) {
  const fwd = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return fwd || req.socket.remoteAddress || 'inconnu';
}

function isHttps(req) {
  return req.socket.encrypted
    || String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim() === 'https';
}

/** Écrit les sessions sur disque : un redémarrage du service ne déconnecte plus. */
let sessionSaveTimer = null;
function saveSessions() {
  clearTimeout(sessionSaveTimer);
  sessionSaveTimer = setTimeout(() => {
    const now = Date.now();
    const out = {};
    for (const [sid, s] of auth.sessions) if (s.exp > now) out[sid] = s;
    writeJsonFile(SESSION_FILE, out);
  }, 400);
}

function loadSessions() {
  const d = readJsonFile(SESSION_FILE) || {};
  const now = Date.now();
  let n = 0;
  for (const [sid, s] of Object.entries(d)) {
    if (s && s.exp > now) { auth.sessions.set(sid, s); n++; }
  }
  if (n) console.log(`✓ Sessions restaurées : ${n} appareil(s) encore connecté(s)`);
}

/** Session valide ? (cookie + non expirée). Prolonge à chaque usage. */
function getSession(req) {
  const sid = parseCookies(req)[COOKIE];
  if (!sid) return null;
  const s = auth.sessions.get(sid);
  if (!s) return null;
  if (s.exp < Date.now()) { auth.sessions.delete(sid); saveSessions(); return null; }
  s.exp = Date.now() + auth.ttlMs;   // session glissante
  saveSessions();
  return { sid, ...s };
}

function openSession(req, res) {
  const sid = crypto.randomBytes(24).toString('hex');
  auth.sessions.set(sid, { exp: Date.now() + auth.ttlMs, ip: clientIp(req) });
  saveSessions();
  const maxAge = Math.floor(auth.ttlMs / 1000);
  const parts = [
    `${COOKIE}=${sid}`,
    'Path=/',
    'HttpOnly',
    'SameSite=Strict',
    `Max-Age=${maxAge}`,
  ];
  if (isHttps(req)) parts.push('Secure');
  res.setHeader('Set-Cookie', parts.join('; '));
  return sid;
}

function closeSession(req, res) {
  const sid = parseCookies(req)[COOKIE];
  if (sid) { auth.sessions.delete(sid); saveSessions(); }
  const parts = [`${COOKIE}=`, 'Path=/', 'HttpOnly', 'SameSite=Strict', 'Max-Age=0'];
  if (isHttps(req)) parts.push('Secure');
  res.setHeader('Set-Cookie', parts.join('; '));
}

function isBlocked(ip) {
  const f = auth.fails.get(ip);
  if (!f) return false;
  if (f.until && f.until > Date.now()) return true;
  if (f.until && f.until <= Date.now()) { auth.fails.delete(ip); return false; }
  return false;
}

function noteFail(ip) {
  const f = auth.fails.get(ip) || { n: 0, until: 0 };
  f.n += 1;
  if (f.n >= auth.maxFails) {
    f.until = Date.now() + auth.blocageMs;
    logAll('warn', `Trop d'essais de mot de passe depuis ${ip} : accès bloqué 10 minutes.`);
  }
  auth.fails.set(ip, f);
}

function noteSuccess(ip) { auth.fails.delete(ip); }

function loginPageHtml() {
  return `<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#ffffff">
<title>Live Encoder — Connexion</title>
<style>
  :root{--accent:#ff2d55;--txt:#0f172a;--muted:#64748b;--line:#e2e8f0;--err:#dc2626}
  *{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
  body{margin:0;min-height:100dvh;display:flex;align-items:center;justify-content:center;
       background:#ffffff;color:var(--txt);font:16px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
       padding:24px}
  .box{width:100%;max-width:380px}
  .logo{display:flex;align-items:center;gap:12px;margin-bottom:28px}
  .play{width:44px;height:44px;border-radius:12px;background:var(--accent);display:grid;place-items:center}
  .play svg{width:20px;height:20px;fill:#fff}
  h1{font-size:19px;margin:0;font-weight:650;letter-spacing:-.2px}
  p.sub{margin:2px 0 0;font-size:13px;color:var(--muted)}
  label{display:block;font-size:13px;font-weight:600;margin-bottom:7px}
  input{width:100%;padding:15px 16px;font-size:16px;border:1px solid var(--line);border-radius:12px;
        background:#fff;color:var(--txt);outline:none}
  input:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(255,45,85,.12)}
  button{width:100%;margin-top:16px;padding:16px;font-size:16px;font-weight:650;color:#fff;
         background:var(--accent);border:0;border-radius:12px}
  button:disabled{opacity:.6}
  .msg{margin-top:14px;font-size:13.5px;color:var(--err);min-height:18px}
  .note{margin-top:26px;font-size:12px;color:var(--muted);line-height:1.5}
</style>
</head>
<body>
  <div class="box">
    <div class="logo">
      <div class="play"><svg viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg></div>
      <div>
        <h1>Live Encoder</h1>
        <p class="sub">Diffusion RTMP vers YouTube</p>
      </div>
    </div>
    <form id="f" autocomplete="on">
      <label for="pw">Mot de passe</label>
      <input type="password" id="pw" name="password" autocomplete="current-password"
             inputmode="text" autofocus required>
      <button type="submit" id="b">Déverrouiller</button>
      <div class="msg" id="m"></div>
    </form>
    <p class="note">Cette page est protégée : elle contrôle une diffusion en cours.
      Sans le mot de passe, personne ne peut la consulter ni l'arrêter.</p>
  </div>
<script>
  var f = document.getElementById('f');
  var b = document.getElementById('b');
  var m = document.getElementById('m');
  f.addEventListener('submit', async function (e) {
    e.preventDefault();
    m.textContent = '';
    b.disabled = true;
    b.textContent = 'Vérification…';
    try {
      var r = await fetch('/api/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: document.getElementById('pw').value }),
      });
      var d = await r.json();
      if (r.ok) { location.href = '/'; return; }
      m.textContent = d.error || 'Mot de passe incorrect.';
    } catch (err) {
      m.textContent = 'Serveur injoignable.';
    }
    b.disabled = false;
    b.textContent = 'Déverrouiller';
  });
</script>
</body>
</html>`;
}

function serveFile(res, filePath) {
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('404'); }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Cache-Control': ext === '.html' ? 'no-store' : 'public, max-age=60',
    });
    res.end(data);
  });
}

function json(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': Buffer.byteLength(body) });
  res.end(body);
}

function readBody(req, limit = 6 * 1024 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) { reject(new Error('Fichier trop volumineux.')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

/** Écrit le corps de la requête directement sur le disque, sans passer par la RAM. */
function streamToFile(req, dest, append = false) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const out = fs.createWriteStream(dest, { flags: append ? 'a' : 'w' });
    let done = false;
    const fail = (e) => {
      if (done) return;
      done = true;
      try { out.destroy(); } catch {}
      reject(e);
    };
    req.on('data', (c) => { size += c.length; });
    req.on('error', fail);
    req.on('aborted', () => fail(new Error('Envoi interrompu par le client.')));
    out.on('error', fail);
    out.on('finish', () => { if (!done) { done = true; resolve(size); } });
    pipeline(req, out, (err) => { if (err) fail(err); else if (!done) { done = true; resolve(size); } });
  });
}

/* ---- Envois en cours (survivent à un redémarrage du service) ---- */
const UPLOAD_DIR = path.join(DATA_DIR, 'uploads');
const UPLOAD_FILE = path.join(DATA_DIR, 'uploads.json');
const uploads = new Map();

function uploadPartPath(id) { return path.join(UPLOAD_DIR, id + '.part'); }

function saveUploads() {
  const now = Date.now();
  for (const [id, u] of uploads) {
    if (now - u.createdAt > 7 * 24 * 3600 * 1000) {   // envois abandonnés depuis 7 j
      uploads.delete(id);
      try { fs.unlinkSync(uploadPartPath(id)); } catch {}
    }
  }
  writeJsonFile(UPLOAD_FILE, [...uploads.values()]);
}

function loadUploads() {
  fs.mkdirSync(UPLOAD_DIR, { recursive: true });
  const arr = readJsonFile(UPLOAD_FILE) || [];
  for (const u of arr) {
    if (!u || !u.id) continue;
    if (fs.existsSync(uploadPartPath(u.id))) uploads.set(u.id, u);
  }
  if (uploads.size) console.log(`✓ Envois interrompus mémorisés : ${uploads.size} (reprise possible)`);
}

function safeJoin(base, rel) {
  const p = path.normalize(path.join(base, rel));
  return p.startsWith(base) ? p : null;
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const p = decodeURIComponent(url.pathname);

  try {
    /* ---------- Authentification : portillon ---------- */
    // /oauth/start exige la session (le lien est cliqué depuis la page protégée) ;
    // seul le retour de Google doit être joignable sans cookie.
    const publicRoutes = new Set(['/login', '/api/login', '/oauth/callback', '/health']);
    const session = getSession(req);

    if (p === '/api/login' && req.method === 'POST') {
      const ip = clientIp(req);
      if (isBlocked(ip)) {
        return json(res, 429, { error: 'Trop d\'essais. Réessaie dans 10 minutes.' });
      }
      let body = {};
      try { body = JSON.parse((await readBody(req, 4096)).toString() || '{}'); } catch {}
      if (!checkPassword(body.password)) {
        noteFail(ip);
        logAll('warn', `Échec de connexion depuis ${ip}.`);
        return json(res, 401, { error: 'Mot de passe incorrect.' });
      }
      noteSuccess(ip);
      openSession(req, res);
      logAll('info', `Connexion réussie depuis ${ip}.`);
      return json(res, 200, { ok: true });
    }

    if (!session && !publicRoutes.has(p)) {
      if (p.startsWith('/api/')) return json(res, 401, { error: 'Authentification requise.', auth: 'required' });
      if (p === '/health' || p.startsWith('/clips/') || p.startsWith('/recordings/')) {
        return json(res, 401, { error: 'Authentification requise.' });
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(loginPageHtml());
    }

    /* ---------- YouTube : configuration OAuth ---------- */
    if (p === '/api/mail/status') {
      const c = mailConf();
      return json(res, 200, {
        configured: mailReady(),
        to: c.MAIL_TO || null,
        from: c.GMAIL_USER || null,
      });
    }

    if (p === '/api/mail/config' && req.method === 'POST') {
      const body = JSON.parse((await readBody(req, 16384)).toString() || '{}');
      const user = String(body.gmailUser || '').trim();
      const pass = String(body.appPassword || '').replace(/\s+/g, '');
      const to = String(body.mailTo || '').trim() || user;
      if (!/^[^@\s]+@[^@\s]+\.[a-z]{2,}$/i.test(user)) return json(res, 400, { error: 'Adresse Gmail invalide.' });
      if (!/^[^@\s]+@[^@\s]+\.[a-z]{2,}$/i.test(to)) return json(res, 400, { error: 'Adresse de destination invalide.' });
      if (pass.replace(/\s/g, '').length !== 16) {
        return json(res, 400, { error: 'Le mot de passe d\'application Google fait 16 caractères (sans les espaces).' });
      }
      const txt = `GMAIL_USER=${user}\nGMAIL_APP_PASSWORD=${pass}\nMAIL_TO=${to}\n`;
      try {
        fs.writeFileSync(MAIL_FILE, txt, { mode: 0o600 });
        fs.chmodSync(MAIL_FILE, 0o600);
      } catch (e) { return json(res, 500, { error: 'Écriture impossible : ' + e.message }); }
      logAll('ok', 'Alerte e-mail configurée pour ' + to);
      return json(res, 200, { ok: true, to });
    }

    if (p === '/api/mail/test' && req.method === 'POST') {
      const r = sendMail('Test d\'alerte réussi',
        'Si tu reçois ce message, les alertes automatiques de l\'encodeur fonctionnent.\n\n'
        + 'Tu seras prévenu uniquement en cas de problème : service arrêté, serveur muet, '
        + 'diffusion figée, disque presque plein.');
      return json(res, r.ok ? 200 : 502, r);
    }

    if (p === '/api/yt/config' && req.method === 'POST') {
      const body = JSON.parse((await readBody(req, 8192)).toString() || '{}');
      const clientId = String(body.clientId || '').trim();
      const clientSecret = String(body.clientSecret || '').trim();
      const redirect = String(body.redirectUri || '').trim();
      if (!/^[A-Za-z0-9_-]+\.apps\.googleusercontent\.com$/.test(clientId)) {
        return json(res, 400, { error: 'ID client invalide (doit finir par .apps.googleusercontent.com).' });
      }
      if (clientSecret.length < 10) return json(res, 400, { error: 'Secret client invalide.' });
      // L'URI n'est pas figée : elle est recalculée à chaque autorisation
      // depuis le domaine réellement utilisé (ainsi un changement de domaine
      // ou un test en local ne cassent rien).
      const cfg = { clientId, clientSecret };
      if (redirect) cfg.redirectUri = redirect;
      if (!writeJsonFile(OAUTH_FILE, cfg)) return json(res, 500, { error: 'Écriture impossible.' });
      logAll('ok', 'Identifiants OAuth YouTube enregistrés sur le serveur.');
      return json(res, 200, { ok: true, redirectUri: redirect || (publicBase(req) + '/oauth/callback') });
    }

    if (p === '/api/yt/status') {
      const t = oauthTokens();
      const c = oauthCfg();
      return json(res, 200, {
        configured: Boolean(c.clientId && c.clientSecret),
        authorized: Boolean(t.refresh_token),
        redirectUri: c.redirectUri || (publicBase(req) + '/oauth/callback'),
        expiresAt: t.expiry || null,
        hasAccessToken: Boolean(t.access_token && Date.now() < (t.expiry || 0)),
        autoBroadcast: Boolean(live.auto.autoBroadcast),
      });
    }

    if (p === '/api/yt/autobroadcast' && req.method === 'POST') {
      const body = JSON.parse((await readBody(req, 4096)).toString() || '{}');
      live.auto.autoBroadcast = Boolean(body.enabled);
      saveAutoSettings();
      logAll('info', 'Création automatique du nouveau direct : '
        + (live.auto.autoBroadcast ? 'activée' : 'désactivée') + '.');
      return json(res, 200, { autoBroadcast: live.auto.autoBroadcast });
    }

    if (p === '/api/yt/broadcast' && req.method === 'POST') {
      try {
        const b = await createLiveBroadcast(live.options && live.options.title);
        live.auto.lastKey = b.streamKey;
        live.auto.lastIngest = b.ingest;
        logAll('ok', `Nouveau direct créé : ${b.watchUrl} (clé …${b.streamKey.slice(-4)})`);
        return json(res, 200, b);
      } catch (e) {
        logAll('error', 'Création du direct impossible : ' + e.message);
        return json(res, 500, { error: e.message });
      }
    }

    if (p === '/api/yt/disconnect' && req.method === 'POST') {
      try { fs.unlinkSync(TOKEN_FILE); } catch {}
      logAll('warn', 'Autorisation YouTube révoquée sur le serveur.');
      return json(res, 200, { ok: true });
    }

    if (p === '/oauth/start') {
      const c = oauthCfg();
      if (!c.clientId || !c.clientSecret) {
        res.writeHead(400, { 'Content-Type': 'text/html; charset=utf-8' });
        return res.end('<h1>Identifiants OAuth absents</h1><p>Renseigne l\'ID et le secret client dans l\'app (Réglages).</p>');
      }
      const state = crypto.randomBytes(16).toString('hex');
      oauthStates.set(state, Date.now() + 10 * 60000);
      const u = new URL('https://accounts.google.com/o/oauth2/v2/auth');
      u.searchParams.set('client_id', c.clientId);
      u.searchParams.set('redirect_uri', c.redirectUri || (publicBase(req) + '/oauth/callback'));
      u.searchParams.set('response_type', 'code');
      u.searchParams.set('scope', 'https://www.googleapis.com/auth/youtube.force-ssl');
      u.searchParams.set('access_type', 'offline');
      u.searchParams.set('prompt', 'consent');
      u.searchParams.set('include_granted_scopes', 'true');
      u.searchParams.set('state', state);
      res.writeHead(302, { Location: u.toString() });
      return res.end();
    }

    if (p === '/oauth/callback') {
      const state = url.searchParams.get('state') || '';
      const code = url.searchParams.get('code') || '';
      const err = url.searchParams.get('error') || '';
      const back = (ok, msg) => {
        res.writeHead(302, { Location: '/?yt=' + (ok ? 'ok' : 'err') + (msg ? '&m=' + encodeURIComponent(msg) : '') });
        res.end();
      };
      if (err) { logAll('error', 'Autorisation YouTube refusée : ' + err); return back(false, err); }
      if (!oauthStates.has(state)) { logAll('error', 'Retour OAuth invalide (state inconnu).'); return back(false, 'state'); }
      oauthStates.delete(state);
      if (!code) return back(false, 'no-code');
      try {
        const tok = await exchangeCode(code, req);
        logAll('ok', 'Autorisation YouTube enregistrée : le serveur renouvellera le jeton tout seul'
          + (tok.refresh_token ? ' (jeton durable obtenu).' : ' — ATTENTION : pas de jeton durable, révoque l\'accès et recommence.'));
        return back(true, '');
      } catch (e) {
        logAll('error', 'Échange du code impossible : ' + e.message);
        return back(false, e.message);
      }
    }

    if (p === '/api/logout' && req.method === 'POST') {
      closeSession(req, res);
      logAll('info', `Déconnexion (${clientIp(req)}).`);
      return json(res, 200, { ok: true });
    }

    if (p === '/api/auth/password' && req.method === 'POST') {
      const body = JSON.parse((await readBody(req, 4096)).toString() || '{}');
      if (!checkPassword(body.current)) return json(res, 403, { error: 'Mot de passe actuel incorrect.' });
      const next = String(body.next || '');
      if (next.length < 6) return json(res, 400, { error: '6 caractères minimum.' });
      setPassword(next);
      auth.sessions.clear();
      openSession(req, res);
      logAll('warn', 'Mot de passe modifié : toutes les autres sessions ont été déconnectées.');
      return json(res, 200, { ok: true });
    }

    if (p === '/login') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(loginPageHtml());
    }

    /* ---------- API ---------- */
    if (p === '/health') {
      // Volontairement minimal et public : sert au chien de garde (auto-repare.sh).
      return json(res, 200, {
        ok: true,
        ffmpeg: binOk(FFMPEG),
        clips: clips.size,
        live: Boolean(live.proc),
        auto: Boolean(live.auto.enabled),
        uptime: Math.round(process.uptime()),
      });
    }

    // Liste
    if (p === '/api/clips' && req.method === 'GET') {
      return json(res, 200, {
        clips: [...clips.values()].sort((a, b) => b.createdAt - a.createdAt),
        state: publicState(),
        ytdlp: ytdlp(),
      });
    }

    // Upload d'un fichier
    if (p === '/api/clips/upload' && req.method === 'POST') {
      const title = url.searchParams.get('title') || 'Clip vidéo';
      const ext = (url.searchParams.get('ext') || '.mp4').replace(/[^a-z0-9.]/gi, '');
      const id = crypto.randomBytes(4).toString('hex') + Date.now().toString(36).slice(-4);
      const rel = path.join('clips', id + ext);
      const abs = path.join(ROOT, rel);

      // Écriture DIRECTE sur le disque : plus aucun fichier en mémoire.
      // C'est ce qui faisait planter le serveur au-delà de ~2 Go.
      const wrote = await streamToFile(req, abs);
      if (!wrote) { try { fs.unlinkSync(abs); } catch {} return json(res, 400, { error: 'Fichier vide.' }); }

      let info;
      try { info = probe(abs); }
      catch (e) {
        try { fs.unlinkSync(abs); } catch {}
        return json(res, 400, { error: e.message });
      }
      const clip = addClip({
        id, title, kind: 'upload', file: rel,
        start: 0, end: info.duration || null, loop: false,
        duration: info.duration, hasAudio: info.hasAudio, size: wrote,
        videoCodec: info.videoCodec, audioCodec: info.audioCodec,
        width: info.width, height: info.height,
      });
      // L'id est généré dans addClip : on réaligne le nom de fichier si besoin
      if (clip.id !== id) {
        const rel2 = path.join('clips', clip.id + ext);
        try { fs.renameSync(abs, path.join(ROOT, rel2)); clip.file = rel2; saveLibrary(); } catch {}
      }
      logAll('info', `Clip ajouté : « ${title} » (${info.duration.toFixed(1)} s, ${info.hasAudio ? 'avec' : 'sans'} audio)`);
      return json(res, 200, { clip, probe: info });
    }

    // Supprimer un clip de la liste (le fichier n'est effacé que s'il est dans clips/)
    if (p === '/api/clips/delete' && req.method === 'POST') {
      const body = JSON.parse((await readBody(req, 65536)).toString() || '{}');
      const clip = clips.get(String(body.id || ''));
      if (!clip) return json(res, 404, { error: 'Clip introuvable.' });
      if (live.clipId === clip.id) return json(res, 409, { error: 'Ce clip est en cours de diffusion.' });
      clips.delete(clip.id);
      saveLibrary();
      try {
        if (clip.file && clip.file.startsWith('clips' + path.sep)) fs.unlinkSync(clipPath(clip));
      } catch {}
      logAll('info', `Clip supprimé : « ${clip.title} »`);
      return json(res, 200, { ok: true });
    }

    /* ---- Upload par morceaux, reprenable après coupure ---- */
    if (p === '/api/clips/upload/start' && req.method === 'POST') {
      const body = JSON.parse((await readBody(req, 65536)).toString() || '{}');
      const title = String(body.title || 'Clip vidéo').slice(0, 120);
      const ext = String(body.ext || '.mp4').replace(/[^a-z0-9.]/gi, '').slice(0, 10) || '.mp4';
      const total = Math.max(0, Number(body.size) || 0);
      // Refus immédiat si le disque n'a pas la place : mieux vaut le dire
      // avant 3 Go d'envoi que d'échouer à 90 %.
      try {
        const st = fs.statfsSync(ROOT);
        const libre = Number(st.bavail) * Number(st.bsize);
        if (total > 0 && libre < total * 1.05) {
          return json(res, 507, {
            error: `Place insuffisante sur le serveur : ${(libre / 1e9).toFixed(1)} Go libres `
              + `pour un fichier de ${(total / 1e9).toFixed(2)} Go. Utilise « fichier déjà sur le serveur ».`,
          });
        }
      } catch {}

      const id = crypto.randomBytes(8).toString('hex');
      fs.mkdirSync(UPLOAD_DIR, { recursive: true });
      fs.writeFileSync(uploadPartPath(id), Buffer.alloc(0));
      uploads.set(id, { id, title, ext, total, createdAt: Date.now() });
      saveUploads();
      logAll('info', `Envoi de « ${title} » démarré (${(total / 1e9).toFixed(2)} Go).`);
      return json(res, 200, { uploadId: id, received: 0 });
    }

    if (p === '/api/clips/upload/status' && req.method === 'GET') {
      const id = url.searchParams.get('uploadId') || '';
      const u = uploads.get(id);
      if (!u) return json(res, 404, { error: 'Envoi inconnu (le serveur a peut-être redémarré).' });
      let received = 0;
      try { received = fs.statSync(uploadPartPath(id)).size; } catch { received = 0; }
      return json(res, 200, { uploadId: id, received, total: u.total, title: u.title });
    }

    if (p === '/api/clips/upload/chunk' && req.method === 'POST') {
      const id = url.searchParams.get('uploadId') || '';
      const offset = Number(url.searchParams.get('offset'));
      const u = uploads.get(id);
      if (!u) return json(res, 404, { error: 'Envoi inconnu : relance l\'envoi depuis le début.' });
      const part = uploadPartPath(id);
      let cur = 0;
      try { cur = fs.statSync(part).size; } catch { cur = 0; }
      if (!Number.isFinite(offset) || offset !== cur) {
        return json(res, 409, { error: 'Position inattendue', received: cur });
      }
      try {
        await streamToFile(req, part, true);
      } catch (e) {
        let after = 0; try { after = fs.statSync(part).size; } catch {}
        logAll('warn', `Envoi interrompu à ${(after / 1e9).toFixed(2)} Go : ${e.message}`);
        return json(res, 499, { error: e.message, received: after });
      }
      let received = 0; try { received = fs.statSync(part).size; } catch {}
      return json(res, 200, { received, total: u.total });
    }

    if (p === '/api/clips/upload/finish' && req.method === 'POST') {
      const body = JSON.parse((await readBody(req, 65536)).toString() || '{}');
      const id = String(body.uploadId || '');
      const u = uploads.get(id);
      if (!u) return json(res, 404, { error: 'Envoi inconnu.' });
      const part = uploadPartPath(id);
      if (!fs.existsSync(part) || fs.statSync(part).size === 0) {
        return json(res, 400, { error: 'Aucune donnée reçue.' });
      }
      const rel = path.join('clips', id + u.ext);
      const abs = path.join(ROOT, rel);
      try { fs.renameSync(part, abs); } catch (e) { return json(res, 500, { error: e.message }); }
      let info;
      try { info = probe(abs); }
      catch (e) {
        try { fs.unlinkSync(abs); } catch {}
        uploads.delete(id); saveUploads();
        return json(res, 400, { error: 'Fichier illisible : ' + e.message });
      }
      const clip = addClip({
        id, title: u.title, kind: 'upload', file: rel,
        start: 0, end: info.duration || null, loop: false,
        duration: info.duration, hasAudio: info.hasAudio, size: info.size,
        videoCodec: info.videoCodec, audioCodec: info.audioCodec,
        width: info.width, height: info.height,
      });
      if (clip.id !== id) {
        const rel2 = path.join('clips', clip.id + u.ext);
        try { fs.renameSync(abs, path.join(ROOT, rel2)); clip.file = rel2; saveLibrary(); } catch {}
      }
      uploads.delete(id); saveUploads();
      logAll('ok', `Clip ajouté : « ${clip.title} » (${fmtTime(info.duration || 0)}, `
        + `${(info.size / 1e9).toFixed(2)} Go${info.hasAudio ? ', avec audio' : ', sans audio'})`);
      return json(res, 200, { clip, probe: info });
    }

    // Enregistrer un fichier déjà présent sur le serveur (gros fichiers : rsync/scp)
    if (p === '/api/clips/adopt' && req.method === 'POST') {
      const body = JSON.parse((await readBody(req, 1024 * 1024)).toString() || '{}');
      const raw = String(body.file || '').trim();
      if (!raw) return json(res, 400, { error: 'Chemin du fichier manquant.' });
      const abs0 = path.resolve(ROOT, raw);
      if (!fs.existsSync(abs0)) return json(res, 400, { error: 'Fichier introuvable : ' + abs0 });
      if (!fs.statSync(abs0).isFile()) return json(res, 400, { error: 'Ce chemin n\'est pas un fichier.' });
      let info;
      try { info = probe(abs0); }
      catch (e) { return json(res, 400, { error: e.message }); }

      const title = String(body.title || path.basename(abs0)).slice(0, 120);
      const clip = addClip({
        title, kind: 'upload', file: path.relative(ROOT, abs0),
        start: 0, end: info.duration || null, loop: Boolean(body.loop),
        duration: info.duration, hasAudio: info.hasAudio, size: info.size,
        videoCodec: info.videoCodec, audioCodec: info.audioCodec,
        width: info.width, height: info.height,
      });
      const gb = (info.size / 1e9).toFixed(2);
      logAll('ok', `Fichier du serveur enregistré : « ${title} » `
        + `(${fmtTime(info.duration)}, ${gb} Go, ${info.videoCodec}/${info.audioCodec}, ${info.width}×${info.height})`);
      const direct = canDirectPlay(clip);
      logAll('info', direct
        ? 'Cet enregistrement sera diffusé en lecture directe (aucun ré-encodage, aucun fichier temporaire).'
        : 'Ré-encodage nécessaire au premier lancement (format source non conforme).');
      return json(res, 200, { clip, probe: info, directPlay: direct });
    }

    // Ajout par lien
    if (p === '/api/clips/link' && req.method === 'POST') {
      const body = JSON.parse((await readBody(req, 1024 * 1024)).toString() || '{}');
      const linkUrl = String(body.url || '').trim();
      if (!/^https?:\/\//i.test(linkUrl)) return json(res, 400, { error: 'Lien invalide (http/https attendu).' });
      if (!ytdlp()) return json(res, 400, { error: 'yt-dlp absent sur le serveur : télécharge la vidéo puis upload-la.' });

      const title = String(body.title || 'Clip depuis un lien').slice(0, 120);
      const clip = addClip({ title, kind: 'link', source: linkUrl, start: 0, end: null, duration: 0 });

      downloadLink(linkUrl, clip.id, (err, file) => {
        if (err) {
          clip.notes = err.message;
          clip.absent = true;
          saveLibrary();
          broadcast({ type: 'clip-updated', clip });
          logAll('error', 'Lien : ' + err.message);
          return;
        }
        try {
          const info = probe(file);
          clip.file = path.relative(ROOT, file);
          clip.duration = info.duration;
          clip.hasAudio = info.hasAudio;
          clip.videoCodec = info.videoCodec;
          clip.audioCodec = info.audioCodec;
          clip.width = info.width;
          clip.height = info.height;
          clip.end = info.duration;
          clip.size = info.size;
          clip.absent = false;
          clip.notes = '';
          saveLibrary();
          broadcast({ type: 'clip-updated', clip });
          logAll('info', `Lien téléchargé : « ${clip.title} » (${info.duration.toFixed(1)} s)`);
        } catch (e) {
          clip.absent = true; clip.notes = e.message; saveLibrary();
          logAll('error', 'Lien : ' + e.message);
        }
      });
      return json(res, 200, { clip, downloading: true });
    }

    // Modification / suppression
    const mClip = p.match(/^\/api\/clips\/([\w-]+)$/);
    if (mClip) {
      const clip = clips.get(mClip[1]);
      if (!clip) return json(res, 404, { error: 'Clip introuvable.' });

      if (req.method === 'PATCH') {
        const body = JSON.parse((await readBody(req, 1024 * 1024)).toString() || '{}');
        for (const k of ['title', 'start', 'end', 'loop']) {
          if (body[k] !== undefined) clip[k] = body[k];
        }
        if (clip.end !== null && clip.duration) clip.end = Math.min(Number(clip.end), clip.duration);
        clip.start = Math.max(0, Math.min(Number(clip.start) || 0, (clip.end || clip.duration || 0) - 0.5));
        saveLibrary();
        broadcast({ type: 'clip-updated', clip });
        return json(res, 200, { clip });
      }
      if (req.method === 'DELETE') {
        clips.delete(clip.id);
        saveLibrary();
        if (clip.file) { try { fs.unlinkSync(path.join(ROOT, clip.file)); } catch {} }
        purgeCache(clip.id);
        broadcast({ type: 'clip-deleted', id: clip.id });
        return json(res, 200, { ok: true });
      }
      if (req.method === 'GET' && url.searchParams.get('stream') === '1') {
        const abs = clipPath(clip);
        if (!abs || !fs.existsSync(abs)) return json(res, 404, { error: 'Fichier absent.' });
        // Lecture du clip pour l'aperçu (découpe appliquée)
        const s = Number(clip.start) || 0;
        const e = clip.end || clip.duration || 0;
        res.writeHead(200, { 'Content-Type': 'video/mp4' });
        const p2 = spawn(FFMPEG, ['-hide_banner', '-loglevel', 'error', '-ss', String(s), '-i', abs, '-to', String(Math.max(0.5, e - s)),
          '-vf', 'scale=-2:360', '-c:v', 'libx264', '-preset', 'ultrafast', '-c:a', 'aac', '-movflags', 'frag_keyframe+empty_moov', '-f', 'mp4', 'pipe:1'],
          { stdio: ['ignore', 'pipe', 'ignore'] });
        p2.stdout.pipe(res);
        req.on('close', () => { try { p2.kill('SIGKILL'); } catch {} });
        return;
      }
    }

    // Démarrage / arrêt / planification
    if (p === '/api/live/start-clip' && req.method === 'POST') {
      const body = JSON.parse((await readBody(req, 1024 * 1024)).toString() || '{}');
      if (live.proc) return json(res, 409, { error: 'Une diffusion est déjà en cours.' });
      const clip = clips.get(body.clipId);
      if (!clip) return json(res, 404, { error: 'Clip introuvable.' });
      if (!clip.file) return json(res, 400, { error: 'Le téléchargement du lien n\'est pas terminé.' });

      // Aucune clé à saisir : si le serveur est autorisé sur YouTube, il crée
      // lui-même le direct et récupère sa clé avant de démarrer FFmpeg.
      let auto = null;
      let streamKey = String(body.streamKey || '').trim();
      if (!streamKey && live.auto.autoBroadcast && ytReady()) {
        try {
          const b = await createLiveBroadcast();
          streamKey = b.streamKey;
          live.auto.lastKey = b.streamKey;
          live.auto.lastIngest = b.ingest;
          auto = { watchUrl: b.watchUrl, broadcastId: b.broadcastId, title: b.title };
          logAll('ok', `Nouveau direct créé automatiquement : ${b.watchUrl}`);
        } catch (e) {
          logAll('error', 'Création du direct impossible : ' + e.message);
        }
      }
      if (!streamKey) {
        streamKey = live.auto.lastKey || live.streamKey || '';
      }

      const options = {
        preset: body.preset, fps: body.fps, height: body.height, ratio: body.ratio,
        streamKey, ingest: body.ingest, mode: body.mode || 'youtube',
        durationMin: body.durationMin || null, untilTs: body.untilTs || null,
      };

      if (body.startAt && Number(body.startAt) > Date.now() + 3000) {
        armStartTimer(Number(body.startAt), clip.id, options);
        return json(res, 200, { scheduled: true, startAt: Number(body.startAt), state: publicState() });
      }
      const r = await startClip(clip.id, options);
      return json(res, 200, { started: true, ...r, auto, state: publicState() });
    }

    /* Réglage de la relance automatique */
    if (p === '/api/live/autorestart' && req.method === 'POST') {
      const body = JSON.parse((await readBody(req, 1024 * 1024)).toString() || '{}');
      if (body.enabled !== undefined) live.auto.enabled = Boolean(body.enabled);
      if (body.delaySec !== undefined) {
        live.auto.delaySec = Math.max(5, Math.min(3600, Number(body.delaySec) || 120));
      }
      if (body.recycleMin !== undefined) {
        live.auto.recycleMin = Math.max(0, Math.min(1440, Number(body.recycleMin) || 0));
        if (live.proc) armRecycle(); else cancelRecycle();
      }
      saveAutoSettings();
      if (!live.auto.enabled) autoCancel('désactivée');
      // Si une relance est en attente, on applique le nouveau délai immédiatement
      else if (live.auto.restartAt) {
        const remaining = Math.max(0, live.auto.restartAt - Date.now()) / 1000;
        if (Math.abs(remaining - live.auto.delaySec) > 1) {
          const last = {
            mode: live.auto.lastMode || 'clip',
            clipId: live.auto.lastClipId || null,
            options: live.options || {},
          };
          autoCancel();
          if (last.mode) autoSchedule(last);
        }
      }
      logAll('info', `Relance automatique : ${live.auto.enabled ? 'activée' : 'désactivée'}`
        + ` · délai ${fmtDelay(live.auto.delaySec)}`);
      return json(res, 200, { auto: publicState().auto });
    }

    /* Recyclage immédiat : nouveau direct tout de suite */
    if (p === '/api/live/recycle' && req.method === 'POST') {
      if (!live.auto.enabled) {
        return json(res, 409, { error: "Active d'abord la relance automatique : sans elle, le nouveau direct ne repartira pas." });
      }
      const ok = recycleNow('demande manuelle');
      return json(res, 200, { recycled: ok, state: publicState() });
    }

    if (p === '/api/live/stop' && req.method === 'POST') {
      cancelSchedule();
      const ok = stopLive();
      return json(res, 200, { stopped: ok, state: publicState() });
    }

    if (p === '/api/live/state') {
      return json(res, 200, { state: publicState(), stats: live.lastStats });
    }

    /* ---- Transport de secours : HTTP au lieu de WebSocket ---- */
    if (p === '/api/logs' && req.method === 'GET') {
      const since = Number(url.searchParams.get('since') || 0);
      return json(res, 200, { logs: logRing.items.filter((l) => l.seq > since), cursor: logRing.seq });
    }

    if (p === '/api/camera/start' && req.method === 'POST') {
      const body = JSON.parse((await readBody(req, 1024 * 1024)).toString() || '{}');
      const r = startCamera(body);
      return json(res, 200, { started: true, ...r, config: body, state: publicState() });
    }

    if (p === '/api/camera/chunk' && req.method === 'POST') {
      const buf = await readBody(req);
      if (!live.proc || live.mode !== 'camera' || !live.proc.stdin.writable) {
        return json(res, 409, { error: 'Aucune session caméra active.' });
      }
      live.bytesIn += buf.length;
      live.proc.stdin.write(buf);
      return json(res, 200, { ok: true, bytesIn: live.bytesIn });
    }

    if (p === '/api/camera/stop' && req.method === 'POST') {
      return json(res, 200, { stopped: stopLive(), state: publicState() });
    }

    /* ---------- Fichiers ---------- */
    if (p.startsWith('/recordings/')) return serveFile(res, path.join(REC_DIR, path.basename(p)));
    if (p.startsWith('/clips/')) return serveFile(res, path.join(CLIPS_DIR, path.basename(p)));

    let sp = p === '/' ? '/index.html' : p;
    const filePath = safeJoin(PUBLIC_DIR, sp.replace(/^\//, ''));
    if (!filePath) { res.writeHead(403); return res.end('403'); }
    return serveFile(res, filePath);

  } catch (e) {
    return json(res, 500, { error: e.message });
  }
});

/* ================================================================== */
/* 9. WebSocket : caméra + abonnements d'état                         */
/* ================================================================== */

server.on('upgrade', (req, socket) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname !== '/ws') { socket.write('HTTP/1.1 404 Not Found\r\n\r\n'); return socket.destroy(); }
  if (!getSession(req)) {
    socket.write('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n');
    return socket.destroy();
  }
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.write('HTTP/1.1 400 Bad Request\r\n\r\n'); return socket.destroy(); }

  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
    + `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
  socket.setNoDelay(true);

  const ws = new WSConnection(socket);
  live.subscribers.add(ws);

  ws.sendJSON({ type: 'ready', presets: PRESETS, ffmpeg: binOk(FFMPEG), state: publicState() });

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      // Flux caméra : uniquement si la session caméra est active
      if (live.mode === 'camera' && live.proc && live.proc.stdin.writable) {
        live.bytesIn += data.length;
        const ok = live.proc.stdin.write(Buffer.isBuffer(data) ? data : Buffer.from(data));
        if (!ok) { ws.pauseRead(); live.proc.stdin.once('drain', () => ws.resumeRead()); }
      }
      return;
    }
    let msg;
    try { msg = JSON.parse(data); } catch { return; }
    try {
      if (msg.type === 'start') {
        const r = startCamera(msg);
        ws.sendJSON({ type: 'started', ...r, config: msg, state: publicState() });
      } else if (msg.type === 'stop') {
        stopLive();
      } else if (msg.type === 'state') {
        ws.sendJSON({ type: 'state', state: publicState() });
      } else if (msg.type === 'ping') {
        ws.sendJSON({ type: 'pong', t: msg.t, bytesIn: live.bytesIn, state: publicState() });
      }
    } catch (e) {
      ws.sendJSON({ type: 'error', message: e.message });
    }
  });

  ws.on('close', () => {
    live.subscribers.delete(ws);
    // NOTE : on n'arrête PAS la diffusion caméra à la déconnexion du navigateur :
    // FFmpeg continue jusqu'à l'heure de fin ou l'arrêt manuel.
  });
});

/* ================================================================== */
/* 10. Démarrage                                                      */
/* ================================================================== */

ensureFfmpeg();
ensureYtdlp();
initAuth();
loadSessions();
loadUploads();
loadAutoSettings();
loadLibrary();
pruneCache();

// Node 18+ interrompt toute requête au bout de 5 minutes (requestTimeout).
// Un envoi de plusieurs Go en 4G dure bien plus longtemps : on désactive.
server.requestTimeout = 0;
server.headersTimeout = 120000;
server.keepAliveTimeout = 120000;

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🎥 Live Encoder prêt sur http://0.0.0.0:${PORT}`);
  console.log(`   yt-dlp : ${ytdlp() ? 'disponible' : 'absent (upload uniquement)'}`);
  console.log(`   accès  : protégé par mot de passe (${auth.source === 'env' ? 'ENCODER_PASSWORD' : 'data/auth.json'})\n`);
  restoreState();

  // Filet de sécurité : arrêt auto même après un redémarrage
  setInterval(() => {
    if (live.proc && live.scheduledEnd && Date.now() > live.scheduledEnd + 1000) {
      live.endReached = true;
      stopLive();
    }
    if (schedule.startAt && Date.now() > schedule.startAt + 5000) {
      // Le timer a pu être perdu (veille du serveur) : on rattrape
      const { clipId, options } = schedule;
      if (clipId && options && !live.proc) {
        schedule.startAt = 0;
        try { startClip(clipId, options); } catch (e) { logAll('error', e.message); }
      }
    }
  }, 2000);
});

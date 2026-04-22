const COLUMNS = [
  'bg_musical_ability', 'bg_listening_frequency', 'bg_artist_influence', 'bg_cultural_influence',
  'bg_ai_familiarity', 'bg_ai_sentiment',
  'bg_discovery_youtube', 'bg_discovery_spotify', 'bg_discovery_applemusic', 'bg_discovery_netease',
  'bg_discovery_tiktok', 'bg_discovery_bilibili', 'bg_discovery_bandcamp', 'bg_discovery_other_cb',
  'bg_discovery_platforms', 'bg_discovery_other', 'bg_heard_ai_before', 'bg_education',
  'song_01_id', 'song_01_context_type', 'song_01_rating', 'song_01_heard_before', 'song_01_attn_check',
  'song_02_id', 'song_02_context_type', 'song_02_rating', 'song_02_heard_before', 'song_02_attn_check',
  'song_03_id', 'song_03_context_type', 'song_03_rating', 'song_03_heard_before', 'song_03_attn_check',
  'song_04_id', 'song_04_context_type', 'song_04_rating', 'song_04_heard_before', 'song_04_attn_check',
  'song_05_id', 'song_05_context_type', 'song_05_rating', 'song_05_heard_before', 'song_05_attn_check',
  'song_06_id', 'song_06_context_type', 'song_06_rating', 'song_06_heard_before', 'song_06_attn_check',
  'song_07_id', 'song_07_context_type', 'song_07_rating', 'song_07_heard_before', 'song_07_attn_check',
  'song_08_id', 'song_08_context_type', 'song_08_rating', 'song_08_heard_before', 'song_08_attn_check',
  'song_09_id', 'song_09_context_type', 'song_09_rating', 'song_09_heard_before', 'song_09_attn_check',
  'meta_selected_pair_ids',
  'refl_sound_vs_context', 'refl_ai_affect', 'refl_fair_compensation', 'refl_open_text',
  'meta_timestamp', 'meta_time_spent_seconds', 'completion_code',
];

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

  const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
  const GITHUB_OWNER = 'honeray';
  const GITHUB_REPO = 'music-survey';
  const CSV_PATH = 'responses.csv';

  if (!GITHUB_TOKEN) {
    return { statusCode: 500, body: 'Server misconfiguration: missing GITHUB_TOKEN' };
  }

  let data;
  try {
    data = JSON.parse(event.body);
  } catch {
    return { statusCode: 400, body: 'Invalid JSON body' };
  }

  const apiUrl = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${CSV_PATH}`;
  const headers = {
    Authorization: `token ${GITHUB_TOKEN}`,
    Accept: 'application/vnd.github.v3+json',
    'Content-Type': 'application/json'
  };

  const csvHeader = COLUMNS.map(k => '"' + k.replace(/"/g, '""') + '"').join(',');
  const csvRow = COLUMNS.map(k => '"' + String(data[k] ?? '').replace(/"/g, '""') + '"').join(',');

  for (let attempt = 0; attempt < 4; attempt++) {
    let sha = null, existingLines = [];
    try {
      const r = await fetch(apiUrl, { headers });
      if (r.ok) {
        const meta = await r.json();
        sha = meta.sha;
        existingLines = Buffer.from(meta.content.replace(/\n/g, ''), 'base64')
          .toString('utf-8').split('\n').filter(l => l.trim());
      } else if (r.status !== 404) {
        throw new Error('read failed: ' + r.status);
      }
    } catch (e) {
      if (attempt < 3) {
        await new Promise(res => setTimeout(res, 500 * (attempt + 1)));
        continue;
      }
      return { statusCode: 502, body: 'GitHub read error: ' + e.message };
    }

    let newContent;
    if (existingLines.length === 0) {
      newContent = csvHeader + '\n' + csvRow + '\n';
    } else {
      // Replace the first line with the fixed header; keep all existing data rows
      const dataLines = existingLines.slice(1);
      newContent = csvHeader + '\n' + dataLines.join('\n') + '\n' + csvRow + '\n';
    }

    const encoded = Buffer.from(newContent, 'utf-8').toString('base64');
    const body = { message: `survey response ${data.completion_code}`, content: encoded };
    if (sha) body.sha = sha;

    const wr = await fetch(apiUrl, { method: 'PUT', headers, body: JSON.stringify(body) });
    if (wr.ok || wr.status === 201) {
      return { statusCode: 200, body: JSON.stringify({ ok: true }) };
    }
    if (wr.status === 409 && attempt < 3) {
      await new Promise(res => setTimeout(res, 300 * (attempt + 1)));
      continue;
    }
    return { statusCode: 502, body: 'GitHub write failed: ' + wr.status };
  }

  return { statusCode: 502, body: 'Max retries exceeded' };
};

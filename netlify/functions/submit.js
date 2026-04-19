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

    const keys = Object.keys(data);
    const csvRow = keys.map(k => '"' + String(data[k] ?? '').replace(/"/g, '""') + '"').join(',');
    let newContent;
    if (existingLines.length === 0) {
      const header = keys.map(k => '"' + k.replace(/"/g, '""') + '"').join(',');
      newContent = header + '\n' + csvRow + '\n';
    } else {
      newContent = existingLines.join('\n') + '\n' + csvRow + '\n';
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

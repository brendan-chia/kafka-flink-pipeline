'use strict';
const form = document.querySelector('#investigation');
const key = document.querySelector('#access-key');
const status = document.querySelector('#status');
const submit = document.querySelector('#submit');
let savedReport = null;
function headers() { return {'Content-Type':'application/json', ...(key.value ? {'X-DataLens-Key':key.value} : {})}; }
async function config() {
  try { const response = await fetch('/v1/assistant/config', {headers:headers()});
    if (!response.ok) throw new Error('Enter the DataLens access key to check configuration.');
    const value = await response.json();
    document.querySelector('#config').textContent = value.configured ? 'OpenAI · ' + value.model : 'Evidence mode · configure OPENAI_API_KEY and DATALENS_OPENAI_MODEL on the server';
  } catch (error) { document.querySelector('#config').textContent = error.message; }
}
key.addEventListener('change', config);
config();
form.addEventListener('submit', async event => {
  event.preventDefault(); submit.disabled = true; status.textContent = 'Collecting evidence and reviewing possible explanations…';
  document.querySelector('#report').hidden = true;
  savedReport = null;
  const body = Object.fromEntries(new FormData(form));
  body.window_seconds = Number(body.window_seconds); body.quality_limit = Number(body.quality_limit);
  const controller = new AbortController(); const timeout = setTimeout(() => controller.abort(), 150000);
  try {
    const response = await fetch('/v1/assistant/investigations', {method:'POST', headers:headers(), body:JSON.stringify(body), signal:controller.signal});
    const report = await response.json();
    if (!response.ok) throw new Error(typeof report.detail === 'string' ? report.detail : 'Check UTC boundaries, window alignment and input values.');
    savedReport = report; render(report); status.textContent = 'Investigation complete. Review evidence gaps before taking action.';
  } catch (error) { status.textContent = error.name === 'AbortError' ? 'The request timed out. Check service health before retrying.' : error.message; }
  finally { clearTimeout(timeout); submit.disabled = false; }
});
function render(report) {
  const titles = {discrepancy:'Revenue disagreement found', no_discrepancy_found:'No disagreement found in the audit', insufficient_evidence:'Insufficient evidence'};
  document.querySelector('#result-title').textContent = titles[report.evidence.status];
  document.querySelector('#report-meta').textContent = report.evidence.investigation_id + ' · Model: ' + report.model_status + ' · ' + report.evidence.observed_at;
  const citations = document.querySelector('#citations'); citations.replaceChildren();
  const anchors = new Map();
  report.citations.forEach((citation,index) => {
    const detail = document.createElement('details'); detail.id = 'citation-' + index; anchors.set(citation.ref, detail.id);
    const summary = document.createElement('summary'); summary.textContent = citation.kind + ' · ' + citation.title;
    const content = document.createElement('pre'); content.textContent = citation.content;
    detail.append(summary,content); citations.append(detail);
  });
  const sections = document.querySelector('#sections'); sections.replaceChildren();
  for (const [field,title] of [['observations','Observed evidence'],['suspected_causes','Suspected causes · unconfirmed'],['missing_evidence','Missing evidence'],['uncertainty','Uncertainty'],['manual_next_steps','Manual next steps']]) {
    const section = document.createElement('section'); section.className = 'result-section';
    const heading = document.createElement('h2'); heading.textContent = title; section.append(heading);
    const list = document.createElement('ul');
    for (const item of report[field]) {
      const row = document.createElement('li'); row.append(document.createTextNode(item.message));
      for (const ref of item.evidence_refs) { const anchor = document.createElement('a'); anchor.href = '#' + anchors.get(ref); anchor.textContent = '[' + ref + ']'; anchor.addEventListener('click', () => {document.getElementById(anchors.get(ref)).open = true;}); row.append(anchor); }
      list.append(row);
    }
    if (!report[field].length) {const empty = document.createElement('p'); empty.textContent = 'No supported hypothesis was returned.'; section.append(empty);}
    section.append(list); sections.append(section);
  }
  document.querySelector('#raw').textContent = JSON.stringify(report.evidence,null,2);
  document.querySelector('#report').hidden = false;
}
document.querySelector('#download').addEventListener('click', () => {
  if (!savedReport) return;
  const url = URL.createObjectURL(new Blob([JSON.stringify(savedReport,null,2)], {type:'application/json'}));
  const link = document.createElement('a'); link.href = url; link.download = 'datalens-' + savedReport.evidence.investigation_id + '.json'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
});

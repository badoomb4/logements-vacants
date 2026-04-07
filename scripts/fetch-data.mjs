import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import Papa from 'papaparse';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const DATA_DIR = path.join(ROOT, 'data');

const SOURCES = {
  communes: 'https://www.data.gouv.fr/api/1/datasets/r/2e0417b4-902d-4c60-90e7-bf5df148cb87',
  departements: 'https://www.data.gouv.fr/api/1/datasets/r/1471825a-4b22-4bd0-ba1a-f99e729cff66',
  regions: 'https://www.data.gouv.fr/api/1/datasets/r/42a34c0a-7c97-4463-b00e-5913ea5f7077',
};

function slugify(str) {
  return str
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/['']/g, '-')
    .replace(/[^a-z0-9-]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

function cleanValue(val) {
  if (val === 's' || val === 'S' || val === '') return null;
  const num = Number(val);
  return isNaN(num) ? val : num;
}

async function fetchCSV(url) {
  console.log(`Fetching ${url}...`);
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`HTTP ${resp.status} for ${url}`);
  const text = await resp.text();
  const { data } = Papa.parse(text, { header: true, delimiter: ';', skipEmptyLines: true });
  return data;
}

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function writeJSON(filePath, data) {
  await ensureDir(path.dirname(filePath));
  await fs.writeFile(filePath, JSON.stringify(data, null, 2), 'utf-8');
}

async function main() {
  console.log('=== Pipeline données LOVAC ===\n');

  // Fetch all CSVs
  const [communesRaw, departementsRaw, regionsRaw] = await Promise.all([
    fetchCSV(SOURCES.communes),
    fetchCSV(SOURCES.departements),
    fetchCSV(SOURCES.regions),
  ]);

  console.log(`\nCommunes brutes: ${communesRaw.length}`);
  console.log(`Départements bruts: ${departementsRaw.length}`);
  console.log(`Régions brutes: ${regionsRaw.length}`);

  if (communesRaw.length < 30000) {
    console.error(`ERREUR: seulement ${communesRaw.length} communes (<30000). Données probablement incomplètes.`);
    process.exit(1);
  }

  // Detect column names (may vary between LOVAC versions)
  const sampleKeys = Object.keys(communesRaw[0]);
  console.log('\nColonnes détectées:', sampleKeys.join(', '));

  // Normalize commune data — LOVAC 2025 column names: CODGEO_25, LIBGEO_25, DEP, LIB_DEP, REG, LIB_REG
  // pp_total_24 (latest total parc privé), pp_vacant_25, pp_vacant_plus_2ans_25
  const communes = communesRaw.map((row) => {
    const codeInsee = row.CODGEO_25 || row.code_commune_INSEE || row.codgeo || '';
    const nom = row.LIBGEO_25 || row.nom_commune || row.libgeo || '';
    const codeDept = row.DEP || row.code_departement || codeInsee.slice(0, 2);
    const nomDept = row.LIB_DEP || row.nom_departement || '';
    const codeRegion = row.REG || row.code_region || '';
    const nomRegion = row.LIB_REG || row.nom_region || '';

    const nbLogPrive = cleanValue(row.pp_total_24 || row.nb_log_prive);
    const nbLogVacant = cleanValue(row.pp_vacant_25 || row.nb_log_vacant);
    const nbLogVacant2ans = cleanValue(row.pp_vacant_plus_2ans_25 || row.nb_log_vacant_2ans);

    // Compute vacancy rates from raw numbers
    const tauxVacance = (nbLogPrive && nbLogVacant !== null) ? Math.round((nbLogVacant / nbLogPrive) * 10000) / 100 : null;
    const tauxVacance2ans = (nbLogPrive && nbLogVacant2ans !== null) ? Math.round((nbLogVacant2ans / nbLogPrive) * 10000) / 100 : null;

    return {
      code_insee: codeInsee.trim(),
      nom: nom.trim(),
      slug: `${slugify(nom)}-${codeInsee.trim()}`,
      code_departement: codeDept.trim(),
      nom_departement: nomDept.trim(),
      code_region: codeRegion.trim(),
      nom_region: nomRegion.trim(),
      nb_log_prive: nbLogPrive,
      nb_log_vacant: nbLogVacant,
      nb_log_vacant_2ans: nbLogVacant2ans,
      taux_vacance: tauxVacance,
      taux_vacance_2ans: tauxVacance2ans,
    };
  }).filter(c => c.code_insee && c.nom);

  console.log(`\nCommunes valides: ${communes.length}`);

  // Group by department
  const byDept = {};
  for (const c of communes) {
    if (!byDept[c.code_departement]) byDept[c.code_departement] = [];
    byDept[c.code_departement].push(c);
  }

  // Group by region
  const byRegion = {};
  for (const c of communes) {
    if (!byRegion[c.code_region]) byRegion[c.code_region] = [];
    byRegion[c.code_region].push(c);
  }

  // Compute department aggregates
  const deptStats = {};
  for (const [codeDept, communesList] of Object.entries(byDept)) {
    const withData = communesList.filter(c => c.nb_log_prive !== null && c.nb_log_vacant !== null);
    const totalPrive = withData.reduce((s, c) => s + (c.nb_log_prive || 0), 0);
    const totalVacant = withData.reduce((s, c) => s + (c.nb_log_vacant || 0), 0);
    const totalVacant2ans = withData.reduce((s, c) => s + (c.nb_log_vacant_2ans || 0), 0);
    const tauxVacance = totalPrive > 0 ? Math.round((totalVacant / totalPrive) * 10000) / 100 : 0;
    const tauxVacance2ans = totalPrive > 0 ? Math.round((totalVacant2ans / totalPrive) * 10000) / 100 : 0;

    // Sort communes by vacancy rate descending
    const sorted = [...communesList].sort((a, b) => (b.taux_vacance || 0) - (a.taux_vacance || 0));

    // Rank each commune in department
    sorted.forEach((c, i) => {
      c.rang_dept = i + 1;
      c.total_communes_dept = communesList.length;
    });

    const nomDept = communesList[0]?.nom_departement || '';
    const codeRegion = communesList[0]?.code_region || '';
    const nomRegion = communesList[0]?.nom_region || '';

    deptStats[codeDept] = {
      code: codeDept,
      nom: nomDept,
      slug: `${slugify(nomDept)}-${codeDept}`,
      code_region: codeRegion,
      nom_region: nomRegion,
      nb_communes: communesList.length,
      nb_log_prive: totalPrive,
      nb_log_vacant: totalVacant,
      nb_log_vacant_2ans: totalVacant2ans,
      taux_vacance: tauxVacance,
      taux_vacance_2ans: tauxVacance2ans,
      top_communes: sorted.slice(0, 20).map(c => ({
        code_insee: c.code_insee,
        nom: c.nom,
        slug: c.slug,
        taux_vacance: c.taux_vacance,
        nb_log_vacant: c.nb_log_vacant,
      })),
    };
  }

  // Compute region aggregates
  const regionStats = {};
  for (const [codeRegion, communesList] of Object.entries(byRegion)) {
    const withData = communesList.filter(c => c.nb_log_prive !== null && c.nb_log_vacant !== null);
    const totalPrive = withData.reduce((s, c) => s + (c.nb_log_prive || 0), 0);
    const totalVacant = withData.reduce((s, c) => s + (c.nb_log_vacant || 0), 0);
    const totalVacant2ans = withData.reduce((s, c) => s + (c.nb_log_vacant_2ans || 0), 0);
    const tauxVacance = totalPrive > 0 ? Math.round((totalVacant / totalPrive) * 10000) / 100 : 0;
    const tauxVacance2ans = totalPrive > 0 ? Math.round((totalVacant2ans / totalPrive) * 10000) / 100 : 0;

    const nomRegion = communesList[0]?.nom_region || '';
    const depts = [...new Set(communesList.map(c => c.code_departement))];

    regionStats[codeRegion] = {
      code: codeRegion,
      nom: nomRegion,
      slug: slugify(nomRegion),
      nb_communes: communesList.length,
      nb_log_prive: totalPrive,
      nb_log_vacant: totalVacant,
      nb_log_vacant_2ans: totalVacant2ans,
      taux_vacance: tauxVacance,
      taux_vacance_2ans: tauxVacance2ans,
      departements: depts.map(d => ({
        code: d,
        nom: deptStats[d]?.nom || '',
        slug: deptStats[d]?.slug || '',
        taux_vacance: deptStats[d]?.taux_vacance || 0,
        nb_log_vacant: deptStats[d]?.nb_log_vacant || 0,
      })).sort((a, b) => b.taux_vacance - a.taux_vacance),
    };
  }

  // National stats
  const allWithData = communes.filter(c => c.nb_log_prive !== null && c.nb_log_vacant !== null);
  const nationalPrive = allWithData.reduce((s, c) => s + (c.nb_log_prive || 0), 0);
  const nationalVacant = allWithData.reduce((s, c) => s + (c.nb_log_vacant || 0), 0);
  const nationalVacant2ans = allWithData.reduce((s, c) => s + (c.nb_log_vacant_2ans || 0), 0);

  const meta = {
    millesime: 2025,
    date_mise_a_jour: new Date().toISOString().split('T')[0],
    nb_communes: communes.length,
    nb_log_prive: nationalPrive,
    nb_log_vacant: nationalVacant,
    nb_log_vacant_2ans: nationalVacant2ans,
    taux_vacance: nationalPrive > 0 ? Math.round((nationalVacant / nationalPrive) * 10000) / 100 : 0,
    taux_vacance_2ans: nationalPrive > 0 ? Math.round((nationalVacant2ans / nationalPrive) * 10000) / 100 : 0,
    top_departements: Object.values(deptStats)
      .sort((a, b) => b.taux_vacance - a.taux_vacance)
      .slice(0, 10)
      .map(d => ({ code: d.code, nom: d.nom, slug: d.slug, taux_vacance: d.taux_vacance, nb_log_vacant: d.nb_log_vacant })),
  };

  // Write all JSON files
  console.log('\n=== Écriture des fichiers JSON ===');

  // Meta
  await writeJSON(path.join(DATA_DIR, 'meta.json'), meta);
  console.log('  meta.json');

  // Departments list
  const deptList = Object.values(deptStats).map(d => ({
    code: d.code, nom: d.nom, slug: d.slug,
    code_region: d.code_region, nom_region: d.nom_region,
    taux_vacance: d.taux_vacance, nb_log_vacant: d.nb_log_vacant,
  })).sort((a, b) => a.code.localeCompare(b.code));
  await writeJSON(path.join(DATA_DIR, 'departements-list.json'), deptList);
  console.log('  departements-list.json');

  // Regions list
  const regionList = Object.values(regionStats).map(r => ({
    code: r.code, nom: r.nom, slug: r.slug,
    taux_vacance: r.taux_vacance, nb_log_vacant: r.nb_log_vacant,
    nb_departements: r.departements.length,
  })).sort((a, b) => a.code.localeCompare(b.code));
  await writeJSON(path.join(DATA_DIR, 'regions-list.json'), regionList);
  console.log('  regions-list.json');

  // Per department files
  for (const [codeDept, stats] of Object.entries(deptStats)) {
    await writeJSON(path.join(DATA_DIR, 'departements', `${codeDept}.json`), stats);
  }
  console.log(`  departements/*.json (${Object.keys(deptStats).length} fichiers)`);

  // Per region files
  for (const [codeRegion, stats] of Object.entries(regionStats)) {
    await writeJSON(path.join(DATA_DIR, 'regions', `${codeRegion}.json`), stats);
  }
  console.log(`  regions/*.json (${Object.keys(regionStats).length} fichiers)`);

  // Per department commune files
  for (const [codeDept, communesList] of Object.entries(byDept)) {
    const sorted = [...communesList].sort((a, b) => (b.taux_vacance || 0) - (a.taux_vacance || 0));
    await writeJSON(path.join(DATA_DIR, 'communes', `${codeDept}.json`), sorted);
  }
  console.log(`  communes/*.json (${Object.keys(byDept).length} fichiers)`);

  // Search index
  const searchIndex = communes.map(c => ({
    nom: c.nom,
    code_insee: c.code_insee,
    slug: c.slug,
    dept: c.code_departement,
    nom_dept: c.nom_departement,
    taux: c.taux_vacance,
    nb_vacant: c.nb_log_vacant,
  }));
  await writeJSON(path.join(DATA_DIR, 'search-index.json'), searchIndex);
  console.log('  search-index.json');

  console.log('\n=== Pipeline terminé avec succès ===');
  console.log(`  ${communes.length} communes`);
  console.log(`  ${Object.keys(deptStats).length} départements`);
  console.log(`  ${Object.keys(regionStats).length} régions`);
  console.log(`  Taux de vacance national: ${meta.taux_vacance}%`);
}

main().catch((err) => {
  console.error('ERREUR FATALE:', err);
  process.exit(1);
});

/**
 * Migration script: Express/Knex (old DB) → NestJS/Prisma (new DB)
 *
 * Ishlatish:
 *   .env faylga quyidagi o'zgaruvchilarni qo'shing, keyin:
 *   node migrate.js
 *
 * .env da kerakli o'zgaruvchilar:
 *   OLD_DB_URL=postgres://...
 *   NEW_DB_URL=postgres://...
 *   AWS_ACCESS_KEY_ID=...
 *   AWS_SECRET_ACCESS_KEY=...
 *   AWS_REGION=...
 *   S3_BUCKET_NAME=...
 *   R2_ACCESS_KEY_ID=...
 *   R2_SECRET_ACCESS_KEY=...
 *   R2_ACCOUNT_ID=...
 *   R2_BUCKET_NAME=...
 *   R2_PUBLIC_DOMAIN=https://pub-xxx.r2.dev   (yoki custom domain, oxirida / yo'q)
 *
 * Tartib:
 *   1.  users        → user
 *   2.  levels       → group_level
 *   3.  teachers     → mentor
 *   4.  admins       → administration
 *   5.  groups       → group
 *   6.  students     → student (access_expires_at = payments oxirgi end_date)
 *   7.  attendance   → attendance (mentor_id yo'q bo'lsa skip)
 *   8.  assignments  → assignment + file (S3→R2 copy)
 *   9.  submissions  → assignment_submission + file (S3→R2 copy)
 *  10.  student_group_history → group_student
 */

require('dotenv').config();

const { Client }           = require('pg');
const { v4: uuidv4 }       = require('uuid');
const { S3Client,
        GetObjectCommand,
        PutObjectCommand,
        HeadObjectCommand } = require('@aws-sdk/client-s3');
const path                 = require('path');

// ─── S3 client (eski — AWS) ───────────────────────────────────────────────────
const s3 = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId:     process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});
const S3_BUCKET = process.env.S3_BUCKET_NAME;

// ─── R2 client (yangi — Cloudflare) ──────────────────────────────────────────
const r2 = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId:     process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
});
const R2_BUCKET        = process.env.R2_BUCKET_NAME;
const R2_PUBLIC_DOMAIN = (process.env.R2_PUBLIC_DOMAIN || '').replace(/\/$/, '');

// ─── DB connections ───────────────────────────────────────────────────────────
const oldDb = new Client({ connectionString: process.env.OLD_DB_URL });
const newDb = new Client({ connectionString: process.env.NEW_DB_URL });

// ─── Sanа filtri ─────────────────────────────────────────────────────────────
// Faqat shu sanadan keyingi ma'lumotlar migrate qilinadi
const SINCE = new Date('2026-04-07T00:00:00.000Z');

// ─── ID mapping tables (old integer id → new uuid) ───────────────────────────
const userMap       = new Map(); // old users.id      → new user.id (uuid)
const levelMap      = new Map(); // old levels.id     → new group_level.id (uuid)
const groupMap      = new Map(); // old groups.id     → new group.id (uuid)
const assignmentMap = new Map(); // old assignments.id → new assignment.id (uuid)
const studentMap    = new Map(); // old students.user_id → new student.id (uuid)

// ─── File helpers: S3 → R2 copy ──────────────────────────────────────────────

/**
 * S3 dagi faylni R2 ga ko'chiradi.
 * @param {string} s3Key  — eski path, masalan "assignments/images/xxx.jpg"
 * @returns {{ fileId, r2Key, r2Url, filename, mimetype, size } | null}
 */
async function copyFileToR2(s3Key) {
  if (!s3Key) return null;

  // URL decode (path da %20 kabi escaped char bo'lishi mumkin)
  const decodedKey = decodeURIComponent(s3Key);

  try {
    // 1. S3 dan stream sifatida yuklab olish
    const getCmd = new GetObjectCommand({ Bucket: S3_BUCKET, Key: decodedKey });
    const s3Res  = await s3.send(getCmd);

    // Stream → Buffer
    const chunks = [];
    for await (const chunk of s3Res.Body) chunks.push(chunk);
    const body = Buffer.concat(chunks);

    const contentType = s3Res.ContentType || guessMime(decodedKey);
    const fileSize    = body.length;
    const filename    = path.basename(decodedKey);

    // 2. R2 ga yuklash (key ni aynan saqlaymiz, structure o'zgarmasin)
    const putCmd = new PutObjectCommand({
      Bucket:      R2_BUCKET,
      Key:         decodedKey,
      Body:        body,
      ContentType: contentType,
    });
    await r2.send(putCmd);

    // 3. Public URL
    const r2Url = `${R2_PUBLIC_DOMAIN}/${decodedKey}`;

    return {
      fileId:   uuidv4(),
      r2Key:    decodedKey,
      r2Url,
      filename,
      mimetype: contentType,
      size:     fileSize,
    };
  } catch (err) {
    // Fayl S3 da topilmasa yoki boshqa xato — skip
    if (err.name === 'NoSuchKey' || err.$metadata?.httpStatusCode === 404) {
      console.warn(`\n     ⚠ S3 da topilmadi, skip: ${decodedKey}`);
    } else {
      console.warn(`\n     ⚠ Fayl ko'chirishda xato (${decodedKey}): ${err.message}`);
    }
    return null;
  }
}

/**
 * R2 da fayl allaqachon borligini tekshiradi (ikkinchi ishlatganda re-upload qilmasin).
 */
async function existsInR2(key) {
  try {
    await r2.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: key }));
    return true;
  } catch {
    return false;
  }
}

/**
 * Fayl kengaytmasiga qarab MIME type taxmin qiladi.
 */
function guessMime(filename) {
  const ext = path.extname(filename).toLowerCase();
  const map = {
    '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
    '.png': 'image/png',  '.gif': 'image/gif',
    '.webp': 'image/webp','.pdf': 'application/pdf',
    '.doc': 'application/msword',
    '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '.odt': 'application/vnd.oasis.opendocument.text',
    '.xls': 'application/vnd.ms-excel',
    '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.ppt': 'application/vnd.ms-powerpoint',
    '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    '.mp4': 'video/mp4', '.mp3': 'audio/mpeg',
    '.zip': 'application/zip',
  };
  return map[ext] || 'application/octet-stream';
}

/**
 * file table ga yozadi va file.id ni qaytaradi.
 * Agar fayl allaqachon R2 da bo'lsa, re-upload qilmaydi.
 */
async function insertFileRecord({ fileId, r2Key, r2Url, filename, mimetype, size }) {
  await newDb.query(`
    INSERT INTO "file" (
      id, key, url, filename, mimetype, size,
      bucket, is_active, created_at, updated_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    ON CONFLICT (key) DO NOTHING
  `, [
    fileId,
    r2Key,
    r2Url,
    filename,
    mimetype,
    size,
    R2_BUCKET,
    true,
    new Date(),
    new Date(),
  ]);

  // Agar conflict bo'lgan bo'lsa, mavjud id ni olamiz
  const { rows } = await newDb.query(
    `SELECT id FROM "file" WHERE key = $1`, [r2Key]
  );
  return rows[0]?.id || fileId;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function splitFullName(fullName) {
  if (!fullName || !fullName.trim()) return { first_name: null, last_name: null };
  const parts = fullName.trim().split(/\s+/);
  if (parts.length === 1) return { first_name: parts[0], last_name: null };
  const last_name  = parts[parts.length - 1];
  const first_name = parts.slice(0, -1).join(' ');
  return { first_name, last_name };
}

function mapRole(oldRole) {
  // 1-project roles: Teacher, Student, Admin (va kichik harfli variantlari)
  const r = (oldRole || '').toLowerCase();
  if (r === 'teacher')  return 'mentor';
  if (r === 'admin')    return 'admin';
  if (r === 'student')  return 'student';
  // Fallback
  return 'student';
}

function mapGroupStatus(isActive) {
  return isActive ? 'active' : 'closed';
}

function mapStudentStatus(oldStatus) {
  const s = (oldStatus || '').toLowerCase().replace(/^'|'$/g, '');
  const map = {
    new:      'new',
    active:   'active',
    inactive: 'expired',
    blocked:  'blocked',
    frozen:   'frozen',
    dropped:  'dropped',
  };
  return map[s] || 'new';
}

async function run(label, fn) {
  process.stdout.write(`  ${label}... `);
  try {
    const count = await fn();
    console.log(`✓  (${count ?? '?'} rows)`);
  } catch (err) {
    console.log(`✗`);
    console.error(`     Error in "${label}":`, err.message);
    throw err;
  }
}

// ─── Migration steps ──────────────────────────────────────────────────────────

// 1. users → user
async function migrateUsers() {
  const { rows } = await oldDb.query(
    `SELECT * FROM users WHERE deleted_at IS NULL AND created_at >= $1 ORDER BY id`,
    [SINCE]
  );

  let count = 0;
  for (const u of rows) {
    const newId = uuidv4();
    userMap.set(u.id, newId);

    const { first_name, last_name } = splitFullName(u.full_name);
    const role = mapRole(u.role);

    await newDb.query(`
      INSERT INTO "user" (
        id, phone, email, password,
        first_name, last_name, avatar_url,
        role, created_at, updated_at, deleted_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
      ON CONFLICT (id) DO NOTHING
    `, [
      newId,
      u.phone_number,
      u.email,
      u.password_hash,
      first_name,
      last_name,
      u.avatar_url,
      role,
      u.created_at,
      u.updated_at,
      u.deleted_at,
    ]);
    count++;
  }
  return count;
}

// 2. levels → group_level
async function migrateLevels() {
  const { rows } = await oldDb.query(`SELECT * FROM levels ORDER BY id`);

  let count = 0;
  for (const l of rows) {
    const newId = uuidv4();
    levelMap.set(l.id, newId);

    await newDb.query(`
      INSERT INTO "group_level" (id, level, created_at)
      VALUES ($1, $2, $3)
      ON CONFLICT (id) DO NOTHING
    `, [newId, l.name, new Date()]);
    count++;
  }
  return count;
}

// 3. teachers → mentor
// users jadvali allaqachon migrate qilingan, shuning uchun faqat mentor record yaratamiz
async function migrateMentors() {
  const { rows } = await oldDb.query(`SELECT * FROM teachers ORDER BY user_id`);

  let count = 0;
  for (const t of rows) {
    const userId = userMap.get(t.user_id);
    if (!userId) continue; // agar user migrate bo'lmagan bo'lsa (masalan, deleted_at bor)

    const mentorId = uuidv4();

    await newDb.query(`
      INSERT INTO "mentor" (id, user_id)
      VALUES ($1, $2)
      ON CONFLICT (user_id) DO NOTHING
    `, [mentorId, userId]);
    count++;
  }
  return count;
}

// 4. admins → administration
async function migrateAdmins() {
  const { rows } = await oldDb.query(`SELECT * FROM admins ORDER BY user_id`);

  let count = 0;
  for (const a of rows) {
    const userId = userMap.get(a.user_id);
    if (!userId) continue;

    await newDb.query(`
      INSERT INTO "administration" (id, user_id)
      VALUES ($1, $2)
      ON CONFLICT (user_id) DO NOTHING
    `, [uuidv4(), userId]);
    count++;
  }
  return count;
}

// 5. groups → group
async function migrateGroups() {
  const { rows } = await oldDb.query(
    `SELECT * FROM groups WHERE created_at >= $1 ORDER BY id`,
    [SINCE]
  );

  let count = 0;
  for (const g of rows) {
    const newId    = uuidv4();
    const levelId  = g.level_id  ? levelMap.get(g.level_id)  : null;
    const mentorId = g.main_teacher_id
      ? await getMentorIdByOldUserId(g.main_teacher_id)
      : null;
    const assistantId = g.assistant_teacher_id
      ? await getMentorIdByOldUserId(g.assistant_teacher_id)
      : null;

    groupMap.set(g.id, newId);

    await newDb.query(`
      INSERT INTO "group" (
        id, name, created_at, status,
        max_students, level_id, mentor_id, assistant_id
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      ON CONFLICT (id) DO NOTHING
    `, [
      newId,
      g.name,
      g.created_at,
      mapGroupStatus(g.is_active),
      g.max_students ?? 25,
      levelId,
      mentorId,
      assistantId,
    ]);
    count++;
  }
  return count;
}

// 6. students → student
// access_expires_at = payments jadvalidagi eng oxirgi end_date
async function migrateStudents() {
  const { rows } = await oldDb.query(`SELECT * FROM students ORDER BY user_id`);

  // Har bir student uchun oxirgi payment end_date ni olish
  const { rows: payRows } = await oldDb.query(`
    SELECT DISTINCT ON (student_id) student_id, end_date
    FROM payments
    ORDER BY student_id, end_date DESC
  `);
  const payMap = new Map(payRows.map(p => [p.student_id, p.end_date]));

  let count = 0;
  for (const s of rows) {
    const userId  = userMap.get(s.user_id);
    if (!userId) continue;

    const studentId = uuidv4();
    studentMap.set(s.user_id, studentId);

    const groupId     = s.current_group_id ? groupMap.get(s.current_group_id) : null;
    const expiresAt   = payMap.get(s.user_id) || null;
    const status      = mapStudentStatus(s.status);

    await newDb.query(`
      INSERT INTO "student" (
        id, user_id, status, group_id, access_expires_at
      ) VALUES ($1,$2,$3,$4,$5)
      ON CONFLICT (user_id) DO NOTHING
    `, [
      studentId,
      userId,
      status,
      groupId,
      expiresAt,
    ]);
    count++;
  }
  return count;
}

// 7. attendance → attendance  (mentor_id yo'q bo'lsa skip)
async function migrateAttendance() {
  const { rows } = await oldDb.query(
    `SELECT * FROM attendance WHERE class_date >= $1 ORDER BY id`,
    [SINCE]
  );

  let count   = 0;
  let skipped = 0;
  for (const a of rows) {
    const studentId = studentMap.get(a.student_id);
    const groupId   = a.group_id   ? groupMap.get(a.group_id) : null;
    const mentorId  = a.marked_by_teacher_id
      ? await getMentorIdByOldUserId(a.marked_by_teacher_id)
      : null;

    if (!studentId || !groupId || !mentorId) { skipped++; continue; }

    await newDb.query(`
      INSERT INTO "attendance" (
        id, created_at, date, is_present,
        student_id, group_id, mentor_id
      ) VALUES ($1,$2,$3,$4,$5,$6,$7)
      ON CONFLICT (student_id, group_id, date) DO NOTHING
    `, [
      uuidv4(),
      new Date(),
      a.class_date,
      a.is_present ?? false,
      studentId,
      groupId,
      mentorId,
    ]);
    count++;
  }
  if (skipped > 0) console.log(`\n     (${skipped} attendance skipped — mentor_id yoki boshqa FK topilmadi)`);
  return count;
}

// 8. assignments → assignment + file (S3→R2)
// Har bir assignment da image_url va file_url bo'lishi mumkin
// Ikkalasi ham file table ga yoziladi, assignment.files[] relation orqali bog'lanadi
async function migrateAssignments() {
  const { rows } = await oldDb.query(
    `SELECT * FROM assignments WHERE created_at >= $1 ORDER BY id`,
    [SINCE]
  );

  let count      = 0;
  let skipped    = 0;
  let filesCopied = 0;

  for (const a of rows) {
    const groupId = a.group_id ? groupMap.get(a.group_id) : null;
    if (!groupId) { skipped++; continue; }

    const { rows: gRows } = await newDb.query(
      `SELECT mentor_id FROM "group" WHERE id = $1`, [groupId]
    );
    const mentorId = gRows[0]?.mentor_id || null;
    if (!mentorId) { skipped++; continue; }

    const newId = uuidv4();
    assignmentMap.set(a.id, newId);

    await newDb.query(`
      INSERT INTO "assignment" (
        id, title, description, due_date,
        max_score, status, created_at,
        group_id, mentor_id
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      ON CONFLICT (id) DO NOTHING
    `, [
      newId,
      a.title,
      a.content,
      a.due_date || new Date(),
      10,
      'active',
      a.created_at,
      groupId,
      mentorId,
    ]);

    // image_url va file_url — ikkalasini ham S3→R2 ga ko'chirib file table ga bog'laymiz
    for (const s3Key of [a.image_url, a.file_url]) {
      if (!s3Key) continue;

      const decodedKey = decodeURIComponent(s3Key);
      const alreadyInR2 = await existsInR2(decodedKey);

      let fileInfo;
      if (alreadyInR2) {
        // R2 da bor — faqat DB ga yoz
        const r2Url   = `${R2_PUBLIC_DOMAIN}/${decodedKey}`;
        const filename = path.basename(decodedKey);
        fileInfo = {
          fileId:   uuidv4(),
          r2Key:    decodedKey,
          r2Url,
          filename,
          mimetype: guessMime(decodedKey),
          size:     0,
        };
      } else {
        fileInfo = await copyFileToR2(s3Key);
      }

      if (!fileInfo) continue;

      const fileId = await insertFileRecord(fileInfo);

      // assignment ↔ file bog'lanish (file.assignment_id)
      await newDb.query(
        `UPDATE "file" SET assignment_id = $1 WHERE id = $2`,
        [newId, fileId]
      );
      filesCopied++;
    }

    count++;
  }

  if (skipped    > 0) console.log(`\n     (${skipped} assignment skipped — group yoki mentor topilmadi)`);
  if (filesCopied > 0) console.log(`\n     (${filesCopied} fayl S3→R2 ga ko'chirildi)`);
  return count;
}

// 9. submissions → assignment_submission + file (S3→R2)
async function migrateSubmissions() {
  const { rows } = await oldDb.query(
    `SELECT * FROM submissions WHERE submitted_at >= $1 ORDER BY id`,
    [SINCE]
  );

  let count      = 0;
  let skipped    = 0;
  let filesCopied = 0;

  for (const s of rows) {
    const assignmentId = assignmentMap.get(s.assignment_id);
    const studentId    = studentMap.get(s.student_id);
    if (!assignmentId || !studentId) { skipped++; continue; }

    const percentage = s.grade != null ? parseFloat(s.grade) : null;
    const score      = percentage != null ? parseFloat((percentage / 10).toFixed(2)) : null;

    const submissionId = uuidv4();

    await newDb.query(`
      INSERT INTO "assignment_submission" (
        id, assignment_id, student_id,
        content, file_url,
        score, percentage,
        comment, created_at, reviewed_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (assignment_id, student_id) DO NOTHING
    `, [
      submissionId,
      assignmentId,
      studentId,
      s.submission_content,
      s.submission_file_url,   // fallback: to'g'ridan-to'g'ri path (file record bo'lmasa)
      score,
      percentage,
      s.teacher_feedback,
      s.submitted_at || new Date(),
      s.graded_by_teacher_id ? new Date() : null,
    ]);

    // submission_file_url va submission_image_url — ikkalasini S3→R2 ga ko'chiramiz
    for (const s3Key of [s.submission_file_url, s.submission_image_url]) {
      if (!s3Key) continue;

      const decodedKey  = decodeURIComponent(s3Key);
      const alreadyInR2 = await existsInR2(decodedKey);

      let fileInfo;
      if (alreadyInR2) {
        const r2Url   = `${R2_PUBLIC_DOMAIN}/${decodedKey}`;
        const filename = path.basename(decodedKey);
        fileInfo = {
          fileId:   uuidv4(),
          r2Key:    decodedKey,
          r2Url,
          filename,
          mimetype: guessMime(decodedKey),
          size:     0,
        };
      } else {
        fileInfo = await copyFileToR2(s3Key);
      }

      if (!fileInfo) continue;

      const fileId = await insertFileRecord(fileInfo);

      // assignment_submission ↔ file bog'lanish
      await newDb.query(
        `UPDATE "file" SET assignment_submission_id = $1 WHERE id = $2`,
        [submissionId, fileId]
      );
      filesCopied++;
    }

    count++;
  }

  if (skipped     > 0) console.log(`\n     (${skipped} submission skipped — assignment yoki student topilmadi)`);
  if (filesCopied > 0) console.log(`\n     (${filesCopied} fayl S3→R2 ga ko'chirildi)`);
  return count;
}

// 10. student_group_history → group_student
async function migrateGroupStudents() {
  const { rows } = await oldDb.query(
    `SELECT * FROM student_group_history WHERE joined_at >= $1 ORDER BY joined_at`,
    [SINCE]
  );

  let count   = 0;
  let skipped = 0;
  for (const h of rows) {
    const studentId = studentMap.get(h.student_id);
    const groupId   = h.group_id ? groupMap.get(h.group_id) : null;

    if (!studentId || !groupId) { skipped++; continue; }

    // Groupning mentor va assistant idlarini olamiz
    const { rows: gRows } = await newDb.query(
      `SELECT mentor_id, assistant_id FROM "group" WHERE id = $1`, [groupId]
    );
    const mentorId    = gRows[0]?.mentor_id    || null;
    const assistantId = gRows[0]?.assistant_id || null;

    await newDb.query(`
      INSERT INTO "group_student" (
        id, group_id, student_id, mentor_id, assistant_id,
        joined_at, left_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7)
      ON CONFLICT (group_id, student_id, left_at) DO NOTHING
    `, [
      uuidv4(),
      groupId,
      studentId,
      mentorId,
      assistantId,
      h.joined_at || new Date(),
      h.left_at,
    ]);
    count++;
  }
  if (skipped > 0) console.log(`\n     (${skipped} group_history skipped — student yoki group topilmadi)`);
  return count;
}

// ─── Helper: old teacher user_id → new mentor.id ─────────────────────────────
async function getMentorIdByOldUserId(oldUserId) {
  const newUserId = userMap.get(oldUserId);
  if (!newUserId) return null;
  const { rows } = await newDb.query(
    `SELECT id FROM "mentor" WHERE user_id = $1`, [newUserId]
  );
  return rows[0]?.id || null;
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  const required = [
    'OLD_DB_URL', 'NEW_DB_URL',
    'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION', 'S3_BUCKET_NAME',
    'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_ACCOUNT_ID', 'R2_BUCKET_NAME',
    'R2_PUBLIC_DOMAIN',
  ];
  const missing = required.filter(k => !process.env[k]);
  if (missing.length) {
    console.error('❌  Quyidagi environment variablelar topilmadi:');
    missing.forEach(k => console.error(`     - ${k}`));
    console.error('\n   .env faylga qo\'shing va qayta ishga tushiring.');
    process.exit(1);
  }

  console.log('🔌  DB larga ulanilmoqda...');
  await oldDb.connect();
  await newDb.connect();
  console.log('✓  Ulanish muvaffaqiyatli\n');

  console.log('🚀  Migratsiya boshlanmoqda...');
  console.log(`📅  Sana filtri: ${SINCE.toISOString()} dan boshlab\n`);

  await run('1. users        → user',              migrateUsers);
  await run('2. levels       → group_level',        migrateLevels);
  await run('3. teachers     → mentor',             migrateMentors);
  await run('4. admins       → administration',     migrateAdmins);
  await run('5. groups       → group',              migrateGroups);
  await run('6. students     → student',            migrateStudents);
  await run('7. attendance   → attendance',         migrateAttendance);
  await run('8. assignments  → assignment',         migrateAssignments);
  await run('9. submissions  → assignment_submission', migrateSubmissions);
  await run('10. student_group_history → group_student', migrateGroupStudents);

  console.log('\n✅  Migratsiya yakunlandi!');
}

main()
  .catch(err => {
    console.error('\n💥  Fatal error:', err.message);
    process.exit(1);
  })
  .finally(async () => {
    await oldDb.end().catch(() => {});
    await newDb.end().catch(() => {});
  });
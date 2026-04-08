/**
 * PARTIAL Migration script: Express/Knex (old DB) → NestJS/Prisma (new DB)
 * Faqat 2026-yil 7-aprel 00:00 (UTC) dan hozirgacha bo'lgan datalar.
 *
 * Ishlatish:
 *   node migrate-partial.js
 *
 * Tartib:
 *   1.  users (mapping uchun hammasi o'qiladi, faqat yangilari insert)
 *   2.  levels (mapping uchun hammasi o'qiladi, faqat yangilari insert)
 *   3.  teachers → mentor (mapping uchun hammasi o'qiladi, faqat yangilari insert)
 *   4.  admins → administration (yangilari)
 *   5.  groups → group (yangilari: created_at >= filter)
 *   6.  students → student (yangilari: created_at >= filter)
 *   7.  attendance → attendance (yangilari: class_date >= filter)
 *   8.  assignments → assignment + file (yangilari: created_at >= filter)
 *   9.  submissions → assignment_submission + file (yangilari: submitted_at >= filter)
 *  10.  student_group_history → group_student (yangilari: joined_at >= filter)
 */

require('dotenv').config();

const { Client }           = require('pg');
const { v4: uuidv4 }       = require('uuid');
const { S3Client,
        GetObjectCommand,
        PutObjectCommand,
        HeadObjectCommand } = require('@aws-sdk/client-s3');
const path                 = require('path');

// ─── Sana filter ──────────────────────────────────────────────────────────────
const MIGRATION_FROM = new Date('2026-04-07T00:00:00.000Z');
console.log(`📅  Filter: ${MIGRATION_FROM.toISOString()} dan hozirgacha\n`);

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

// ─── ID mapping tables (old integer id → new uuid) ───────────────────────────
// Eslatma: mapping tablalar BARCHA (eski + yangi) ma'lumotlarni o'z ichiga oladi,
// chunki yangi entities ularga FK orqali bog'liq bo'lishi mumkin.
const userMap       = new Map();
const levelMap      = new Map();
const groupMap      = new Map();
const assignmentMap = new Map();
const studentMap    = new Map();

// ─── File helpers: S3 → R2 copy ──────────────────────────────────────────────

async function copyFileToR2(s3Key) {
  if (!s3Key) return null;

  const decodedKey = decodeURIComponent(s3Key);

  try {
    const getCmd = new GetObjectCommand({ Bucket: S3_BUCKET, Key: decodedKey });
    const s3Res  = await s3.send(getCmd);

    const chunks = [];
    for await (const chunk of s3Res.Body) chunks.push(chunk);
    const body = Buffer.concat(chunks);

    const contentType = s3Res.ContentType || guessMime(decodedKey);
    const fileSize    = body.length;
    const filename    = path.basename(decodedKey);

    const putCmd = new PutObjectCommand({
      Bucket:      R2_BUCKET,
      Key:         decodedKey,
      Body:        body,
      ContentType: contentType,
    });
    await r2.send(putCmd);

    const r2Url = `${R2_PUBLIC_DOMAIN}/${decodedKey}`;

    return { fileId: uuidv4(), r2Key: decodedKey, r2Url, filename, mimetype: contentType, size: fileSize };
  } catch (err) {
    if (err.name === 'NoSuchKey' || err.$metadata?.httpStatusCode === 404) {
      console.warn(`\n     ⚠ S3 da topilmadi, skip: ${decodedKey}`);
    } else {
      console.warn(`\n     ⚠ Fayl ko'chirishda xato (${decodedKey}): ${err.message}`);
    }
    return null;
  }
}

async function existsInR2(key) {
  try {
    await r2.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: key }));
    return true;
  } catch {
    return false;
  }
}

function guessMime(filename) {
  const ext = path.extname(filename).toLowerCase();
  const map = {
    '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
    '.png': 'image/png',  '.gif': 'image/gif',
    '.webp': 'image/webp', '.pdf': 'application/pdf',
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

async function insertFileRecord({ fileId, r2Key, r2Url, filename, mimetype, size }) {
  await newDb.query(`
    INSERT INTO "file" (
      id, key, url, filename, mimetype, size,
      bucket, is_active, created_at, updated_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    ON CONFLICT (key) DO NOTHING
  `, [fileId, r2Key, r2Url, filename, mimetype, size, R2_BUCKET, true, new Date(), new Date()]);

  const { rows } = await newDb.query(`SELECT id FROM "file" WHERE key = $1`, [r2Key]);
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
  const r = (oldRole || '').toLowerCase();
  if (r === 'teacher') return 'mentor';
  if (r === 'admin')   return 'admin';
  if (r === 'student') return 'student';
  return 'student';
}

function mapGroupStatus(isActive) {
  return isActive ? 'active' : 'closed';
}

function mapStudentStatus(oldStatus) {
  const s = (oldStatus || '').toLowerCase().replace(/^'|'$/g, '');
  const map = { new: 'new', active: 'active', inactive: 'expired', blocked: 'blocked', frozen: 'frozen', dropped: 'dropped' };
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

// ─── Helper: old teacher user_id → new mentor.id ─────────────────────────────
async function getMentorIdByOldUserId(oldUserId) {
  const newUserId = userMap.get(oldUserId);
  if (!newUserId) return null;
  const { rows } = await newDb.query(`SELECT id FROM "mentor" WHERE user_id = $1`, [newUserId]);
  return rows[0]?.id || null;
}

// ─── Migration steps ──────────────────────────────────────────────────────────

/**
 * 1. users → user
 *
 * Mapping uchun BARCHA userlar o'qiladi (eski ham).
 * Lekin yangi DB ga faqat created_at >= MIGRATION_FROM bo'lganlar insert qilinadi.
 * (Eski userlar oldingi full migration da allaqachon bor deb faraz qilinadi.)
 */
async function migrateUsers() {
  // Mapping uchun barchasini o'qiymiz (FK bog'lanishlar uchun kerak)
  const { rows: allUsers } = await oldDb.query(`
    SELECT id FROM users WHERE deleted_at IS NULL ORDER BY id
  `);

  // Yangi DB dagi mavjud user'larning old_id <-> new_id mapping ini tiklash
  // (Avvalgi migratsiyada yaratilganlar uchun — lekin biz bu scriptda ularni skip qilamiz)
  // Shuning uchun faqat yangilarini insert qilamiz, mapping ni esa DB dan o'qimaymiz.
  // Eslatma: Agar oldingi full migration bo'lgan bo'lsa, mapping ni tiklash uchun
  // phone_number yoki email orqali match qilish kerak bo'ladi.
  // Bu script faqat birinchi marta ishlatiladi deb faraz qiladi (yoki test uchun).

  // Faqat yangi yaratilganlarni insert qilamiz
  const { rows } = await oldDb.query(`
    SELECT * FROM users
    WHERE deleted_at IS NULL
      AND created_at >= $1
    ORDER BY id
  `, [MIGRATION_FROM]);

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

  // Mapping ni to'ldirish: yangi DB da allaqachon bor userlar uchun ham
  // phone_number orqali match qilamiz (FK lar to'g'ri ishlashi uchun)
  const { rows: oldAllRows } = await oldDb.query(`
    SELECT id, phone_number FROM users WHERE deleted_at IS NULL ORDER BY id
  `);
  for (const ou of oldAllRows) {
    if (userMap.has(ou.id)) continue; // Allaqachon set qilingan
    const { rows: matched } = await newDb.query(
      `SELECT id FROM "user" WHERE phone = $1 LIMIT 1`, [ou.phone_number]
    );
    if (matched[0]) userMap.set(ou.id, matched[0].id);
  }

  return count;
}

/**
 * 2. levels → group_level
 *
 * Levellar odatda kam o'zgaradi. Mapping uchun barchasini o'qiymiz,
 * yangilari insert qilinadi.
 */
async function migrateLevels() {
  const { rows } = await oldDb.query(`SELECT * FROM levels ORDER BY id`);

  let count = 0;
  for (const l of rows) {
    // DB da mavjudligini tekshiramiz (name orqali)
    const { rows: existing } = await newDb.query(
      `SELECT id FROM "group_level" WHERE level = $1 LIMIT 1`, [l.name]
    );

    if (existing[0]) {
      // Mavjud — faqat mapping ga qo'shamiz
      levelMap.set(l.id, existing[0].id);
    } else {
      // Yangi — insert qilamiz
      const newId = uuidv4();
      levelMap.set(l.id, newId);
      await newDb.query(`
        INSERT INTO "group_level" (id, level, created_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (id) DO NOTHING
      `, [newId, l.name, new Date()]);
      count++;
    }
  }
  return count;
}

/**
 * 3. teachers → mentor
 *
 * Yangi userlar bilan birga kelgan teacherlar insert qilinadi.
 * Mapping uchun eski teacherlar ham DB dan o'qiladi.
 */
async function migrateMentors() {
  const { rows } = await oldDb.query(`SELECT * FROM teachers ORDER BY user_id`);

  let count = 0;
  for (const t of rows) {
    const userId = userMap.get(t.user_id);
    if (!userId) continue;

    // DB da mavjudligini tekshiramiz
    const { rows: existing } = await newDb.query(
      `SELECT id FROM "mentor" WHERE user_id = $1 LIMIT 1`, [userId]
    );
    if (existing[0]) continue; // Allaqachon bor — skip

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

/**
 * 4. admins → administration
 *
 * Yangi adminlar insert qilinadi.
 */
async function migrateAdmins() {
  const { rows } = await oldDb.query(`SELECT * FROM admins ORDER BY user_id`);

  let count = 0;
  for (const a of rows) {
    const userId = userMap.get(a.user_id);
    if (!userId) continue;

    const { rows: existing } = await newDb.query(
      `SELECT id FROM "administration" WHERE user_id = $1 LIMIT 1`, [userId]
    );
    if (existing[0]) continue; // Allaqachon bor — skip

    await newDb.query(`
      INSERT INTO "administration" (id, user_id)
      VALUES ($1, $2)
      ON CONFLICT (user_id) DO NOTHING
    `, [uuidv4(), userId]);
    count++;
  }
  return count;
}

/**
 * 5. groups → group
 *
 * Faqat created_at >= MIGRATION_FROM bo'lgan grouplar.
 * Eski grouplar mapping uchun DB dan o'qiladi.
 */
async function migrateGroups() {
  // Eski DB dagi barcha grouplar uchun mapping ni tiklash (name orqali)
  const { rows: allOldGroups } = await oldDb.query(`SELECT id, name FROM groups ORDER BY id`);
  for (const g of allOldGroups) {
    const { rows: existing } = await newDb.query(
      `SELECT id FROM "group" WHERE name = $1 LIMIT 1`, [g.name]
    );
    if (existing[0]) groupMap.set(g.id, existing[0].id);
  }

  // Yangi grouplarni insert qilamiz
  const { rows } = await oldDb.query(`
    SELECT * FROM groups
    WHERE created_at >= $1
    ORDER BY id
  `, [MIGRATION_FROM]);

  let count = 0;
  for (const g of rows) {
    if (groupMap.has(g.id)) continue; // Allaqachon mapping da bor — skip

    const newId       = uuidv4();
    const levelId     = g.level_id ? levelMap.get(g.level_id) : null;
    const mentorId    = g.main_teacher_id       ? await getMentorIdByOldUserId(g.main_teacher_id)       : null;
    const assistantId = g.assistant_teacher_id  ? await getMentorIdByOldUserId(g.assistant_teacher_id)  : null;

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

/**
 * 6. students → student
 *
 * Faqat created_at >= MIGRATION_FROM bo'lgan studentlar.
 * Eski studentlar mapping uchun DB dan o'qiladi.
 */
async function migrateStudents() {
  // Mapping tiklash: eski studentlar uchun (phone orqali user match)
  const { rows: allOldStudents } = await oldDb.query(
    `SELECT s.user_id, u.phone_number FROM students s JOIN users u ON s.user_id = u.id ORDER BY s.user_id`
  );
  for (const s of allOldStudents) {
    const { rows: existing } = await newDb.query(
      `SELECT st.id FROM student st JOIN "user" u ON st.user_id = u.id WHERE u.phone = $1 LIMIT 1`,
      [s.phone_number]
    );
    if (existing[0]) studentMap.set(s.user_id, existing[0].id);
  }

  // Yangi studentlarni insert qilamiz
  const { rows } = await oldDb.query(`
    SELECT * FROM students
    WHERE created_at >= $1
    ORDER BY user_id
  `, [MIGRATION_FROM]);

  // Payment end_date
  const { rows: payRows } = await oldDb.query(`
    SELECT DISTINCT ON (student_id) student_id, end_date
    FROM payments
    ORDER BY student_id, end_date DESC
  `);
  const payMap = new Map(payRows.map(p => [p.student_id, p.end_date]));

  let count = 0;
  for (const s of rows) {
    const userId = userMap.get(s.user_id);
    if (!userId) continue;

    if (studentMap.has(s.user_id)) continue; // Allaqachon bor — skip

    const studentId   = uuidv4();
    studentMap.set(s.user_id, studentId);

    const groupId   = s.current_group_id ? groupMap.get(s.current_group_id) : null;
    const expiresAt = payMap.get(s.user_id) || null;
    const status    = mapStudentStatus(s.status);

    await newDb.query(`
      INSERT INTO "student" (
        id, user_id, status, group_id, access_expires_at
      ) VALUES ($1,$2,$3,$4,$5)
      ON CONFLICT (user_id) DO NOTHING
    `, [studentId, userId, status, groupId, expiresAt]);
    count++;
  }
  return count;
}

/**
 * 7. attendance → attendance
 *
 * Faqat class_date >= MIGRATION_FROM bo'lgan yozuvlar.
 */
async function migrateAttendance() {
  const { rows } = await oldDb.query(`
    SELECT * FROM attendance
    WHERE class_date >= $1
    ORDER BY id
  `, [MIGRATION_FROM]);

  let count   = 0;
  let skipped = 0;
  for (const a of rows) {
    const studentId = studentMap.get(a.student_id);
    const groupId   = a.group_id ? groupMap.get(a.group_id) : null;
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
    `, [uuidv4(), new Date(), a.class_date, a.is_present ?? false, studentId, groupId, mentorId]);
    count++;
  }
  if (skipped > 0) console.log(`\n     (${skipped} attendance skipped — mentor_id yoki FK topilmadi)`);
  return count;
}

/**
 * 8. assignments → assignment + file (S3→R2)
 *
 * Faqat created_at >= MIGRATION_FROM bo'lgan topshiriqlar.
 */
async function migrateAssignments() {
  const { rows } = await oldDb.query(`
    SELECT * FROM assignments
    WHERE created_at >= $1
    ORDER BY id
  `, [MIGRATION_FROM]);

  let count       = 0;
  let skipped     = 0;
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
    `, [newId, a.title, a.content, a.due_date || new Date(), 10, 'active', a.created_at, groupId, mentorId]);

    for (const s3Key of [a.image_url, a.file_url]) {
      if (!s3Key) continue;

      const decodedKey   = decodeURIComponent(s3Key);
      const alreadyInR2  = await existsInR2(decodedKey);

      let fileInfo;
      if (alreadyInR2) {
        fileInfo = {
          fileId:   uuidv4(),
          r2Key:    decodedKey,
          r2Url:    `${R2_PUBLIC_DOMAIN}/${decodedKey}`,
          filename:  path.basename(decodedKey),
          mimetype:  guessMime(decodedKey),
          size:      0,
        };
      } else {
        fileInfo = await copyFileToR2(s3Key);
      }

      if (!fileInfo) continue;

      const fileId = await insertFileRecord(fileInfo);
      await newDb.query(`UPDATE "file" SET assignment_id = $1 WHERE id = $2`, [newId, fileId]);
      filesCopied++;
    }

    count++;
  }

  if (skipped     > 0) console.log(`\n     (${skipped} assignment skipped — group yoki mentor topilmadi)`);
  if (filesCopied > 0) console.log(`\n     (${filesCopied} fayl S3→R2 ga ko'chirildi)`);
  return count;
}

/**
 * 9. submissions → assignment_submission + file (S3→R2)
 *
 * Faqat submitted_at >= MIGRATION_FROM bo'lgan topshirmalar.
 */
async function migrateSubmissions() {
  const { rows } = await oldDb.query(`
    SELECT * FROM submissions
    WHERE submitted_at >= $1
    ORDER BY id
  `, [MIGRATION_FROM]);

  let count       = 0;
  let skipped     = 0;
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
      s.submission_file_url,
      score,
      percentage,
      s.teacher_feedback,
      s.submitted_at || new Date(),
      s.graded_by_teacher_id ? new Date() : null,
    ]);

    for (const s3Key of [s.submission_file_url, s.submission_image_url]) {
      if (!s3Key) continue;

      const decodedKey  = decodeURIComponent(s3Key);
      const alreadyInR2 = await existsInR2(decodedKey);

      let fileInfo;
      if (alreadyInR2) {
        fileInfo = {
          fileId:   uuidv4(),
          r2Key:    decodedKey,
          r2Url:    `${R2_PUBLIC_DOMAIN}/${decodedKey}`,
          filename:  path.basename(decodedKey),
          mimetype:  guessMime(decodedKey),
          size:      0,
        };
      } else {
        fileInfo = await copyFileToR2(s3Key);
      }

      if (!fileInfo) continue;

      const fileId = await insertFileRecord(fileInfo);
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

/**
 * 10. student_group_history → group_student
 *
 * Faqat joined_at >= MIGRATION_FROM bo'lgan yozuvlar.
 */
async function migrateGroupStudents() {
  const { rows } = await oldDb.query(`
    SELECT * FROM student_group_history
    WHERE joined_at >= $1
    ORDER BY joined_at
  `, [MIGRATION_FROM]);

  let count   = 0;
  let skipped = 0;
  for (const h of rows) {
    const studentId = studentMap.get(h.student_id);
    const groupId   = h.group_id ? groupMap.get(h.group_id) : null;

    if (!studentId || !groupId) { skipped++; continue; }

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
    process.exit(1);
  }

  console.log('🔌  DB larga ulanilmoqda...');
  await oldDb.connect();
  await newDb.connect();
  console.log('✓  Ulanish muvaffaqiyatli\n');

  console.log('🚀  Qisman migratsiya boshlanmoqda...\n');

  await run('1. users (yangilari)           → user',              migrateUsers);
  await run('2. levels (mapping + yangilari) → group_level',      migrateLevels);
  await run('3. teachers (yangilari)         → mentor',           migrateMentors);
  await run('4. admins (yangilari)           → administration',   migrateAdmins);
  await run('5. groups (yangilari)           → group',            migrateGroups);
  await run('6. students (yangilari)         → student',          migrateStudents);
  await run('7. attendance (7-apreldan)      → attendance',       migrateAttendance);
  await run('8. assignments (7-apreldan)     → assignment',       migrateAssignments);
  await run('9. submissions (7-apreldan)     → assignment_submission', migrateSubmissions);
  await run('10. group_history (7-apreldan)  → group_student',   migrateGroupStudents);

  console.log('\n✅  Qisman migratsiya yakunlandi!');
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
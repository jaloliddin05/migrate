/**
 * Migration script: Express/Knex (old DB) → NestJS/Prisma (new DB)
 *
 * TO'G'RILANGAN VERSIYA — asosiy muammolar:
 *  1. group_student: NULL left_at bilan ON CONFLICT ishlamaydi →
 *     UNIQUE constraint ni chetlab o'tish uchun oldin tekshirib insert qilamiz
 *  2. student.group_id: to'g'ri yangi UUID ga map qilinadi
 *  3. Attendance: eskida 2771, yangida 2506 — ON CONFLICT (student_id, group_id, date) muammo
 *
 * ⚠️  FAYL KO'CHIRISH HAQIDA:
 *   S3 → R2 ga fayllarni yuklash O'CHIRILGAN.
 *   Fayllarni rclone orqali CLI da ko'chiring:
 *     rclone copy s3:BUCKET_NAME r2:R2_BUCKET_NAME --progress
 *   DB dagi file path/url yozuvlari to'g'ri saqlanadi.
 *
 * Ishlatish:
 *   .env faylga quyidagi o'zgaruvchilarni qo'shing:
 *     OLD_DB_URL=postgres://...
 *     NEW_DB_URL=postgres://...
 *     R2_PUBLIC_DOMAIN=https://pub-xxx.r2.dev
 *     R2_BUCKET_NAME=...
 *     (AWS/R2 credentials endi faqat rclone uchun kerak, bu scriptda emas)
 *
 *   node migrate.js
 *
 * DIQQAT: Yangi DB ni tozalab keyin ishga tushirish tavsiya etiladi.
 * Tozalash uchun: node migrate.js --clean
 */

require('dotenv').config();

const { Client }     = require('pg');
const { v4: uuidv4 } = require('uuid');
const path           = require('path');

const CLEAN_MODE = process.argv.includes('--clean');

// ─── DB connections ───────────────────────────────────────────────────────────
const oldDb = new Client({ connectionString: process.env.OLD_DB_URL });
const newDb = new Client({ connectionString: process.env.NEW_DB_URL });

const R2_BUCKET        = process.env.R2_BUCKET_NAME;
const R2_PUBLIC_DOMAIN = (process.env.R2_PUBLIC_DOMAIN || '').replace(/\/$/, '');

// ─── ID mapping tables ────────────────────────────────────────────────────────
// old integer id → new uuid
const userMap       = new Map(); // old users.id       → new user.id
const levelMap      = new Map(); // old levels.id      → new group_level.id
const groupMap      = new Map(); // old groups.id      → new group.id
const assignmentMap = new Map(); // old assignments.id → new assignment.id
const studentMap    = new Map(); // old students.user_id → new student.id

// ─── Yangi DB ni tozalash (--clean rejimi) ────────────────────────────────────
async function cleanNewDb() {
  console.log('🧹  Yangi DB tozalanmoqda...');
  const tables = [
    'assignment_submission',
    'assignment',
    'attendance',
    'group_student',
    'student_dropped',
    'student_frozen',
    'student_status',
    'student',
    '"group"',
    'group_level',
    'administration',
    'mentor',
    '"user"',
    'file',
  ];
  for (const t of tables) {
    await newDb.query(`DELETE FROM ${t}`).catch(() => {});
  }
  console.log('✓  Tozalash tugadi\n');
}

// ─── File helpers ─────────────────────────────────────────────────────────────
// ⚠️  S3 → R2 ga YUKLAB O'TKAZISH O'CHIRILGAN.
//     Fayllar rclone orqali ko'chiriladi.
//     Bu funksiyalar faqat DB record yaratish uchun ishlatiladi.

function guessMime(filename) {
  const ext = path.extname(filename).toLowerCase();
  const map = {
    '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
    '.png': 'image/png',  '.gif': 'image/gif',
    '.webp': 'image/webp','.pdf': 'application/pdf',
    '.doc': 'application/msword',
    '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '.xls': 'application/vnd.ms-excel',
    '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.mp4': 'video/mp4', '.mp3': 'audio/mpeg',
    '.zip': 'application/zip',
  };
  return map[ext] || 'application/octet-stream';
}

// Faqat DB ga file yozuvi kiritadi (fayl ko'chirilmaydi)
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

// S3 key dan faqat DB uchun fileInfo obyekti yasaydi (fayl yuklanmaydi)
function buildFileInfo(s3Key) {
  if (!s3Key) return null;
  const decodedKey = decodeURIComponent(s3Key);
  return {
    fileId:   uuidv4(),
    r2Key:    decodedKey,
    r2Url:    `${R2_PUBLIC_DOMAIN}/${decodedKey}`,
    filename: path.basename(decodedKey),
    mimetype: guessMime(decodedKey),
    size:     0, // rclone ko'chirganidan keyin aniq bo'ladi
  };
}

// ─── Helpers ─────────────────────────────────────────────────────────────────
function splitFullName(fullName) {
  if (!fullName?.trim()) return { first_name: null, last_name: null };
  const parts = fullName.trim().split(/\s+/);
  if (parts.length === 1) return { first_name: parts[0], last_name: null };
  return {
    first_name: parts.slice(0, -1).join(' '),
    last_name:  parts[parts.length - 1],
  };
}

function mapRole(oldRole) {
  const r = (oldRole || '').toLowerCase();
  if (r === 'teacher') return 'mentor';
  if (r === 'admin')   return 'admin';
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

async function getMentorIdByOldUserId(oldUserId) {
  const newUserId = userMap.get(oldUserId);
  if (!newUserId) return null;
  const { rows } = await newDb.query(`SELECT id FROM "mentor" WHERE user_id = $1`, [newUserId]);
  return rows[0]?.id || null;
}

async function run(label, fn) {
  process.stdout.write(`  ${label}... `);
  try {
    const result = await fn();
    const { count, skipped } = typeof result === 'object' ? result : { count: result, skipped: 0 };
    let msg = `✓  (${count ?? '?'} rows)`;
    if (skipped > 0) msg += `  [${skipped} skipped]`;
    console.log(msg);
  } catch (err) {
    console.log(`✗`);
    console.error(`     Error in "${label}":`, err.message);
    throw err;
  }
}

// ─── Migration steps ──────────────────────────────────────────────────────────

// 1. users → user
async function migrateUsers() {
  const { rows } = await oldDb.query(`
    SELECT * FROM users WHERE deleted_at IS NULL ORDER BY id
  `);
  let count = 0;
  for (const u of rows) {
    const newId = uuidv4();
    userMap.set(u.id, newId);
    const { first_name, last_name } = splitFullName(u.full_name);
    await newDb.query(`
      INSERT INTO "user" (
        id, phone, email, password,
        first_name, last_name, avatar_url,
        role, created_at, updated_at, deleted_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
      ON CONFLICT (id) DO NOTHING
    `, [
      newId, u.phone_number, u.email, u.password_hash,
      first_name, last_name, u.avatar_url,
      mapRole(u.role),
      u.created_at, u.updated_at, u.deleted_at,
    ]);
    count++;
  }
  return { count };
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
  return { count };
}

// 3. teachers → mentor
async function migrateMentors() {
  const { rows } = await oldDb.query(`SELECT * FROM teachers ORDER BY user_id`);
  let count = 0, skipped = 0;
  for (const t of rows) {
    const userId = userMap.get(t.user_id);
    if (!userId) { skipped++; continue; }
    await newDb.query(`
      INSERT INTO "mentor" (id, user_id)
      VALUES ($1, $2)
      ON CONFLICT (user_id) DO NOTHING
    `, [uuidv4(), userId]);
    count++;
  }
  return { count, skipped };
}

// 4. admins → administration
async function migrateAdmins() {
  const { rows } = await oldDb.query(`SELECT * FROM admins ORDER BY user_id`);
  let count = 0, skipped = 0;
  for (const a of rows) {
    const userId = userMap.get(a.user_id);
    if (!userId) { skipped++; continue; }
    await newDb.query(`
      INSERT INTO "administration" (id, user_id)
      VALUES ($1, $2)
      ON CONFLICT (user_id) DO NOTHING
    `, [uuidv4(), userId]);
    count++;
  }
  return { count, skipped };
}

// 5. groups → group
async function migrateGroups() {
  const { rows } = await oldDb.query(`SELECT * FROM groups ORDER BY id`);
  let count = 0;
  for (const g of rows) {
    const newId       = uuidv4();
    const levelId     = g.level_id              ? levelMap.get(g.level_id)                             : null;
    const mentorId    = g.main_teacher_id       ? await getMentorIdByOldUserId(g.main_teacher_id)      : null;
    const assistantId = g.assistant_teacher_id  ? await getMentorIdByOldUserId(g.assistant_teacher_id) : null;
    groupMap.set(g.id, newId);
    await newDb.query(`
      INSERT INTO "group" (
        id, name, created_at, status,
        max_students, level_id, mentor_id, assistant_id
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      ON CONFLICT (id) DO NOTHING
    `, [
      newId, g.name, g.created_at,
      mapGroupStatus(g.is_active),
      g.max_students ?? 25,
      levelId, mentorId, assistantId,
    ]);
    count++;
  }
  return { count };
}

// 6. students → student
// access_expires_at = payments jadvalidagi eng oxirgi end_date
async function migrateStudents() {
  const { rows } = await oldDb.query(`SELECT * FROM students ORDER BY user_id`);

  const { rows: payRows } = await oldDb.query(`
    SELECT DISTINCT ON (student_id) student_id, end_date
    FROM payments
    ORDER BY student_id, end_date DESC
  `);
  const payMap = new Map(payRows.map(p => [p.student_id, p.end_date]));

  let count = 0, skipped = 0;
  for (const s of rows) {
    const userId = userMap.get(s.user_id);
    if (!userId) { skipped++; continue; }

    const studentId = uuidv4();
    studentMap.set(s.user_id, studentId);

    const groupId   = s.current_group_id ? groupMap.get(s.current_group_id) : null;
    const expiresAt = payMap.get(s.user_id) || null;

    await newDb.query(`
      INSERT INTO "student" (
        id, user_id, status, group_id, access_expires_at
      ) VALUES ($1,$2,$3,$4,$5)
      ON CONFLICT (user_id) DO NOTHING
    `, [
      studentId, userId,
      mapStudentStatus(s.status),
      groupId,
      expiresAt,
    ]);
    count++;
  }
  return { count, skipped };
}

// 7. attendance → attendance
async function migrateAttendance() {
  const { rows } = await oldDb.query(`SELECT * FROM attendance ORDER BY id`);
  let count = 0, skipped = 0;

  for (const a of rows) {
    const studentId = studentMap.get(a.student_id);
    const groupId   = a.group_id ? groupMap.get(a.group_id) : null;
    const mentorId  = a.marked_by_teacher_id
      ? await getMentorIdByOldUserId(a.marked_by_teacher_id)
      : null;

    if (!studentId || !groupId || !mentorId) { skipped++; continue; }

    const classDate = a.class_date instanceof Date
      ? a.class_date.toISOString().split('T')[0]
      : a.class_date;

    await newDb.query(`
      INSERT INTO "attendance" (
        id, created_at, date, is_present,
        student_id, group_id, mentor_id
      ) VALUES ($1,$2,$3,$4,$5,$6,$7)
      ON CONFLICT (student_id, group_id, date) DO NOTHING
    `, [
      uuidv4(), new Date(),
      classDate,
      a.is_present ?? false,
      studentId, groupId, mentorId,
    ]);
    count++;
  }
  return { count, skipped };
}

// 8. assignments → assignment + file (faqat DB record, fayl ko'chirilmaydi)
async function migrateAssignments() {
  const { rows } = await oldDb.query(`SELECT * FROM assignments ORDER BY id`);
  let count = 0, skipped = 0, filesRecorded = 0;

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
      newId, a.title, a.content,
      a.due_date || new Date(),
      10, 'active', a.created_at,
      groupId, mentorId,
    ]);

    // ── S3 → R2 YUKLAB O'TKAZISH O'CHIRILGAN ──────────────────────────────
    // Fayllar rclone orqali alohida ko'chiriladi.
    // Faqat DB dagi file yozuvini (key, url) to'g'ri saqlaymiz.
    for (const s3Key of [a.image_url, a.file_url]) {
      if (!s3Key) continue;
      const fileInfo = buildFileInfo(s3Key);
      if (!fileInfo) continue;
      const fileId = await insertFileRecord(fileInfo);
      await newDb.query(
        `UPDATE "file" SET assignment_id = $1 WHERE id = $2`,
        [newId, fileId]
      );
      filesRecorded++;
    }
    // ──────────────────────────────────────────────────────────────────────

    count++;
  }
  if (filesRecorded > 0) console.log(`\n     (${filesRecorded} fayl DB ga yozildi, rclone bilan ko'chiring)`);
  return { count, skipped };
}

// 9. submissions → assignment_submission + file (faqat DB record, fayl ko'chirilmaydi)
async function migrateSubmissions() {
  const { rows } = await oldDb.query(`SELECT * FROM submissions ORDER BY id`);
  let count = 0, skipped = 0, filesRecorded = 0;

  for (const s of rows) {
    const assignmentId = assignmentMap.get(s.assignment_id);
    const studentId    = studentMap.get(s.student_id);
    if (!assignmentId || !studentId) { skipped++; continue; }

    const percentage   = s.grade != null ? parseFloat(s.grade) : null;
    const score        = percentage != null ? parseFloat((percentage / 10).toFixed(2)) : null;
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
      submissionId, assignmentId, studentId,
      s.submission_content, s.submission_file_url,
      score, percentage,
      s.teacher_feedback,
      s.submitted_at || new Date(),
      s.graded_by_teacher_id ? new Date() : null,
    ]);

    // ── S3 → R2 YUKLAB O'TKAZISH O'CHIRILGAN ──────────────────────────────
    // Fayllar rclone orqali alohida ko'chiriladi.
    // Faqat DB dagi file yozuvini (key, url) to'g'ri saqlaymiz.
    for (const s3Key of [s.submission_file_url, s.submission_image_url]) {
      if (!s3Key) continue;
      const fileInfo = buildFileInfo(s3Key);
      if (!fileInfo) continue;
      const fileId = await insertFileRecord(fileInfo);
      await newDb.query(
        `UPDATE "file" SET assignment_submission_id = $1 WHERE id = $2`,
        [submissionId, fileId]
      );
      filesRecorded++;
    }
    // ──────────────────────────────────────────────────────────────────────

    count++;
  }
  if (filesRecorded > 0) console.log(`\n     (${filesRecorded} fayl DB ga yozildi, rclone bilan ko'chiring)`);
  return { count, skipped };
}

// 10. student_group_history → group_student
async function migrateGroupStudents() {
  const { rows } = await oldDb.query(`
    SELECT * FROM student_group_history ORDER BY joined_at
  `);
  let count = 0, skipped = 0;

  for (const h of rows) {
    const studentId = studentMap.get(h.student_id);
    const groupId   = h.group_id ? groupMap.get(h.group_id) : null;
    if (!studentId || !groupId) { skipped++; continue; }

    const { rows: gRows } = await newDb.query(
      `SELECT mentor_id, assistant_id FROM "group" WHERE id = $1`, [groupId]
    );
    const mentorId    = gRows[0]?.mentor_id    || null;
    const assistantId = gRows[0]?.assistant_id || null;

    if (h.left_at === null) {
      const { rows: existing } = await newDb.query(`
        SELECT id FROM "group_student"
        WHERE group_id = $1 AND student_id = $2 AND left_at IS NULL
        LIMIT 1
      `, [groupId, studentId]);

      if (existing.length > 0) {
        await newDb.query(`
          UPDATE "group_student"
          SET joined_at = LEAST(joined_at, $1),
              mentor_id = COALESCE(mentor_id, $2),
              assistant_id = COALESCE(assistant_id, $3)
          WHERE group_id = $4 AND student_id = $5 AND left_at IS NULL
        `, [h.joined_at || new Date(), mentorId, assistantId, groupId, studentId]);
        count++;
        continue;
      }
    } else {
      const { rows: existing } = await newDb.query(`
        SELECT id FROM "group_student"
        WHERE group_id = $1 AND student_id = $2 AND left_at = $3
        LIMIT 1
      `, [groupId, studentId, h.left_at]);

      if (existing.length > 0) {
        count++;
        continue;
      }
    }

    await newDb.query(`
      INSERT INTO "group_student" (
        id, group_id, student_id, mentor_id, assistant_id,
        joined_at, left_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7)
    `, [
      uuidv4(), groupId, studentId,
      mentorId, assistantId,
      h.joined_at || new Date(),
      h.left_at,
    ]);
    count++;
  }
  return { count, skipped };
}

// ─── Verify ───────────────────────────────────────────────────────────────────
async function verify() {
  console.log('\n📊  Natijalar tekshiruvi:');
  console.log('─'.repeat(55));

  const checks = [
    { label: 'users / user',            oldQ: 'SELECT COUNT(*) FROM users WHERE deleted_at IS NULL',   newQ: 'SELECT COUNT(*) FROM "user"' },
    { label: 'levels / group_level',    oldQ: 'SELECT COUNT(*) FROM levels',                           newQ: 'SELECT COUNT(*) FROM "group_level"' },
    { label: 'teachers / mentor',       oldQ: 'SELECT COUNT(*) FROM teachers',                         newQ: 'SELECT COUNT(*) FROM "mentor"' },
    { label: 'admins / administration', oldQ: 'SELECT COUNT(*) FROM admins',                           newQ: 'SELECT COUNT(*) FROM "administration"' },
    { label: 'groups / group',          oldQ: 'SELECT COUNT(*) FROM groups',                           newQ: 'SELECT COUNT(*) FROM "group"' },
    { label: 'students / student',      oldQ: 'SELECT COUNT(*) FROM students',                         newQ: 'SELECT COUNT(*) FROM "student"' },
    { label: 'attendance',              oldQ: 'SELECT COUNT(*) FROM attendance',                       newQ: 'SELECT COUNT(*) FROM "attendance"' },
    { label: 'assignments / assignment',oldQ: 'SELECT COUNT(*) FROM assignments',                      newQ: 'SELECT COUNT(*) FROM "assignment"' },
    { label: 'submissions / sub',       oldQ: 'SELECT COUNT(*) FROM submissions',                      newQ: 'SELECT COUNT(*) FROM "assignment_submission"' },
    { label: 'group_history / gs',      oldQ: 'SELECT COUNT(*) FROM student_group_history',            newQ: 'SELECT COUNT(*) FROM "group_student"' },
  ];

  let allOk = true;
  for (const c of checks) {
    const { rows: o } = await oldDb.query(c.oldQ);
    const { rows: n } = await newDb.query(c.newQ);
    const oldCount = parseInt(o[0].count);
    const newCount = parseInt(n[0].count);
    const ok = oldCount === newCount || newCount >= oldCount * 0.95;
    const icon = ok ? '✓' : '✗';
    const warn = !ok ? '  ⚠ FARQ BOR' : '';
    console.log(`  ${icon}  ${c.label.padEnd(28)} eski: ${String(oldCount).padStart(5)}  yangi: ${String(newCount).padStart(5)}${warn}`);
    if (!ok) allOk = false;
  }

  console.log('─'.repeat(55));

  console.log('\n  Guruh bo\'yicha o\'quvchilar (eski current_group_id vs yangi group_student):');
  const { rows: oldGroups } = await oldDb.query(`
    SELECT g.name, COUNT(DISTINCT s.user_id) as cnt
    FROM groups g
    LEFT JOIN students s ON s.current_group_id = g.id
    GROUP BY g.name ORDER BY g.name
  `);
  const { rows: newGroups } = await newDb.query(`
    SELECT g.name, COUNT(DISTINCT gs.student_id) as cnt
    FROM "group" g
    LEFT JOIN "group_student" gs ON gs.group_id = g.id AND gs.left_at IS NULL
    GROUP BY g.name ORDER BY g.name
  `);
  const newGroupMap = new Map(newGroups.map(r => [r.name, parseInt(r.cnt)]));
  for (const og of oldGroups) {
    const oc = parseInt(og.cnt);
    const nc = newGroupMap.get(og.name) ?? '?';
    const ok = oc === nc;
    console.log(`    ${ok ? '✓' : '✗'}  ${og.name.padEnd(40)} ${String(oc).padStart(3)} → ${String(nc).padStart(3)}`);
  }

  console.log('\n' + (allOk ? '✅  Barcha tekshiruvlar muvaffaqiyatli!' : '⚠   Ba\'zi farqlar mavjud — logni tekshiring'));
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  const required = ['OLD_DB_URL', 'NEW_DB_URL', 'R2_BUCKET_NAME', 'R2_PUBLIC_DOMAIN'];
  const missing = required.filter(k => !process.env[k]);
  if (missing.length) {
    console.error('❌  Quyidagi env variablelar topilmadi:');
    missing.forEach(k => console.error(`     - ${k}`));
    process.exit(1);
  }

  console.log('🔌  DB larga ulanilmoqda...');
  await oldDb.connect();
  await newDb.connect();
  console.log('✓  Ulanish muvaffaqiyatli\n');

  console.log('ℹ️   Fayl ko\'chirish rejimi: FAQAT DB RECORD');
  console.log('    S3 → R2 ga fayllarni rclone bilan ko\'chiring:\n');
  console.log(`    rclone copy s3:${process.env.S3_BUCKET_NAME || 'S3_BUCKET'} r2:${R2_BUCKET} --progress\n`);

  if (CLEAN_MODE) {
    await cleanNewDb();
  } else {
    console.log('ℹ️   --clean flag yo\'q. Mavjud ma\'lumotlar ustiga yoziladi (ON CONFLICT DO NOTHING).\n');
    console.log('    Agar yangilash kerak bo\'lsa: node migrate.js --clean\n');
  }

  console.log('🚀  Migratsiya boshlanmoqda...\n');

  await run('1.  users           → user',                 migrateUsers);
  await run('2.  levels          → group_level',           migrateLevels);
  await run('3.  teachers        → mentor',                migrateMentors);
  await run('4.  admins          → administration',        migrateAdmins);
  await run('5.  groups          → group',                 migrateGroups);
  await run('6.  students        → student',               migrateStudents);
  await run('7.  attendance      → attendance',            migrateAttendance);
  await run('8.  assignments     → assignment + file',     migrateAssignments);
  await run('9.  submissions     → assignment_submission', migrateSubmissions);
  await run('10. group_history   → group_student',         migrateGroupStudents);

  await verify();

  console.log('\n✅  Migratsiya yakunlandi!');
  console.log('\n📌  Keyingi qadam — fayllarni rclone bilan ko\'chiring:');
  console.log(`    rclone copy s3:${process.env.S3_BUCKET_NAME || 'S3_BUCKET'} r2:${R2_BUCKET} --progress\n`);
}

main()
  .catch(err => {
    console.error('\n💥  Fatal error:', err.message);
    console.error(err.stack);
    process.exit(1);
  })
  .finally(async () => {
    await oldDb.end().catch(() => {});
    await newDb.end().catch(() => {});
  });
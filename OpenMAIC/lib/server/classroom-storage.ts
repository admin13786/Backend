import { promises as fs } from 'fs';
import path from 'path';
import os from 'os';
import type { NextRequest } from 'next/server';
import type { Scene, Stage } from '@/lib/types/stage';

// IMPORTANT:
// In some container/deployment environments `process.cwd()` may not be writable
// (e.g. /app). Use a safe default writable directory instead.
const DATA_ROOT_DIR =
  process.env.OPENMAIC_DATA_ROOT || path.join(os.homedir(), '.openmaic-data');

export const CLASSROOMS_DIR = path.join(DATA_ROOT_DIR, 'classrooms');
export const CLASSROOM_JOBS_DIR = path.join(DATA_ROOT_DIR, 'classroom-jobs');

async function ensureDir(dir: string) {
  await fs.mkdir(dir, { recursive: true });
}

export async function ensureClassroomsDir() {
  await ensureDir(CLASSROOMS_DIR);
}

export async function ensureClassroomJobsDir() {
  await ensureDir(CLASSROOM_JOBS_DIR);
}

export async function writeJsonFileAtomic(filePath: string, data: unknown) {
  const dir = path.dirname(filePath);
  await ensureDir(dir);

  const tempFilePath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  const content = JSON.stringify(data, null, 2);
  await fs.writeFile(tempFilePath, content, 'utf-8');
  await fs.rename(tempFilePath, filePath);
}

export function buildRequestOrigin(req: NextRequest): string {
  const forwardedHost = req.headers.get('x-forwarded-host');
  if (forwardedHost) {
    const proto = req.headers.get('x-forwarded-proto') || 'http';
    // 如果通过 /openmaic 代理访问，需要保留路径前缀
    const forwardedPrefix = req.headers.get('x-forwarded-prefix') || '';
    return `${proto}://${forwardedHost}${forwardedPrefix}`;
  }
  return req.nextUrl.origin;
}

/**
 * Origin for absolute URLs in redirects / links shown to browsers.
 * Docker often reports host 0.0.0.0 — browsers cannot open that (ERR_ADDRESS_INVALID).
 * Set OPENMAIC_PUBLIC_ORIGIN (e.g. http://localhost:3000) when behind a proxy or odd bind address.
 */
export function resolvePublicOrigin(req: NextRequest): string {
  const explicit =
    process.env.OPENMAIC_PUBLIC_ORIGIN?.trim() || process.env.NEXT_PUBLIC_APP_URL?.trim();
  if (explicit) {
    return explicit.replace(/\/$/, '');
  }
  let origin = buildRequestOrigin(req);
  try {
    const parsed = new URL(origin);
    if (parsed.hostname === '0.0.0.0') {
      parsed.hostname = 'localhost';
      origin = parsed.origin;
    }
  } catch {
    /* keep origin */
  }
  return origin;
}

export interface PersistedClassroomData {
  id: string;
  stage: Stage;
  scenes: Scene[];
  createdAt: string;
}

export function isValidClassroomId(id: string): boolean {
  return /^[a-zA-Z0-9_-]+$/.test(id);
}

export async function readClassroom(id: string): Promise<PersistedClassroomData | null> {
  const filePath = path.join(CLASSROOMS_DIR, `${id}.json`);
  try {
    const content = await fs.readFile(filePath, 'utf-8');
    return JSON.parse(content) as PersistedClassroomData;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return null;
    }
    throw error;
  }
}

export async function persistClassroom(
  data: {
    id: string;
    stage: Stage;
    scenes: Scene[];
  },
  baseUrl: string,
): Promise<PersistedClassroomData & { url: string }> {
  const classroomData: PersistedClassroomData = {
    id: data.id,
    stage: data.stage,
    scenes: data.scenes,
    createdAt: new Date().toISOString(),
  };

  await ensureClassroomsDir();
  const filePath = path.join(CLASSROOMS_DIR, `${data.id}.json`);
  await writeJsonFileAtomic(filePath, classroomData);

  return {
    ...classroomData,
    url: `${baseUrl}/classroom/${data.id}`,
  };
}

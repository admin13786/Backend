import { NextRequest, NextResponse } from 'next/server';
import { resolvePublicOrigin } from '@/lib/server/classroom-storage';

function readString(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function attachPrefillCookie(res: NextResponse, title: string) {
  res.cookies.set('openmaic_prefill', encodeURIComponent(title), {
    httpOnly: false,
    sameSite: 'lax',
    maxAge: 5 * 60, // 5 minutes
    path: '/',
  });
}

function extractPayload(body: unknown): {
  title?: string;
  classroomId?: string;
  redirect?: boolean;
  to?: string;
} {
  if (!body || typeof body !== 'object') return {};
  const b = body as Record<string, unknown>;

  const title = readString(b.title) ?? readString(b.content) ?? readString(b.text);
  const classroomId = readString(b.classroomId) ?? readString(b.classroom_id);
  const redirect = typeof b.redirect === 'boolean' ? b.redirect : undefined;
  const to = readString(b.to);

  return { title, classroomId, redirect, to };
}

export async function POST(req: NextRequest) {
  let body: unknown = null;
  try {
    body = await req.json();
  } catch {
    // allow empty body and return 400 below
  }

  const { title, classroomId, redirect, to } = extractPayload(body);
  if (!title) {
    return NextResponse.json(
      { ok: false, error: 'Missing required field: title (or content/text)' },
      { status: 400 },
    );
  }

  const shouldRedirect = redirect !== false;

  // Optional: redirect immediately when classroomId is known.
  if (shouldRedirect && classroomId) {
    const res = NextResponse.redirect(
      (() => {
        const base = `${resolvePublicOrigin(req)}/`;
        const url = new URL(`/classroom/${encodeURIComponent(classroomId)}`, base);
        url.searchParams.set('prefill', title);
        return url;
      })(),
      302,
    );
    attachPrefillCookie(res, title);
    return res;
  }

  if (shouldRedirect && to === 'home') {
    const res = NextResponse.redirect(
      (() => {
        const base = `${resolvePublicOrigin(req)}/`;
        const home = new URL('/', base);
        home.searchParams.set('prefill', title);
        return home;
      })(),
      302,
    );
    attachPrefillCookie(res, title);
    return res;
  }

  const res = NextResponse.json({ ok: true });
  attachPrefillCookie(res, title);
  return res;
}

export async function GET(req: NextRequest) {
  const url = new URL(req.url);
  const title = readString(url.searchParams.get('title') ?? url.searchParams.get('content') ?? undefined);
  const classroomId = readString(url.searchParams.get('classroomId') ?? undefined);
  const to = readString(url.searchParams.get('to') ?? undefined);

  if (!title) {
    return NextResponse.json({ ok: false, error: 'Missing required param: title' }, { status: 400 });
  }

  if (classroomId) {
    const res = NextResponse.redirect(
      (() => {
        const base = `${resolvePublicOrigin(req)}/`;
        const redirectUrl = new URL(`/classroom/${encodeURIComponent(classroomId)}`, base);
        redirectUrl.searchParams.set('prefill', title);
        return redirectUrl;
      })(),
      302,
    );
    attachPrefillCookie(res, title);
    return res;
  }

  if (to === 'home') {
    const res = NextResponse.redirect(
      (() => {
        const base = `${resolvePublicOrigin(req)}/`;
        const home = new URL('/', base);
        home.searchParams.set('prefill', title);
        return home;
      })(),
      302,
    );
    attachPrefillCookie(res, title);
    return res;
  }

  const res = NextResponse.json({ ok: true });
  attachPrefillCookie(res, title);
  return res;
}

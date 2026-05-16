import { NextResponse } from 'next/server';

const TOPIC_NAME = 'linkroom-ingestion';
const PROJECT_ID = process.env.NEXT_PUBLIC_GCP_PROJECT_ID || process.env.GCP_PROJECT_ID;

// Get an OAuth2 access token from the service account credentials
async function getAccessToken(): Promise<string> {
  const credsJson = process.env.GOOGLE_CREDENTIALS_JSON;
  if (!credsJson) throw new Error('GOOGLE_CREDENTIALS_JSON is not set');

  const creds = JSON.parse(credsJson);

  // Create a JWT assertion signed with the private key
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: 'RS256', typ: 'JWT' };
  const payload = {
    iss: creds.client_email,
    scope: 'https://www.googleapis.com/auth/pubsub',
    aud: 'https://oauth2.googleapis.com/token',
    iat: now,
    exp: now + 3600,
  };

  const encode = (obj: object) =>
    Buffer.from(JSON.stringify(obj)).toString('base64url');

  const signingInput = `${encode(header)}.${encode(payload)}`;

  // Import the private key and sign
  const privateKey = await crypto.subtle.importKey(
    'pkcs8',
    pemToDer(creds.private_key),
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    false,
    ['sign']
  );

  const signature = await crypto.subtle.sign(
    'RSASSA-PKCS1-v1_5',
    privateKey,
    new TextEncoder().encode(signingInput)
  );

  const jwt = `${signingInput}.${Buffer.from(signature).toString('base64url')}`;

  // Exchange JWT for an access token
  const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: jwt,
    }),
  });

  const tokenData = await tokenRes.json();
  if (!tokenData.access_token) throw new Error(`Token exchange failed: ${JSON.stringify(tokenData)}`);
  return tokenData.access_token;
}

function pemToDer(pem: string): ArrayBuffer {
  const b64 = pem
    .replace(/-----BEGIN PRIVATE KEY-----/, '')
    .replace(/-----END PRIVATE KEY-----/, '')
    .replace(/\n/g, '');
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes.buffer;
}

export async function POST(req: Request) {
  try {
    const body = await req.json();
    const { workspace_id, uploaded_by, source_type, raw_text, file_url, file_name } = body;

    if (!workspace_id || !uploaded_by || !source_type || !raw_text) {
      return NextResponse.json({ error: 'Missing required fields' }, { status: 400 });
    }

    const payload = {
      workspace_id,
      uploaded_by,
      timestamp: new Date().toISOString(),
      source_type,
      raw_text,
      ...(file_url ? { file_url } : {}),
      ...(file_name ? { file_name } : {}),
    };

    // Publish via REST API (avoids gRPC issues in serverless environments)
    const accessToken = await getAccessToken();
    const topicPath = `projects/${PROJECT_ID}/topics/${TOPIC_NAME}`;
    const pubRes = await fetch(
      `https://pubsub.googleapis.com/v1/${topicPath}:publish`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${accessToken}`,
        },
        body: JSON.stringify({
          messages: [
            { data: Buffer.from(JSON.stringify(payload)).toString('base64') },
          ],
        }),
      }
    );

    if (!pubRes.ok) {
      const err = await pubRes.json();
      console.error('PubSub REST error:', err);
      return NextResponse.json({ error: 'Failed to publish message' }, { status: 500 });
    }

    return NextResponse.json({ message: 'Ingestion payload accepted.' }, { status: 202 });
  } catch (error) {
    console.error('Ingest error:', error);
    return NextResponse.json({ error: 'Failed to process ingestion' }, { status: 500 });
  }
}

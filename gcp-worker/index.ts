import * as dotenv from 'dotenv';
import path from 'path';

// Load environment variables from the parent directory's .env.local file
dotenv.config({ path: path.resolve(__dirname, '../../.env.local') });

import { cloudEvent } from '@google-cloud/functions-framework';
import { Firestore, FieldValue } from '@google-cloud/firestore';

const PROJECT_ID = process.env.GCP_PROJECT_ID || process.env.NEXT_PUBLIC_GCP_PROJECT_ID;
const LOCATION = process.env.GCP_LOCATION || 'us-central1';

const firestore = new Firestore({ projectId: PROJECT_ID, preferRest: true });

interface PubSubMessage {
  workspace_id: string;
  uploaded_by: string;
  timestamp: string;
  source_type: "email_string" | "meeting_transcript" | "document";
  raw_text: string;
  file_data?: string;
  file_name?: string;
}

cloudEvent('processIngestion', async (cloudEvent: any) => {
  const base64name = cloudEvent.data?.message?.data;
  if (!base64name) {
    console.error('No data found in the Pub/Sub message.');
    return;
  }

  const payloadString = Buffer.from(base64name, 'base64').toString();
  const payload: PubSubMessage = JSON.parse(payloadString);

  console.log(`Processing payload for workspace: ${payload.workspace_id}`);

  // API key read only from environment — never hardcoded
  const apiKey = process.env.GEMINI_API_KEY || '';

  try {
    let embeddingVector: number[] = Array(768).fill(0);
    
    // 1. Generate Embeddings via Service Account OAuth2 (Gemini Embedding 2)
    try {
      const { GoogleAuth } = require('google-auth-library');
      const auth = new GoogleAuth({
        scopes: ['https://www.googleapis.com/auth/generative-language', 'https://www.googleapis.com/auth/cloud-platform']
      });
      const client = await auth.getClient();
      const tokenResponse = await client.getAccessToken();
      const bearerToken = `Bearer ${tokenResponse.token}`;

      const embeddingUrl = `https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-2:embedContent`;
      const embeddingRes = await fetch(embeddingUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': bearerToken },
        body: JSON.stringify({ content: { parts: [{ text: payload.raw_text }] } })
      });
      const embeddingData = await embeddingRes.json();
      if (embeddingData.embedding?.values) {
        embeddingVector = embeddingData.embedding.values;
        console.log("✅ Embeddings generated via Service Account (Gemini Embedding 2)");
      } else if (apiKey) {
        // Fallback: API key for embeddings
        const fallbackUrl = `https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-2:embedContent?key=${apiKey}`;
        const fallbackRes = await fetch(fallbackUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ content: { parts: [{ text: payload.raw_text }] } })
        });
        const fallbackData = await fallbackRes.json();
        if (fallbackData.embedding?.values) {
          embeddingVector = fallbackData.embedding.values;
          console.log("✅ Embeddings generated via API key fallback");
        } else {
          console.warn("Embedding fetch failed:", JSON.stringify(embeddingData));
        }
      } else {
        console.warn("Embedding failed (no fallback available):", JSON.stringify(embeddingData));
      }
    } catch (e) {
      console.warn("Embedding fetch failed, using zero-vector", e);
    }


    // 2. Save Chunk to Firestore Immediately
    if (payload.source_type !== 'refresh_ai' as any) {
      const ledgerChunk = {
        workspace_id: payload.workspace_id,
        uploaded_by: payload.uploaded_by,
        timestamp: payload.timestamp || new Date().toISOString(),
        source_type: payload.source_type,
        raw_text: payload.raw_text,
        file_data: payload.file_data || null,
        file_name: payload.file_name || null,
        ...(embeddingVector.length > 0 && embeddingVector[0] !== 0 ? { embedding_vector: FieldValue.vector(embeddingVector) } : {})
      };

      const docRef = await firestore.collection('workspace_ledger_chunks').add(ledgerChunk);
      console.log(`Document written with ID: ${docRef.id}`);
    } else {
      console.log("Refresh requested. Skipping chunk save.");
    }

    // 3. Fetch Historical Context for Multi-Tenant Analysis
    const snapshot = await firestore.collection('workspace_ledger_chunks')
      .where('workspace_id', '==', payload.workspace_id)
      .get();
      
    const allChunks = snapshot.docs.map(d => d.data() as any).sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());
    
    let historyText = `PROJECT TRANSACTION HISTORY:\n\n`;
    allChunks.forEach(c => {
      historyText += `--- EVENT [${c.source_type}] FROM [${c.uploaded_by}] ON [${c.timestamp}] ---\n${c.raw_text}\n\n`;
    });

    // 4. Ecosystem Governance AI Orchestration
    const prompt = `You are an Ecosystem Governance AI Architect monitoring B2B partner linkages.
    Analyze the transaction history provided and map out relationship operational metrics.
    
    CRITICAL OUTPUT INSTRUCTIONS:
    Return STRICTLY a raw JSON object containing these exact keys:
    1. "summary_markdown": A concise 3-4 sentence cohesive narrative paragraph outlining current progress. Explicitly state the names of the organizations involved. Do NOT use bullet points, and do NOT use markdown bolding (**).
    2. "relationship_health_score": An integer from 1 to 100 evaluating collaboration velocity, response alignment, and trust.
    3. "stagnation_risk": A string matching exactly either "LOW", "MEDIUM", or "HIGH".
    4. "admin_intervention_required": A boolean (true or false) indicating if an ecosystem admin needs to step in to resolve a standstill or contract deadlock.
    5. "action_items": An array of objects each containing "task", "assignee_domain", and "status" ('pending' or 'completed').

    TRANSACTION LEDGER CONTEXT:
    ${historyText}`;

    let summary_markdown = `Active relationship ledger processing with ${allChunks.length} logged data transactions.`;
    let relationship_health_score = 100;
    let stagnation_risk = "LOW";
    let admin_intervention_required = false;
    let action_items: any[] = [{ task: "Review latest transaction timeline", assignee_domain: payload.uploaded_by, status: 'pending' }];

    // --- AI ORCHESTRATION PIPELINE ---
    let aiSuccess = false;

    const callGemini = async (authHeader: string, modelName: string): Promise<boolean> => {
      try {
        console.log(`Attempting Gemini (${modelName}) via Service Account OAuth2...`);
        const url = `https://generativelanguage.googleapis.com/v1beta/models/${modelName}:generateContent`;
        const res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': authHeader },
          body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] })
        });
        const data = await res.json();

        if (data.error) {
          console.warn(`[${modelName}] error [${data.error.code}]: ${data.error.message?.substring(0, 120)}`);
          return false;
        }

        if (data.candidates?.[0]?.content?.parts?.[0]?.text) {
          const text = data.candidates[0].content.parts[0].text;
          const jsonMatch = text.match(/\{[\s\S]*\}/);
          if (jsonMatch) {
            const aiExtracted = JSON.parse(jsonMatch[0]);
            summary_markdown = aiExtracted.summary_markdown || summary_markdown;
            relationship_health_score = aiExtracted.relationship_health_score ?? relationship_health_score;
            stagnation_risk = aiExtracted.stagnation_risk || stagnation_risk;
            admin_intervention_required = aiExtracted.admin_intervention_required ?? admin_intervention_required;
            action_items = aiExtracted.action_items || action_items;
            console.log(`✅ AI Success via Service Account (${modelName})!`);
            return true;
          } else {
            console.warn(`[${modelName}] returned text but no JSON block found.`);
          }
        }
      } catch (e: any) {
        console.warn(`[${modelName}] exception:`, e.message);
      }
      return false;
    };

    // PRIMARY: Use Service Account (linkroom-key.json) to get OAuth2 Bearer token
    try {
      const { GoogleAuth } = require('google-auth-library');
      const auth = new GoogleAuth({
        scopes: ['https://www.googleapis.com/auth/generative-language', 'https://www.googleapis.com/auth/cloud-platform']
      });
      const client = await auth.getClient();
      const tokenResponse = await client.getAccessToken();
      const bearerToken = `Bearer ${tokenResponse.token}`;

      // Try models in order of preference
      const modelsToTry = ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-2.0-flash-lite'];
      for (const model of modelsToTry) {
        if (aiSuccess) break;
        aiSuccess = await callGemini(bearerToken, model);
      }
    } catch (authErr: any) {
      console.warn('Service Account auth failed, will try API key fallback:', authErr.message);
    }

    // FALLBACK: API key (from .env.local) if SA auth fails
    if (!aiSuccess && apiKey) {
      const modelsToAttempt = ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-2.0-flash-lite'];
      for (const modelName of modelsToAttempt) {
        if (aiSuccess) break;
        try {
          console.log(`Fallback: Attempting ${modelName} via API key...`);
          const geminiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${modelName}:generateContent?key=${apiKey}`;
          const geminiRes = await fetch(geminiUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] })
          });
          const geminiData = await geminiRes.json();
          if (geminiData.error) {
            console.warn(`API key fallback [${modelName}] error: [${geminiData.error.code}] ${geminiData.error.message?.substring(0, 80)}`);
            continue;
          }
          if (geminiData.candidates?.[0]?.content?.parts?.[0]?.text) {
            const text = geminiData.candidates[0].content.parts[0].text;
            const jsonMatch = text.match(/\{[\s\S]*\}/);
            if (jsonMatch) {
              const aiExtracted = JSON.parse(jsonMatch[0]);
              summary_markdown = aiExtracted.summary_markdown || summary_markdown;
              relationship_health_score = aiExtracted.relationship_health_score ?? relationship_health_score;
              stagnation_risk = aiExtracted.stagnation_risk || stagnation_risk;
              admin_intervention_required = aiExtracted.admin_intervention_required ?? admin_intervention_required;
              action_items = aiExtracted.action_items || action_items;
              aiSuccess = true;
              console.log(`✅ AI Success via API key fallback (${modelName})!`);
            }
          }
        } catch (e: any) {
          console.warn(`API key fallback ${modelName} exception:`, e.message);
        }
      }
    }

    if (!aiSuccess) {
      console.error("❌ All AI paths exhausted. Saving default placeholder.");
    }

    // 6. Save Complete Ecosystem Governance Metrics to Firestore
    await firestore.collection('room_summaries').doc(payload.workspace_id).set({
      summary_markdown: summary_markdown.replace(/\*\*/g, ''),
      relationship_health_score,
      stagnation_risk,
      admin_intervention_required,
      action_items,
      updated_at: new Date().toISOString()
    });
    console.log(`Global Summary and Ecosystem Analytics updated for workspace: ${payload.workspace_id}`);

  } catch (error) {
    console.error('Error processing ingestion:', error);
    throw error;
  }
});
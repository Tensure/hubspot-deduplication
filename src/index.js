import 'dotenv/config';
import { Client } from '@hubspot/api-client';

const hubspotClient = new Client({
  accessToken: process.env.HUBSPOT_PRIVATE_APP_TOKEN
});

async function testConnection() {
  try {
    const response = await hubspotClient.crm.contacts.basicApi.getPage(5);
    console.log("✅ Connected to HubSpot Sandbox");
    console.log("Sample Contact IDs:");
    response.results.forEach(c => console.log(c.id));
  } catch (err) {
    console.error("❌ Connection Failed");
    console.error(err.response?.body || err.message);
  }
}

testConnection();
import "dotenv/config";
import fs from "fs";
import XLSX from "xlsx";
import { Client } from "@hubspot/api-client";
import {
  reduceGroup,
  normalizeEmail,
  validEmail,
  getEmailDomain,
  normalizeString,
  similarNames
} from "./sharedRules.js";

const LAST_RUN_FILE = "./lastRun.json";

function getLastRun() {
  try {
    if (!fs.existsSync(LAST_RUN_FILE)) return 0;
    const data = JSON.parse(fs.readFileSync(LAST_RUN_FILE, "utf-8"));
    return data.lastRun || 0;
  } catch {
    return 0;
  }
}

function saveLastRun(ts) {
  fs.writeFileSync(LAST_RUN_FILE, JSON.stringify({ lastRun: ts }));
}

async function fetchRecentContacts(lastRun) {
  const results = [];
  let after = undefined;

  while (true) {
    const resp = await hubspotSafeCall(() =>
      hubspot.crm.contacts.searchApi.doSearch({
        limit: 100,
        after,
        properties: CONTACT_PROPS,
        filterGroups: [
          {
            filters: [
              {
                propertyName: "lastmodifieddate",
                operator: "GT",
                value: lastRun
              }
            ]
          }
        ]
      })
    );

    results.push(...resp.results);

    if (!resp.paging?.next?.after) break;
    after = resp.paging.next.after;
  }

  return results;
}

async function hubspotSafeCall(fn, attempt = 1) {
  try {
    return await fn();
  } catch (err) {
    const msg = err?.message || "";
    const code = err?.code;

    if (
      attempt < 5 &&
      (
        msg.includes("ECONNRESET") ||
        msg.includes("socket hang up") ||
        msg.includes("ETIMEDOUT") ||
        code === "ECONNRESET" ||
        code === "ENOTFOUND" ||
        code === "ETIMEDOUT" ||
        code === 429 ||
        code === 500 ||
        code === 502 ||
        code === 503 ||
        code === 504 ||
        msg.includes("ENOTFOUND") ||
        msg.includes("HTTP-Code: 429") ||
        msg.includes("HTTP-Code: 500") ||
        msg.includes("HTTP-Code: 502") ||
        msg.includes("HTTP-Code: 503") ||
        msg.includes("HTTP-Code: 504")
      )
    ) {
      console.log(`Retry HubSpot request (attempt ${attempt})`);
      await new Promise(r => setTimeout(r, 1000 * attempt));
      return hubspotSafeCall(fn, attempt + 1);
    }

    throw err;
  }
}

async function withRetry(fn, label, attempt = 1) {
  try {
    return await fn();
  } catch (err) {
    const code = err?.code;
    const status = err?.body?.status || "";
    const category = err?.body?.category || "";
    const msg = err?.message || "";

    const retryable =
      code === "ECONNRESET" ||
      msg.includes("ECONNRESET") ||
      msg.includes("socket hang up") ||
      msg.includes("ETIMEDOUT") ||
      category === "INTERNAL_ERROR" ||
      msg.includes("HTTP-Code: 500");

    if (retryable && attempt < 5) {
      console.log(`Retrying ${label} (attempt ${attempt})`);
      await new Promise(r => setTimeout(r, 1000 * attempt));
      return withRetry(fn, label, attempt + 1);
    }

    throw err;
  }
}

async function runWithConcurrency(tasks, limit = 3) {
  const results = [];
  let i = 0;

  async function worker() {
    while (i < tasks.length) {
      const current = i++;
      results[current] = await tasks[current]();
    }
  }

  const workers = Array.from({ length: limit }, worker);
  await Promise.all(workers);

  return results;
}

const hubspot = new Client({
  accessToken: process.env.HUBSPOT_PRIVATE_APP_TOKEN
});

const EXECUTE_MERGE =
  String(process.env.EXECUTE_MERGE || "false").toLowerCase() === "true";

const CONTACT_PROPS = [
  "firstname",
  "lastname",
  "email",
  "work_email",
  "phone",
  "mobilephone",
  "annualrevenue",
  "contact_tier",
  "hs_sequences_is_enrolled",
  "lifecyclestage",
  "hubspot_owner_id",
  "bdr_partner",
  "notes_last_updated",
  "company",
  "linkedin_url"
];

async function fetchAllContacts() {
  const results = [];
  let after = undefined;

  while (true) {
    const resp = await hubspotSafeCall(() =>
      hubspot.crm.contacts.basicApi.getPage(100, after, CONTACT_PROPS)
    );

    results.push(...resp.results);

    if (!resp.paging?.next?.after) break;
    after = resp.paging.next.after;
  }

  return results;
}

async function fetchCompanyContext(contactId) {
  try {
    const assoc = await hubspotSafeCall(() =>
      hubspot.crm.contacts.associationsApi.getAll(contactId, "companies")
    );

    const companyIds = (assoc.results || []).map(r => r.id).filter(Boolean);

    if (!companyIds.length) {
      return {
        has_known_company: false,
        company_phone: "",
        company_domain: ""
      };
    }

    const companyId = companyIds[0];

    const company = await hubspotSafeCall(() =>
      hubspot.crm.companies.basicApi.getById(companyId, ["phone", "domain"])
    );

    return {
      has_known_company: true,
      company_phone: company?.properties?.phone || "",
      company_domain: company?.properties?.domain || ""
    };
  } catch {
    return {
      has_known_company: false,
      company_phone: "",
      company_domain: ""
    };
  }
}

async function toRuleContact(hsContact) {
  const props = hsContact.properties || {};
  const ctx = await fetchCompanyContext(hsContact.id);

  return {
    id: hsContact.id,
    email: props.email || "",
    work_email: props.work_email || "",
    phone: props.phone || "",
    mobilephone: props.mobilephone || "",
    annualrevenue: props.annualrevenue || "",
    contact_tier: props.contact_tier || "",
    hs_sequences_is_enrolled: props.hs_sequences_is_enrolled || "",
    lifecyclestage: props.lifecyclestage || "",
    hubspot_owner_id: props.hubspot_owner_id || "",
    bdr_partner: props.bdr_partner || "",
    notes_last_updated: props.notes_last_updated || "",
    has_known_company: ctx.has_known_company,
    company_phone: ctx.company_phone,
    firstname: props.firstname || "",
    lastname: props.lastname || "",
    domain: getEmailDomain(props.email || props.work_email),
    company_domain: ctx.company_domain || "",
    linkedin: props.linkedin || props.linkedin_url || "",
    company: props.company || ""
  };
}

function buildIdentityKeys(contact) {
  const keys = [];

  const email = normalizeEmail(contact.email);
  const rawWork = contact.work_email || "";
  const work = validEmail(rawWork) ? normalizeEmail(rawWork) : "";

  if (email) keys.push(`EMAIL_${email}`);
  if (work) keys.push(`WORK_${work}`);

  const domain = contact.domain || contact.company_domain;

  if (domain && contact.firstname && contact.lastname) {
    const fname = normalizeString(contact.firstname);
    const lname = normalizeString(contact.lastname);
    keys.push(`FUZZY_${fname}_${lname}_${domain}`);
  }

  if (!email && !work && contact.firstname && contact.lastname) {
    const fname = normalizeString(contact.firstname);
    const lname = normalizeString(contact.lastname);

    if (domain) {
      keys.push(`NAME_DOMAIN_${fname}_${lname}_${domain}`);
    } else {
      keys.push(`NAME_ONLY_${fname}_${lname}`);
    }
  }

  if (contact.firstname && contact.lastname && contact.company_domain) {
    const fname = normalizeString(contact.firstname);
    const lname = normalizeString(contact.lastname);
    const comp = normalizeString(contact.company_domain);
    keys.push(`NAME_COMPANY_${fname}_${lname}_${comp}`);
  }

  if (contact.lastname && contact.domain) {
    const lname = normalizeString(contact.lastname);
    const domainVal = normalizeString(contact.domain);
    keys.push(`LASTNAME_DOMAIN_${lname}_${domainVal}`);
  }

  if (contact.firstname && contact.lastname && contact.company) {
    const fname = normalizeString(contact.firstname);
    const lname = normalizeString(contact.lastname);
    const company = normalizeString(contact.company);
    keys.push(`NAME_COMPANY_${fname}_${lname}_${company}`);
  }

  return keys;
}

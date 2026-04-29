import "dotenv/config";
import fs from "fs";
import XLSX from "xlsx";
import { Client } from "@hubspot/api-client";
import { reduceGroup, normalizeEmail, validEmail, getEmailDomain, normalizeString, similarNames } from "./sharedRules.js";

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

    if (
      attempt < 5 &&
      (
        msg.includes("ECONNRESET") ||
        msg.includes("socket hang up") ||
        msg.includes("ETIMEDOUT") ||
        err.code === "ECONNRESET"
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

const EXECUTE_MERGE = String(process.env.EXECUTE_MERGE || "false").toLowerCase() === "true";
const mode = process.argv[2] || "full";

/**
 * TEST MODE:
 * Only groups containing one of these contact IDs will be processed.
 * Leave empty [] for normal full run later.
 */
const TEST_CONTACT_IDS = new Set([
  "8268777541",
  "8270065971",
  "150847773567",
  "150847462325",
  "253205",
  "255019",
  "193429512323",
  "181480388499",
  "8271445945",
  "193416275459"
]);

const TEST_MODE = TEST_CONTACT_IDS.size > 0;

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

// async function fetchAllContacts() {
//   const results = [];
//   let after = undefined;

//   while (true) {
//     const resp = await hubspotSafeCall(() =>
//       hubspot.crm.contacts.basicApi.getPage(
//         100,
//         after,
//         CONTACT_PROPS
//       )
//     );

//     results.push(...resp.results);

//     if (!resp.paging?.next?.after) break;
//     after = resp.paging.next.after;
//   }

//   return results;
// }


async function fetchOnlyTestContacts() {
  const results = [];

  for (const id of TEST_CONTACT_IDS) {
    try {
      const resp = await hubspot.crm.contacts.basicApi.getById(
        id,
        CONTACT_PROPS
      );

      results.push(resp);
    } catch (err) {
      console.error(`Failed to fetch contact ${id}`, err?.message);
    }
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

  // 🔹 NEW: Strong fallback grouping (name + company)
if (contact.firstname && contact.lastname && contact.company_domain) {
  const fname = normalizeString(contact.firstname);
  const lname = normalizeString(contact.lastname);
  const comp = normalizeString(contact.company_domain);

  keys.push(`NAME_COMPANY_${fname}_${lname}_${comp}`);
}

// 🔹 NEW: Domain + last name grouping (handles Dimitriy vs Dmitriy)
if (contact.lastname && contact.domain) {
  const lname = normalizeString(contact.lastname);
  const domain = normalizeString(contact.domain);

  keys.push(`LASTNAME_DOMAIN_${lname}_${domain}`);
}

// Strong fallback grouping: same first + last + company name
if (contact.firstname && contact.lastname && contact.company) {
  const fname = normalizeString(contact.firstname);
  const lname = normalizeString(contact.lastname);
  const company = normalizeString(contact.company);

  keys.push(`NAME_COMPANY_${fname}_${lname}_${company}`);
}

  return keys;
}

function pickMasterId(contacts) {
  const withEmail = contacts.find(c => c.email && c.email.trim() !== "");
  if (withEmail) return withEmail.id;

  const withWork = contacts.find(c => c.work_email && c.work_email.trim() !== "");
  if (withWork) return withWork.id;

  return contacts[0]?.id || "";
}

async function updateMaster(masterId, merged) {
  const props = {};

  if (merged.work_email) props.work_email = merged.work_email;
  if (merged.phone) props.phone = merged.phone;
  if (merged.mobilephone) props.mobilephone = merged.mobilephone;
  if (merged.annualrevenue) props.annualrevenue = merged.annualrevenue;
  if (merged.lifecyclestage) props.lifecyclestage = merged.lifecyclestage;
  if (merged.hubspot_owner_id) props.hubspot_owner_id = merged.hubspot_owner_id;
  if (merged.bdr_partner) props.bdr_partner = merged.bdr_partner;

  if (Object.keys(props).length === 0) return;

  await withRetry(
    () =>
      hubspot.crm.contacts.basicApi.update(masterId, {
        properties: props
      }),
    `update master ${masterId}`
  );
}

async function mergeIntoMaster(masterId, duplicateId) {
  await withRetry(
    () =>
      hubspot.apiRequest({
        method: "POST",
        path: "/crm/v3/objects/contacts/merge",
        body: {
          primaryObjectId: String(masterId),
          objectIdToMerge: String(duplicateId)
        }
      }),
    `merge ${duplicateId} -> ${masterId}`
  );
}

// function isFuzzyMatchSafe(c1, c2) {
//   const domain1 = getEmailDomain(c1.email || c1.work_email);
//   const domain2 = getEmailDomain(c2.email || c2.work_email);

//   if (domain1 && domain2) {
//     if (domain1 !== domain2) return false;
//   }

//   return similarNames(
//     `${c1.firstname} ${c1.lastname}`,
//     `${c2.firstname} ${c2.lastname}`
//   );
// }


function normalizePhoneValue(p) {
  return String(p || "").replace(/\D/g, "");
}

function normalizeLinkedIn(v) {
  return String(v || "")
    .toLowerCase()
    .trim()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/$/, "");
}

function isFuzzyMatchSafe(c1, c2) {
  const domain1 = getEmailDomain(c1.email || c1.work_email);
  const domain2 = getEmailDomain(c2.email || c2.work_email);

  const fname1 = normalizeString(c1.firstname);
  const fname2 = normalizeString(c2.firstname);
  const lname1 = normalizeString(c1.lastname);
  const lname2 = normalizeString(c2.lastname);

  const sameFirstName = fname1 && fname2 && fname1 === fname2;
  const sameLastName = lname1 && lname2 && lname1 === lname2;

  const sameCompany =
    normalizeString(c1.company || "") &&
    normalizeString(c1.company || "") === normalizeString(c2.company || "");

  const phone1 =
    normalizePhoneValue(c1.phone) ||
    normalizePhoneValue(c1.mobilephone) ||
    normalizePhoneValue(c1.company_phone);

  const phone2 =
    normalizePhoneValue(c2.phone) ||
    normalizePhoneValue(c2.mobilephone) ||
    normalizePhoneValue(c2.company_phone);

  const phoneMatch = phone1 && phone2 && phone1 === phone2;

  const linkedin1 = normalizeLinkedIn(c1.linkedin);
  const linkedin2 = normalizeLinkedIn(c2.linkedin);

  const linkedinMatch = linkedin1 && linkedin2 && linkedin1 === linkedin2;

  // Existing rule
  if (domain1 && domain2 && domain1 === domain2) {
    return similarNames(
      `${c1.firstname} ${c1.lastname}`,
      `${c2.firstname} ${c2.lastname}`
    );
  }

  // New safe rule: same exact name + same company + phone/linkedin
  if (sameFirstName && sameLastName && sameCompany && (phoneMatch || linkedinMatch)) {
    return true;
  }

  // New safe rule: name variation + same domain + same last name + phone
  if (domain1 && domain2 && domain1 === domain2 && sameLastName && phoneMatch) {
    return true;
  }

  return false;
}

async function main() {
  let pass = 1;

  while (true) {
    console.log(`\n============================`);
    console.log(`PASS ${pass}`);
    console.log(`============================\n`);

    let rawContacts = [];
    let recentRaw = [];

    if (mode === "incremental") {
      const lastRun = getLastRun();
      console.log(`Last run timestamp: ${lastRun}`);

      recentRaw = await fetchRecentContacts(lastRun);
      console.log(`Fetched ${recentRaw.length} recent contacts`);

      if (recentRaw.length === 0) {
        console.log("No new/updated contacts. Exiting.");
        return;
      }

      //rawContacts = await fetchAllContacts();
      if (TEST_MODE) {
        rawContacts = await fetchOnlyTestContacts();
        console.log(`Fetched ${rawContacts.length} TEST contacts`);
        } else {
        rawContacts = await fetchAllContacts();
        console.log(`Fetched ${rawContacts.length} contacts`);
        }
      console.log(`Fetched ${rawContacts.length} total contacts`);
    } else {
      //rawContacts = await fetchAllContacts();

      if (TEST_MODE) {
        rawContacts = await fetchOnlyTestContacts();
        console.log(`Fetched ${rawContacts.length} TEST contacts`);
        } else {
        rawContacts = await fetchAllContacts();
        console.log(`Fetched ${rawContacts.length} contacts`);
        }
      console.log(`Fetched ${rawContacts.length} contacts`);
    }

    const contacts = [];
    for (const hsContact of rawContacts) {
      contacts.push(await toRuleContact(hsContact));
    }

    const grouped = new Map();

    for (const c of contacts) {
      const keys = buildIdentityKeys(c);

      for (const key of keys) {
        if (!grouped.has(key)) grouped.set(key, []);
        grouped.get(key).push(c);
      }
    }

    let hasMerges = false;

    const mergedPreview = [];
    const auditLog = [];
    const excluded = [];
    const mergeExecution = [];
    const processed = new Set();

    const recentIds = new Set((recentRaw || []).map(c => String(c.id)));

    for (const [groupKey, groupContacts] of grouped.entries()) {
      const activeContacts = groupContacts.filter(c => !processed.has(c.id));

      if (activeContacts.length < 2) continue;
      if (groupContacts.length < 2) continue;

      // TEST MODE: process only groups containing at least one whitelisted ID
      if (TEST_MODE) {
        const hasTestContact = activeContacts.some(c => TEST_CONTACT_IDS.has(String(c.id)));
        if (!hasTestContact) continue;
      }

      if (mode === "incremental") {
        const hasRecent = activeContacts.some(c => recentIds.has(String(c.id)));
        if (!hasRecent) continue;
      }

      if (groupKey.startsWith("FUZZY_")) {
        const safe = activeContacts.every((c, i, arr) =>
          i === 0 || isFuzzyMatchSafe(arr[0], c)
        );

        if (!safe) {
          if (TEST_MODE && activeContacts.some(c => TEST_CONTACT_IDS.has(String(c.id)))) {
            excluded.push({
              GROUP_KEY: groupKey,
              CONTACT_IDS: activeContacts.map(c => c.id).join(", "),
              REASON: "unsafe_fuzzy_match_skipped"
            });
          }
          continue;
        }
      }

      const result = reduceGroup(activeContacts);

      if (!result.mergeAllowed || !result.merged) {
        excluded.push({
          GROUP_KEY: groupKey,
          CONTACT_IDS: groupContacts.map(c => c.id).join(", "),
          REASON: result.excludedReason || "group_not_merged"
        });
        continue;
      }

      hasMerges = true;

      const masterId = pickMasterId(activeContacts);
      const duplicateIds = activeContacts
        .map(c => String(c.id))
        .filter(id => id !== String(masterId));

      mergedPreview.push({
        GROUP_KEY: groupKey,
        MASTER_ID: masterId,
        EMAIL: result.merged.email,
        WORK_EMAIL: result.merged.work_email,
        PHONE: result.merged.phone,
        MOBILEPHONE: result.merged.mobilephone,
        LIFECYCLESTAGE: result.merged.lifecyclestage,
        NOTES_LAST_UPDATED: result.merged.notes_last_updated,
        HUBSPOT_OWNER_ID: result.merged.hubspot_owner_id,
        BDR_PARTNER: result.merged.bdr_partner,
        CONTACT_TIER: result.merged.contact_tier,
        ANNUALREVENUE: result.merged.annualrevenue,
        MERGING_IDS: duplicateIds.join(", ")
      });

      for (const a of result.audit) {
        auditLog.push({
          GROUP_KEY: groupKey,
          MASTER_ID: masterId,
          FIELD: a.field,
          VALUE: a.value,
          RULE: a.rule
        });
      }

      if (EXECUTE_MERGE) {
        try {
          const tasks = duplicateIds.map(dupId => async () => {
            await mergeIntoMaster(masterId, dupId);

            mergeExecution.push({
              GROUP_KEY: groupKey,
              MASTER_ID: masterId,
              MERGED_ID: dupId,
              STATUS: "merged"
            });

            await new Promise(r => setTimeout(r, 200));
          });

          await runWithConcurrency(tasks, 3);
          await updateMaster(masterId, result.merged);
        } catch (err) {
          mergeExecution.push({
            GROUP_KEY: groupKey,
            MASTER_ID: masterId,
            MERGED_ID: duplicateIds.join(", "),
            STATUS: "failed",
            ERROR: err?.body?.message || err?.message || "unknown error"
          });

          console.error(`Failed group ${groupKey}:`, err?.body?.message || err?.message);
        }
      } else {
        for (const dupId of duplicateIds) {
          mergeExecution.push({
            GROUP_KEY: groupKey,
            MASTER_ID: masterId,
            MERGED_ID: dupId,
            STATUS: "dry_run"
          });
        }
      }
    }

    const wb = XLSX.utils.book_new();

    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(mergedPreview), "merged_preview");
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(auditLog), "audit_log");
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(excluded), "excluded");
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(mergeExecution), "merge_execution");

    XLSX.writeFile(wb, `hubspot_merge_review_pass_${pass}.xlsx`);
    console.log(`Generated hubspot_merge_review_pass_${pass}.xlsx`);

    if (mode === "incremental" && hasMerges) {
      saveLastRun(Date.now());
    }

    if (!hasMerges) {
      console.log("\n✅ No more duplicates found. Stopping.");
      break;
    }

    pass++;
  }
}

main().catch(err => {
  console.error(err?.response?.body || err);
  process.exit(1);
});
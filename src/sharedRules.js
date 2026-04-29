// src/sharedRules.js

export function isBlank(v) {
  return v === null || v === undefined || String(v).trim() === "";
}

export function validEmail(v) {
  if (isBlank(v)) return false;
  return String(v).includes("@");
}

// export function normalizeEmail(v) {
//   if (!validEmail(v)) return "";
//   return String(v).trim().toLowerCase();
// }

export function normalizeEmail(v) {
  if (!validEmail(v)) return "";

  let email = String(v).trim().toLowerCase();

  const [local, domain] = email.split("@");

  if (!domain) return email;

  // 🔥 REMOVE + PART
  const cleanLocal = local.split("+")[0];

  return `${cleanLocal}@${domain}`;
}

export function normalizePhone(v) {
  if (isBlank(v)) return "";
  return String(v).replace(/\D+/g, "");
}

export function toNumber(v) {
  if (isBlank(v)) return 0;
  const n = Number(String(v).replace(/[^0-9.-]/g, ""));
  return Number.isNaN(n) ? 0 : n;
}

export function toBool(v) {
  if (typeof v === "boolean") return v;
  return String(v).trim().toUpperCase() === "TRUE";
}

export function latestTimestamp(t1, t2) {
  return Math.max(Number(t1 || 0), Number(t2 || 0));
}

export function chooseMostRecentValue(v1, v2, t1, t2) {
  const aBlank = isBlank(v1);
  const bBlank = isBlank(v2);

  if (!aBlank && bBlank) return { value: v1, source: 1 };
  if (aBlank && !bBlank) return { value: v2, source: 2 };
  if (aBlank && bBlank) return { value: "", source: 0 };

  return Number(t1 || 0) >= Number(t2 || 0)
    ? { value: v1, source: 1 }
    : { value: v2, source: 2 };
}

export const lifecycleRank = {
  unqualified: 0,
  subscriber: 1,
  lead: 2,
  marketingqualifiedlead: 3,
  salesqualifiedlead: 4,
  salesaccepted: 5,
  ignored: 6,
  opportunity: 7,
  customer: 8,
  other: -1
};

export function normalizeLifecycle(v) {
  if (isBlank(v)) return "";
  return String(v).trim().toLowerCase().replace(/\s+/g, "");
}

export function resolveLifecycleStage(v1, v2) {
  const a = normalizeLifecycle(v1);
  const b = normalizeLifecycle(v2);

  if (!a && !b) return { value: "", rule: "both_unknown" };
  if (a && !b) return { value: v1, rule: "value1_known" };
  if (!a && b) return { value: v2, rule: "value2_known" };
  if (a === b) return { value: v1, rule: "same" };

  const r1 = lifecycleRank[a] ?? -1;
  const r2 = lifecycleRank[b] ?? -1;

  return r1 >= r2
    ? { value: v1, rule: "higher_lifecycle_1" }
    : { value: v2, rule: "higher_lifecycle_2" };
}

// export const tierRank = {
//   "1": 3, // Tier 1 highest
//   "2": 2,
//   "3": 1
// };

export const tierRank = {
  "tier 1": 3,
  "tier 2": 2,
  "tier 3": 1
};

export function resolveContactTier(v1, v2) {
//   const a = isBlank(v1) ? "" : String(v1).trim();
//   const b = isBlank(v2) ? "" : String(v2).trim();

  const a = isBlank(v1) ? "" : String(v1).trim().toLowerCase();
  const b = isBlank(v2) ? "" : String(v2).trim().toLowerCase();

  if (!a && !b) return { value: "", rule: "both_unknown" };
  if (a && !b) return { value: a, rule: "value1_known" };
  if (!a && b) return { value: b, rule: "value2_known" };
  if (a === b) return { value: a, rule: "same" };

  const r1 = tierRank[a] ?? 0;
  const r2 = tierRank[b] ?? 0;

  return r1 >= r2
    ? { value: a, rule: "higher_tier_1" }
    : { value: b, rule: "higher_tier_2" };
}

export function annualRevenueRank(v) {
  const s = String(v || "").trim();
  if (!s) return 0;

  const normalized = s
    .replace(/\$/g, "")
    .replace(/,/g, "")
    .toUpperCase();

  if (normalized.includes("B")) {
    const m = normalized.match(/([0-9.]+)/);
    return m ? Number(m[1]) * 1000 : 0;
  }

  if (normalized.includes("M")) {
    const nums = normalized.match(/([0-9.]+)/g);
    if (!nums) return 0;
    return Math.max(...nums.map(Number));
  }

  return toNumber(normalized);
}

export function resolveAnnualRevenue(v1, v2) {
  const aBlank = isBlank(v1);
  const bBlank = isBlank(v2);

  if (!aBlank && bBlank) return { value: v1, rule: "value1_known" };
  if (aBlank && !bBlank) return { value: v2, rule: "value2_known" };
  if (aBlank && bBlank) return { value: "", rule: "both_unknown" };
  if (String(v1).trim() === String(v2).trim()) return { value: v1, rule: "same" };

  return annualRevenueRank(v1) >= annualRevenueRank(v2)
    ? { value: v1, rule: "higher_revenue_1" }
    : { value: v2, rule: "higher_revenue_2" };
}

export function resolveLastActivity(v1, v2) {
  const a = Number(v1 || 0);
  const b = Number(v2 || 0);

  if (!a && !b) return { value: "", rule: "both_unknown" };
  if (a >= b) return { value: a || v1, rule: "most_recent_1" };
  return { value: b || v2, rule: "most_recent_2" };
}

export function resolveEmail(contact1, contact2) {
  const e1 = normalizeEmail(contact1.email);
  const e2 = normalizeEmail(contact2.email);
  const w1 = normalizeEmail(contact1.work_email);
  const w2 = normalizeEmail(contact2.work_email);

  const t1 = Number(contact1.notes_last_updated || 0);
  const t2 = Number(contact2.notes_last_updated || 0);

  if (e1 && e2 && e1 === e2) {
    return { value: e1, mergeAllowed: true, rule: "same_email" };
  }

  if (e1 && !e2) {
    return { value: e1, mergeAllowed: true, rule: "email1_known" };
  }

  if (!e1 && e2) {
    return { value: e2, mergeAllowed: true, rule: "email2_known" };
  }

  if (e1 && e2 && e1 !== e2) {
    if (t1 >= t2) {
      return { value: e1, mergeAllowed: true, rule: "different_email_most_recent_1" };
    }
    return { value: e2, mergeAllowed: true, rule: "different_email_most_recent_2" };
  }

//   if (w1) {
//     return { value: w1, mergeAllowed: true, rule: "fallback_work_email_1" };
//   }

//   if (w2) {
//     return { value: w2, mergeAllowed: true, rule: "fallback_work_email_2" };
//   }

//   return {
//     value: "",
//     mergeAllowed: false,
//     rule: "no_email_or_work_email_do_not_merge"
//   };

if (w1) {
  return { value: w1, mergeAllowed: true, rule: "fallback_work_email_1" };
}

if (w2) {
  return { value: w2, mergeAllowed: true, rule: "fallback_work_email_2" };
}

// 🔥 NEW: allow merge if BOTH emails missing (Instruction #3)
return {
  value: "",
  mergeAllowed: true,
  rule: "no_email_but_allowed_via_secondary_match"
};
}

export function resolveWorkEmail(contact1, contact2, resolvedEmail) {
  const w1 = normalizeEmail(contact1.work_email);
  const w2 = normalizeEmail(contact2.work_email);

  if (w1 && w2 && w1 === w2) {
    return { value: w1, rule: "same_work_email" };
  }

  if (w1 && !w2) {
    return { value: w1, rule: "work_email1_known" };
  }

  if (!w1 && w2) {
    return { value: w2, rule: "work_email2_known" };
  }

  if (!w1 && !w2 && resolvedEmail) {
    return { value: resolvedEmail, rule: "copy_known_email_to_work_email" };
  }

  return { value: "", rule: "both_unknown" };
}

export function resolveSequenceExclusion(contact1, contact2) {
  const s1 = toBool(contact1.hs_sequences_is_enrolled);
  const s2 = toBool(contact2.hs_sequences_is_enrolled);

  if (s1 || s2) {
    return { excluded: true, rule: "currently_in_sequence_true_do_not_merge" };
  }

  return { excluded: false, rule: "allowed" };
}

export function resolveContactOwner(contact1, contact2, resolvedBdrPartner) {
  const o1 = contact1.hubspot_owner_id;
  const o2 = contact2.hubspot_owner_id;

  if (!isBlank(o1) && !isBlank(o2) && String(o1).trim() === String(o2).trim()) {
    return { value: o1, rule: "same_owner" };
  }

  if (!isBlank(o1) && isBlank(o2)) {
    return { value: o1, rule: "owner1_known" };
  }

  if (isBlank(o1) && !isBlank(o2)) {
    return { value: o2, rule: "owner2_known" };
  }

  if (isBlank(o1) && isBlank(o2) && !isBlank(resolvedBdrPartner)) {
    return { value: resolvedBdrPartner, rule: "fallback_to_bdr_partner" };
  }

//   return { value: "", rule: "both_owner_unknown" };

    const t1 = Number(contact1.notes_last_updated || 0);
    const t2 = Number(contact2.notes_last_updated || 0);

    return t1 >= t2
    ? { value: contact1.hubspot_owner_id || "", rule: "fallback_most_recent_1" }
    : { value: contact2.hubspot_owner_id || "", rule: "fallback_most_recent_2" };
}

export function resolveBdrPartner(contact1, contact2) {
  const b1 = contact1.bdr_partner;
  const b2 = contact2.bdr_partner;

  if (!isBlank(b1) && !isBlank(b2) && String(b1).trim() === String(b2).trim()) {
    return { value: b1, rule: "same_bdr" };
  }

  if (!isBlank(b1) && isBlank(b2)) {
    return { value: b1, rule: "bdr1_known" };
  }

  if (isBlank(b1) && !isBlank(b2)) {
    return { value: b2, rule: "bdr2_known" };
  }

  const c1Known = !!contact1.has_known_company;
  const c2Known = !!contact2.has_known_company;

  if (!isBlank(b1) && !isBlank(b2) && String(b1).trim() !== String(b2).trim()) {
    if (c1Known && !c2Known) {
      return { value: b1, rule: "different_bdr_contact1_has_known_company" };
    }
    if (!c1Known && c2Known) {
      return { value: b2, rule: "different_bdr_contact2_has_known_company" };
    }

    const t1 = Number(contact1.notes_last_updated || 0);
    const t2 = Number(contact2.notes_last_updated || 0);

    return t1 >= t2
      ? { value: b1, rule: "different_bdr_most_recent_1" }
      : { value: b2, rule: "different_bdr_most_recent_2" };
  }

  return { value: "", rule: "both_bdr_unknown" };
}

export function resolvePhone(contact1, contact2) {
  const p1 = normalizePhone(contact1.phone);
  const p2 = normalizePhone(contact2.phone);

  if (p1 && p2 && p1 === p2) {
    return { value: p1, rule: "same_phone" };
  }

  if (p1 && !p2) {
    return { value: p1, rule: "phone1_known" };
  }

  if (!p1 && p2) {
    return { value: p2, rule: "phone2_known" };
  }

  if (p1 && p2 && p1 !== p2) {
    const t1 = Number(contact1.notes_last_updated || 0);
    const t2 = Number(contact2.notes_last_updated || 0);

    return t1 >= t2
      ? { value: p1, rule: "different_phone_most_recent_1" }
      : { value: p2, rule: "different_phone_most_recent_2" };
  }

  const cp1 = normalizePhone(contact1.company_phone);
  const cp2 = normalizePhone(contact2.company_phone);

  if (cp1) return { value: cp1, rule: "fallback_company_phone_1" };
  if (cp2) return { value: cp2, rule: "fallback_company_phone_2" };

  return { value: "", rule: "both_phone_unknown" };
}

export function resolveMobilePhone(contact1, contact2) {
  const m1 = normalizePhone(contact1.mobilephone);
  const m2 = normalizePhone(contact2.mobilephone);

  if (m1 && m2 && m1 === m2) {
    return { value: m1, rule: "same_mobile" };
  }

  if (m1 && !m2) {
    return { value: m1, rule: "mobile1_known" };
  }

  if (!m1 && m2) {
    return { value: m2, rule: "mobile2_known" };
  }

  if (m1 && m2 && m1 !== m2) {
    const t1 = Number(contact1.notes_last_updated || 0);
    const t2 = Number(contact2.notes_last_updated || 0);

    return t1 >= t2
      ? { value: m1, rule: "different_mobile_most_recent_1" }
      : { value: m2, rule: "different_mobile_most_recent_2" };
  }

  return { value: "", rule: "both_mobile_unknown" };
}

export function buildMergedPair(contact1, contact2) {
  const seq = resolveSequenceExclusion(contact1, contact2);
  if (seq.excluded) {
    return {
      mergeAllowed: false,
      excludedReason: seq.rule,
      merged: null,
      audit: []
    };
  }

  const email = resolveEmail(contact1, contact2);
  if (!email.mergeAllowed) {
    return {
      mergeAllowed: false,
      excludedReason: email.rule,
      merged: null,
      audit: [
        { field: "email", value: "", rule: email.rule }
      ]
    };
  }

  const workEmail = resolveWorkEmail(contact1, contact2, email.value);
  const lifecycle = resolveLifecycleStage(contact1.lifecyclestage, contact2.lifecyclestage);
  const lastActivity = resolveLastActivity(contact1.notes_last_updated, contact2.notes_last_updated);
  const bdr = resolveBdrPartner(contact1, contact2);
  const owner = resolveContactOwner(contact1, contact2, bdr.value);
  const phone = resolvePhone(contact1, contact2);
  const mobile = resolveMobilePhone(contact1, contact2);
  const tier = resolveContactTier(contact1.contact_tier, contact2.contact_tier);
  const revenue = resolveAnnualRevenue(contact1.annualrevenue, contact2.annualrevenue);

  const merged = {
    id: contact1.id,
    email: email.value,
    work_email: workEmail.value,
    lifecyclestage: lifecycle.value,
    notes_last_updated: lastActivity.value,
    hs_sequences_is_enrolled: false,
    hubspot_owner_id: owner.value,
    bdr_partner: bdr.value,
    phone: phone.value,
    mobilephone: mobile.value,
    contact_tier: tier.value,
    annualrevenue: revenue.value
  };

  const audit = [
    { field: "email", value: email.value, rule: email.rule },
    { field: "work_email", value: workEmail.value, rule: workEmail.rule },
    { field: "lifecyclestage", value: lifecycle.value, rule: lifecycle.rule },
    { field: "notes_last_updated", value: lastActivity.value, rule: lastActivity.rule },
    { field: "hubspot_owner_id", value: owner.value, rule: owner.rule },
    { field: "bdr_partner", value: bdr.value, rule: bdr.rule },
    { field: "phone", value: phone.value, rule: phone.rule },
    { field: "mobilephone", value: mobile.value, rule: mobile.rule },
    { field: "contact_tier", value: tier.value, rule: tier.rule },
    { field: "annualrevenue", value: revenue.value, rule: revenue.rule }
  ];

  return {
    mergeAllowed: true,
    excludedReason: "",
    merged,
    audit
  };
}

export function reduceGroup(contacts) {
  if (!contacts.length) {
    return {
      mergeAllowed: false,
      excludedReason: "empty_group",
      merged: null,
      audit: []
    };
  }

  let current = { ...contacts[0] };
  let combinedAudit = [];

  for (let i = 1; i < contacts.length; i += 1) {
    const next = contacts[i];
    const result = buildMergedPair(current, next);

    if (!result.mergeAllowed) {
      combinedAudit.push({
        field: "_group_exclusion",
        value: "",
        rule: `${result.excludedReason} against contact ${next.id}`
      });
      continue;
    }

    current = {
      ...current,
      ...result.merged
    };

    combinedAudit = combinedAudit.concat(
      result.audit.map(a => ({
        ...a,
        rule: `${a.rule} (with contact ${next.id})`
      }))
    );
  }

  return {
    mergeAllowed: true,
    excludedReason: "",
    merged: current,
    audit: combinedAudit
  };
}


export function normalizeString(v) {
  if (!v) return "";
  return String(v)
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ");
}

export function getEmailDomain(email) {
  if (!email || !email.includes("@")) return "";
  return email.split("@")[1].toLowerCase();
}

export function similarNames(n1, n2) {
  n1 = normalizeString(n1);
  n2 = normalizeString(n2);

  if (!n1 || !n2) return false;

  // simple fuzzy: exact OR contains
  return n1 === n2 || n1.includes(n2) || n2.includes(n1);
}
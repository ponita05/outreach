#!/usr/bin/env python3
"""Prospect contacts using TinyFish Agent API streaming endpoint.

Reads companies from companies.txt and appends discovered contacts to contacts.csv.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import requests
from dotenv import load_dotenv


TINYFISH_RUN_SSE_URL = "https://agent.tinyfish.ai/v1/automation/run-sse"

CSV_COLUMNS = [
    "Contact #",
    "Outreach Status",
    "Contact Name",
    "Contact Role / Title",
    "Company / Organization",
    "Company Size",
    "LinkedIn URL",
    "Email",
    "Phone",
    "Source Notes",
]

PASS_DEFINITIONS: List[Tuple[str, str]] = [
    (
        "Procurement",
        "Find up to 30 people at [company] in procurement, purchasing, sourcing, or vendor management roles. "
        "Find their name, title, LinkedIn URL, email, and phone.",
    ),
    (
        "Operations",
        "Find up to 30 people at [company] in operations, COO, VP operations, director of operations, or "
        "operational management roles. Find their name, title, LinkedIn URL, email, and phone.",
    ),
    (
        "Supply Chain",
        "Find up to 30 people at [company] in supply chain, inventory management, logistics, warehouse, or "
        "materials management roles. Find their name, title, LinkedIn URL, email, and phone.",
    ),
    (
        "LinkedIn",
        "Search LinkedIn for people who currently work at [company] in procurement, operations, supply chain, "
        "or inventory roles. Find up to 30 people with their LinkedIn profile URLs, titles, and names.",
    ),
]

EXTRA_PASS_DEFINITIONS: List[Tuple[str, str]] = [
    (
        "Director-Level",
        "Find up to 30 people at [company] who are directors, senior directors, or heads of procurement, "
        "operations, logistics, inventory, or supply chain. Find their name, title, LinkedIn URL, email, and phone.",
    ),
    (
        "VP-Level",
        "Find up to 30 people at [company] who are VP, SVP, or executive leaders in procurement, operations, "
        "sourcing, supply chain, or logistics. Find their name, title, LinkedIn URL, email, and phone.",
    ),
    (
        "Logistics-Warehouse",
        "Find up to 30 people at [company] in warehouse operations, fulfillment, transportation, distribution, "
        "or logistics management roles. Find their name, title, LinkedIn URL, email, and phone.",
    ),
    (
        "Planning-Inventory",
        "Find up to 30 people at [company] in planning, demand planning, inventory control, materials planning, "
        "or replenishment roles. Find their name, title, LinkedIn URL, email, and phone.",
    ),
]


def load_companies(companies_path: Path) -> List[Tuple[str, str]]:
    companies: List[Tuple[str, str]] = []
    if not companies_path.exists():
        raise FileNotFoundError(f"Missing companies file: {companies_path}")

    with companies_path.open("r", encoding="utf-8") as file:
        for line_number, raw_line in enumerate(file, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            parts = [p.strip() for p in line.split(",", 1)]
            if len(parts) != 2 or not parts[0] or not parts[1]:
                print(
                    f"[WARN] Skipping malformed line {line_number} in companies.txt: {raw_line.rstrip()}",
                    file=sys.stderr,
                )
                continue
            companies.append((parts[0], parts[1]))

    return companies


def ensure_csv(csv_path: Path) -> None:
    if csv_path.exists() and csv_path.stat().st_size > 0:
        return
    with csv_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_COLUMNS)
        writer.writeheader()


def get_next_contact_number(csv_path: Path) -> int:
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return 1

    highest = 0
    with csv_path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                highest = max(highest, int(str(row.get("Contact #", "")).strip()))
            except ValueError:
                continue
    return highest + 1


def build_goal(company_name: str, website: str, pass_goal_template: str) -> str:
    pass_goal = pass_goal_template.replace("[company]", company_name)
    return (
        f"Target company: {company_name}. Website: {website}. "
        f"{pass_goal} "
        "Browse the company website and LinkedIn presence/pages/profiles where needed. "
        "Return the output as JSON with a top-level key 'contacts' containing an array of objects with these exact keys: "
        "Contact Name, Contact Role / Title, Company / Organization, Company Size, LinkedIn URL, Email, Phone, Source Notes. "
        "If a field is unknown, leave it as an empty string."
    )


def normalize_url(website: str) -> str:
    value = website.strip()
    if not value:
        return value
    if value.startswith("http://") or value.startswith("https://"):
        return value
    return f"https://{value}"


def build_payload(company_name: str, website: str, pass_goal_template: str) -> Dict[str, Any]:
    normalized_website = normalize_url(website)
    return {
        "url": normalized_website,
        "goal": build_goal(company_name, normalized_website, pass_goal_template),
        "metadata": {
            "project": "atrope-outreach",
            "company": company_name,
            "website": normalized_website,
        },
        "output": {
            "type": "json",
            "schema": {
                "type": "object",
                "properties": {
                    "contacts": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "Contact Name": {"type": "string"},
                                "Contact Role / Title": {"type": "string"},
                                "Company / Organization": {"type": "string"},
                                "Company Size": {"type": "string"},
                                "LinkedIn URL": {"type": "string"},
                                "Email": {"type": "string"},
                                "Phone": {"type": "string"},
                                "Source Notes": {"type": "string"},
                            },
                            "required": [
                                "Contact Name",
                                "Contact Role / Title",
                                "Company / Organization",
                                "Company Size",
                                "LinkedIn URL",
                                "Email",
                                "Phone",
                                "Source Notes",
                            ],
                            "additionalProperties": True,
                        },
                    }
                },
                "required": ["contacts"],
                "additionalProperties": True,
            },
        },
    }


def _extract_text_payload(obj: Any) -> List[str]:
    texts: List[str] = []
    if isinstance(obj, str):
        texts.append(obj)
        return texts

    if isinstance(obj, dict):
        for key in (
            "message",
            "content",
            "text",
            "output",
            "result",
            "final_output",
            "response",
            "data",
            "purpose",
            "reason",
            "outcome",
            "observation",
            "status",
        ):
            if key in obj:
                texts.extend(_extract_text_payload(obj[key]))
        return texts

    if isinstance(obj, list):
        for item in obj:
            texts.extend(_extract_text_payload(item))
    return texts


def _looks_like_contact(item: Dict[str, Any]) -> bool:
    role = str(item.get("Contact Role / Title", "")).lower()
    name = str(item.get("Contact Name", "")).strip()
    if not name:
        return False
    keywords = [
        "procurement",
        "operations",
        "inventory",
        "supply chain",
        "logistics",
        "purchasing",
    ]
    return any(k in role for k in keywords) or bool(item.get("LinkedIn URL"))


def _normalize_contact(raw: Dict[str, Any], company_name: str) -> Dict[str, str]:
    mapped = {
        "Contact Name": str(raw.get("Contact Name", "") or "").strip(),
        "Contact Role / Title": str(raw.get("Contact Role / Title", "") or "").strip(),
        "Company / Organization": str(raw.get("Company / Organization", company_name) or company_name).strip(),
        "Company Size": str(raw.get("Company Size", "") or "").strip(),
        "LinkedIn URL": str(raw.get("LinkedIn URL", "") or "").strip(),
        "Email": str(raw.get("Email", "") or "").strip(),
        "Phone": str(raw.get("Phone", "") or "").strip(),
        "Source Notes": str(raw.get("Source Notes", "") or "").strip(),
    }
    return mapped


def parse_contacts_from_json(obj: Any, company_name: str) -> List[Dict[str, str]]:
    contacts: List[Dict[str, str]] = []

    if isinstance(obj, dict):
        if isinstance(obj.get("contacts"), list):
            for item in obj["contacts"]:
                if isinstance(item, dict):
                    contact = _normalize_contact(item, company_name)
                    if _looks_like_contact(contact):
                        contacts.append(contact)

        for value in obj.values():
            contacts.extend(parse_contacts_from_json(value, company_name))

    elif isinstance(obj, list):
        for item in obj:
            contacts.extend(parse_contacts_from_json(item, company_name))

    return contacts


def contact_dedupe_key(contact: Dict[str, str]) -> Tuple[str, str, str]:
    return (
        contact.get("Contact Name", "").strip().lower(),
        contact.get("Company / Organization", "").strip().lower(),
        contact.get("LinkedIn URL", "").strip().lower(),
    )


def contact_filled_field_count(contact: Dict[str, str]) -> int:
    fields = [
        "Contact Name",
        "Contact Role / Title",
        "Company / Organization",
        "Company Size",
        "LinkedIn URL",
        "Email",
        "Phone",
        "Source Notes",
    ]
    return sum(1 for field in fields if str(contact.get(field, "")).strip())


def dedupe_contacts(contacts: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
    best_by_key: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    for contact in contacts:
        key = contact_dedupe_key(contact)
        existing = best_by_key.get(key)
        if existing is None:
            best_by_key[key] = contact
            continue

        if contact_filled_field_count(contact) > contact_filled_field_count(existing):
            best_by_key[key] = contact

    return list(best_by_key.values())


def load_existing_contact_keys(csv_path: Path) -> set[Tuple[str, str, str]]:
    keys: set[Tuple[str, str, str]] = set()
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return keys

    with csv_path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            normalized = {
                "Contact Name": str(row.get("Contact Name", "") or "").strip(),
                "Company / Organization": str(row.get("Company / Organization", "") or "").strip(),
                "LinkedIn URL": str(row.get("LinkedIn URL", "") or "").strip(),
            }
            keys.add(contact_dedupe_key(normalized))
    return keys


def log_tinyfish_event(parsed: Dict[str, Any]) -> bool:
    event_type = str(parsed.get("type", "")).upper()

    if event_type == "STARTED":
        run_id = str(parsed.get("run_id", "")).strip()
        if run_id:
            print(f"  [TinyFish] Run started: {run_id}")
        else:
            print("  [TinyFish] Run started")
        return False

    if event_type == "STREAMING_URL":
        url = str(parsed.get("streaming_url", "")).strip()
        if url:
            print(f"  [TinyFish] Streaming URL: {url}")
        return False

    if event_type == "PROGRESS":
        purpose = str(parsed.get("purpose", "")).strip()
        if purpose:
            print(f"  [TinyFish:progress] {purpose}")
        return False

    if event_type == "COMPLETE":
        status = str(parsed.get("status", "")).strip()
        result = parsed.get("result")

        if status:
            print(f"  [TinyFish] COMPLETE status={status}")
        else:
            print("  [TinyFish] COMPLETE")

        if isinstance(result, dict):
            result_status = str(result.get("status", "")).strip()
            reason = str(result.get("reason", "")).strip()
            outcome = str(result.get("outcome", "")).strip()

            if result_status:
                print(f"  [TinyFish] Result status: {result_status}")
            if outcome:
                print(f"  [TinyFish] Outcome: {outcome}")
            if reason:
                print(f"  [TinyFish] Reason: {reason[:320]}")
        return True

    if event_type == "ERROR":
        err = str(parsed.get("error", "") or parsed.get("message", "")).strip()
        if err:
            print(f"  [TinyFish:error] {err}")
        else:
            print("  [TinyFish:error] Unknown error event")
        return False

    if event_type == "HEARTBEAT":
        return False

    return False


def mask_api_key(api_key: str) -> str:
    key = api_key.strip()
    if not key:
        return "<empty>"
    if len(key) <= 10:
        return f"{key[:2]}*** (len={len(key)})"
    return f"{key[:6]}...{key[-4:]} (len={len(key)})"


def run_tinyfish_for_company(
    api_key: str,
    company_name: str,
    website: str,
    pass_goal_template: str,
    timeout_seconds: int,
) -> List[Dict[str, str]]:
    payload = build_payload(company_name, website, pass_goal_template)

    auth_attempts: List[Tuple[str, Dict[str, str]]] = [
        (
            "Bearer",
            {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Accept": "text/event-stream",
            },
        ),
        (
            "x-api-key",
            {
                "x-api-key": api_key,
                "Content-Type": "application/json",
                "Accept": "text/event-stream",
            },
        ),
    ]

    last_http_error: requests.HTTPError | None = None

    for attempt_index, (auth_name, headers) in enumerate(auth_attempts, start=1):
        collected_json_objects: List[Any] = []
        collected_text_fragments: List[str] = []
        print(f"  [TinyFish] Auth attempt {attempt_index}/{len(auth_attempts)} using {auth_name}")

        try:
            with requests.post(
                TINYFISH_RUN_SSE_URL,
                headers=headers,
                json=payload,
                stream=True,
                timeout=timeout_seconds,
            ) as response:
                if response.status_code == 401 and attempt_index < len(auth_attempts):
                    preview = (response.text or "")[:300]
                    print(
                        f"  [TinyFish] Unauthorized with {auth_name}; retrying next auth mode."
                        + (f" Response: {preview}" if preview else "")
                    )
                    continue

                response.raise_for_status()

                print(f"  [TinyFish] Connected (HTTP {response.status_code}), streaming events...")
                is_complete = False
                for raw_line in response.iter_lines(decode_unicode=True):
                    if raw_line is None:
                        continue
                    line = raw_line.strip()
                    if not line:
                        continue

                    if line.startswith("event:"):
                        event_name = line.split(":", 1)[1].strip()
                        if event_name:
                            print(f"  [TinyFish:event] {event_name}")
                        continue

                    if not line.startswith("data:"):
                        continue

                    data = line.split(":", 1)[1].strip()
                    if not data:
                        continue

                    if data == "[DONE]":
                        print("  [TinyFish] Stream complete.")
                        break

                    try:
                        parsed = json.loads(data)
                        collected_json_objects.append(parsed)
                        if isinstance(parsed, dict):
                            if log_tinyfish_event(parsed):
                                is_complete = True

                        for text in _extract_text_payload(parsed):
                            text_clean = text.strip()
                            if text_clean:
                                collected_text_fragments.append(text_clean)
                                print(f"  [TinyFish] {text_clean[:220]}")
                    except json.JSONDecodeError:
                        collected_text_fragments.append(data)
                        print(f"  [TinyFish] {data[:220]}")

                    if is_complete:
                        print("  [TinyFish] Complete event received, closing stream.")
                        break

            contacts: List[Dict[str, str]] = []
            for obj in collected_json_objects:
                contacts.extend(parse_contacts_from_json(obj, company_name))

            if contacts:
                return dedupe_contacts(contacts)

            for text in collected_text_fragments:
                try:
                    maybe_json = json.loads(text)
                except json.JSONDecodeError:
                    continue
                contacts.extend(parse_contacts_from_json(maybe_json, company_name))

            return dedupe_contacts(contacts)

        except requests.HTTPError as exc:
            last_http_error = exc
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 401 and attempt_index < len(auth_attempts):
                print(f"  [TinyFish] Unauthorized with {auth_name}; retrying next auth mode.")
                continue
            raise

    if last_http_error is not None:
        raise last_http_error
    return []


def append_contacts(
    csv_path: Path,
    contacts: List[Dict[str, str]],
    next_contact_number: int,
) -> int:
    if not contacts:
        return next_contact_number

    with csv_path.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_COLUMNS)
        for contact in contacts:
            row = {
                "Contact #": next_contact_number,
                "Outreach Status": "",
                "Contact Name": contact.get("Contact Name", ""),
                "Contact Role / Title": contact.get("Contact Role / Title", ""),
                "Company / Organization": contact.get("Company / Organization", ""),
                "Company Size": contact.get("Company Size", ""),
                "LinkedIn URL": contact.get("LinkedIn URL", ""),
                "Email": contact.get("Email", ""),
                "Phone": contact.get("Phone", ""),
                "Source Notes": contact.get("Source Notes", ""),
            }
            writer.writerow(row)
            next_contact_number += 1

    return next_contact_number


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TinyFish contact prospecting")
    parser.add_argument(
        "--companies-file",
        default="companies.txt",
        help="Path to companies file (default: companies.txt)",
    )
    parser.add_argument(
        "--output-csv",
        default="contacts.csv",
        help="Path to output CSV (default: contacts.csv)",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=2.0,
        help="Delay between company requests in seconds (default: 2)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=180,
        help="HTTP timeout per request in seconds (default: 180)",
    )
    parser.add_argument(
        "--target-new-contacts",
        type=int,
        default=0,
        help="Stop early after appending this many new contacts (default: 0 = process all companies)",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    api_key = os.getenv("TINYFISH_API_KEY", "").strip()
    if not api_key:
        print("[ERROR] Missing TINYFISH_API_KEY. Add it to your .env file.", file=sys.stderr)
        return 1

    print(f"[INFO] Loaded TINYFISH_API_KEY: {mask_api_key(api_key)}")

    args = parse_args()
    companies_path = Path(args.companies_file)
    output_csv_path = Path(args.output_csv)

    try:
        companies = load_companies(companies_path)
    except Exception as exc:
        print(f"[ERROR] Failed loading companies: {exc}", file=sys.stderr)
        return 1

    if not companies:
        print("[WARN] No companies found in companies.txt")
        return 0

    ensure_csv(output_csv_path)
    next_contact_number = get_next_contact_number(output_csv_path)
    existing_contact_keys = load_existing_contact_keys(output_csv_path)

    print(f"Starting prospecting for {len(companies)} compan{'y' if len(companies) == 1 else 'ies'}")
    print(f"Input: {companies_path}")
    print(f"Output: {output_csv_path}")

    total_contacts = 0
    target_new_contacts = max(0, args.target_new_contacts)
    pass_definitions = PASS_DEFINITIONS + EXTRA_PASS_DEFINITIONS if target_new_contacts > 0 else PASS_DEFINITIONS

    if target_new_contacts > 0:
        print(
            f"Target mode enabled: stop after {target_new_contacts} new contacts. "
            f"Using {len(pass_definitions)} passes per company."
        )

    for index, (company_name, website) in enumerate(companies, start=1):
        if target_new_contacts > 0 and total_contacts >= target_new_contacts:
            print("\n" + "=" * 70)
            print(f"Target reached ({total_contacts}/{target_new_contacts}). Stopping early.")
            break

        print("\n" + "=" * 70)
        print(f"[{index}/{len(companies)}] Processing {company_name} ({website})")
        try:
            company_contacts: List[Dict[str, str]] = []
            for pass_index, (pass_name, pass_goal_template) in enumerate(pass_definitions, start=1):
                pass_contacts: List[Dict[str, str]] = []
                try:
                    pass_contacts = run_tinyfish_for_company(
                        api_key=api_key,
                        company_name=company_name,
                        website=website,
                        pass_goal_template=pass_goal_template,
                        timeout_seconds=args.timeout_seconds,
                    )
                except requests.HTTPError as exc:
                    body_preview = ""
                    if exc.response is not None and exc.response.text:
                        body_preview = f" | Response: {exc.response.text[:300]}"
                    print(
                        f"  [ERROR] HTTP error for {company_name} pass {pass_name}: {exc}{body_preview}",
                        file=sys.stderr,
                    )
                except requests.RequestException as exc:
                    print(f"  [ERROR] Request error for {company_name} pass {pass_name}: {exc}", file=sys.stderr)
                except Exception as exc:
                    print(f"  [ERROR] Unexpected error for {company_name} pass {pass_name}: {exc}", file=sys.stderr)

                company_contacts.extend(pass_contacts)
                print(
                    f"[{company_name}] Pass {pass_index}/{len(pass_definitions)} ({pass_name})... "
                    f"found {len(pass_contacts)}"
                )

            deduped_company_contacts = dedupe_contacts(company_contacts)
            new_contacts: List[Dict[str, str]] = []
            for contact in deduped_company_contacts:
                if contact_dedupe_key(contact) in existing_contact_keys:
                    continue
                new_contacts.append(contact)

            print(
                f"[{company_name}] After dedup: {len(new_contacts)} new contacts. Appending to CSV."
            )

            if new_contacts:
                next_contact_number = append_contacts(output_csv_path, new_contacts, next_contact_number)
                total_contacts += len(new_contacts)
                for contact in new_contacts:
                    existing_contact_keys.add(contact_dedupe_key(contact))
                print(f"  [OK] Appended {len(new_contacts)} row(s) to {output_csv_path}")

                if target_new_contacts > 0 and total_contacts >= target_new_contacts:
                    print(f"  [OK] Target reached during this company ({total_contacts}/{target_new_contacts}).")
            else:
                print("  [INFO] No new contacts to append for this company.")

        except requests.HTTPError as exc:
            body_preview = ""
            if exc.response is not None and exc.response.text:
                body_preview = f" | Response: {exc.response.text[:300]}"
            print(f"  [ERROR] HTTP error for {company_name}: {exc}{body_preview}", file=sys.stderr)
        except requests.RequestException as exc:
            print(f"  [ERROR] Request error for {company_name}: {exc}", file=sys.stderr)
        except Exception as exc:
            print(f"  [ERROR] Unexpected error for {company_name}: {exc}", file=sys.stderr)

        if index < len(companies):
            print(f"  [WAIT] Sleeping {args.delay_seconds:.1f}s before next company...")
            time.sleep(args.delay_seconds)

    print("\n" + "=" * 70)
    print(f"Done. Total contacts appended this run: {total_contacts}")
    if target_new_contacts > 0 and total_contacts < target_new_contacts:
        print(
            f"Target not reached: appended {total_contacts}/{target_new_contacts} new contacts. "
            "Add more companies or run again later."
        )
    print(f"CSV file: {output_csv_path.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

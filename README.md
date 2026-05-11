# atrope-outreach

Contact prospecting script for Atrope using the TinyFish Agent API (`/run-sse`) with live streaming logs.

## What it does

- Reads target companies from `companies.txt` (`Company Name,Website` per line)
- Calls TinyFish one company at a time to find procurement/operations/inventory/supply chain/logistics contacts
- Streams TinyFish progress to your terminal
- Appends contacts immediately to `contacts.csv` (does not wait until the end)
- Continues on errors (logs failure for that company and moves on)

## Files

- `prospect_contacts.py` - main script
- `companies.txt` - sample input file
- `.env.example` - required environment variable template

## Setup

1. Create and activate a virtual environment (optional but recommended)
2. Install dependencies:

```bash
python3 -m pip install requests python-dotenv
```

3. Create your `.env` file from the example and set your key:

```bash
cp .env.example .env
```

Then edit `.env` and set:

```env
TINYFISH_API_KEY=your_real_tinyfish_key
```

## Run

```bash
python3 prospect_contacts.py
```

Optional flags:

```bash
python3 prospect_contacts.py --companies-file companies.txt --output-csv contacts.csv --delay-seconds 2 --timeout-seconds 180
```

## Output CSV columns

The script writes `contacts.csv` with exactly these headers:

- `Contact #`
- `Outreach Status`
- `Contact Name`
- `Contact Role / Title`
- `Company / Organization`
- `Company Size`
- `LinkedIn URL`
- `Email`
- `Phone`
- `Source Notes`

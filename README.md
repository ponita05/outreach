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
- `companies.txt` - input companies (editable)
- `.env.example` - environment variable template
- `contacts.csv` - output file (created/appended by script)

## Requirements

- Python 3.10+
- A TinyFish API key

## Run on any laptop (Mac, Linux, Windows)

### 1) Open a terminal in the `outreach` folder

If you already have the repo locally:

```bash
cd /path/to/atrope-outreach/outreach
```

If you need to clone first:

```bash
git clone <your-repo-url>
cd atrope-outreach/outreach
```

### 2) (Recommended) create and activate a virtual environment

Mac/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Windows (PowerShell):

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
```

### 3) Install dependencies

Mac/Linux:

```bash
python3 -m pip install requests python-dotenv
```

Windows:

```powershell
py -m pip install requests python-dotenv
```

### 4) Configure your API key

Create `.env` from template:

Mac/Linux:

```bash
cp .env.example .env
```

Windows (PowerShell):

```powershell
copy .env.example .env
```

Edit `.env` and set:

```env
TINYFISH_API_KEY=your_real_tinyfish_key
```

### 5) Edit `companies.txt`

Format is one company per line, comma-separated:

```txt
Company Name,https://company-website.com
```

Use real company sites (not placeholder domains like `example.com`) to get real contacts.

### 6) Run the script

Mac/Linux:

```bash
python3 prospect_contacts.py
```

Windows:

```powershell
py prospect_contacts.py
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

## Troubleshooting

- `can't open file ... prospect_contacts.py`: you are in the wrong folder; `cd` into `.../outreach` first.
- `401 Unauthorized`: API key issue; confirm `.env` is in `outreach/` and `TINYFISH_API_KEY` is valid.
- `No matching contacts found`: usually means no relevant public contacts found for that website, or the site is a placeholder.

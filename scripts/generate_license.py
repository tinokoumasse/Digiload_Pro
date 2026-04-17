"""
Digiload Pro — License Key Generator v1.0
Internal tool — runs on YOUR machine, never on customer systems.

Usage:
    python generate_license.py --gate-id=3 --module=video_tracking --customer=ACME
    python generate_license.py --gate-id=3 --module=multi_angle --expires=2027-12-31
    python generate_license.py --list-all

Keys are HMAC-signed with your private LICENSE_SECRET.
The same secret is embedded in digiload_pro.py for validation.
No internet needed for validation on ZED Box.
"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import csv
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────────────────────
# YOUR PRIVATE SECRET — keep this safe, never commit to Git
# Set as environment variable: export DIGILOAD_LICENSE_SECRET=your_secret
# Must match LICENSE_SECRET in digiload_pro.py and central_app.py .env
# ─────────────────────────────────────────────────────────────────────────────
LICENSE_SECRET = os.environ.get(
    "DIGILOAD_LICENSE_SECRET",
    "DIGILOAD_LICENSE_SECRET_REPLACE_IN_PRODUCTION"
)

MODULES = ["video_tracking", "multi_angle"]

# ─────────────────────────────────────────────────────────────────────────────
# GENERATE
# ─────────────────────────────────────────────────────────────────────────────
def generate(gate_id: int, module: str, customer: str = "",
             expires: str = "", years: int = 1) -> str:
    """
    Generate a license key for a specific gate and module.

    Key format: base64(payload_json).hmac_hex
    Payload:    { gate_id, module, customer, expires, issued_at }
    """
    if module not in MODULES:
        raise ValueError(f"Unknown module: {module}. Valid: {MODULES}")

    # Expiry
    if expires:
        exp_date = expires
    else:
        exp_date = (datetime.now() + timedelta(days=365 * years)).strftime("%Y-%m-%d")

    payload = {
        "gate_id":    gate_id,
        "module":     module,
        "customer":   customer,
        "expires":    exp_date + "T23:59:59",
        "issued_at":  datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }

    payload_json = json.dumps(payload, separators=(",", ":"))
    payload_b64  = base64.b64encode(payload_json.encode()).decode().rstrip("=")
    sig          = hmac.new(
        LICENSE_SECRET.encode(),
        payload_b64.encode(),
        hashlib.sha256
    ).hexdigest()

    return f"{payload_b64}.{sig}"


def decode(key: str) -> dict:
    """Decode and display a license key without validating."""
    try:
        payload_b64, _ = key.rsplit(".", 1)
        payload_json   = base64.b64decode(payload_b64 + "==").decode()
        return json.loads(payload_json)
    except Exception as e:
        return {"error": str(e)}


def validate(key: str, gate_id: int, module: str) -> bool:
    """Validate a license key (same logic as digiload_pro.py)."""
    try:
        parts = key.rsplit(".", 1)
        if len(parts) != 2:
            return False
        payload_b64, sig = parts
        expected = hmac.new(
            LICENSE_SECRET.encode(),
            payload_b64.encode(),
            hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return False
        payload = json.loads(base64.b64decode(payload_b64 + "=="))
        if payload.get("gate_id") != gate_id:   return False
        if payload.get("module")  != module:     return False
        expires = payload.get("expires","")
        if expires and expires < datetime.now().isoformat():
            return False
        return True
    except Exception:
        return False


def generate_bulk(csv_path: str, output_path: str, years: int = 1):
    """
    Generate licenses from a CSV file.

    CSV format:
        gate_id,module,customer,expires
        1,video_tracking,ACME,2027-12-31
        2,video_tracking,ACME,
        2,multi_angle,ACME,
    """
    results = []
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                key = generate(
                    gate_id  = int(row["gate_id"]),
                    module   = row["module"].strip(),
                    customer = row.get("customer","").strip(),
                    expires  = row.get("expires","").strip(),
                    years    = years,
                )
                results.append({**row, "license_key": key})
                print(f"  ✅ Gate {row['gate_id']} — {row['module']}")
            except Exception as e:
                print(f"  ❌ Gate {row['gate_id']} — {e}")
                results.append({**row, "license_key": f"ERROR: {e}"})

    with open(output_path, "w", newline="") as f:
        if results:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
    print(f"\n  Saved to: {output_path}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Digiload Pro License Key Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate video_tracking license for gate 3, 1 year
  python generate_license.py --gate-id=3 --module=video_tracking --customer=ACME

  # Generate with custom expiry
  python generate_license.py --gate-id=3 --module=multi_angle --expires=2027-12-31

  # Generate for 5 years
  python generate_license.py --gate-id=5 --module=video_tracking --years=5

  # Decode an existing key
  python generate_license.py --decode=<key>

  # Validate a key
  python generate_license.py --validate=<key> --gate-id=3 --module=video_tracking

  # Bulk generate from CSV
  python generate_license.py --bulk=gates.csv --output=licenses.csv
        """
    )

    parser.add_argument("--gate-id",  type=int,  help="Gate ID")
    parser.add_argument("--module",   type=str,  help=f"Module: {MODULES}")
    parser.add_argument("--customer", type=str,  default="", help="Customer name")
    parser.add_argument("--expires",  type=str,  default="", help="Expiry date YYYY-MM-DD")
    parser.add_argument("--years",    type=int,  default=1,  help="License duration in years")
    parser.add_argument("--decode",   type=str,  help="Decode a license key")
    parser.add_argument("--validate", type=str,  help="Validate a license key")
    parser.add_argument("--bulk",     type=str,  help="Bulk generate from CSV file")
    parser.add_argument("--output",   type=str,  default="licenses_output.csv", help="Output CSV path")

    args = parser.parse_args()

    print("=" * 60)
    print("  Digiload Pro — License Key Generator")
    print("=" * 60)

    if LICENSE_SECRET == "DIGILOAD_LICENSE_SECRET_REPLACE_IN_PRODUCTION":
        print("\n  ⚠️  WARNING: Using default secret!")
        print("  Set: export DIGILOAD_LICENSE_SECRET=your_secret\n")

    # Decode
    if args.decode:
        payload = decode(args.decode)
        print("\n  License payload:")
        for k, v in payload.items():
            print(f"    {k:12} {v}")
        return

    # Validate
    if args.validate:
        if not args.gate_id or not args.module:
            print("  --gate-id and --module required for validation")
            return
        ok = validate(args.validate, args.gate_id, args.module)
        print(f"\n  Validation: {'✅ VALID' if ok else '❌ INVALID'}")
        return

    # Bulk
    if args.bulk:
        print(f"\n  Bulk generating from: {args.bulk}")
        generate_bulk(args.bulk, args.output, args.years)
        return

    # Single
    if not args.gate_id or not args.module:
        parser.print_help()
        return

    try:
        key = generate(
            gate_id  = args.gate_id,
            module   = args.module,
            customer = args.customer,
            expires  = args.expires,
            years    = args.years,
        )
        payload = decode(key)

        print(f"\n  Gate ID:   {args.gate_id}")
        print(f"  Module:    {args.module}")
        print(f"  Customer:  {args.customer or '—'}")
        print(f"  Expires:   {payload.get('expires','—')}")
        print(f"  Issued:    {payload.get('issued_at','—')}")
        print(f"\n  License Key:")
        print(f"  {key}")
        print(f"\n  ✅ Paste this into config.json → modules → {args.module} → license_key")

    except Exception as e:
        print(f"\n  ❌ Error: {e}")


if __name__ == "__main__":
    main()

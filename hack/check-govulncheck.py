#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import pathlib
import sys

ALLOWED_ARTIFACTS = {"juicefs.plori", "juicefs.plori-linux-amd64", "juicefs.plori-linux-arm64"}


def read_stream(path: pathlib.Path):
    raw = path.read_text(encoding="utf-8")
    decoder = json.JSONDecoder()
    offset = 0
    while offset < len(raw):
        while offset < len(raw) and raw[offset].isspace():
            offset += 1
        if offset == len(raw):
            return
        value, offset = decoder.raw_decode(raw, offset)
        yield value


def main() -> int:
    parser = argparse.ArgumentParser(description="Gate reachable govulncheck findings with expiring waivers")
    parser.add_argument("scan", type=pathlib.Path)
    parser.add_argument("waivers", type=pathlib.Path)
    parser.add_argument("artifact")
    args = parser.parse_args()
    if args.artifact not in ALLOWED_ARTIFACTS:
        raise ValueError(f"unsupported artifact: {args.artifact}")

    findings = {}
    config = None
    saw_sbom = False
    for event in read_stream(args.scan):
        if "config" in event:
            config = event["config"]
        if "SBOM" in event:
            saw_sbom = True
        finding = event.get("finding")
        if finding and any(frame.get("function") for frame in finding.get("trace", [])):
            findings.setdefault(finding["osv"], finding)

    if config is None or config.get("scan_level") != "symbol" or not saw_sbom:
        raise ValueError("incomplete govulncheck symbol scan")

    waiver_doc = json.loads(args.waivers.read_text(encoding="utf-8"))
    if waiver_doc.get("schemaVersion") != 1:
        raise ValueError("unsupported waiver schemaVersion")

    today = dt.datetime.now(dt.timezone.utc).date()
    active = {}
    for waiver in waiver_doc.get("waivers", []):
        missing = {"id", "artifact", "expires", "reason"} - waiver.keys()
        if missing:
            raise ValueError(f"waiver is missing fields: {sorted(missing)}")
        expires = dt.date.fromisoformat(waiver["expires"])
        if waiver["artifact"] not in ALLOWED_ARTIFACTS:
            raise ValueError(f"unsupported waiver artifact: {waiver['artifact']}")
        if not isinstance(waiver["reason"], str) or not waiver["reason"].strip():
            raise ValueError("vulnerability waiver reason must not be empty")
        key = (waiver["artifact"], waiver["id"])
        if expires < today:
            raise ValueError(f"expired vulnerability waiver: {key} expired {expires}")
        if expires > today + dt.timedelta(days=90):
            raise ValueError(f"vulnerability waiver exceeds 90 days: {key} expires {expires}")
        if key in active:
            raise ValueError(f"duplicate vulnerability waiver: {key}")
        active[key] = waiver

    unwaived = []
    used = set()
    for osv in sorted(findings):
        key = (args.artifact, osv)
        if key in active:
            used.add(key)
            print(f"WAIVED {osv} for {args.artifact} until {active[key]['expires']}: {active[key]['reason']}")
        else:
            unwaived.append(osv)

    unused = sorted(key for key in active if key[0] == args.artifact and key not in used)
    if unused:
        raise ValueError(f"unused vulnerability waivers must be removed: {unused}")
    if unwaived:
        print(f"Unwaived reachable vulnerabilities in {args.artifact}: {', '.join(unwaived)}", file=sys.stderr)
        return 1
    print(f"No unwaived reachable vulnerabilities in {args.artifact}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"govulncheck gate failed: {error}", file=sys.stderr)
        raise SystemExit(2)

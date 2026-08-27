#!/usr/bin/env python3
# Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Keeps common/flag_decls.h in step with the DEFINE_ sites it mirrors.

Definitions live next to the code that reads them; declarations are collected
in one header so other modules -- src/client above all -- have a single place
to include. Nothing but this check couples the two, and both halves have
drifted before: a flag defined and never declared, and a group comment naming
a file that no longer exists.

Compares name AND type in both directions, and checks that every group comment
names a real file which does define the flags listed under it.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DECLS = ROOT / "common" / "flag_decls.h"

# Tool-local flags are declared beside their own .cc, and `conf` is deliberately
# isolated in the standalone blockcache_flags target.
EXCLUDED = (ROOT / "tools", ROOT / "utils" / "flags.cc")

# DEFINE_validator shares the prefix but declares nothing, so it is excluded.
DEFINE_RE = re.compile(r"^DEFINE_(?!validator\b)(\w+)\((\w+)\s*,", re.M)
DECLARE_RE = re.compile(r"^DECLARE_(\w+)\((\w+)\)\s*;", re.M)
GROUP_RE = re.compile(r"^//\s*([\w/]+\.cc)", re.M)


def excluded(path):
    return any(path == e or e in path.parents for e in EXCLUDED)


def defined_flags():
    out = {}
    for cc in sorted(ROOT.rglob("*.cc")):
        if excluded(cc):
            continue
        for type_, name in DEFINE_RE.findall(cc.read_text()):
            out[name] = (type_, cc.relative_to(ROOT).as_posix())
    return out


def declared_flags():
    text = DECLS.read_text()
    declared = {name: type_ for type_, name in DECLARE_RE.findall(text)}

    # Attribute each declaration to the group comment above it.
    group_of, group = {}, None
    for line in text.splitlines():
        if m := GROUP_RE.match(line.strip()):
            group = m.group(1)
        elif m := DECLARE_RE.match(line.strip()):
            group_of[m.group(2)] = group
    return declared, group_of


def main():
    defined = defined_flags()
    declared, group_of = declared_flags()
    errors = []

    for name, (type_, where) in sorted(defined.items()):
        if name not in declared:
            errors.append(f"{where}: --{name} is defined but not declared in "
                          f"{DECLS.name}")
        elif declared[name] != type_:
            errors.append(f"--{name}: defined as {type_} in {where} but "
                          f"declared as {declared[name]}")

    for name in sorted(declared):
        if name not in defined:
            errors.append(f"{DECLS.name}: --{name} is declared but no "
                          f"DEFINE_ for it exists under {ROOT.name}/")
            continue
        group, where = group_of.get(name), defined[name][1]
        if group != where:
            errors.append(f"{DECLS.name}: --{name} sits under '// {group}' "
                          f"but is defined in {where}")

    for e in errors:
        print(e, file=sys.stderr)
    print(f"checked {len(defined)} flags", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())

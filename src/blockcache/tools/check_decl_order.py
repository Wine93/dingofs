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

"""Checks that each .cc defines members in the order its .h declares them.

    python3 check_decl_order.py <dir-or-file> ...

Only members present in both files are compared, so a header may declare what
it likes inline; what is checked is that the two files tell the same story in
the same order. See docs/cn/style.md.
"""

import os
import re
import sys

CLASS_RE = re.compile(
    r'^\s*(?:class|struct)\s+'
    r'(?:alignas\s*\([^)]*\)\s*|\[\[[^\]]*\]\]\s*)*'
    r'(?:\w+\s+)*?'
    r'(?:\w+::)*(\w+)'
    r'(?:\s+final)?'
    r'\s*(?::[^;{]*)?'
    r'\s*\{?\s*$')
DECL_RE = re.compile(r'^\s+(?:[\w:<>,&*\s]+?\s)?(~?\w+|operator\s*[^\s(]+)\s*\(')
DEF_RE = re.compile(r'^[A-Za-z_][^=]*?(\w+)::(~?\w+|operator\s*[^\s(]+)\s*\(')
SKIP = ('using ', 'typedef ', 'friend ', 'static_assert', 'return ', '#')
KEYWORDS = ('if', 'for', 'while', 'switch', 'catch', 'else', 'do', 'sizeof')


def code_of(line):
    """The line with string literals and trailing comment removed."""
    line = re.sub(r'"(\\.|[^"\\])*"', '""', line)
    line = re.sub(r"'(\\.|[^'\\])*'", "''", line)
    return line.split('//')[0]


def declarations(path):
    """[(class, member)] in header order; bodies of inline members are skipped."""
    out, stack, depth = [], [], 0
    for line in open(path):
        code = code_of(line)
        stripped = code.strip()
        at_member_level = bool(stack) and depth == stack[-1][1]

        if at_member_level and not stripped.startswith(SKIP) and '(' in stripped:
            m = DECL_RE.match(code)
            if m and m.group(1) not in KEYWORDS:
                out.append((stack[-1][0], m.group(1)))

        m = CLASS_RE.match(code)
        opened = code.count('{')
        if m and opened:
            stack.append((m.group(1), depth + 1))
        depth += opened - code.count('}')
        while stack and depth < stack[-1][1]:
            stack.pop()
    return out


def definitions(path):
    """[(class, member)] in .cc order."""
    out = []
    for line in open(path):
        if not line or line[0].isspace() or line.startswith(SKIP):
            continue
        m = DEF_RE.match(line)
        if m:
            out.append((m.group(1), m.group(2)))
    return out


def dedup(pairs):
    """First occurrence only: overloads share a name, so only their first
    declaration and first definition can be compared for order."""
    seen, out = set(), []
    for pair in pairs:
        if pair not in seen:
            seen.add(pair)
            out.append(pair)
    return out


def check(cc):
    header = cc[:-3] + '.h'
    if not os.path.exists(header):
        return []
    decls, defs = dedup(declarations(header)), dedup(definitions(cc))
    wanted = [d for d in decls if d in defs]
    got = [d for d in defs if d in decls]
    if wanted == got:
        return []
    return ['%s: definition order does not match %s\n  header: %s\n  source: %s'
            % (cc, os.path.basename(header),
               ', '.join('%s::%s' % d for d in wanted),
               ', '.join('%s::%s' % d for d in got))]


def main(argv):
    sources = []
    for arg in argv or ['.']:
        if os.path.isfile(arg):
            sources.append(arg)
            continue
        for root, _, files in os.walk(arg):
            sources += [os.path.join(root, f) for f in files if f.endswith('.cc')]

    problems = [p for cc in sorted(sources) for p in check(cc)]
    for problem in problems:
        print(problem)
    print('checked %d sources, %d out of order' % (len(sources), len(problems)))
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

#!/usr/bin/env python3
"""
Find the EXACT source of any SyntaxWarning in your ICA tools workspace.

Usage:
    python3 find_syntax_warnings.py [directory]

Default directory is the current one. This will:
  1. Clear all __pycache__ directories recursively
  2. Compile every .py file with SyntaxWarning treated as an error
  3. Print the exact file and line of any warning

If Python 3.13+ flags something, this will tell you WHICH file and WHICH line.
"""
import os
import sys
import warnings
import py_compile
import shutil


def clear_caches(root):
    cleared = 0
    for dirpath, dirnames, _ in os.walk(root):
        if "__pycache__" in dirnames:
            cache_path = os.path.join(dirpath, "__pycache__")
            try:
                shutil.rmtree(cache_path)
                cleared += 1
            except Exception as e:
                print(f"⚠️  Could not clear {cache_path}: {e}")
    return cleared


def check_file(path):
    """Compile a single file with SyntaxWarnings captured."""
    try:
        with open(path, encoding="utf-8") as f:
            src = f.read()
    except Exception as e:
        return [("read-error", 0, str(e))]

    warnings_found = []
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            compile(src, path, "exec")
        except SyntaxError as e:
            warnings_found.append(("SyntaxError", e.lineno or 0, str(e)))
        for w in caught:
            if issubclass(w.category, SyntaxWarning):
                warnings_found.append((w.category.__name__, w.lineno or 0, str(w.message)))
    return warnings_found


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    root = os.path.abspath(root)

    print(f"🔍 Scanning {root}")
    print(f"🐍 Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
    print()

    cleared = clear_caches(root)
    if cleared:
        print(f"🧹 Cleared {cleared} __pycache__ directories")
        print()

    total_files = 0
    total_warnings = 0
    for dirpath, _, filenames in os.walk(root):
        for fname in filenames:
            if not fname.endswith(".py"):
                continue
            total_files += 1
            full = os.path.join(dirpath, fname)
            rel = os.path.relpath(full, root)
            warnings_list = check_file(full)
            if warnings_list:
                for kind, lineno, msg in warnings_list:
                    total_warnings += 1
                    print(f"⚠️  {rel}:{lineno}")
                    print(f"    [{kind}] {msg}")
                    print()

    print(f"Scanned {total_files} files, found {total_warnings} issues")


if __name__ == "__main__":
    main()

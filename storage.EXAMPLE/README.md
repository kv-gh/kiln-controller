# Storage Directory Template

This directory contains example kiln firing profiles.

## Setup

Copy this entire directory to create your working storage:

```bash
cp -r storage.EXAMPLE storage
```

The `storage/` directory is gitignored, so your personal firing profiles and schedules won't be committed to version control.

## Directory Structure

- `profiles/` - Kiln firing schedule profiles (JSON files)
- `schedules.json` - Scheduled firing times (created automatically)

## Example Profiles Included

- `cone-05-long-bisque.json` - Bisque firing to cone 05
- `cone-6-long-glaze.json` - Glaze firing to cone 6
- `test-fast.json` - Fast test profile
- `test-200-250.json` - Temperature range test

You can create your own profiles through the web interface or by copying and modifying these examples.

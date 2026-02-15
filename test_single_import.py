# test_single_import.py
"""
Test importing a single hand history file to see the exact error.
"""
import sys
from pathlib import Path

try:
    print("Step 1: Import parser...")
    from importer.gg_hand_history_parser import parse_file

    print("✓ Parser imported")

    print("\nStep 2: Import spot classifier...")
    from importer.spot_classifier import classify_preflop

    print("✓ Spot classifier imported")

    print("\nStep 3: Import HandImporter...")
    from importer.hand_importer import HandImporter

    print("✓ HandImporter imported")

    print("\nStep 4: Find a hand history file...")
    from importer.tournament_importer import build_import_config

    config_path = Path("config/config.yaml")
    cfg = build_import_config(config_path)

    hand_dir = cfg.hand_histories.input_dir
    txt_files = list(hand_dir.glob("*.txt"))

    if not txt_files:
        print(f"❌ No files found in {hand_dir}")
        sys.exit(1)

    test_file = txt_files[0]
    print(f"✓ Using: {test_file.name}")

    print("\nStep 5: Parse the file...")
    hands = parse_file(str(test_file))
    print(f"✓ Parsed {len(hands)} hands")

    print("\nStep 6: Classify spots...")
    classified = [classify_preflop(h) for h in hands]
    print(f"✓ Classified {len(classified)} hands")

    print("\nStep 7: Test _to_row conversion...")
    from importer.hand_importer import _to_row

    for i, hand in enumerate(classified[:3]):  # Test first 3 hands
        try:
            row = _to_row(hand)
            print(f"  ✓ Hand {i + 1}: Converted to row with {len(row)} fields")
        except AttributeError as e:
            print(f"  ❌ Hand {i + 1}: AttributeError - {e}")
            print(f"     Hand has these attributes: {list(hand.__dict__.keys())[:10]}...")
            break

    print("\n✅ ALL STEPS PASSED! You can now run: python main.py ingest-hands")

except Exception as e:
    import traceback

    print(f"\n❌ ERROR at step: {type(e).__name__}: {e}")
    print(f"\nFull traceback:")
    print(traceback.format_exc())
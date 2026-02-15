# diagnose_parser_error.py
"""
Run this to see the actual error that's happening during parsing.
"""
import sys
import traceback

# Try to import and parse a single file
try:
    print("Testing parser import...")
    from importer.gg_hand_history_parser import parse_file

    print("✓ Parser imported successfully")

    # Try to parse one of the failed files
    test_file = r"C:\Users\jwent\OneDrive\Desktop\Poker\Poker Sites\GG Poker\Hand Histories\GG20260211-1055 - Spin&Gold #1.txt"

    print(f"\nTrying to parse: {test_file}")
    hands = parse_file(test_file)

    print(f"✓ SUCCESS! Parsed {len(hands)} hands")
    if hands:
        print(f"\nFirst hand details:")
        h = hands[0]
        print(f"  Tournament: {h.tournament_id}")
        print(f"  Hand ID: {h.hand_id}")
        print(f"  Hero position: {h.hero_position}")
        print(f"  Hero cards: {h.hero_cards}")
        print(f"  Stack: {h.hero_stack_start}")

except Exception as e:
    print(f"\n❌ ERROR: {type(e).__name__}: {e}")
    print(f"\nFull traceback:")
    print(traceback.format_exc())

    # Try to identify the specific issue
    if "has no attribute" in str(e):
        print("\n🔍 This is an AttributeError - likely a regex capture group mismatch")
        print("   Check that regex patterns match the code that accesses them")
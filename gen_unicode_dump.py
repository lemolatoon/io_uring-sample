import os
import unicodedata

OUTPUT_FILE = "unicode_dump.txt"
TARGET_SIZE = 1 * 1024 * 1024 * 1024  # 1GB

def unicode_generator():
    for code_point in range(0x110000):
        # Skip surrogate pair range (invalid Unicode scalar values)
        if 0xD800 <= code_point <= 0xDFFF:
            continue
        try:
            char = chr(code_point)
            category = unicodedata.category(char)
            # Skip unassigned characters (Cn) and non-printable control characters
            if category == 'Cn' or not char.isprintable():
                continue
            yield char.encode('utf-8')
        except UnicodeEncodeError:
            continue

with open(OUTPUT_FILE, 'wb') as f:
    total_written = 0
    while total_written < TARGET_SIZE:
        for encoded_char in unicode_generator():
            f.write(encoded_char)
            total_written += len(encoded_char)
            if total_written >= TARGET_SIZE:
                break

print(f"Finished writing {total_written / (1024**2):.2f} MB to {OUTPUT_FILE}")

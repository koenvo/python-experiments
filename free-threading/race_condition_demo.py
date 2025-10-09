"""
Race condition demo: += operator is not atomic

counter[0] += 1 actually does:
1. temp = counter[0]     (READ)
2. temp = temp + 1       (MODIFY)
3. counter[0] = temp     (WRITE)

With GIL: Steps complete before thread switch (mostly safe)
Without GIL: Multiple threads in steps 1-3 simultaneously (data loss)
"""

import sys
import threading


def increment(counter: list, iterations: int):
    for _ in range(iterations):
        counter[0] += 1  # NOT ATOMIC!


def main():
    num_threads = 10
    iterations = 100_000
    expected = num_threads * iterations

    gil_enabled = getattr(sys, '_is_gil_enabled', lambda: True)()
    mode = "GIL-based" if gil_enabled else "Free-threaded"

    print(f"Python: {sys.version.split()[0]}")
    print(f"Mode: {mode}")
    print(f"Expected: {expected:,}\n")

    for i in range(5):
        counter = [0]
        threads = [threading.Thread(target=increment, args=(counter, iterations)) for _ in range(num_threads)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        lost = expected - counter[0]
        loss_pct = (lost / expected) * 100
        print(f"Run {i+1}: {counter[0]:,} (lost: {lost:,} = {loss_pct:.1f}%)")


if __name__ == "__main__":
    main()
import pandas as pd
import os

def split_dataset(file_path, train_ratio=0.7, val_ratio=0.15):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    df = pd.read_csv(file_path)

    total = len(df)
    train_end = int(total * train_ratio)
    val_end = int(total * (train_ratio + val_ratio))

    train_df = df[:train_end]
    val_df = df[train_end:val_end]
    test_df = df[val_end:]

    # Output filenames
    base = file_path.replace(".csv", "")
    train_df.to_csv(base + "_train.csv", index=False)
    val_df.to_csv(base + "_val.csv", index=False)
    test_df.to_csv(base + "_test.csv", index=False)

    print(f"Done splitting → {file_path}")
    print(f"Train: {len(train_df)}, Val: {len(val_df)}, Test: {len(test_df)}")

# ---- RUN FOR EACH FILE ----
files = [
    "btc_hourly_mc.csv",
    "btc_daily_mc.csv",
    "btc_weekly_mc.csv"
]

for f in files:
    split_dataset(f)

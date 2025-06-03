import pandas as pd

pd.set_option('display.max_rows', 10000)

# Đọc file parquet
df = pd.read_parquet('./output_data/interactions_limited.parquet')  # Bạn có thể chỉ định engine nếu cần: engine='pyarrow' hoặc 'fastparquet'
df1 = pd.read_parquet('./output_data/user_features_limited.parquet')
df2 = pd.read_parquet('./output_data/video_features_limited.parquet')
df3 = pd.read_parquet('./output_data/user_channel_interactions_limited.parquet')
# Hiển thị dữ liệu
print("INTERACTION DATA")
print(df.head())
print("Tổng số dòng tương tác:", len(df))
print("Tổng số user thực hiện tương tác (không trùng):", df['user_id'].nunique())
print("\nUSER FEATURE")
print(df1.head())
print("Tổng số user (có thể trùng):", len(df1))
print("Tổng số user (không trùng):", df1['user_id'].nunique())
print("\nVIDEO FEATURE")
print(df2.head())
print("\nUSER CHANNEL INTERACTION")
print(df3.head())
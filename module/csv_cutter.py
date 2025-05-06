import pandas as pd

# Đọc file CSV gốc
df = pd.read_csv('customers-2000000.csv')

# Số dòng muốn cắt (ví dụ: 100)
num_records = 1000000

# Lấy phần cần cắt
df_cut = df.head(num_records)

# Lưu vào file mới
df_cut.to_csv(f'customers-{num_records}.csv', index=False)
# Dự án Thu thập Dữ liệu và Hệ thống Gợi ý Video

## Giới thiệu 

Dự án này bao gồm hai thành phần chính:
1.  **Thu thập và Xử lý Dữ liệu (`COLLECT_DATA`):** Các scripts và quy trình để thu thập dữ liệu từ nguồn và xử lý chúng thành các tập dataset sạch, sẵn sàng cho việc huấn luyện mô hình.
2.  **Dịch vụ Gợi ý Video (`recommendation_api`):** Một API service sử dụng mô hình LightFM đã được huấn luyện để cung cấp các gợi ý video cho người dùng.

Mô hình gợi ý được huấn luyện riêng biệt (trên Kaggle) và các artifacts của mô hình (model, dataset object, etc.) được sử dụng bởi API service.

## Cấu trúc Thư mục  

Dưới đây là mô tả sơ lược về các thư mục chính trong dự án:

-   `config/`: Chứa các file cấu hình cho quá trình thu thập và xử lý dữ liệu.
-   `data/`: Có thể chứa dữ liệu thô ban đầu hoặc các tập dữ liệu trung gian.
    -   `data/raw/video_view_logs`: Ví dụ về nơi lưu trữ logs dữ liệu thô.
-   `data_collection/`: Chứa các module Python liên quan đến việc thu thập dữ liệu.
-   `data_processing/`: Chứa các module Python cho việc làm sạch và xử lý dữ liệu.
-   `notebooks/`: (Nếu có) Chứa các Jupyter Notebooks dùng cho việc khám phá dữ liệu, thử nghiệm.
-   `output_data/`: Nơi lưu trữ các tập dataset đã được xử lý, sẵn sàng cho huấn luyện, hoặc các model artifacts nếu muốn.
-   `recommendation_api/`: Chứa mã nguồn và các tệp cần thiết để chạy API dịch vụ gợi ý.
    -   `app.py`: File Flask chính cho API.
    -   `*.joblib`: Các model artifacts đã huấn luyện (model, dataset, dataframes, matrices) cần được đặt ở đây.
-   `sample_logs/`: Chứa các file log mẫu.
-   `scripts/`: Có thể chứa các script tiện ích hoặc script chạy chính khác.
-   `tests/`: Chứa các unit test hoặc integration test.
-   `venv/` hoặc `venv_api/`: Thư mục môi trường ảo Python (nên được thêm vào `.gitignore`).
-   `main_data_collection.py`: Script chính để chạy quy trình thu thập và xử lý dữ liệu.
-   `README.md`: File hướng dẫn này.
-   `requirements.txt`: Liệt kê các thư viện Python cần thiết cho toàn bộ dự án.

## Cài đặt 

### Điều kiện tiên quyết  

-   Python (khuyến nghị phiên bản 3.7 trở lên)
-   `pip` (trình quản lý gói Python)

### Tạo và Kích hoạt Môi trường ảo 

1.  Mở terminal hoặc command prompt, di chuyển đến thư mục gốc của dự án.
2.  Tạo môi trường ảo:
    ```bash
    python -m venv venv
    ```
3.  Kích hoạt môi trường ảo:
    * Trên Windows (Command Prompt):
        ```bash
        venv\Scripts\activate
        ```
    * Trên Windows (PowerShell):
        ```bash
        .\venv\Scripts\Activate.ps1
        ```
    * Trên macOS/Linux:
        ```bash
        source venv/bin/activate
        ```
    Bạn sẽ thấy `(venv)` xuất hiện ở đầu dòng lệnh khi môi trường ảo được kích hoạt.

### Cài đặt Thư viện Phụ thuộc (Install Dependencies)

Khi môi trường ảo đã được kích hoạt, cài đặt tất cả các thư viện cần thiết từ file `requirements.txt`:
```bash
pip install -r requirements.txt

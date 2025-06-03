import os
import pandas as pd
from datetime import datetime

# Import các module đã tạo
from data_collection import mysql_extractor, mongodb_extractor, log_processor
from data_processing import transformer
import configparser

# --- Cấu hình Đường dẫn Output ---
OUTPUT_DIR = "output_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# def main():
#     print(f"--- Bắt đầu Pipeline Thu thập Dữ liệu (v3 - User Demographics: username, birthday, gender) - {datetime.now()} ---")

#     # --- 1. Trích xuất Dữ liệu ---
#     print("\n[PHẦN 1: TRÍCH XUẤT DỮ LIỆU]")
    
#     mysql_conn_videodb = mysql_extractor.get_mysql_connection(section='mysql_prod_videodb')
#     mysql_conn_dbcms = mysql_extractor.get_mysql_connection(section='mysql_prod_dbcms')
#     mongo_db = mongodb_extractor.get_mongodb_client()

#     video_items_df = pd.DataFrame()
#     channel_info_df = pd.DataFrame()
#     if mysql_conn_videodb:
#         video_items_df = mysql_extractor.fetch_video_items(mysql_conn_videodb)
#         channel_info_df = mysql_extractor.fetch_channel_info(mysql_conn_videodb)
#         mysql_conn_videodb.close()
#     else:
#         print("CẢNH BÁO: Không thể kết nối MySQL (videodb).")
#         # Dữ liệu mẫu
#         video_items_df = pd.DataFrame({'video_id': ['v_db_1'], 'channel_id': ['ch_db_1']})
#         channel_info_df = pd.DataFrame({'channel_id': ['ch_db_1']})


#     user_demographics_df = pd.DataFrame()
#     if mysql_conn_dbcms:
#         user_demographics_df = mysql_extractor.fetch_user_demographics(mysql_conn_dbcms)
#         mysql_conn_dbcms.close()
#     else:
#         print("CẢNH BÁO: Không thể kết nối MySQL (dbcms).")
#         # Dữ liệu mẫu
#         user_demographics_df = pd.DataFrame({
#             'user_key': ['u_log_1', 'u_log_2', 'u_db_1'], 
#             'age': [25, 30, 22], 
#             'gender': ['Nam', 'Nữ', 'Nam']
#         })
#         user_demographics_df['user_key'] = user_demographics_df['user_key'].astype(str)


#     likes_df = pd.DataFrame()
#     follows_df = pd.DataFrame()
#     comments_df = pd.DataFrame()
#     if mongo_db:
#         likes_df = mongodb_extractor.fetch_video_likes(mongo_db)
#         follows_df = mongodb_extractor.fetch_channel_follows(mongo_db)
#         comments_df = mongodb_extractor.fetch_comments(mongo_db)
#     else:
#         print("CẢNH BÁO: Không thể kết nối MongoDB.")
#         # Dữ liệu mẫu
#         likes_df = pd.DataFrame({'user_id': ['u_log_1'], 'video_id': ['v_log_1'], 'interaction_type': ['like'], 'timestamp': [pd.to_datetime('2023-01-05')]})


#     print("\nĐang xử lý file log...")
#     watch_log_df = log_processor.process_log_files()
#     if watch_log_df.empty:
#         print("CẢNH BÁO: Không có dữ liệu từ log file. Sử dụng dữ liệu xem mẫu.")
#         watch_log_df = pd.DataFrame({
#             'user_id': ['u_log_1', 'u_log_1', 'u_log_2'], 'video_id': ['v_log_1', 'v_log_2', 'v_log_1'],
#             'interaction_type': ['watch', 'watch', 'watch'],
#             'watch_time_seconds': [100.0, 20.0, 50.5],
#             'video_duration_seconds': [120.0, 80.0, 60.0],
#             'watch_percentage': [83.33, 25.0, 84.17],
#             'timestamp': pd.to_datetime(['2023-01-05', '2023-01-06', '2023-01-05']),
#             'client_type': ['ANDROID', 'IOS', 'ANDROID'], 'network_type': ['WIFI', '4G', 'WIFI']
#         })

#     # --- 2. Chuyển đổi và Kết hợp Dữ liệu ---
#     print("\n[PHẦN 2: CHUYỂN ĐỔI VÀ KẾT HỢP DỮ LIỆU]")
#     interactions_df, video_features_df, user_features_df, user_channel_interactions_df = \
#         transformer.transform_data(watch_log_df, likes_df, comments_df, follows_df,
#                                    video_items_df, channel_info_df, user_demographics_df)

#     # --- 3. Lưu trữ Dữ liệu Đã Xử lý ---
#     # ... (Logic lưu trữ giữ nguyên như trước) ...
#     print("\n[PHẦN 3: LƯU TRỮ DỮ LIỆU ĐÃ XỬ LÝ]")
#     if not interactions_df.empty:
#         interactions_path = os.path.join(OUTPUT_DIR, "interactions.parquet")
#         interactions_df.to_parquet(interactions_path, index=False)
#         print(f"Đã lưu interactions_df vào: {interactions_path} (Shape: {interactions_df.shape})")

#     if not video_features_df.empty:
#         video_features_path = os.path.join(OUTPUT_DIR, "video_features.parquet")
#         video_features_df.to_parquet(video_features_path, index=False)
#         print(f"Đã lưu video_features_df vào: {video_features_path} (Shape: {video_features_df.shape})")

#     if not user_features_df.empty:
#         user_features_path = os.path.join(OUTPUT_DIR, "user_features.parquet")
#         user_features_df.to_parquet(user_features_path, index=False)
#         print(f"Đã lưu user_features_df vào: {user_features_path} (Shape: {user_features_df.shape})")
    
#     if not user_channel_interactions_df.empty:
#         user_channel_interactions_path = os.path.join(OUTPUT_DIR, "user_channel_interactions.parquet")
#         user_channel_interactions_df.to_parquet(user_channel_interactions_path, index=False)
#         print(f"Đã lưu user_channel_interactions_df vào: {user_channel_interactions_path} (Shape: {user_channel_interactions_df.shape})")


#     print(f"\n--- Hoàn thành Pipeline Thu thập Dữ liệu (v3) - {datetime.now()} ---")


def main():
    print(f"--- Bắt đầu Pipeline Thu thập Dữ liệu (v3 - User Demographics, LIMIT 1000 from DB) - {datetime.now()} ---")

    # --- 1. Trích xuất Dữ liệu ---
    print("\n[PHẦN 1: TRÍCH XUẤT DỮ LIỆU]")
    
    # Giới hạn số lượng bản ghi lấy từ DB
    DB_ROW_LIMIT = 100000

    # Kết nối DB videodb (chứa video, channel)
    mysql_conn_videodb = mysql_extractor.get_mysql_connection(section='mysql_prod_videodb')
    # Kết nối DB dbcms (chứa user demographics)
    mysql_conn_dbcms = mysql_extractor.get_mysql_connection(section='mysql_prod_dbcms')
    # Kết nối MongoDB
    mongo_db = mongodb_extractor.get_mongodb_client()

    # Trích xuất từ MySQL - videodb
    video_items_df = pd.DataFrame()
    channel_info_df = pd.DataFrame()
    if mysql_conn_videodb:
        print(f"Trích xuất tối đa {DB_ROW_LIMIT} bản ghi từ videodb (MySQL)...")
        video_items_df = mysql_extractor.fetch_video_items(mysql_conn_videodb, limit=DB_ROW_LIMIT)
        print("VIDEO ITEM")
        print(video_items_df.head())

        channel_info_df = mysql_extractor.fetch_channel_info(mysql_conn_videodb, limit=DB_ROW_LIMIT)
        print("CHANNEL INFO")
        print(channel_info_df.head())

        mysql_conn_videodb.close()
    else:
        print("CẢNH BÁO: Không thể kết nối MySQL (videodb). Sẽ sử dụng dữ liệu mẫu nếu có.")
        # Dữ liệu mẫu
        video_items_df = pd.DataFrame({'video_id': ['v_db_1'], 'channel_id': ['ch_db_1']})
        channel_info_df = pd.DataFrame({'channel_id': ['ch_db_1']})


    # Trích xuất từ MySQL - dbcms (user demographics)
    user_demographics_df = pd.DataFrame()
    if mysql_conn_dbcms:
        print(f"Trích xuất tối đa {DB_ROW_LIMIT} bản ghi từ dbcms (MySQL)...")
        # QUAN TRỌNG: Đảm bảo hàm fetch_user_demographics trong mysql_extractor.py
        # đã được cập nhật đúng tên bảng và tên cột cho bảng users của bạn trong dbcms.
        user_demographics_df = mysql_extractor.fetch_user_demographics(mysql_conn_dbcms, limit=DB_ROW_LIMIT)
        print("USERS ")
        print(user_demographics_df.head())
        mysql_conn_dbcms.close()
    else:
        print("CẢNH BÁO: Không thể kết nối MySQL (dbcms). Sẽ sử dụng dữ liệu user demographics mẫu nếu có.")
        # Dữ liệu mẫu
        user_demographics_df = pd.DataFrame({
            'user_key': ['u_log_1', 'u_log_2', 'u_db_1'], 
            'age': [25, 30, 22], 
            'gender': ['Nam', 'Nữ', 'Nam']
        })
        user_demographics_df['user_key'] = user_demographics_df['user_key'].astype(str)


    # Trích xuất từ MongoDB
    likes_df = pd.DataFrame()
    follows_df = pd.DataFrame()
    comments_df = pd.DataFrame()
    if mongo_db:
        print(f"Trích xuất tối đa {DB_ROW_LIMIT} bản ghi từ mỗi collection trong MongoDB...")
        likes_df = mongodb_extractor.fetch_video_likes(mongo_db, limit=DB_ROW_LIMIT)
        print("VIDEO LIKED")
        print(likes_df.head())
        follows_df = mongodb_extractor.fetch_channel_follows(mongo_db, limit=DB_ROW_LIMIT)
        print("\nCHANNLE FOLLOW")
        print(follows_df.head())
        comments_df = mongodb_extractor.fetch_comments(mongo_db, limit=DB_ROW_LIMIT)
        print("\nCOMMENT")
        print(comments_df.head())
    else:
        print("CẢNH BÁO: Không thể kết nối MongoDB. Sẽ sử dụng dữ liệu MongoDB mẫu nếu có.")
        # Dữ liệu mẫu
        likes_df = pd.DataFrame({'user_id': ['u_log_1'], 'video_id': ['v_log_1'], 'interaction_type': ['like'], 'timestamp': [pd.to_datetime('2023-01-05')]})


    print("\nĐang xử lý file log...")
    # Xử lý log file vẫn có thể xử lý toàn bộ file log có sẵn trong thư mục đã cấu hình,
    # vì chúng ta thường xử lý log theo từng file hoặc theo ngày.
    # Nếu muốn giới hạn cả số lượng dòng log được xử lý, bạn cần thêm logic vào log_processor.py
    watch_log_df = log_processor.process_log_files() 
    if watch_log_df.empty:
        print("CẢNH BÁO: Không có dữ liệu từ log file. Sử dụng dữ liệu xem mẫu.")
        watch_log_df = pd.DataFrame({
            'user_id': ['u_log_1', 'u_log_1', 'u_log_2'], 'video_id': ['v_log_1', 'v_log_2', 'v_log_1'],
            'interaction_type': ['watch', 'watch', 'watch'],
            'watch_time_seconds': [100.0, 20.0, 50.5],
            'video_duration_seconds': [120.0, 80.0, 60.0],
            'watch_percentage': [83.33, 25.0, 84.17],
            'timestamp': pd.to_datetime(['2023-01-05', '2023-01-06', '2023-01-05']),
            'client_type': ['ANDROID', 'IOS', 'ANDROID'], 'network_type': ['WIFI', '4G', 'WIFI']
        })

    # --- 2. Chuyển đổi và Kết hợp Dữ liệu ---
    print("\n[PHẦN 2: CHUYỂN ĐỔI VÀ KẾT HỢP DỮ LIỆU]")
    interactions_df, video_features_df, user_features_df, user_channel_interactions_df = \
        transformer.transform_data(watch_log_df, likes_df, comments_df, follows_df,
                                   video_items_df, channel_info_df, user_demographics_df)

    # --- 3. Lưu trữ Dữ liệu Đã Xử lý ---
    print("\n[PHẦN 3: LƯU TRỮ DỮ LIỆU ĐÃ XỬ LÝ]")
    if not interactions_df.empty:
        interactions_path = os.path.join(OUTPUT_DIR, "interactions_limited.parquet") # Đổi tên file output để biết là dữ liệu giới hạn
        interactions_df.to_parquet(interactions_path, index=False)
        print(f"Đã lưu interactions_df vào: {interactions_path} (Shape: {interactions_df.shape})")

    if not video_features_df.empty:
        video_features_path = os.path.join(OUTPUT_DIR, "video_features_limited.parquet")
        video_features_df.to_parquet(video_features_path, index=False)
        print(f"Đã lưu video_features_df vào: {video_features_path} (Shape: {video_features_df.shape})")

    if not user_features_df.empty:
        user_features_path = os.path.join(OUTPUT_DIR, "user_features_limited.parquet")
        user_features_df.to_parquet(user_features_path, index=False)
        print(f"Đã lưu user_features_df vào: {user_features_path} (Shape: {user_features_df.shape})")
    
    if not user_channel_interactions_df.empty:
        user_channel_interactions_path = os.path.join(OUTPUT_DIR, "user_channel_interactions_limited.parquet")
        user_channel_interactions_df.to_parquet(user_channel_interactions_path, index=False)
        print(f"Đã lưu user_channel_interactions_df vào: {user_channel_interactions_path} (Shape: {user_channel_interactions_df.shape})")


    print(f"\n--- Hoàn thành Pipeline Thu thập Dữ liệu (v3 - LIMIT 1000 from DB) - {datetime.now()} ---")



if __name__ == "__main__":
    os.makedirs("config", exist_ok=True)
    os.makedirs("output_data", exist_ok=True)
    os.makedirs("data_collection", exist_ok=True)
    os.makedirs("data_processing", exist_ok=True)

    # Tạo file __init__.py để các thư mục được coi là package
    open("data_collection/__init__.py", 'a').close()
    open("data_processing/__init__.py", 'a').close()

    # Tạo file config/db_config.ini mẫu nếu chưa có
    # if not os.path.exists('config/db_config.ini'):
    #     with open('config/db_config.ini', 'w') as f:
    #         f.write("[mysql_prod]\nhost = localhost\nuser = root\npassword = yourpassword\ndatabase = yourdb\nport = 3306\n\n")
    #         f.write("[mongodb_prod]\nconnection_string = mongodb://localhost:27017/\ndatabase = yourmongod\n")
    
    # Tạo file config/log_config.ini mẫu (đã có trong log_processor.py, nhưng đảm bảo nó tồn tại)
    # if not os.path.exists('config/log_config.ini'):
    #      with open('config/log_config.ini', 'w') as f:
    #         f.write("[log_processing]\n")
    #         # QUAN TRỌNG: Thay thế bằng đường dẫn thực tế đến thư mục log của bạn
    #         # Hoặc tạo thư mục sample_logs và file log mẫu như trong log_processor.py để test
    #         f.write("log_directory = ./sample_logs/\n") 
    #         f.write("log_file_pattern = *.log\n")
    #         f.write("[lcdr_fields_index]\n")
    #         f.write("dt_server_log = 0\nmsisdn = 2\nclient_type = 3\nrevision = 4\nnetwork_type = 5\n")
    #         f.write("video_id = 6\nstate = 7\ntime_log_client = 8\nwatch_array = 10\naverage_lag = 12\n")
    #         f.write("average_watch_segment = 13\nip_address = 15\nuser_agent = 16\nerror_desc = 20\n")
    #         f.write("volume = 21\ndomain = 22\nbandwidth_array = 23\nnetwork_array = 24\nvideo_duration_ms = 25\n")

    # Tạo thư mục sample_logs và file log mẫu nếu chưa có để test log_processor
    # sample_log_dir_main = './sample_logs/'
    # os.makedirs(sample_log_dir_main, exist_ok=True)
    # sample_log_file_main = os.path.join(sample_log_dir_main, 'video_view_main_test.log')
    # if not os.path.exists(sample_log_file_main):
    #     with open(sample_log_file_main, 'w') as f:
    #         f.write("2025-05-20 18:51:26|APP|+67076797570|ANDROID|16167|WIFI|57075733|END|2517,14337,1,0,0,0,1|IGNORE_LAG|14337:0|A|0|14337|XXX|192.168.1.202|UserAgentString|mediaLink|pageLink|E||0.1|domain.com|BW_ARR|NW_ARR|60000\n")
    #         f.write("2025-05-21 10:00:00|APP|+67011111111|IOS|17000|4G|57075735|END|1000,60000,0,0,0,0,0|IGNORE_LAG|0-30000,35000-60000|A|0|27500|XXX|10.0.0.1|UserAgentStringIOS|mediaLink3|pageLink3|E||0.5|domain.com|BW_ARR3|NW_ARR3|90000\n")
    
    main()
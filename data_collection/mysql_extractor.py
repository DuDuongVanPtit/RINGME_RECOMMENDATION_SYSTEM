import mysql.connector
import pandas as pd
import configparser
import os
from datetime import datetime

def get_mysql_connection(config_path='config/db_config.ini', section='mysql_prod_videodb'):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"File cấu hình không tìm thấy: {config_path}")
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    if section not in config:
        raise ValueError(f"Section [{section}] không có trong file cấu hình: {config_path}")
        
    db_config = config[section]
    try:
        conn = mysql.connector.connect(
            host=db_config.get('host'),
            user=db_config.get('user'),
            password=db_config.get('password'),
            database=db_config.get('database'),
            port=db_config.get('port')
        )
        print(f"Đã kết nối thành công đến MySQL database: {db_config.get('database')} (section: {section})")
        return conn
    except mysql.connector.Error as err:
        print(f"Lỗi kết nối MySQL cho section [{section}]: {err}")
        return None

def fetch_video_items(conn, limit=None):
    if not conn:
        return pd.DataFrame()
    
    query = """
        SELECT 
            id AS video_id, 
            cate_id, 
            channel_id, 
            video_title, 
            video_desc,
            video_time AS video_duration_seconds, 
            publish_time,
            created_at AS video_created_at,
            total_views,
            total_likes,
            total_shares,
            total_comments,
            total_saves,
            is_short,
            is_hot,
            is_new,
            is_paid
        FROM vcs_video_item
        WHERE actived = 10
    """
    if limit:
        query += f" LIMIT {limit}"
        
    try:
        df = pd.read_sql(query, conn)
        print(f"Đã trích xuất {len(df)} bản ghi từ vsc_video_item.")
        return df
    except Exception as e:
        print(f"Lỗi khi trích xuất vsc_video_item: {e}")
        return pd.DataFrame()

def fetch_channel_info(conn, limit=None):
    if not conn:
        return pd.DataFrame()
        
    query = """
        SELECT 
            id AS channel_id, 
            msisdn AS channel_msisdn, 
            channel_name, 
            description AS channel_description,
            num_follows,
            num_videos,
            num_likes AS channel_total_likes,
            num_views AS channel_total_views,
            is_official,
            created_at AS channel_created_at
        FROM vcs_channel
        WHERE actived = 10
    """
    if limit:
        query += f" LIMIT {limit}"
        
    try:
        df = pd.read_sql(query, conn)
        print(f"Đã trích xuất {len(df)} bản ghi từ vcs_channel.")
        return df
    except Exception as e:
        print(f"Lỗi khi trích xuất vcs_channel: {e}")
        return pd.DataFrame()
    
def fetch_user_demographics(conn_dbcms, limit=None):
    if not conn_dbcms:
        return pd.DataFrame()
    users_table = 'users'
    username_col = 'username' 
    birthday_col = 'birthday'
    gender_col = 'gender'

    query = f"""
        SELECT 
            {username_col} AS user_key, 
            {birthday_col} AS birthday, 
            {gender_col} AS gender 
        FROM {users_table}
    """
    limit = 1000000
    if limit:
        query += f" LIMIT {limit}"
        
    try:
        df = pd.read_sql(query, conn_dbcms)
        print(f"Đã trích xuất {len(df)} bản ghi thông tin người dùng từ {users_table} (dbcms).")
        
        if df.empty:
            return pd.DataFrame(columns=['user_key', 'age', 'gender'])

        df['user_key'] = df['user_key'].astype(str)
        
        def calculate_age(born):
            if pd.isna(born):
                return None
            try:
                today = datetime.today()
                return today.year - born.year - ((today.month, today.day) < (born.month, born.day))
            except Exception as e:
                print("Exception:", e)
                return None

        df['birthday'] = pd.to_datetime(df['birthday'], errors='coerce')
        df['age'] = df['birthday'].apply(calculate_age)

        def map_gender(gender_int):
            if pd.isna(gender_int):
                return "Không xác định"
            if gender_int == 0: return "Nam" 
            elif gender_int == 1: return "Nữ"
            elif gender_int == 2: return "Khác"
            else: return "Không xác định"
                
        df['gender_str'] = df['gender'].apply(map_gender)
        df_output = df[['user_key', 'age', 'gender_str']].copy()
        df_output.rename(columns={'gender_str': 'gender'}, inplace=True)
        
        return df_output
        
    except Exception as e:
        print(f"Lỗi khi trích xuất thông tin người dùng từ {users_table} (dbcms): {e}")
        return pd.DataFrame(columns=['user_key', 'age', 'gender'])

if __name__ == '__main__':
    mysql_conn_videodb = get_mysql_connection(section='mysql_prod_videodb')
    if mysql_conn_videodb:
        video_df = fetch_video_items(mysql_conn_videodb, limit=2)
        print("\nVideo Items Sample (videodb):\n", video_df.head())
        channel_df = fetch_channel_info(mysql_conn_videodb, limit=2)
        print("\nChannel Info Sample (videodb):\n", channel_df.head())
        mysql_conn_videodb.close()
        print("Đã đóng kết nối MySQL (videodb).")

    mysql_conn_dbcms = get_mysql_connection(section='mysql_prod_dbcms')
    if mysql_conn_dbcms:
        user_demo_df = fetch_user_demographics(mysql_conn_dbcms, limit=2)
        print("\nUser Demographics (dbcms):\n", user_demo_df.head())
        mysql_conn_dbcms.close()
        print("Đã đóng kết nối MySQL (dbcms).")

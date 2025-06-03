from pymongo import MongoClient
import pandas as pd
import configparser
import os
from datetime import datetime

def get_mongodb_client(config_path='config/db_config.ini', section='mongodb_prod'):
    """Thiết lập kết nối đến MongoDB."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"File cấu hình không tìm thấy: {config_path}")

    config = configparser.ConfigParser()
    config.read(config_path)

    if section not in config:
        raise ValueError(f"Section [{section}] không có trong file cấu hình.")

    db_config = config[section]
    try:
        client = MongoClient(db_config.get('connection_string'))
        db = client[db_config.get('database')]
        print(f"Đã kết nối thành công đến MongoDB database: {db_config.get('database')}")
        return db
    except Exception as e:
        print(f"Lỗi kết nối MongoDB: {e}")
        return None

def fetch_video_likes(db, limit=None):
    """Trích xuất dữ liệu lượt thích video."""
    if db is None:
        return pd.DataFrame()
    
    collection = db['vcs_video_liked']  
    query_filter = {} 
    
    cursor = collection.find(query_filter)
    if limit:
        cursor = cursor.limit(limit)
        
    likes_data = []
    print(f"\n--- Đang kiểm tra collection: {collection.name} ---")  
    doc_count = 0
    for doc in cursor:
        if doc_count < 5:  
            print(f"Document thô {doc_count + 1}: {doc}")
        doc_count += 1
 
        liked_time_raw = doc.get('likedTime') 
        liked_timestamp = None
        if isinstance(liked_time_raw, dict) and '$numberLong' in liked_time_raw:
            try:
                liked_timestamp = pd.to_datetime(int(liked_time_raw['$numberLong']), unit='ms', errors='coerce')
            except ValueError:
                pass 
        elif isinstance(liked_time_raw, dict) and '$date' in liked_time_raw:
            liked_timestamp = pd.to_datetime(liked_time_raw['$date'], errors='coerce')
        elif isinstance(liked_time_raw, (int, float)): 
            liked_timestamp = pd.to_datetime(liked_time_raw, unit='ms', errors='coerce')

        likes_data.append({
            'user_id': doc.get('msisdn'),
            'video_id': str(doc.get('videoId')), 
            'interaction_type': 'like',
            'timestamp': liked_timestamp
        })
    
    if doc_count == 0:
        print(f"!!! Không tìm thấy document nào trong collection {collection.name} với filter {query_filter}")

    df = pd.DataFrame(likes_data)
    print(f"Đã trích xuất {len(df)} lượt thích video.")
    return df

def fetch_channel_follows(db, limit=None):
    """Trích xuất dữ liệu theo dõi kênh."""
    if db is None:
        return pd.DataFrame()
        
    collection = db['vcs_channel_following']
    query_filter = {}
    
    cursor = collection.find(query_filter)
    if limit:
        cursor = cursor.limit(limit)
        
    follows_data = []
    for doc in cursor:
        follow_at_raw = doc.get('followAt')
        follow_timestamp = pd.to_datetime(follow_at_raw, errors='coerce')  

        follows_data.append({
            'user_id': doc.get('msisdn'),
            'channel_id': str(doc.get('channelId')),
            'interaction_type': 'follow_channel',
            'timestamp': follow_timestamp
        })

    df = pd.DataFrame(follows_data)
    print(f"Đã trích xuất {len(df)} lượt theo dõi kênh.")
    return df

def parse_timestamp(raw):
    """Chuyển đổi trường thời gian từ MongoDB thành datetime."""
    if isinstance(raw, dict) and '$date' in raw:
        return pd.to_datetime(raw['$date'], errors='coerce')
    return pd.to_datetime(raw, errors='coerce') if raw else None

def fetch_comments(db, limit=None):
    if db is None:
        return pd.DataFrame()

    comments_data = []

    # Lấy comment gốc
    for doc in db['vcs_comment_info'].find({}).limit(limit if limit else 0):
        comment_timestamp = parse_timestamp(doc.get('commentAt') or doc.get('serverTime'))

        comments_data.append({
            'user_id': doc.get('msisdn'),
            'video_id': str(doc.get('contentId')),
            'interaction_type': 'comment',
            'timestamp': comment_timestamp,
            'comment_content': doc.get('content')
        })

    # Lấy comment reply
    for doc in db['vcs_comment_reply'].find({}).limit(limit if limit else 0):
        comment_timestamp = parse_timestamp(doc.get('commentAt') or doc.get('serverTime'))

        comments_data.append({
            'user_id': doc.get('msisdn'),
            'video_id': str(doc.get('contentId')),
            'interaction_type': 'reply_comment',
            'timestamp': comment_timestamp,
            'comment_content': doc.get('content')
        })

    df = pd.DataFrame(comments_data)
    print(f"Đã trích xuất {len(df)} bình luận và trả lời.")
    return df

if __name__ == '__main__':
    # Test thử
    mongo_db = get_mongodb_client()
    if mongo_db is not None:
        likes_df = fetch_video_likes(mongo_db, limit=5)
        print("\nVideo Likes Sample:\n", likes_df.head())
        
        follows_df = fetch_channel_follows(mongo_db, limit=5)
        print("\nChannel Follows Sample:\n", follows_df.head())
        
        comments_df = fetch_comments(mongo_db, limit=10)
        print("\nComments Sample:\n", comments_df.head())
        
        # mongo_db.client.close()
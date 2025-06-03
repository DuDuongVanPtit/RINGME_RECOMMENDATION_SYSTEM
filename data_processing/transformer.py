import pandas as pd
from datetime import datetime

def create_interaction_strength(row, watch_weight=5, like_weight=3, comment_weight=2, follow_weight=4):
    strength = 0
    if row['interaction_type'] == 'watch':
        if 'watch_percentage' in row and pd.notna(row['watch_percentage']):
            strength = (row['watch_percentage'] / 100.0) * watch_weight
        elif 'watch_time_seconds' in row and pd.notna(row['watch_time_seconds']):
            if row['watch_time_seconds'] > 60: strength = watch_weight
            elif row['watch_time_seconds'] > 30: strength = watch_weight * 0.6
            elif row['watch_time_seconds'] > 10: strength = watch_weight * 0.3
            else: strength = watch_weight * 0.1
    elif row['interaction_type'] == 'like':
        strength = like_weight
    elif row['interaction_type'] == 'comment' or row['interaction_type'] == 'reply_comment':
        strength = comment_weight
    elif row['interaction_type'] == 'follow_channel':
        strength = follow_weight
    return round(strength, 2)

def transform_data(watch_log_df, likes_df, comments_df, follows_df, 
                   video_items_df, channel_info_df, user_demographics_df):
    print("\n--- Bắt đầu quá trình Transform Data ---")

    interactions_list = []
    if not watch_log_df.empty:
        watch_log_df_copy = watch_log_df.copy()
        watch_log_df_copy = watch_log_df_copy[['user_id', 'video_id', 'interaction_type', 'timestamp', 'watch_time_seconds', 'watch_percentage']]
        interactions_list.append(watch_log_df_copy)
    if not likes_df.empty:
        likes_df_copy = likes_df.copy()
        likes_df_copy = likes_df_copy[['user_id', 'video_id', 'interaction_type', 'timestamp']]
        interactions_list.append(likes_df_copy)
    if not comments_df.empty:
        comments_df_copy = comments_df.copy()
        comments_df_copy = comments_df_copy[['user_id', 'video_id', 'interaction_type', 'timestamp']]
        interactions_list.append(comments_df_copy)

    if not interactions_list:
        print("Không có dữ liệu tương tác video nào để xử lý.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    interactions_df = pd.concat(interactions_list, ignore_index=True)
    interactions_df['user_id'] = interactions_df['user_id'].astype(str).str.replace(r'^\+', '', regex=True)
    interactions_df['video_id'] = interactions_df['video_id'].astype(str)
    interactions_df['interaction_strength'] = interactions_df.apply(create_interaction_strength, axis=1)
    interactions_df = interactions_df[interactions_df['interaction_strength'] > 0]
    interactions_df = interactions_df.dropna(subset=['timestamp'])
    interactions_df = interactions_df.sort_values(by=['user_id', 'timestamp'])
    print(f"Tổng số tương tác video hợp lệ: {len(interactions_df)}")

    user_channel_interactions_df = pd.DataFrame()
    if not follows_df.empty:
        user_channel_interactions_df = follows_df.copy()
        user_channel_interactions_df['user_id'] = user_channel_interactions_df['user_id'].astype(str).str.replace(r'^\+', '', regex=True)
        user_channel_interactions_df['channel_id'] = user_channel_interactions_df['channel_id'].astype(str)
        user_channel_interactions_df['interaction_strength'] = user_channel_interactions_df.apply(create_interaction_strength, axis=1)
        user_channel_interactions_df = user_channel_interactions_df[user_channel_interactions_df['interaction_strength'] > 0]
        user_channel_interactions_df = user_channel_interactions_df.dropna(subset=['timestamp'])
        print(f"Số lượng tương tác theo dõi kênh hợp lệ: {len(user_channel_interactions_df)}")

    if not video_items_df.empty and not channel_info_df.empty:
        video_features_df = pd.merge(video_items_df, channel_info_df, on='channel_id', how='left', suffixes=('', '_channel'))
        video_features_df['video_id'] = video_features_df['video_id'].astype(str)
    elif not video_items_df.empty:
        video_features_df = video_items_df.copy()
        video_features_df['video_id'] = video_features_df['video_id'].astype(str)
    else:
        video_features_df = pd.DataFrame()

    if not interactions_df.empty:
        unique_user_ids_from_interactions = interactions_df['user_id'].unique()
        user_features_df = pd.DataFrame({'user_id': unique_user_ids_from_interactions})
        if not user_demographics_df.empty:
            user_demographics_df_copy = user_demographics_df.copy()
            if 'user_key' in user_demographics_df_copy.columns:
                user_demographics_df_copy['user_key'] = user_demographics_df_copy['user_key'].astype(str).str.replace(r'^\+', '', regex=True)
            user_features_df = pd.merge(user_features_df, 
                                        user_demographics_df_copy[['user_key', 'age', 'gender']], 
                                        left_on='user_id', 
                                        right_on='user_key', 
                                        how='left')
            if 'user_key' in user_features_df.columns:
                user_features_df = user_features_df.drop(columns=['user_key'])
            print(f"Đã kết hợp user_features_df với user_demographics_df. Shape mới: {user_features_df.shape}")
            if 'age' not in user_features_df.columns:
                print("CẢNH BÁO: Merge user demographics có thể không thành công.")
        else:
            print("Không có dữ liệu user_demographics_df.")
            user_features_df['age'] = None
            user_features_df['gender'] = None
    else:
        user_features_df = pd.DataFrame(columns=['user_id', 'age', 'gender'])
        print("Không có dữ liệu interactions_df để tạo user_features_df.")
        
    print("--- Hoàn thành Transform Data ---")
    return interactions_df, video_features_df, user_features_df, user_channel_interactions_df

if __name__ == '__main__':
    sample_watch_log = pd.DataFrame({
        'user_id': ['+u1', 'u1', 'u2', 'u3'], 'video_id': ['v1', 'v2', 'v1', 'v3'],
        'interaction_type': ['watch', 'watch', 'watch', 'watch'],
        'timestamp': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-03']),
        'watch_time_seconds': [100, 20, 10, 150], 'watch_percentage': [80.0, 20.0, 9.0, 95.0]
    })
    sample_likes = pd.DataFrame({
        'user_id': ['u1'], 'video_id': ['v1'], 'interaction_type': ['like'],
        'timestamp': pd.to_datetime(['2023-01-01'])
    })
    sample_comments = pd.DataFrame() 
    sample_follows = pd.DataFrame()
    sample_videos = pd.DataFrame({
        'video_id': ['v1', 'v2', 'v3'], 'cate_id': [101, 102, 101], 'channel_id': ['c1', 'c1', 'c2'],
        'video_duration_seconds': [120, 100, 160]
    })
    sample_channels = pd.DataFrame({
        'channel_id': ['c1', 'c2'], 'channel_name': ['Channel A', 'Channel B']
    })
    sample_user_demographics = pd.DataFrame({
        'user_key': ['u1', 'u2', 'u4_not_in_interactions', '+u3'],
        'age': [25, 32, 40, 28],
        'gender': ['Nam', 'Nữ', 'Nam', 'Nữ']
    })

    interactions, videos, users, _ = transform_data(
        sample_watch_log, sample_likes, sample_comments, sample_follows,
        sample_videos, sample_channels, sample_user_demographics
    )
    print("\nTransformed Interactions:\n", interactions)
    print("\nTransformed Video Features:\n", videos.head())
    print("\nTransformed User Features:\n", users)

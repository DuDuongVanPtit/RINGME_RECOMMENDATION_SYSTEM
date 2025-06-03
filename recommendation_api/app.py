from flask import Flask, request, jsonify
import joblib
import numpy as np
import pandas as pd
from lightfm import LightFM
from lightfm.data import Dataset

app = Flask(__name__)

MODEL_FILENAME = 'lightfm_model.joblib'
DATASET_FILENAME = 'lightfm_dataset.joblib'
INTERACTIONS_FILENAME = 'interactions_agg_df.joblib'
USER_FEATURES_FILENAME = 'user_features_matrix.joblib'
ITEM_FEATURES_FILENAME = 'item_features_matrix.joblib'

model = None
dataset = None
interactions_agg_df = None
user_features_matrix = None
item_features_matrix = None
user_id_map = None
internal_to_video_id_map = None

def load_artifacts():
    global model, dataset, interactions_agg_df, user_features_matrix, item_features_matrix
    global user_id_map, internal_to_video_id_map

    try:
        print(f"Đang tải model từ '{MODEL_FILENAME}'...")
        model = joblib.load(MODEL_FILENAME)
        print("Tải model thành công.")

        print(f"Đang tải dataset từ '{DATASET_FILENAME}'...")
        dataset = joblib.load(DATASET_FILENAME)
        raw_user_id_map, _, raw_item_id_map, _ = dataset.mapping()
        user_id_map = raw_user_id_map
        internal_to_video_id_map = {v: k for k, v in raw_item_id_map.items()}
        print("Tải dataset và tạo ánh xạ ID thành công.")

        print(f"Đang tải interactions_agg_df từ '{INTERACTIONS_FILENAME}'...")
        interactions_agg_df = joblib.load(INTERACTIONS_FILENAME)
        interactions_agg_df['user_id'] = interactions_agg_df['user_id'].astype(str)
        interactions_agg_df['video_id'] = interactions_agg_df['video_id'].astype(str)
        print("Tải interactions_agg_df thành công.")

        try:
            print(f"Đang tải user_features_matrix từ '{USER_FEATURES_FILENAME}'...")
            user_features_matrix = joblib.load(USER_FEATURES_FILENAME)
            print("Tải user_features_matrix thành công.")
        except FileNotFoundError:
            print(f"Cảnh báo: Không tìm thấy tệp '{USER_FEATURES_FILENAME}'.")
            user_features_matrix = None

        try:
            print(f"Đang tải item_features_matrix từ '{ITEM_FEATURES_FILENAME}'...")
            item_features_matrix = joblib.load(ITEM_FEATURES_FILENAME)
            print("Tải item_features_matrix thành công.")
        except FileNotFoundError:
            print(f"Cảnh báo: Không tìm thấy tệp '{ITEM_FEATURES_FILENAME}'.")
            item_features_matrix = None

        print("Tất cả các artifact đã được tải và chuẩn bị!")
        return True

    except FileNotFoundError as e:
        print(f"LỖI KHỞI TẠO: Không tìm thấy tệp: {e}.")
        return False
    except Exception as e:
        print(f"LỖI KHỞI TẠO: {e}")
        return False

app_initialized_successfully = load_artifacts()

def get_recommendations_for_api_call(original_user_id_str, num_recommendations=10):
    if not app_initialized_successfully:
        return {"error": "Hệ thống gợi ý chưa được khởi tạo thành công."}

    if original_user_id_str not in user_id_map:
        return {"error": f"Người dùng '{original_user_id_str}' không tồn tại trong dữ liệu."}

    internal_user_id = user_id_map[original_user_id_str]
    n_items = dataset.interactions_shape()[1]

    user_interactions = interactions_agg_df[interactions_agg_df['user_id'] == original_user_id_str]
    known_positives_original_ids = set(user_interactions['video_id'].tolist())

    all_internal_item_ids = np.arange(n_items)

    scores = model.predict(
        internal_user_id,
        all_internal_item_ids,
        user_features=user_features_matrix,
        item_features=item_features_matrix,
        num_threads=1
    )

    sorted_internal_item_ids = np.argsort(-scores)

    recommendations = []
    for internal_item_id in sorted_internal_item_ids:
        original_video_id = internal_to_video_id_map.get(internal_item_id)
        if original_video_id and original_video_id not in known_positives_original_ids:
            recommendations.append(original_video_id)
            if len(recommendations) >= num_recommendations:
                break

    return {"user_id": original_user_id_str, "recommendations": recommendations}

@app.route('/recommend', methods=['GET'])
def recommend_endpoint():
    if not app_initialized_successfully:
        return jsonify({"error": "Hệ thống gợi ý chưa sẵn sàng."}), 503

    user_id_param = request.args.get('user_id', type=str)
    try:
        k_param = request.args.get('k', default=10, type=int)
        if k_param <= 0 or k_param > 100:
             k_param = 10 
    except ValueError:
        k_param = 10

    if not user_id_param:
        return jsonify({"error": "Tham số 'user_id' là bắt buộc."}), 400

    result = get_recommendations_for_api_call(user_id_param, k_param)

    if "error" in result:
        if "không tồn tại" in result["error"]:
             return jsonify(result), 404
        else:
             return jsonify(result), 500

    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

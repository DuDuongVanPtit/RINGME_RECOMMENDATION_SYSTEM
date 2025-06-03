import os
import glob
import pandas as pd
import configparser
from datetime import datetime
import gzip 

def parse_lcdr_line(line, field_indices):
    parts = line.strip().split('|')
    num_expected_parts = max(v for k, v in field_indices.items() if isinstance(v, int)) + 1
    if len(parts) < num_expected_parts: return None
    try:
        dt_server_log_str = parts[field_indices['dt_server_log']]
        msisdn = parts[field_indices['msisdn']]
        video_id_str = parts[field_indices['video_id']]
        state = parts[field_indices['state']]
        time_log_client_str = parts[field_indices['time_log_client']]
        watch_array_str = parts[field_indices['watch_array']]
        video_duration_ms_str = parts[field_indices['video_duration_ms']]

        if state != "END" or not msisdn or not video_id_str or not watch_array_str or not video_duration_ms_str or video_duration_ms_str == '0':
            return None

        interaction_timestamp = pd.to_datetime(dt_server_log_str, errors='coerce')
        if pd.isna(interaction_timestamp): return None

        video_id = int(video_id_str)
        video_duration_seconds = int(video_duration_ms_str) / 1000.0
        if video_duration_seconds <= 0: return None

        total_watch_time_ms = 0
        if ':' in watch_array_str:
            try: total_watch_time_ms = int(watch_array_str.split(':')[0])
            except ValueError: pass
        elif '-' in watch_array_str and ',' in watch_array_str:
            segments = watch_array_str.split(',')
            for segment in segments:
                if '-' in segment:
                    try:
                        start, end = map(int, segment.split('-'))
                        total_watch_time_ms += (end - start)
                    except ValueError: pass
        elif watch_array_str.isdigit():
             try: total_watch_time_ms = int(watch_array_str)
             except ValueError: pass

        if total_watch_time_ms == 0 and time_log_client_str:
            time_log_parts = time_log_client_str.split(',')
            if len(time_log_parts) > 1:
                try: total_watch_time_ms = int(time_log_parts[1])
                except ValueError: pass

        if total_watch_time_ms <= 0: return None

        watch_time_seconds = total_watch_time_ms / 1000.0
        watch_percentage = (watch_time_seconds / video_duration_seconds) * 100 if video_duration_seconds > 0 else 0

        client_type = parts[field_indices['client_type']]
        network_type = parts[field_indices['network_type']]

        return {
            'user_id': msisdn, 'video_id': video_id, 'interaction_type': 'watch',
            'watch_time_seconds': watch_time_seconds,
            'video_duration_seconds': video_duration_seconds,
            'watch_percentage': round(watch_percentage, 2),
            'timestamp': interaction_timestamp,
            'client_type': client_type, 'network_type': network_type,
        }
    except (IndexError, ValueError) as e: 
        return None
    except Exception as e:
        return None


def process_log_files(config_path='config/log_config.ini'): 
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"File cấu hình log không tìm thấy: {config_path}")

    config = configparser.ConfigParser()
    config.read(config_path)

    log_dir = config['log_processing'].get('log_directory')
    file_pattern = config['log_processing'].get('log_file_pattern') 

    field_indices = {k: int(v) for k, v in config['lcdr_fields_index'].items()}

    if not os.path.isdir(log_dir):
        print(f"Thư mục log không tồn tại: {log_dir}")
        return pd.DataFrame()

    all_log_entries = []
    log_files_to_process = glob.glob(os.path.join(log_dir, file_pattern)) 

    if not log_files_to_process:
        print(f"Không tìm thấy file log nào với pattern '{file_pattern}' trong '{log_dir}'")
        return pd.DataFrame()

    print(f"Tìm thấy {len(log_files_to_process)} file log để xử lý.")

    for log_file_path in log_files_to_process:
        print(f"Đang xử lý file: {log_file_path}")
        try:
            with gzip.open(log_file_path, 'rt', encoding='utf-8', errors='ignore') as f: 
                for line_number, line in enumerate(f):
                    parsed_entry = parse_lcdr_line(line, field_indices)
                    if parsed_entry:
                        all_log_entries.append(parsed_entry)
        except Exception as e:
            print(f"Lỗi khi đọc file {log_file_path}: {e}")

    df = pd.DataFrame(all_log_entries)
    if not df.empty:
        df['video_id'] = df['video_id'].astype(str)
    print(f"Đã xử lý xong tất cả file log. Tổng số {len(df)} bản ghi xem video hợp lệ.")
    return df


if __name__ == '__main__':
    # Tạo file log_config.ini mẫu (nếu chưa có)
    # if not os.path.exists('config/log_config.ini'):
    #     os.makedirs('config', exist_ok=True)
    #     with open('config/log_config.ini', 'w') as f:
    #         f.write("[log_processing]\n")
    #         f.write("log_directory = ./sample_logs/\n") # Thay bằng đường dẫn thực tế
    #         f.write("log_file_pattern = *.log\n")
    #         f.write("[lcdr_fields_index]\n")
    #         f.write("dt_server_log = 0\nmsisdn = 2\nclient_type = 3\nrevision = 4\nnetwork_type = 5\n")
    #         f.write("video_id = 6\nstate = 7\ntime_log_client = 8\nwatch_array = 10\naverage_lag = 12\n")
    #         f.write("average_watch_segment = 13\nip_address = 15\nuser_agent = 16\nerror_desc = 20\n")
    #         f.write("volume = 21\ndomain = 22\nbandwidth_array = 23\nnetwork_array = 24\nvideo_duration_ms = 25\n")
    
    # Tạo thư mục sample_logs và file log mẫu để test
    # sample_log_dir = './sample_logs/'
    # os.makedirs(sample_log_dir, exist_ok=True)
    # sample_log_file = os.path.join(sample_log_dir, 'video_view_test.log')
    # with open(sample_log_file, 'w') as f:
    #     f.write("2025-05-20 18:51:26|APP|+67076797570|ANDROID|16167|WIFI|57075733|END|2517,14337,1,0,0,0,1|IGNORE_LAG|14337:0|A|0|14337|XXX|192.168.1.202|UserAgentString|mediaLink|pageLink|E||0.1|domain.com|BW_ARR|NW_ARR|60000\n")
    #     f.write("2025-05-20 18:52:00|APP|+67076797571|IOS|16168|4G|57075734|END|3000,30000,0,0,0,0,0|IGNORE_LAG|0-15000,16000-30000|A|0|15000|XXX|192.168.1.203|UserAgentString2|mediaLink2|pageLink2|E|SomeError|0.2|domain.com|BW_ARR2|NW_ARR2|120000\n")
    #     f.write("2025-05-20 18:53:00|APP|+67076797570|ANDROID|16167|WIFI|57075734|PLAYING|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|120000\n") # Sẽ bị bỏ qua vì STATE != END
    #     f.write("2025-05-20 18:53:00|APP|+67076797570|ANDROID|16167|WIFI|57075734|END|...|...||A|0|0|XXX|...|...|...|...|E||...|...|...|...|0\n") # Sẽ bị bỏ qua vì video_duration_ms = 0

    watch_log_df = process_log_files(config_path='config/log_config.ini') 
    print("\nProcessed Watch Log Sample:\n", watch_log_df.head())
from pathlib import Path
import shutil
import pandas as pd



def split_yoochoose_clicks(
    source_path: Path,
    output_dir: Path,
    chunk_size: int = 250000,
    max_chunks: int = None
):
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    col_names = [
        "session_id",
        "event_timestamp",
        "item_id",
        "category"
    ]
    total_rows = 0
    chunk_count = 0

    reader = pd.read_csv(
        source_path,
        header=None,
        names=col_names,
        chunksize=chunk_size,
        dtype=str
    )

    for chunk in reader: 
        chunk_count+=1 

        if(len(chunk.columns) != 4):
            raise ValueError(f"Lỗi Schema: Chunk {chunk_count} có {len(chunk.columns)} cột (Kỳ vọng: 4)")
        if(chunk.empty):
            raise ValueError(f"Lỗi Dữ liệu: Chunk {chunk_count} bị rỗng")
        out_filename = f"yoochoose_clicks_part_{chunk_count:06d}.dat"
        out_path = output_dir / out_filename

        chunk.to_csv(out_path, index = False, header = False)

        current_rows = len(chunk)
        total_rows += current_rows

        print(f"Đã ghi {out_filename} | Rows: {current_rows:,} | Cumulative: {total_rows:,}")

        if max_chunks and chunk_count >= max_chunks:
            print(f"Đã đạt tới giới hạn test {max_chunks} chunks, dừng quá trình split")
            break

    if not max_chunks:
        assert total_rows == 33_003_944, f"Lỗi toàn vẹn dữ liệu: Tổng rows = {total_rows} (Kỳ vọng: 33,003,944)"   

    print(f"Hoàn tất tách file\nTổng files: {chunk_count}\nTổng row: {total_rows:,}") 


if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    SOURCE_PATH = PROJECT_ROOT / "data" / "raw" / "yoochoose" / "yoochoose-clicks.dat"
    OUTPUT_DIR = PROJECT_ROOT / "data" / "replay" / "yoochoose" / "chunks"

    if not SOURCE_PATH.exists():
        raise FileNotFoundError(f"Không tìm thấy source file: {SOURCE_PATH}")

    #Chạy chế độ TEST trước (4 chunks = 1,000,000 rows)
    split_yoochoose_clicks(
        source_path=SOURCE_PATH,
        output_dir=OUTPUT_DIR,
        chunk_size=250_000,
        max_chunks=None
    )
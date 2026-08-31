from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy import Connection


from fastorder.ingestion.incremental.table_config import (
    IncrementalTableConfig,
    ORDERS_CONFIG,
)

from fastorder.ingestion.incremental.table_extractor import (
    extract_table_batch,
    get_upper_watermark,
)


from fastorder.ingestion.incremental.bronze_writer import write_bronze_batch
from fastorder.ingestion.incremental.checkpoint_manager import save_checkpoint_atomic, load_checkpoint
from fastorder.ingestion.incremental.pending_batch_manager import (
    build_pending_batch_context,
    save_pending_batch_context_atomic,
    delete_pending_batch_context,
    load_pending_batch_context
)

def _extract_batch_number(extraction_id: str) -> int:
    marker = "_batch_"
    if marker not in extraction_id:
        raise RuntimeError(f"extraction_id sai dinh dang: {extraction_id}")
    
    suffix = extraction_id.rsplit(marker, 1)[-1]
    
    try:
        batch_number = int(suffix)
    except ValueError as error:
        raise RuntimeError(f"Khong doc duoc batch number tu: {extraction_id}") from error

    if batch_number <= 0:
        raise RuntimeError("Batch number trong extraction_id phai lon hon 0.")

    return batch_number

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def _watermark_key(
    watermark: Dict[str, Any],
    config: IncrementalTableConfig,
) -> tuple:

    timestamp_value = datetime.strptime(
        watermark[
            config.watermark_column
        ],
        TIMESTAMP_FORMAT,
    )

    return (
        timestamp_value,
        *(
            watermark[column]
            for column
            in config.primary_key_columns
        ),
    )




def run_table_incremental_ingestion(
    conn: Connection,
    config: IncrementalTableConfig,
    checkpoint_path: Path,
    pending_context_path: Path,
    batch_size: int,
    run_id: str,
    run_started_at: datetime,
    run_upper_watermark: Optional[
        Dict[str, Any]
    ] = None,
) -> Dict[str, Any]:

    if (
        not isinstance(batch_size, int)
        or isinstance(batch_size, bool)
        or batch_size <= 0
    ):
        raise ValueError(
            "batch_size phai la so nguyen > 0."
        )

    if not run_id:
        raise ValueError(
            "run_id khong duoc de trong."
        )

    if not isinstance(
        run_started_at,
        datetime,
    ):
        raise ValueError(
            "run_started_at phai la datetime object."
        )

    table_name = config.table_name

    checkpoint = load_checkpoint(
        checkpoint_path,
        table_name,
    )

    current_lower = (
        checkpoint["watermark"]
    )

    pending_context = (
        load_pending_batch_context(
            pending_context_path,
            table_name,
        )
    )

    effective_run_id = run_id
    effective_run_started_at = (
        run_started_at
    )
    effective_batch_size = batch_size


    if pending_context:

        effective_run_id = (
            pending_context["run_id"]
        )

        effective_run_started_at = (
            datetime.strptime(
                pending_context[
                    "ingested_at"
                ],
                TIMESTAMP_FORMAT,
            )
        )

        effective_batch_size = (
            pending_context["batch_size"]
        )

        run_upper_watermark = (
            pending_context[
                "run_upper_watermark"
            ]
        )

    else:

        if run_upper_watermark is None:

            run_upper_watermark = (
                get_upper_watermark(
                    conn=conn,
                    config=config,
                )
            )

        if not run_upper_watermark:
            return {
                "status": "source_empty",
                "run_id": effective_run_id,
                "batches_committed": 0,
                "records_written": 0,
                "final_watermark":
                    current_lower,
                "output_paths": [],
            }

    curr_key = _watermark_key(
        current_lower,
        config,
    )

    upper_key = _watermark_key(
        run_upper_watermark,
        config,
    )

    next_batch_number = 1

    batches_committed_this_invocation = 0

    total_records = 0

    output_paths = []


    if pending_context:

        p_lower_key = _watermark_key(
            pending_context[
                "lower_watermark"
            ],
            config,
        )

        p_upper_key = _watermark_key(
            pending_context[
                "batch_upper_watermark"
            ],
            config,
        )

        if curr_key == p_lower_key:

            result = process_one_table_batch(
                conn=conn,
                config=config,

                lower_watermark=
                    pending_context[
                        "lower_watermark"
                    ],

                run_upper_watermark=
                    run_upper_watermark,

                batch_size=
                    effective_batch_size,

                checkpoint_path=
                    checkpoint_path,

                pending_context_path=
                    pending_context_path,

                run_id=
                    effective_run_id,

                extraction_id=
                    pending_context[
                        "extraction_id"
                    ],

                ingested_at=
                    effective_run_started_at,

                extraction_upper_watermark=
                    pending_context[
                        "batch_upper_watermark"
                    ],
            )

            if (
                result["status"]
                == "completed"
            ):
                raise RuntimeError(
                    "Batch phuc hoi tra ve rong. "
                    "Du lieu nguon co the da "
                    "bi thay doi bat thuong."
                )

            if (
                result["next_watermark"]
                != pending_context[
                    "batch_upper_watermark"
                ]
            ):
                raise RuntimeError(
                    "Batch phuc hoi khong "
                    "ket thuc dung batch upper "
                    "da luu."
                )

            current_lower = (
                result["next_watermark"]
            )

            curr_key = _watermark_key(
                current_lower,
                config,
            )

            total_records += (
                result["records_written"]
            )

            output_paths.append(
                result["output_path"]
            )

            batches_committed_this_invocation += 1

            next_batch_number = (
                _extract_batch_number(
                    pending_context[
                        "extraction_id"
                    ]
                )
                + 1
            )

        elif curr_key == p_upper_key:

            delete_pending_batch_context(
                pending_context_path
            )

            next_batch_number = (
                _extract_batch_number(
                    pending_context[
                        "extraction_id"
                    ]
                )
                + 1
            )

        else:
            raise RuntimeError(
                "Trang thai mau thuan: "
                f"Checkpoint {current_lower} "
                "khong khop voi "
                "Pending Context."
            )

    if (
        curr_key == upper_key
        and total_records == 0
    ):
        return {
            "status":
                "no_new_data",

            "run_id":
                effective_run_id,

            "batches_committed":
                0,

            "records_written":
                0,

            "final_watermark":
                current_lower,

            "output_paths":
                [],
        }

    if curr_key > upper_key:
        raise RuntimeError(
            "Trang thai khong hop le: "
            f"Lower watermark "
            f"({current_lower}) "
            "lon hon Upper watermark "
            f"({run_upper_watermark})."
        )

    while curr_key < upper_key:

        extraction_id = (
            f"{effective_run_id}"
            f"_batch_"
            f"{next_batch_number:06d}"
        )

        result = process_one_table_batch(
            conn=conn,
            config=config,

            lower_watermark=
                current_lower,

            run_upper_watermark=
                run_upper_watermark,

            batch_size=
                effective_batch_size,

            checkpoint_path=
                checkpoint_path,

            pending_context_path=
                pending_context_path,

            run_id=
                effective_run_id,

            extraction_id=
                extraction_id,

            ingested_at=
                effective_run_started_at,
        )

        if (
            result["status"]
            == "completed"
        ):
            raise RuntimeError(
                "Extractor tra ve batch rong "
                "du current watermark van "
                "nho hon run upper watermark. "
                "(Loi Data Anomaly)"
            )

        next_watermark = (
            result["next_watermark"]
        )

        next_key = _watermark_key(
            next_watermark,
            config,
        )

        if next_key <= curr_key:
            raise RuntimeError(
                "Runner khong tien len! "
                f"Next watermark "
                f"({next_watermark}) "
                "khong lon hon Lower "
                f"watermark hien tai "
                f"({current_lower})."
            )

        current_lower = (
            next_watermark
        )

        curr_key = next_key

        total_records += (
            result["records_written"]
        )

        output_paths.append(
            result["output_path"]
        )

        next_batch_number += 1

        batches_committed_this_invocation += 1

    return {
        "status":
            "run_completed",

        "run_id":
            effective_run_id,

        "run_upper_watermark":
            run_upper_watermark,

        "final_watermark":
            current_lower,

        "batches_committed":
            batches_committed_this_invocation,

        "records_written":
            total_records,

        "output_paths":
            output_paths,
    }

def process_one_orders_batch(
    conn: Connection,
    lower_watermark: Dict[str, Any],
    run_upper_watermark: Dict[str, Any],
    batch_size: int,
    checkpoint_path: Path,
    pending_context_path: Path,
    run_id: str,
    extraction_id: str,
    ingested_at: datetime,
    extraction_upper_watermark: Optional[
        Dict[str, Any]
    ] = None,
) -> Dict[str, Any]:

    return process_one_table_batch(
        conn=conn,
        config=ORDERS_CONFIG,
        lower_watermark=lower_watermark,
        run_upper_watermark=
            run_upper_watermark,
        batch_size=batch_size,
        checkpoint_path=checkpoint_path,
        pending_context_path=
            pending_context_path,
        run_id=run_id,
        extraction_id=extraction_id,
        ingested_at=ingested_at,
        extraction_upper_watermark=
            extraction_upper_watermark,
    )



def process_one_table_batch(
    conn: Connection,
    config: IncrementalTableConfig,
    lower_watermark: Dict[str, Any],
    run_upper_watermark: Dict[str, Any],
    batch_size: int,
    checkpoint_path: Path,
    pending_context_path: Path,
    run_id: str,
    extraction_id: str,
    ingested_at: datetime,
    extraction_upper_watermark: Optional[
        Dict[str, Any]
    ] = None,
) -> Dict[str, Any]:

    effective_extraction_upper = (
        extraction_upper_watermark
        if extraction_upper_watermark is not None
        else run_upper_watermark
    )

    batch_records, next_watermark = (
        extract_table_batch(
            conn=conn,
            config=config,
            lower_watermark=lower_watermark,
            upper_watermark=
                effective_extraction_upper,
            batch_size=batch_size,
        )
    )

    if not batch_records:
        return {
            "status": "completed",
            "records_written": 0,
            "checkpoint_updated": False,
            "pending_context_deleted": False,
        }

    pending_context = (
        build_pending_batch_context(
            table_name=config.table_name,
            run_id=run_id,
            run_upper_watermark=
                run_upper_watermark,
            lower_watermark=
                lower_watermark,
            batch_upper_watermark=
                next_watermark,
            extraction_id=extraction_id,
            ingested_at=ingested_at.strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            ),
            batch_size=batch_size,
        )
    )

    save_pending_batch_context_atomic(
        file_path=pending_context_path,
        context=pending_context,
        expected_table_name=
            config.table_name,
    )

    output_path = write_bronze_batch(
        records=batch_records,
        table_name=config.table_name,
        extraction_id=extraction_id,
        ingested_at=ingested_at,
    )

    new_checkpoint = {
        "version": 1,
        "table_name": config.table_name,
        "watermark": next_watermark,
    }

    save_checkpoint_atomic(
        checkpoint_path=checkpoint_path,
        checkpoint=new_checkpoint,
        expected_table_name=
            config.table_name,
    )

    delete_pending_batch_context(
        pending_context_path
    )

    return {
        "status": "batch_committed",
        "records_written":
            len(batch_records),
        "output_path":
            output_path,
        "next_watermark":
            next_watermark,
        "checkpoint_updated":
            True,
        "pending_context_deleted":
            True,
    }

def run_orders_incremental_ingestion(
    conn: Connection,
    checkpoint_path: Path,
    pending_context_path: Path,
    batch_size: int,
    run_id: str,
    run_started_at: datetime,
    run_upper_watermark: Optional[
        Dict[str, Any]
    ] = None,
) -> Dict[str, Any]:

    return run_table_incremental_ingestion(
        conn=conn,
        config=ORDERS_CONFIG,
        checkpoint_path=checkpoint_path,
        pending_context_path=
            pending_context_path,
        batch_size=batch_size,
        run_id=run_id,
        run_started_at=
            run_started_at,
        run_upper_watermark=
            run_upper_watermark,
    )


if __name__ == "__main__":
    import io
    import pandas as pd
    from datetime import datetime
    from pathlib import Path
    from unittest.mock import patch
    from fastorder.db.connection import get_engine

    from fastorder.ingestion.incremental.orders_extractor import (
        get_upper_watermark as get_orders_upper_watermark,
        extract_orders_batch,
    )

    from fastorder.storage.adls_client import get_bronze_file_system_client
    from fastorder.ingestion.incremental.checkpoint_manager import load_checkpoint, save_checkpoint_atomic

    print("-" * 50)
    print("BAT DAU TEST: Crash Recovery sau Checkpoint, truoc khi xoa Pending")
    print("-" * 50)

    test_checkpoint_path = Path("test_adls_crash_2_ckpt.json")
    test_pending_context_path = Path("test_adls_crash_2_pending.json")

    def _cleanup_local():
        for p in [test_checkpoint_path, test_pending_context_path, Path(f"{test_pending_context_path}.tmp")]:
            if p.exists(): p.unlink()

    _cleanup_local()
    engine = get_engine()
    fs_client = get_bronze_file_system_client()
    
    test_started_at = datetime.now()
    ingestion_date_str = test_started_at.strftime("%Y-%m-%d")

    test_run_id = (
        "test_adls_crash_2_"
        + test_started_at.strftime(
            "%Y%m%d%H%M%S%f"
        )
    )

    try:
        with engine.connect() as conn:
            full_upper = get_orders_upper_watermark(conn)
            if not full_upper:
                raise RuntimeError("Bang rong, khong du du lieu de test!")
                
            initial_lower_wm = {"updated_at": "1970-01-01T00:00:00.000000", "order_id": ""}
            
            twelve_records, test_upper = extract_orders_batch(
                conn=conn, lower_watermark=initial_lower_wm, upper_watermark=full_upper, batch_size=12
            )
            
            if len(twelve_records) != 12:
                raise RuntimeError("Smoke test can dung 12 records.")

            save_checkpoint_atomic(test_checkpoint_path, {"version": 1, "table_name": "orders", "watermark": initial_lower_wm}, "orders")

            print("\n1. Gia lap Crash sau Checkpoint, truoc khi xoa Pending (Batch 1)...")
            
            # Patch delete_pending_batch_context de tao crash
            with patch(f"{__name__}.delete_pending_batch_context", side_effect=RuntimeError("Gia lap crash truoc khi xoa pending")):
                try:
                    run_orders_incremental_ingestion(
                        conn=conn,
                        checkpoint_path=test_checkpoint_path,
                        pending_context_path=
                            test_pending_context_path,
                        batch_size=5,
                        run_id=test_run_id,
                        run_started_at=test_started_at,
                        run_upper_watermark=test_upper,
                    )
                except RuntimeError as e:
                    assert "Gia lap crash truoc khi xoa pending" in str(e)
            
            # Assertions sau crash
            saved_ckpt = load_checkpoint(test_checkpoint_path, "orders")
            assert saved_ckpt["watermark"] != initial_lower_wm, "Checkpoint phai tien len sau khi ghi thanh cong!"
            assert test_pending_context_path.exists(), "Pending context khong ton tai sau crash!"
            
            test_prefix = (
                f"orders/"
                f"ingestion_date={ingestion_date_str}/"
                f"extraction_id={test_run_id}_batch_"
            )
            paths_after_crash = list(fs_client.get_paths(path=f"orders/ingestion_date={ingestion_date_str}"))
            
            parquet_files_crash = [
                p.name for p in paths_after_crash 
                if p.name.startswith(test_prefix) and p.name.endswith("/part-000.parquet")
            ]
            
            assert len(parquet_files_crash) == 1, f"Mong doi dung 1 file tren ADLS, thuc te co {len(parquet_files_crash)}"
            print("  [PASS] He thong dinh crash, Checkpoint DA TIEN LEN, Pending thanh rac, ADLS da xuat hien Batch 1.")

            print("\n2. Bat dau chay lai de don rac va hoan tat tien trinh (Resume)...")
            recovery_result = run_orders_incremental_ingestion(
                conn=conn,
                checkpoint_path=test_checkpoint_path,
                pending_context_path=test_pending_context_path,
                batch_size=999,
                run_id=f"{test_run_id}_NEW_ID",
                run_started_at=datetime.now(),
                run_upper_watermark=test_upper
            )
            
            # Assertions sau recovery
            assert recovery_result["status"] == "run_completed"
            
            # Quan trong: Vi batch 1 (5 records) da commit hoan toan, 
            # resume chi xu ly 7 records con lai, commit 2 batch.
            assert recovery_result["records_written"] == 7
            assert recovery_result["batches_committed"] == 2
            
            assert (
                recovery_result["run_id"]
                == test_run_id
            ), (
                "He thong khong ke thua "
                "run_id tu pending"
            )
            assert not test_pending_context_path.exists(), "Pending context rac khong bi don dep sau khi hoan tat"
            
            final_ckpt = load_checkpoint(test_checkpoint_path, "orders")
            assert final_ckpt["watermark"] == test_upper, "Checkpoint cuoi cung khong dat test_upper"
            
            paths_after_recovery = list(fs_client.get_paths(path=f"orders/ingestion_date={ingestion_date_str}"))
            parquet_files_recovery = [
                p.name for p in paths_after_recovery 
                if p.name.startswith(test_prefix) and p.name.endswith("/part-000.parquet")
            ]
            
            assert len(parquet_files_recovery) == 3, f"Mong doi dung 3 files, nhung co {len(parquet_files_recovery)} files tren mây."
            
            all_ids = []
            for remote_path in parquet_files_recovery:
                file_client = fs_client.get_file_client(remote_path)
                data = file_client.download_file().readall()
                df = pd.read_parquet(io.BytesIO(data))
                all_ids.extend(df["order_id"].tolist())
                
            expected_ids = {record["order_id"] for record in twelve_records}
            assert set(all_ids) == expected_ids, "Du lieu ADLS sau recovery khong khop voi 12 records nguon."
            assert len(all_ids) == 12, "Tong so record doc tu ADLS khong dung 12"
            
            print("  [PASS] Don rac va phuc hoi hoan hao. Batch 2 & 3 hoan tat, tong 12 records khong thieu khong thua.")

            print("-" * 50)
            print("CRASH AFTER CHECKPOINT / BEFORE PENDING DELETE TEST COMPLETE: PERFECT PASS")
            print("-" * 50)

    finally:
        _cleanup_local()
        print("\nHoan tat don dep file local test.")
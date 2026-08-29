from fastorder.storage.adls_client import (
    get_bronze_file_system_client,
)


fs_client = get_bronze_file_system_client()

test_marker = (
    "/extraction_id=test_customers_generic_"
)

paths = list(
    fs_client.get_paths(
        path="customers"
    )
)

test_files = [
    path.name
    for path in paths
    if (
        test_marker in path.name
        and not path.is_directory
    )
]

print(
    f"Found {len(test_files)} "
    "Customers test files."
)

for path in test_files:
    print(
        "DELETE:",
        path,
    )

for path in test_files:
    fs_client.delete_file(
        path
    )

print(
    "Customers test Bronze cleanup: DONE"
)
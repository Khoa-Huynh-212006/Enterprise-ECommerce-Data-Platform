from fastorder.ingestion.file_based.manifest_manager import (
    create_initial_manifest,
)


manifest = create_initial_manifest()

print(manifest)
print("Version:", manifest.version)
print("Source:", manifest.source_name)
print("Entries:", len(manifest.entries))